from __future__ import annotations

import pandas as pd
import pytest

from pylondrina.errors import PylondrinaError
from pylondrina.queries.flows import (
    _apply_h3_rollup_if_needed,
    _coerce_datetime_series,
    _make_window_end,
    _make_window_start,
    _resolve_reconstruction_join_info,
    _safe_sort_correspondence_df,
    _truncate_query_issues,
    _unique_non_null_values,
)
from pylondrina.reports import Issue


# -----------------------------------------------------------------------------
# Bloque 1. Utilidades internas de soporte para reconstrucción
# -----------------------------------------------------------------------------


def test_resolve_reconstruction_join_info_builds_effective_join_keys_from_aggregation_spec(
    op13_small_flowdataset_factory,
    op13_request_ctx_factory,
):
    """Verifica resolución completa de llaves efectivas, ventanas y configuración temporal."""
    flows = op13_small_flowdataset_factory(
        include_flow_to_trips=False,
        include_extra_unmatched_flow=False,
    )

    issues = []
    request_ctx = op13_request_ctx_factory(
        n_flows_input=len(flows.flows),
        used_source="trips_argument",
        reconstruction_attempted=True,
        n_trips_input=4,
    )

    join_info = _resolve_reconstruction_join_info(
        flows.flows,
        aggregation_spec=flows.aggregation_spec,
        issues=issues,
        request_ctx=request_ctx,
    )

    assert issues == []
    assert join_info["join_key_columns"] == [
        "origin_h3_index",
        "destination_h3_index",
        "window_start_utc",
        "window_end_utc",
        "mode",
        "purpose",
    ]
    assert join_info["group_by"] == ["mode", "purpose"]
    assert join_info["window_columns"] == [
        "window_start_utc",
        "window_end_utc",
    ]
    assert join_info["time_aggregation"] == "hour"
    assert join_info["time_basis"] == "origin"
    assert join_info["h3_resolution_target"] == 8


def test_resolve_reconstruction_join_info_raises_when_group_by_is_not_interpretable(
    op13_small_flowdataset_factory,
    op13_request_ctx_factory,
):
    """Verifica error fatal cuando `group_by` no es una secuencia interpretable."""
    flows = op13_small_flowdataset_factory(
        include_flow_to_trips=False,
        include_extra_unmatched_flow=False,
    )

    issues = []
    request_ctx = op13_request_ctx_factory(
        n_flows_input=len(flows.flows),
        used_source="trips_argument",
        reconstruction_attempted=True,
        n_trips_input=4,
    )

    bad_aggregation_spec = dict(flows.aggregation_spec)
    bad_aggregation_spec["group_by"] = "mode"

    with pytest.raises(PylondrinaError) as excinfo:
        _resolve_reconstruction_join_info(
            flows.flows,
            aggregation_spec=bad_aggregation_spec,
            issues=issues,
            request_ctx=request_ctx,
        )

    error = excinfo.value

    assert error.issue is not None
    assert (
        error.issue.code
        == "GET_TRIPS_FROM_FLOWS.RECON.AGGREGATION_KEYS_UNRECOVERABLE"
    )
    assert error.issue.details["reason"] == "group_by_not_sequence"


def test_apply_h3_rollup_if_needed_rolls_to_coarser_resolution_without_mutating_input(
    op13_small_tripdataset_factory,
    op13_request_ctx_factory,
    h3_to_parent,
):
    """Verifica roll-up H3 a resolución más gruesa sin mutar el TripDataset fuente."""
    trips = op13_small_tripdataset_factory(res=8)

    issues = []
    request_ctx = op13_request_ctx_factory(
        used_source="trips_argument",
        reconstruction_attempted=True,
        n_trips_input=len(trips.data),
    )

    before = trips.data.copy(deep=True)

    rolled = _apply_h3_rollup_if_needed(
        trips.data.loc[
            :,
            ["origin_h3_index", "destination_h3_index"],
        ].copy(),
        target_resolution=7,
        issues=issues,
        request_ctx=request_ctx,
    )

    assert issues == []
    assert rolled is not None
    assert rolled.loc[0, "origin_h3_index"] == h3_to_parent(
        before.loc[0, "origin_h3_index"],
        7,
    )
    assert rolled.loc[0, "destination_h3_index"] == h3_to_parent(
        before.loc[0, "destination_h3_index"],
        7,
    )

    pd.testing.assert_frame_equal(trips.data, before)


def test_apply_h3_rollup_if_needed_raises_when_target_resolution_is_finer_than_input(
    op13_small_tripdataset_factory,
    op13_request_ctx_factory,
):
    """Verifica error fatal si se solicita una resolución H3 más fina que la disponible."""
    trips = op13_small_tripdataset_factory(res=8)

    issues = []
    request_ctx = op13_request_ctx_factory(
        used_source="trips_argument",
        reconstruction_attempted=True,
        n_trips_input=len(trips.data),
    )

    with pytest.raises(PylondrinaError) as excinfo:
        _apply_h3_rollup_if_needed(
            trips.data.loc[
                :,
                ["origin_h3_index", "destination_h3_index"],
            ].copy(),
            target_resolution=9,
            issues=issues,
            request_ctx=request_ctx,
        )

    error = excinfo.value

    assert error.issue is not None
    assert (
        error.issue.code
        == "GET_TRIPS_FROM_FLOWS.RECON.AGGREGATION_KEYS_UNRECOVERABLE"
    )
    assert (
        error.issue.details["reason"]
        == "target_h3_resolution_finer_than_input"
    )


def test_temporal_helpers_coerce_to_utc_naive_and_build_hourly_windows():
    """Verifica coerción temporal a UTC naive y construcción de ventanas horarias."""
    series = pd.Series(
        [
            "2026-01-01T08:05:00Z",
            "2026-01-01T08:15:00-03:00",
        ]
    )

    coerced = _coerce_datetime_series(series)
    window_start = _make_window_start(coerced, "hour")
    window_end = _make_window_end(window_start, "hour")

    assert coerced.iloc[0] == pd.Timestamp("2026-01-01 08:05:00")
    assert coerced.iloc[1] == pd.Timestamp("2026-01-01 11:15:00")

    assert window_start.iloc[0] == pd.Timestamp("2026-01-01 08:00:00")
    assert window_start.iloc[1] == pd.Timestamp("2026-01-01 11:00:00")

    assert window_end.iloc[0] == pd.Timestamp("2026-01-01 09:00:00")
    assert window_end.iloc[1] == pd.Timestamp("2026-01-01 12:00:00")


def test_truncate_query_issues_emits_contractual_limits_and_truncation_issue():
    """Verifica truncamiento de issues y construcción consistente del bloque `limits`."""
    issues_all = [
        Issue(
            level="warning",
            code="GET_TRIPS_FROM_FLOWS.SOURCE.PREFERRED_SOURCE_UNUSABLE",
            message="w1",
        ),
        Issue(
            level="warning",
            code="GET_TRIPS_FROM_FLOWS.OUTPUT.PARTIAL_COVERAGE",
            message="w2",
        ),
        Issue(
            level="warning",
            code="GET_TRIPS_FROM_FLOWS.OUTPUT.EMPTY_RESULT",
            message="w3",
        ),
    ]

    retained, limits = _truncate_query_issues(
        issues_all,
        max_issues=2,
    )

    assert len(retained) == 2
    assert (
        retained[-1].code
        == "GET_TRIPS_FROM_FLOWS.REPORT.ISSUES_TRUNCATED"
    )

    assert limits is not None
    assert limits["max_issues"] == 2
    assert limits["issues_truncated"] is True
    assert limits["n_issues_detected_total"] == len(issues_all)
    assert limits["n_issues_emitted"] == len(retained)


def test_sort_and_unique_helpers_preserve_stable_public_ordering_rules():
    """Verifica orden estable de correspondencias y unicidad no nula preservando aparición."""
    correspondence = pd.DataFrame(
        [
            {"flow_id": "f_b", "movement_id": "m2"},
            {"flow_id": "f_a", "movement_id": "m3"},
            {"flow_id": "f_a", "movement_id": "m1"},
        ]
    )

    sorted_df = _safe_sort_correspondence_df(correspondence)

    assert sorted_df["flow_id"].tolist() == ["f_a", "f_a", "f_b"]
    assert sorted_df["movement_id"].tolist() == ["m1", "m3", "m2"]

    values = _unique_non_null_values(
        [None, "m1", "m2", "m1", pd.NA, "m3", "m2"]
    )

    assert values == ["m1", "m2", "m3"]