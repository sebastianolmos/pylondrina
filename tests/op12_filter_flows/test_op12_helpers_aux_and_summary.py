from __future__ import annotations

import pandas as pd

from pylondrina.transforms.flows_filtering import (
    _build_filter_flows_summary,
    _resolve_filtered_flow_to_trips,
)


# -----------------------------------------------------------------------------
# Bloque 5. Helper principal _resolve_filtered_flow_to_trips
# -----------------------------------------------------------------------------


def test_resolve_filtered_flow_to_trips_returns_not_requested_when_auxiliary_is_disabled(
    small_flowdataset_factory,
    request_ctx_factory,
):
    """Verifica que el auxiliar no se conserve cuando `keep_flow_to_trips=False`."""
    flows, _ = small_flowdataset_factory()
    issues = []

    kept_flow_ids = set(flows.flows["flow_id"].head(2).tolist())

    result, status = _resolve_filtered_flow_to_trips(
        flows.flow_to_trips,
        kept_flow_ids=kept_flow_ids,
        keep_flow_to_trips=False,
        issues=issues,
        request_ctx=request_ctx_factory(
            flows,
            keep_flow_to_trips=False,
        ),
    )

    assert result is None
    assert status == "not_requested"
    assert issues == []


def test_resolve_filtered_flow_to_trips_reports_missing_when_requested_auxiliary_is_absent(
    small_flowdataset_factory,
    request_ctx_factory,
    assert_issue_codes,
):
    """Verifica retorno `None` e issue informativo si el auxiliar solicitado no existe."""
    flows, _ = small_flowdataset_factory()
    issues = []

    kept_flow_ids = set(flows.flows["flow_id"].head(2).tolist())

    result, status = _resolve_filtered_flow_to_trips(
        None,
        kept_flow_ids=kept_flow_ids,
        keep_flow_to_trips=True,
        issues=issues,
        request_ctx=request_ctx_factory(
            flows,
            keep_flow_to_trips=True,
        ),
    )

    assert result is None
    assert status == "missing"

    assert_issue_codes(
        issues,
        expected_present=[
            "FLT_FLOW.AUX.FLOW_TO_TRIPS_REQUESTED_BUT_MISSING",
        ],
    )


def test_resolve_filtered_flow_to_trips_discards_invalid_auxiliary_structure(
    small_flowdataset_factory,
    request_ctx_factory,
    assert_issue_codes,
):
    """Verifica descarte explícito si el auxiliar no contiene su estructura mínima."""
    flows, _ = small_flowdataset_factory()
    issues = []

    bad_aux = pd.DataFrame(
        {
            "flow_id": flows.flows["flow_id"].head(2).tolist(),
            # falta movement_id
        }
    )

    kept_flow_ids = {flows.flows["flow_id"].iloc[0]}

    result, status = _resolve_filtered_flow_to_trips(
        bad_aux,
        kept_flow_ids=kept_flow_ids,
        keep_flow_to_trips=True,
        issues=issues,
        request_ctx=request_ctx_factory(
            flows,
            keep_flow_to_trips=True,
        ),
    )

    assert result is None
    assert status == "discarded_invalid"

    assert_issue_codes(
        issues,
        expected_present=[
            "FLT_FLOW.AUX.FLOW_TO_TRIPS_INVALID",
        ],
    )


def test_resolve_filtered_flow_to_trips_syncs_rows_to_retained_flow_ids(
    small_flowdataset_factory,
    request_ctx_factory,
    assert_issue_codes,
):
    """Verifica que el auxiliar conserve solo filas asociadas a `flow_id` retenidos."""
    flows, _ = small_flowdataset_factory()
    issues = []

    unique_aux_flow_ids = flows.flow_to_trips["flow_id"].drop_duplicates().tolist()
    kept_flow_ids = {unique_aux_flow_ids[0], unique_aux_flow_ids[-1]}

    expected = flows.flow_to_trips.loc[
        flows.flow_to_trips["flow_id"].isin(kept_flow_ids)
    ].copy(deep=True)

    result, status = _resolve_filtered_flow_to_trips(
        flows.flow_to_trips,
        kept_flow_ids=kept_flow_ids,
        keep_flow_to_trips=True,
        issues=issues,
        request_ctx=request_ctx_factory(
            flows,
            keep_flow_to_trips=True,
        ),
    )

    assert status == "synced"
    assert result is not None

    pd.testing.assert_frame_equal(result, expected)

    assert set(result["flow_id"].unique().tolist()) == kept_flow_ids

    assert_issue_codes(
        issues,
        expected_present=[
            "FLT_FLOW.AUX.FLOW_TO_TRIPS_SYNCED",
        ],
    )


# -----------------------------------------------------------------------------
# Bloque 6. Helper principal _build_filter_flows_summary
# -----------------------------------------------------------------------------


def test_build_filter_flows_summary_returns_stable_minimal_structure():
    """Verifica la estructura canónica del summary sin bloque opcional `limits`."""
    rows_in = 10
    rows_out = 4
    dropped_by_filter = {"where": 4, "h3_cells": 2}
    filters_requested = ["where", "h3_cells"]
    filters_applied = ["where", "h3_cells"]
    filters_omitted: list[str] = []
    flow_to_trips_status = "synced"

    summary = _build_filter_flows_summary(
        rows_in=rows_in,
        rows_out=rows_out,
        dropped_by_filter=dropped_by_filter,
        filters_requested=filters_requested,
        filters_applied=filters_applied,
        filters_omitted=filters_omitted,
        flow_to_trips_status=flow_to_trips_status,
        limits=None,
    )

    expected_summary = {
        "rows_in": rows_in,
        "rows_out": rows_out,
        "dropped_total": rows_in - rows_out,
        "dropped_by_filter": dropped_by_filter,
        "filters_requested": filters_requested,
        "filters_applied": filters_applied,
        "filters_omitted": filters_omitted,
        "flow_to_trips_status": flow_to_trips_status,
    }

    assert summary == expected_summary
    assert "limits" not in summary


def test_build_filter_flows_summary_includes_limits_when_present():
    """Verifica incorporación estable del bloque `limits` para truncamiento de issues."""
    rows_in = 10
    rows_out = 0
    dropped_by_filter = {"where": 6, "h3_cells": 4}
    filters_requested = ["where", "h3_cells"]
    filters_applied = ["where", "h3_cells"]
    filters_omitted: list[str] = []
    flow_to_trips_status = "missing"

    limits = {
        "max_issues": 3,
        "issues_truncated": True,
        "n_issues_emitted": 3,
        "n_issues_detected_total": 7,
    }

    summary = _build_filter_flows_summary(
        rows_in=rows_in,
        rows_out=rows_out,
        dropped_by_filter=dropped_by_filter,
        filters_requested=filters_requested,
        filters_applied=filters_applied,
        filters_omitted=filters_omitted,
        flow_to_trips_status=flow_to_trips_status,
        limits=limits,
    )

    expected_summary = {
        "rows_in": rows_in,
        "rows_out": rows_out,
        "dropped_total": rows_in - rows_out,
        "dropped_by_filter": dropped_by_filter,
        "filters_requested": filters_requested,
        "filters_applied": filters_applied,
        "filters_omitted": filters_omitted,
        "flow_to_trips_status": flow_to_trips_status,
        "limits": limits,
    }

    assert summary == expected_summary
    assert summary["limits"]["issues_truncated"] is True