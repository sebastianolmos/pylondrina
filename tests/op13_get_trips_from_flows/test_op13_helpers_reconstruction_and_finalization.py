from __future__ import annotations

from copy import deepcopy

import pandas as pd
import pytest

from pylondrina.datasets import TripDataset
from pylondrina.errors import PylondrinaError
from pylondrina.queries.flows import (
    _finalize_flow_trip_correspondence,
    _reconstruct_correspondence_from_trips,
)


# -----------------------------------------------------------------------------
# Bloque 2.6 a 2.7 - Reconstrucción exacta desde trips
# -----------------------------------------------------------------------------


def test_reconstruct_correspondence_from_trips_rebuilds_exact_pairs_and_preserves_trip_input(
    op13_small_tripdataset_factory,
    op13_small_flowdataset_factory,
    op13_request_ctx_factory,
):
    """Verifica reconstrucción exacta flow→movement, `trip_id` opcional y no mutación de `trips.data`."""
    trips = op13_small_tripdataset_factory()
    flows = op13_small_flowdataset_factory(
        include_flow_to_trips=False,
        include_extra_unmatched_flow=True,
    )

    issues = []
    request_ctx = op13_request_ctx_factory(
        n_flows_input=len(flows.flows),
        n_trips_input=len(trips.data),
        used_source="trips_argument",
        reconstruction_attempted=True,
    )

    trips_before = trips.data.copy(deep=True)

    provisional, movement_universe, join_info = (
        _reconstruct_correspondence_from_trips(
            flows.flows,
            trips,
            aggregation_spec=flows.aggregation_spec,
            issues=issues,
            request_ctx=request_ctx,
        )
    )

    assert issues == []
    assert provisional.columns.tolist() == [
        "flow_id",
        "movement_id",
        "trip_id",
    ]

    expected_pairs = pd.DataFrame(
        [
            {
                "flow_id": "f_ab_bus_work_h08",
                "movement_id": "m0",
                "trip_id": "t0",
            },
            {
                "flow_id": "f_ab_bus_work_h08",
                "movement_id": "m1",
                "trip_id": "t1",
            },
            {
                "flow_id": "f_ac_metro_study_h09",
                "movement_id": "m2",
                "trip_id": "t2",
            },
        ]
    )

    pd.testing.assert_frame_equal(
        provisional.reset_index(drop=True),
        expected_pairs.reset_index(drop=True),
        check_dtype=False,
        check_categorical=False,
    )

    assert movement_universe == trips.data["movement_id"].dropna().tolist()

    assert join_info["join_key_columns"] == [
        "origin_h3_index",
        "destination_h3_index",
        "window_start_utc",
        "window_end_utc",
        "mode",
        "purpose",
    ]

    pd.testing.assert_frame_equal(trips.data, trips_before)


def test_reconstruct_correspondence_from_trips_raises_when_required_trip_columns_are_missing(
    op13_small_tripdataset_factory,
    op13_small_flowdataset_factory,
    op13_request_ctx_factory,
):
    """Verifica error fatal cuando faltan columnas necesarias para reproducir la llave efectiva."""
    trips = op13_small_tripdataset_factory()

    trips_bad = TripDataset(
        data=trips.data.drop(columns=["purpose"]).copy(deep=True),
        schema=trips.schema,
        metadata=deepcopy(trips.metadata),
    )

    flows = op13_small_flowdataset_factory(
        include_flow_to_trips=False,
        include_extra_unmatched_flow=False,
    )

    issues = []
    request_ctx = op13_request_ctx_factory(
        n_flows_input=len(flows.flows),
        n_trips_input=len(trips_bad.data),
        used_source="trips_argument",
        reconstruction_attempted=True,
    )

    with pytest.raises(PylondrinaError) as excinfo:
        _reconstruct_correspondence_from_trips(
            flows.flows,
            trips_bad,
            aggregation_spec=flows.aggregation_spec,
            issues=issues,
            request_ctx=request_ctx,
        )

    error = excinfo.value

    assert error.issue is not None
    assert (
        error.issue.code
        == "GET_TRIPS_FROM_FLOWS.RECON.MISSING_REQUIRED_COLUMNS"
    )
    assert "purpose" in error.issue.details["missing_columns"]


# -----------------------------------------------------------------------------
# Bloque 2.8 a 2.10 - Normalización final y cobertura
# -----------------------------------------------------------------------------


def test_finalize_flow_trip_correspondence_normalizes_duplicates_foreign_flows_and_summary(
    op13_small_flowdataset_factory,
    op13_request_ctx_factory,
):
    """Verifica filtrado de `flow_id` ajenos, deduplicación exacta, orden estable y summary final."""
    flows = op13_small_flowdataset_factory(
        include_flow_to_trips=False,
        include_extra_unmatched_flow=False,
    )

    provisional = pd.DataFrame(
        [
            {
                "flow_id": "f_ac_metro_study_h09",
                "movement_id": "m2",
                "trip_id": "t2",
            },
            {
                "flow_id": "f_ab_bus_work_h08",
                "movement_id": "m1",
                "trip_id": "t1",
            },
            {
                "flow_id": "f_ab_bus_work_h08",
                "movement_id": "m0",
                "trip_id": "t0",
            },
            {
                "flow_id": "f_ab_bus_work_h08",
                "movement_id": "m0",
                "trip_id": "t0",
            },
            {
                "flow_id": "f_unknown",
                "movement_id": "mx",
                "trip_id": "tx",
            },
        ]
    )

    issues = []
    request_ctx = op13_request_ctx_factory(
        n_flows_input=len(flows.flows),
        used_source="trips_argument",
        reconstruction_attempted=True,
        n_trips_input=3,
    )

    final_df, summary = _finalize_flow_trip_correspondence(
        provisional,
        flows_df=flows.flows,
        movement_universe=["m2", "m1", "m0"],
        issues=issues,
        request_ctx=request_ctx,
        join_info={
            "join_key_columns": [
                "origin_h3_index",
                "destination_h3_index",
                "window_start_utc",
                "window_end_utc",
                "mode",
                "purpose",
            ],
            "group_by": ["mode", "purpose"],
            "window_columns": [
                "window_start_utc",
                "window_end_utc",
            ],
        },
    )

    expected_final = pd.DataFrame(
        [
            {
                "flow_id": "f_ab_bus_work_h08",
                "movement_id": "m0",
                "trip_id": "t0",
            },
            {
                "flow_id": "f_ab_bus_work_h08",
                "movement_id": "m1",
                "trip_id": "t1",
            },
            {
                "flow_id": "f_ac_metro_study_h09",
                "movement_id": "m2",
                "trip_id": "t2",
            },
        ]
    )

    assert issues == []
    assert final_df.columns.tolist() == [
        "flow_id",
        "movement_id",
        "trip_id",
    ]

    pd.testing.assert_frame_equal(
        final_df.reset_index(drop=True),
        expected_final.reset_index(drop=True),
        check_dtype=False,
        check_categorical=False,
    )

    assert summary == {
        "n_rows_out": len(expected_final),
        "n_unique_flows_out": expected_final["flow_id"].nunique(),
        "n_unique_movements_out": expected_final["movement_id"].nunique(),
        "n_unmatched_flows": 0,
        "n_unmatched_movements": 0,
    }


def test_finalize_flow_trip_correspondence_reports_partial_coverage(
    op13_small_flowdataset_factory,
    op13_request_ctx_factory,
    assert_issue_codes,
):
    """Verifica warning de cobertura parcial cuando quedan flows o movements sin correspondencia."""
    flows = op13_small_flowdataset_factory(
        include_flow_to_trips=False,
        include_extra_unmatched_flow=True,
    )

    provisional = pd.DataFrame(
        [
            {
                "flow_id": "f_ab_bus_work_h08",
                "movement_id": "m0",
                "trip_id": "t0",
            },
            {
                "flow_id": "f_ab_bus_work_h08",
                "movement_id": "m1",
                "trip_id": "t1",
            },
            {
                "flow_id": "f_ac_metro_study_h09",
                "movement_id": "m2",
                "trip_id": "t2",
            },
        ]
    )

    movement_universe = ["m0", "m1", "m2", "m3"]

    issues = []
    request_ctx = op13_request_ctx_factory(
        n_flows_input=len(flows.flows),
        used_source="trips_argument",
        reconstruction_attempted=True,
        n_trips_input=len(movement_universe),
    )

    final_df, summary = _finalize_flow_trip_correspondence(
        provisional,
        flows_df=flows.flows,
        movement_universe=movement_universe,
        issues=issues,
        request_ctx=request_ctx,
        join_info={
            "join_key_columns": [
                "origin_h3_index",
                "destination_h3_index",
                "window_start_utc",
                "window_end_utc",
                "mode",
                "purpose",
            ],
            "group_by": ["mode", "purpose"],
            "window_columns": [
                "window_start_utc",
                "window_end_utc",
            ],
        },
    )

    matched_flow_ids = set(final_df["flow_id"].dropna().tolist())
    valid_flow_ids = set(flows.flows["flow_id"].dropna().tolist())
    matched_movement_ids = set(final_df["movement_id"].dropna().tolist())
    all_movement_ids = set(movement_universe)

    assert len(final_df) == len(provisional)
    assert summary["n_unmatched_flows"] == len(
        valid_flow_ids - matched_flow_ids
    )
    assert summary["n_unmatched_movements"] == len(
        all_movement_ids - matched_movement_ids
    )

    assert_issue_codes(
        issues,
        ["GET_TRIPS_FROM_FLOWS.OUTPUT.PARTIAL_COVERAGE"],
    )


def test_finalize_flow_trip_correspondence_returns_empty_result_with_warning_when_no_pairs_remain(
    op13_small_flowdataset_factory,
    op13_request_ctx_factory,
    assert_issue_codes,
):
    """Verifica salida vacía retornable y warning explícito cuando no queda ninguna correspondencia."""
    flows = op13_small_flowdataset_factory(
        include_flow_to_trips=False,
        include_extra_unmatched_flow=False,
    )

    provisional = pd.DataFrame(columns=["flow_id", "movement_id"])
    movement_universe = ["m0", "m1"]

    issues = []
    request_ctx = op13_request_ctx_factory(
        n_flows_input=len(flows.flows),
        used_source="trips_argument",
        reconstruction_attempted=True,
        n_trips_input=len(movement_universe),
    )

    final_df, summary = _finalize_flow_trip_correspondence(
        provisional,
        flows_df=flows.flows,
        movement_universe=movement_universe,
        issues=issues,
        request_ctx=request_ctx,
        join_info={
            "join_key_columns": [
                "origin_h3_index",
                "destination_h3_index",
                "window_start_utc",
                "window_end_utc",
                "mode",
                "purpose",
            ],
            "group_by": ["mode", "purpose"],
            "window_columns": [
                "window_start_utc",
                "window_end_utc",
            ],
        },
    )

    valid_flow_ids = set(flows.flows["flow_id"].dropna().tolist())
    all_movement_ids = set(movement_universe)

    assert final_df.empty is True
    assert summary == {
        "n_rows_out": 0,
        "n_unique_flows_out": 0,
        "n_unique_movements_out": 0,
        "n_unmatched_flows": len(valid_flow_ids),
        "n_unmatched_movements": len(all_movement_ids),
    }

    assert_issue_codes(
        issues,
        ["GET_TRIPS_FROM_FLOWS.OUTPUT.EMPTY_RESULT"],
    )