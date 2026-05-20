from __future__ import annotations

from copy import deepcopy

import pandas as pd
import pytest

from pylondrina.errors import PylondrinaError
from pylondrina.queries.flows import get_trips_from_flows


# -----------------------------------------------------------------------------
# Helpers locales del archivo
# -----------------------------------------------------------------------------


def _assert_flowdataset_unchanged(flows, snapshot: dict) -> None:
    """Verifica que OP-13 no haya mutado el FlowDataset consultado."""
    pd.testing.assert_frame_equal(flows.flows, snapshot["flows"])

    if snapshot["flow_to_trips"] is None:
        assert flows.flow_to_trips is None
    else:
        pd.testing.assert_frame_equal(
            flows.flow_to_trips,
            snapshot["flow_to_trips"],
        )

    assert flows.aggregation_spec == snapshot["aggregation_spec"]
    assert flows.metadata == snapshot["metadata"]
    assert flows.provenance == snapshot["provenance"]
    assert flows.source_trips is snapshot["source_trips"]


def _assert_tripdataset_unchanged(trips, snapshot: dict) -> None:
    """Verifica que OP-13 no haya mutado el TripDataset usado para reconstrucción."""
    pd.testing.assert_frame_equal(trips.data, snapshot["data"])
    assert trips.metadata == snapshot["metadata"]
    assert trips.provenance == snapshot["provenance"]
    assert trips.schema_version == snapshot["schema_version"]


def _sort_correspondence(df: pd.DataFrame) -> pd.DataFrame:
    """Ordena una correspondencia flow→trip por las columnas contractuales disponibles."""
    sort_columns = [
        column
        for column in ["flow_id", "movement_id", "trip_id"]
        if column in df.columns
    ]
    return df.sort_values(sort_columns).reset_index(drop=True)


def _summary_from_correspondence(
    correspondence_df: pd.DataFrame,
    *,
    flows_df: pd.DataFrame,
    trips_df: pd.DataFrame | None,
) -> dict:
    """Construye el summary esperado desde la salida y los universos de entrada."""
    matched_flow_ids = set(correspondence_df["flow_id"].dropna())
    input_flow_ids = set(flows_df["flow_id"].dropna())

    if trips_df is None:
        n_unmatched_movements = 0
    else:
        matched_movement_ids = set(correspondence_df["movement_id"].dropna())
        input_movement_ids = set(trips_df["movement_id"].dropna())
        n_unmatched_movements = len(input_movement_ids - matched_movement_ids)

    return {
        "n_rows_out": len(correspondence_df),
        "n_unique_flows_out": correspondence_df["flow_id"].nunique(),
        "n_unique_movements_out": correspondence_df["movement_id"].nunique(),
        "n_unmatched_flows": len(input_flow_ids - matched_flow_ids),
        "n_unmatched_movements": n_unmatched_movements,
    }


def _expected_hourly_correspondence_from_trips(
    *,
    flows_df: pd.DataFrame,
    trips_df: pd.DataFrame,
) -> pd.DataFrame:
    """Deriva la correspondencia esperada para flows horarios H3 resolución 8."""
    trips_prepared = trips_df.copy(deep=True)

    origin_time = pd.to_datetime(
        trips_prepared["origin_time_utc"],
        utc=True,
        errors="coerce",
    ).dt.tz_convert(None)

    trips_prepared["window_start_utc"] = origin_time.dt.floor("h")
    trips_prepared["window_end_utc"] = (
        trips_prepared["window_start_utc"] + pd.Timedelta(hours=1)
    )

    join_columns = [
        "origin_h3_index",
        "destination_h3_index",
        "window_start_utc",
        "window_end_utc",
        "mode",
        "purpose",
    ]

    joined = trips_prepared.merge(
        flows_df.loc[:, ["flow_id", *join_columns]],
        on=join_columns,
        how="inner",
    )

    return _sort_correspondence(
        joined.loc[:, ["flow_id", "movement_id", "trip_id"]]
    )


def _expected_daily_rollup_correspondence_from_trips(
    *,
    flows_df: pd.DataFrame,
    trips_df: pd.DataFrame,
    h3_to_parent,
    target_resolution: int,
) -> pd.DataFrame:
    """Deriva la correspondencia esperada para flows con roll-up H3 y ventana diaria."""
    trips_prepared = trips_df.copy(deep=True)

    trips_prepared["origin_h3_index"] = trips_prepared["origin_h3_index"].map(
        lambda cell: h3_to_parent(cell, target_resolution)
    )
    trips_prepared["destination_h3_index"] = trips_prepared[
        "destination_h3_index"
    ].map(lambda cell: h3_to_parent(cell, target_resolution))

    origin_time = pd.to_datetime(
        trips_prepared["origin_time_utc"],
        utc=True,
        errors="coerce",
    ).dt.tz_convert(None)

    trips_prepared["window_start_utc"] = origin_time.dt.floor("D")
    trips_prepared["window_end_utc"] = (
        trips_prepared["window_start_utc"] + pd.Timedelta(days=1)
    )

    join_columns = [
        "origin_h3_index",
        "destination_h3_index",
        "window_start_utc",
        "window_end_utc",
        "mode",
        "purpose",
    ]

    joined = trips_prepared.merge(
        flows_df.loc[:, ["flow_id", *join_columns]],
        on=join_columns,
        how="inner",
    )

    return _sort_correspondence(
        joined.loc[:, ["flow_id", "movement_id", "trip_id"]]
    )


# -----------------------------------------------------------------------------
# Test 1.1 - Camino feliz usando flow_to_trips como fuente directa
# -----------------------------------------------------------------------------


def test_get_trips_from_flows_integration_uses_direct_flow_to_trips_without_side_effects(
    flowdataset_with_trip_links_factory,
    snapshot_flowdataset_state,
):
    """Verifica happy path con `flow_to_trips`, reporte estable y ausencia de side effects."""
    flows = deepcopy(flowdataset_with_trip_links_factory())
    flows_before = snapshot_flowdataset_state(flows)

    correspondence_df, report = get_trips_from_flows(
        flows,
        max_issues=20,
    )

    expected_correspondence = _sort_correspondence(
        flows.flow_to_trips.loc[:, ["flow_id", "movement_id"]].copy(deep=True)
    )

    assert correspondence_df.columns.tolist() == ["flow_id", "movement_id"]

    pd.testing.assert_frame_equal(
        _sort_correspondence(correspondence_df),
        expected_correspondence,
        check_dtype=False,
        check_categorical=False,
    )

    assert report.ok is True
    assert report.parameters == {
        "max_issues": 20,
        "used_source": "flow_to_trips",
        "reconstruction_attempted": False,
        "n_flows_input": len(flows.flows),
        "n_trips_input": None,
    }
    assert report.summary == _summary_from_correspondence(
        correspondence_df,
        flows_df=flows.flows,
        trips_df=None,
    )
    assert report.issues == []

    _assert_flowdataset_unchanged(flows, flows_before)


# -----------------------------------------------------------------------------
# Test 1.2 - Reconstrucción exacta desde trips_argument
# -----------------------------------------------------------------------------


def test_get_trips_from_flows_integration_reconstructs_from_trips_argument_without_side_effects(
    canonical_tripdataset_factory,
    flowdataset_small_factory,
    snapshot_flowdataset_state,
    snapshot_tripdataset_state,
    assert_issue_codes,
):
    """Verifica reconstrucción exacta desde `trips_argument`, `trip_id` opcional y cobertura parcial."""
    trips = deepcopy(canonical_tripdataset_factory())
    flows = deepcopy(flowdataset_small_factory())

    flows_before = snapshot_flowdataset_state(flows)
    trips_before = snapshot_tripdataset_state(trips)

    correspondence_df, report = get_trips_from_flows(
        flows,
        trips=trips,
        max_issues=20,
    )

    expected_correspondence = _expected_hourly_correspondence_from_trips(
        flows_df=flows.flows,
        trips_df=trips.data,
    )

    assert correspondence_df.columns.tolist() == [
        "flow_id",
        "movement_id",
        "trip_id",
    ]

    pd.testing.assert_frame_equal(
        _sort_correspondence(correspondence_df),
        expected_correspondence,
        check_dtype=False,
        check_categorical=False,
    )

    assert report.ok is True
    assert report.parameters == {
        "max_issues": 20,
        "used_source": "trips_argument",
        "reconstruction_attempted": True,
        "n_flows_input": len(flows.flows),
        "n_trips_input": len(trips.data),
    }
    assert report.summary == _summary_from_correspondence(
        correspondence_df,
        flows_df=flows.flows,
        trips_df=trips.data,
    )

    assert_issue_codes(
        report.issues,
        ["GET_TRIPS_FROM_FLOWS.OUTPUT.PARTIAL_COVERAGE"],
    )

    _assert_flowdataset_unchanged(flows, flows_before)
    _assert_tripdataset_unchanged(trips, trips_before)


# -----------------------------------------------------------------------------
# Test 1.3 - Degradación con warning y fallback a trips_argument
# -----------------------------------------------------------------------------


def test_get_trips_from_flows_integration_degrades_from_invalid_direct_auxiliary_to_trips_argument(
    canonical_tripdataset_factory,
    flowdataset_small_factory,
    snapshot_flowdataset_state,
    snapshot_tripdataset_state,
):
    """Verifica warning por auxiliar inválido y fallback correcto hacia `trips_argument`."""
    trips = deepcopy(canonical_tripdataset_factory())
    flows = deepcopy(flowdataset_small_factory())

    flows.flow_to_trips = pd.DataFrame(
        [
            {"flow_id": "f_ab_bus_work_h08"},
            {"flow_id": "f_ac_metro_study_h09"},
        ]
    )

    flows_before = snapshot_flowdataset_state(flows)
    trips_before = snapshot_tripdataset_state(trips)

    correspondence_df, report = get_trips_from_flows(
        flows,
        trips=trips,
        max_issues=20,
    )

    expected_correspondence = _expected_hourly_correspondence_from_trips(
        flows_df=flows.flows,
        trips_df=trips.data,
    )

    codes = [issue.code for issue in report.issues]

    assert report.ok is True
    assert report.parameters["used_source"] == "trips_argument"
    assert report.parameters["reconstruction_attempted"] is True
    assert report.parameters["n_trips_input"] == len(trips.data)

    assert (
        "GET_TRIPS_FROM_FLOWS.SOURCE.PREFERRED_SOURCE_UNUSABLE"
        in codes
    )
    assert "GET_TRIPS_FROM_FLOWS.OUTPUT.PARTIAL_COVERAGE" in codes

    assert correspondence_df.columns.tolist() == [
        "flow_id",
        "movement_id",
        "trip_id",
    ]

    pd.testing.assert_frame_equal(
        _sort_correspondence(correspondence_df),
        expected_correspondence,
        check_dtype=False,
        check_categorical=False,
    )

    _assert_flowdataset_unchanged(flows, flows_before)
    _assert_tripdataset_unchanged(trips, trips_before)


# -----------------------------------------------------------------------------
# Test 1.4 - Tercer fallback: uso de flows.source_trips
# -----------------------------------------------------------------------------


def test_get_trips_from_flows_integration_uses_flows_source_trips_as_third_fallback(
    flowdataset_source_trips_only_factory,
    snapshot_flowdataset_state,
    assert_issue_codes,
):
    """Verifica uso de `flows.source_trips` como fallback vivo en memoria."""
    flows = deepcopy(flowdataset_source_trips_only_factory())
    flows_before = snapshot_flowdataset_state(flows)

    source_trips = flows.source_trips

    correspondence_df, report = get_trips_from_flows(
        flows,
        trips=None,
        max_issues=20,
    )

    expected_correspondence = _expected_hourly_correspondence_from_trips(
        flows_df=flows.flows,
        trips_df=source_trips.data,
    )

    assert correspondence_df.columns.tolist() == [
        "flow_id",
        "movement_id",
        "trip_id",
    ]

    pd.testing.assert_frame_equal(
        _sort_correspondence(correspondence_df),
        expected_correspondence,
        check_dtype=False,
        check_categorical=False,
    )

    assert report.ok is True
    assert report.parameters == {
        "max_issues": 20,
        "used_source": "flows.source_trips",
        "reconstruction_attempted": True,
        "n_flows_input": len(flows.flows),
        "n_trips_input": len(source_trips.data),
    }
    assert report.summary == _summary_from_correspondence(
        correspondence_df,
        flows_df=flows.flows,
        trips_df=source_trips.data,
    )

    assert_issue_codes(
        report.issues,
        ["GET_TRIPS_FROM_FLOWS.OUTPUT.PARTIAL_COVERAGE"],
    )

    _assert_flowdataset_unchanged(flows, flows_before)


# -----------------------------------------------------------------------------
# Test 1.5 - Fatal de precondición: falta flow_id en flows.flows
# -----------------------------------------------------------------------------


def test_get_trips_from_flows_integration_raises_when_flow_id_is_missing(
    flowdataset_with_trip_links_factory,
    snapshot_flowdataset_state,
):
    """Verifica fatal de precondición cuando `flows.flows` no contiene `flow_id`."""
    flows = deepcopy(flowdataset_with_trip_links_factory())
    flows.flows = flows.flows.drop(columns=["flow_id"]).copy()

    flows_before = snapshot_flowdataset_state(flows)

    with pytest.raises(PylondrinaError) as excinfo:
        get_trips_from_flows(
            flows,
            max_issues=20,
        )

    error = excinfo.value

    assert error.issue is not None
    assert error.issue.code == "GET_TRIPS_FROM_FLOWS.DATA.MISSING_FLOW_ID"

    _assert_flowdataset_unchanged(flows, flows_before)


# -----------------------------------------------------------------------------
# Test 1.6 - Fatal operativo: no existe ninguna fuente usable
# -----------------------------------------------------------------------------


def test_get_trips_from_flows_integration_raises_when_no_usable_source_exists(
    flowdataset_small_factory,
    snapshot_flowdataset_state,
):
    """Verifica fatal cuando no hay `flow_to_trips`, `trips_argument` ni `flows.source_trips`."""
    flows = deepcopy(flowdataset_small_factory())
    flows.flow_to_trips = None
    flows.source_trips = None

    flows_before = snapshot_flowdataset_state(flows)

    with pytest.raises(PylondrinaError) as excinfo:
        get_trips_from_flows(
            flows,
            trips=None,
            max_issues=20,
        )

    error = excinfo.value

    assert error.issue is not None
    assert error.issue.code == "GET_TRIPS_FROM_FLOWS.SOURCE.NO_USABLE_SOURCE"

    _assert_flowdataset_unchanged(flows, flows_before)


# -----------------------------------------------------------------------------
# Test 1.7 - Roll-up H3 y ventanas temporales diarias
# -----------------------------------------------------------------------------


def test_get_trips_from_flows_integration_reconstructs_with_h3_rollup_and_daily_windows(
    canonical_tripdataset_factory,
    flowdataset_rollup_temporal_factory,
    h3_to_parent,
    snapshot_flowdataset_state,
    snapshot_tripdataset_state,
    assert_issue_codes,
):
    """Verifica reconstrucción con roll-up H3 a resolución gruesa y ventanas diarias."""
    trips = deepcopy(canonical_tripdataset_factory())
    flows = deepcopy(flowdataset_rollup_temporal_factory())

    flows_before = snapshot_flowdataset_state(flows)
    trips_before = snapshot_tripdataset_state(trips)

    correspondence_df, report = get_trips_from_flows(
        flows,
        trips=trips,
        max_issues=20,
    )

    expected_correspondence = _expected_daily_rollup_correspondence_from_trips(
        flows_df=flows.flows,
        trips_df=trips.data,
        h3_to_parent=h3_to_parent,
        target_resolution=flows.aggregation_spec["h3_resolution"],
    )

    assert correspondence_df.columns.tolist() == [
        "flow_id",
        "movement_id",
        "trip_id",
    ]

    pd.testing.assert_frame_equal(
        _sort_correspondence(correspondence_df),
        expected_correspondence,
        check_dtype=False,
        check_categorical=False,
    )

    assert report.ok is True
    assert report.parameters == {
        "max_issues": 20,
        "used_source": "trips_argument",
        "reconstruction_attempted": True,
        "n_flows_input": len(flows.flows),
        "n_trips_input": len(trips.data),
    }
    assert report.summary == _summary_from_correspondence(
        correspondence_df,
        flows_df=flows.flows,
        trips_df=trips.data,
    )

    assert_issue_codes(
        report.issues,
        ["GET_TRIPS_FROM_FLOWS.OUTPUT.PARTIAL_COVERAGE"],
    )

    _assert_flowdataset_unchanged(flows, flows_before)
    _assert_tripdataset_unchanged(trips, trips_before)