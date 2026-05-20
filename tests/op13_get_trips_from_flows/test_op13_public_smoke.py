from __future__ import annotations

import pandas as pd
import pytest

from pylondrina.errors import PylondrinaError
from pylondrina.queries.flows import get_trips_from_flows


# -----------------------------------------------------------------------------
# Bloque 3. Smoke tests públicos de get_trips_from_flows
# -----------------------------------------------------------------------------


def test_get_trips_from_flows_smoke_uses_direct_flow_to_trips_without_side_effects(
    op13_small_flowdataset_factory,
    snapshot_flowdataset_state,
):
    """Verifica el camino feliz público usando `flow_to_trips` directo y sin mutar el FlowDataset."""
    flows = op13_small_flowdataset_factory(
        include_flow_to_trips=True,
        duplicate_direct_pairs=False,
        include_extra_unmatched_flow=False,
    )

    flows_before = snapshot_flowdataset_state(flows)

    correspondence_df, report = get_trips_from_flows(
        flows,
        max_issues=20,
    )

    expected_correspondence = (
        flows.flow_to_trips.loc[:, ["flow_id", "movement_id"]]
        .copy(deep=True)
        .reset_index(drop=True)
    )

    assert correspondence_df.columns.tolist() == ["flow_id", "movement_id"]

    pd.testing.assert_frame_equal(
        correspondence_df.reset_index(drop=True),
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

    assert report.summary == {
        "n_rows_out": len(expected_correspondence),
        "n_unique_flows_out": expected_correspondence["flow_id"].nunique(),
        "n_unique_movements_out": expected_correspondence[
            "movement_id"
        ].nunique(),
        "n_unmatched_flows": 0,
        "n_unmatched_movements": 0,
    }
    assert report.issues == []

    pd.testing.assert_frame_equal(flows.flows, flows_before["flows"])
    pd.testing.assert_frame_equal(
        flows.flow_to_trips,
        flows_before["flow_to_trips"],
    )
    assert flows.aggregation_spec == flows_before["aggregation_spec"]
    assert flows.metadata == flows_before["metadata"]
    assert flows.provenance == flows_before["provenance"]
    assert flows.source_trips is flows_before["source_trips"]


def test_get_trips_from_flows_smoke_reconstructs_from_trips_argument_and_preserves_inputs(
    op13_small_tripdataset_factory,
    op13_small_flowdataset_factory,
    snapshot_flowdataset_state,
    snapshot_tripdataset_state,
    assert_issue_codes,
):
    """Verifica reconstrucción pública desde `trips_argument`, `trip_id` opcional y ausencia de mutaciones."""
    trips = op13_small_tripdataset_factory()
    flows = op13_small_flowdataset_factory(
        include_flow_to_trips=False,
        include_extra_unmatched_flow=False,
    )

    flows_before = snapshot_flowdataset_state(flows)
    trips_before = snapshot_tripdataset_state(trips)

    correspondence_df, report = get_trips_from_flows(
        flows,
        trips=trips,
        max_issues=20,
    )

    assert correspondence_df.columns.tolist() == [
        "flow_id",
        "movement_id",
        "trip_id",
    ]

    assert set(correspondence_df["flow_id"]).issubset(
        set(flows.flows["flow_id"])
    )
    assert set(correspondence_df["movement_id"]).issubset(
        set(trips.data["movement_id"])
    )
    assert set(correspondence_df["trip_id"]).issubset(
        set(trips.data["trip_id"])
    )

    assert report.ok is True
    assert report.parameters == {
        "max_issues": 20,
        "used_source": "trips_argument",
        "reconstruction_attempted": True,
        "n_flows_input": len(flows.flows),
        "n_trips_input": len(trips.data),
    }

    matched_flow_ids = set(correspondence_df["flow_id"].dropna())
    matched_movement_ids = set(correspondence_df["movement_id"].dropna())
    input_flow_ids = set(flows.flows["flow_id"].dropna())
    input_movement_ids = set(trips.data["movement_id"].dropna())

    assert report.summary == {
        "n_rows_out": len(correspondence_df),
        "n_unique_flows_out": correspondence_df["flow_id"].nunique(),
        "n_unique_movements_out": correspondence_df[
            "movement_id"
        ].nunique(),
        "n_unmatched_flows": len(input_flow_ids - matched_flow_ids),
        "n_unmatched_movements": len(
            input_movement_ids - matched_movement_ids
        ),
    }

    assert_issue_codes(
        report.issues,
        ["GET_TRIPS_FROM_FLOWS.OUTPUT.PARTIAL_COVERAGE"],
    )

    pd.testing.assert_frame_equal(flows.flows, flows_before["flows"])
    assert flows.flow_to_trips is flows_before["flow_to_trips"]
    assert flows.aggregation_spec == flows_before["aggregation_spec"]
    assert flows.metadata == flows_before["metadata"]
    assert flows.provenance == flows_before["provenance"]
    assert flows.source_trips is flows_before["source_trips"]

    pd.testing.assert_frame_equal(trips.data, trips_before["data"])
    assert trips.schema is trips_before["schema"]
    assert getattr(trips, "schema_effective", None) is trips_before[
        "schema_effective"
    ]
    assert trips.schema_version == trips_before["schema_version"]
    assert trips.metadata == trips_before["metadata"]
    assert getattr(trips, "provenance", None) == trips_before["provenance"]


def test_get_trips_from_flows_smoke_degrades_to_trips_argument_when_direct_auxiliary_is_invalid(
    op13_small_tripdataset_factory,
    op13_small_flowdataset_factory,
    snapshot_flowdataset_state,
    snapshot_tripdataset_state,
):
    """Verifica fallback público a `trips_argument` cuando `flow_to_trips` existe pero es inválido."""
    trips = op13_small_tripdataset_factory()
    flows = op13_small_flowdataset_factory(
        include_flow_to_trips=True,
        include_extra_unmatched_flow=False,
    )

    flows.flow_to_trips = flows.flow_to_trips.loc[:, ["flow_id"]].copy()

    flows_before = snapshot_flowdataset_state(flows)
    trips_before = snapshot_tripdataset_state(trips)

    correspondence_df, report = get_trips_from_flows(
        flows,
        trips=trips,
        max_issues=20,
    )

    issue_codes = [issue.code for issue in report.issues]

    assert report.parameters["used_source"] == "trips_argument"
    assert report.parameters["reconstruction_attempted"] is True
    assert report.parameters["n_trips_input"] == len(trips.data)

    assert (
        "GET_TRIPS_FROM_FLOWS.SOURCE.PREFERRED_SOURCE_UNUSABLE"
        in issue_codes
    )

    assert correspondence_df.columns.tolist() == [
        "flow_id",
        "movement_id",
        "trip_id",
    ]
    assert set(correspondence_df["flow_id"]).issubset(
        set(flows.flows["flow_id"])
    )
    assert set(correspondence_df["movement_id"]).issubset(
        set(trips.data["movement_id"])
    )

    pd.testing.assert_frame_equal(flows.flows, flows_before["flows"])
    pd.testing.assert_frame_equal(
        flows.flow_to_trips,
        flows_before["flow_to_trips"],
    )
    assert flows.metadata == flows_before["metadata"]

    pd.testing.assert_frame_equal(trips.data, trips_before["data"])
    assert trips.metadata == trips_before["metadata"]


def test_get_trips_from_flows_smoke_raises_when_no_usable_source_exists(
    op13_small_flowdataset_factory,
    snapshot_flowdataset_state,
):
    """Verifica error fatal cuando no existe ninguna fuente utilizable para construir la correspondencia."""
    flows = op13_small_flowdataset_factory(
        include_flow_to_trips=False,
        include_extra_unmatched_flow=False,
        source_trips=None,
    )

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

    pd.testing.assert_frame_equal(flows.flows, flows_before["flows"])
    assert flows.flow_to_trips is flows_before["flow_to_trips"]
    assert flows.aggregation_spec == flows_before["aggregation_spec"]
    assert flows.metadata == flows_before["metadata"]
    assert flows.provenance == flows_before["provenance"]
    assert flows.source_trips is flows_before["source_trips"]


def test_get_trips_from_flows_smoke_returns_empty_result_with_warning_when_no_pairs_match(
    op13_small_tripdataset_factory,
    op13_small_flowdataset_factory,
    snapshot_flowdataset_state,
    snapshot_tripdataset_state,
):
    """Verifica resultado vacío retornable cuando la reconstrucción es interpretable pero no encuentra matches."""
    trips = op13_small_tripdataset_factory()

    flows = op13_small_flowdataset_factory(
        include_flow_to_trips=False,
        include_extra_unmatched_flow=False,
    )

    flows.flows = pd.DataFrame(
        [
            {
                "flow_id": "f_nomatch",
                "origin_h3_index": flows.flows.loc[0, "destination_h3_index"],
                "destination_h3_index": flows.flows.loc[0, "origin_h3_index"],
                "window_start_utc": pd.Timestamp("2026-01-01 12:00:00"),
                "window_end_utc": pd.Timestamp("2026-01-01 13:00:00"),
                "mode": "walk",
                "purpose": "leisure",
                "flow_count": 1,
                "flow_value": 1.0,
            }
        ]
    )

    flows_before = snapshot_flowdataset_state(flows)
    trips_before = snapshot_tripdataset_state(trips)

    correspondence_df, report = get_trips_from_flows(
        flows,
        trips=trips,
        max_issues=20,
    )

    issue_codes = [issue.code for issue in report.issues]

    assert correspondence_df.empty is True
    assert report.ok is True
    assert "GET_TRIPS_FROM_FLOWS.OUTPUT.EMPTY_RESULT" in issue_codes

    assert report.summary == {
        "n_rows_out": 0,
        "n_unique_flows_out": 0,
        "n_unique_movements_out": 0,
        "n_unmatched_flows": len(flows.flows),
        "n_unmatched_movements": trips.data["movement_id"].nunique(),
    }

    pd.testing.assert_frame_equal(flows.flows, flows_before["flows"])
    assert flows.metadata == flows_before["metadata"]

    pd.testing.assert_frame_equal(trips.data, trips_before["data"])
    assert trips.metadata == trips_before["metadata"]


def test_get_trips_from_flows_smoke_raises_when_flow_id_is_missing_from_flows_contract(
    op13_small_flowdataset_factory,
    snapshot_flowdataset_state,
):
    """Verifica fatal de preflight cuando `flows.flows` no contiene la columna canónica `flow_id`."""
    flows = op13_small_flowdataset_factory(
        include_flow_to_trips=True,
        include_extra_unmatched_flow=False,
    )
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

    pd.testing.assert_frame_equal(flows.flows, flows_before["flows"])
    assert flows.metadata == flows_before["metadata"]


def test_get_trips_from_flows_smoke_truncates_report_issues_but_keeps_tabular_output_coherent(
    op13_small_tripdataset_factory,
    op13_small_flowdataset_factory,
    assert_issue_codes,
):
    """Verifica truncamiento explícito del reporte sin degradar la salida tabular final."""
    trips = op13_small_tripdataset_factory()
    flows = op13_small_flowdataset_factory(
        include_flow_to_trips=True,
        include_extra_unmatched_flow=True,
    )

    flows.flow_to_trips = flows.flow_to_trips.loc[:, ["flow_id"]].copy()

    correspondence_df, report = get_trips_from_flows(
        flows,
        trips=trips,
        max_issues=1,
    )

    assert_issue_codes(
        report.issues,
        ["GET_TRIPS_FROM_FLOWS.REPORT.ISSUES_TRUNCATED"],
    )

    assert "limits" in report.summary

    limits = report.summary["limits"]
    assert limits["max_issues"] == 1
    assert limits["issues_truncated"] is True
    assert limits["n_issues_detected_total"] >= 2
    assert limits["n_issues_emitted"] == 1

    assert correspondence_df.columns.tolist() == [
        "flow_id",
        "movement_id",
        "trip_id",
    ]
    assert report.parameters["used_source"] == "trips_argument"