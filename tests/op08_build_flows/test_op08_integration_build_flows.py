from __future__ import annotations

import copy

import pandas as pd
import pytest

from pylondrina.datasets import FlowDataset
from pylondrina.errors import ValidationError
from pylondrina.reports import FlowBuildReport
from pylondrina.transforms.flows import FlowBuildOptions, build_flows


def test_build_flows_happy_path_on_rich_validated_tripdataset(
    tripdataset_ready_for_flows,
    clone_tripdataset,
) -> None:
    """Verifica build feliz sobre un TripDataset rico ya importado y validado."""
    trips = clone_tripdataset(tripdataset_ready_for_flows)

    data_before = trips.data.copy(deep=True)
    metadata_events_before = copy.deepcopy(trips.metadata.get("events", []))

    flow_ds, report = build_flows(
        trips,
        options=FlowBuildOptions(
            h3_resolution=6,
            group_by=["purpose"],
            min_trips_per_flow=2,
            keep_flow_to_trips=False,
            require_validated=True,
        ),
    )

    assert isinstance(flow_ds, FlowDataset)
    assert isinstance(report, FlowBuildReport)
    assert report.ok is True

    assert len(flow_ds.flows) > 0
    assert {
        "flow_id",
        "origin_h3_index",
        "destination_h3_index",
        "flow_count",
        "flow_value",
    }.issubset(flow_ds.flows.columns)
    assert "purpose" in flow_ds.flows.columns

    assert flow_ds.aggregation_spec["h3_resolution"] == 6
    assert flow_ds.aggregation_spec["group_by"] == ["purpose"]
    assert "effective_flow_keys" in flow_ds.aggregation_spec

    assert flow_ds.metadata["is_validated"] is False
    assert flow_ds.metadata["events"][-1]["op"] == "build_flows"
    assert flow_ds.metadata["events"][-1]["summary"] == report.summary
    assert flow_ds.metadata["events"][-1]["parameters"] == report.parameters

    assert "derived_from" in flow_ds.provenance
    assert isinstance(flow_ds.provenance["derived_from"], list)
    assert flow_ds.source_trips is trips

    assert report.summary["n_trips_in"] == len(trips.data)
    assert report.summary["n_flows_out"] == len(flow_ds.flows)
    assert report.summary["n_flow_to_trips_rows"] is None

    pd.testing.assert_frame_equal(
        trips.data.reset_index(drop=True),
        data_before.reset_index(drop=True),
    )
    assert trips.metadata.get("events", []) == metadata_events_before


def test_build_flows_returns_empty_flowdataset_when_threshold_removes_all_flows(
    tripdataset_validated_small,
    clone_tripdataset,
    assert_issue_present,
) -> None:
    """Verifica resultado vacío recuperable cuando el umbral mínimo descarta todos los flows."""
    trips = clone_tripdataset(tripdataset_validated_small)

    flow_ds, report = build_flows(
        trips,
        options=FlowBuildOptions(
            h3_resolution=8,
            group_by=["mode", "purpose"],
            time_aggregation="none",
            min_trips_per_flow=len(trips.data) + 10,
            keep_flow_to_trips=False,
            require_validated=True,
        ),
    )

    assert report.ok is True
    assert len(flow_ds.flows) == 0

    assert_issue_present(
        report.issues,
        "FLOW.OUTPUT.EMPTY_AFTER_THRESHOLD",
    )

    assert report.summary["n_trips_in"] == len(trips.data)
    assert report.summary["n_trips_eligible"] > 0
    assert report.summary["n_flows_out"] == 0

    assert flow_ds.metadata["events"][-1]["op"] == "build_flows"


def test_build_flows_raises_when_no_movement_is_buildable(
    tripdataset_non_buildable,
    clone_tripdataset,
) -> None:
    """Verifica error fatal cuando ningún movement conserva H3 OD utilizable."""
    trips = clone_tripdataset(tripdataset_non_buildable)

    with pytest.raises(ValidationError) as excinfo:
        build_flows(
            trips,
            options=FlowBuildOptions(
                h3_resolution=8,
                group_by=None,
                time_aggregation="none",
                min_trips_per_flow=1,
                keep_flow_to_trips=False,
                require_validated=True,
            ),
        )

    assert excinfo.value.code == "FLOW.OUTPUT.NO_BUILDABLE_MOVEMENTS"


def test_build_flows_with_temporal_segmentation_preserves_event_report_consistency_and_source_metadata(
    tripdataset_ready_for_flows,
    clone_tripdataset,
) -> None:
    """Verifica consistencia entre evento, reporte y no mutación del metadata del TripDataset fuente."""
    trips = clone_tripdataset(tripdataset_ready_for_flows)
    initial_trip_events = copy.deepcopy(trips.metadata.get("events", []))

    flow_ds, build_report = build_flows(
        trips,
        options=FlowBuildOptions(
            h3_resolution=8,
            group_by=["mode"],
            time_aggregation="week",
            time_basis="origin",
            min_trips_per_flow=2,
            keep_flow_to_trips=False,
            require_validated=True,
        ),
    )

    assert build_report.ok is True

    event = flow_ds.metadata["events"][-1]

    assert event["op"] == "build_flows"
    assert event["summary"] == build_report.summary
    assert event["parameters"] == build_report.parameters
    assert "issues_summary" in event

    assert build_report.summary["n_trips_in"] == len(trips.data)
    assert build_report.summary["n_flows_out"] == len(flow_ds.flows)

    assert trips.metadata.get("events", []) == initial_trip_events


def test_build_flows_with_flow_to_trips_keeps_backlinks_consistent_with_eligible_movements(
    tripdataset_validated_small,
    clone_tripdataset,
) -> None:
    """Verifica backlinks flow_to_trips y cobertura de movements elegibles."""
    trips = clone_tripdataset(tripdataset_validated_small)

    flow_ds, build_report = build_flows(
        trips,
        options=FlowBuildOptions(
            h3_resolution=8,
            group_by=["mode"],
            time_aggregation="none",
            min_trips_per_flow=1,
            keep_flow_to_trips=True,
            require_validated=True,
        ),
    )

    assert build_report.ok is True

    assert flow_ds.flow_to_trips is not None
    assert set(flow_ds.flow_to_trips.columns) == {
        "flow_id",
        "movement_id",
    }

    assert build_report.summary["n_flow_to_trips_rows"] == len(flow_ds.flow_to_trips)
    assert len(flow_ds.flow_to_trips) == build_report.summary["n_trips_eligible"]

    assert flow_ds.flow_to_trips["flow_id"].notna().all()
    assert flow_ds.flow_to_trips["movement_id"].notna().all()

    assert set(flow_ds.flow_to_trips["flow_id"]).issubset(
        set(flow_ds.flows["flow_id"])
    )

    # Con min_trips_per_flow=1 y fixture buildable, cada movement elegible queda representado.
    assert set(flow_ds.flow_to_trips["movement_id"]) == set(
        trips.data["movement_id"]
    )