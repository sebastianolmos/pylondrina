from __future__ import annotations

import pytest

from pylondrina.datasets import FlowDataset
from pylondrina.errors import ValidationError
from pylondrina.reports import FlowBuildReport
from pylondrina.transforms.flows import FlowBuildOptions, build_flows


def test_build_flows_happy_path_minimal_returns_flowdataset_report_and_traceability(
    make_tripdataset_for_flows,
) -> None:
    """Verifica construcción mínima de flows sin segmentación ni dimensión temporal."""
    trips = make_tripdataset_for_flows(
        validated=True,
        tier="tier_1",
    )

    flow_ds, report = build_flows(
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

    assert isinstance(flow_ds, FlowDataset)
    assert isinstance(report, FlowBuildReport)
    assert report.ok is True

    assert {
        "flow_id",
        "origin_h3_index",
        "destination_h3_index",
        "flow_count",
        "flow_value",
    }.issubset(flow_ds.flows.columns)

    assert report.summary["n_trips_in"] == len(trips.data)
    assert report.summary["n_flows_out"] == len(flow_ds.flows)

    assert flow_ds.flow_to_trips is None

    assert flow_ds.metadata["is_validated"] is False
    assert flow_ds.metadata["events"][-1]["op"] == "build_flows"

    assert "derived_from" in flow_ds.provenance
    assert isinstance(flow_ds.provenance["derived_from"], list)
    assert flow_ds.source_trips is trips


def test_build_flows_with_group_by_and_backlinks_returns_segmented_flows_and_flow_to_trips(
    make_tripdataset_for_flows,
) -> None:
    """Verifica segmentación por mode y construcción del auxiliar flow_to_trips."""
    trips = make_tripdataset_for_flows(
        validated=True,
        tier="tier_1",
    )

    flow_ds, report = build_flows(
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

    assert report.ok is True

    assert "mode" in flow_ds.flows.columns

    assert flow_ds.flow_to_trips is not None
    assert set(flow_ds.flow_to_trips.columns) == {
        "flow_id",
        "movement_id",
    }

    assert report.parameters["group_by"] == ["mode"]
    assert report.summary["n_trips_in"] == len(trips.data)
    assert report.summary["n_flows_out"] == len(flow_ds.flows)
    assert report.summary["n_flow_to_trips_rows"] == len(flow_ds.flow_to_trips)

    assert set(flow_ds.flow_to_trips["movement_id"]).issubset(
        set(trips.data["movement_id"])
    )
    assert set(flow_ds.flow_to_trips["flow_id"]).issubset(
        set(flow_ds.flows["flow_id"])
    )


def test_build_flows_with_hourly_time_aggregation_adds_consistent_flow_windows(
    make_tripdataset_for_flows,
) -> None:
    """Verifica construcción de flows con ventanas temporales horarias coherentes."""
    trips = make_tripdataset_for_flows(
        validated=True,
        tier="tier_1",
    )

    flow_ds, report = build_flows(
        trips,
        options=FlowBuildOptions(
            h3_resolution=8,
            group_by=["mode"],
            time_aggregation="hour",
            time_basis="origin",
            min_trips_per_flow=1,
            keep_flow_to_trips=False,
            require_validated=True,
        ),
    )

    assert report.ok is True

    assert "window_start_utc" in flow_ds.flows.columns
    assert "window_end_utc" in flow_ds.flows.columns

    assert report.parameters["time_aggregation"] == "hour"
    assert report.parameters["time_basis"] == "origin"
    assert report.summary["n_flows_out"] == len(flow_ds.flows)

    assert (
        flow_ds.flows["window_end_utc"]
        > flow_ds.flows["window_start_utc"]
    ).all()


def test_build_flows_raises_when_validated_trips_are_required_but_input_is_unvalidated(
    make_tripdataset_for_flows,
) -> None:
    """Verifica error fatal cuando se exige validación previa y trips no está validado."""
    trips = make_tripdataset_for_flows(
        validated=False,
        tier="tier_1",
    )

    with pytest.raises(ValidationError) as excinfo:
        build_flows(
            trips,
            options=FlowBuildOptions(
                h3_resolution=8,
                require_validated=True,
            ),
        )

    assert excinfo.value.code == "FLOW.VALIDATION.REQUIRED_NOT_VALIDATED"