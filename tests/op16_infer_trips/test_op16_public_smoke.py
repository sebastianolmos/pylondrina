from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pandas as pd
from pandas.testing import assert_frame_equal

from pylondrina.datasets import TraceDataset, TripDataset
from pylondrina.reports import InferenceReport
from pylondrina.schema import TripSchema
from pylondrina.transforms.inference import InferTripsOptions, infer_trips_from_traces


def test_infer_trips_from_traces_smoke_consecutive_points_happy_path(
    trace_points_validated: TraceDataset,
    clone_tracedataset: Callable[[TraceDataset], TraceDataset],
    make_trip_schema_rich_bootstrap: Callable[[], TripSchema],
    make_points_options: Callable[..., InferTripsOptions],
    trip_min_fields: tuple[str, ...],
    assert_issue_present: Callable[[Any, str], None],
    assert_json_safe: Callable[[Any, str], None],
) -> None:
    """Verifica el camino feliz mínimo de OP-16 en modo consecutive_points."""
    traces = clone_tracedataset(trace_points_validated)
    traces_before_df = traces.data.copy(deep=True)
    traces_before_metadata = traces.metadata.copy()

    trip_schema = make_trip_schema_rich_bootstrap()
    propagate_trace_fields = {
        "location_ref": "both",
        "poi_cat": "both",
        "device_type": "origin",
        "accuracy": "destination",
    }

    options = make_points_options(
        infer_mode="consecutive_points",
        propagate_trace_fields=propagate_trace_fields,
    )

    trip_dataset, report = infer_trips_from_traces(
        traces,
        trip_schema,
        options=options,
        value_correspondence={
            "origin_poi_cat": {"education": "study", "leisure": "wellbeing"},
            "destination_poi_cat": {"education": "study", "leisure": "wellbeing"},
        },
        provenance={"case": "pytest_op16_smoke_consecutive_points"},
    )

    expected_trips = (
        traces_before_df.sort_values(["user_id", "time_utc", "point_id"])
        .groupby("user_id", sort=False)
        .size()
        .sub(1)
        .clip(lower=0)
        .sum()
    )

    assert isinstance(trip_dataset, TripDataset)
    assert isinstance(report, InferenceReport)

    assert report.ok is True
    assert report.summary["infer_mode"] == "consecutive_points"
    assert report.summary["n_points_in"] == len(traces_before_df)
    assert report.summary["n_trips_out"] == len(trip_dataset.data) == expected_trips

    assert set(trip_min_fields).issubset(set(trip_dataset.data.columns))
    assert trip_dataset.data["movement_id"].notna().all()
    assert trip_dataset.data["movement_id"].is_unique
    assert (trip_dataset.data["trip_id"] == trip_dataset.data["movement_id"]).all()
    assert (trip_dataset.data["movement_seq"] == 0).all()

    assert trip_dataset.data["origin_h3_index"].notna().all()
    assert trip_dataset.data["destination_h3_index"].notna().all()
    assert trip_dataset.metadata["h3"]["resolution"] == report.parameters["h3_resolution"]

    expected_propagated_columns = {
        "origin_location_ref",
        "destination_location_ref",
        "origin_poi_cat",
        "destination_poi_cat",
        "origin_device_type",
        "destination_accuracy",
    }
    assert expected_propagated_columns.issubset(set(trip_dataset.data.columns))

    assert trip_dataset.metadata["is_validated"] is False
    assert len(trip_dataset.metadata["events"]) == 1

    event = trip_dataset.metadata["events"][0]
    assert event["op"] == "infer_trips"
    assert event["summary"] == report.summary
    assert event["parameters"] == report.parameters
    assert event["parameters"]["infer_mode"] == "consecutive_points"
    assert event["parameters"]["propagate_trace_fields"] == propagate_trace_fields
    assert event["parameters"]["validation_bypass_used"] is False
    assert event["parameters"]["value_correspondence_used"] is True

    assert trip_dataset.field_correspondence == {}
    assert trip_dataset.schema is trip_schema
    assert trip_dataset.schema_effective.temporal["tier"] == "tier_1"
    assert set(trip_min_fields).issubset(set(trip_dataset.schema_effective.fields_effective))

    assert trip_dataset.provenance["derived_from"][0]["source_type"] == "traces"
    assert trip_dataset.provenance["derived_from"][0]["dataset_id"] == traces_before_metadata["dataset_id"]
    assert trip_dataset.provenance["prior_events_summary"]["n_events"] == len(
        traces_before_metadata["events"]
    )
    assert (
        trip_dataset.provenance["prior_events_summary"]["last_event_op"]
        == traces_before_metadata["events"][-1]["op"]
    )
    assert trip_dataset.provenance["user_provenance"] == {
        "case": "pytest_op16_smoke_consecutive_points"
    }

    assert_issue_present(report, "INF.H3.DERIVED")
    assert_issue_present(report, "INF.PROPAGATION.APPLIED")
    assert_issue_present(report, "INF.OK.SUMMARY")

    assert_json_safe(report.summary, "report.summary")
    assert_json_safe(report.parameters, "report.parameters")
    assert_json_safe(event, "infer_trips event")
    assert_json_safe(trip_dataset.metadata, "trip_dataset.metadata")

    assert_frame_equal(traces.data, traces_before_df)
    assert traces.metadata == traces_before_metadata