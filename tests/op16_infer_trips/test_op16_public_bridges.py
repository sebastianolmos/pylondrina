from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pandas as pd
from pandas.testing import assert_frame_equal

from pylondrina.datasets import TraceDataset, TripDataset
from pylondrina.importing_traces import ImportTraceOptions, import_traces_from_dataframe
from pylondrina.reports import ConsistencyReport, ImportReport, InferenceReport
from pylondrina.schema import TraceSchema, TripSchema
from pylondrina.transforms.inference import InferTripsOptions, infer_trips_from_traces
from pylondrina.validation_traces import TraceValidationOptions, validate_traces


def _expected_consecutive_point_trips(trace_df: pd.DataFrame) -> int:
    """Calcula viajes esperados por pares consecutivos dentro de cada usuario."""
    ordered = trace_df.sort_values(["user_id", "time_utc", "point_id"])
    return int(ordered.groupby("user_id", sort=False).size().sub(1).clip(lower=0).sum())


def _assert_trace_pipeline_events(traces: TraceDataset) -> None:
    """Verifica que el TraceDataset contiene eventos de importación y validación."""
    assert traces.metadata["is_validated"] is True

    event_ops = [event["op"] for event in traces.metadata["events"]]
    assert event_ops == ["import_traces", "validate_traces"]


def _assert_inferred_tripdataset_does_not_copy_trace_event_history(
    *,
    trip_dataset: TripDataset,
    infer_report: InferenceReport,
    traces: TraceDataset,
    assert_json_safe: Callable[[Any, str], None],
) -> None:
    """Verifica que OP-16 resume el historial previo sin copiarlo como events del output."""
    assert trip_dataset.metadata["is_validated"] is False
    assert len(trip_dataset.metadata["events"]) == 1

    event = trip_dataset.metadata["events"][0]
    assert event["op"] == "infer_trips"
    assert event["summary"] == infer_report.summary
    assert event["parameters"] == infer_report.parameters

    prior_events = traces.metadata["events"]
    assert trip_dataset.provenance["derived_from"][0]["source_type"] == "traces"
    assert trip_dataset.provenance["derived_from"][0]["dataset_id"] == traces.metadata[
        "dataset_id"
    ]
    assert trip_dataset.provenance["prior_events_summary"]["n_events"] == len(prior_events)
    assert trip_dataset.provenance["prior_events_summary"]["ops"] == [
        event["op"] for event in prior_events
    ]
    assert trip_dataset.provenance["prior_events_summary"]["last_event_op"] == (
        prior_events[-1]["op"]
    )

    assert_json_safe(infer_report.summary, "infer_report.summary")
    assert_json_safe(infer_report.parameters, "infer_report.parameters")
    assert_json_safe(event, "infer_trips event")
    assert_json_safe(trip_dataset.metadata, "trip_dataset.metadata")
    assert_json_safe(trip_dataset.provenance, "trip_dataset.provenance")


def test_bridge_import_validate_infer_points_generates_point_id_and_summarizes_prior_events(
    make_raw_points_no_pointid_df: Callable[[], pd.DataFrame],
    make_trace_schema_rich: Callable[[], TraceSchema],
    make_trip_schema_min: Callable[..., TripSchema],
    raw_field_map_no_point_id: dict[str, str],
    make_points_options: Callable[..., InferTripsOptions],
    assert_issue_present: Callable[[Any, str], None],
    assert_json_safe: Callable[[Any, str], None],
) -> None:
    """Verifica el puente OP-14 -> OP-15 -> OP-16 en modo points con point_id generado."""
    raw_df = make_raw_points_no_pointid_df()
    raw_before = raw_df.copy(deep=True)

    trace_schema = make_trace_schema_rich()
    trip_schema = make_trip_schema_min()

    traces_imported, import_report = import_traces_from_dataframe(
        raw_df,
        trace_schema,
        source_name="raw_points_no_pointid",
        options=ImportTraceOptions(
            keep_extra_fields=True,
            selected_fields=None,
            strict=False,
            source_timezone="America/Santiago",
        ),
        field_correspondence=raw_field_map_no_point_id,
        provenance={"case": "pytest_bridge_import_validate_infer_points"},
    )

    assert isinstance(traces_imported, TraceDataset)
    assert isinstance(import_report, ImportReport)
    assert import_report.ok is True
    assert import_report.summary["rows_in"] == len(raw_before)
    assert import_report.summary["rows_out"] == len(traces_imported.data)
    assert import_report.summary["point_id_generated"] is True

    assert traces_imported.metadata["is_validated"] is False
    assert traces_imported.metadata["events"][0]["op"] == "import_traces"

    assert "point_id" in traces_imported.data.columns
    assert traces_imported.data["point_id"].notna().all()
    assert traces_imported.data["point_id"].is_unique

    for field in ("location_ref", "poi_cat", "device_type", "raw_batch"):
        assert field in traces_imported.data.columns

    validate_report = validate_traces(
        traces_imported,
        options=TraceValidationOptions(strict=False),
    )

    assert isinstance(validate_report, ConsistencyReport)
    assert validate_report.summary["ok"] is True
    assert validate_report.summary["n_errors"] == 0
    _assert_trace_pipeline_events(traces_imported)

    traces_before_infer_df = traces_imported.data.copy(deep=True)
    traces_before_infer_metadata = traces_imported.metadata.copy()

    trip_dataset, infer_report = infer_trips_from_traces(
        traces_imported,
        trip_schema,
        options=make_points_options(),
        value_correspondence=None,
        provenance={"case": "pytest_bridge_import_validate_infer_points"},
    )

    expected_trips = _expected_consecutive_point_trips(traces_before_infer_df)

    assert isinstance(trip_dataset, TripDataset)
    assert isinstance(infer_report, InferenceReport)
    assert infer_report.ok is True
    assert infer_report.summary["infer_mode"] == "consecutive_points"
    assert infer_report.summary["n_points_in"] == len(traces_before_infer_df)
    assert infer_report.summary["n_trips_out"] == len(trip_dataset.data) == expected_trips

    assert "origin_h3_index" in trip_dataset.data.columns
    assert "destination_h3_index" in trip_dataset.data.columns
    assert trip_dataset.data["origin_h3_index"].notna().all()
    assert trip_dataset.data["destination_h3_index"].notna().all()

    _assert_inferred_tripdataset_does_not_copy_trace_event_history(
        trip_dataset=trip_dataset,
        infer_report=infer_report,
        traces=traces_imported,
        assert_json_safe=assert_json_safe,
    )

    assert trip_dataset.provenance["user_provenance"] == {
        "case": "pytest_bridge_import_validate_infer_points"
    }

    assert_issue_present(infer_report, "INF.H3.DERIVED")
    assert_issue_present(infer_report, "INF.OK.SUMMARY")

    assert_frame_equal(raw_df, raw_before)
    assert_frame_equal(traces_imported.data, traces_before_infer_df)
    assert traces_imported.metadata == traces_before_infer_metadata


def test_bridge_import_validate_infer_clusters_respects_selected_fields_before_propagation(
    make_raw_clusters_with_pointid_df: Callable[[], pd.DataFrame],
    make_trace_schema_rich: Callable[[], TraceSchema],
    make_trip_schema_rich_bootstrap: Callable[[], TripSchema],
    raw_field_map_with_point_id: dict[str, str],
    make_cluster_options: Callable[..., InferTripsOptions],
    assert_issue_present: Callable[[Any, str], None],
    assert_json_safe: Callable[[Any, str], None],
) -> None:
    """Verifica el puente OP-14 -> OP-15 -> OP-16 en clusters con selected_fields estrictos."""
    raw_df = make_raw_clusters_with_pointid_df()
    raw_before = raw_df.copy(deep=True)

    trace_schema = make_trace_schema_rich()
    trip_schema = make_trip_schema_rich_bootstrap()
    selected_fields = ["location_ref", "poi_cat", "accuracy", "device_type"]

    traces_imported, import_report = import_traces_from_dataframe(
        raw_df,
        trace_schema,
        source_name="raw_clusters_with_pointid",
        options=ImportTraceOptions(
            keep_extra_fields=False,
            selected_fields=selected_fields,
            strict=False,
            source_timezone="America/Santiago",
        ),
        field_correspondence=raw_field_map_with_point_id,
        provenance={"case": "pytest_bridge_import_validate_infer_clusters"},
    )

    assert isinstance(traces_imported, TraceDataset)
    assert isinstance(import_report, ImportReport)
    assert import_report.ok is True
    assert import_report.summary["rows_in"] == len(raw_before)
    assert import_report.summary["rows_out"] == len(traces_imported.data)
    assert import_report.summary["point_id_generated"] is False

    assert traces_imported.metadata["is_validated"] is False
    assert traces_imported.metadata["events"][0]["op"] == "import_traces"

    trace_core = {"point_id", "user_id", "time_utc", "latitude", "longitude"}
    assert trace_core.issubset(set(traces_imported.data.columns))
    assert set(selected_fields).issubset(set(traces_imported.data.columns))

    raw_or_unselected_fields = {
        "source_app",
        "confidence",
        "note",
        "provider",
        "raw_batch",
        "raw_quality_flag",
        "source_app_raw",
        "confidence_score",
        "note_raw",
        "provider_name",
    }
    assert raw_or_unselected_fields.isdisjoint(set(traces_imported.data.columns))

    validate_report = validate_traces(traces_imported)

    assert isinstance(validate_report, ConsistencyReport)
    assert validate_report.summary["ok"] is True
    assert validate_report.summary["n_errors"] == 0
    _assert_trace_pipeline_events(traces_imported)

    traces_before_infer_df = traces_imported.data.copy(deep=True)
    traces_before_infer_metadata = traces_imported.metadata.copy()

    propagation = {
        "location_ref": "both",
        "poi_cat": "both",
        "accuracy": "origin",
    }

    trip_dataset, infer_report = infer_trips_from_traces(
        traces_imported,
        trip_schema,
        options=make_cluster_options(
            cluster_radius_m=50.0,
            cluster_max_time_gap_s=300.0,
            propagate_trace_fields=propagation,
        ),
        value_correspondence={
            "origin_poi_cat": {},
            "destination_poi_cat": {},
        },
        provenance={"case": "pytest_bridge_import_validate_infer_clusters"},
    )

    expected_trips_from_clusters = (
        infer_report.summary["n_clusters_out"]
        - traces_before_infer_df["user_id"].nunique()
    )

    assert isinstance(trip_dataset, TripDataset)
    assert isinstance(infer_report, InferenceReport)
    assert infer_report.ok is True
    assert infer_report.summary["infer_mode"] == "consecutive_clusters"
    assert infer_report.summary["n_points_in"] == len(traces_before_infer_df)
    assert infer_report.summary["n_trips_out"] == len(trip_dataset.data)
    assert infer_report.summary["n_trips_out"] == expected_trips_from_clusters

    expected_propagated_columns = {
        "origin_location_ref",
        "destination_location_ref",
        "origin_poi_cat",
        "destination_poi_cat",
        "origin_accuracy",
    }
    assert expected_propagated_columns.issubset(set(trip_dataset.data.columns))

    not_propagated_columns = {
        "origin_source_app",
        "destination_source_app",
        "origin_confidence",
        "destination_confidence",
        "origin_note",
        "destination_note",
        "origin_provider",
        "destination_provider",
    }
    assert not_propagated_columns.isdisjoint(set(trip_dataset.data.columns))

    assert trip_dataset.metadata["mappings"]["field_propagation"] == propagation
    assert trip_dataset.metadata["h3"]["resolution"] == infer_report.parameters[
        "h3_resolution"
    ]

    _assert_inferred_tripdataset_does_not_copy_trace_event_history(
        trip_dataset=trip_dataset,
        infer_report=infer_report,
        traces=traces_imported,
        assert_json_safe=assert_json_safe,
    )

    assert trip_dataset.provenance["user_provenance"] == {
        "case": "pytest_bridge_import_validate_infer_clusters"
    }

    assert_issue_present(infer_report, "INF.CLUSTERS.MODE_APPLIED")
    assert_issue_present(infer_report, "INF.H3.DERIVED")
    assert_issue_present(infer_report, "INF.PROPAGATION.APPLIED")
    assert_issue_present(infer_report, "INF.OK.SUMMARY")

    assert_frame_equal(raw_df, raw_before)
    assert_frame_equal(traces_imported.data, traces_before_infer_df)
    assert traces_imported.metadata == traces_before_infer_metadata