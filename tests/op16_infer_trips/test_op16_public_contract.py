from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pandas as pd
from pandas.testing import assert_frame_equal

from pylondrina.datasets import TraceDataset, TripDataset
from pylondrina.reports import InferenceReport
from pylondrina.schema import TripSchema
from pylondrina.transforms.inference import InferTripsOptions, infer_trips_from_traces


def _expected_consecutive_point_trips(trace_df: pd.DataFrame) -> int:
    """Calcula cuántos viajes point-to-point deberían salir desde la traza fuente."""
    ordered = trace_df.sort_values(["user_id", "time_utc", "point_id"])
    return int(ordered.groupby("user_id", sort=False).size().sub(1).clip(lower=0).sum())


def _assert_common_inference_output_contract(
    *,
    trip_dataset: TripDataset,
    report: InferenceReport,
    traces_before_df: pd.DataFrame,
    traces_before_metadata: dict[str, Any],
    trip_min_fields: tuple[str, ...],
    assert_json_safe: Callable[[Any, str], None],
) -> None:
    """Verifica invariantes comunes de una inferencia OP-16 retornable."""
    assert isinstance(trip_dataset, TripDataset)
    assert isinstance(report, InferenceReport)

    assert report.summary["n_points_in"] == len(traces_before_df)
    assert report.summary["n_trips_out"] == len(trip_dataset.data)
    assert set(trip_min_fields).issubset(set(trip_dataset.data.columns))

    assert trip_dataset.metadata["is_validated"] is False
    assert len(trip_dataset.metadata["events"]) == 1

    event = trip_dataset.metadata["events"][0]
    assert event["op"] == "infer_trips"
    assert event["summary"] == report.summary
    assert event["parameters"] == report.parameters

    assert trip_dataset.schema_effective.temporal["tier"] == "tier_1"
    assert set(trip_min_fields).issubset(set(trip_dataset.schema_effective.fields_effective))

    assert trip_dataset.provenance["derived_from"][0]["source_type"] == "traces"
    assert trip_dataset.provenance["derived_from"][0]["dataset_id"] == traces_before_metadata[
        "dataset_id"
    ]
    assert trip_dataset.provenance["prior_events_summary"]["n_events"] == len(
        traces_before_metadata.get("events", [])
    )

    assert_json_safe(report.summary, "report.summary")
    assert_json_safe(report.parameters, "report.parameters")
    assert_json_safe(event, "infer_trips event")
    assert_json_safe(trip_dataset.metadata, "trip_dataset.metadata")


def test_infer_trips_points_rich_contract_preserves_trace_input_and_builds_effective_schema(
    trace_points_validated: TraceDataset,
    clone_tracedataset: Callable[[TraceDataset], TraceDataset],
    make_trip_schema_rich_bootstrap: Callable[[], TripSchema],
    make_points_options: Callable[..., InferTripsOptions],
    trip_min_fields: tuple[str, ...],
    assert_issue_present: Callable[[Any, str], None],
    assert_json_safe: Callable[[Any, str], None],
) -> None:
    """Verifica contrato público rico de consecutive_points con propagación, H3 y schema_effective."""
    traces = clone_tracedataset(trace_points_validated)
    traces_before_df = traces.data.copy(deep=True)
    traces_before_metadata = traces.metadata.copy()

    propagation = {
        "location_ref": "both",
        "poi_cat": "both",
        "device_type": "origin",
        "accuracy": "destination",
    }
    value_correspondence = {
        "origin_poi_cat": {"education": "study", "leisure": "wellbeing"},
        "destination_poi_cat": {"education": "study", "leisure": "wellbeing"},
    }

    trip_dataset, report = infer_trips_from_traces(
        traces,
        make_trip_schema_rich_bootstrap(),
        options=make_points_options(propagate_trace_fields=propagation),
        value_correspondence=value_correspondence,
        provenance={"case": "pytest_op16_points_rich_contract"},
    )

    _assert_common_inference_output_contract(
        trip_dataset=trip_dataset,
        report=report,
        traces_before_df=traces_before_df,
        traces_before_metadata=traces_before_metadata,
        trip_min_fields=trip_min_fields,
        assert_json_safe=assert_json_safe,
    )

    assert report.ok is True
    assert report.summary["infer_mode"] == "consecutive_points"
    assert report.summary["n_trips_out"] == _expected_consecutive_point_trips(traces_before_df)

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
    assert set(trip_dataset.metadata["mappings"]["field_propagation"]) == set(propagation)

    assert trip_dataset.provenance["user_provenance"] == {
        "case": "pytest_op16_points_rich_contract"
    }
    assert trip_dataset.provenance["prior_events_summary"]["last_event_op"] == (
        traces_before_metadata["events"][-1]["op"]
    )

    assert report.parameters["infer_mode"] == "consecutive_points"
    assert report.parameters["propagate_trace_fields"] == propagation
    assert report.parameters["value_correspondence_used"] is True
    assert report.parameters["validation_bypass_used"] is False

    assert_issue_present(report, "INF.H3.DERIVED")
    assert_issue_present(report, "INF.PROPAGATION.APPLIED")
    assert_issue_present(report, "INF.OK.SUMMARY")

    assert_frame_equal(traces.data, traces_before_df)
    assert traces.metadata == traces_before_metadata


def test_infer_trips_clusters_rich_contract_uses_cluster_frontier_points(
    trace_clusters_validated: TraceDataset,
    clone_tracedataset: Callable[[TraceDataset], TraceDataset],
    make_trip_schema_rich_bootstrap: Callable[[], TripSchema],
    make_cluster_options: Callable[..., InferTripsOptions],
    trip_min_fields: tuple[str, ...],
    assert_issue_present: Callable[[Any, str], None],
    assert_json_safe: Callable[[Any, str], None],
) -> None:
    """Verifica consecutive_clusters y la regla de frontera entre clusters consecutivos."""
    traces = clone_tracedataset(trace_clusters_validated)
    traces_before_df = traces.data.copy(deep=True)
    traces_before_metadata = traces.metadata.copy()

    propagation = {
        "location_ref": "both",
        "poi_cat": "both",
        "accuracy": "both",
    }

    trip_dataset, report = infer_trips_from_traces(
        traces,
        make_trip_schema_rich_bootstrap(),
        options=make_cluster_options(
            cluster_radius_m=50.0,
            cluster_max_time_gap_s=300.0,
            propagate_trace_fields=propagation,
        ),
        value_correspondence=None,
        provenance={"case": "pytest_op16_clusters_rich_contract"},
    )

    _assert_common_inference_output_contract(
        trip_dataset=trip_dataset,
        report=report,
        traces_before_df=traces_before_df,
        traces_before_metadata=traces_before_metadata,
        trip_min_fields=trip_min_fields,
        assert_json_safe=assert_json_safe,
    )

    assert report.ok is True
    assert report.summary["infer_mode"] == "consecutive_clusters"
    assert report.summary["n_clusters_out"] >= traces_before_df["user_id"].nunique()
    assert report.summary["n_trips_out"] == len(trip_dataset.data)

    expected_trips_from_clusters = (
        report.summary["n_clusters_out"] - traces_before_df["user_id"].nunique()
    )
    assert report.summary["n_trips_out"] == expected_trips_from_clusters

    source_u1 = (
        traces_before_df.loc[traces_before_df["user_id"] == "u1"]
        .sort_values(["time_utc", "point_id"])
        .reset_index(drop=True)
    )
    output_u1 = (
        trip_dataset.data.loc[trip_dataset.data["user_id"] == "u1"]
        .sort_values(["origin_time_utc", "destination_time_utc", "movement_id"])
        .reset_index(drop=True)
    )

    expected_origin_boundary = source_u1.iloc[1]
    expected_destination_boundary = source_u1.iloc[2]
    first_u1_trip = output_u1.iloc[0]

    assert first_u1_trip["origin_location_ref"] == expected_origin_boundary["location_ref"]
    assert first_u1_trip["destination_location_ref"] == expected_destination_boundary[
        "location_ref"
    ]
    assert pd.Timestamp(first_u1_trip["origin_time_utc"]) == pd.Timestamp(
        expected_origin_boundary["time_utc"]
    )
    assert pd.Timestamp(first_u1_trip["destination_time_utc"]) == pd.Timestamp(
        expected_destination_boundary["time_utc"]
    )

    expected_propagated_columns = {
        "origin_location_ref",
        "destination_location_ref",
        "origin_poi_cat",
        "destination_poi_cat",
        "origin_accuracy",
        "destination_accuracy",
    }
    assert expected_propagated_columns.issubset(set(trip_dataset.data.columns))

    assert_issue_present(report, "INF.CLUSTERS.MODE_APPLIED")
    assert_issue_present(report, "INF.H3.DERIVED")
    assert_issue_present(report, "INF.PROPAGATION.APPLIED")
    assert_issue_present(report, "INF.OK.SUMMARY")

    assert_frame_equal(traces.data, traces_before_df)
    assert traces.metadata == traces_before_metadata


def test_infer_trips_records_validation_bypass_when_unvalidated_traces_are_explicitly_allowed(
    trace_points_unvalidated: TraceDataset,
    clone_tracedataset: Callable[[TraceDataset], TraceDataset],
    make_trip_schema_min: Callable[..., TripSchema],
    make_points_options: Callable[..., InferTripsOptions],
    trip_min_fields: tuple[str, ...],
    assert_issue_present: Callable[[Any, str], None],
    assert_json_safe: Callable[[Any, str], None],
) -> None:
    """Verifica bypass explícito de validación previa sin lanzar excepción."""
    traces = clone_tracedataset(trace_points_unvalidated)
    traces_before_df = traces.data.copy(deep=True)
    traces_before_metadata = traces.metadata.copy()

    trip_dataset, report = infer_trips_from_traces(
        traces,
        make_trip_schema_min(),
        options=make_points_options(require_validated_traces=False),
    )

    _assert_common_inference_output_contract(
        trip_dataset=trip_dataset,
        report=report,
        traces_before_df=traces_before_df,
        traces_before_metadata=traces_before_metadata,
        trip_min_fields=trip_min_fields,
        assert_json_safe=assert_json_safe,
    )

    assert report.summary["infer_mode"] == "consecutive_points"
    assert report.summary["n_trips_out"] == _expected_consecutive_point_trips(traces_before_df)

    assert report.parameters["require_validated_traces"] is False
    assert report.parameters["validation_bypass_used"] is True
    assert trip_dataset.metadata["events"][0]["parameters"]["validation_bypass_used"] is True

    assert_issue_present(report, "INF.PRECONDITION.VALIDATION_BYPASS_USED")
    assert_issue_present(report, "INF.H3.DERIVED")
    assert_issue_present(report, "INF.OK.SUMMARY")

    assert_frame_equal(traces.data, traces_before_df)
    assert traces.metadata == traces_before_metadata


def test_infer_trips_extends_effective_domains_for_propagated_categorical_fields(
    trace_points_validated: TraceDataset,
    clone_tracedataset: Callable[[TraceDataset], TraceDataset],
    make_trip_schema_rich_extendable: Callable[[], TripSchema],
    make_points_options: Callable[..., InferTripsOptions],
    trip_min_fields: tuple[str, ...],
    assert_issue_present: Callable[[Any, str], None],
    assert_json_safe: Callable[[Any, str], None],
) -> None:
    """Verifica extensión controlada de dominios categóricos propagados al output."""
    traces = clone_tracedataset(trace_points_validated)
    traces_before_df = traces.data.copy(deep=True)
    traces_before_metadata = traces.metadata.copy()

    trip_dataset, report = infer_trips_from_traces(
        traces,
        make_trip_schema_rich_extendable(),
        options=make_points_options(
            propagate_trace_fields={"poi_cat": "both", "location_ref": "both"}
        ),
        value_correspondence={
            "origin_poi_cat": {},
            "destination_poi_cat": {},
        },
        provenance={"case": "pytest_op16_domain_extension"},
    )

    _assert_common_inference_output_contract(
        trip_dataset=trip_dataset,
        report=report,
        traces_before_df=traces_before_df,
        traces_before_metadata=traces_before_metadata,
        trip_min_fields=trip_min_fields,
        assert_json_safe=assert_json_safe,
    )

    assert report.ok is True
    assert "origin_poi_cat" in trip_dataset.data.columns
    assert "destination_poi_cat" in trip_dataset.data.columns

    origin_domain = trip_dataset.schema_effective.domains_effective["origin_poi_cat"]
    destination_domain = trip_dataset.schema_effective.domains_effective[
        "destination_poi_cat"
    ]

    observed_origin_values = set(trip_dataset.data["origin_poi_cat"].dropna().astype(str))
    observed_destination_values = set(
        trip_dataset.data["destination_poi_cat"].dropna().astype(str)
    )

    assert origin_domain["extendable"] is True
    assert destination_domain["extendable"] is True
    assert observed_origin_values.issubset(set(origin_domain["values"]))
    assert observed_destination_values.issubset(set(destination_domain["values"]))
    assert {"home", "work"}.issubset(set(origin_domain["values"]))
    assert {"home", "work"}.issubset(set(destination_domain["values"]))
    assert origin_domain["added_values"]
    assert destination_domain["added_values"]

    assert trip_dataset.schema_effective.dtype_effective["origin_poi_cat"] == "categorical"
    assert (
        trip_dataset.schema_effective.dtype_effective["destination_poi_cat"]
        == "categorical"
    )

    assert_issue_present(report, "DOM.EXTENSION.APPLIED")
    assert_issue_present(report, "INF.PROPAGATION.APPLIED")
    assert_issue_present(report, "INF.OK.SUMMARY")

    assert_frame_equal(traces.data, traces_before_df)
    assert traces.metadata == traces_before_metadata


def test_infer_trips_returns_empty_dataset_when_thresholds_drop_all_candidates(
    trace_points_validated: TraceDataset,
    clone_tracedataset: Callable[[TraceDataset], TraceDataset],
    make_trip_schema_min: Callable[..., TripSchema],
    make_points_options: Callable[..., InferTripsOptions],
    trip_min_fields: tuple[str, ...],
    assert_issue_present: Callable[[Any, str], None],
    assert_json_safe: Callable[[Any, str], None],
) -> None:
    """Verifica ruta retornable con dataset vacío cuando max_time_delta_s descarta todo."""
    traces = clone_tracedataset(trace_points_validated)
    traces_before_df = traces.data.copy(deep=True)
    traces_before_metadata = traces.metadata.copy()

    trip_dataset, report = infer_trips_from_traces(
        traces,
        make_trip_schema_min(),
        options=make_points_options(
            max_time_delta_s=60.0,
            min_distance_m=None,
        ),
        provenance={"case": "pytest_op16_zero_trips"},
    )

    _assert_common_inference_output_contract(
        trip_dataset=trip_dataset,
        report=report,
        traces_before_df=traces_before_df,
        traces_before_metadata=traces_before_metadata,
        trip_min_fields=trip_min_fields,
        assert_json_safe=assert_json_safe,
    )

    expected_candidates = _expected_consecutive_point_trips(traces_before_df)

    assert len(trip_dataset.data) == 0
    assert report.summary["infer_mode"] == "consecutive_points"
    assert report.summary["n_candidates_in"] == expected_candidates
    assert report.summary["n_candidates_dropped"] == expected_candidates
    assert report.summary["n_trips_out"] == 0
    assert report.summary["dropped_by_reason"]["max_time_delta_s"] == expected_candidates

    assert set(trip_min_fields).issubset(set(trip_dataset.data.columns))
    assert trip_dataset.metadata["events"][0]["op"] == "infer_trips"
    assert trip_dataset.metadata["is_validated"] is False
    assert trip_dataset.metadata["h3"]["resolution"] == report.parameters["h3_resolution"]

    assert_issue_present(report, "INF.CANDIDATES.DROPPED_MAX_TIME_DELTA")
    assert_issue_present(report, "INF.CANDIDATES.NO_MATERIALIZABLE_CANDIDATES")
    assert_issue_present(report, "INF.WARN.ZERO_TRIPS")
    assert_issue_present(report, "INF.OK.SUMMARY")

    assert_frame_equal(traces.data, traces_before_df)
    assert traces.metadata == traces_before_metadata