from __future__ import annotations

from collections.abc import Callable

from pandas.testing import assert_frame_equal
import pytest

from pylondrina.datasets import TraceDataset
from pylondrina.errors import InferenceError
from pylondrina.schema import TripSchema
from pylondrina.transforms.inference import InferTripsOptions, infer_trips_from_traces


def test_infer_trips_rejects_unvalidated_traces_by_default(
    trace_points_unvalidated: TraceDataset,
    clone_tracedataset: Callable[[TraceDataset], TraceDataset],
    make_trip_schema_min: Callable[..., TripSchema],
    make_points_options: Callable[..., InferTripsOptions],
) -> None:
    """Verifica que OP-16 aborta si traces no están validadas y no hay bypass explícito."""
    traces = clone_tracedataset(trace_points_unvalidated)
    traces_before_df = traces.data.copy(deep=True)
    traces_before_metadata = traces.metadata.copy()

    with pytest.raises(InferenceError) as exc_info:
        infer_trips_from_traces(
            traces,
            make_trip_schema_min(),
            options=make_points_options(),
        )

    exc = exc_info.value
    assert exc.code == "INF.PRECONDITION.TRACES_NOT_VALIDATED"
    assert exc.issue is not None
    assert exc.issue.code == "INF.PRECONDITION.TRACES_NOT_VALIDATED"
    assert exc.issues is not None
    assert any(
        issue.code == "INF.PRECONDITION.TRACES_NOT_VALIDATED"
        for issue in exc.issues
    )

    assert_frame_equal(traces.data, traces_before_df)
    assert traces.metadata == traces_before_metadata


def test_infer_trips_strict_domains_rejects_non_extendable_categorical_output(
    trace_points_validated: TraceDataset,
    clone_tracedataset: Callable[[TraceDataset], TraceDataset],
    make_trip_schema_rich_blocked: Callable[[], TripSchema],
    make_points_options: Callable[..., InferTripsOptions],
) -> None:
    """Verifica que strict_domains=True aborta ante valores fuera de dominio no extendible."""
    traces = clone_tracedataset(trace_points_validated)
    traces_before_df = traces.data.copy(deep=True)
    traces_before_metadata = traces.metadata.copy()

    with pytest.raises(InferenceError) as exc_info:
        infer_trips_from_traces(
            traces,
            make_trip_schema_rich_blocked(),
            options=make_points_options(
                strict_domains=True,
                propagate_trace_fields={"poi_cat": "both"},
            ),
        )

    exc = exc_info.value
    assert exc.code == "DOM.STRICT.OUT_OF_DOMAIN_ABORT"
    assert exc.issue is not None
    assert exc.issue.code == "DOM.STRICT.OUT_OF_DOMAIN_ABORT"
    assert exc.issues is not None
    assert any(issue.code == "DOM.POLICY.FIELD_NOT_EXTENDABLE" for issue in exc.issues)
    assert any(issue.code == "DOM.STRICT.OUT_OF_DOMAIN_ABORT" for issue in exc.issues)

    assert exc.details is not None

    summary = exc.details["summary"]
    event = exc.details["event"]

    assert isinstance(summary, dict)
    assert event["op"] == "infer_trips"
    assert event["summary"] == summary
    assert event["parameters"]["strict_domains"] is True
    assert event["parameters"]["infer_mode"] == "consecutive_points"
    assert summary["infer_mode"] == "consecutive_points"
    assert summary["n_points_in"] == len(traces_before_df)
    assert summary["n_trips_out"] >= 0

    assert_frame_equal(traces.data, traces_before_df)
    assert traces.metadata == traces_before_metadata


def test_infer_trips_clusters_require_positive_cluster_radius(
    trace_clusters_validated: TraceDataset,
    clone_tracedataset: Callable[[TraceDataset], TraceDataset],
    make_trip_schema_min: Callable[..., TripSchema],
    make_cluster_options: Callable[..., InferTripsOptions],
) -> None:
    """Verifica que consecutive_clusters aborta si falta cluster_radius_m."""
    traces = clone_tracedataset(trace_clusters_validated)
    traces_before_df = traces.data.copy(deep=True)
    traces_before_metadata = traces.metadata.copy()

    with pytest.raises(InferenceError) as exc_info:
        infer_trips_from_traces(
            traces,
            make_trip_schema_min(),
            options=make_cluster_options(cluster_radius_m=None),
        )

    exc = exc_info.value
    assert exc.code == "INF.OPTIONS.INVALID_CLUSTER_RADIUS"
    assert exc.issue is not None
    assert exc.issue.code == "INF.OPTIONS.INVALID_CLUSTER_RADIUS"
    assert exc.issues is not None
    assert any(issue.code == "INF.OPTIONS.INVALID_CLUSTER_RADIUS" for issue in exc.issues)

    assert_frame_equal(traces.data, traces_before_df)
    assert traces.metadata == traces_before_metadata


def test_infer_trips_clusters_require_positive_cluster_max_time_gap(
    trace_clusters_validated: TraceDataset,
    clone_tracedataset: Callable[[TraceDataset], TraceDataset],
    make_trip_schema_min: Callable[..., TripSchema],
    make_cluster_options: Callable[..., InferTripsOptions],
) -> None:
    """Verifica que consecutive_clusters aborta si falta cluster_max_time_gap_s."""
    traces = clone_tracedataset(trace_clusters_validated)
    traces_before_df = traces.data.copy(deep=True)
    traces_before_metadata = traces.metadata.copy()

    with pytest.raises(InferenceError) as exc_info:
        infer_trips_from_traces(
            traces,
            make_trip_schema_min(),
            options=make_cluster_options(cluster_max_time_gap_s=None),
        )

    exc = exc_info.value
    assert exc.code == "INF.OPTIONS.INVALID_CLUSTER_MAX_TIME_GAP"
    assert exc.issue is not None
    assert exc.issue.code == "INF.OPTIONS.INVALID_CLUSTER_MAX_TIME_GAP"
    assert exc.issues is not None
    assert any(
        issue.code == "INF.OPTIONS.INVALID_CLUSTER_MAX_TIME_GAP"
        for issue in exc.issues
    )

    assert_frame_equal(traces.data, traces_before_df)
    assert traces.metadata == traces_before_metadata