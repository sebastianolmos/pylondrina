from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from typing import Any

import numpy as np
import pandas as pd
import pytest

import pylondrina.transforms.inference as inference_mod
from pylondrina.datasets import TraceDataset, TripDataset
from pylondrina.errors import InferenceError
from pylondrina.reports import InferenceReport, Issue
from pylondrina.schema import DomainSpec, FieldSpec, TraceSchema, TripSchema
from pylondrina.transforms.inference import (
    InferTripsOptions,
    _build_cluster_candidates,
    _build_inference_outputs,
    _build_issues_summary,
    _build_point_candidates,
    _build_sequential_clusters,
    _empty_candidates_frame,
    _enrich_trip_dataframe,
    _evaluate_candidates,
    _expected_propagated_columns,
    _materialize_trip_dataframe,
    _normalize_optional_number,
    _normalize_output_categorical_field,
    _normalize_propagate_trace_fields,
    _prepare_trace_workframe,
    _safe_distance_meters,
    _safe_time_gap_seconds,
    _same_place_mask,
    _summarize_prior_events,
    _time_range_summary,
    _to_json_safe,
    _resolve_infer_request,
)


def _issue_codes(issues: Sequence[Issue]) -> list[str]:
    return [issue.code for issue in issues]


def _assert_has_code(issues: Sequence[Issue], code: str) -> None:
    codes = _issue_codes(issues)
    assert code in codes, f"No se encontró {code}. Codes emitidos: {codes}"


def _request_ctx_from_df(
    df: pd.DataFrame,
    *,
    schema_version: str,
    propagation: dict[str, str] | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": schema_version,
        "available_fields": list(df.columns),
        "propagate_trace_fields": dict(propagation or {}),
        "n_points_in": len(df),
        "n_users_in": df["user_id"].nunique() if "user_id" in df.columns else 0,
    }


def test_inference_helpers_normalize_numbers_and_propagation_requests(
    make_trace_dataset: Callable[..., TraceDataset],
    make_points_options: Callable[..., InferTripsOptions],
) -> None:
    """Verifica normalización de thresholds y de propagate_trace_fields en OP-16."""
    issues: list[Issue] = []

    value = _normalize_optional_number(
        issues,
        code="INF.OPTIONS.INVALID_MIN_DISTANCE",
        option_name="min_distance_m",
        value="150",
        allow_zero=True,
    )

    assert value == 150.0
    assert issues == []

    issues = []
    with pytest.raises(InferenceError) as exc_info:
        _normalize_optional_number(
            issues,
            code="INF.OPTIONS.INVALID_MIN_DISTANCE",
            option_name="min_distance_m",
            value=-1,
            allow_zero=True,
        )

    assert exc_info.value.code == "INF.OPTIONS.INVALID_MIN_DISTANCE"
    _assert_has_code(issues, "INF.OPTIONS.INVALID_MIN_DISTANCE")

    traces = make_trace_dataset()
    issues = []

    prop_map = _normalize_propagate_trace_fields(
        issues,
        traces=traces,
        propagate_trace_fields={"poi_cat": "both", "location_ref": "origin"},
    )

    assert prop_map == {"poi_cat": "both", "location_ref": "origin"}
    assert issues == []

    issues = []
    with pytest.raises(InferenceError) as exc_info:
        _normalize_propagate_trace_fields(
            issues,
            traces=traces,
            propagate_trace_fields={
                "time_utc": "origin",
            },
        )

    assert exc_info.value.code == "INF.PROPAGATION.RESERVED_TARGET_CONFLICT"
    _assert_has_code(issues, "INF.PROPAGATION.RESERVED_TARGET_CONFLICT")

    assert make_points_options(propagate_trace_fields=prop_map).propagate_trace_fields == prop_map


def test_inference_helpers_prepare_workframe_and_compute_spatiotemporal_safe_values(
    make_trace_points_df: Callable[[], pd.DataFrame],
) -> None:
    """Verifica workframe local, same_place, distancia, gap temporal y columnas propagadas."""
    raw = make_trace_points_df()
    raw_before = raw.copy(deep=True)

    work = _prepare_trace_workframe(raw)

    assert "_row_idx" in work.columns
    assert pd.api.types.is_datetime64tz_dtype(work["time_utc"])
    assert pd.api.types.is_numeric_dtype(work["latitude"])
    assert pd.api.types.is_numeric_dtype(work["longitude"])
    pd.testing.assert_frame_equal(raw, raw_before)

    candidates_df = pd.DataFrame(
        {
            "origin_location_ref": ["A", "B", None, "X"],
            "destination_location_ref": ["A", "C", "Z", "X"],
        }
    )
    mask = _same_place_mask(candidates_df)

    assert mask.tolist() == [True, False, False, True]

    dist = _safe_distance_meters(-33.45, -70.66, -33.46, -70.67)
    assert dist is not None and dist > 0

    assert _safe_distance_meters(-33.45, -70.66, None, -70.67) is None

    t1 = pd.Timestamp("2026-01-01T08:00:00Z")
    t2 = pd.Timestamp("2026-01-01T08:10:00Z")

    assert _safe_time_gap_seconds(t1, t2) == 600.0
    assert _safe_time_gap_seconds(pd.NaT, t2) is None

    cols = _expected_propagated_columns({"poi_cat": "both", "location_ref": "origin"})

    assert cols == ["origin_poi_cat", "destination_poi_cat", "origin_location_ref"]


def test_inference_helpers_normalize_output_categorical_fields(
    assert_issue_present: Callable[[Any, str], None],
) -> None:
    """Verifica bootstrap, value mapping y degradación por cardinalidad en categóricos."""
    series = pd.Series(["home", "home", pd.NA], dtype="string")
    field_spec = FieldSpec(
        name="origin_poi_cat",
        dtype="categorical",
        required=False,
        domain=DomainSpec(values=[], extendable=True),
    )

    out_s, domain_eff, applied_map, issues, dtype_eff = _normalize_output_categorical_field(
        series,
        field_name="origin_poi_cat",
        field_spec=field_spec,
        value_mapping={"home": "residential"},
        strict_domains=False,
    )

    assert str(out_s.dtype) == "category"
    assert out_s.astype("string").iloc[:2].tolist() == ["residential", "residential"]
    assert pd.isna(out_s.astype("string").iloc[2])
    assert domain_eff["values"] == ["residential"]
    assert applied_map == {"home": "residential"}
    assert dtype_eff == "categorical"
    assert_issue_present(issues, "MAP.VALUES.APPLIED")
    assert_issue_present(issues, "DOM.INFERENCE.APPLIED")

    high_cardinality = pd.Series([f"v{i}" for i in range(20)], dtype="string")
    field_spec = FieldSpec(
        name="destination_poi_cat",
        dtype="categorical",
        required=False,
        domain=DomainSpec(values=[], extendable=True),
    )

    out_s, domain_eff, applied_map, issues, dtype_eff = _normalize_output_categorical_field(
        high_cardinality,
        field_name="destination_poi_cat",
        field_spec=field_spec,
        value_mapping={},
        strict_domains=False,
    )

    assert str(out_s.dtype) == "string"
    assert dtype_eff == "string"
    assert domain_eff.get("degraded") is True
    assert applied_map == {}
    assert_issue_present(issues, "DOM.INFERENCE.DEGRADED_TO_STRING")


def test_inference_helpers_summarize_issues_events_time_range_and_json_payloads(
    make_issue: Callable[..., Issue],
    assert_json_safe: Callable[[Any, str], None],
) -> None:
    """Verifica summaries de issues/eventos, rango temporal y conversión JSON-safe."""
    issues = [
        make_issue("info", "INF.OK.SUMMARY"),
        make_issue("warning", "DOM.INFERENCE.DEGRADED_TO_STRING"),
        make_issue("warning", "DOM.INFERENCE.DEGRADED_TO_STRING"),
    ]

    issues_summary = _build_issues_summary(issues)

    assert issues_summary["counts"] == {"info": 1, "warning": 2, "error": 0}
    assert issues_summary["counts_by_code"]["DOM.INFERENCE.DEGRADED_TO_STRING"] == 2

    prior_events = [{"op": "import_traces"}, {"op": "validate_traces"}]
    prior_summary = _summarize_prior_events(prior_events)

    assert prior_summary["n_events"] == len(prior_events)
    assert prior_summary["ops"] == ["import_traces", "validate_traces"]
    assert prior_summary["last_event_op"] == "validate_traces"

    time_range = _time_range_summary(
        pd.to_datetime(
            pd.Series(["2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"]),
            utc=True,
        )
    )

    assert time_range == {
        "start": "2026-01-01T00:00:00Z",
        "end": "2026-01-02T00:00:00Z",
    }

    payload = {
        "ts": pd.Timestamp("2026-01-01T00:00:00Z"),
        "na": pd.NA,
        "nested": [np.int64(1), pd.Timestamp("2026-01-02T00:00:00Z")],
    }

    safe_payload = _to_json_safe(payload)

    assert safe_payload["ts"] == "2026-01-01T00:00:00Z"
    assert safe_payload["na"] is None
    assert safe_payload["nested"][0] == 1
    assert_json_safe(safe_payload, "safe_payload")


def test_resolve_infer_request_builds_effective_request_and_preconditions(
    make_trace_points_df: Callable[[], pd.DataFrame],
    make_trace_dataset: Callable[..., TraceDataset],
    make_trip_schema_min: Callable[..., TripSchema],
    make_points_options: Callable[..., InferTripsOptions],
    make_cluster_options: Callable[..., InferTripsOptions],
    assert_issue_present: Callable[[Any, str], None],
) -> None:
    """Verifica request efectivo, bypass explícito y precondición fuerte de clusters."""
    trace_points_df = make_trace_points_df()
    traces_points = make_trace_dataset(
        trace_points_df,
        validated=True,
        dataset_id="trace_points_ds",
    )
    traces_unvalidated = make_trace_dataset(
        trace_points_df,
        validated=False,
        dataset_id="trace_points_ds_unvalidated",
        events=[{"op": "import_traces"}],
    )
    trip_schema_min = make_trip_schema_min(include_propagated_categoricals=True)

    issues: list[Issue] = []
    options_eff, parameters_eff, request_ctx = _resolve_infer_request(
        issues,
        traces=traces_points,
        trip_schema=trip_schema_min,
        options=make_points_options(propagate_trace_fields={"poi_cat": "both"}),
        value_correspondence={"origin_poi_cat": {"home": "residential"}},
        provenance={"notebook": "helper_tests"},
    )

    assert issues == []
    assert options_eff.infer_mode == "consecutive_points"
    assert options_eff.propagate_trace_fields == {"poi_cat": "both"}
    assert parameters_eff["value_correspondence_used"] is True
    assert parameters_eff["validation_bypass_used"] is False
    assert request_ctx["n_points_in"] == len(trace_points_df)
    assert request_ctx["n_users_in"] == trace_points_df["user_id"].nunique()
    assert request_ctx["schema_version"] == trip_schema_min.version

    issues = []
    options_eff, parameters_eff, _ = _resolve_infer_request(
        issues,
        traces=traces_unvalidated,
        trip_schema=make_trip_schema_min(),
        options=make_points_options(require_validated_traces=False),
        value_correspondence=None,
        provenance=None,
    )

    assert_issue_present(issues, "INF.PRECONDITION.VALIDATION_BYPASS_USED")
    assert parameters_eff["validation_bypass_used"] is True
    assert options_eff.require_validated_traces is False

    issues = []
    with pytest.raises(InferenceError):
        _resolve_infer_request(
            issues,
            traces=traces_points,
            trip_schema=make_trip_schema_min(),
            options=make_cluster_options(
                cluster_radius_m=None,
                cluster_max_time_gap_s=None,
            ),
            value_correspondence=None,
            provenance=None,
        )

    assert_issue_present(issues, "INF.OPTIONS.INVALID_CLUSTER_RADIUS")


def test_build_point_candidates_pairs_consecutive_points_and_propagates_frontier_fields(
    make_trace_points_df: Callable[[], pd.DataFrame],
    make_trace_dataset: Callable[..., TraceDataset],
    make_points_options: Callable[..., InferTripsOptions],
    assert_issue_present: Callable[[Any, str], None],
) -> None:
    """Verifica pairing por usuario en consecutive_points y propagación de campos frontera."""
    trace_points_df = make_trace_points_df()
    traces_points = make_trace_dataset(
        trace_points_df,
        validated=True,
        dataset_id="trace_points_ds",
    )

    options_eff = make_points_options(propagate_trace_fields={"poi_cat": "both"})
    request_ctx = _request_ctx_from_df(
        trace_points_df,
        schema_version="trip-v1",
        propagation={"poi_cat": "both"},
    )

    issues: list[Issue] = []
    candidates = _build_point_candidates(
        issues,
        traces_points.data,
        options_eff=options_eff,
        request_ctx=request_ctx,
    )

    expected_pairs = (
        trace_points_df.sort_values(["user_id", "time_utc", "point_id"])
        .groupby("user_id", sort=False)
        .size()
        .sub(1)
        .clip(lower=0)
        .sum()
    )

    assert_issue_present(issues, "INF.CANDIDATES.POINTS_MODE_APPLIED")
    assert len(candidates) == expected_pairs
    assert candidates["origin_point_id"].tolist() == ["p0", "p1"]
    assert candidates["destination_point_id"].tolist() == ["p1", "p2"]
    assert {"origin_poi_cat", "destination_poi_cat"}.issubset(candidates.columns)
    assert candidates["same_place"].tolist() == [False, True]


def test_build_clusters_and_cluster_candidates_use_sequential_frontier_points(
    make_trace_clusters_df: Callable[[], pd.DataFrame],
    make_trace_dataset: Callable[..., TraceDataset],
    make_cluster_options: Callable[..., InferTripsOptions],
    assert_issue_present: Callable[[Any, str], None],
) -> None:
    """Verifica clustering secuencial y candidatos entre puntos frontera de clusters."""
    trace_clusters_df = make_trace_clusters_df()
    traces_clusters = make_trace_dataset(
        trace_clusters_df,
        validated=True,
        dataset_id="trace_clusters_ds",
    )

    options_eff = make_cluster_options(
        cluster_radius_m=50.0,
        cluster_max_time_gap_s=300.0,
        propagate_trace_fields={"poi_cat": "both"},
    )

    clusters = _build_sequential_clusters(
        traces_clusters.data,
        options_eff=options_eff,
    )

    assert len(clusters) == 2
    assert clusters["first_point_id"].tolist() == ["p0", "p2"]
    assert clusters["last_point_id"].tolist() == ["p1", "p3"]
    assert clusters["n_points"].tolist() == [2, 2]

    request_ctx = _request_ctx_from_df(
        trace_clusters_df,
        schema_version="trip-v1",
        propagation={"poi_cat": "both"},
    )

    issues: list[Issue] = []
    cluster_candidates = _build_cluster_candidates(
        issues,
        traces_clusters.data,
        clusters,
        options_eff=options_eff,
        request_ctx=request_ctx,
    )

    assert_issue_present(issues, "INF.CLUSTERS.MODE_APPLIED")
    assert len(cluster_candidates) == max(len(clusters) - trace_clusters_df["user_id"].nunique(), 0)

    row = cluster_candidates.iloc[0]
    assert row["origin_point_id"] == clusters.iloc[0]["last_point_id"]
    assert row["destination_point_id"] == clusters.iloc[1]["first_point_id"]
    assert row["origin_poi_cat"] == trace_clusters_df.loc[1, "poi_cat"]
    assert row["destination_poi_cat"] == trace_clusters_df.loc[2, "poi_cat"]


def test_evaluate_candidates_drops_threshold_same_place_and_invalid_candidates(
    make_trace_points_df: Callable[[], pd.DataFrame],
    make_trace_dataset: Callable[..., TraceDataset],
    make_points_options: Callable[..., InferTripsOptions],
    assert_issue_present: Callable[[Any, str], None],
) -> None:
    """Verifica descartes por threshold, same_place e inválidos con drop_invalid=False."""
    trace_points_df = make_trace_points_df()
    traces_points = make_trace_dataset(trace_points_df, validated=True)

    options_eff = make_points_options(
        max_time_delta_s=1_200,
        drop_invalid=True,
    )
    request_ctx = _request_ctx_from_df(
        trace_points_df,
        schema_version="trip-v1",
    )

    candidates = _build_point_candidates(
        [],
        traces_points.data,
        options_eff=options_eff,
        request_ctx=request_ctx,
    )

    issues: list[Issue] = []
    candidates_out, eval_info = _evaluate_candidates(
        issues,
        candidates,
        options_eff=options_eff,
        request_ctx=request_ctx,
    )

    assert_issue_present(issues, "INF.CANDIDATES.DROPPED_MAX_TIME_DELTA")
    assert_issue_present(issues, "INF.CANDIDATES.DROPPED_SAME_PLACE")
    assert eval_info["n_candidates_in"] == len(candidates)
    assert eval_info["n_candidates_dropped"] == len(candidates) - len(candidates_out)
    assert eval_info["n_trips_out"] == len(candidates_out)
    assert eval_info["dropped_by_reason"]["max_time_delta_s"] >= 1
    assert eval_info["dropped_by_reason"]["same_place"] >= 1
    assert len(candidates_out) == 1

    options_eff = make_points_options(drop_invalid=False)
    candidates = _build_point_candidates(
        [],
        traces_points.data,
        options_eff=options_eff,
        request_ctx=request_ctx,
    )

    candidates_bad = candidates.copy(deep=True)
    candidates_bad.loc[candidates_bad.index[0], "destination_point_id"] = pd.NA
    candidates_bad.loc[candidates_bad.index[-1], "destination_location_ref"] = "C"
    candidates_bad["same_place"] = _same_place_mask(candidates_bad)

    issues = []
    candidates_out, eval_info = _evaluate_candidates(
        issues,
        candidates_bad,
        options_eff=options_eff,
        request_ctx=request_ctx,
    )

    assert_issue_present(issues, "INF.CANDIDATES.INVALID_RETAINED")
    assert eval_info["n_candidates_in"] == len(candidates_bad)
    assert len(candidates_out) == len(candidates_bad) - eval_info["n_candidates_dropped"]
    assert len(candidates_out) == 1


def test_materialize_trip_dataframe_builds_core_and_propagated_columns(
    make_trace_points_df: Callable[[], pd.DataFrame],
    make_trace_dataset: Callable[..., TraceDataset],
    make_points_options: Callable[..., InferTripsOptions],
    assert_issue_present: Callable[[Any, str], None],
) -> None:
    """Verifica materialización del núcleo TripDataset y columnas propagadas."""
    trace_points_df = make_trace_points_df()
    traces_points = make_trace_dataset(trace_points_df, validated=True)

    propagation = {"poi_cat": "both"}
    options_eff = make_points_options(propagate_trace_fields=propagation)
    request_ctx = _request_ctx_from_df(
        trace_points_df,
        schema_version="trip-v1",
        propagation=propagation,
    )

    candidates = _build_point_candidates(
        [],
        traces_points.data,
        options_eff=options_eff,
        request_ctx=request_ctx,
    )
    candidates_out, _ = _evaluate_candidates(
        [],
        candidates,
        options_eff=options_eff,
        request_ctx=request_ctx,
    )

    issues: list[Issue] = []
    trip_df, materialization_info = _materialize_trip_dataframe(
        issues,
        candidates_out,
        options_eff=options_eff,
        request_ctx=request_ctx,
    )

    assert_issue_present(issues, "INF.PROPAGATION.APPLIED")

    expected_columns = {
        "movement_id",
        "user_id",
        "origin_longitude",
        "origin_latitude",
        "destination_longitude",
        "destination_latitude",
        "origin_time_utc",
        "destination_time_utc",
        "trip_id",
        "movement_seq",
        "origin_poi_cat",
        "destination_poi_cat",
    }
    assert expected_columns.issubset(set(trip_df.columns))
    assert trip_df["movement_id"].notna().all()
    assert trip_df["movement_id"].is_unique
    assert (trip_df["trip_id"] == trip_df["movement_id"]).all()
    assert (trip_df["movement_seq"] == 0).all()
    assert materialization_info["created_columns"] == [
        "origin_poi_cat",
        "destination_poi_cat",
    ]

    assert _empty_candidates_frame(candidates).empty


def test_materialize_trip_dataframe_emits_soft_width_warning_when_cap_is_exceeded(
    make_trace_points_df: Callable[[], pd.DataFrame],
    make_trace_dataset: Callable[..., TraceDataset],
    make_points_options: Callable[..., InferTripsOptions],
    assert_issue_present: Callable[[Any, str], None],
) -> None:
    """Verifica el guardrail soft de ancho del output sin alterar el cap global permanentemente."""
    trace_points_df = make_trace_points_df()
    traces_points = make_trace_dataset(trace_points_df, validated=True)

    propagation = {"poi_cat": "both"}
    options_eff = make_points_options(propagate_trace_fields=propagation)
    request_ctx = _request_ctx_from_df(
        trace_points_df,
        schema_version="trip-v1",
        propagation=propagation,
    )

    candidates = _build_point_candidates(
        [],
        traces_points.data,
        options_eff=options_eff,
        request_ctx=request_ctx,
    )
    candidates_out, _ = _evaluate_candidates(
        [],
        candidates,
        options_eff=options_eff,
        request_ctx=request_ctx,
    )

    old_soft = inference_mod.TRIPDATASET_COLUMNS_SOFT_CAP
    issues: list[Issue] = []

    try:
        inference_mod.TRIPDATASET_COLUMNS_SOFT_CAP = 5
        _materialize_trip_dataframe(
            issues,
            candidates_out,
            options_eff=options_eff,
            request_ctx=request_ctx,
        )
    finally:
        inference_mod.TRIPDATASET_COLUMNS_SOFT_CAP = old_soft

    assert_issue_present(issues, "INF.OUTPUT.SOFT_WIDTH_EXCEEDED")


def test_enrich_trip_dataframe_derives_h3_and_normalizes_categorical_outputs(
    make_trace_points_df: Callable[[], pd.DataFrame],
    make_trace_dataset: Callable[..., TraceDataset],
    make_trip_schema_min: Callable[..., TripSchema],
    make_points_options: Callable[..., InferTripsOptions],
    assert_issue_present: Callable[[Any, str], None],
) -> None:
    """Verifica enriquecimiento con H3, value_correspondence y dominio categórico efectivo."""
    trace_points_df = make_trace_points_df()
    traces_points = make_trace_dataset(trace_points_df, validated=True)
    trip_schema_with_categoricals = make_trip_schema_min(
        include_propagated_categoricals=True,
    )

    propagation = {"poi_cat": "both"}
    options_eff = make_points_options(
        h3_resolution=8,
        propagate_trace_fields=propagation,
    )
    request_ctx = _request_ctx_from_df(
        trace_points_df,
        schema_version=trip_schema_with_categoricals.version,
        propagation=propagation,
    )

    candidates = _build_point_candidates(
        [],
        traces_points.data,
        options_eff=options_eff,
        request_ctx=request_ctx,
    )
    candidates_out, _ = _evaluate_candidates(
        [],
        candidates,
        options_eff=options_eff,
        request_ctx=request_ctx,
    )
    trip_df, _ = _materialize_trip_dataframe(
        [],
        candidates_out,
        options_eff=options_eff,
        request_ctx=request_ctx,
    )

    issues: list[Issue] = []
    trip_df_enriched, enrich_info = _enrich_trip_dataframe(
        issues,
        trip_df,
        trip_schema=trip_schema_with_categoricals,
        value_correspondence={
            "origin_poi_cat": {"home": "residential"},
            "destination_poi_cat": {"work": "employment"},
        },
        options_eff=options_eff,
        request_ctx=request_ctx,
    )

    assert_issue_present(issues, "INF.H3.DERIVED")
    assert_issue_present(issues, "MAP.VALUES.APPLIED")
    assert {"origin_h3_index", "destination_h3_index"}.issubset(trip_df_enriched.columns)
    assert trip_df_enriched["origin_h3_index"].notna().all()
    assert trip_df_enriched["destination_h3_index"].notna().all()
    assert trip_df_enriched["origin_poi_cat"].astype("string").tolist() == [
        "residential"
    ]
    assert trip_df_enriched["destination_poi_cat"].astype("string").tolist() == [
        "employment"
    ]
    assert enrich_info["h3_meta"]["resolution"] == options_eff.h3_resolution
    assert enrich_info["dtype_effective"]["origin_poi_cat"] == "categorical"


def test_enrich_trip_dataframe_degrades_high_cardinality_categorical_to_string(
    make_trip_schema_min: Callable[..., TripSchema],
    make_points_options: Callable[..., InferTripsOptions],
    assert_issue_present: Callable[[Any, str], None],
) -> None:
    """Verifica degradación a string cuando bootstrap categórico observa alta cardinalidad."""
    trip_schema_with_categoricals = make_trip_schema_min(
        include_propagated_categoricals=True,
    )

    n_rows = 20
    trip_df_high_card = pd.DataFrame(
        {
            "movement_id": [f"m{i}" for i in range(n_rows)],
            "user_id": ["u1"] * n_rows,
            "origin_longitude": [-70.66] * n_rows,
            "origin_latitude": [-33.45] * n_rows,
            "destination_longitude": [-70.67] * n_rows,
            "destination_latitude": [-33.46] * n_rows,
            "origin_time_utc": pd.to_datetime(
                ["2026-01-01T08:00:00Z"] * n_rows,
                utc=True,
            ),
            "destination_time_utc": pd.to_datetime(
                ["2026-01-01T08:10:00Z"] * n_rows,
                utc=True,
            ),
            "trip_id": [f"m{i}" for i in range(n_rows)],
            "movement_seq": [0] * n_rows,
            "origin_poi_cat": [f"cat_{i}" for i in range(n_rows)],
            "destination_poi_cat": ["stable"] * n_rows,
        }
    )

    options_eff = make_points_options(h3_resolution=8)
    issues: list[Issue] = []
    trip_df_enriched, enrich_info = _enrich_trip_dataframe(
        issues,
        trip_df_high_card,
        trip_schema=trip_schema_with_categoricals,
        value_correspondence=None,
        options_eff=options_eff,
        request_ctx={
            "schema_version": trip_schema_with_categoricals.version,
            "available_fields": list(trip_df_high_card.columns),
            "propagate_trace_fields": {},
            "n_points_in": 0,
            "n_users_in": 0,
        },
    )

    assert_issue_present(issues, "DOM.INFERENCE.DEGRADED_TO_STRING")
    assert str(trip_df_enriched["origin_poi_cat"].dtype) == "string"
    assert enrich_info["dtype_effective"]["origin_poi_cat"] == "string"


def test_build_inference_outputs_closes_tripdataset_report_metadata_event_and_provenance(
    make_trace_points_df: Callable[[], pd.DataFrame],
    make_trace_dataset: Callable[..., TraceDataset],
    make_trip_schema_min: Callable[..., TripSchema],
    make_points_options: Callable[..., InferTripsOptions],
    assert_issue_present: Callable[[Any, str], None],
    assert_json_safe: Callable[[Any, str], None],
) -> None:
    """Verifica cierre de OP-16: TripDataset, InferenceReport, metadata, evento y provenance."""
    trace_points_df = make_trace_points_df()
    traces_points = make_trace_dataset(
        trace_points_df,
        validated=True,
        dataset_id="trace_points_ds",
    )
    trip_schema_with_categoricals = make_trip_schema_min(
        include_propagated_categoricals=True,
    )

    options_eff, parameters_eff, request_ctx = _resolve_infer_request(
        [],
        traces=traces_points,
        trip_schema=trip_schema_with_categoricals,
        options=make_points_options(propagate_trace_fields={"poi_cat": "both"}),
        value_correspondence={
            "origin_poi_cat": {"home": "residential"},
            "destination_poi_cat": {"work": "employment"},
        },
        provenance={"notebook": "helper_tests"},
    )

    candidates = _build_point_candidates(
        [],
        traces_points.data,
        options_eff=options_eff,
        request_ctx=request_ctx,
    )
    candidates_out, eval_info = _evaluate_candidates(
        [],
        candidates,
        options_eff=options_eff,
        request_ctx=request_ctx,
    )
    trip_df, materialization_info = _materialize_trip_dataframe(
        [],
        candidates_out,
        options_eff=options_eff,
        request_ctx=request_ctx,
    )
    trip_df_enriched, enrich_info = _enrich_trip_dataframe(
        [],
        trip_df,
        trip_schema=trip_schema_with_categoricals,
        value_correspondence={
            "origin_poi_cat": {"home": "residential"},
            "destination_poi_cat": {"work": "employment"},
        },
        options_eff=options_eff,
        request_ctx=request_ctx,
    )

    issues: list[Issue] = []
    trip_dataset, report = _build_inference_outputs(
        issues,
        traces=traces_points,
        trip_df=trip_df_enriched,
        trip_schema=trip_schema_with_categoricals,
        options_eff=options_eff,
        parameters_effective=parameters_eff,
        request_ctx=request_ctx,
        eval_info=eval_info,
        materialization_info=materialization_info,
        enrich_info=enrich_info,
        value_correspondence={
            "origin_poi_cat": {"home": "residential"},
            "destination_poi_cat": {"work": "employment"},
        },
        provenance={"notebook": "helper_tests"},
        clusters_df=None,
    )

    assert isinstance(trip_dataset, TripDataset)
    assert isinstance(report, InferenceReport)
    assert report.summary["infer_mode"] == "consecutive_points"
    assert report.summary["n_points_in"] == len(trace_points_df)
    assert report.summary["n_trips_out"] == len(trip_dataset.data)

    assert trip_dataset.field_correspondence == {}
    assert trip_dataset.schema is trip_schema_with_categoricals
    assert trip_dataset.schema_effective.temporal["tier"] == "tier_1"
    assert trip_dataset.metadata["is_validated"] is False
    assert trip_dataset.metadata["events"][0]["op"] == "infer_trips"
    assert trip_dataset.metadata["events"][0]["parameters"]["infer_mode"] == (
        "consecutive_points"
    )
    assert trip_dataset.metadata["temporal"]["tier"] == "tier_1"
    assert trip_dataset.metadata["h3"]["resolution"] == options_eff.h3_resolution

    assert trip_dataset.provenance["derived_from"][0]["source_type"] == "traces"
    assert trip_dataset.provenance["derived_from"][0]["dataset_id"] == "trace_points_ds"
    assert trip_dataset.provenance["user_provenance"] == {"notebook": "helper_tests"}

    assert len(trip_dataset.metadata["events"]) == 1
    assert trip_dataset.metadata["events"][0]["summary"] == report.summary
    assert trip_dataset.metadata["events"][0]["issues_summary"] == _build_issues_summary(
        issues
    )

    assert_issue_present(issues, "INF.OK.SUMMARY")

    assert_json_safe(trip_dataset.metadata, "trip_dataset.metadata")
    assert_json_safe(trip_dataset.provenance, "trip_dataset.provenance")
    assert_json_safe(report.summary, "report.summary")
    assert_json_safe(report.parameters, "report.parameters")