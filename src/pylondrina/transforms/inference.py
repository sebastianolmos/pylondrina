# -------------------------
# file: pylondrina/transforms/inference.py
# -------------------------
from __future__ import annotations

import json
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Mapping, Optional, Sequence, Tuple

import h3
import numpy as np
import pandas as pd
from pandas.api import types as ptypes

from pylondrina.datasets import TraceDataset, TripDataset
from pylondrina.errors import InferenceError, SchemaError
from pylondrina.issues.catalogo_infer_trips import INFER_TRIPS_ISSUES
from pylondrina.issues.core import emit_and_maybe_raise, emit_issue
from pylondrina.reports import InferenceReport, Issue
from pylondrina.schema import DomainSpec, TripSchema, TripSchemaEffective

EXCEPTION_MAP_INFER = {
    "schema": SchemaError,
    "inference": InferenceError,
}

TRIPDATASET_COLUMNS_SOFT_CAP = 256
TRIPDATASET_COLUMNS_HARD_CAP = 1024

CATEGORICAL_INFERENCE_K_MAX = 10_000
CATEGORICAL_INFERENCE_ALPHA_DECLARED = 0.05

TRACE_MIN_FIELDS = ("point_id", "user_id", "time_utc", "latitude", "longitude")
TRIP_MIN_FIELDS = (
    "movement_id",
    "user_id",
    "origin_longitude",
    "origin_latitude",
    "destination_longitude",
    "destination_latitude",
    "origin_time_utc",
    "destination_time_utc",
    "origin_h3_index",
    "destination_h3_index",
    "trip_id",
    "movement_seq",
)

_RESERVED_OUTPUT_FIELDS = set(TRIP_MIN_FIELDS)
_ALLOWED_INFER_MODES = ("consecutive_points", "consecutive_clusters")
_ALLOWED_PROPAGATION_MODES = {"origin", "destination", "both"}


@dataclass(frozen=True)
class InferTripsOptions:
    """
    Opciones efectivas para inferir viajes a partir de trazas discretas.

    Attributes
    ----------
    infer_mode : {"consecutive_points", "consecutive_clusters"}, default="consecutive_points"
        Estrategia de inferencia a usar sobre las trazas ya ordenadas por usuario-tiempo.
    strict : bool, default=False
        Si True, errores operacionales no fatales escalan a excepción después de construir evidencia.
    strict_domains : bool, default=False
        Si True, errores de dominios/categóricos del output escalan después de construir evidencia.
    require_validated_traces : bool, default=True
        Si True, exige traces.metadata["is_validated"] == True antes de inferir.
    drop_invalid : bool, default=True
        Si True, candidatos estructuralmente inválidos se excluyen del output y se reportan.
    h3_resolution : int, default=8
        Resolución H3 usada para derivar origin_h3_index y destination_h3_index.
    max_time_delta_s : float, optional
        Umbral máximo de separación temporal permitido entre extremos del viaje.
    min_time_delta_s : float, optional
        Umbral mínimo de separación temporal permitido entre extremos del viaje.
    min_distance_m : float, optional
        Umbral mínimo de separación espacial permitido entre extremos del viaje.
    cluster_radius_m : float, optional
        Radio máximo entre puntos consecutivos para absorberlos en un mismo cluster secuencial.
    cluster_max_time_gap_s : float, optional
        Gap máximo entre puntos consecutivos para absorberlos en un mismo cluster secuencial.
    propagate_trace_fields : mapping, optional
        Mapeo campo_traza -> {'origin','destination','both'} para propagar atributos frontera.
    """

    infer_mode: Literal["consecutive_points", "consecutive_clusters"] = "consecutive_points"
    strict: bool = False
    strict_domains: bool = False
    require_validated_traces: bool = True
    drop_invalid: bool = True
    h3_resolution: int = 8
    max_time_delta_s: Optional[float] = None
    min_time_delta_s: Optional[float] = None
    min_distance_m: Optional[float] = None
    cluster_radius_m: Optional[float] = None
    cluster_max_time_gap_s: Optional[float] = None
    propagate_trace_fields: Optional[Mapping[str, Literal["origin", "destination", "both"]]] = None

def infer_trips_from_traces(
    traces: TraceDataset,
    trip_schema: TripSchema,
    *,
    options: InferTripsOptions | None = None,
    value_correspondence: Mapping[str, Mapping[Any, Any]] | None = None,
    provenance: dict[str, Any] | None = None,
) -> Tuple[TripDataset, InferenceReport]:
    """
    Infiere un TripDataset simple a partir de un TraceDataset discreto.

    La operación no muta el TraceDataset de entrada, no escribe en disco y
    construye un nuevo TripDataset con metadata/evento/provenance propios.
    """
    issues: List[Issue] = []

    # Se resuelve primero el request efectivo y se cierran las precondiciones fatales.
    options_eff, parameters_effective, request_ctx = _resolve_infer_request(
        issues,
        traces=traces,
        trip_schema=trip_schema,
        options=options,
        value_correspondence=value_correspondence,
        provenance=provenance,
    )

    # Se trabaja siempre sobre una copia local para no mutar traces.data durante el pipeline.
    work = traces.data.copy(deep=True)

    # Se bifurca el corazón algorítmico según el modo de inferencia efectivo.
    if options_eff.infer_mode == "consecutive_points":
        candidates = _build_point_candidates(
            issues,
            work,
            options_eff=options_eff,
            request_ctx=request_ctx,
        )
        clusters_df = None
    else:
        clusters_df = _build_sequential_clusters(
            work,
            options_eff=options_eff,
        )
        candidates = _build_cluster_candidates(
            issues,
            work,
            clusters_df,
            options_eff=options_eff,
            request_ctx=request_ctx,
        )

    # Se evalúan thresholds y se deja evidencia agregada antes de materializar el output.
    candidates_out, eval_info = _evaluate_candidates(
        issues,
        candidates,
        options_eff=options_eff,
        request_ctx=request_ctx,
    )

    # Se materializa recién ahora la tabla canónica base de trips.
    trip_df, materialization_info = _materialize_trip_dataframe(
        issues,
        candidates_out,
        options_eff=options_eff,
        request_ctx=request_ctx,
    )

    # Se completa el contrato tabular del output con H3 y normalización categórica.
    trip_df, enrich_info = _enrich_trip_dataframe(
        issues,
        trip_df,
        trip_schema=trip_schema,
        value_correspondence=value_correspondence,
        options_eff=options_eff,
        request_ctx=request_ctx,
    )

    # Se cierra el dataset derivado, el reporte y el evento final de la operación.
    trip_dataset, report = _build_inference_outputs(
        issues,
        traces=traces,
        trip_df=trip_df,
        trip_schema=trip_schema,
        options_eff=options_eff,
        parameters_effective=parameters_effective,
        request_ctx=request_ctx,
        eval_info=eval_info,
        materialization_info=materialization_info,
        enrich_info=enrich_info,
        value_correspondence=value_correspondence,
        provenance=provenance,
        clusters_df=clusters_df,
    )

    # Si la política efectiva lo exige, se escala recién después de construir evidencia completa.
    _raise_if_inference_must_abort(
        report,
        trip_dataset,
        options_eff=options_eff,
    )

    return trip_dataset, report


# -----------------------------------------------------------------------------
# Helpers internos principales del pipeline de inferencia
# -----------------------------------------------------------------------------


def _resolve_infer_request(
    issues: List[Issue],
    *,
    traces: TraceDataset,
    trip_schema: TripSchema,
    options: InferTripsOptions | None,
    value_correspondence: Mapping[str, Mapping[Any, Any]] | None,
    provenance: dict[str, Any] | None,
) -> tuple[InferTripsOptions, Dict[str, Any], Dict[str, Any]]:
    """
    Normaliza options, valida precondiciones y resuelve el request efectivo.

    Emite
    -----
    - INF.INPUT.INVALID_TRACES_OBJECT
    - INF.INPUT.MISSING_DATAFRAME
    - INF.INPUT.EMPTY_DATAFRAME
    - INF.INPUT.MISSING_MIN_TRACE_FIELDS
    - INF.PRECONDITION.TRACES_NOT_VALIDATED
    - INF.PRECONDITION.VALIDATION_BYPASS_USED
    - INF.OPTIONS.UNKNOWN_INFER_MODE
    - INF.OPTIONS.INVALID_H3_RESOLUTION
    - INF.OPTIONS.INVALID_MAX_TIME_DELTA
    - INF.OPTIONS.INVALID_MIN_TIME_DELTA
    - INF.OPTIONS.INVALID_MIN_DISTANCE
    - INF.OPTIONS.INCONSISTENT_TIME_THRESHOLDS
    - INF.OPTIONS.INVALID_CLUSTER_RADIUS
    - INF.OPTIONS.INVALID_CLUSTER_MAX_TIME_GAP
    - INF.OPTIONS.INVALID_PROPAGATE_TRACE_FIELDS
    - SCH.TRIP_SCHEMA.INVALID_VERSION
    - SCH.TRIP_SCHEMA.EMPTY_FIELDS
    - SCH.TRIP_SCHEMA.MISSING_MIN_OUTPUT_FIELDS
    - MAP.VALUES.UNKNOWN_CANONICAL_FIELD
    - PROV.INPUT.INVALID_USER_PROVENANCE
    - INF.PROPAGATION.UNKNOWN_TRACE_FIELD
    - INF.PROPAGATION.RESERVED_TARGET_CONFLICT
    """
    options_raw = options or InferTripsOptions()

    if not isinstance(traces, TraceDataset):
        # Se emite fatal porque el contrato de entrada exige TraceDataset utilizable.
        emit_and_maybe_raise(
            issues,
            INFER_TRIPS_ISSUES,
            "INF.INPUT.INVALID_TRACES_OBJECT",
            strict=False,
            exception_map=EXCEPTION_MAP_INFER,
            default_exception=InferenceError,
            received_type=type(traces).__name__,
        )

    if not hasattr(traces, "data") or not isinstance(traces.data, pd.DataFrame):
        # Se emite fatal porque toda la inferencia opera sobre traces.data como superficie viva.
        emit_and_maybe_raise(
            issues,
            INFER_TRIPS_ISSUES,
            "INF.INPUT.MISSING_DATAFRAME",
            strict=False,
            exception_map=EXCEPTION_MAP_INFER,
            default_exception=InferenceError,
            attribute="data",
            reason="missing_or_not_dataframe",
        )

    infer_mode = getattr(options_raw, "infer_mode", "consecutive_points")
    if infer_mode not in _ALLOWED_INFER_MODES:
        # Se emite fatal porque el contrato vigente solo cierra dos modos de inferencia.
        emit_and_maybe_raise(
            issues,
            INFER_TRIPS_ISSUES,
            "INF.OPTIONS.UNKNOWN_INFER_MODE",
            strict=False,
            exception_map=EXCEPTION_MAP_INFER,
            default_exception=InferenceError,
            value=infer_mode,
        )

    h3_resolution = getattr(options_raw, "h3_resolution", 8)
    if not isinstance(h3_resolution, int) or not 0 <= h3_resolution <= 15:
        # Se emite fatal porque el output mínimo exige H3 OD derivable con resolución interpretable.
        emit_and_maybe_raise(
            issues,
            INFER_TRIPS_ISSUES,
            "INF.OPTIONS.INVALID_H3_RESOLUTION",
            strict=False,
            exception_map=EXCEPTION_MAP_INFER,
            default_exception=InferenceError,
            value=h3_resolution,
        )

    max_time_delta_s = _normalize_optional_number(
        issues,
        code="INF.OPTIONS.INVALID_MAX_TIME_DELTA",
        option_name="max_time_delta_s",
        value=getattr(options_raw, "max_time_delta_s", None),
        allow_zero=False,
    )
    min_time_delta_s = _normalize_optional_number(
        issues,
        code="INF.OPTIONS.INVALID_MIN_TIME_DELTA",
        option_name="min_time_delta_s",
        value=getattr(options_raw, "min_time_delta_s", None),
        allow_zero=True,
    )
    min_distance_m = _normalize_optional_number(
        issues,
        code="INF.OPTIONS.INVALID_MIN_DISTANCE",
        option_name="min_distance_m",
        value=getattr(options_raw, "min_distance_m", None),
        allow_zero=True,
    )

    if (
        min_time_delta_s is not None
        and max_time_delta_s is not None
        and min_time_delta_s > max_time_delta_s
    ):
        # Se emite fatal porque los umbrales temporales no pueden quedar invertidos.
        emit_and_maybe_raise(
            issues,
            INFER_TRIPS_ISSUES,
            "INF.OPTIONS.INCONSISTENT_TIME_THRESHOLDS",
            strict=False,
            exception_map=EXCEPTION_MAP_INFER,
            default_exception=InferenceError,
            min_value=min_time_delta_s,
            max_value=max_time_delta_s,
        )

    cluster_radius_m = getattr(options_raw, "cluster_radius_m", None)
    cluster_max_time_gap_s = getattr(options_raw, "cluster_max_time_gap_s", None)
    if infer_mode == "consecutive_clusters":
        cluster_radius_m = _normalize_optional_number(
            issues,
            code="INF.OPTIONS.INVALID_CLUSTER_RADIUS",
            option_name="cluster_radius_m",
            value=cluster_radius_m,
            allow_zero=False,
        )
        cluster_max_time_gap_s = _normalize_optional_number(
            issues,
            code="INF.OPTIONS.INVALID_CLUSTER_MAX_TIME_GAP",
            option_name="cluster_max_time_gap_s",
            value=cluster_max_time_gap_s,
            allow_zero=False,
        )

        if cluster_radius_m is None:
            # En modo clusters este parámetro no es opcional; su ausencia es inconsistencia fatal.
            emit_and_maybe_raise(
                issues,
                INFER_TRIPS_ISSUES,
                "INF.OPTIONS.INVALID_CLUSTER_RADIUS",
                strict=False,
                exception_map=EXCEPTION_MAP_INFER,
                default_exception=InferenceError,
                option="cluster_radius_m",
                value=cluster_radius_m,
                expected="positive number",
                action="abort",
            )

        if cluster_max_time_gap_s is None:
            # En modo clusters este parámetro no es opcional; su ausencia es inconsistencia fatal.
            emit_and_maybe_raise(
                issues,
                INFER_TRIPS_ISSUES,
                "INF.OPTIONS.INVALID_CLUSTER_MAX_TIME_GAP",
                strict=False,
                exception_map=EXCEPTION_MAP_INFER,
                default_exception=InferenceError,
                option="cluster_max_time_gap_s",
                value=cluster_max_time_gap_s,
                expected="positive number",
                action="abort",
            )
    else:
        cluster_radius_m = None
        cluster_max_time_gap_s = None

    propagate_trace_fields = _normalize_propagate_trace_fields(
        issues,
        traces=traces,
        propagate_trace_fields=getattr(options_raw, "propagate_trace_fields", None),
    )

    if not isinstance(trip_schema, TripSchema):
        # Se emite fatal porque OP-16 necesita un TripSchema usable para cerrar el output.
        emit_and_maybe_raise(
            issues,
            INFER_TRIPS_ISSUES,
            "SCH.TRIP_SCHEMA.EMPTY_FIELDS",
            strict=False,
            exception_map=EXCEPTION_MAP_INFER,
            default_exception=SchemaError,
            schema_version=getattr(trip_schema, "version", None),
            fields_size=0,
        )

    schema_version = getattr(trip_schema, "version", None)
    if not isinstance(schema_version, str) or schema_version.strip() == "":
        # Se emite fatal porque el schema de salida debe dejar versión trazable.
        emit_and_maybe_raise(
            issues,
            INFER_TRIPS_ISSUES,
            "SCH.TRIP_SCHEMA.INVALID_VERSION",
            strict=False,
            exception_map=EXCEPTION_MAP_INFER,
            default_exception=SchemaError,
            schema_version=schema_version,
            schema_name=type(trip_schema).__name__,
        )

    schema_fields = getattr(trip_schema, "fields", None)
    if not isinstance(schema_fields, dict) or len(schema_fields) == 0:
        # Se emite fatal porque el TripSchema no puede estar vacío para materializar el output.
        emit_and_maybe_raise(
            issues,
            INFER_TRIPS_ISSUES,
            "SCH.TRIP_SCHEMA.EMPTY_FIELDS",
            strict=False,
            exception_map=EXCEPTION_MAP_INFER,
            default_exception=SchemaError,
            schema_version=schema_version,
            fields_size=0 if not isinstance(schema_fields, dict) else len(schema_fields),
        )

    missing_output_fields = [field for field in TRIP_MIN_FIELDS if field not in schema_fields]
    if missing_output_fields:
        # Se emite fatal porque el output inferido debe cubrir el núcleo canónico mínimo de trips.
        emit_and_maybe_raise(
            issues,
            INFER_TRIPS_ISSUES,
            "SCH.TRIP_SCHEMA.MISSING_MIN_OUTPUT_FIELDS",
            strict=False,
            exception_map=EXCEPTION_MAP_INFER,
            default_exception=SchemaError,
            missing_fields=missing_output_fields,
            schema_fields_sample=_sample_list(schema_fields.keys(), 20),
            schema_fields_total=len(schema_fields),
        )

    for field_name in (value_correspondence or {}).keys():
        if field_name not in schema_fields:
            # Se emite fatal porque value_correspondence solo puede apuntar a campos del TripSchema.
            emit_and_maybe_raise(
                issues,
                INFER_TRIPS_ISSUES,
                "MAP.VALUES.UNKNOWN_CANONICAL_FIELD",
                strict=False,
                exception_map=EXCEPTION_MAP_INFER,
                default_exception=InferenceError,
                field=field_name,
                schema_fields_sample=_sample_list(schema_fields.keys(), 20),
                schema_fields_total=len(schema_fields),
            )

    if provenance is not None and (not isinstance(provenance, dict) or not _json_is_serializable(provenance)):
        # Se emite fatal porque el provenance adicional debe ser serializable para vivir en el dataset derivado.
        emit_and_maybe_raise(
            issues,
            INFER_TRIPS_ISSUES,
            "PROV.INPUT.INVALID_USER_PROVENANCE",
            strict=False,
            exception_map=EXCEPTION_MAP_INFER,
            default_exception=InferenceError,
            field="provenance",
            reason="not_json_serializable",
            expected="JSON-safe dict or degradable mapping",
        )

    available_fields = list(traces.data.columns)
    missing_min_fields = [field for field in TRACE_MIN_FIELDS if field not in traces.data.columns]
    if missing_min_fields:
        # Se emite fatal porque el mínimo canónico de traces es precondición técnica directa de OP-16.
        emit_and_maybe_raise(
            issues,
            INFER_TRIPS_ISSUES,
            "INF.INPUT.MISSING_MIN_TRACE_FIELDS",
            strict=False,
            exception_map=EXCEPTION_MAP_INFER,
            default_exception=InferenceError,
            missing_fields=missing_min_fields,
            available_fields_sample=_sample_list(available_fields, 20),
            available_fields_total=len(available_fields),
        )

    if len(traces.data) == 0:
        # Se emite warning porque el contrato permite devolver un resultado vacío sobre input vacío.
        emit_issue(
            issues,
            INFER_TRIPS_ISSUES,
            "INF.INPUT.EMPTY_DATAFRAME",
            infer_mode=infer_mode,
            strict=bool(getattr(options_raw, "strict", False)),
            strict_domains=bool(getattr(options_raw, "strict_domains", False)),
            drop_invalid=bool(getattr(options_raw, "drop_invalid", True)),
            require_validated_traces=bool(getattr(options_raw, "require_validated_traces", True)),
            h3_resolution=h3_resolution,
            max_time_delta_s=max_time_delta_s,
            min_time_delta_s=min_time_delta_s,
            min_distance_m=min_distance_m,
            cluster_radius_m=cluster_radius_m,
            cluster_max_time_gap_s=cluster_max_time_gap_s,
            n_points_in=0,
        )

    is_validated_flag = False
    if isinstance(getattr(traces, "metadata", None), dict):
        is_validated_flag = bool(traces.metadata.get("is_validated", False))
    require_validated_traces = bool(getattr(options_raw, "require_validated_traces", True))
    if require_validated_traces and not is_validated_flag:
        # Se emite fatal porque la inferencia exige trazas validadas por defecto.
        emit_and_maybe_raise(
            issues,
            INFER_TRIPS_ISSUES,
            "INF.PRECONDITION.TRACES_NOT_VALIDATED",
            strict=False,
            exception_map=EXCEPTION_MAP_INFER,
            default_exception=InferenceError,
            flag_value=is_validated_flag,
            details={
                **{
                    "infer_mode": infer_mode,
                    "strict": bool(getattr(options_raw, "strict", False)),
                    "strict_domains": bool(getattr(options_raw, "strict_domains", False)),
                    "drop_invalid": bool(getattr(options_raw, "drop_invalid", True)),
                    "require_validated_traces": require_validated_traces,
                    "h3_resolution": h3_resolution,
                    "max_time_delta_s": max_time_delta_s,
                    "min_time_delta_s": min_time_delta_s,
                    "min_distance_m": min_distance_m,
                    "cluster_radius_m": cluster_radius_m,
                    "cluster_max_time_gap_s": cluster_max_time_gap_s,
                },
                "flag_field": "is_validated",
                "flag_value": is_validated_flag,
                "expected": True,
                "action": "abort",
            },
        )
    if not require_validated_traces:
        # Se emite warning para dejar trazado el bypass explícito de validación de traces.
        emit_issue(
            issues,
            INFER_TRIPS_ISSUES,
            "INF.PRECONDITION.VALIDATION_BYPASS_USED",
            infer_mode=infer_mode,
            strict=bool(getattr(options_raw, "strict", False)),
            strict_domains=bool(getattr(options_raw, "strict_domains", False)),
            drop_invalid=bool(getattr(options_raw, "drop_invalid", True)),
            require_validated_traces=require_validated_traces,
            h3_resolution=h3_resolution,
            max_time_delta_s=max_time_delta_s,
            min_time_delta_s=min_time_delta_s,
            min_distance_m=min_distance_m,
            cluster_radius_m=cluster_radius_m,
            cluster_max_time_gap_s=cluster_max_time_gap_s,
            flag_value=is_validated_flag,
        )

    options_eff = InferTripsOptions(
        infer_mode=infer_mode,
        strict=bool(getattr(options_raw, "strict", False)),
        strict_domains=bool(getattr(options_raw, "strict_domains", False)),
        require_validated_traces=require_validated_traces,
        drop_invalid=bool(getattr(options_raw, "drop_invalid", True)),
        h3_resolution=int(h3_resolution),
        max_time_delta_s=max_time_delta_s,
        min_time_delta_s=min_time_delta_s,
        min_distance_m=min_distance_m,
        cluster_radius_m=cluster_radius_m,
        cluster_max_time_gap_s=cluster_max_time_gap_s,
        propagate_trace_fields=propagate_trace_fields,
    )

    parameters_effective = {
        "infer_mode": options_eff.infer_mode,
        "strict": options_eff.strict,
        "strict_domains": options_eff.strict_domains,
        "require_validated_traces": options_eff.require_validated_traces,
        "drop_invalid": options_eff.drop_invalid,
        "h3_resolution": options_eff.h3_resolution,
        "max_time_delta_s": options_eff.max_time_delta_s,
        "min_time_delta_s": options_eff.min_time_delta_s,
        "min_distance_m": options_eff.min_distance_m,
        "cluster_radius_m": options_eff.cluster_radius_m,
        "cluster_max_time_gap_s": options_eff.cluster_max_time_gap_s,
        "propagate_trace_fields": dict(options_eff.propagate_trace_fields or {}),
        "value_correspondence_used": bool(value_correspondence),
        "validation_bypass_used": not options_eff.require_validated_traces,
    }

    n_points_in = int(len(traces.data))
    n_users_in = int(traces.data["user_id"].nunique(dropna=True)) if "user_id" in traces.data.columns else 0
    request_ctx = {
        "schema_version": schema_version,
        "available_fields": available_fields,
        "propagate_trace_fields": dict(options_eff.propagate_trace_fields or {}),
        "n_points_in": n_points_in,
        "n_users_in": n_users_in,
    }
    return options_eff, parameters_effective, request_ctx


def _build_point_candidates(
    issues: List[Issue],
    traces_df: pd.DataFrame,
    *,
    options_eff: InferTripsOptions,
    request_ctx: Dict[str, Any],
) -> pd.DataFrame:
    """
    Construye candidatos OD entre puntos consecutivos del mismo usuario.

    Emite
    -----
    - INF.CANDIDATES.POINTS_MODE_APPLIED
    - INF.CANDIDATES.NO_CANDIDATES_BUILT
    """
    # Se prepara una vista ordenada y tipada mínima sin tocar el dataset de entrada.
    work = _prepare_trace_workframe(traces_df)
    work = work.sort_values(["user_id", "time_utc", "point_id"], kind="mergesort").reset_index(drop=True)

    # Se desplazan columnas dentro de cada usuario para formar pares consecutivos canónicos.
    group = work.groupby("user_id", sort=False, dropna=False)
    next_point_id = group["point_id"].shift(-1)
    next_time = group["time_utc"].shift(-1)
    next_lat = group["latitude"].shift(-1)
    next_lon = group["longitude"].shift(-1)
    next_loc_ref = group["location_ref"].shift(-1) if "location_ref" in work.columns else pd.Series(index=work.index, dtype="object")

    candidates = pd.DataFrame(
        {
            "user_id": work["user_id"],
            "origin_point_id": work["point_id"],
            "destination_point_id": next_point_id,
            "origin_time_utc": work["time_utc"],
            "destination_time_utc": next_time,
            "origin_latitude": work["latitude"],
            "origin_longitude": work["longitude"],
            "destination_latitude": next_lat,
            "destination_longitude": next_lon,
            "origin_row_idx": work["_row_idx"],
            "destination_row_idx": group["_row_idx"].shift(-1),
            "origin_location_ref": work["location_ref"] if "location_ref" in work.columns else None,
            "destination_location_ref": next_loc_ref if "location_ref" in work.columns else None,
        }
    )

    candidates = candidates[candidates["destination_point_id"].notna()].reset_index(drop=True)
    if len(candidates) == 0:
        # Se emite info/warning porque puede no haber pares consecutivos sin que el request sea inválido.
        emit_issue(
            issues,
            INFER_TRIPS_ISSUES,
            "INF.CANDIDATES.NO_CANDIDATES_BUILT",
            **_request_details(options_eff, request_ctx),
            reason="no_consecutive_pairs",
        )
        return candidates

    # Se calculan métricas mínimas por candidato antes del filtrado operacional.
    candidates["delta_t_s"] = (
        candidates["destination_time_utc"] - candidates["origin_time_utc"]
    ).dt.total_seconds()
    candidates["distance_m"] = _haversine_meters(
        candidates["origin_latitude"].to_numpy(),
        candidates["origin_longitude"].to_numpy(),
        candidates["destination_latitude"].to_numpy(),
        candidates["destination_longitude"].to_numpy(),
    )
    candidates["same_place"] = _same_place_mask(candidates)

    # Se propagan atributos extra frontera solo si el usuario los pidió explícitamente.
    propagation_map = dict(options_eff.propagate_trace_fields or {})
    for field_name, mode in propagation_map.items():
        if mode in {"origin", "both"}:
            candidates[f"origin_{field_name}"] = work[field_name].values[candidates["origin_row_idx"].astype(int).to_numpy()]
        if mode in {"destination", "both"}:
            candidates[f"destination_{field_name}"] = work[field_name].values[candidates["destination_row_idx"].astype(int).to_numpy()]

    # Se emite info para dejar trazado el volumen de candidatos generado por el modo simple.
    emit_issue(
        issues,
        INFER_TRIPS_ISSUES,
        "INF.CANDIDATES.POINTS_MODE_APPLIED",
        **_request_details(options_eff, request_ctx),
        n_candidates_in=int(len(candidates)),
    )
    return candidates


def _build_sequential_clusters(
    traces_df: pd.DataFrame,
    *,
    options_eff: InferTripsOptions,
) -> pd.DataFrame:
    """
    Construye clusters secuenciales por usuario bajo proximidad temporal y espacial.

    Emite
    -----
    No emite issues directamente; deja esa evidencia al helper que consume los clusters.
    """
    # Se ordena una copia de trabajo y se agrupa secuencialmente sin centroides ni puntos artificiales.
    work = _prepare_trace_workframe(traces_df)
    work = work.sort_values(["user_id", "time_utc", "point_id"], kind="mergesort").reset_index(drop=True)

    cluster_rows: List[Dict[str, Any]] = []
    cluster_id = 0

    for user_id, user_df in work.groupby("user_id", sort=False, dropna=False):
        if len(user_df) == 0:
            continue

        indices = user_df.index.to_list()
        current_start = indices[0]
        current_last = indices[0]
        current_n_points = 1

        for idx in indices[1:]:
            prev = work.loc[current_last]
            curr = work.loc[idx]
            gap_s = _safe_time_gap_seconds(prev["time_utc"], curr["time_utc"])
            dist_m = _safe_distance_meters(prev["latitude"], prev["longitude"], curr["latitude"], curr["longitude"])
            same_cluster = (
                gap_s is not None
                and dist_m is not None
                and gap_s <= float(options_eff.cluster_max_time_gap_s)
                and dist_m <= float(options_eff.cluster_radius_m)
            )

            if same_cluster:
                current_last = idx
                current_n_points += 1
                continue

            cluster_rows.append(
                _cluster_record(
                    cluster_id=cluster_id,
                    user_id=user_id,
                    work=work,
                    start_idx=current_start,
                    last_idx=current_last,
                    n_points=current_n_points,
                )
            )
            cluster_id += 1
            current_start = idx
            current_last = idx
            current_n_points = 1

        cluster_rows.append(
            _cluster_record(
                cluster_id=cluster_id,
                user_id=user_id,
                work=work,
                start_idx=current_start,
                last_idx=current_last,
                n_points=current_n_points,
            )
        )
        cluster_id += 1

    return pd.DataFrame(cluster_rows)


def _build_cluster_candidates(
    issues: List[Issue],
    traces_df: pd.DataFrame,
    clusters_df: pd.DataFrame,
    *,
    options_eff: InferTripsOptions,
    request_ctx: Dict[str, Any],
) -> pd.DataFrame:
    """
    Construye candidatos OD entre clusters consecutivos usando puntos frontera.

    Emite
    -----
    - INF.CLUSTERS.MODE_APPLIED
    - INF.CANDIDATES.NO_CANDIDATES_BUILT
    """
    work = _prepare_trace_workframe(traces_df)
    work = work.sort_values(["user_id", "time_utc", "point_id"], kind="mergesort").reset_index(drop=True)
    if clusters_df.empty:
        # Se emite warning porque no hubo clusters utilizables para construir viajes.
        emit_issue(
            issues,
            INFER_TRIPS_ISSUES,
            "INF.CANDIDATES.NO_CANDIDATES_BUILT",
            **_request_details(options_eff, request_ctx),
            reason="no_clusters_built",
        )
        return pd.DataFrame()

    clusters_sorted = clusters_df.sort_values(["user_id", "cluster_start_utc", "cluster_id"], kind="mergesort").reset_index(drop=True)
    next_cluster_id = clusters_sorted.groupby("user_id", dropna=False)["cluster_id"].shift(-1)
    next_first_idx = clusters_sorted.groupby("user_id", dropna=False)["first_row_idx"].shift(-1)
    next_first_point_id = clusters_sorted.groupby("user_id", dropna=False)["first_point_id"].shift(-1)
    next_cluster_start = clusters_sorted.groupby("user_id", dropna=False)["cluster_start_utc"].shift(-1)

    candidates = pd.DataFrame(
        {
            "user_id": clusters_sorted["user_id"],
            "origin_cluster_id": clusters_sorted["cluster_id"],
            "destination_cluster_id": next_cluster_id,
            "origin_point_id": clusters_sorted["last_point_id"],
            "destination_point_id": next_first_point_id,
            "origin_row_idx": clusters_sorted["last_row_idx"],
            "destination_row_idx": next_first_idx,
            "origin_time_utc": clusters_sorted["cluster_end_utc"],
            "destination_time_utc": next_cluster_start,
        }
    )
    candidates = candidates[candidates["destination_cluster_id"].notna()].reset_index(drop=True)
    if len(candidates) == 0:
        # Se emite warning porque puede no haber clusters consecutivos suficientes por usuario.
        emit_issue(
            issues,
            INFER_TRIPS_ISSUES,
            "INF.CANDIDATES.NO_CANDIDATES_BUILT",
            **_request_details(options_eff, request_ctx),
            reason="no_consecutive_clusters",
        )
        emit_issue(
            issues,
            INFER_TRIPS_ISSUES,
            "INF.CLUSTERS.MODE_APPLIED",
            **_request_details(options_eff, request_ctx),
            n_clusters_out=int(len(clusters_sorted)),
            n_candidates_in=0,
            clusters_sample=_sample_records(clusters_sorted, limit=5),
        )
        return candidates

    origin_idx = candidates["origin_row_idx"].astype(int).to_numpy()
    destination_idx = candidates["destination_row_idx"].astype(int).to_numpy()
    candidates["origin_latitude"] = work["latitude"].values[origin_idx]
    candidates["origin_longitude"] = work["longitude"].values[origin_idx]
    candidates["destination_latitude"] = work["latitude"].values[destination_idx]
    candidates["destination_longitude"] = work["longitude"].values[destination_idx]
    candidates["delta_t_s"] = (
        candidates["destination_time_utc"] - candidates["origin_time_utc"]
    ).dt.total_seconds()
    candidates["distance_m"] = _haversine_meters(
        candidates["origin_latitude"].to_numpy(),
        candidates["origin_longitude"].to_numpy(),
        candidates["destination_latitude"].to_numpy(),
        candidates["destination_longitude"].to_numpy(),
    )
    if "location_ref" in work.columns:
        candidates["origin_location_ref"] = work["location_ref"].values[origin_idx]
        candidates["destination_location_ref"] = work["location_ref"].values[destination_idx]
    candidates["same_place"] = _same_place_mask(candidates)

    propagation_map = dict(options_eff.propagate_trace_fields or {})
    for field_name, mode in propagation_map.items():
        if mode in {"origin", "both"}:
            candidates[f"origin_{field_name}"] = work[field_name].values[origin_idx]
        if mode in {"destination", "both"}:
            candidates[f"destination_{field_name}"] = work[field_name].values[destination_idx]

    # Se emite info para dejar trazado el colapso secuencial y el volumen resultante.
    emit_issue(
        issues,
        INFER_TRIPS_ISSUES,
        "INF.CLUSTERS.MODE_APPLIED",
        **_request_details(options_eff, request_ctx),
        n_clusters_out=int(len(clusters_sorted)),
        n_candidates_in=int(len(candidates)),
        clusters_sample=_sample_records(clusters_sorted, limit=5),
    )
    return candidates


def _evaluate_candidates(
    issues: List[Issue],
    candidates: pd.DataFrame,
    *,
    options_eff: InferTripsOptions,
    request_ctx: Dict[str, Any],
) -> tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Evalúa thresholds, clasifica descartes y deja listo el conjunto materializable.

    Emite
    -----
    - INF.CANDIDATES.DROPPED_MAX_TIME_DELTA
    - INF.CANDIDATES.DROPPED_MIN_TIME_DELTA
    - INF.CANDIDATES.DROPPED_MIN_DISTANCE
    - INF.CANDIDATES.DROPPED_SAME_PLACE
    - INF.CANDIDATES.INVALID_DROPPED
    - INF.CANDIDATES.INVALID_RETAINED
    - INF.CANDIDATES.NO_MATERIALIZABLE_CANDIDATES
    """
    if candidates is None or candidates.empty:
        return _empty_candidates_frame(candidates), {
            "n_candidates_in": 0,
            "n_candidates_dropped": 0,
            "n_trips_out": 0,
            "dropped_by_reason": {},
            "n_clusters_out": None,
        }

    candidates_eval = candidates.copy(deep=True)
    n_candidates_in = int(len(candidates_eval))
    dropped_by_reason: Dict[str, int] = {}

    invalid_mask = (
        candidates_eval["user_id"].isna()
        | candidates_eval["origin_time_utc"].isna()
        | candidates_eval["destination_time_utc"].isna()
        | candidates_eval["origin_latitude"].isna()
        | candidates_eval["origin_longitude"].isna()
        | candidates_eval["destination_latitude"].isna()
        | candidates_eval["destination_longitude"].isna()
        | candidates_eval["destination_point_id"].isna()
        | candidates_eval["origin_point_id"].isna()
    )
    threshold_mask = pd.Series(False, index=candidates_eval.index)

    if options_eff.max_time_delta_s is not None:
        mask = candidates_eval["delta_t_s"].notna() & (candidates_eval["delta_t_s"] > float(options_eff.max_time_delta_s))
        if int(mask.sum()) > 0:
            dropped_by_reason["max_time_delta_s"] = int(mask.sum())
            # Se emite info porque estos pares se descartan por el umbral temporal máximo configurado.
            emit_issue(
                issues,
                INFER_TRIPS_ISSUES,
                "INF.CANDIDATES.DROPPED_MAX_TIME_DELTA",
                **_request_details(options_eff, request_ctx),
                n_candidates_in=n_candidates_in,
                n_candidates_dropped=int(mask.sum()),
                n_trips_out=0,
                threshold=options_eff.max_time_delta_s,
                row_indices_sample=_sample_index(candidates_eval.index[mask], 10),
                pairs_sample=_sample_pairs(candidates_eval.loc[mask]),
                reason="max_time_delta_exceeded",
            )
        threshold_mask |= mask

    if options_eff.min_time_delta_s is not None:
        mask = candidates_eval["delta_t_s"].notna() & (candidates_eval["delta_t_s"] < float(options_eff.min_time_delta_s))
        if int(mask.sum()) > 0:
            dropped_by_reason["min_time_delta_s"] = int(mask.sum())
            # Se emite info porque estos pares se descartan por el umbral temporal mínimo configurado.
            emit_issue(
                issues,
                INFER_TRIPS_ISSUES,
                "INF.CANDIDATES.DROPPED_MIN_TIME_DELTA",
                **_request_details(options_eff, request_ctx),
                n_candidates_in=n_candidates_in,
                n_candidates_dropped=int(mask.sum()),
                n_trips_out=0,
                threshold=options_eff.min_time_delta_s,
                row_indices_sample=_sample_index(candidates_eval.index[mask], 10),
                pairs_sample=_sample_pairs(candidates_eval.loc[mask]),
                reason="min_time_delta_not_reached",
            )
        threshold_mask |= mask

    if options_eff.min_distance_m is not None:
        mask = candidates_eval["distance_m"].notna() & (candidates_eval["distance_m"] < float(options_eff.min_distance_m))
        if int(mask.sum()) > 0:
            dropped_by_reason["min_distance_m"] = int(mask.sum())
            # Se emite info porque estos pares se descartan por proximidad espacial mínima no alcanzada.
            emit_issue(
                issues,
                INFER_TRIPS_ISSUES,
                "INF.CANDIDATES.DROPPED_MIN_DISTANCE",
                **_request_details(options_eff, request_ctx),
                n_candidates_in=n_candidates_in,
                n_candidates_dropped=int(mask.sum()),
                n_trips_out=0,
                threshold=options_eff.min_distance_m,
                row_indices_sample=_sample_index(candidates_eval.index[mask], 10),
                pairs_sample=_sample_pairs(candidates_eval.loc[mask]),
                reason="min_distance_not_reached",
            )
        threshold_mask |= mask

    same_place_mask = candidates_eval["same_place"].fillna(False).astype(bool)
    if int(same_place_mask.sum()) > 0:
        dropped_by_reason["same_place"] = int(same_place_mask.sum())
        # Se emite info porque estos pares comparten location_ref y se consideran pseudo-viajes triviales.
        emit_issue(
            issues,
            INFER_TRIPS_ISSUES,
            "INF.CANDIDATES.DROPPED_SAME_PLACE",
            **_request_details(options_eff, request_ctx),
            n_candidates_in=n_candidates_in,
            n_candidates_dropped=int(same_place_mask.sum()),
            n_trips_out=0,
            row_indices_sample=_sample_index(candidates_eval.index[same_place_mask], 10),
            pairs_sample=_sample_pairs(candidates_eval.loc[same_place_mask]),
            reason="same_place_location_ref",
        )
    threshold_mask |= same_place_mask

    invalid_only_mask = invalid_mask & ~threshold_mask
    if int(invalid_only_mask.sum()) > 0:
        dropped_by_reason["invalid_candidate"] = int(invalid_only_mask.sum())
        if options_eff.drop_invalid:
            # Se emite info porque estos candidatos quedan fuera del output por problemas estructurales recuperables.
            emit_issue(
                issues,
                INFER_TRIPS_ISSUES,
                "INF.CANDIDATES.INVALID_DROPPED",
                **_request_details(options_eff, request_ctx),
                n_candidates_in=n_candidates_in,
                n_candidates_dropped=int(invalid_only_mask.sum()),
                n_trips_out=0,
                row_indices_sample=_sample_index(candidates_eval.index[invalid_only_mask], 10),
                pairs_sample=_sample_pairs(candidates_eval.loc[invalid_only_mask]),
                reason="invalid_candidates",
            )
        else:
            # Se emite warning para dejar trazado que hubo candidatos inválidos no tratados como fatales en esta etapa.
            emit_issue(
                issues,
                INFER_TRIPS_ISSUES,
                "INF.CANDIDATES.INVALID_RETAINED",
                **_request_details(options_eff, request_ctx),
                n_candidates_in=n_candidates_in,
                n_candidates_dropped=int(invalid_only_mask.sum()),
                n_trips_out=0,
                row_indices_sample=_sample_index(candidates_eval.index[invalid_only_mask], 10),
                pairs_sample=_sample_pairs(candidates_eval.loc[invalid_only_mask]),
                reason="invalid_candidates_retained_for_review",
            )

    keep_mask = ~threshold_mask
    materializable_mask = keep_mask & ~invalid_mask
    candidates_out = candidates_eval.loc[materializable_mask].reset_index(drop=True)

    n_candidates_dropped = int(n_candidates_in - len(candidates_out))
    if len(candidates_out) == 0 and n_candidates_in > 0:
        # Se emite error recuperable porque ya no queda conjunto materializable para cerrar el output.
        emit_issue(
            issues,
            INFER_TRIPS_ISSUES,
            "INF.CANDIDATES.NO_MATERIALIZABLE_CANDIDATES",
            **_request_details(options_eff, request_ctx),
            n_candidates_in=n_candidates_in,
            n_candidates_dropped=n_candidates_dropped,
            n_trips_out=0,
            reason="all_candidates_eliminated_before_materialization",
        )

    return candidates_out, {
        "n_candidates_in": n_candidates_in,
        "n_candidates_dropped": n_candidates_dropped,
        "n_trips_out": int(len(candidates_out)),
        "dropped_by_reason": dropped_by_reason,
        "n_clusters_out": None,
    }


def _materialize_trip_dataframe(
    issues: List[Issue],
    candidates_df: pd.DataFrame,
    *,
    options_eff: InferTripsOptions,
    request_ctx: Dict[str, Any],
) -> tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Materializa la tabla canónica base de trips a partir de los candidatos válidos.

    Emite
    -----
    - INF.PROPAGATION.APPLIED
    - INF.OUTPUT.SOFT_WIDTH_EXCEEDED
    - INF.OUTPUT.HARD_WIDTH_EXCEEDED
    - INF.OUTPUT.MISSING_REQUIRED_COLUMNS
    """
    propagation_map = dict(options_eff.propagate_trace_fields or {})

    if candidates_df is None or candidates_df.empty:
        trip_df = pd.DataFrame(columns=list(TRIP_MIN_FIELDS) + _expected_propagated_columns(propagation_map))
        if propagation_map:
            # Se emite info para dejar trazada la intención efectiva de propagación incluso con resultado vacío.
            emit_issue(
                issues,
                INFER_TRIPS_ISSUES,
                "INF.PROPAGATION.APPLIED",
                **_request_details(options_eff, request_ctx),
                created_columns_sample=_sample_list(_expected_propagated_columns(propagation_map), 10),
                created_columns_total=len(_expected_propagated_columns(propagation_map)),
                target_columns=_expected_propagated_columns(propagation_map),
                reason="propagation_requested_on_empty_output",
            )
        return trip_df, {
            "created_columns": _expected_propagated_columns(propagation_map),
            "n_columns_out": len(trip_df.columns),
        }

    trip_df = pd.DataFrame(
        {
            "movement_id": [f"m{i}" for i in range(len(candidates_df))],
            "user_id": candidates_df["user_id"].astype("string"),
            "origin_longitude": candidates_df["origin_longitude"],
            "origin_latitude": candidates_df["origin_latitude"],
            "destination_longitude": candidates_df["destination_longitude"],
            "destination_latitude": candidates_df["destination_latitude"],
            "origin_time_utc": candidates_df["origin_time_utc"],
            "destination_time_utc": candidates_df["destination_time_utc"],
        }
    )
    trip_df["trip_id"] = trip_df["movement_id"]
    trip_df["movement_seq"] = 0

    created_columns: List[str] = []
    for field_name, mode in propagation_map.items():
        if mode in {"origin", "both"}:
            col_name = f"origin_{field_name}"
            trip_df[col_name] = candidates_df[col_name] if col_name in candidates_df.columns else pd.Series([pd.NA] * len(trip_df))
            created_columns.append(col_name)
        if mode in {"destination", "both"}:
            col_name = f"destination_{field_name}"
            trip_df[col_name] = candidates_df[col_name] if col_name in candidates_df.columns else pd.Series([pd.NA] * len(trip_df))
            created_columns.append(col_name)

    if created_columns:
        # Se emite info porque la propagación explícita forma parte del contrato observable del output.
        emit_issue(
            issues,
            INFER_TRIPS_ISSUES,
            "INF.PROPAGATION.APPLIED",
            **_request_details(options_eff, request_ctx),
            created_columns_sample=_sample_list(created_columns, 10),
            created_columns_total=len(created_columns),
            target_columns=created_columns,
            reason="field_propagation_applied",
        )

    n_columns_out = len(trip_df.columns)
    if n_columns_out > TRIPDATASET_COLUMNS_HARD_CAP:
        # Se emite fatal porque el output quedó demasiado ancho para seguir siendo razonable en memoria.
        emit_and_maybe_raise(
            issues,
            INFER_TRIPS_ISSUES,
            "INF.OUTPUT.HARD_WIDTH_EXCEEDED",
            strict=False,
            exception_map=EXCEPTION_MAP_INFER,
            default_exception=InferenceError,
            n_columns=n_columns_out,
            soft_cap=TRIPDATASET_COLUMNS_SOFT_CAP,
            hard_cap=TRIPDATASET_COLUMNS_HARD_CAP,
            created_columns_sample=_sample_list(created_columns, 10),
            created_columns_total=len(created_columns),
        )
    elif n_columns_out > TRIPDATASET_COLUMNS_SOFT_CAP:
        # Se emite warning porque la propagación dejó una tabla muy ancha aunque todavía utilizable.
        emit_issue(
            issues,
            INFER_TRIPS_ISSUES,
            "INF.OUTPUT.SOFT_WIDTH_EXCEEDED",
            n_columns=n_columns_out,
            soft_cap=TRIPDATASET_COLUMNS_SOFT_CAP,
            hard_cap=TRIPDATASET_COLUMNS_HARD_CAP,
            created_columns_sample=_sample_list(created_columns, 10),
            created_columns_total=len(created_columns),
        )

    missing_required_now = [field for field in TRIP_MIN_FIELDS if field not in trip_df.columns and field not in {"origin_h3_index", "destination_h3_index"}]
    if missing_required_now:
        # Se emite fatal porque ni siquiera la tabla base de trips pudo materializar el núcleo previo a H3.
        emit_and_maybe_raise(
            issues,
            INFER_TRIPS_ISSUES,
            "INF.OUTPUT.MISSING_REQUIRED_COLUMNS",
            strict=False,
            exception_map=EXCEPTION_MAP_INFER,
            default_exception=InferenceError,
            missing_fields=missing_required_now,
            required_fields=TRIP_MIN_FIELDS,
            output_fields_sample=_sample_list(trip_df.columns, 20),
            output_fields_total=len(trip_df.columns),
        )

    return trip_df, {
        "created_columns": created_columns,
        "n_columns_out": n_columns_out,
    }


def _enrich_trip_dataframe(
    issues: List[Issue],
    trip_df: pd.DataFrame,
    *,
    trip_schema: TripSchema,
    value_correspondence: Mapping[str, Mapping[Any, Any]] | None,
    options_eff: InferTripsOptions,
    request_ctx: Dict[str, Any],
) -> tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Deriva H3 y aplica normalización categórica solo sobre campos de salida materializados.

    Emite
    -----
    - INF.H3.DERIVED
    - INF.H3.DERIVATION_FAILED
    - MAP.VALUES.APPLIED
    - MAP.VALUES.FIELD_NOT_MATERIALIZED
    - MAP.VALUES.NON_CATEGORICAL_FIELD
    - MAP.VALUES.UNKNOWN_CANONICAL_VALUE
    - DOM.POLICY.FIELD_NOT_EXTENDABLE
    - DOM.POLICY.MAPPING_REQUIRES_EXTENSION_BLOCKED
    - DOM.STRICT.OUT_OF_DOMAIN_ABORT
    - DOM.EXTENSION.APPLIED
    - DOM.INFERENCE.APPLIED
    - DOM.INFERENCE.DEGRADED_TO_STRING
    - INF.OUTPUT.MISSING_REQUIRED_COLUMNS
    """
    enriched = trip_df.copy(deep=True)
    h3_meta = {"resolution": int(options_eff.h3_resolution)}
    applied_value_correspondence: Dict[str, Dict[str, Any]] = {}
    domains_effective: Dict[str, Any] = {}
    dtype_effective: Dict[str, str] = {}

    # Se derivan siempre los índices H3 OD porque el output mínimo los exige para flujos posteriores.
    if len(enriched) == 0:
        enriched["origin_h3_index"] = pd.Series(dtype="object")
        enriched["destination_h3_index"] = pd.Series(dtype="object")
    else:
        origin_h3 = _derive_h3_series(
            enriched["origin_latitude"],
            enriched["origin_longitude"],
            options_eff.h3_resolution,
        )
        destination_h3 = _derive_h3_series(
            enriched["destination_latitude"],
            enriched["destination_longitude"],
            options_eff.h3_resolution,
        )
        failed_mask = origin_h3.isna() | destination_h3.isna()
        if int(failed_mask.sum()) > 0:
            # Se emite error porque la ausencia de H3 vuelve incompleto el output mínimo de trips.
            emit_issue(
                issues,
                INFER_TRIPS_ISSUES,
                "INF.H3.DERIVATION_FAILED",
                **_request_details(options_eff, request_ctx),
                n_candidates_in=request_ctx.get("n_candidates_in", 0),
                n_candidates_dropped=request_ctx.get("n_candidates_dropped", 0),
                n_trips_out=int(len(enriched) - int(failed_mask.sum())),
                row_indices_sample=_sample_index(enriched.index[failed_mask], 10),
                sample_rows=_sample_records(enriched.loc[failed_mask, ["origin_latitude", "origin_longitude", "destination_latitude", "destination_longitude"]], 5),
                reason="h3_derivation_failed",
            )
        enriched["origin_h3_index"] = origin_h3
        enriched["destination_h3_index"] = destination_h3
        enriched = enriched[~failed_mask].reset_index(drop=True)
        if len(enriched) > 0:
            # Se emite info porque el output quedó espacialmente habilitado para el resto del pipeline.
            emit_issue(
                issues,
                INFER_TRIPS_ISSUES,
                "INF.H3.DERIVED",
                **_request_details(options_eff, request_ctx),
                n_trips_out=int(len(enriched)),
            )

    schema_fields = trip_schema.fields

    categorical_output_fields = [
        field_name
        for field_name, field_spec in schema_fields.items()
        if field_name in enriched.columns and getattr(field_spec, "dtype", None) == "categorical"
    ]

    for field_name in categorical_output_fields:
        mapping = dict((value_correspondence or {}).get(field_name, {}))

        normalized_series, field_domain_effective, field_applied_map, field_issues, field_dtype_effective = (
            _normalize_output_categorical_field(
                enriched[field_name],
                field_name=field_name,
                field_spec=schema_fields[field_name],
                value_mapping=mapping,
                strict_domains=options_eff.strict_domains,
            )
        )

        enriched[field_name] = normalized_series
        domains_effective[field_name] = field_domain_effective
        if field_applied_map:
            applied_value_correspondence[field_name] = field_applied_map
        if field_dtype_effective is not None:
            dtype_effective[field_name] = field_dtype_effective
        issues.extend(field_issues)

    missing_required_final = [field for field in TRIP_MIN_FIELDS if field not in enriched.columns]
    if missing_required_final:
        # Se emite fatal porque el dataframe enriquecido perdió parte del núcleo mínimo del output.
        emit_and_maybe_raise(
            issues,
            INFER_TRIPS_ISSUES,
            "INF.OUTPUT.MISSING_REQUIRED_COLUMNS",
            strict=False,
            exception_map=EXCEPTION_MAP_INFER,
            default_exception=InferenceError,
            missing_fields=missing_required_final,
            required_fields=TRIP_MIN_FIELDS,
            output_fields_sample=_sample_list(enriched.columns, 20),
            output_fields_total=len(enriched.columns),
        )

    return enriched, {
        "h3_meta": h3_meta,
        "domains_effective": domains_effective,
        "value_correspondence_applied": applied_value_correspondence,
        "dtype_effective": dtype_effective,
    }


def _build_inference_outputs(
    issues: List[Issue],
    *,
    traces: TraceDataset,
    trip_df: pd.DataFrame,
    trip_schema: TripSchema,
    options_eff: InferTripsOptions,
    parameters_effective: Dict[str, Any],
    request_ctx: Dict[str, Any],
    eval_info: Dict[str, Any],
    materialization_info: Dict[str, Any],
    enrich_info: Dict[str, Any],
    value_correspondence: Mapping[str, Mapping[Any, Any]] | None,
    provenance: dict[str, Any] | None,
    clusters_df: pd.DataFrame | None,
) -> tuple[TripDataset, InferenceReport]:
    """
    Construye schema_effective, metadata, provenance, evento y InferenceReport.

    Emite
    -----
    - PROV.DERIVED_FROM_BUILD_FAILED
    - INF.WARN.ZERO_TRIPS
    - INF.OK.SUMMARY
    """
    summary = {
        "infer_mode": options_eff.infer_mode,
        "n_points_in": int(request_ctx.get("n_points_in", 0)),
        "n_candidates_in": int(eval_info.get("n_candidates_in", 0)),
        "n_candidates_dropped": int(eval_info.get("n_candidates_dropped", 0)),
        "n_trips_out": int(len(trip_df)),
        "dropped_by_reason": dict(eval_info.get("dropped_by_reason", {})),
    }
    if options_eff.infer_mode == "consecutive_clusters":
        summary["n_clusters_out"] = int(0 if clusters_df is None else len(clusters_df))

    if len(trip_df) == 0:
        # Se emite warning porque el output final quedó sin filas aun cuando la operación fue interpretable.
        emit_issue(
            issues,
            INFER_TRIPS_ISSUES,
            "INF.WARN.ZERO_TRIPS",
            **_request_details(options_eff, request_ctx),
            n_candidates_in=summary["n_candidates_in"],
            n_candidates_dropped=summary["n_candidates_dropped"],
            n_trips_out=0,
            dropped_by_reason=summary["dropped_by_reason"],
            n_clusters_out=summary.get("n_clusters_out"),
        )

    # Se emite un info final de cierre para dejar un resumen operativo compacto de la inferencia.
    emit_issue(
        issues,
        INFER_TRIPS_ISSUES,
        "INF.OK.SUMMARY",
        **_request_details(options_eff, request_ctx),
        n_candidates_in=summary["n_candidates_in"],
        n_candidates_dropped=summary["n_candidates_dropped"],
        n_trips_out=summary["n_trips_out"],
        dropped_by_reason=summary["dropped_by_reason"],
        n_clusters_out=summary.get("n_clusters_out"),
    )

    schema_effective = TripSchemaEffective(
        dtype_effective=dict(enrich_info.get("dtype_effective", {})),
        overrides={},
        domains_effective=dict(enrich_info.get("domains_effective", {})),
        temporal={"tier": "tier_1"},
        fields_effective=list(trip_df.columns),
    )

    prior_events_summary = _summarize_prior_events(getattr(traces, "metadata", {}).get("events"))
    derived_from_entry = {
        "source_type": "traces",
        "dataset_id": getattr(traces, "metadata", {}).get("dataset_id") if isinstance(getattr(traces, "metadata", None), dict) else None,
        "schema_version": getattr(getattr(traces, "schema", None), "version", None),
        "n_rows": int(len(traces.data)),
        "time_range_utc": _time_range_summary(traces.data["time_utc"]) if isinstance(traces.data, pd.DataFrame) and "time_utc" in traces.data.columns else None,
    }
    provenance_out: Dict[str, Any] = {
        "derived_from": [derived_from_entry],
        "prior_events_summary": prior_events_summary,
    }
    if provenance is not None:
        provenance_out["user_provenance"] = provenance

    metadata: Dict[str, Any] = {
        "dataset_id": f"tripds_{uuid.uuid4().hex}",
        "is_validated": False,
        "events": [],
        "temporal": {
            "tier": "tier_1",
            "fields_present": ["origin_time_utc", "destination_time_utc"],
        },
    }
    h3_meta = enrich_info.get("h3_meta")
    if h3_meta:
        metadata["h3"] = h3_meta

    mappings_meta: Dict[str, Any] = {}
    if enrich_info.get("value_correspondence_applied"):
        mappings_meta["value_correspondence"] = enrich_info["value_correspondence_applied"]
    if options_eff.propagate_trace_fields:
        mappings_meta["field_propagation"] = dict(options_eff.propagate_trace_fields)
    if mappings_meta:
        metadata["mappings"] = mappings_meta

    event = {
        "op": "infer_trips",
        "ts_utc": _utc_now_iso(),
        "parameters": parameters_effective,
        "summary": summary,
        "issues_summary": _build_issues_summary(issues),
    }
    metadata["events"] = [event]

    trip_dataset = TripDataset(
        data=trip_df,
        schema=trip_schema,
        schema_version=getattr(trip_schema, "version", "0.0.0"),
        provenance=provenance_out,
        field_correspondence={},
        value_correspondence=dict(enrich_info.get("value_correspondence_applied", {})),
        metadata=metadata,
        schema_effective=schema_effective,
    )
    trip_dataset.metadata["is_validated"] = False

    ok = not any(issue.level == "error" for issue in issues)
    report = InferenceReport(
        ok=ok,
        issues=list(issues),
        summary=summary,
        parameters=parameters_effective,
    )
    return trip_dataset, report


# -----------------------------------------------------------------------------
# Helpers internos de uso general
# -----------------------------------------------------------------------------


def _normalize_optional_number(
    issues: List[Issue],
    *,
    code: str,
    option_name: str,
    value: Any,
    allow_zero: bool,
) -> Optional[float]:
    """Normaliza un threshold numérico opcional y emite fatal si no es interpretable."""
    if value is None:
        return None
    try:
        value_float = float(value)
    except (TypeError, ValueError):
        # Se emite fatal porque el threshold no puede interpretarse como número.
        emit_and_maybe_raise(
            issues,
            INFER_TRIPS_ISSUES,
            code,
            strict=False,
            exception_map=EXCEPTION_MAP_INFER,
            default_exception=InferenceError,
            option=option_name,
            value=value,
        )
    if (allow_zero and value_float < 0) or (not allow_zero and value_float <= 0):
        # Se emite fatal porque el threshold quedó fuera del rango permitido por contrato.
        emit_and_maybe_raise(
            issues,
            INFER_TRIPS_ISSUES,
            code,
            strict=False,
            exception_map=EXCEPTION_MAP_INFER,
            default_exception=InferenceError,
            option=option_name,
            value=value,
        )
    return value_float


def _normalize_propagate_trace_fields(
    issues: List[Issue],
    *,
    traces: TraceDataset,
    propagate_trace_fields: Mapping[str, Any] | None,
) -> Dict[str, str]:
    """Normaliza y valida el request de propagación de atributos desde traces."""
    if propagate_trace_fields is None:
        return {}
    if not isinstance(propagate_trace_fields, Mapping):
        # Se emite fatal porque la estructura del request de propagación debe ser mapping interpretable.
        emit_and_maybe_raise(
            issues,
            INFER_TRIPS_ISSUES,
            "INF.OPTIONS.INVALID_PROPAGATE_TRACE_FIELDS",
            strict=False,
            exception_map=EXCEPTION_MAP_INFER,
            default_exception=InferenceError,
            option="propagate_trace_fields",
            value=repr(propagate_trace_fields),
            reason="not_mapping",
        )
    normalized: Dict[str, str] = {}
    target_columns: List[str] = []
    for field_name, mode_raw in propagate_trace_fields.items():
        if not isinstance(field_name, str) or field_name.strip() == "":
            # Se emite fatal porque cada entrada de propagación debe identificar un campo de traces real.
            emit_and_maybe_raise(
                issues,
                INFER_TRIPS_ISSUES,
                "INF.OPTIONS.INVALID_PROPAGATE_TRACE_FIELDS",
                strict=False,
                exception_map=EXCEPTION_MAP_INFER,
                default_exception=InferenceError,
                option="propagate_trace_fields",
                value=repr(propagate_trace_fields),
                reason="invalid_field_name",
            )
        mode = str(mode_raw).strip().lower()
        if mode not in _ALLOWED_PROPAGATION_MODES:
            # Se emite fatal porque solo se permite origin/destination/both como semántica de propagación.
            emit_and_maybe_raise(
                issues,
                INFER_TRIPS_ISSUES,
                "INF.OPTIONS.INVALID_PROPAGATE_TRACE_FIELDS",
                strict=False,
                exception_map=EXCEPTION_MAP_INFER,
                default_exception=InferenceError,
                option="propagate_trace_fields",
                value=repr(propagate_trace_fields),
                reason="invalid_propagation_mode",
            )
        if field_name not in traces.data.columns:
            # Se emite fatal porque no se puede propagar un campo ausente en traces.data.
            emit_and_maybe_raise(
                issues,
                INFER_TRIPS_ISSUES,
                "INF.PROPAGATION.UNKNOWN_TRACE_FIELD",
                strict=False,
                exception_map=EXCEPTION_MAP_INFER,
                default_exception=InferenceError,
                field=field_name,
                available_fields_sample=_sample_list(traces.data.columns, 20),
                available_fields_total=len(traces.data.columns),
            )
        normalized[field_name] = mode
        if mode in {"origin", "both"}:
            target_columns.append(f"origin_{field_name}")
        if mode in {"destination", "both"}:
            target_columns.append(f"destination_{field_name}")

    collisions = [col for col, count in Counter(target_columns).items() if count > 1 or col in _RESERVED_OUTPUT_FIELDS]
    if collisions:
        # Se emite fatal porque la propagación no puede colisionar con el núcleo canónico ni consigo misma.
        emit_and_maybe_raise(
            issues,
            INFER_TRIPS_ISSUES,
            "INF.PROPAGATION.RESERVED_TARGET_CONFLICT",
            strict=False,
            exception_map=EXCEPTION_MAP_INFER,
            default_exception=InferenceError,
            field=collisions[0] if collisions else None,
            propagate_trace_fields=dict(normalized),
            target_columns=target_columns,
            created_columns_sample=_sample_list(target_columns, 10),
            created_columns_total=len(target_columns),
            reason="reserved_or_duplicate_target_columns",
        )
    return normalized


def _prepare_trace_workframe(traces_df: pd.DataFrame) -> pd.DataFrame:
    """Construye una copia local tipada mínima para pairing/clustering sin mutar traces.data."""
    work = traces_df.copy(deep=True).reset_index(drop=True)
    work["_row_idx"] = np.arange(len(work), dtype="int64")
    work["time_utc"] = pd.to_datetime(work["time_utc"], errors="coerce", utc=True)
    work["latitude"] = pd.to_numeric(work["latitude"], errors="coerce")
    work["longitude"] = pd.to_numeric(work["longitude"], errors="coerce")
    if "user_id" in work.columns:
        work["user_id"] = work["user_id"].astype("string")
    if "point_id" in work.columns:
        work["point_id"] = work["point_id"].astype("string")
    return work


def _cluster_record(
    *,
    cluster_id: int,
    user_id: Any,
    work: pd.DataFrame,
    start_idx: int,
    last_idx: int,
    n_points: int,
) -> Dict[str, Any]:
    """Construye el registro interno mínimo de un cluster secuencial."""
    return {
        "cluster_id": cluster_id,
        "user_id": user_id,
        "cluster_start_utc": work.loc[start_idx, "time_utc"],
        "cluster_end_utc": work.loc[last_idx, "time_utc"],
        "first_point_id": work.loc[start_idx, "point_id"],
        "last_point_id": work.loc[last_idx, "point_id"],
        "n_points": int(n_points),
        "first_row_idx": int(work.loc[start_idx, "_row_idx"]),
        "last_row_idx": int(work.loc[last_idx, "_row_idx"]),
    }


def _same_place_mask(candidates_df: pd.DataFrame) -> pd.Series:
    """Calcula la regla same_place usando location_ref cuando ambos extremos la exponen."""
    if "origin_location_ref" not in candidates_df.columns or "destination_location_ref" not in candidates_df.columns:
        return pd.Series(False, index=candidates_df.index)
    return (
        candidates_df["origin_location_ref"].notna()
        & candidates_df["destination_location_ref"].notna()
        & (candidates_df["origin_location_ref"].astype("string") == candidates_df["destination_location_ref"].astype("string"))
    )


def _haversine_meters(lat1: np.ndarray, lon1: np.ndarray, lat2: np.ndarray, lon2: np.ndarray) -> np.ndarray:
    """Calcula distancias Haversine vectorizadas en metros."""
    lat1 = np.asarray(lat1, dtype=float)
    lon1 = np.asarray(lon1, dtype=float)
    lat2 = np.asarray(lat2, dtype=float)
    lon2 = np.asarray(lon2, dtype=float)

    invalid = np.isnan(lat1) | np.isnan(lon1) | np.isnan(lat2) | np.isnan(lon2)
    r = 6_371_000.0
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi / 2.0) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2.0) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    out = r * c
    out[invalid] = np.nan
    return out


def _safe_distance_meters(lat1: Any, lon1: Any, lat2: Any, lon2: Any) -> Optional[float]:
    """Calcula una distancia puntual segura en metros o None si falta evidencia mínima."""
    try:
        if pd.isna(lat1) or pd.isna(lon1) or pd.isna(lat2) or pd.isna(lon2):
            return None
        return float(_haversine_meters(np.array([lat1]), np.array([lon1]), np.array([lat2]), np.array([lon2]))[0])
    except Exception:
        return None


def _safe_time_gap_seconds(t1: Any, t2: Any) -> Optional[float]:
    """Calcula un gap temporal seguro en segundos o None si alguno de los extremos no es usable."""
    if pd.isna(t1) or pd.isna(t2):
        return None
    try:
        return float((t2 - t1).total_seconds())
    except Exception:
        return None


def _empty_candidates_frame(candidates: pd.DataFrame | None) -> pd.DataFrame:
    """Devuelve un dataframe vacío preservando columnas si existe un intermedio previo."""
    if candidates is None:
        return pd.DataFrame()
    return candidates.iloc[0:0].copy()


def _expected_propagated_columns(propagation_map: Mapping[str, str]) -> List[str]:
    """Expande el mapping de propagación a la lista de columnas esperadas en el output."""
    cols: List[str] = []
    for field_name, mode in propagation_map.items():
        if mode in {"origin", "both"}:
            cols.append(f"origin_{field_name}")
        if mode in {"destination", "both"}:
            cols.append(f"destination_{field_name}")
    return cols


def _derive_h3_series(lat_s: pd.Series, lon_s: pd.Series, resolution: int) -> pd.Series:
    """Deriva una serie H3 a partir de coordenadas lat/lon ya parseadas."""
    out_values: List[Optional[str]] = []
    for lat, lon in zip(lat_s, lon_s):
        try:
            if pd.isna(lat) or pd.isna(lon):
                out_values.append(None)
            else:
                out_values.append(h3.latlng_to_cell(float(lat), float(lon), int(resolution)))
        except Exception:
            out_values.append(None)
    return pd.Series(out_values, index=lat_s.index, dtype="object")


def _normalize_output_categorical_field(
    series: pd.Series,
    *,
    field_name: str,
    field_spec: Any,
    value_mapping: Mapping[Any, Any],
    strict_domains: bool,
) -> tuple[pd.Series, Dict[str, Any], Dict[str, Any], List[Issue], Optional[str]]:
    """Normaliza un campo categórico del output y construye dominio efectivo mínimo."""
    issues: List[Issue] = []
    s = series.copy(deep=True)
    if ptypes.is_categorical_dtype(s):
        s = s.astype("string")
    elif not ptypes.is_string_dtype(s):
        s = s.astype("string")

    domain: DomainSpec | None = getattr(field_spec, "domain", None)
    base_values = list(domain.values) if isinstance(domain, DomainSpec) else []
    extendable = bool(domain.extendable) if isinstance(domain, DomainSpec) else False
    aliases = dict(domain.aliases or {}) if isinstance(domain, DomainSpec) and domain.aliases else {}

    mapping_eff = {k: v for k, v in dict(value_mapping or {}).items()}
    changed_count = 0
    applied_map: Dict[str, Any] = {}
    normalized_values: List[Any] = []

    for value in s.tolist():
        if pd.isna(value):
            normalized_values.append(pd.NA)
            continue
        value_text = str(value)
        mapped_value = mapping_eff.get(value_text, aliases.get(value_text, value_text))
        if mapped_value != value_text:
            changed_count += 1
            applied_map[value_text] = mapped_value
        normalized_values.append(mapped_value)

    s = pd.Series(normalized_values, index=s.index, dtype="string")
    non_null_values = [str(v) for v in s.dropna().tolist()]
    observed_unique = list(dict.fromkeys(non_null_values))
    n_rows_non_null = len(non_null_values)
    n_unique_observed = len(observed_unique)
    cardinality_limit = min(CATEGORICAL_INFERENCE_K_MAX, max(1, int(CATEGORICAL_INFERENCE_ALPHA_DECLARED * max(n_rows_non_null, 1))))

    if changed_count > 0:
        # Se emite info porque efectivamente hubo normalización de valores en el output inferido.
        emit_issue(
            issues,
            INFER_TRIPS_ISSUES,
            "MAP.VALUES.APPLIED",
            field=field_name,
            n_values_changed=changed_count,
            policy="value_correspondence",
        )

    if domain is None:
        return s, {}, applied_map, issues, None

    if len(base_values) == 0:
        if n_unique_observed > cardinality_limit:
            # Se emite warning porque el bootstrap categórico supera la heurística permitida y se degrada a texto.
            emit_issue(
                issues,
                INFER_TRIPS_ISSUES,
                "DOM.INFERENCE.DEGRADED_TO_STRING",
                field=field_name,
                n_rows_non_null=n_rows_non_null,
                n_unique_observed=n_unique_observed,
                alpha=CATEGORICAL_INFERENCE_ALPHA_DECLARED,
                k_max=CATEGORICAL_INFERENCE_K_MAX,
                cardinality_limit=cardinality_limit,
                observed_values_sample=_sample_list(observed_unique, 20),
                observed_values_total=n_unique_observed,
                fallback_dtype="string",
                reason="high_cardinality_for_categorical_inference",
            )
            return s, {"values": [], "extendable": bool(extendable), "degraded": True}, applied_map, issues, "string"
        # Se emite info porque el dominio efectivo se infirió bootstrap desde los valores observados.
        emit_issue(
            issues,
            INFER_TRIPS_ISSUES,
            "DOM.INFERENCE.APPLIED",
            field=field_name,
            n_rows_non_null=n_rows_non_null,
            n_unique_observed=n_unique_observed,
            alpha=CATEGORICAL_INFERENCE_ALPHA_DECLARED,
            k_max=CATEGORICAL_INFERENCE_K_MAX,
            cardinality_limit=cardinality_limit,
            observed_values_sample=_sample_list(observed_unique, 20),
            observed_values_total=n_unique_observed,
        )
        return s.astype("category"), {"values": observed_unique, "extendable": bool(extendable)}, applied_map, issues, "categorical"

    out_of_domain = [value for value in observed_unique if value not in base_values]
    if out_of_domain:
        if not extendable:
            # Se emite warning porque el schema no permite extensión automática de dominio para este campo.
            emit_issue(
                issues,
                INFER_TRIPS_ISSUES,
                "DOM.POLICY.FIELD_NOT_EXTENDABLE",
                field=field_name,
                strict_domains=strict_domains,
                domain_extendable=False,
            )
            if strict_domains:
                # Se emite error porque strict_domains=True exige abortar ante valores fuera de dominio no extendibles.
                emit_issue(
                    issues,
                    INFER_TRIPS_ISSUES,
                    "DOM.STRICT.OUT_OF_DOMAIN_ABORT",
                    field=field_name,
                    unknown_count=len(out_of_domain),
                    total_count=n_rows_non_null,
                    unknown_rate=float(len(out_of_domain) / max(1, n_rows_non_null)),
                    unknown_examples=_sample_list(out_of_domain, 20),
                    policy="strict_domains",
                )
            else:
                # Se emite error recuperable porque el mapeo requeriría extensión que la política del campo bloquea.
                emit_issue(
                    issues,
                    INFER_TRIPS_ISSUES,
                    "DOM.POLICY.MAPPING_REQUIRES_EXTENSION_BLOCKED",
                    field=field_name,
                    strict_domains=strict_domains,
                    domain_extendable=False,
                    unmapped_examples=_sample_list(out_of_domain, 20),
                    unmapped_count=len(out_of_domain),
                    reason="extension_required_but_disallowed",
                )
            return s, {"values": base_values, "extendable": False}, applied_map, issues, "categorical"

        # Se emite info porque el dominio efectivo se extendió de forma controlada durante la inferencia.
        emit_issue(
            issues,
            INFER_TRIPS_ISSUES,
            "DOM.EXTENSION.APPLIED",
            field=field_name,
            n_added=len(out_of_domain),
            added_values_sample=_sample_list(out_of_domain, 20),
            added_values_total=len(out_of_domain),
            policy="extendable_domain",
        )
        return s.astype("category"), {"values": base_values + out_of_domain, "extendable": True, "added_values": out_of_domain}, applied_map, issues, "categorical"

    return s.astype("category"), {"values": base_values, "extendable": bool(extendable)}, applied_map, issues, "categorical"


def _raise_if_inference_must_abort(
    report: InferenceReport,
    trip_dataset: TripDataset,
    *,
    options_eff: InferTripsOptions,
) -> None:
    """Escala errores de inferencia recién después de construir reporte, evento y dataset derivado."""
    error_issues = [issue for issue in report.issues if issue.level == "error"]
    if not error_issues:
        return

    if not options_eff.strict and not options_eff.strict_domains:
        return

    domain_error_codes = {
        "DOM.POLICY.MAPPING_REQUIRES_EXTENSION_BLOCKED",
        "DOM.STRICT.OUT_OF_DOMAIN_ABORT",
    }
    domain_error = next((issue for issue in error_issues if issue.code in domain_error_codes), None)
    if domain_error is not None and options_eff.strict_domains:
        raise InferenceError(
            domain_error.message,
            code=domain_error.code,
            details={
                "summary": report.summary,
                "event": trip_dataset.metadata.get("events", [])[-1] if trip_dataset.metadata.get("events") else None,
            },
            issue=domain_error,
            issues=report.issues,
        )

    if options_eff.strict:
        first_error = error_issues[0]
        raise InferenceError(
            first_error.message,
            code=first_error.code,
            details={
                "summary": report.summary,
                "event": trip_dataset.metadata.get("events", [])[-1] if trip_dataset.metadata.get("events") else None,
            },
            issue=first_error,
            issues=report.issues,
        )


def _request_details(options_eff: InferTripsOptions, request_ctx: Dict[str, Any]) -> Dict[str, Any]:
    """Arma el bloque pequeño y repetible de parámetros efectivos que acompaña las emisiones."""
    return {
        "infer_mode": options_eff.infer_mode,
        "strict": options_eff.strict,
        "strict_domains": options_eff.strict_domains,
        "drop_invalid": options_eff.drop_invalid,
        "require_validated_traces": options_eff.require_validated_traces,
        "h3_resolution": options_eff.h3_resolution,
        "max_time_delta_s": options_eff.max_time_delta_s,
        "min_time_delta_s": options_eff.min_time_delta_s,
        "min_distance_m": options_eff.min_distance_m,
        "cluster_radius_m": options_eff.cluster_radius_m,
        "cluster_max_time_gap_s": options_eff.cluster_max_time_gap_s,
        "n_points_in": int(request_ctx.get("n_points_in", 0)),
        "n_users_in": int(request_ctx.get("n_users_in", 0)),
        "propagate_trace_fields": dict(options_eff.propagate_trace_fields or {}),
    }


def _sample_list(values: Sequence[Any], limit: int = 10) -> List[Any]:
    """Devuelve una muestra estable y corta de una secuencia para details JSON-safe."""
    out: List[Any] = []
    for value in list(values)[:limit]:
        out.append(value.item() if hasattr(value, "item") else value)
    return out


def _sample_index(index_like: Sequence[Any], limit: int = 10) -> List[Any]:
    """Devuelve una muestra compacta de índices/posiciones."""
    return _sample_list(list(index_like), limit=limit)


def _sample_pairs(df: pd.DataFrame, limit: int = 5) -> List[Dict[str, Any]]:
    """Resume pares OD candidatos para evidencia ligera en issues."""
    if df is None or df.empty:
        return []
    cols = [
        col
        for col in ["user_id", "origin_point_id", "destination_point_id", "delta_t_s", "distance_m"]
        if col in df.columns
    ]
    return _sample_records(df.loc[:, cols], limit=limit)


def _sample_records(df: pd.DataFrame, limit: int = 5) -> List[Dict[str, Any]]:
    """Convierte un subconjunto pequeño del dataframe a records JSON-safe."""
    if df is None or len(df) == 0:
        return []
    sample = df.head(limit).copy()
    for col in sample.columns:
        if ptypes.is_datetime64_any_dtype(sample[col]):
            sample[col] = sample[col].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    records = sample.to_dict(orient="records")
    return [_to_json_safe(record) for record in records]


def _build_issues_summary(issues: Sequence[Issue]) -> Dict[str, Any]:
    """Construye un resumen pequeño y estable de issues por nivel y por code."""
    counts_by_level = Counter(issue.level for issue in issues)
    counts_by_code = Counter(issue.code for issue in issues)
    return {
        "counts": {
            "info": int(counts_by_level.get("info", 0)),
            "warning": int(counts_by_level.get("warning", 0)),
            "error": int(counts_by_level.get("error", 0)),
        },
        "counts_by_code": dict(counts_by_code),
    }


def _summarize_prior_events(events: Any) -> Dict[str, Any]:
    """Resume el historial previo sin copiar eventos completos al dataset derivado."""
    if not isinstance(events, list):
        return {"n_events": 0, "ops": [], "last_event_op": None}
    ops = [event.get("op") for event in events if isinstance(event, dict) and event.get("op")]
    return {
        "n_events": int(len(events)),
        "ops": ops[:10],
        "last_event_op": ops[-1] if ops else None,
    }


def _time_range_summary(series: pd.Series) -> Optional[Dict[str, Any]]:
    """Resume el rango temporal UTC de una serie cuando es calculable."""
    parsed = pd.to_datetime(series, errors="coerce", utc=True)
    parsed = parsed.dropna()
    if len(parsed) == 0:
        return None
    return {
        "start": parsed.min().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end": parsed.max().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def _json_is_serializable(obj: Any) -> bool:
    """Indica si un objeto es serializable a JSON sin transformaciones adicionales."""
    try:
        json.dumps(obj)
        return True
    except (TypeError, ValueError):
        return False


def _to_json_safe(obj: Any) -> Any:
    """Convierte objetos frecuentes de numpy/pandas a representaciones JSON-safe."""
    if isinstance(obj, dict):
        return {str(k): _to_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_json_safe(v) for v in obj]
    if isinstance(obj, tuple):
        return [_to_json_safe(v) for v in obj]
    if isinstance(obj, pd.Timestamp):
        if obj.tzinfo is None:
            return obj.strftime("%Y-%m-%dT%H:%M:%SZ")
        return obj.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
    if isinstance(obj, np.generic):
        return obj.item()
    if pd.isna(obj):
        return None
    return obj


def _utc_now_iso() -> str:
    """Retorna un timestamp UTC ISO-8601 pequeño y consistente para eventos."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
