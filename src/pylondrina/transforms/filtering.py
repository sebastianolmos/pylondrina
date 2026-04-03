# -------------------------
# file: pylondrina/transforms/filtering.py
# -------------------------
from __future__ import annotations

import copy
import json
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Literal, Mapping, Optional, Sequence, Tuple, Union

import h3
import pandas as pd
from pandas.api import types as ptypes

from pylondrina.datasets import TripDataset
from pylondrina.errors import FilterError, PylondrinaError
from pylondrina.issues.catalogo_filter_trips import FILTER_TRIPS_ISSUES
from pylondrina.issues.core import emit_and_maybe_raise, emit_issue
from pylondrina.reports import Issue, OperationReport
from pylondrina.schema import TripSchemaEffective

# Tipos del contrato público.
BBox = Tuple[float, float, float, float]
"""Bounding box (min_lon, min_lat, max_lon, max_lat)."""

LonLat = Tuple[float, float]
"""Coordenada (lon, lat)."""

Polygon = Sequence[LonLat]
"""Polígono como secuencia de vértices (lon, lat)."""

TimePredicate = Literal["starts_within", "ends_within", "contains", "overlaps"]
"""
Predicado temporal entre el intervalo del viaje y el intervalo [start, end].
El intervalo del viaje se define SIEMPRE como: [origin_time_utc, destination_time_utc]

- "starts_within": origin_time_utc cae dentro de [start, end].
- "ends_within": destination_time_utc cae dentro de [start, end].
- "contains": [origin_time_utc, destination_time_utc] contiene completamente [start, end].
- "overlaps": los intervalos se intersectan.
"""

SpatialPredicate = Literal["origin", "destination", "both", "either"]
"""
Predicado espacial para decidir sobre qué extremo(s) aplicar un filtro espacial.

- "origin": aplica el filtro espacial al punto de origen.
- "destination": aplica el filtro espacial al punto de destino.
- "both": requiere que origen Y destino cumplan el filtro (AND).
- "either": requiere que origen O destino cumpla el filtro (OR).
"""

WhereOp = Literal["eq", "in", "ne", "not_in", "is_null", "not_null", "gt", "gte", "lt", "lte", "between",]
"""
Operadores soportados por `FilterOptions.where` (v1.1).

- eq / in: igualdad y pertenencia (también pueden expresarse implícitamente).
- ne / not_in: desigualdad y no-pertenencia.
- is_null / not_null: filtros por nulidad (aplican a cualquier campo).
- gt / gte / lt / lte / between: comparaciones numéricas (int/float).
"""

WhereValue = Union[Any, Sequence[Any], Mapping[WhereOp, Any]]
WhereClause = Mapping[str, WhereValue]


_ALLOWED_SPATIAL_PREDICATES = {"origin", "destination", "both", "either"}
_ALLOWED_WHERE_OPS = {
    "eq",
    "ne",
    "in",
    "not_in",
    "is_null",
    "not_null",
    "gt",
    "gte",
    "lt",
    "lte",
    "between",
}
_ALLOWED_WHERE_OPS_BY_DTYPE = {
    "string": {"eq", "ne", "in", "not_in", "is_null", "not_null"},
    "categorical": {"eq", "ne", "in", "not_in", "is_null", "not_null"},
    "int": {"eq", "ne", "in", "not_in", "is_null", "not_null", "gt", "gte", "lt", "lte", "between"},
    "float": {"eq", "ne", "in", "not_in", "is_null", "not_null", "gt", "gte", "lt", "lte", "between"},
    "datetime": {"eq", "ne", "in", "not_in", "is_null", "not_null", "gt", "gte", "lt", "lte", "between"},
    "bool": {"eq", "ne", "is_null", "not_null"},
}
_SUMMARY_FILTER_KEYS = ("where", "time", "bbox", "polygon", "h3_cells")
_TIME_FIELDS = ("origin_time_utc", "destination_time_utc")
_ORIGIN_LATLON_FIELDS = ("origin_longitude", "origin_latitude")
_DESTINATION_LATLON_FIELDS = ("destination_longitude", "destination_latitude")



class _FilterTypeError(TypeError, PylondrinaError):
    """Adaptador interno para errores fatales que deben comportarse como TypeError."""

    def __init__(
        self,
        message: str,
        *,
        code: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
        issue: Optional[Issue] = None,
        issues: Optional[Sequence[Issue]] = None,
    ) -> None:
        PylondrinaError.__init__(
            self,
            message,
            code=code,
            details=details,
            issue=issue,
            issues=issues,
        )


class _FilterValueError(ValueError, PylondrinaError):
    """Adaptador interno para errores fatales que deben comportarse como ValueError."""

    def __init__(
        self,
        message: str,
        *,
        code: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
        issue: Optional[Issue] = None,
        issues: Optional[Sequence[Issue]] = None,
    ) -> None:
        PylondrinaError.__init__(
            self,
            message,
            code=code,
            details=details,
            issue=issue,
            issues=issues,
        )

EXCEPTION_MAP_FILTER = {
    "type": _FilterTypeError,
    "value": _FilterValueError,
    "filter": FilterError,
}

@dataclass(frozen=True)
class TimeFilter:
    """
    Filtro temporal sobre viajes, comparando el intervalo [origin_time_utc, destination_time_utc]
    contra el intervalo [start, end] usando un predicado.

    Attributes
    ----------
    start:
        Inicio del intervalo temporal (inclusive).
    end:
        Fin del intervalo temporal (inclusive).
    predicate:
        Relación entre intervalos (ver TimePredicate).
    """

    start: str
    end: str
    predicate: TimePredicate = "overlaps"


@dataclass(frozen=True)
class FilterOptions:
    """
    Opciones para filtrar un TripDataset por criterios de atributos (`where`), tiempo y/o espacio.

    Reglas de combinación (v1.1)
    ----------------------------
    - Los criterios presentes se combinan como intersección (AND).
    - Dentro de `where`, cada key también se combina por AND.

    Parámetros / Atributos
    ----------------------
    where : Optional[WhereClause]
        Filtros declarativos por campos. `where` es un diccionario `{campo -> condición}`.
        La condición puede escribirse como:
        - Escalar: equivale a `{"eq": valor}`
        - Secuencia: equivale a `{"in": [..]}`
        - Dict operador->valor: permite `eq, in, ne, not_in, is_null, not_null, gt, gte, lt, lte, between`.
    time : Optional[TimeFilter]
        Filtro temporal sobre el intervalo del viaje definido por [origin_time_utc, destination_time_utc]
    bbox : Optional[BBox]
        Filtro espacial por bounding box (min_lon, min_lat, max_lon, max_lat).
    polygon : Optional[Polygon]
        Filtro espacial por polígono lon/lat (secuencia de vértices).
    h3_cells : Optional[Iterable[str]]
        Filtro espacial por conjunto de celdas H3 permitidas.
    spatial_predicate : SpatialPredicate
        Sobre qué extremo(s) del viaje aplica el filtro espacial: "origin", "destination", "both", "either".
    origin_h3_field : str
        Nombre del campo H3 de origen (para filtros por H3).
    destination_h3_field : str
        Nombre del campo H3 de destino (para filtros por H3).
    keep_metadata : bool
        Si True, agrega un evento de filtrado en `TripDataset.metadata["events"]` del dataset resultante.
    strict : bool
        Si True, configuraciones inválidas pueden escalar; si False, se degradan a issues.

    Emite
    ------
    No emite issues directamente; la emisión ocurre en la función pública y sus helpers.
    """

    where: Optional[WhereClause] = None
    time: Optional[TimeFilter] = None

    bbox: Optional[BBox] = None
    polygon: Optional[Polygon] = None
    h3_cells: Optional[Iterable[str]] = None

    spatial_predicate: SpatialPredicate = "origin"
    origin_h3_field: str = "origin_h3_index"
    destination_h3_field: str = "destination_h3_index"

    keep_metadata: bool = True
    strict: bool = False


def filter_trips(
    trips: TripDataset,
    *,
    options: Optional[FilterOptions] = None,
    max_issues: int = 1000,
    sample_rows_per_issue: int = 20,
) -> Tuple[TripDataset, OperationReport]:
    """
    Filtra un TripDataset combinando criterios por atributos, tiempo y/o espacio.

    Parameters
    ----------
    trips : TripDataset
        Dataset de entrada en formato Golondrina.
    options : FilterOptions, optional
        Request declarativo del filtrado. Si es None, se usan defaults efectivos.
    max_issues : int, default=1000
        Límite de issues retenidos en el reporte final.
    sample_rows_per_issue : int, default=20
        Límite de muestra de filas descartadas guardadas en `Issue.details`.

    Returns
    -------
    tuple[TripDataset, OperationReport]
        Nuevo dataset filtrado y reporte estructurado de la operación.
    """
    issues_all: List[Issue] = []

    # ------------------------------------------------------------------
    # 1) Se normaliza el request efectivo y se resuelven abortos fatales.
    # ------------------------------------------------------------------
    options_eff, parameters, filters_requested = _normalize_filter_request(
        trips,
        options=options,
        max_issues=max_issues,
        sample_rows_per_issue=sample_rows_per_issue,
        issues=issues_all,
    )

    # ------------------------------------------------------------------
    # 2) Se construyen las máscaras locales por eje, sin combinar todavía.
    # ------------------------------------------------------------------
    where_mask, where_applied, where_omitted = _build_where_mask(
        trips,
        where=options_eff.where,
        sample_rows_per_issue=sample_rows_per_issue,
        issues=issues_all,
    )
    time_mask, time_applied, time_omitted = _build_time_mask(
        trips,
        time=options_eff.time,
        sample_rows_per_issue=sample_rows_per_issue,
        issues=issues_all,
    )
    spatial_masks, spatial_applied, spatial_omitted = _build_spatial_mask(
        trips,
        bbox=options_eff.bbox,
        polygon=options_eff.polygon,
        h3_cells=options_eff.h3_cells,
        spatial_predicate=options_eff.spatial_predicate,
        origin_h3_field=options_eff.origin_h3_field,
        destination_h3_field=options_eff.destination_h3_field,
        sample_rows_per_issue=sample_rows_per_issue,
        issues=issues_all,
    )

    mask_items: List[Tuple[str, pd.Series]] = []
    if where_mask is not None:
        mask_items.append(("where", where_mask))
    if time_mask is not None:
        mask_items.append(("time", time_mask))
    for spatial_name in ("bbox", "polygon", "h3_cells"):
        spatial_mask = spatial_masks.get(spatial_name)
        if spatial_mask is not None:
            mask_items.append((spatial_name, spatial_mask))

    filters_applied = []
    if where_applied:
        filters_applied.append("where")
    if time_applied:
        filters_applied.append("time")
    filters_applied.extend(spatial_applied)

    filters_omitted = []
    if where_omitted:
        filters_omitted.append("where")
    if time_omitted:
        filters_omitted.append("time")
    filters_omitted.extend(spatial_omitted)

    # ------------------------------------------------------------------
    # 3) Se combinan las máscaras con AND global y se cuantifica el efecto.
    # ------------------------------------------------------------------
    survival_mask, dropped_by_filter, rows_in, rows_out_preview, dropped_total = _combine_filter_masks(
        trips,
        mask_items=mask_items,
        filters_requested=filters_requested,
        filters_applied=filters_applied,
        filters_omitted=filters_omitted,
        issues=issues_all,
    )

    # ------------------------------------------------------------------
    # 4) Se hace el commit único final y se reconstruye el dataset derivado.
    # ------------------------------------------------------------------
    filtered_trips = _materialize_filtered_tripdataset(
        trips,
        mask_survival=survival_mask,
        keep_metadata=bool(options_eff.keep_metadata),
    )

    # ------------------------------------------------------------------
    # 5) Se construye el summary y el reporte final con trazabilidad compacta.
    # ------------------------------------------------------------------
    issues_effective, limits_block = _truncate_issues_with_limit(
        issues_all,
        max_issues=max_issues,
    )
    summary = build_filter_summary(
        rows_in=rows_in,
        rows_out=rows_out_preview,
        dropped_by_filter=dropped_by_filter,
        filters_requested=filters_requested,
        filters_applied=filters_applied,
        filters_omitted=filters_omitted,
        limits=limits_block,
    )
    ok = not any(issue.level == "error" for issue in issues_all)
    report = OperationReport(
        ok=ok,
        issues=issues_effective,
        summary=summary,
        parameters=parameters,
    )
    issues_summary = _build_issues_summary(issues_effective)

    # ------------------------------------------------------------------
    # 6) Se registra el evento si corresponde y recién después se resuelve strict.
    # ------------------------------------------------------------------
    if bool(options_eff.keep_metadata):
        if not isinstance(filtered_trips.metadata.get("events"), list):
            filtered_trips.metadata["events"] = []
        filtered_trips.metadata["events"].append(
            {
                "op": "filter_trips",
                "ts_utc": _utc_now_iso(),
                "parameters": parameters,
                "summary": summary,
                "issues_summary": issues_summary,
            }
        )

    if bool(options_eff.strict) and not ok:
        error_issue = next((issue for issue in issues_effective if issue.level == "error"), None)
        raise FilterError(
            "filter_trips detectó errores de datos y strict=True exige abortar.",
            code=error_issue.code if error_issue is not None else None,
            details=error_issue.details if error_issue is not None else None,
            issue=error_issue,
            issues=issues_effective,
        )

    return filtered_trips, report


def build_filter_summary(
    *,
    rows_in: int,
    rows_out: int,
    dropped_by_filter: Mapping[str, int],
    filters_requested: Sequence[str],
    filters_applied: Sequence[str],
    filters_omitted: Sequence[str],
    limits: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Construye el summary canónico y estable de `filter_trips`.

    Emite
    ------
    No emite issues directamente.
    """
    summary = {
        "rows_in": int(rows_in),
        "rows_out": int(rows_out),
        "dropped_total": int(rows_in) - int(rows_out),
        "dropped_by_filter": {
            filter_name: int(dropped_by_filter.get(filter_name, 0))
            for filter_name in _SUMMARY_FILTER_KEYS
        },
        "filters_requested": list(filters_requested),
        "filters_applied": list(filters_applied),
        "filters_omitted": list(filters_omitted),
    }
    if limits is not None:
        summary["limits"] = limits
    return summary


# -----------------------------------------------------------------------------
# Helpers internos principales del pipeline de la operación
# -----------------------------------------------------------------------------


def _normalize_filter_request(
    trips: Any,
    *,
    options: Optional[FilterOptions],
    max_issues: int,
    sample_rows_per_issue: int,
    issues: List[Issue],
) -> Tuple[FilterOptions, Dict[str, Any], List[str]]:
    """
    Normaliza el request efectivo y resuelve abortos fatales de configuración.

    Emite
    ------
    FLT.CONFIG.INVALID_TRIPS_OBJECT
    FLT.CONFIG.MISSING_DATAFRAME
    FLT.CONFIG.INVALID_OPTIONS_OBJECT
    FLT.CONFIG.INVALID_MAX_ISSUES
    FLT.CONFIG.INVALID_SAMPLE_ROWS_PER_ISSUE
    FLT.WHERE.INVALID_CLAUSE
    FLT.TIME.INVALID_FILTER
    FLT.TIME.INVALID_RANGE
    FLT.SPATIAL.INVALID_PREDICATE
    FLT.SPATIAL.INVALID_BBOX
    FLT.SPATIAL.INVALID_POLYGON
    FLT.SPATIAL.INVALID_H3_CELLS
    """
    # Se asegura temprano que el input sea realmente un TripDataset usable.
    if not isinstance(trips, TripDataset):
        # Se aborta porque sin TripDataset no existe superficie contractual interpretable.
        emit_and_maybe_raise(
            issues,
            FILTER_TRIPS_ISSUES,
            "FLT.CONFIG.INVALID_TRIPS_OBJECT",
            strict=False,
            exception_map=EXCEPTION_MAP_FILTER,
            default_exception=_FilterTypeError,
            received_type=type(trips).__name__,
        )

    # Se fija la superficie normativa de trabajo: TripDataset.data.
    if not hasattr(trips, "data") or not isinstance(trips.data, pd.DataFrame):
        # Se aborta porque OP-05 siempre opera sobre trips.data como DataFrame.
        emit_and_maybe_raise(
            issues,
            FILTER_TRIPS_ISSUES,
            "FLT.CONFIG.MISSING_DATAFRAME",
            strict=False,
            exception_map=EXCEPTION_MAP_FILTER,
            default_exception=_FilterTypeError,
            received_type=type(getattr(trips, "data", None)).__name__,
            reason="missing_or_not_dataframe",
        )

    # Se valida que `options` tenga forma interpretable antes de construir parámetros.
    if options is not None and not isinstance(options, FilterOptions):
        # Se aborta porque el request no puede normalizarse sin un FilterOptions válido.
        emit_and_maybe_raise(
            issues,
            FILTER_TRIPS_ISSUES,
            "FLT.CONFIG.INVALID_OPTIONS_OBJECT",
            strict=False,
            exception_map=EXCEPTION_MAP_FILTER,
            default_exception=_FilterTypeError,
            received_type=type(options).__name__,
        )

    # Se valida el límite de issues antes de ejecutar cualquier parte del pipeline.
    if not isinstance(max_issues, int) or max_issues <= 0:
        # Se aborta porque el control de tamaño del reporte quedó inválido.
        emit_and_maybe_raise(
            issues,
            FILTER_TRIPS_ISSUES,
            "FLT.CONFIG.INVALID_MAX_ISSUES",
            strict=False,
            exception_map=EXCEPTION_MAP_FILTER,
            default_exception=_FilterValueError,
            max_issues=max_issues,
        )

    # Se valida el límite de muestra usado para evidencia agregada por issue.
    if not isinstance(sample_rows_per_issue, int) or sample_rows_per_issue <= 0:
        # Se aborta porque la política de muestreo no es interpretable.
        emit_and_maybe_raise(
            issues,
            FILTER_TRIPS_ISSUES,
            "FLT.CONFIG.INVALID_SAMPLE_ROWS_PER_ISSUE",
            strict=False,
            exception_map=EXCEPTION_MAP_FILTER,
            default_exception=_FilterValueError,
            sample_rows_per_issue=sample_rows_per_issue,
        )

    options_eff = options or FilterOptions()

    # Se sanea `where` a forma serializable, pero sin reinterpretar errores recuperables por campo.
    where_norm: Optional[Dict[str, Any]]
    if options_eff.where is None:
        where_norm = None
    else:
        if not isinstance(options_eff.where, Mapping):
            # Se aborta porque la cláusula where completa no es interpretable como mapping.
            emit_and_maybe_raise(
                issues,
                FILTER_TRIPS_ISSUES,
                "FLT.WHERE.INVALID_CLAUSE",
                strict=False,
                exception_map=EXCEPTION_MAP_FILTER,
                default_exception=_FilterValueError,
                received_type=type(options_eff.where).__name__,
                observed=repr(options_eff.where)[:200],
            )
        where_norm = _normalize_where_for_parameters(options_eff.where)

    # Se valida el filtro temporal como objeto y se normaliza a strings ISO UTC.
    time_norm: Optional[TimeFilter]
    if options_eff.time is None:
        time_norm = None
    else:
        if not isinstance(options_eff.time, TimeFilter):
            # Se aborta porque el filtro temporal completo no cumple el contrato público.
            emit_and_maybe_raise(
                issues,
                FILTER_TRIPS_ISSUES,
                "FLT.TIME.INVALID_FILTER",
                strict=False,
                exception_map=EXCEPTION_MAP_FILTER,
                default_exception=_FilterValueError,
                received_type=type(options_eff.time).__name__,
                observed=repr(options_eff.time)[:200],
            )
        start_iso = _normalize_iso_timestamp_or_abort(
            options_eff.time.start,
            issues,
            code="FLT.TIME.INVALID_RANGE",
            predicate=options_eff.time.predicate,
            value_role="start",
        )
        end_iso = _normalize_iso_timestamp_or_abort(
            options_eff.time.end,
            issues,
            code="FLT.TIME.INVALID_RANGE",
            predicate=options_eff.time.predicate,
            value_role="end",
        )
        start_ts = pd.Timestamp(start_iso)
        end_ts = pd.Timestamp(end_iso)
        if start_ts >= end_ts:
            # Se aborta porque el rango temporal pedido quedó invertido o vacío.
            emit_and_maybe_raise(
                issues,
                FILTER_TRIPS_ISSUES,
                "FLT.TIME.INVALID_RANGE",
                strict=False,
                exception_map=EXCEPTION_MAP_FILTER,
                default_exception=_FilterValueError,
                predicate=options_eff.time.predicate,
                start=start_iso,
                end=end_iso,
                observed={"start": start_iso, "end": end_iso},
                expected="start < end",
            )
        if options_eff.time.predicate not in {"starts_within", "ends_within", "contains", "overlaps"}:
            # Se aborta porque el predicado temporal quedó fuera del contrato cerrado.
            emit_and_maybe_raise(
                issues,
                FILTER_TRIPS_ISSUES,
                "FLT.TIME.INVALID_FILTER",
                strict=False,
                exception_map=EXCEPTION_MAP_FILTER,
                default_exception=_FilterValueError,
                received_type=type(options_eff.time.predicate).__name__,
                observed=options_eff.time.predicate,
            )
        time_norm = TimeFilter(start=start_iso, end=end_iso, predicate=options_eff.time.predicate)

    # Se valida el predicado espacial antes de evaluar cualquier filtro espacial concreto.
    spatial_predicate = str(options_eff.spatial_predicate)
    if spatial_predicate not in _ALLOWED_SPATIAL_PREDICATES:
        # Se aborta porque el predicado espacial no tiene semántica cerrada en v1.1.
        emit_and_maybe_raise(
            issues,
            FILTER_TRIPS_ISSUES,
            "FLT.SPATIAL.INVALID_PREDICATE",
            strict=False,
            exception_map=EXCEPTION_MAP_FILTER,
            default_exception=_FilterValueError,
            spatial_predicate=spatial_predicate,
        )

    # Se valida y normaliza bbox solo si fue solicitado explícitamente.
    bbox_norm = None
    if options_eff.bbox is not None:
        bbox_norm = _normalize_bbox_or_abort(options_eff.bbox, issues)

    # Se valida y normaliza polygon solo si fue solicitado explícitamente.
    polygon_norm = None
    if options_eff.polygon is not None:
        polygon_norm = _normalize_polygon_or_abort(options_eff.polygon, issues)

    # Se valida y normaliza la whitelist H3 solo si fue solicitada explícitamente.
    h3_cells_norm = None
    if options_eff.h3_cells is not None:
        h3_cells_norm = _normalize_h3_cells_or_abort(options_eff.h3_cells, issues)

    options_norm = FilterOptions(
        where=where_norm,
        time=time_norm,
        bbox=bbox_norm,
        polygon=polygon_norm,
        h3_cells=h3_cells_norm,
        spatial_predicate=spatial_predicate,
        origin_h3_field=str(options_eff.origin_h3_field),
        destination_h3_field=str(options_eff.destination_h3_field),
        keep_metadata=bool(options_eff.keep_metadata),
        strict=bool(options_eff.strict),
    )

    filters_requested: List[str] = []
    if where_norm is not None:
        filters_requested.append("where")
    if time_norm is not None:
        filters_requested.append("time")
    if bbox_norm is not None:
        filters_requested.append("bbox")
    if polygon_norm is not None:
        filters_requested.append("polygon")
    if h3_cells_norm is not None:
        filters_requested.append("h3_cells")

    parameters = {
        "where": _to_json_serializable_or_none(where_norm),
        "time": asdict(time_norm) if time_norm is not None else None,
        "bbox": list(bbox_norm) if bbox_norm is not None else None,
        "polygon": _to_json_serializable_or_none(polygon_norm),
        "h3_cells": list(h3_cells_norm) if h3_cells_norm is not None else None,
        "spatial_predicate": spatial_predicate,
        "origin_h3_field": str(options_eff.origin_h3_field),
        "destination_h3_field": str(options_eff.destination_h3_field),
        "keep_metadata": bool(options_eff.keep_metadata),
        "strict": bool(options_eff.strict),
        "max_issues": int(max_issues),
        "sample_rows_per_issue": int(sample_rows_per_issue),
    }
    return options_norm, parameters, filters_requested


def _build_where_mask(
    trips: TripDataset,
    *,
    where: Optional[WhereClause],
    sample_rows_per_issue: int,
    issues: List[Issue],
) -> Tuple[Optional[pd.Series], bool, bool]:
    """
    Construye la máscara del eje `where` y emite issues operacionales asociados.

    Emite
    ------
    FLT.WHERE.FIELD_NOT_FOUND
    FLT.WHERE.OP_UNKNOWN
    FLT.WHERE.OP_INCOMPATIBLE
    FLT.WHERE.INVALID_VALUE_SHAPE
    FLT.INFO.WHERE_APPLIED
    """
    if where is None:
        return None, False, False

    data = trips.data
    field_masks: List[pd.Series] = []
    applied_fields: List[str] = []
    omitted = False

    # Se evalúa cada campo del where por separado para mantener evidencias claras.
    for field_name, raw_clause in where.items():
        normalized_clause, clause_shape = _normalize_where_field_clause(raw_clause)
        if normalized_clause is None:
            # Se omite la cláusula completa porque su forma no es compatible con el DSL cerrado.
            emit_issue(
                issues,
                FILTER_TRIPS_ISSUES,
                "FLT.WHERE.INVALID_VALUE_SHAPE",
                field=str(field_name),
                op=None,
                clause_shape=clause_shape,
                op_value=_to_json_serializable_or_none(raw_clause),
                op_value_type=type(raw_clause).__name__,
                available_fields_sample=_sample_list(list(data.columns), limit=10),
                available_fields_total=len(data.columns),
                allowed_ops=sorted(_ALLOWED_WHERE_OPS),
                dtype_effective=_resolve_field_dtype(trips, str(field_name), None),
                expected="scalar | list/tuple | mapping[op, value]",
                observed=repr(raw_clause)[:200],
            )
            omitted = True
            continue

        if str(field_name) not in data.columns:
            # Se omite la cláusula porque la columna pedida no existe en trips.data.
            emit_issue(
                issues,
                FILTER_TRIPS_ISSUES,
                "FLT.WHERE.FIELD_NOT_FOUND",
                field=str(field_name),
                op=None,
                clause_shape=clause_shape,
                op_value=_to_json_serializable_or_none(raw_clause),
                op_value_type=type(raw_clause).__name__,
                available_fields_sample=_sample_list(list(data.columns), limit=10),
                available_fields_total=len(data.columns),
                allowed_ops=sorted(_ALLOWED_WHERE_OPS),
                dtype_effective=None,
                expected="existing column in trips.data",
                observed=str(field_name),
            )
            omitted = True
            continue

        series = data[str(field_name)]
        dtype_effective = _resolve_field_dtype(trips, str(field_name), series)
        allowed_ops = _allowed_ops_for_dtype(dtype_effective)
        field_mask = pd.Series(True, index=data.index, dtype=bool)
        clause_invalid = False

        # Se combinan los operadores del mismo campo con AND, tal como fija el contrato.
        for op_name, op_value in normalized_clause.items():
            if op_name not in _ALLOWED_WHERE_OPS:
                # Se omite la cláusula porque el operador no pertenece al catálogo soportado.
                emit_issue(
                    issues,
                    FILTER_TRIPS_ISSUES,
                    "FLT.WHERE.OP_UNKNOWN",
                    field=str(field_name),
                    op=str(op_name),
                    clause_shape=clause_shape,
                    op_value=_to_json_serializable_or_none(op_value),
                    op_value_type=type(op_value).__name__,
                    available_fields_sample=_sample_list(list(data.columns), limit=10),
                    available_fields_total=len(data.columns),
                    allowed_ops=sorted(allowed_ops),
                    dtype_effective=dtype_effective,
                    expected="supported operator",
                    observed=str(op_name),
                )
                clause_invalid = True
                break

            if op_name not in allowed_ops:
                # Se omite la cláusula porque el operador no es compatible con el dtype efectivo.
                emit_issue(
                    issues,
                    FILTER_TRIPS_ISSUES,
                    "FLT.WHERE.OP_INCOMPATIBLE",
                    field=str(field_name),
                    op=str(op_name),
                    clause_shape=clause_shape,
                    op_value=_to_json_serializable_or_none(op_value),
                    op_value_type=type(op_value).__name__,
                    available_fields_sample=_sample_list(list(data.columns), limit=10),
                    available_fields_total=len(data.columns),
                    allowed_ops=sorted(allowed_ops),
                    dtype_effective=dtype_effective,
                    expected=f"operator in {sorted(allowed_ops)}",
                    observed=str(op_name),
                )
                clause_invalid = True
                break

            is_valid_value, expected_desc = _validate_where_operator_value(op_name, op_value, dtype_effective)
            if not is_valid_value:
                # Se omite la cláusula porque el valor del operador no respeta la forma esperada.
                emit_issue(
                    issues,
                    FILTER_TRIPS_ISSUES,
                    "FLT.WHERE.INVALID_VALUE_SHAPE",
                    field=str(field_name),
                    op=str(op_name),
                    clause_shape=clause_shape,
                    op_value=_to_json_serializable_or_none(op_value),
                    op_value_type=type(op_value).__name__,
                    available_fields_sample=_sample_list(list(data.columns), limit=10),
                    available_fields_total=len(data.columns),
                    allowed_ops=sorted(allowed_ops),
                    dtype_effective=dtype_effective,
                    expected=expected_desc,
                    observed=repr(op_value)[:200],
                )
                clause_invalid = True
                break

            op_mask = _evaluate_where_operator_mask(series, dtype_effective, op_name, op_value)
            field_mask &= op_mask

        if clause_invalid:
            omitted = True
            continue

        field_masks.append(field_mask)
        applied_fields.append(str(field_name))

    if not field_masks:
        return None, False, omitted

    # Se consolida el eje where y se deja evidencia resumida de su efecto real.
    where_mask = pd.Series(True, index=data.index, dtype=bool)
    for mask in field_masks:
        where_mask &= mask.fillna(False).astype(bool)

    removed_mask = ~where_mask
    removed_evidence = _build_removed_rows_evidence(
        data,
        removed_mask,
        sample_rows_per_issue=sample_rows_per_issue,
        value_fields=applied_fields,
    )

    # Se deja evidencia operacional del eje where aunque no descarte filas.
    emit_issue(
        issues,
        FILTER_TRIPS_ISSUES,
        "FLT.INFO.WHERE_APPLIED",
        where_fields=applied_fields,
        rows_in_scope=len(data),
        rows_out_scope=int(where_mask.sum()),
        rows_removed_scope=int(removed_mask.sum()),
        row_indices_sample_removed=removed_evidence["row_indices_sample_removed"],
        movement_ids_sample_removed=removed_evidence["movement_ids_sample_removed"],
        values_sample=removed_evidence["values_sample"],
        policy="and_between_fields_and_ops",
    )
    return where_mask, True, omitted


def _build_time_mask(
    trips: TripDataset,
    *,
    time: Optional[TimeFilter],
    sample_rows_per_issue: int,
    issues: List[Issue],
) -> Tuple[Optional[pd.Series], bool, bool]:
    """
    Construye la máscara temporal respetando tiers y predicados cerrados en v1.1.

    Emite
    ------
    FLT.TIME.UNSUPPORTED_TIER
    FLT.TIME.MISSING_REQUIRED_COLUMNS
    FLT.INFO.TIME_APPLIED
    """
    if time is None:
        return None, False, False

    data = trips.data
    temporal_meta = trips.metadata.get("temporal") if isinstance(trips.metadata, dict) else None
    temporal_tier = _extract_temporal_tier(temporal_meta, data)
    fields_present = temporal_meta.get("fields_present") if isinstance(temporal_meta, dict) else None

    if temporal_tier != "tier_1":
        # Se omite el filtro temporal porque el dataset no ofrece temporalidad absoluta evaluable.
        emit_issue(
            issues,
            FILTER_TRIPS_ISSUES,
            "FLT.TIME.UNSUPPORTED_TIER",
            predicate=time.predicate,
            start=time.start,
            end=time.end,
            temporal_tier=temporal_tier,
            fields_present=_to_json_serializable_or_none(fields_present),
            missing_fields=list(_TIME_FIELDS),
            expected="tier_1 with origin_time_utc and destination_time_utc",
            observed=temporal_tier,
        )
        return None, False, True

    missing_time_fields = [field_name for field_name in _TIME_FIELDS if field_name not in data.columns]
    if missing_time_fields:
        # Se omite el filtro temporal porque faltan columnas base para el intervalo del viaje.
        emit_issue(
            issues,
            FILTER_TRIPS_ISSUES,
            "FLT.TIME.MISSING_REQUIRED_COLUMNS",
            predicate=time.predicate,
            start=time.start,
            end=time.end,
            temporal_tier=temporal_tier,
            fields_present=_to_json_serializable_or_none(fields_present),
            missing_fields=missing_time_fields,
            expected="origin_time_utc and destination_time_utc",
            observed=_sample_list(list(data.columns), limit=10),
        )
        return None, False, True

    origin_ts = pd.to_datetime(data["origin_time_utc"], errors="coerce", utc=True)
    destination_ts = pd.to_datetime(data["destination_time_utc"], errors="coerce", utc=True)
    start_ts = pd.Timestamp(time.start)
    end_ts = pd.Timestamp(time.end)

    if time.predicate == "starts_within":
        time_mask = origin_ts.ge(start_ts) & origin_ts.lt(end_ts)
    elif time.predicate == "ends_within":
        time_mask = destination_ts.ge(start_ts) & destination_ts.lt(end_ts)
    elif time.predicate == "contains":
        time_mask = origin_ts.ge(start_ts) & destination_ts.le(end_ts)
    else:
        time_mask = origin_ts.lt(end_ts) & destination_ts.gt(start_ts)

    time_mask = time_mask.fillna(False).astype(bool)
    removed_mask = ~time_mask
    removed_evidence = _build_removed_rows_evidence(
        data,
        removed_mask,
        sample_rows_per_issue=sample_rows_per_issue,
        value_fields=["origin_time_utc", "destination_time_utc"],
    )

    # Se registra el efecto del eje temporal sobre todo el dataset de entrada.
    emit_issue(
        issues,
        FILTER_TRIPS_ISSUES,
        "FLT.INFO.TIME_APPLIED",
        predicate=time.predicate,
        start=time.start,
        end=time.end,
        rows_in_scope=len(data),
        rows_out_scope=int(time_mask.sum()),
        rows_removed_scope=int(removed_mask.sum()),
        row_indices_sample_removed=removed_evidence["row_indices_sample_removed"],
        movement_ids_sample_removed=removed_evidence["movement_ids_sample_removed"],
        values_sample=removed_evidence["values_sample"],
        policy="interval_trip_vs_interval_query",
    )
    return time_mask, True, False


def _build_spatial_mask(
    trips: TripDataset,
    *,
    bbox: Optional[BBox],
    polygon: Optional[Polygon],
    h3_cells: Optional[Iterable[str]],
    spatial_predicate: SpatialPredicate,
    origin_h3_field: str,
    destination_h3_field: str,
    sample_rows_per_issue: int,
    issues: List[Issue],
) -> Tuple[Dict[str, Optional[pd.Series]], List[str], List[str]]:
    """
    Construye las máscaras espaciales por subtipo y concentra la evidencia del eje espacial.

    Emite
    ------
    FLT.SPATIAL.MISSING_REQUIRED_COLUMNS
    FLT.SPATIAL.H3_FIELD_MISSING
    FLT.INFO.BBOX_APPLIED
    FLT.INFO.POLYGON_APPLIED
    FLT.INFO.H3_APPLIED
    """
    data = trips.data
    masks: Dict[str, Optional[pd.Series]] = {"bbox": None, "polygon": None, "h3_cells": None}
    applied: List[str] = []
    omitted: List[str] = []

    # Se evalúa bbox como subfiltro espacial independiente.
    if bbox is not None:
        missing_bbox_fields = _required_latlon_fields_for_predicate(spatial_predicate)
        missing_bbox_fields = [field_name for field_name in missing_bbox_fields if field_name not in data.columns]
        if missing_bbox_fields:
            # Se omite bbox porque no existe superficie OD suficiente para evaluarlo.
            emit_issue(
                issues,
                FILTER_TRIPS_ISSUES,
                "FLT.SPATIAL.MISSING_REQUIRED_COLUMNS",
                spatial_filter="bbox",
                spatial_predicate=spatial_predicate,
                missing_fields=missing_bbox_fields,
                available_fields_sample=_sample_list(list(data.columns), limit=10),
                available_fields_total=len(data.columns),
                bbox=list(bbox),
                polygon_n_vertices=None,
                origin_h3_field=origin_h3_field,
                destination_h3_field=destination_h3_field,
                h3_invalid_sample=None,
                h3_invalid_total=None,
                expected="required origin/destination lon-lat columns",
                observed=_sample_list(list(data.columns), limit=10),
            )
            omitted.append("bbox")
        else:
            bbox_mask = _evaluate_spatial_point_predicate(
                data,
                spatial_predicate=spatial_predicate,
                point_predicate=lambda lon, lat: _point_in_bbox(lon, lat, bbox),
            )
            masks["bbox"] = bbox_mask
            removed_mask = ~bbox_mask
            removed_evidence = _build_removed_rows_evidence(
                data,
                removed_mask,
                sample_rows_per_issue=sample_rows_per_issue,
                value_fields=_latlon_value_fields_for_predicate(spatial_predicate),
            )
            # Se deja evidencia resumida del efecto del bbox sobre el dataset completo.
            emit_issue(
                issues,
                FILTER_TRIPS_ISSUES,
                "FLT.INFO.BBOX_APPLIED",
                rows_in_scope=len(data),
                rows_out_scope=int(bbox_mask.sum()),
                rows_removed_scope=int(removed_mask.sum()),
                row_indices_sample_removed=removed_evidence["row_indices_sample_removed"],
                movement_ids_sample_removed=removed_evidence["movement_ids_sample_removed"],
                values_sample=removed_evidence["values_sample"],
                policy="and_between_spatial_filters",
                bbox=list(bbox),
                spatial_predicate=spatial_predicate,
            )
            applied.append("bbox")

    # Se evalúa polygon como subfiltro espacial independiente.
    if polygon is not None:
        missing_polygon_fields = _required_latlon_fields_for_predicate(spatial_predicate)
        missing_polygon_fields = [field_name for field_name in missing_polygon_fields if field_name not in data.columns]
        if missing_polygon_fields:
            # Se omite polygon porque faltan coordenadas requeridas para el predicado espacial.
            emit_issue(
                issues,
                FILTER_TRIPS_ISSUES,
                "FLT.SPATIAL.MISSING_REQUIRED_COLUMNS",
                spatial_filter="polygon",
                spatial_predicate=spatial_predicate,
                missing_fields=missing_polygon_fields,
                available_fields_sample=_sample_list(list(data.columns), limit=10),
                available_fields_total=len(data.columns),
                bbox=None,
                polygon_n_vertices=len(polygon),
                origin_h3_field=origin_h3_field,
                destination_h3_field=destination_h3_field,
                h3_invalid_sample=None,
                h3_invalid_total=None,
                expected="required origin/destination lon-lat columns",
                observed=_sample_list(list(data.columns), limit=10),
            )
            omitted.append("polygon")
        else:
            polygon_mask = _evaluate_spatial_point_predicate(
                data,
                spatial_predicate=spatial_predicate,
                point_predicate=lambda lon, lat: _point_in_polygon(lon, lat, polygon),
            )
            masks["polygon"] = polygon_mask
            removed_mask = ~polygon_mask
            removed_evidence = _build_removed_rows_evidence(
                data,
                removed_mask,
                sample_rows_per_issue=sample_rows_per_issue,
                value_fields=_latlon_value_fields_for_predicate(spatial_predicate),
            )
            # Se deja evidencia resumida del efecto del polígono sobre el dataset completo.
            emit_issue(
                issues,
                FILTER_TRIPS_ISSUES,
                "FLT.INFO.POLYGON_APPLIED",
                rows_in_scope=len(data),
                rows_out_scope=int(polygon_mask.sum()),
                rows_removed_scope=int(removed_mask.sum()),
                row_indices_sample_removed=removed_evidence["row_indices_sample_removed"],
                movement_ids_sample_removed=removed_evidence["movement_ids_sample_removed"],
                values_sample=removed_evidence["values_sample"],
                policy="and_between_spatial_filters",
                polygon_n_vertices=len(polygon),
                spatial_predicate=spatial_predicate,
            )
            applied.append("polygon")

    # Se evalúa h3_cells como subfiltro espacial independiente.
    if h3_cells is not None:
        required_h3_fields = _required_h3_fields_for_predicate(
            spatial_predicate,
            origin_h3_field=origin_h3_field,
            destination_h3_field=destination_h3_field,
        )
        missing_h3_fields = [field_name for field_name in required_h3_fields if field_name not in data.columns]
        if missing_h3_fields:
            # Se omite h3_cells porque faltan los campos H3 declarados en el request.
            emit_issue(
                issues,
                FILTER_TRIPS_ISSUES,
                "FLT.SPATIAL.H3_FIELD_MISSING",
                spatial_filter="h3_cells",
                spatial_predicate=spatial_predicate,
                missing_fields=missing_h3_fields,
                available_fields_sample=_sample_list(list(data.columns), limit=10),
                available_fields_total=len(data.columns),
                bbox=None,
                polygon_n_vertices=None,
                origin_h3_field=origin_h3_field,
                destination_h3_field=destination_h3_field,
                h3_invalid_sample=None,
                h3_invalid_total=None,
                expected="configured H3 fields present in trips.data",
                observed=_sample_list(list(data.columns), limit=10),
            )
            omitted.append("h3_cells")
        else:
            whitelist = set(h3_cells)
            h3_mask = _evaluate_h3_predicate(
                data,
                spatial_predicate=spatial_predicate,
                origin_h3_field=origin_h3_field,
                destination_h3_field=destination_h3_field,
                whitelist=whitelist,
            )
            masks["h3_cells"] = h3_mask
            removed_mask = ~h3_mask
            removed_evidence = _build_removed_rows_evidence(
                data,
                removed_mask,
                sample_rows_per_issue=sample_rows_per_issue,
                value_fields=_h3_value_fields_for_predicate(
                    spatial_predicate,
                    origin_h3_field=origin_h3_field,
                    destination_h3_field=destination_h3_field,
                ),
            )
            # Se deja evidencia resumida del efecto de la whitelist H3 sobre el dataset completo.
            emit_issue(
                issues,
                FILTER_TRIPS_ISSUES,
                "FLT.INFO.H3_APPLIED",
                rows_in_scope=len(data),
                rows_out_scope=int(h3_mask.sum()),
                rows_removed_scope=int(removed_mask.sum()),
                row_indices_sample_removed=removed_evidence["row_indices_sample_removed"],
                movement_ids_sample_removed=removed_evidence["movement_ids_sample_removed"],
                values_sample=removed_evidence["values_sample"],
                policy="and_between_spatial_filters",
                spatial_predicate=spatial_predicate,
                origin_h3_field=origin_h3_field,
                destination_h3_field=destination_h3_field,
                h3_cells_count=len(list(h3_cells)),
                h3_cells_sample=_sample_list(list(h3_cells), limit=10),
            )
            applied.append("h3_cells")

    return masks, applied, omitted


def _combine_filter_masks(
    trips: TripDataset,
    *,
    mask_items: Sequence[Tuple[str, pd.Series]],
    filters_requested: Sequence[str],
    filters_applied: Sequence[str],
    filters_omitted: Sequence[str],
    issues: List[Issue],
) -> Tuple[pd.Series, Dict[str, int], int, int, int]:
    """
    Combina máscaras con AND global y calcula el impacto incremental real del filtrado.

    Emite
    ------
    FLT.INFO.NO_FILTERS_DEFINED
    FLT.INFO.FILTER_WITHOUT_EFFECT
    FLT.OUTPUT.EMPTY_RESULT
    """
    rows_in = len(trips.data)
    survivor_mask = pd.Series(True, index=trips.data.index, dtype=bool)
    dropped_by_filter = {filter_name: 0 for filter_name in _SUMMARY_FILTER_KEYS}

    # Se combinan los ejes aplicados en el orden contractual de la operación.
    for filter_name in _SUMMARY_FILTER_KEYS:
        selected_mask = None
        for current_name, current_mask in mask_items:
            if current_name == filter_name:
                selected_mask = current_mask
                break
        if selected_mask is None:
            continue

        current_index = survivor_mask[survivor_mask].index
        current_mask = selected_mask.loc[current_index].fillna(False).astype(bool)
        dropped_now = int((~current_mask).sum())
        dropped_by_filter[filter_name] = dropped_now
        if dropped_now > 0:
            survivor_mask.loc[current_index[~current_mask]] = False

    rows_out = int(survivor_mask.sum())
    dropped_total = int(rows_in - rows_out)

    if not filters_requested:
        # Se informa explícitamente que la operación retornó un dataset derivado sin filtros activos.
        emit_issue(
            issues,
            FILTER_TRIPS_ISSUES,
            "FLT.INFO.NO_FILTERS_DEFINED",
            rows_in=rows_in,
            rows_out=rows_out,
            dropped_total=dropped_total,
            filters_requested=list(filters_requested),
            filters_applied=list(filters_applied),
            filters_omitted=list(filters_omitted),
            reason="no_filters_defined",
        )
    elif dropped_total == 0:
        # Se informa que los filtros se evaluaron, pero no produjeron cambios efectivos.
        emit_issue(
            issues,
            FILTER_TRIPS_ISSUES,
            "FLT.INFO.FILTER_WITHOUT_EFFECT",
            rows_in=rows_in,
            rows_out=rows_out,
            dropped_total=dropped_total,
            filters_requested=list(filters_requested),
            filters_applied=list(filters_applied),
            filters_omitted=list(filters_omitted),
            reason="no_rows_filtered" if filters_applied else "all_filters_omitted",
        )

    if filters_requested and rows_out == 0:
        # Se deja warning porque el resultado vacío es válido, pero importante de trazar.
        emit_issue(
            issues,
            FILTER_TRIPS_ISSUES,
            "FLT.OUTPUT.EMPTY_RESULT",
            rows_in=rows_in,
            rows_out=rows_out,
            dropped_total=dropped_total,
            filters_requested=list(filters_requested),
            filters_applied=list(filters_applied),
            filters_omitted=list(filters_omitted),
            dropped_by_filter={k: int(v) for k, v in dropped_by_filter.items()},
            reason="empty_result_after_filter",
        )

    return survivor_mask, dropped_by_filter, rows_in, rows_out, dropped_total


def _materialize_filtered_tripdataset(
    trips: TripDataset,
    *,
    mask_survival: pd.Series,
    keep_metadata: bool,
) -> TripDataset:
    """
    Materializa el commit único final y reconstruye el TripDataset derivado.

    Emite
    ------
    No emite issues directamente.
    """
    # Se materializa el subconjunto final una sola vez al final del pipeline.
    data_out = trips.data.loc[mask_survival].copy(deep=True)

    # Se reconstruye metadata según la política cerrada de keep_metadata.
    metadata_out = _build_metadata_out(trips.metadata, keep_metadata=keep_metadata)
    metadata_out["is_validated"] = _extract_validated_flag(trips.metadata)

    return TripDataset(
        data=data_out,
        schema=trips.schema,
        schema_version=trips.schema_version,
        provenance=copy.deepcopy(trips.provenance),
        field_correspondence=copy.deepcopy(getattr(trips, "field_correspondence", {})),
        value_correspondence=copy.deepcopy(getattr(trips, "value_correspondence", {})),
        metadata=metadata_out,
        schema_effective=_clone_schema_effective(getattr(trips, "schema_effective", None)),
    )


# -----------------------------------------------------------------------------
# Helpers internos de uso general
# -----------------------------------------------------------------------------


def _truncate_issues_with_limit(
    issues_all: Sequence[Issue],
    *,
    max_issues: int,
) -> Tuple[List[Issue], Optional[Dict[str, Any]]]:
    """Aplica el límite de issues y agrega el issue final de truncamiento si corresponde."""
    total_detected = len(issues_all)
    if total_detected <= max_issues:
        return list(issues_all), None

    retained = list(issues_all[: max_issues - 1]) if max_issues > 1 else []
    # Se agrega un último issue explícito para que el truncamiento quede visible en el reporte.
    emit_issue(
        retained,
        FILTER_TRIPS_ISSUES,
        "FLT.LIMIT.ISSUES_TRUNCATED",
        max_issues=max_issues,
        n_issues_emitted=max_issues,
        n_issues_detected_total=total_detected,
        action="truncated_report",
    )
    limits = {
        "max_issues": int(max_issues),
        "issues_truncated": True,
        "n_issues_emitted": len(retained),
        "n_issues_detected_total": int(total_detected),
    }
    return retained, limits


def _build_issues_summary(issues: Sequence[Issue]) -> Dict[str, Any]:
    """Resume issues por severidad y código para el evento de metadata."""
    level_counts = Counter(issue.level for issue in issues)
    code_counts = Counter(issue.code for issue in issues)
    return {
        "counts": {
            "info": int(level_counts.get("info", 0)),
            "warning": int(level_counts.get("warning", 0)),
            "error": int(level_counts.get("error", 0)),
        },
        "top_codes": [
            {"code": code, "count": int(count)}
            for code, count in sorted(code_counts.items(), key=lambda item: (-item[1], item[0]))[:10]
        ],
    }


def _normalize_where_for_parameters(where: Mapping[str, Any]) -> Dict[str, Any]:
    """Convierte `where` a una forma serializable sin reinterpretar su semántica recuperable."""
    normalized: Dict[str, Any] = {}
    for field_name, raw_clause in where.items():
        if isinstance(raw_clause, Mapping):
            normalized[str(field_name)] = {
                str(op_name): _to_json_serializable_or_none(op_value)
                for op_name, op_value in raw_clause.items()
            }
        elif isinstance(raw_clause, tuple):
            normalized[str(field_name)] = [_to_json_serializable_or_none(value) for value in raw_clause]
        elif isinstance(raw_clause, list):
            normalized[str(field_name)] = [_to_json_serializable_or_none(value) for value in raw_clause]
        elif isinstance(raw_clause, set):
            normalized[str(field_name)] = [_to_json_serializable_or_none(value) for value in sorted(raw_clause, key=lambda value: repr(value))]
        else:
            normalized[str(field_name)] = _to_json_serializable_or_none(raw_clause)
    return normalized


def _normalize_where_field_clause(raw_clause: Any) -> Tuple[Optional[Dict[str, Any]], str]:
    """Normaliza una cláusula por campo a forma operator->value o retorna None si es ilegible."""
    if isinstance(raw_clause, Mapping):
        return {str(op_name): op_value for op_name, op_value in raw_clause.items()}, "mapping"
    if isinstance(raw_clause, tuple):
        return {"in": list(raw_clause)}, "sequence"
    if isinstance(raw_clause, list):
        return {"in": list(raw_clause)}, "sequence"
    if isinstance(raw_clause, set):
        return None, "set"
    if isinstance(raw_clause, (str, int, float, bool, datetime, pd.Timestamp)) or raw_clause is None:
        return {"eq": raw_clause}, "scalar"
    return None, type(raw_clause).__name__


def _resolve_field_dtype(trips: TripDataset, field_name: str, series: Optional[pd.Series]) -> Optional[str]:
    """Resuelve el dtype lógico con la precedencia schema_effective -> schema -> dtype observado."""
    schema_effective = getattr(trips, "schema_effective", None)
    dtype_effective = getattr(schema_effective, "dtype_effective", None)
    if isinstance(dtype_effective, dict):
        dtype = dtype_effective.get(field_name)
        if isinstance(dtype, str):
            return dtype

    schema_fields = getattr(getattr(trips, "schema", None), "fields", None)
    if isinstance(schema_fields, dict):
        field_spec = schema_fields.get(field_name)
        dtype = getattr(field_spec, "dtype", None) if field_spec is not None else None
        if isinstance(dtype, str):
            return dtype

    if series is not None:
        if ptypes.is_bool_dtype(series):
            return "bool"
        if ptypes.is_datetime64_any_dtype(series):
            return "datetime"
        if ptypes.is_integer_dtype(series):
            return "int"
        if ptypes.is_float_dtype(series):
            return "float"
        if ptypes.is_categorical_dtype(series):
            return "categorical"
    return None


def _allowed_ops_for_dtype(dtype_effective: Optional[str]) -> set[str]:
    """Retorna el conjunto de operadores permitidos para un dtype lógico."""
    if dtype_effective in _ALLOWED_WHERE_OPS_BY_DTYPE:
        return set(_ALLOWED_WHERE_OPS_BY_DTYPE[dtype_effective])
    return {"eq", "ne", "in", "not_in", "is_null", "not_null"}


def _validate_where_operator_value(op_name: str, op_value: Any, dtype_effective: Optional[str]) -> Tuple[bool, str]:
    """Valida la forma esperada del valor según operador y dtype lógico."""
    if op_name in {"eq", "ne"}:
        if dtype_effective == "datetime":
            return isinstance(op_value, str), "datetime eq/ne expects ISO-8601 string"
        return True, "scalar JSON-safe value"

    if op_name in {"in", "not_in"}:
        valid = isinstance(op_value, (list, tuple)) and len(op_value) > 0 and not isinstance(op_value, set)
        return valid, "in/not_in expect non-empty list or tuple"

    if op_name in {"gt", "gte", "lt", "lte"}:
        if dtype_effective == "datetime":
            return isinstance(op_value, str), f"{op_name} expects ISO-8601 string for datetime"
        return isinstance(op_value, (int, float)) and not isinstance(op_value, bool), f"{op_name} expects numeric scalar"

    if op_name == "between":
        if not isinstance(op_value, (list, tuple)) or len(op_value) != 2:
            return False, "between expects list/tuple of length 2"
        low, high = op_value[0], op_value[1]
        if dtype_effective == "datetime":
            return isinstance(low, str) and isinstance(high, str), "between expects (start_iso, end_iso) for datetime"
        valid_numeric = all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in (low, high))
        return valid_numeric, "between expects (min, max) numeric pair"

    if op_name in {"is_null", "not_null"}:
        return op_value is True, f"{op_name} expects literal True"

    return False, "unsupported operator"


def _evaluate_where_operator_mask(series: pd.Series, dtype_effective: Optional[str], op_name: str, op_value: Any) -> pd.Series:
    """Evalúa un operador individual del DSL `where` sobre una serie."""
    if op_name == "eq":
        if dtype_effective == "datetime":
            scalar = pd.Timestamp(op_value)
            return pd.to_datetime(series, errors="coerce", utc=True).eq(scalar)
        return series.eq(op_value)

    if op_name == "ne":
        if dtype_effective == "datetime":
            scalar = pd.Timestamp(op_value)
            return pd.to_datetime(series, errors="coerce", utc=True).ne(scalar)
        return series.ne(op_value)

    if op_name == "in":
        if dtype_effective == "datetime":
            values = [pd.Timestamp(value) for value in list(op_value)]
            return pd.to_datetime(series, errors="coerce", utc=True).isin(values)
        return series.isin(list(op_value))

    if op_name == "not_in":
        if dtype_effective == "datetime":
            values = [pd.Timestamp(value) for value in list(op_value)]
            return ~pd.to_datetime(series, errors="coerce", utc=True).isin(values)
        return ~series.isin(list(op_value))

    if op_name == "is_null":
        return series.isna()

    if op_name == "not_null":
        return series.notna()

    if dtype_effective == "datetime":
        series_dt = pd.to_datetime(series, errors="coerce", utc=True)
        if op_name == "gt":
            return series_dt.gt(pd.Timestamp(op_value))
        if op_name == "gte":
            return series_dt.ge(pd.Timestamp(op_value))
        if op_name == "lt":
            return series_dt.lt(pd.Timestamp(op_value))
        if op_name == "lte":
            return series_dt.le(pd.Timestamp(op_value))
        if op_name == "between":
            low, high = op_value
            low_ts = pd.Timestamp(low)
            high_ts = pd.Timestamp(high)
            return series_dt.ge(low_ts) & series_dt.le(high_ts)

    series_num = pd.to_numeric(series, errors="coerce")
    if op_name == "gt":
        return series_num.gt(op_value)
    if op_name == "gte":
        return series_num.ge(op_value)
    if op_name == "lt":
        return series_num.lt(op_value)
    if op_name == "lte":
        return series_num.le(op_value)
    if op_name == "between":
        low, high = op_value
        return series_num.ge(low) & series_num.le(high)

    return pd.Series(False, index=series.index, dtype=bool)


def _required_latlon_fields_for_predicate(spatial_predicate: str) -> List[str]:
    """Retorna las columnas lon/lat requeridas según el predicado espacial."""
    if spatial_predicate == "origin":
        return list(_ORIGIN_LATLON_FIELDS)
    if spatial_predicate == "destination":
        return list(_DESTINATION_LATLON_FIELDS)
    return list(_ORIGIN_LATLON_FIELDS + _DESTINATION_LATLON_FIELDS)


def _latlon_value_fields_for_predicate(spatial_predicate: str) -> List[str]:
    """Retorna las columnas lon/lat que conviene muestrear como evidencia espacial."""
    if spatial_predicate == "origin":
        return list(_ORIGIN_LATLON_FIELDS)
    if spatial_predicate == "destination":
        return list(_DESTINATION_LATLON_FIELDS)
    return list(_ORIGIN_LATLON_FIELDS + _DESTINATION_LATLON_FIELDS)


def _required_h3_fields_for_predicate(
    spatial_predicate: str,
    *,
    origin_h3_field: str,
    destination_h3_field: str,
) -> List[str]:
    """Retorna las columnas H3 requeridas según el predicado espacial."""
    if spatial_predicate == "origin":
        return [origin_h3_field]
    if spatial_predicate == "destination":
        return [destination_h3_field]
    return [origin_h3_field, destination_h3_field]


def _h3_value_fields_for_predicate(
    spatial_predicate: str,
    *,
    origin_h3_field: str,
    destination_h3_field: str,
) -> List[str]:
    """Retorna las columnas H3 que conviene muestrear como evidencia espacial."""
    if spatial_predicate == "origin":
        return [origin_h3_field]
    if spatial_predicate == "destination":
        return [destination_h3_field]
    return [origin_h3_field, destination_h3_field]


def _evaluate_spatial_point_predicate(
    data: pd.DataFrame,
    *,
    spatial_predicate: str,
    point_predicate,
) -> pd.Series:
    """Evalúa un predicado puntual sobre origen/destino respetando el predicado espacial."""
    origin_mask = pd.Series(False, index=data.index, dtype=bool)
    destination_mask = pd.Series(False, index=data.index, dtype=bool)

    if spatial_predicate in {"origin", "both", "either"}:
        origin_mask = _evaluate_point_predicate_on_fields(
            data,
            lon_field="origin_longitude",
            lat_field="origin_latitude",
            point_predicate=point_predicate,
        )
    if spatial_predicate in {"destination", "both", "either"}:
        destination_mask = _evaluate_point_predicate_on_fields(
            data,
            lon_field="destination_longitude",
            lat_field="destination_latitude",
            point_predicate=point_predicate,
        )

    if spatial_predicate == "origin":
        return origin_mask
    if spatial_predicate == "destination":
        return destination_mask
    if spatial_predicate == "both":
        return origin_mask & destination_mask
    return origin_mask | destination_mask


def _evaluate_point_predicate_on_fields(
    data: pd.DataFrame,
    *,
    lon_field: str,
    lat_field: str,
    point_predicate,
) -> pd.Series:
    """Evalúa un predicado puntual fila a fila sobre un par lon/lat."""
    lon = pd.to_numeric(data[lon_field], errors="coerce")
    lat = pd.to_numeric(data[lat_field], errors="coerce")
    mask_values = []
    for lon_value, lat_value in zip(lon.tolist(), lat.tolist()):
        if pd.isna(lon_value) or pd.isna(lat_value):
            mask_values.append(False)
            continue
        mask_values.append(bool(point_predicate(float(lon_value), float(lat_value))))
    return pd.Series(mask_values, index=data.index, dtype=bool)


def _evaluate_h3_predicate(
    data: pd.DataFrame,
    *,
    spatial_predicate: str,
    origin_h3_field: str,
    destination_h3_field: str,
    whitelist: set[str],
) -> pd.Series:
    """Evalúa pertenencia a whitelist H3 respetando el predicado espacial."""
    origin_mask = pd.Series(False, index=data.index, dtype=bool)
    destination_mask = pd.Series(False, index=data.index, dtype=bool)

    if spatial_predicate in {"origin", "both", "either"}:
        origin_mask = data[origin_h3_field].map(lambda value: _normalize_h3_value(value) in whitelist if _normalize_h3_value(value) is not None else False).fillna(False).astype(bool)
    if spatial_predicate in {"destination", "both", "either"}:
        destination_mask = data[destination_h3_field].map(lambda value: _normalize_h3_value(value) in whitelist if _normalize_h3_value(value) is not None else False).fillna(False).astype(bool)

    if spatial_predicate == "origin":
        return origin_mask
    if spatial_predicate == "destination":
        return destination_mask
    if spatial_predicate == "both":
        return origin_mask & destination_mask
    return origin_mask | destination_mask


def _build_removed_rows_evidence(
    data: pd.DataFrame,
    removed_mask: pd.Series,
    *,
    sample_rows_per_issue: int,
    value_fields: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """Construye una muestra compacta de filas descartadas para `Issue.details`."""
    removed_index = list(data.index[removed_mask])[:sample_rows_per_issue]
    if removed_index:
        removed_frame = data.loc[removed_index]
    else:
        removed_frame = data.iloc[0:0]

    movement_ids = None
    if "movement_id" in removed_frame.columns:
        movement_ids = [_json_safe_scalar(value) for value in removed_frame["movement_id"].tolist()[:sample_rows_per_issue]]

    values_sample = None
    if value_fields:
        available_fields = [field_name for field_name in value_fields if field_name in removed_frame.columns]
        if available_fields:
            values_sample = [
                {
                    str(column_name): _json_safe_scalar(value)
                    for column_name, value in row.items()
                }
                for row in removed_frame[available_fields].head(sample_rows_per_issue).to_dict(orient="records")
            ]

    return {
        "row_indices_sample_removed": [_json_safe_scalar(index_value) for index_value in removed_index],
        "movement_ids_sample_removed": movement_ids,
        "values_sample": values_sample,
    }


def _build_metadata_out(metadata: Any, *, keep_metadata: bool) -> Dict[str, Any]:
    """Reconstruye metadata según la política cerrada de `keep_metadata`."""
    if keep_metadata:
        metadata_out = _clone_metadata(metadata)
        if not isinstance(metadata_out.get("events"), list):
            metadata_out["events"] = []
        return metadata_out

    metadata_out: Dict[str, Any] = {}
    if isinstance(metadata, dict):
        for key_name in ("dataset_id", "is_validated", "temporal", "h3", "schema", "domains_effective"):
            if key_name in metadata:
                metadata_out[key_name] = copy.deepcopy(metadata[key_name])
    return metadata_out


def _clone_metadata(metadata: Any) -> Dict[str, Any]:
    """Copia metadata de forma controlada y garantiza una estructura dict usable."""
    if not isinstance(metadata, dict):
        return {}
    return copy.deepcopy(metadata)


def _clone_schema_effective(schema_effective: Any) -> TripSchemaEffective:
    """Copia `schema_effective` o crea una vista vacía si el input no es interpretable."""
    if isinstance(schema_effective, TripSchemaEffective):
        return copy.deepcopy(schema_effective)
    if isinstance(schema_effective, dict):
        return TripSchemaEffective(
            dtype_effective=copy.deepcopy(schema_effective.get("dtype_effective", {})),
            overrides=copy.deepcopy(schema_effective.get("overrides", {})),
            domains_effective=copy.deepcopy(schema_effective.get("domains_effective", {})),
            temporal=copy.deepcopy(schema_effective.get("temporal", {})),
            fields_effective=copy.deepcopy(schema_effective.get("fields_effective", [])),
        )
    return TripSchemaEffective()


def _extract_validated_flag(metadata: Any) -> bool:
    """Lee el estado validado desde metadata, tolerando trazas antiguas del core."""
    if not isinstance(metadata, dict):
        return False
    if "is_validated" in metadata:
        return bool(metadata.get("is_validated", False))
    flags = metadata.get("flags", {})
    if isinstance(flags, dict):
        return bool(flags.get("validated", False))
    return False


def _extract_temporal_tier(temporal_meta: Any, data: pd.DataFrame) -> str:
    """Extrae el tier temporal desde metadata o lo infiere de forma conservadora desde columnas."""
    if isinstance(temporal_meta, dict):
        tier = temporal_meta.get("tier")
        if isinstance(tier, str) and tier in {"tier_1", "tier_2", "tier_3"}:
            return tier
    if all(field_name in data.columns for field_name in _TIME_FIELDS):
        return "tier_1"
    if all(field_name in data.columns for field_name in ("origin_time_local_hhmm", "destination_time_local_hhmm")):
        return "tier_2"
    return "tier_3"


def _normalize_bbox_or_abort(raw_bbox: Any, issues: List[Issue]) -> BBox:
    """Valida bbox y la retorna como tupla canónica (min_lon, min_lat, max_lon, max_lat)."""
    if isinstance(raw_bbox, (str, bytes)) or not isinstance(raw_bbox, Sequence) or len(raw_bbox) != 4:
        emit_and_maybe_raise(
            issues,
            FILTER_TRIPS_ISSUES,
            "FLT.SPATIAL.INVALID_BBOX",
            strict=False,
            exception_map=EXCEPTION_MAP_FILTER,
            default_exception=_FilterValueError,
            bbox=_to_json_serializable_or_none(raw_bbox),
            observed=repr(raw_bbox)[:200],
        )
    try:
        min_lon, min_lat, max_lon, max_lat = [float(value) for value in raw_bbox]
    except Exception:
        emit_and_maybe_raise(
            issues,
            FILTER_TRIPS_ISSUES,
            "FLT.SPATIAL.INVALID_BBOX",
            strict=False,
            exception_map=EXCEPTION_MAP_FILTER,
            default_exception=_FilterValueError,
            bbox=_to_json_serializable_or_none(raw_bbox),
            observed=repr(raw_bbox)[:200],
        )
    if min_lon > max_lon or min_lat > max_lat:
        emit_and_maybe_raise(
            issues,
            FILTER_TRIPS_ISSUES,
            "FLT.SPATIAL.INVALID_BBOX",
            strict=False,
            exception_map=EXCEPTION_MAP_FILTER,
            default_exception=_FilterValueError,
            bbox=[min_lon, min_lat, max_lon, max_lat],
            observed={"min_lon": min_lon, "min_lat": min_lat, "max_lon": max_lon, "max_lat": max_lat},
        )
    return (min_lon, min_lat, max_lon, max_lat)


def _normalize_polygon_or_abort(raw_polygon: Any, issues: List[Issue]) -> List[Tuple[float, float]]:
    """Valida polygon y lo retorna como lista canónica de vértices (lon, lat)."""
    if isinstance(raw_polygon, (str, bytes)) or not isinstance(raw_polygon, Sequence) or len(raw_polygon) < 3:
        emit_and_maybe_raise(
            issues,
            FILTER_TRIPS_ISSUES,
            "FLT.SPATIAL.INVALID_POLYGON",
            strict=False,
            exception_map=EXCEPTION_MAP_FILTER,
            default_exception=_FilterValueError,
            polygon_n_vertices=len(raw_polygon) if hasattr(raw_polygon, "__len__") else None,
            observed=repr(raw_polygon)[:200],
        )
    vertices: List[Tuple[float, float]] = []
    try:
        for vertex in raw_polygon:
            if isinstance(vertex, (str, bytes)) or not isinstance(vertex, Sequence) or len(vertex) != 2:
                raise ValueError("invalid vertex")
            lon_value, lat_value = float(vertex[0]), float(vertex[1])
            vertices.append((lon_value, lat_value))
    except Exception:
        emit_and_maybe_raise(
            issues,
            FILTER_TRIPS_ISSUES,
            "FLT.SPATIAL.INVALID_POLYGON",
            strict=False,
            exception_map=EXCEPTION_MAP_FILTER,
            default_exception=_FilterValueError,
            polygon_n_vertices=len(raw_polygon) if hasattr(raw_polygon, "__len__") else None,
            observed=repr(raw_polygon)[:200],
        )
    if len(vertices) < 3:
        emit_and_maybe_raise(
            issues,
            FILTER_TRIPS_ISSUES,
            "FLT.SPATIAL.INVALID_POLYGON",
            strict=False,
            exception_map=EXCEPTION_MAP_FILTER,
            default_exception=_FilterValueError,
            polygon_n_vertices=len(vertices),
            observed=repr(raw_polygon)[:200],
        )
    return vertices


def _normalize_h3_cells_or_abort(raw_h3_cells: Any, issues: List[Issue]) -> List[str]:
    """Valida la whitelist H3 y la retorna como lista única preservando orden."""
    if isinstance(raw_h3_cells, (str, bytes)) or not isinstance(raw_h3_cells, Iterable):
        emit_and_maybe_raise(
            issues,
            FILTER_TRIPS_ISSUES,
            "FLT.SPATIAL.INVALID_H3_CELLS",
            strict=False,
            exception_map=EXCEPTION_MAP_FILTER,
            default_exception=_FilterValueError,
            observed=repr(raw_h3_cells)[:200],
            h3_invalid_sample=[repr(raw_h3_cells)[:100]],
            h3_invalid_total=1,
        )
    normalized: List[str] = []
    seen = set()
    invalid_values: List[str] = []
    for raw_value in raw_h3_cells:
        value_norm = _normalize_h3_value(raw_value)
        if value_norm is None or not h3.is_valid_cell(value_norm):
            invalid_values.append(repr(raw_value)[:100])
            continue
        if value_norm in seen:
            continue
        seen.add(value_norm)
        normalized.append(value_norm)
    if invalid_values:
        emit_and_maybe_raise(
            issues,
            FILTER_TRIPS_ISSUES,
            "FLT.SPATIAL.INVALID_H3_CELLS",
            strict=False,
            exception_map=EXCEPTION_MAP_FILTER,
            default_exception=_FilterValueError,
            observed=repr(raw_h3_cells)[:200],
            h3_invalid_sample=invalid_values[:10],
            h3_invalid_total=len(invalid_values),
        )
    return normalized


def _normalize_iso_timestamp_or_abort(
    value: Any,
    issues: List[Issue],
    *,
    code: str,
    predicate: Optional[str] = None,
    value_role: Literal["start", "end"] = "start",
) -> str:
    """Normaliza un timestamp a ISO-8601 UTC o aborta con el code indicado."""
    error_ctx = {
        "predicate": predicate,
        "start": value if value_role == "start" else None,
        "end": value if value_role == "end" else None,
    }
    if not isinstance(value, str):
        emit_and_maybe_raise(
            issues,
            FILTER_TRIPS_ISSUES,
            code,
            strict=False,
            exception_map=EXCEPTION_MAP_FILTER,
            default_exception=_FilterValueError,
            observed=repr(value)[:200],
            expected="ISO-8601 timestamp string",
            **error_ctx,
        )
    timestamp = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(timestamp):
        emit_and_maybe_raise(
            issues,
            FILTER_TRIPS_ISSUES,
            code,
            strict=False,
            exception_map=EXCEPTION_MAP_FILTER,
            default_exception=_FilterValueError,
            observed=repr(value)[:200],
            expected="parseable ISO-8601 timestamp string",
            **error_ctx,
        )
    return timestamp.isoformat().replace("+00:00", "Z")


def _point_in_bbox(lon: float, lat: float, bbox: BBox) -> bool:
    """Chequea pertenencia puntual a un bounding box lon/lat."""
    min_lon, min_lat, max_lon, max_lat = bbox
    return min_lon <= lon <= max_lon and min_lat <= lat <= max_lat


def _point_in_polygon(lon: float, lat: float, polygon: Sequence[Tuple[float, float]]) -> bool:
    """Evalúa pertenencia puntual a un polígono simple usando ray casting."""
    inside = False
    n_vertices = len(polygon)
    x_prev, y_prev = polygon[-1]
    for idx in range(n_vertices):
        x_curr, y_curr = polygon[idx]
        intersects = ((y_curr > lat) != (y_prev > lat)) and (
            lon < (x_prev - x_curr) * (lat - y_curr) / ((y_prev - y_curr) or 1e-12) + x_curr
        )
        if intersects:
            inside = not inside
        x_prev, y_prev = x_curr, y_curr
    return inside


def _normalize_h3_value(value: Any) -> Optional[str]:
    """Normaliza un valor H3 puntual a texto usable o retorna None."""
    if value is None or pd.isna(value):
        return None
    value_text = str(value).strip()
    if value_text == "":
        return None
    return value_text


def _sample_list(values: Sequence[Any], *, limit: int) -> List[Any]:
    """Toma una muestra JSON-safe pequeña de una secuencia para details de issues."""
    return [_json_safe_scalar(value) for value in list(values)[:limit]]


def _json_safe_scalar(value: Any) -> Any:
    """Normaliza un escalar a una forma JSON-friendly estable."""
    if value is None:
        return None
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if pd.isna(value):
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _to_json_serializable_or_none(obj: Any) -> Any:
    """Convierte dict/list anidados a una forma JSON-safe sin fallback silencioso complejo."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return {str(key): _to_json_serializable_or_none(value) for key, value in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_json_serializable_or_none(value) for value in obj]
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    if pd.isna(obj):
        return None
    if _json_is_serializable(obj):
        return obj
    return _json_safe_scalar(obj)


def _json_is_serializable(obj: Any) -> bool:
    """Chequea si un objeto puede serializarse directamente a JSON."""
    try:
        json.dumps(obj)
        return True
    except Exception:
        return False


def _utc_now_iso() -> str:
    """Retorna timestamp UTC ISO-8601 para eventos del módulo."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
