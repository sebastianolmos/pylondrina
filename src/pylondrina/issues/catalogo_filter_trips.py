from __future__ import annotations

from pylondrina.issues.core import IssueSpec


"""
Catálogo de issues para OP-05: filter_trips.

Notas normativas v1.1
---------------------
- Este catálogo cubre tanto abortos fatales de input/config como rutas retornables.
- Los problemas recuperables por eje (where / time / spatial) se modelan como
  issues nivel "error" con fatal=False:
  - strict=False -> issue + omitir regla/eje
  - strict=True -> evidencia primero y luego FilterError
- Los abortos fatales de input/config no dependen de strict.
- `EMPTY_RESULT` es retornable y queda como warning.
- `NO_FILTERS_DEFINED` y `FILTER_WITHOUT_EFFECT` representan casos de
  "sin cambios efectivos".
"""


def _info(code: str, message: str, *, details_keys=(), defaults=None) -> IssueSpec:
    return IssueSpec(
        code=code,
        level="info",
        message_template=message,
        details_keys=tuple(details_keys),
        defaults=defaults or {},
        fatal=False,
        exception=None,
    )


def _warn(code: str, message: str, *, details_keys=(), defaults=None) -> IssueSpec:
    return IssueSpec(
        code=code,
        level="warning",
        message_template=message,
        details_keys=tuple(details_keys),
        defaults=defaults or {},
        fatal=False,
        exception=None,
    )


def _err(
    code: str,
    message: str,
    *,
    details_keys=(),
    defaults=None,
    exception: str = "filter",
    fatal: bool = False,
) -> IssueSpec:
    return IssueSpec(
        code=code,
        level="error",
        message_template=message,
        details_keys=tuple(details_keys),
        defaults=defaults or {},
        fatal=fatal,
        exception=exception,
    )


def _fatal_type(code: str, message: str, *, details_keys=(), defaults=None) -> IssueSpec:
    return IssueSpec(
        code=code,
        level="error",
        message_template=message,
        details_keys=tuple(details_keys),
        defaults=defaults or {},
        fatal=True,
        exception="type",
    )


def _fatal_value(code: str, message: str, *, details_keys=(), defaults=None) -> IssueSpec:
    return IssueSpec(
        code=code,
        level="error",
        message_template=message,
        details_keys=tuple(details_keys),
        defaults=defaults or {},
        fatal=True,
        exception="value",
    )


_COMMON_NO_CHANGES_DETAILS = (
    "rows_in",
    "rows_out",
    "dropped_total",
    "filters_requested",
    "filters_applied",
    "filters_omitted",
    "reason",
)

_COMMON_LIMIT_DETAILS = (
    "max_issues",
    "n_issues_emitted",
    "n_issues_detected_total",
    "action",
)

_COMMON_AXIS_APPLIED_DETAILS = (
    "check",
    "rows_in_scope",
    "rows_out_scope",
    "rows_removed_scope",
    "row_indices_sample_removed",
    "movement_ids_sample_removed",
    "values_sample",
    "policy",
)

_COMMON_WHERE_ERROR_DETAILS = (
    "check",
    "field",
    "op",
    "clause_shape",
    "op_value",
    "op_value_type",
    "available_fields_sample",
    "available_fields_total",
    "allowed_ops",
    "dtype_effective",
    "expected",
    "observed",
    "reason",
    "action",
)

_COMMON_TIME_ERROR_DETAILS = (
    "check",
    "predicate",
    "start",
    "end",
    "temporal_tier",
    "fields_present",
    "missing_fields",
    "expected",
    "observed",
    "reason",
    "action",
)

_COMMON_SPATIAL_ERROR_DETAILS = (
    "check",
    "spatial_filter",
    "spatial_predicate",
    "missing_fields",
    "available_fields_sample",
    "available_fields_total",
    "bbox",
    "polygon_n_vertices",
    "origin_h3_field",
    "destination_h3_field",
    "h3_invalid_sample",
    "h3_invalid_total",
    "expected",
    "observed",
    "reason",
    "action",
)


FILTER_TRIPS_ISSUES: dict[str, IssueSpec] = {
    # ------------------------------------------------------------------
    # CONFIG / INPUT FATAL
    # ------------------------------------------------------------------
    "FLT.CONFIG.INVALID_TRIPS_OBJECT": _fatal_type(
        "FLT.CONFIG.INVALID_TRIPS_OBJECT",
        "filter_trips esperaba un TripDataset, pero recibió {received_type!r}.",
        details_keys=("received_type",),
    ),
    "FLT.CONFIG.MISSING_DATAFRAME": _fatal_type(
        "FLT.CONFIG.MISSING_DATAFRAME",
        "filter_trips requiere `trips.data` como pandas.DataFrame interpretable.",
        details_keys=("received_type", "reason"),
    ),
    "FLT.CONFIG.INVALID_OPTIONS_OBJECT": _fatal_type(
        "FLT.CONFIG.INVALID_OPTIONS_OBJECT",
        "filter_trips esperaba `options` de tipo FilterOptions o None, pero recibió {received_type!r}.",
        details_keys=("received_type",),
    ),
    "FLT.CONFIG.INVALID_MAX_ISSUES": _fatal_value(
        "FLT.CONFIG.INVALID_MAX_ISSUES",
        "max_issues debe ser un entero positivo, pero se recibió {max_issues!r}.",
        details_keys=("max_issues", "expected", "reason"),
        defaults={"expected": "positive int", "reason": "invalid_max_issues"},
    ),
    "FLT.CONFIG.INVALID_SAMPLE_ROWS_PER_ISSUE": _fatal_value(
        "FLT.CONFIG.INVALID_SAMPLE_ROWS_PER_ISSUE",
        "sample_rows_per_issue debe ser un entero positivo, pero se recibió {sample_rows_per_issue!r}.",
        details_keys=("sample_rows_per_issue", "expected", "reason"),
        defaults={"expected": "positive int", "reason": "invalid_sample_rows_per_issue"},
    ),

    # ------------------------------------------------------------------
    # WHERE
    # ------------------------------------------------------------------
    "FLT.WHERE.INVALID_CLAUSE": _fatal_value(
        "FLT.WHERE.INVALID_CLAUSE",
        "La cláusula `where` no es interpretable como mapping campo -> condición.",
        details_keys=("check", "received_type", "observed", "expected", "reason", "action"),
        defaults={
            "check": "where",
            "expected": "mapping[str, scalar|sequence|op_dict]",
            "reason": "where_not_interpretable",
            "action": "abort",
        },
    ),
    "FLT.WHERE.FIELD_NOT_FOUND": _err(
        "FLT.WHERE.FIELD_NOT_FOUND",
        "No se puede aplicar la cláusula where sobre {field!r}: la columna no existe en trips.data.",
        details_keys=_COMMON_WHERE_ERROR_DETAILS,
        defaults={
            "check": "where",
            "reason": "field_not_found",
            "action": "omit_field_clause",
        },
    ),
    "FLT.WHERE.OP_UNKNOWN": _err(
        "FLT.WHERE.OP_UNKNOWN",
        "El operador {op!r} no es soportado para la cláusula where de {field!r}; la regla se omitirá.",
        details_keys=_COMMON_WHERE_ERROR_DETAILS,
        defaults={
            "check": "where",
            "reason": "unknown_operator",
            "action": "omit_field_clause",
        },
    ),
    "FLT.WHERE.OP_INCOMPATIBLE": _err(
        "FLT.WHERE.OP_INCOMPATIBLE",
        "El operador {op!r} no es compatible con el dtype efectivo {dtype_effective!r} de {field!r}; la regla se omitirá.",
        details_keys=_COMMON_WHERE_ERROR_DETAILS,
        defaults={
            "check": "where",
            "reason": "operator_incompatible_with_dtype",
            "action": "omit_field_clause",
        },
    ),
    "FLT.WHERE.INVALID_VALUE_SHAPE": _err(
        "FLT.WHERE.INVALID_VALUE_SHAPE",
        "La forma del valor para {field!r} / operador {op!r} no es válida; la regla se omitirá.",
        details_keys=_COMMON_WHERE_ERROR_DETAILS,
        defaults={
            "check": "where",
            "reason": "invalid_operator_value_shape",
            "action": "omit_field_clause",
        },
    ),
    "FLT.INFO.WHERE_APPLIED": _info(
        "FLT.INFO.WHERE_APPLIED",
        "Se aplicó la cláusula where y el subconjunto pasó de {rows_in_scope} a {rows_out_scope} filas en este eje.",
        details_keys=(
            "check",
            "where_fields",
            "rows_in_scope",
            "rows_out_scope",
            "rows_removed_scope",
            "row_indices_sample_removed",
            "movement_ids_sample_removed",
            "values_sample",
            "policy",
        ),
        defaults={"check": "where"},
    ),

    # ------------------------------------------------------------------
    # TIME
    # ------------------------------------------------------------------
    "FLT.TIME.INVALID_FILTER": _fatal_value(
        "FLT.TIME.INVALID_FILTER",
        "La configuración de `time` no es interpretable como TimeFilter válido.",
        details_keys=("check", "received_type", "observed", "expected", "reason", "action"),
        defaults={
            "check": "time",
            "expected": "TimeFilter(start: str, end: str, predicate: str)",
            "reason": "time_not_interpretable",
            "action": "abort",
        },
    ),
    "FLT.TIME.INVALID_RANGE": _fatal_value(
        "FLT.TIME.INVALID_RANGE",
        "El rango temporal solicitado no es válido o no se puede interpretar (start={start!r}, end={end!r}, predicate={predicate!r}).",
        details_keys=("check", "predicate", "start", "end", "expected", "observed", "reason", "action"),
        defaults={
            "check": "time",
            "reason": "invalid_time_range",
            "action": "abort",
        },
    ),
    "FLT.TIME.UNSUPPORTED_TIER": _err(
        "FLT.TIME.UNSUPPORTED_TIER",
        "No se puede aplicar el filtro temporal: el dataset está en {temporal_tier!r} y OP-05 solo evalúa Tier 1 absoluto; la regla se omitirá.",
        details_keys=_COMMON_TIME_ERROR_DETAILS,
        defaults={
            "check": "time",
            "action": "omit_time_filter",
            "reason": "unsupported_temporal_tier",
        },
    ),
    "FLT.TIME.MISSING_REQUIRED_COLUMNS": _err(
        "FLT.TIME.MISSING_REQUIRED_COLUMNS",
        "No se puede aplicar el filtro temporal: faltan columnas temporales requeridas (missing={missing_fields!r}); la regla se omitirá.",
        details_keys=_COMMON_TIME_ERROR_DETAILS,
        defaults={
            "check": "time",
            "action": "omit_time_filter",
            "reason": "missing_required_time_columns",
        },
    ),
    "FLT.INFO.TIME_APPLIED": _info(
        "FLT.INFO.TIME_APPLIED",
        "Se aplicó el filtro temporal ({predicate!r}) y el subconjunto pasó de {rows_in_scope} a {rows_out_scope} filas en este eje.",
        details_keys=_COMMON_AXIS_APPLIED_DETAILS + (
            "predicate",
            "start",
            "end",
        ),
        defaults={"check": "time"},
    ),

    # ------------------------------------------------------------------
    # SPATIAL
    # ------------------------------------------------------------------
    "FLT.SPATIAL.INVALID_PREDICATE": _fatal_value(
        "FLT.SPATIAL.INVALID_PREDICATE",
        "spatial_predicate={spatial_predicate!r} no es válido para OP-05.",
        details_keys=("check", "spatial_predicate", "expected", "reason", "action"),
        defaults={
            "check": "spatial",
            "expected": ["origin", "destination", "both", "either"],
            "reason": "invalid_spatial_predicate",
            "action": "abort",
        },
    ),
    "FLT.SPATIAL.INVALID_BBOX": _fatal_value(
        "FLT.SPATIAL.INVALID_BBOX",
        "La geometría bbox no es interpretable como (min_lon, min_lat, max_lon, max_lat).",
        details_keys=("check", "bbox", "observed", "expected", "reason", "action"),
        defaults={
            "check": "spatial",
            "expected": "tuple/list[float, float, float, float] with min<=max",
            "reason": "invalid_bbox",
            "action": "abort",
        },
    ),
    "FLT.SPATIAL.INVALID_POLYGON": _fatal_value(
        "FLT.SPATIAL.INVALID_POLYGON",
        "La geometría polygon no es interpretable como una secuencia válida de vértices lon/lat.",
        details_keys=("check", "polygon_n_vertices", "observed", "expected", "reason", "action"),
        defaults={
            "check": "spatial",
            "expected": "sequence[tuple[lon, lat]] with at least 3 vertices",
            "reason": "invalid_polygon",
            "action": "abort",
        },
    ),
    "FLT.SPATIAL.INVALID_H3_CELLS": _fatal_value(
        "FLT.SPATIAL.INVALID_H3_CELLS",
        "La whitelist `h3_cells` contiene valores H3 inválidos o no interpretables.",
        details_keys=(
            "check",
            "h3_invalid_sample",
            "h3_invalid_total",
            "observed",
            "expected",
            "reason",
            "action",
        ),
        defaults={
            "check": "spatial",
            "expected": "iterable[str] de índices H3 válidos",
            "reason": "invalid_h3_cells",
            "action": "abort",
        },
    ),
    "FLT.SPATIAL.MISSING_REQUIRED_COLUMNS": _err(
        "FLT.SPATIAL.MISSING_REQUIRED_COLUMNS",
        "No se puede aplicar el filtro espacial {spatial_filter!r}: faltan columnas requeridas (missing={missing_fields!r}); la regla se omitirá.",
        details_keys=_COMMON_SPATIAL_ERROR_DETAILS,
        defaults={
            "check": "spatial",
            "action": "omit_spatial_filter",
            "reason": "missing_required_spatial_columns",
        },
    ),
    "FLT.SPATIAL.H3_FIELD_MISSING": _err(
        "FLT.SPATIAL.H3_FIELD_MISSING",
        "No se puede aplicar h3_cells: faltan los campos H3 declarados explícitamente (missing={missing_fields!r}); la regla se omitirá.",
        details_keys=_COMMON_SPATIAL_ERROR_DETAILS,
        defaults={
            "check": "spatial",
            "spatial_filter": "h3_cells",
            "action": "omit_h3_filter",
            "reason": "declared_h3_field_missing",
        },
    ),
    "FLT.INFO.BBOX_APPLIED": _info(
        "FLT.INFO.BBOX_APPLIED",
        "Se aplicó el filtro bbox y el subconjunto pasó de {rows_in_scope} a {rows_out_scope} filas en este eje.",
        details_keys=_COMMON_AXIS_APPLIED_DETAILS + (
            "bbox",
            "spatial_predicate",
        ),
        defaults={"check": "spatial"},
    ),
    "FLT.INFO.POLYGON_APPLIED": _info(
        "FLT.INFO.POLYGON_APPLIED",
        "Se aplicó el filtro polygon y el subconjunto pasó de {rows_in_scope} a {rows_out_scope} filas en este eje.",
        details_keys=_COMMON_AXIS_APPLIED_DETAILS + (
            "polygon_n_vertices",
            "spatial_predicate",
        ),
        defaults={"check": "spatial"},
    ),
    "FLT.INFO.H3_APPLIED": _info(
        "FLT.INFO.H3_APPLIED",
        "Se aplicó el filtro h3_cells y el subconjunto pasó de {rows_in_scope} a {rows_out_scope} filas en este eje.",
        details_keys=_COMMON_AXIS_APPLIED_DETAILS + (
            "spatial_predicate",
            "origin_h3_field",
            "destination_h3_field",
            "h3_cells_count",
            "h3_cells_sample",
        ),
        defaults={"check": "spatial"},
    ),

    # ------------------------------------------------------------------
    # OUTPUT / LIMIT
    # ------------------------------------------------------------------
    "FLT.INFO.NO_FILTERS_DEFINED": _info(
        "FLT.INFO.NO_FILTERS_DEFINED",
        "No se definieron filtros activos; la operación retorna un nuevo dataset sin cambios efectivos.",
        details_keys=_COMMON_NO_CHANGES_DETAILS,
        defaults={"reason": "no_filters_defined"},
    ),
    "FLT.INFO.FILTER_WITHOUT_EFFECT": _info(
        "FLT.INFO.FILTER_WITHOUT_EFFECT",
        "Los filtros evaluados no produjeron cambios efectivos: no se descartaron filas.",
        details_keys=_COMMON_NO_CHANGES_DETAILS,
        defaults={"reason": "no_rows_filtered"},
    ),
    "FLT.OUTPUT.EMPTY_RESULT": _warn(
        "FLT.OUTPUT.EMPTY_RESULT",
        "El filtrado produjo un dataset vacío.",
        details_keys=(
            "rows_in",
            "rows_out",
            "dropped_total",
            "filters_requested",
            "filters_applied",
            "filters_omitted",
            "dropped_by_filter",
            "reason",
        ),
        defaults={"reason": "empty_result_after_filter"},
    ),
    "FLT.LIMIT.ISSUES_TRUNCATED": _warn(
        "FLT.LIMIT.ISSUES_TRUNCATED",
        "El reporte alcanzó max_issues={max_issues}; los hallazgos adicionales fueron truncados.",
        details_keys=_COMMON_LIMIT_DETAILS,
        defaults={"action": "truncated_report"},
    ),
}