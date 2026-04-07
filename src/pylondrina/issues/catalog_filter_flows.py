from __future__ import annotations

from pylondrina.issues.core import IssueSpec


"""
Catálogo de issues para OP-12: filter_flows.

Notas normativas v1.1
---------------------
- Este catálogo sigue el cierre concreto vigente de OP-12 y no el contrato viejo
  anclado a `origin_h3` / `destination_h3` / `count`.
- El prefijo oficial de codes para esta operación queda en `FLT_FLOW.*`.
- OP-12 es drop-only, en memoria, retorna `FlowDataset + OperationReport`,
  no muta el input y preserva `metadata["is_validated"]` en toda ruta retornable.
- Los errores fatales de input/configuración no dependen de `strict`.
- Los errores recuperables por eje (`where`, `h3_cells`, `flow_to_trips`)
  se emiten con `level="error"` pero `fatal=False`; con `strict=True` la operación
  debe construir evidencia, registrar evento si corresponde, y luego escalar con
  `FilterError`.
- `max_issues` es guardarraíl; el truncamiento se modela con un issue explícito.
- La evidencia rica debe vivir en `Issue.details` y/o en el evento, no en un
  summary inflado.
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


def _fatal_filter(code: str, message: str, *, details_keys=(), defaults=None) -> IssueSpec:
    return _err(
        code,
        message,
        details_keys=details_keys,
        defaults=defaults,
        exception="filter",
        fatal=True,
    )


_REQUEST_DETAILS = (
    "dataset_id",
    "artifact_id",
    "strict",
    "where_provided",
    "h3_cells_provided",
    "spatial_predicate",
    "keep_flow_to_trips",
    "keep_metadata",
    "max_issues",
)

_RESULT_DETAILS = _REQUEST_DETAILS + (
    "n_flows_in",
    "n_flows_out",
    "rows_in",
    "rows_out",
    "dropped_total",
)

_WHERE_RULE_DETAILS = _RESULT_DETAILS + (
    "field",
    "operator",
    "value",
    "value_repr",
    "expected_type",
    "dtype_observed",
    "available_fields_sample",
    "available_fields_total",
    "reason",
    "action",
)

_H3_AXIS_DETAILS = _RESULT_DETAILS + (
    "invalid_cells_sample",
    "invalid_cells_count",
    "valid_cells_count",
    "missing_fields",
    "reason",
    "action",
)

_AUX_DETAILS = _RESULT_DETAILS + (
    "flow_to_trips_status",
    "flow_to_trips_rows_in",
    "flow_to_trips_rows_out",
    "missing_fields",
    "reason",
    "action",
)

_LIMIT_DETAILS = (
    "max_issues",
    "n_issues_detected_total",
    "n_issues_emitted",
    "issues_truncated",
    "reason",
    "action",
)

_NO_CHANGES_DETAILS = _RESULT_DETAILS + (
    "filters_requested",
    "filters_applied",
    "filters_omitted",
    "reason",
)

_APPLIED_AXIS_DETAILS = _RESULT_DETAILS + (
    "filters_requested",
    "filters_applied",
    "filters_omitted",
    "dropped_by_filter",
    "flow_id_sample_removed",
    "rows_sample_removed",
    "reason",
    "action",
)


FILTER_FLOWS_ISSUES: dict[str, IssueSpec] = {
    # ------------------------------------------------------------------
    # CONFIG / INPUT / CONTRATO
    # ------------------------------------------------------------------
    "FLT_FLOW.CONFIG.INVALID_FLOWS_OBJECT": _fatal_filter(
        "FLT_FLOW.CONFIG.INVALID_FLOWS_OBJECT",
        "filter_flows esperaba un FlowDataset interpretable, pero recibió {received_type!r}.",
        details_keys=("received_type", "reason", "action"),
        defaults={"reason": "invalid_flows_object", "action": "abort"},
    ),
    "FLT_FLOW.CONFIG.MISSING_FLOWS_DATAFRAME": _fatal_filter(
        "FLT_FLOW.CONFIG.MISSING_FLOWS_DATAFRAME",
        "filter_flows requiere `flows.flows` como pandas.DataFrame interpretable.",
        details_keys=("received_type", "reason", "action"),
        defaults={"reason": "missing_or_invalid_flows_dataframe", "action": "abort"},
    ),
    "FLT_FLOW.CONTRACT.MISSING_CANONICAL_COLUMNS": _fatal_filter(
        "FLT_FLOW.CONTRACT.MISSING_CANONICAL_COLUMNS",
        "FlowDataset.flows no cumple el contrato interno canónico mínimo requerido por OP-12; faltan columnas obligatorias.",
        details_keys=(
            "missing_columns",
            "available_columns_sample",
            "available_columns_total",
            "reason",
            "action",
        ),
        defaults={"reason": "missing_canonical_columns", "action": "abort"},
    ),
    "FLT_FLOW.CONFIG.INVALID_OPTIONS_OBJECT": _fatal_filter(
        "FLT_FLOW.CONFIG.INVALID_OPTIONS_OBJECT",
        "filter_flows esperaba `options` de tipo FlowFilterOptions o None, pero recibió {received_type!r}.",
        details_keys=("received_type", "reason", "action"),
        defaults={"reason": "invalid_options_object", "action": "abort"},
    ),
    "FLT_FLOW.CONFIG.INVALID_MAX_ISSUES": _fatal_filter(
        "FLT_FLOW.CONFIG.INVALID_MAX_ISSUES",
        "`max_issues` debe ser un entero positivo, pero se recibió {max_issues!r}.",
        details_keys=("max_issues", "reason", "action"),
        defaults={"reason": "invalid_max_issues", "action": "abort"},
    ),
    "FLT_FLOW.CONFIG.WHERE_NOT_INTERPRETABLE": _fatal_filter(
        "FLT_FLOW.CONFIG.WHERE_NOT_INTERPRETABLE",
        "La estructura top-level de `where` no es interpretable para OP-12.",
        details_keys=_REQUEST_DETAILS + (
            "where_repr",
            "received_type",
            "reason",
            "action",
        ),
        defaults={"reason": "where_not_interpretable", "action": "abort"},
    ),
    "FLT_FLOW.CONFIG.INVALID_H3_CELLS": _fatal_filter(
        "FLT_FLOW.CONFIG.INVALID_H3_CELLS",
        "`h3_cells` no es interpretable como iterable de celdas H3 saneables para OP-12.",
        details_keys=_REQUEST_DETAILS + (
            "received_type",
            "invalid_items_sample",
            "reason",
            "action",
        ),
        defaults={"reason": "invalid_h3_cells_container", "action": "abort"},
    ),
    "FLT_FLOW.CONFIG.H3_CELLS_EMPTY_AFTER_NORMALIZATION": _fatal_filter(
        "FLT_FLOW.CONFIG.H3_CELLS_EMPTY_AFTER_NORMALIZATION",
        "`h3_cells` quedó vacío tras normalización/deduplicación; OP-12 no puede construir un eje espacial coherente.",
        details_keys=_REQUEST_DETAILS + (
            "h3_cells_count_raw",
            "h3_cells_count_normalized",
            "reason",
            "action",
        ),
        defaults={"reason": "h3_cells_empty_after_normalization", "action": "abort"},
    ),
    "FLT_FLOW.CONFIG.INVALID_SPATIAL_PREDICATE": _fatal_filter(
        "FLT_FLOW.CONFIG.INVALID_SPATIAL_PREDICATE",
        "`spatial_predicate={spatial_predicate!r}` no es válido para OP-12.",
        details_keys=_REQUEST_DETAILS + (
            "expected",
            "reason",
            "action",
        ),
        defaults={
            "expected": ("origin", "destination", "both", "either"),
            "reason": "invalid_spatial_predicate",
            "action": "abort",
        },
    ),
    "FLT_FLOW.CONFIG.NON_SERIALIZABLE_PARAMETER": _fatal_filter(
        "FLT_FLOW.CONFIG.NON_SERIALIZABLE_PARAMETER",
        "La configuración efectiva de filter_flows contiene un parámetro no serializable en {option_name!r}.",
        details_keys=_REQUEST_DETAILS + (
            "option_name",
            "value_repr",
            "reason",
            "action",
        ),
        defaults={"reason": "non_serializable_parameter", "action": "abort"},
    ),

    # ------------------------------------------------------------------
    # WHERE
    # ------------------------------------------------------------------
    "FLT_FLOW.WHERE.APPLIED": _info(
        "FLT_FLOW.WHERE.APPLIED",
        "Se aplicó el eje `where` sobre FlowDataset.flows.",
        details_keys=_APPLIED_AXIS_DETAILS + (
            "fields_evaluated",
            "rules_evaluated",
        ),
        defaults={"reason": "where_applied", "action": "apply_where_axis"},
    ),
    "FLT_FLOW.WHERE.FIELD_MISSING": _err(
        "FLT_FLOW.WHERE.FIELD_MISSING",
        "El eje `where` no puede evaluar el campo {field!r} porque no existe en FlowDataset.flows; esa regla se omitirá.",
        details_keys=_WHERE_RULE_DETAILS,
        defaults={"reason": "field_missing", "action": "omit_where_rule"},
    ),
    "FLT_FLOW.WHERE.OPERATOR_UNKNOWN": _err(
        "FLT_FLOW.WHERE.OPERATOR_UNKNOWN",
        "El eje `where` recibió un operador no soportado para el campo {field!r}: {operator!r}; esa regla se omitirá.",
        details_keys=_WHERE_RULE_DETAILS,
        defaults={"reason": "operator_unknown", "action": "omit_where_rule"},
    ),
    "FLT_FLOW.WHERE.OPERATOR_INCOMPATIBLE": _err(
        "FLT_FLOW.WHERE.OPERATOR_INCOMPATIBLE",
        "El operador {operator!r} no es compatible con el campo {field!r} bajo el contrato vigente de OP-12; esa regla se omitirá.",
        details_keys=_WHERE_RULE_DETAILS,
        defaults={"reason": "operator_incompatible_with_field", "action": "omit_where_rule"},
    ),
    "FLT_FLOW.WHERE.INVALID_VALUE_SHAPE": _err(
        "FLT_FLOW.WHERE.INVALID_VALUE_SHAPE",
        "El valor entregado para {field!r} con operador {operator!r} no tiene una forma interpretable; esa regla se omitirá.",
        details_keys=_WHERE_RULE_DETAILS + (
            "expected_shape",
        ),
        defaults={"reason": "invalid_value_shape", "action": "omit_where_rule"},
    ),
    "FLT_FLOW.WHERE.EMPTY_SEQUENCE": _err(
        "FLT_FLOW.WHERE.EMPTY_SEQUENCE",
        "El eje `where` recibió una secuencia vacía para {field!r}; la regla no es interpretable y se omitirá.",
        details_keys=_WHERE_RULE_DETAILS,
        defaults={"reason": "empty_sequence_not_allowed", "action": "omit_where_rule"},
    ),
    "FLT_FLOW.WHERE.DATETIME_PARSE_FAILED": _err(
        "FLT_FLOW.WHERE.DATETIME_PARSE_FAILED",
        "No fue posible interpretar el valor temporal entregado para {field!r} en el eje `where`; esa regla se omitirá.",
        details_keys=_WHERE_RULE_DETAILS + (
            "parse_error",
        ),
        defaults={"reason": "datetime_parse_failed", "action": "omit_where_rule"},
    ),

    # ------------------------------------------------------------------
    # H3
    # ------------------------------------------------------------------
    "FLT_FLOW.H3.APPLIED": _info(
        "FLT_FLOW.H3.APPLIED",
        "Se aplicó el eje `h3_cells` sobre `origin_h3_index` / `destination_h3_index`.",
        details_keys=_APPLIED_AXIS_DETAILS + (
            "h3_cells_count",
        ),
        defaults={"reason": "h3_axis_applied", "action": "apply_h3_axis"},
    ),
    "FLT_FLOW.H3.INVALID_CELL_VALUES": _err(
        "FLT_FLOW.H3.INVALID_CELL_VALUES",
        "El eje `h3_cells` contiene valores H3 inválidos; se omitirán esas celdas y el filtro continuará solo con las válidas.",
        details_keys=_H3_AXIS_DETAILS,
        defaults={"reason": "invalid_h3_cell_values", "action": "drop_invalid_h3_cells"},
    ),
    "FLT_FLOW.H3.COLUMNS_MISSING": _err(
        "FLT_FLOW.H3.COLUMNS_MISSING",
        "No se puede evaluar el eje `h3_cells` porque faltan columnas H3 requeridas en FlowDataset.flows; el eje se omitirá.",
        details_keys=_H3_AXIS_DETAILS,
        defaults={"reason": "missing_required_h3_columns", "action": "omit_h3_axis"},
    ),

    # ------------------------------------------------------------------
    # AUXILIAR flow_to_trips
    # ------------------------------------------------------------------
    "FLT_FLOW.AUX.FLOW_TO_TRIPS_SYNCED": _info(
        "FLT_FLOW.AUX.FLOW_TO_TRIPS_SYNCED",
        "Se filtró `flow_to_trips` para mantener consistencia con los `flow_id` retenidos.",
        details_keys=_AUX_DETAILS,
        defaults={"reason": "flow_to_trips_synced", "action": "filter_auxiliary"},
    ),
    "FLT_FLOW.AUX.FLOW_TO_TRIPS_REQUESTED_BUT_MISSING": _info(
        "FLT_FLOW.AUX.FLOW_TO_TRIPS_REQUESTED_BUT_MISSING",
        "Se solicitó conservar `flow_to_trips`, pero el auxiliar no está presente; el resultado se devolverá con `flow_to_trips=None`.",
        details_keys=_AUX_DETAILS,
        defaults={"reason": "flow_to_trips_missing", "action": "set_auxiliary_none"},
    ),
    "FLT_FLOW.AUX.FLOW_TO_TRIPS_INVALID": _err(
        "FLT_FLOW.AUX.FLOW_TO_TRIPS_INVALID",
        "`flow_to_trips` existe, pero no es utilizable para sincronización; el resultado se devolverá con `flow_to_trips=None`.",
        details_keys=_AUX_DETAILS,
        defaults={"reason": "invalid_flow_to_trips_structure", "action": "discard_auxiliary"},
    ),

    # ------------------------------------------------------------------
    # NO CHANGES / RESULTADO
    # ------------------------------------------------------------------
    "FLT_FLOW.NO_CHANGES.NO_FILTERS_DEFINED": _info(
        "FLT_FLOW.NO_CHANGES.NO_FILTERS_DEFINED",
        "No se definieron filtros efectivos; OP-12 finalizó sin cambios efectivos.",
        details_keys=_NO_CHANGES_DETAILS,
        defaults={"reason": "no_filters_defined"},
    ),
    "FLT_FLOW.NO_CHANGES.FILTER_WITHOUT_EFFECT": _info(
        "FLT_FLOW.NO_CHANGES.FILTER_WITHOUT_EFFECT",
        "Se definieron filtros efectivos, pero no produjeron cambios sobre el dataset de flujos.",
        details_keys=_NO_CHANGES_DETAILS,
        defaults={"reason": "filters_without_effect"},
    ),
    "FLT_FLOW.RESULT.EMPTY_DATASET": _warn(
        "FLT_FLOW.RESULT.EMPTY_DATASET",
        "El filtrado produjo un FlowDataset vacío.",
        details_keys=_RESULT_DETAILS + (
            "filters_requested",
            "filters_applied",
            "filters_omitted",
            "dropped_by_filter",
            "flow_to_trips_status",
            "reason",
        ),
        defaults={"reason": "empty_dataset_after_filter"},
    ),

    # ------------------------------------------------------------------
    # REPORT / EVENTO / LÍMITES
    # ------------------------------------------------------------------
    "FLT_FLOW.REPORT.ISSUES_TRUNCATED": _warn(
        "FLT_FLOW.REPORT.ISSUES_TRUNCATED",
        "La lista de issues de filter_flows fue truncada por `max_issues={max_issues!r}`.",
        details_keys=_LIMIT_DETAILS,
        defaults={
            "issues_truncated": True,
            "reason": "max_issues_reached",
            "action": "truncate_issues",
        },
    ),
    "FLT_FLOW.EVENT.APPEND_FAILED": _warn(
        "FLT_FLOW.EVENT.APPEND_FAILED",
        "No fue posible anexar el evento `filter_flows` en `metadata['events']`; el dataset filtrado se devolvió igualmente.",
        details_keys=_REQUEST_DETAILS + (
            "reason",
            "recovered",
            "recovery_action",
        ),
        defaults={
            "reason": "event_append_failed",
            "recovered": True,
            "recovery_action": "return_dataset_without_event_append",
        },
    ),
}