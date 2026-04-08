from __future__ import annotations

from pylondrina.issues.core import IssueSpec


"""
Catálogo de issues para OP-13: get_trips_from_flows.

Notas normativas v1.1
---------------------
- OP-13 es una query pura: retorna `pd.DataFrame + OperationReport`.
- No crea datasets derivados, no registra eventos y no modifica metadata.
- No usa `strict` ni `*Options`; `max_issues` actúa solo como guardarraíl.
- El contrato mínimo de salida es `flow_id + movement_id`; `trip_id` es opcional.
- La evidencia rica debe ir en `Issue.details`, no en un summary inflado.
- Los fatales cubren solo request no interpretable o reconstrucción no
  garantizable.
- Los warnings cubren degradaciones controladas, normalización de duplicados,
  cobertura parcial, resultado vacío y truncamiento.
"""

# ----------------------------------------------------------------------
# Helpers de construcción
# ----------------------------------------------------------------------


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
    exception: str | None = None,
    fatal: bool = True,
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
    return _err(
        code,
        message,
        details_keys=details_keys,
        defaults=defaults,
        exception="type",
        fatal=True,
    )


def _fatal_value(code: str, message: str, *, details_keys=(), defaults=None) -> IssueSpec:
    return _err(
        code,
        message,
        details_keys=details_keys,
        defaults=defaults,
        exception="value",
        fatal=True,
    )


def _fatal(code: str, message: str, *, details_keys=(), defaults=None) -> IssueSpec:
    return _err(
        code,
        message,
        details_keys=details_keys,
        defaults=defaults,
        exception=None,
        fatal=True,
    )


# ----------------------------------------------------------------------
# Bloques de details reutilizables
# ----------------------------------------------------------------------

_REQUEST_DETAILS = (
    "max_issues",
    "n_flows_input",
    "n_trips_input",
    "used_source",
    "reconstruction_attempted",
)

_INPUT_DETAILS = (
    "received_type",
    "has_flows_attr",
    "flows_attr_type",
    "reason",
)

_FLOW_FIELD_DETAILS = _REQUEST_DETAILS + (
    "field",
    "available_fields_sample",
    "available_fields_total",
    "reason",
    "action",
)

_SOURCE_DETAILS = _REQUEST_DETAILS + (
    "preferred_source",
    "checked_sources",
    "source_failures",
    "required_columns",
    "missing_columns",
    "reason",
    "action",
)

_DUPLICATE_DETAILS = _REQUEST_DETAILS + (
    "source",
    "n_rows_in",
    "n_rows_out",
    "n_duplicate_pairs",
    "reason",
    "action",
)

_RECON_FIELD_DETAILS = _REQUEST_DETAILS + (
    "source",
    "required_columns",
    "missing_columns",
    "join_key_columns",
    "group_by",
    "window_columns",
    "reason",
    "action",
)

_AGG_KEYS_DETAILS = _REQUEST_DETAILS + (
    "source",
    "aggregation_spec_keys_present",
    "group_by",
    "window_columns",
    "join_key_columns",
    "reason",
    "action",
)

_OUTPUT_COVERAGE_DETAILS = _REQUEST_DETAILS + (
    "source",
    "join_key_columns",
    "group_by",
    "window_columns",
    "n_rows_out",
    "n_unmatched_movements",
    "n_unmatched_flows",
    "example_values",
    "reason",
    "action",
)

_LIMIT_DETAILS = (
    "max_issues",
    "n_issues_emitted",
    "n_issues_detected_total",
    "issues_truncated",
    "action",
)


# ----------------------------------------------------------------------
# Catálogo principal
# ----------------------------------------------------------------------

GET_TRIPS_FROM_FLOWS_ISSUES: dict[str, IssueSpec] = {
    # ------------------------------------------------------------------
    # CONFIG / PREFLIGHT FATAL
    # ------------------------------------------------------------------
    "GET_TRIPS_FROM_FLOWS.CONFIG.INVALID_FLOWS_INPUT": _fatal_type(
        "GET_TRIPS_FROM_FLOWS.CONFIG.INVALID_FLOWS_INPUT",
        "get_trips_from_flows esperaba un FlowDataset interpretable con `flows.flows` tipo DataFrame, pero recibió {received_type!r}.",
        details_keys=_INPUT_DETAILS,
        defaults={
            "reason": "invalid_flows_input",
        },
    ),
    "GET_TRIPS_FROM_FLOWS.PARAM.INVALID_MAX_ISSUES": _fatal_value(
        "GET_TRIPS_FROM_FLOWS.PARAM.INVALID_MAX_ISSUES",
        "El parámetro {field!r} es inválido: se esperaba un entero positivo y se recibió {value!r}.",
        details_keys=("field", "value", "expected", "reason"),
        defaults={
            "field": "max_issues",
            "expected": "positive_int",
            "reason": "non_positive_or_non_int",
        },
    ),
    "GET_TRIPS_FROM_FLOWS.DATA.MISSING_FLOW_ID": _fatal(
        "GET_TRIPS_FROM_FLOWS.DATA.MISSING_FLOW_ID",
        "El FlowDataset no contiene la columna requerida {field!r} en `flows.flows`.",
        details_keys=_FLOW_FIELD_DETAILS,
        defaults={
            "field": "flow_id",
            "reason": "missing_required_column",
            "action": "abort",
        },
    ),
    # ------------------------------------------------------------------
    # RESOLUCIÓN DE FUENTE
    # ------------------------------------------------------------------
    "GET_TRIPS_FROM_FLOWS.SOURCE.NO_USABLE_SOURCE": _fatal(
        "GET_TRIPS_FROM_FLOWS.SOURCE.NO_USABLE_SOURCE",
        "No existe ninguna fuente usable para construir la correspondencia flujo-viajes.",
        details_keys=_SOURCE_DETAILS,
        defaults={
            "reason": "no_usable_source",
            "action": "abort",
        },
    ),
    "GET_TRIPS_FROM_FLOWS.SOURCE.PREFERRED_SOURCE_UNUSABLE": _warn(
        "GET_TRIPS_FROM_FLOWS.SOURCE.PREFERRED_SOURCE_UNUSABLE",
        "La fuente prioritaria {preferred_source!r} no fue usable; la operación continuó con {used_source!r}.",
        details_keys=_SOURCE_DETAILS,
        defaults={
            "reason": "preferred_source_unusable",
            "action": "use_fallback",
        },
    ),
    "GET_TRIPS_FROM_FLOWS.SOURCE.DUPLICATE_PAIRS_NORMALIZED": _warn(
        "GET_TRIPS_FROM_FLOWS.SOURCE.DUPLICATE_PAIRS_NORMALIZED",
        "Se normalizaron {n_duplicate_pairs!r} pares exactos duplicados en la tabla de correspondencia flujo-viajes proveniente de {source!r}.",
        details_keys=_DUPLICATE_DETAILS,
        defaults={
            "source": "flow_to_trips",
            "reason": "exact_duplicate_pairs",
            "action": "drop_exact_duplicates",
        },
    ),
    # ------------------------------------------------------------------
    # RECONSTRUCCIÓN
    # ------------------------------------------------------------------
    "GET_TRIPS_FROM_FLOWS.RECON.MISSING_REQUIRED_COLUMNS": _fatal(
        "GET_TRIPS_FROM_FLOWS.RECON.MISSING_REQUIRED_COLUMNS",
        "No se puede reconstruir la correspondencia flujo-viajes: faltan columnas requeridas en {source!r} ({missing_columns!r}).",
        details_keys=_RECON_FIELD_DETAILS,
        defaults={
            "reason": "missing_required_columns",
            "action": "abort",
        },
    ),
    "GET_TRIPS_FROM_FLOWS.RECON.AGGREGATION_KEYS_UNRECOVERABLE": _fatal(
        "GET_TRIPS_FROM_FLOWS.RECON.AGGREGATION_KEYS_UNRECOVERABLE",
        "No se puede reconstruir la correspondencia flujo-viajes: `aggregation_spec` no permite recuperar con garantías las llaves efectivas de agregación.",
        details_keys=_AGG_KEYS_DETAILS,
        defaults={
            "reason": "aggregation_keys_unrecoverable",
            "action": "abort",
        },
    ),
    # ------------------------------------------------------------------
    # SALIDA / COBERTURA
    # ------------------------------------------------------------------
    "GET_TRIPS_FROM_FLOWS.OUTPUT.PARTIAL_COVERAGE": _warn(
        "GET_TRIPS_FROM_FLOWS.OUTPUT.PARTIAL_COVERAGE",
        "La correspondencia flujo-viajes quedó parcial: {n_unmatched_movements!r} movements y {n_unmatched_flows!r} flows quedaron sin correspondencia.",
        details_keys=_OUTPUT_COVERAGE_DETAILS,
        defaults={
            "reason": "partial_coverage",
            "action": "return_partial_correspondence",
        },
    ),
    "GET_TRIPS_FROM_FLOWS.OUTPUT.EMPTY_RESULT": _warn(
        "GET_TRIPS_FROM_FLOWS.OUTPUT.EMPTY_RESULT",
        "La tabla de correspondencia flujo-viajes resultó vacía, aunque la operación fue interpretable.",
        details_keys=_OUTPUT_COVERAGE_DETAILS,
        defaults={
            "reason": "empty_result",
            "action": "return_empty_dataframe",
        },
    ),
    # ------------------------------------------------------------------
    # REPORT / LÍMITES
    # ------------------------------------------------------------------
    "GET_TRIPS_FROM_FLOWS.REPORT.ISSUES_TRUNCATED": _warn(
        "GET_TRIPS_FROM_FLOWS.REPORT.ISSUES_TRUNCATED",
        "La lista de issues de get_trips_from_flows fue truncada por max_issues={max_issues!r}.",
        details_keys=_LIMIT_DETAILS,
        defaults={
            "issues_truncated": True,
            "action": "truncate_issues",
        },
    ),
}