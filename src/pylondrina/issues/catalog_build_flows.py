from __future__ import annotations

from pylondrina.issues.core import IssueSpec


"""
Catálogo de issues para OP-08: build_flows.

Notas normativas v1.1
---------------------
- OP-08 retorna `FlowBuildReport`; el catálogo prioriza hallazgos agregados por
  check/regla, no emisiones fila-a-fila.
- Los abortos fatales de input/config no dependen de `strict`.
- `strict` solo separa rutas recuperables explícitas cuando la implementación las
  habilite; no rebaja precondiciones fatales ya cerradas por contrato.
- `max_issues` es guardarraíl; el truncamiento se modela con un issue explícito.
- En ausencia de una excepción específica de build en el core actual, los fatales
  se clasifican usando familias existentes (`validate` / `schema`) según el tipo
  de problema.
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
    exception: str = "validate",
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


_COMMON_BUILD_DETAILS = (
    "check",
    "reason",
    "action",
    "n_trips_in",
    "n_trips_eligible",
    "n_trips_dropped",
    "n_flows_out",
    "h3_resolution_input",
    "h3_resolution_target",
    "group_by",
    "time_aggregation",
    "time_basis",
    "tier",
)

_COMMON_FIELD_DETAILS = (
    "check",
    "reason",
    "action",
    "missing_fields",
    "group_by_invalid_fields",
    "available_fields_sample",
    "available_fields_total",
    "n_trips_in",
)

_COMMON_DROP_DETAILS = (
    "check",
    "reason",
    "n_trips_in",
    "n_trips_eligible",
    "n_trips_dropped",
    "n_violations",
    "row_indices_sample",
    "values_sample",
)

_COMMON_LIMIT_DETAILS = (
    "max_issues",
    "n_issues_emitted",
    "n_issues_detected_total",
    "issues_truncated",
)

BUILD_FLOWS_ISSUES: dict[str, IssueSpec] = {
    # ------------------------------------------------------------------
    # INPUT / PRECONDICIONES
    # ------------------------------------------------------------------
    "FLOW.INPUT.INVALID_TRIPDATASET": _err(
        "FLOW.INPUT.INVALID_TRIPDATASET",
        "El objeto recibido no es interpretable como TripDataset para build_flows.",
        details_keys=("received_type", "expected_type", "action"),
        defaults={"expected_type": "TripDataset", "action": "abort"},
        exception="validate",
    ),
    "FLOW.INPUT.INVALID_DATA_SURFACE": _err(
        "FLOW.INPUT.INVALID_DATA_SURFACE",
        "TripDataset.data no está disponible o no es tabular; no es posible construir flujos.",
        details_keys=("attribute", "data_type", "reason", "action"),
        defaults={"attribute": "data", "action": "abort"},
        exception="validate",
    ),
    "FLOW.INPUT.NO_TRIPS": _err(
        "FLOW.INPUT.NO_TRIPS",
        "No se pueden construir flujos: el TripDataset de entrada no contiene movements.",
        details_keys=("check", "n_trips_in", "reason", "action"),
        defaults={"check": "input", "reason": "empty_dataset", "action": "abort"},
        exception="validate",
    ),
    "FLOW.INPUT.REQUIRED_FIELDS_MISSING": _err(
        "FLOW.INPUT.REQUIRED_FIELDS_MISSING",
        "Faltan campos mínimos para construir flujos: {missing_fields}.",
        details_keys=_COMMON_FIELD_DETAILS,
        defaults={"check": "input", "reason": "required_fields_missing", "action": "abort"},
        exception="validate",
    ),
    "FLOW.VALIDATION.REQUIRED_NOT_VALIDATED": _err(
        "FLOW.VALIDATION.REQUIRED_NOT_VALIDATED",
        "build_flows requiere un TripDataset validado, pero metadata['is_validated']={validated_flag!r}.",
        details_keys=("check", "require_validated", "validated_flag", "reason", "action"),
        defaults={"check": "precheck", "reason": "validated_required", "action": "abort"},
        exception="validate",
    ),

    # ------------------------------------------------------------------
    # CONFIG / REQUEST
    # ------------------------------------------------------------------
    "FLOW.CONFIG.INVALID_H3_RESOLUTION": _err(
        "FLOW.CONFIG.INVALID_H3_RESOLUTION",
        "La resolución H3 objetivo {h3_resolution_target!r} no es válida para build_flows.",
        details_keys=("check", "h3_resolution_target", "reason", "action"),
        defaults={"check": "config", "reason": "invalid_h3_resolution", "action": "abort"},
        exception="schema",
    ),
    "FLOW.CONFIG.INVALID_TIME_AGGREGATION": _err(
        "FLOW.CONFIG.INVALID_TIME_AGGREGATION",
        "time_aggregation={time_aggregation!r} no pertenece al contrato vigente de build_flows.",
        details_keys=("check", "time_aggregation", "expected", "reason", "action"),
        defaults={
            "check": "config",
            "expected": ["none", "hour", "day", "week"],
            "reason": "invalid_time_aggregation",
            "action": "abort",
        },
        exception="schema",
    ),
    "FLOW.CONFIG.INVALID_TIME_BASIS": _err(
        "FLOW.CONFIG.INVALID_TIME_BASIS",
        "time_basis={time_basis!r} no pertenece al contrato vigente de build_flows.",
        details_keys=("check", "time_basis", "expected", "reason", "action"),
        defaults={
            "check": "config",
            "expected": ["origin", "destination"],
            "reason": "invalid_time_basis",
            "action": "abort",
        },
        exception="schema",
    ),
    "FLOW.CONFIG.INVALID_MIN_TRIPS_PER_FLOW": _err(
        "FLOW.CONFIG.INVALID_MIN_TRIPS_PER_FLOW",
        "min_trips_per_flow={min_trips_per_flow!r} debe ser un entero positivo.",
        details_keys=("check", "min_trips_per_flow", "reason", "action"),
        defaults={"check": "config", "reason": "invalid_min_trips_per_flow", "action": "abort"},
        exception="schema",
    ),
    "FLOW.CONFIG.INVALID_MAX_ISSUES": _err(
        "FLOW.CONFIG.INVALID_MAX_ISSUES",
        "max_issues={max_issues!r} debe ser un entero positivo.",
        details_keys=("check", "max_issues", "reason", "action"),
        defaults={"check": "config", "reason": "invalid_max_issues", "action": "abort"},
        exception="schema",
    ),
    "FLOW.CONFIG.GROUP_BY_INVALID_FIELDS": _err(
        "FLOW.CONFIG.GROUP_BY_INVALID_FIELDS",
        "group_by contiene campos inexistentes o no interpretables para la agregación: {group_by_invalid_fields}.",
        details_keys=_COMMON_FIELD_DETAILS,
        defaults={"check": "config", "reason": "invalid_group_by_fields", "action": "abort"},
        exception="schema",
    ),

    # ------------------------------------------------------------------
    # AGREGACIÓN ESPACIAL / TEMPORAL
    # ------------------------------------------------------------------
    "FLOW.AGG.H3_RESOLUTION_TOO_FINE": _err(
        "FLOW.AGG.H3_RESOLUTION_TOO_FINE",
        "La resolución H3 objetivo {h3_resolution_target!r} es más fina que la resolución de entrada {h3_resolution_input!r}.",
        details_keys=("check", "h3_resolution_input", "h3_resolution_target", "reason", "action"),
        defaults={"check": "aggregation", "reason": "target_resolution_too_fine", "action": "abort"},
        exception="validate",
    ),
    "FLOW.AGG.H3_INVALID_OR_MIXED": _err(
        "FLOW.AGG.H3_INVALID_OR_MIXED",
        "Los índices H3 del TripDataset son inválidos o mezclan resoluciones incompatibles para build_flows.",
        details_keys=(
            "check",
            "reason",
            "action",
            "h3_resolution_input",
            "h3_resolution_target",
            "n_violations",
            "row_indices_sample",
            "values_sample",
        ),
        defaults={"check": "aggregation", "reason": "invalid_or_mixed_h3", "action": "abort"},
        exception="validate",
    ),
    "FLOW.TEMPORAL.TIER_NOT_SUPPORTED": _err(
        "FLOW.TEMPORAL.TIER_NOT_SUPPORTED",
        "time_aggregation={time_aggregation!r} requiere Tier 1, pero el dataset está en {tier!r}.",
        details_keys=("check", "tier", "time_aggregation", "reason", "action"),
        defaults={"check": "temporal", "reason": "tier_not_supported", "action": "abort"},
        exception="validate",
    ),
    "FLOW.TEMPORAL.REQUIRED_TIME_FIELDS_MISSING": _err(
        "FLOW.TEMPORAL.REQUIRED_TIME_FIELDS_MISSING",
        "Faltan campos temporales mínimos para construir flujos con dimensión temporal: {missing_fields}.",
        details_keys=_COMMON_FIELD_DETAILS,
        defaults={"check": "temporal", "reason": "required_time_fields_missing", "action": "abort"},
        exception="validate",
    ),

    # ------------------------------------------------------------------
    # BUILDABLE SET / OUTPUT
    # ------------------------------------------------------------------
    "FLOW.OUTPUT.MOVEMENTS_DROPPED_MISSING_OD_H3": _warn(
        "FLOW.OUTPUT.MOVEMENTS_DROPPED_MISSING_OD_H3",
        "Se descartaron movements porque no tenían ambos H3 OD requeridos para construir flujos (n={n_trips_dropped}).",
        details_keys=_COMMON_DROP_DETAILS,
        defaults={"check": "buildable_set", "reason": "missing_od_h3"},
    ),
    "FLOW.OUTPUT.NO_BUILDABLE_MOVEMENTS": _err(
        "FLOW.OUTPUT.NO_BUILDABLE_MOVEMENTS",
        "No quedó ningún movement buildable tras exigir ambos H3 OD.",
        details_keys=("check", "n_trips_in", "n_trips_eligible", "n_trips_dropped", "reason", "action"),
        defaults={"check": "buildable_set", "reason": "no_buildable_movements", "action": "abort"},
        exception="validate",
    ),
    "FLOW.OUTPUT.EMPTY_AFTER_THRESHOLD": _warn(
        "FLOW.OUTPUT.EMPTY_AFTER_THRESHOLD",
        "La agregación finalizó sin flujos luego de aplicar min_trips_per_flow={min_trips_per_flow!r}.",
        details_keys=(
            "check",
            "min_trips_per_flow",
            "n_trips_in",
            "n_trips_eligible",
            "n_flows_out",
            "reason",
        ),
        defaults={"check": "output", "reason": "empty_after_threshold"},
    ),

    # ------------------------------------------------------------------
    # BACKLINKS / AUXILIARES
    # ------------------------------------------------------------------
    "FLOW.BACKLINK.MOVEMENT_ID_REQUIRED": _err(
        "FLOW.BACKLINK.MOVEMENT_ID_REQUIRED",
        "keep_flow_to_trips=True requiere el campo movement_id en TripDataset.data.",
        details_keys=("check", "missing_fields", "reason", "action"),
        defaults={"check": "backlink", "missing_fields": ["movement_id"], "reason": "movement_id_required", "action": "abort"},
        exception="validate",
    ),

    # ------------------------------------------------------------------
    # PROVENANCE / METADATA
    # ------------------------------------------------------------------
    "FLOW.PROV.PRIOR_EVENTS_MISSING": _warn(
        "FLOW.PROV.PRIOR_EVENTS_MISSING",
        "No se encontraron eventos previos en TripDataset.metadata['events']; prior_events_summary quedará vacío.",
        details_keys=("check", "prior_events_present", "reason"),
        defaults={"check": "provenance", "prior_events_present": False, "reason": "prior_events_missing"},
    ),
    "FLOW.METADATA.DATASET_ID_CREATED": _info(
        "FLOW.METADATA.DATASET_ID_CREATED",
        "Se generó dataset_id para el FlowDataset derivado: {dataset_id!r}.",
        details_keys=("dataset_id", "generator", "stored_in"),
        defaults={"stored_in": "metadata.dataset_id"},
    ),

    # ------------------------------------------------------------------
    # REPORT / LIMITES
    # ------------------------------------------------------------------
    "FLOW.REPORT.ISSUES_TRUNCATED": _warn(
        "FLOW.REPORT.ISSUES_TRUNCATED",
        "La lista de issues de build_flows fue truncada por max_issues={max_issues!r}.",
        details_keys=_COMMON_LIMIT_DETAILS,
        defaults={"issues_truncated": True},
    ),
}