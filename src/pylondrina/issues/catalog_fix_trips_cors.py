from __future__ import annotations

from pylondrina.issues.core import IssueSpec


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
    exception: str = "fix",
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


_COMMON_FIX_DETAILS = (
    "kind",
    "n_rows_total",
    "sample_rows_per_issue",
)

_COMMON_LIMIT_DETAILS = (
    "max_issues",
    "n_issues_emitted",
    "n_issues_detected_total",
    "issues_truncated",
)

FIX_TRIPS_CORRESPONDENCE_ISSUES: dict[str, IssueSpec] = {
    # ------------------------------------------------------------------
    # INPUT / PRECHECKS FATALES
    # Nota:
    # - estos codes existen para mantener catálogo/exception mapping coherente,
    #   aunque algunos abortos fatales pueden ocurrir antes de construir report/event.
    # ------------------------------------------------------------------
    "FIX.INPUT.INVALID_TRIPS_OBJECT": _err(
        "FIX.INPUT.INVALID_TRIPS_OBJECT",
        "El objeto entregado a fix_trips_correspondence no es un TripDataset utilizable.",
        details_keys=("received_type", "expected_type", "action"),
        defaults={"expected_type": "TripDataset", "action": "abort"},
        exception="fix",
        fatal=True,
    ),
    "FIX.INPUT.MISSING_DATAFRAME": _err(
        "FIX.INPUT.MISSING_DATAFRAME",
        "El TripDataset no expone una tabla usable en trips.data para ejecutar fix_trips_correspondence.",
        details_keys=("attribute", "reason", "action"),
        defaults={"attribute": "data", "action": "abort"},
        exception="fix",
        fatal=True,
    ),
    "FIX.CORRECTIONS.INVALID_FIELD_STRUCTURE": _err(
        "FIX.CORRECTIONS.INVALID_FIELD_STRUCTURE",
        "field_corrections tiene una estructura inválida; se esperaba un Mapping[str, str] interpretable.",
        details_keys=("received_type", "expected_type", "reason", "action"),
        defaults={
            "expected_type": "Mapping[str, str]",
            "reason": "invalid_field_corrections_structure",
            "action": "abort",
        },
        exception="fix",
        fatal=True,
    ),
    "FIX.CORRECTIONS.INVALID_VALUE_STRUCTURE": _err(
        "FIX.CORRECTIONS.INVALID_VALUE_STRUCTURE",
        "value_corrections tiene una estructura inválida; se esperaba un Mapping[str, Mapping[Any, Any]] interpretable.",
        details_keys=("received_type", "expected_type", "reason", "action"),
        defaults={
            "expected_type": "Mapping[str, Mapping[Any, Any]]",
            "reason": "invalid_value_corrections_structure",
            "action": "abort",
        },
        exception="fix",
        fatal=True,
    ),
    "FIX.CORRECTIONS.INVALID_RULE_STRUCTURE": _err(
        "FIX.CORRECTIONS.INVALID_RULE_STRUCTURE",
        "La regla de corrección para {input_key!r} no se puede interpretar de forma segura.",
        details_keys=("kind", "input_key", "rule_value", "reason", "action"),
        defaults={"action": "abort"},
        exception="fix",
        fatal=True,
    ),
    "FIX.CONTEXT.INVALID_ROOT": _err(
        "FIX.CONTEXT.INVALID_ROOT",
        "correspondence_context debe ser dict o None; se recibió un tipo no interpretable.",
        details_keys=("received_type", "expected_type", "action"),
        defaults={"expected_type": "dict | None", "action": "abort"},
        exception="fix",
        fatal=True,
    ),
    "FIX.FIELD.AMBIGUOUS_MULTI_SOURCE_TO_SAME_TARGET": _err(
        "FIX.FIELD.AMBIGUOUS_MULTI_SOURCE_TO_SAME_TARGET",
        "field_corrections contiene una colisión ambigua: múltiples columnas origen intentan mapearse al mismo target canónico {target_column!r}.",
        details_keys=("kind", "target_column", "source_columns", "reason", "action"),
        defaults={
            "kind": "field_corrections",
            "reason": "ambiguous_multi_source_to_same_target",
            "action": "abort",
        },
        exception="fix",
        fatal=True,
    ),

    # ------------------------------------------------------------------
    # FIELD CORRECTIONS (recuperables / escalables con strict)
    # ------------------------------------------------------------------
    "FIX.FIELD.SOURCE_COLUMN_MISSING": _warn(
        "FIX.FIELD.SOURCE_COLUMN_MISSING",
        "No se puede aplicar la corrección de campo {source_column!r} -> {target_column!r} porque la columna origen no existe en trips.data.",
        details_keys=_COMMON_FIX_DETAILS + (
            "source_column",
            "target_column",
            "available_columns_sample",
            "available_columns_total",
            "action",
        ),
        defaults={"kind": "field_corrections", "action": "skip_rule"},
    ),
    "FIX.FIELD.TARGET_NOT_IN_SCHEMA": _err(
        "FIX.FIELD.TARGET_NOT_IN_SCHEMA",
        "La corrección de campo {source_column!r} -> {target_column!r} no es válida porque {target_column!r} no pertenece al TripSchema.",
        details_keys=_COMMON_FIX_DETAILS + (
            "source_column",
            "target_column",
            "reason",
            "allowed_targets_sample",
            "allowed_targets_total",
            "action",
        ),
        defaults={
            "kind": "field_corrections",
            "reason": "target_not_in_schema",
            "action": "skip_rule",
        },
        exception="fix",
        fatal=False,
    ),
    "FIX.FIELD.TARGET_ALREADY_EXISTS": _err(
        "FIX.FIELD.TARGET_ALREADY_EXISTS",
        "No se puede aplicar la corrección de campo {source_column!r} -> {target_column!r} porque el target ya existe en el dataset y la regla implicaría sobrescritura o colisión.",
        details_keys=_COMMON_FIX_DETAILS + (
            "source_column",
            "target_column",
            "reason",
            "action",
        ),
        defaults={
            "kind": "field_corrections",
            "reason": "target_already_exists",
            "action": "skip_rule",
        },
        exception="fix",
        fatal=False,
    ),
    "FIX.FIELD.RULE_NOT_ALLOWED": _err(
        "FIX.FIELD.RULE_NOT_ALLOWED",
        "La corrección de campo {source_column!r} -> {target_column!r} no respeta la política cerrada de OP-03 ({reason}).",
        details_keys=_COMMON_FIX_DETAILS + (
            "source_column",
            "target_column",
            "reason",
            "action",
        ),
        defaults={
            "kind": "field_corrections",
            "action": "skip_rule",
        },
        exception="fix",
        fatal=False,
    ),
    "FIX.FIELD.PARTIAL_APPLY": _warn(
        "FIX.FIELD.PARTIAL_APPLY",
        "Las correcciones de campos se aplicaron solo parcialmente (requested={requested_count}, applied={applied_count}, omitted={omitted_count}).",
        details_keys=_COMMON_FIX_DETAILS + (
            "requested_count",
            "applied_count",
            "omitted_count",
            "mapping_sample",
        ),
        defaults={"kind": "field_corrections"},
    ),

    # ------------------------------------------------------------------
    # VALUE CORRECTIONS (recuperables / escalables con strict)
    # ------------------------------------------------------------------
    "FIX.VALUE.FIELD_MISSING": _warn(
        "FIX.VALUE.FIELD_MISSING",
        "No se puede aplicar value_corrections sobre {field!r} porque el campo no existe en el dataset resultante tras field_corrections.",
        details_keys=_COMMON_FIX_DETAILS + (
            "field",
            "available_columns_sample",
            "available_columns_total",
            "action",
        ),
        defaults={"kind": "value_corrections", "action": "skip_field"},
    ),
    "FIX.VALUE.FIELD_NOT_COMPATIBLE": _err(
        "FIX.VALUE.FIELD_NOT_COMPATIBLE",
        "No se puede aplicar value_corrections sobre {field!r} porque el campo no es compatible con la política cerrada de OP-03 ({reason}).",
        details_keys=_COMMON_FIX_DETAILS + (
            "field",
            "field_type",
            "reason",
            "action",
        ),
        defaults={
            "kind": "value_corrections",
            "action": "skip_field",
        },
        exception="fix",
        fatal=False,
    ),
    "FIX.VALUE.SOURCE_VALUES_NOT_FOUND": _warn(
        "FIX.VALUE.SOURCE_VALUES_NOT_FOUND",
        "Algunas reglas de value_corrections para {field!r} no tuvieron efecto porque los valores origen no aparecen en el dataset.",
        details_keys=_COMMON_FIX_DETAILS + (
            "field",
            "missing_values_sample",
            "n_missing_values",
            "action",
        ),
        defaults={"kind": "value_corrections", "action": "skip_rules"},
    ),
    "FIX.VALUE.TARGET_ALREADY_PRESENT": _warn(
        "FIX.VALUE.TARGET_ALREADY_PRESENT",
        "La corrección {source_value!r} -> {target_value!r} en {field!r} apunta a un valor que ya está presente; la regla puede ser redundante.",
        details_keys=_COMMON_FIX_DETAILS + (
            "field",
            "source_value",
            "target_value",
            "reason",
        ),
        defaults={
            "kind": "value_corrections",
            "reason": "target_already_present",
        },
    ),
    "FIX.VALUE.PARTIAL_APPLY": _warn(
        "FIX.VALUE.PARTIAL_APPLY",
        "Las correcciones de valores se aplicaron solo parcialmente (requested_fields={requested_fields_count}, applied_fields={applied_fields_count}, replacements={replacements_count}).",
        details_keys=_COMMON_FIX_DETAILS + (
            "requested_fields_count",
            "applied_fields_count",
            "replacements_count",
            "mapping_sample",
        ),
        defaults={"kind": "value_corrections"},
    ),

    # ------------------------------------------------------------------
    # CONTEXT (saneamiento recuperable)
    # ------------------------------------------------------------------
    "FIX.CONTEXT.UNKNOWN_KEYS_DROPPED": _warn(
        "FIX.CONTEXT.UNKNOWN_KEYS_DROPPED",
        "correspondence_context contiene claves no reconocidas; esas claves se descartarán del evento.",
        details_keys=_COMMON_FIX_DETAILS + (
            "unknown_keys",
            "allowed_keys",
            "action",
        ),
        defaults={
            "kind": "context",
            "action": "drop_unknown_keys",
        },
    ),
    "FIX.CONTEXT.NON_SERIALIZABLE_DROPPED": _warn(
        "FIX.CONTEXT.NON_SERIALIZABLE_DROPPED",
        "correspondence_context contiene valores no serializables; esos fragmentos se descartarán del evento.",
        details_keys=_COMMON_FIX_DETAILS + (
            "dropped_paths",
            "reason",
            "action",
        ),
        defaults={
            "kind": "context",
            "reason": "non_serializable_values",
            "action": "drop_non_serializable_fragments",
        },
    ),

    # ------------------------------------------------------------------
    # DOMAINS / ESTADO EFECTIVO
    # ------------------------------------------------------------------
    "FIX.DOMAINS.UPDATED": _info(
        "FIX.DOMAINS.UPDATED",
        "Se actualizaron los domains_effective para los campos tocados por value_corrections.",
        details_keys=(
            "kind",
            "updated_fields",
            "added_values_by_field",
            "n_rows_total",
        ),
        defaults={"kind": "domains_effective"},
    ),

    # ------------------------------------------------------------------
    # INFO resumida de aplicación
    # ------------------------------------------------------------------
    "FIX.INFO.FIELD_CORRECTIONS_APPLIED": _info(
        "FIX.INFO.FIELD_CORRECTIONS_APPLIED",
        "Se aplicaron correcciones de campos (requested={requested_count}, applied={applied_count}).",
        details_keys=(
            "kind",
            "requested_count",
            "applied_count",
            "mapping_sample",
            "n_rows_total",
        ),
        defaults={"kind": "field_corrections"},
    ),
    "FIX.INFO.VALUE_CORRECTIONS_APPLIED": _info(
        "FIX.INFO.VALUE_CORRECTIONS_APPLIED",
        "Se aplicaron correcciones de valores (requested_fields={requested_fields_count}, applied_fields={applied_fields_count}, replacements={replacements_count}).",
        details_keys=(
            "kind",
            "requested_fields_count",
            "applied_fields_count",
            "replacements_count",
            "mapping_sample",
            "n_rows_total",
        ),
        defaults={"kind": "value_corrections"},
    ),

    # ------------------------------------------------------------------
    # SIN CAMBIOS EFECTIVOS
    # Mantengo el namespace NOOP por consistencia con el contrato vigente.
    # ------------------------------------------------------------------
    "FIX.NO_EFFECTIVE_CHANGES.NO_CORRECTIONS": _info(
        "FIX.NO_EFFECTIVE_CHANGES.NO_CORRECTIONS",
        "No se proporcionaron correcciones; la operación terminó sin cambios efectivos.",
        details_keys=(
            "kind",
            "field_corrections_provided",
            "value_corrections_provided",
            "n_rows_total",
        ),
        defaults={"kind": "noop"},
    ),
    "FIX.NO_EFFECTIVE_CHANGES.NO_EFFECTIVE_CHANGES": _warn(
        "FIX.NO_EFFECTIVE_CHANGES.NO_EFFECTIVE_CHANGES",
        "Se proporcionaron correcciones, pero la operación terminó sin cambios efectivos en el dataset.",
        details_keys=(
            "kind",
            "field_corrections_provided",
            "value_corrections_provided",
            "requested_field_rules",
            "requested_value_rules",
            "n_rows_total",
        ),
        defaults={"kind": "noop"},
    ),

    # ------------------------------------------------------------------
    # CORE / LIMITS
    # ------------------------------------------------------------------
    "FIX.CORE.ISSUES_TRUNCATED": _warn(
        "FIX.CORE.ISSUES_TRUNCATED",
        "Se alcanzó el límite máximo de issues permitido; el reporte fue truncado.",
        details_keys=_COMMON_LIMIT_DETAILS,
        defaults={"issues_truncated": True},
    ),
}
