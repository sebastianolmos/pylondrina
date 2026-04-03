from __future__ import annotations

from pylondrina.issues.core import IssueSpec


"""
Catálogo de issues para OP-04: clean_trips.

Notas normativas v1.1
---------------------
- Este catálogo cubre solo rutas retornables de OP-04.
- Los abortos fatales de configuración/programmer error ocurren antes del retorno
  normal y se manejan fuera de este catálogo con TypeError/ValueError.
- OP-04 no usa strict, CleanError, max_issues ni sample_rows_per_issue.
- Si clean_trips retorna normalmente, el report debe quedar con ok=True.
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


_COMMON_NO_CHANGES_DETAILS = (
    "rows_in",
    "rows_out",
    "dropped_total",
    "active_rules",
    "omitted_rules",
    "reason",
)

_COMMON_RULE_DETAILS = (
    "rule",
    "missing_fields",
    "available_fields_sample",
    "available_fields_total",
    "reason",
    "action",
)

CLEAN_TRIPS_ISSUES: dict[str, IssueSpec] = {
    # ------------------------------------------------------------------
    # NO CHANGES / TRAZABILIDAD
    # ------------------------------------------------------------------
    "CLN.NO_CHANGES.NO_RULES_ACTIVE": _info(
        "CLN.NO_CHANGES.NO_RULES_ACTIVE",
        "No se aplicó ninguna regla de limpieza: no hay reglas activas o todas quedaron inactivas.",
        details_keys=_COMMON_NO_CHANGES_DETAILS,
        defaults={
            "reason": "no_rules_active",
        },
    ),
    "CLN.NO_CHANGES.NO_ROWS_DROPPED": _info(
        "CLN.NO_CHANGES.NO_ROWS_DROPPED",
        "La limpieza finalizó sin cambios efectivos: no se eliminaron filas.",
        details_keys=_COMMON_NO_CHANGES_DETAILS,
        defaults={
            "reason": "no_rows_dropped",
        },
    ),

    # ------------------------------------------------------------------
    # RULES OMITIDAS / BEST-EFFORT
    # ------------------------------------------------------------------
    "CLN.RULE.FIELDS_MISSING": _warn(
        "CLN.RULE.FIELDS_MISSING",
        "No se puede aplicar la regla {rule!r}: faltan columnas requeridas (missing={missing_fields!r}).",
        details_keys=_COMMON_RULE_DETAILS,
        defaults={
            "action": "rule_omitted",
            "reason": "missing_required_columns",
        },
    ),
    "CLN.RULE.DEFAULT_DUPLICATES_SUBSET_UNAVAILABLE": _warn(
        "CLN.RULE.DEFAULT_DUPLICATES_SUBSET_UNAVAILABLE",
        "No se puede aplicar la regla de duplicados: el subset por defecto no está disponible o quedó vacío.",
        details_keys=(
            "rule",
            "subset_default",
            "subset_effective",
            "schema_required",
            "available_fields_sample",
            "available_fields_total",
            "reason",
            "action",
        ),
        defaults={
            "rule": "duplicates",
            "action": "rule_omitted",
            "reason": "default_duplicates_subset_unavailable",
        },
    ),
    "CLN.RULE.TEMPORAL_RULE_NOT_EVALUABLE": _warn(
        "CLN.RULE.TEMPORAL_RULE_NOT_EVALUABLE",
        "No se puede aplicar la regla temporal {rule!r}: el dataset no es evaluable en Tier 1 o los tiempos no son comparables.",
        details_keys=(
            "rule",
            "temporal_tier",
            "fields_present",
            "missing_fields",
            "reason",
            "action",
        ),
        defaults={
            "rule": "origin_after_destination",
            "action": "rule_omitted",
            "reason": "temporal_rule_not_evaluable",
        },
    ),
    "CLN.RULE.FIELD_NOT_CATEGORICAL": _warn(
        "CLN.RULE.FIELD_NOT_CATEGORICAL",
        "No se puede aplicar drop por valores categóricos sobre {field!r}: el campo no es categórico ni interpretable como tal.",
        details_keys=(
            "rule",
            "field",
            "dtype_observed",
            "reason",
            "action",
        ),
        defaults={
            "rule": "categorical_values",
            "action": "rule_entry_omitted",
            "reason": "field_not_categorical",
        },
    ),

    # ------------------------------------------------------------------
    # RESULTADO
    # ------------------------------------------------------------------
    "CLN.RESULT.EMPTY_DATASET": _warn(
        "CLN.RESULT.EMPTY_DATASET",
        "La limpieza produjo un dataset vacío.",
        details_keys=(
            "rows_in",
            "rows_out",
            "dropped_total",
            "active_rules",
            "dropped_by_rule",
            "reason",
        ),
        defaults={
            "reason": "empty_dataset_after_clean",
        },
    ),
}

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


CLEAN_TRIPS_ISSUES.update({
    "CLN.CONFIG.INVALID_TRIPS_OBJECT": _fatal_type(
        "CLN.CONFIG.INVALID_TRIPS_OBJECT",
        "clean_trips esperaba un TripDataset, pero recibió {received_type!r}.",
        details_keys=("received_type",),
    ),
    "CLN.CONFIG.MISSING_DATAFRAME": _fatal_type(
        "CLN.CONFIG.MISSING_DATAFRAME",
        "clean_trips requiere `trips.data` como pandas.DataFrame interpretable.",
        details_keys=("received_type", "reason"),
    ),
    "CLN.CONFIG.INVALID_OPTIONS_OBJECT": _fatal_type(
        "CLN.CONFIG.INVALID_OPTIONS_OBJECT",
        "clean_trips esperaba `options` de tipo CleanOptions o None, pero recibió {received_type!r}.",
        details_keys=("received_type",),
    ),
    "CLN.CONFIG.INVALID_NULL_FIELDS": _fatal_value(
        "CLN.CONFIG.INVALID_NULL_FIELDS",
        "La configuración `drop_rows_with_nulls_in_fields` no es interpretable como secuencia de strings.",
        details_keys=("received_type", "invalid_fields_sample", "reason"),
    ),
    "CLN.CONFIG.INVALID_DUPLICATES_SUBSET": _fatal_value(
        "CLN.CONFIG.INVALID_DUPLICATES_SUBSET",
        "La configuración de `duplicates_subset` no es interpretable o referencia columnas inexistentes.",
        details_keys=("received_type", "duplicates_subset", "invalid_fields_sample", "missing_fields", "reason"),
    ),
    "CLN.CONFIG.INVALID_CATEGORICAL_MAPPING": _fatal_value(
        "CLN.CONFIG.INVALID_CATEGORICAL_MAPPING",
        "La configuración `drop_rows_by_categorical_values` no es interpretable como mapping campo -> secuencia.",
        details_keys=("field", "received_type", "reason"),
    ),
    "CLN.CONFIG.NON_SERIALIZABLE_PARAMETER": _fatal_value(
        "CLN.CONFIG.NON_SERIALIZABLE_PARAMETER",
        "La configuración de Clean contiene un valor no saneable/serializable en {option_name!r}.",
        details_keys=("field", "option_name", "value_repr"),
    ),
})