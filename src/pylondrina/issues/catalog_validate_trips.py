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


_COMMON_ROW_DETAILS = (
    "check",
    "n_rows_total",
    "n_violations",
    "row_indices_sample",
    "values_sample",
    "raw_values_sample",
)

VALIDATE_TRIPS_ISSUES: dict[str, IssueSpec] = {
    # ------------------------------------------------------------------
    # INPUT
    # ------------------------------------------------------------------
    "VAL.INPUT.INVALID_TRIPS_OBJECT": _err(
        "VAL.INPUT.INVALID_TRIPS_OBJECT",
        "El objeto entregado a validate_trips no es un TripDataset utilizable.",
        details_keys=("received_type", "expected_type", "action"),
        defaults={"expected_type": "TripDataset", "action": "abort"},
        exception="validate",
        fatal=True,
    ),
    "VAL.INPUT.MISSING_DATAFRAME": _err(
        "VAL.INPUT.MISSING_DATAFRAME",
        "El TripDataset no expone una tabla de validación usable en trips.data.",
        details_keys=("attribute", "reason", "action"),
        defaults={"attribute": "data", "action": "abort"},
        exception="validate",
        fatal=True,
    ),

    # ------------------------------------------------------------------
    # SCHEMA
    # ------------------------------------------------------------------
    "VAL.SCHEMA.MISSING_SCHEMA": _err(
        "VAL.SCHEMA.MISSING_SCHEMA",
        "El TripDataset no tiene un schema interpretable; no es posible ejecutar validate_trips.",
        details_keys=("schema_present", "action"),
        defaults={"schema_present": False, "action": "abort"},
        exception="schema",
        fatal=True,
    ),
    "VAL.SCHEMA.EMPTY_FIELDS": _err(
        "VAL.SCHEMA.EMPTY_FIELDS",
        "El TripSchema no define fields; no existe contrato de validación ejecutable.",
        details_keys=("schema_version", "fields_size", "action"),
        defaults={"action": "abort"},
        exception="schema",
        fatal=True,
    ),
    "VAL.SCHEMA.REQUIRED_FIELD_UNKNOWN": _err(
        "VAL.SCHEMA.REQUIRED_FIELD_UNKNOWN",
        "El TripSchema declara campos requeridos que no existen en fields: {unknown_required}.",
        details_keys=("unknown_required", "required", "schema_fields_sample", "schema_fields_total", "action"),
        defaults={"action": "abort"},
        exception="schema",
        fatal=True,
    ),
    "VAL.SCHEMA.UNKNOWN_DTYPE": _err(
        "VAL.SCHEMA.UNKNOWN_DTYPE",
        "El campo {field!r} declara un dtype no soportado por validate_trips: {dtype!r}.",
        details_keys=("field", "dtype", "supported_dtypes", "action"),
        defaults={"action": "abort"},
        exception="schema",
        fatal=True,
    ),
    "VAL.SCHEMA.UNKNOWN_CONSTRAINT": _err(
        "VAL.SCHEMA.UNKNOWN_CONSTRAINT",
        "El campo {field!r} declara una constraint no soportada por validate_trips: {constraint!r}.",
        details_keys=("field", "constraint", "supported_constraints", "action"),
        defaults={"action": "abort"},
        exception="schema",
        fatal=True,
    ),
    "VAL.SCHEMA.CONSTRAINT_NOT_ALLOWED_FOR_DTYPE": _err(
        "VAL.SCHEMA.CONSTRAINT_NOT_ALLOWED_FOR_DTYPE",
        "La constraint {constraint!r} no está permitida para el dtype {dtype!r} del campo {field!r}.",
        details_keys=("field", "dtype", "constraint", "allowed_constraints", "action"),
        defaults={"action": "abort"},
        exception="schema",
        fatal=True,
    ),
    "VAL.SCHEMA.REQUIRED_NULLABLE_CONFLICT": _err(
        "VAL.SCHEMA.REQUIRED_NULLABLE_CONFLICT",
        "El campo {field!r} es required=True pero declara nullable=True; el schema es inconsistente.",
        details_keys=("field", "required", "nullable", "reason", "action"),
        defaults={
            "required": True,
            "nullable": True,
            "reason": "required_nullable_conflict",
            "action": "abort",
        },
        exception="schema",
        fatal=True,
    ),
    "VAL.SCHEMA.CONSTRAINT_PARAMS_INVALID": _warn(
        "VAL.SCHEMA.CONSTRAINT_PARAMS_INVALID",
        "La constraint {constraint!r} del campo {field!r} tiene parámetros inválidos o incompletos; se omitirá solo esa constraint.",
        details_keys=("check", "field", "constraint", "expected_params", "received_params", "reason", "action"),
        defaults={"check": "constraints", "action": "skip_constraint"},
    ),

    # ------------------------------------------------------------------
    # CONFIG / OPTIONS
    # ------------------------------------------------------------------
    "VAL.CONFIG.INVALID_MAX_ISSUES": _err(
        "VAL.CONFIG.INVALID_MAX_ISSUES",
        "max_issues={max_issues!r} es inválido; debe ser un entero positivo.",
        details_keys=("max_issues", "expected", "action"),
        defaults={"expected": "int > 0", "action": "abort"},
        exception="validate",
        fatal=True,
    ),
    "VAL.CONFIG.INVALID_SAMPLE_ROWS_PER_ISSUE": _err(
        "VAL.CONFIG.INVALID_SAMPLE_ROWS_PER_ISSUE",
        "sample_rows_per_issue={sample_rows_per_issue!r} es inválido; debe ser un entero positivo.",
        details_keys=("sample_rows_per_issue", "expected", "action"),
        defaults={"expected": "int > 0", "action": "abort"},
        exception="validate",
        fatal=True,
    ),
    "VAL.CONFIG.INVALID_VALIDATE_DOMAINS_MODE": _err(
        "VAL.CONFIG.INVALID_VALIDATE_DOMAINS_MODE",
        "validate_domains={validate_domains!r} no es un modo válido para OP-02.",
        details_keys=("validate_domains", "expected", "action"),
        defaults={"expected": ["off", "full", "sample"], "action": "abort"},
        exception="validate",
        fatal=True,
    ),
    "VAL.CONFIG.INVALID_DOMAINS_SAMPLE_FRAC": _err(
        "VAL.CONFIG.INVALID_DOMAINS_SAMPLE_FRAC",
        "domains_sample_frac={domains_sample_frac!r} es inválido; debe estar en el intervalo (0, 1].",
        details_keys=("domains_sample_frac", "expected", "action"),
        defaults={"expected": "(0, 1]", "action": "abort"},
        exception="validate",
        fatal=True,
    ),
    "VAL.CONFIG.INVALID_DOMAINS_MIN_RATIO": _err(
        "VAL.CONFIG.INVALID_DOMAINS_MIN_RATIO",
        "domains_min_in_domain_ratio={domains_min_in_domain_ratio!r} es inválido; debe estar en el intervalo [0, 1].",
        details_keys=("domains_min_in_domain_ratio", "expected", "action"),
        defaults={"expected": "[0, 1]", "action": "abort"},
        exception="validate",
        fatal=True,
    ),
    "VAL.CONFIG.UNSUPPORTED_OPTION_ENABLED": _err(
        "VAL.CONFIG.UNSUPPORTED_OPTION_ENABLED",
        "Se activó una opción no soportada por el contrato vigente de OP-02: {option_name!r}.",
        details_keys=("option_name", "option_value", "reason", "action"),
        defaults={"reason": "unsupported_in_v1_1", "action": "abort"},
        exception="validate",
        fatal=True,
    ),
    "VAL.CONFIG.DUPLICATES_SUBSET_NOT_PROVIDED": _err(
        "VAL.CONFIG.DUPLICATES_SUBSET_NOT_PROVIDED",
        "validate_duplicates=True requiere duplicates_subset explícito; no se definió subset usable.",
        details_keys=("validate_duplicates", "duplicates_subset", "action"),
        defaults={"validate_duplicates": True, "action": "abort"},
        exception="validate",
        fatal=True,
    ),
    "VAL.CONFIG.DUPLICATES_SUBSET_EMPTY": _err(
        "VAL.CONFIG.DUPLICATES_SUBSET_EMPTY",
        "duplicates_subset está vacío; no es posible ejecutar el check de duplicados con un subset vacío.",
        details_keys=("duplicates_subset", "action"),
        defaults={"action": "abort"},
        exception="validate",
        fatal=True,
    ),
    "VAL.CONFIG.DUPLICATES_SUBSET_UNKNOWN_FIELD": _err(
        "VAL.CONFIG.DUPLICATES_SUBSET_UNKNOWN_FIELD",
        "duplicates_subset contiene campos inexistentes en trips.data: {unknown_fields}.",
        details_keys=("duplicates_subset", "unknown_fields", "available_columns_sample", "available_columns_total", "action"),
        defaults={"action": "abort"},
        exception="validate",
        fatal=True,
    ),

    # ------------------------------------------------------------------
    # CORE: REQUIRED / TYPES / CONSTRAINTS
    # ------------------------------------------------------------------
    "VAL.CORE.REQUIRED_COLUMNS_MISSING": _err(
        "VAL.CORE.REQUIRED_COLUMNS_MISSING",
        "Faltan columnas requeridas según el TripSchema: {missing_required}.",
        details_keys=("check", "missing_required", "required", "available_columns_sample", "available_columns_total", "action"),
        defaults={"check": "required_columns", "action": "report_error"},
        exception="validate",
        fatal=False,
    ),
    "VAL.CORE.TYPE_OR_FORMAT_INVALID": _err(
        "VAL.CORE.TYPE_OR_FORMAT_INVALID",
        "El campo {field!r} contiene valores no interpretables para el dtype/formato esperado ({dtype_expected!r}).",
        details_keys=_COMMON_ROW_DETAILS + ("field", "dtype_expected", "parse_fail_count", "total_count", "fail_rate", "action"),
        defaults={"check": "types_and_formats", "action": "report_error"},
        exception="validate",
        fatal=False,
    ),
    "VAL.CORE.NULLABILITY_VIOLATION": _err(
        "VAL.CORE.NULLABILITY_VIOLATION",
        "El campo {field!r} no admite nulos según nullable efectivo, pero se detectaron filas con valores faltantes.",
        details_keys=_COMMON_ROW_DETAILS + ("field", "nullable_effective", "action"),
        defaults={"check": "constraints", "nullable_effective": False, "action": "report_error"},
        exception="validate",
        fatal=False,
    ),
    "VAL.CORE.OD_SPATIAL_BOTH_MISSING": _err(
        "VAL.CORE.OD_SPATIAL_BOTH_MISSING",
        "La regla de OD parcial falló: la fila no tiene ni origen espacial completo ni destino espacial completo en coordenadas.",
        details_keys=_COMMON_ROW_DETAILS + ("fields_checked", "allow_partial_od_spatial", "action"),
        defaults={"check": "constraints", "allow_partial_od_spatial": True, "action": "report_error"},
        exception="validate",
        fatal=False,
    ),
    "VAL.CORE.CONSTRAINT_VIOLATION": _err(
        "VAL.CORE.CONSTRAINT_VIOLATION",
        "El campo {field!r} viola la constraint declarativa {constraint!r}.",
        details_keys=_COMMON_ROW_DETAILS + ("field", "constraint", "expected", "observed_sample", "action"),
        defaults={"check": "constraints", "action": "report_error"},
        exception="validate",
        fatal=False,
    ),

    # ------------------------------------------------------------------
    # DOMAINS
    # ------------------------------------------------------------------
    "VAL.DOMAIN.MISSING_DOMAIN_INFO": _warn(
        "VAL.DOMAIN.MISSING_DOMAIN_INFO",
        "No existe información de dominio usable para el campo categórico {field!r}; se omitirá su validación de dominio.",
        details_keys=("check", "field", "domain_source_attempted", "reason", "action"),
        defaults={"check": "domains", "action": "skip_field"},
    ),
    "VAL.DOMAIN.RATIO_BELOW_MIN": _err(
        "VAL.DOMAIN.RATIO_BELOW_MIN",
        "El campo {field!r} no cumple el mínimo requerido de cobertura en dominio ({ratio_in_domain:.4f} < {min_required_ratio:.4f}).",
        details_keys=_COMMON_ROW_DETAILS + (
            "field",
            "mode",
            "ratio_in_domain",
            "min_required_ratio",
            "n_checked_non_null",
            "n_in_domain",
            "domain_values_sample",
            "action",
        ),
        defaults={"check": "domains", "action": "report_error"},
        exception="validate",
        fatal=False,
    ),
    "VAL.DOMAIN.PARTIAL_COVERAGE": _warn(
        "VAL.DOMAIN.PARTIAL_COVERAGE",
        "El campo {field!r} tiene cobertura parcial en dominio ({ratio_in_domain:.4f}); supera el mínimo, pero no alcanza cobertura completa.",
        details_keys=_COMMON_ROW_DETAILS + (
            "field",
            "mode",
            "ratio_in_domain",
            "min_required_ratio",
            "n_checked_non_null",
            "n_in_domain",
            "domain_values_sample",
            "action",
        ),
        defaults={"check": "domains", "action": "report_warning"},
    ),

    # ------------------------------------------------------------------
    # TEMPORAL
    # ------------------------------------------------------------------
    "VAL.TEMPORAL.ORIGIN_AFTER_DESTINATION": _err(
        "VAL.TEMPORAL.ORIGIN_AFTER_DESTINATION",
        "Se detectaron filas donde origin_time_utc es posterior a destination_time_utc.",
        details_keys=_COMMON_ROW_DETAILS + ("origin_field", "destination_field", "action"),
        defaults={
            "check": "temporal_consistency",
            "origin_field": "origin_time_utc",
            "destination_field": "destination_time_utc",
            "action": "report_error",
        },
        exception="validate",
        fatal=False,
    ),

    # ------------------------------------------------------------------
    # DUPLICATES
    # ------------------------------------------------------------------
    "VAL.DUPLICATES.ROWS_FOUND": _err(
        "VAL.DUPLICATES.ROWS_FOUND",
        "Se detectaron filas duplicadas según duplicates_subset={duplicates_subset}.",
        details_keys=_COMMON_ROW_DETAILS + ("duplicates_subset", "duplicate_keys_sample", "action"),
        defaults={"check": "duplicates", "action": "report_error"},
        exception="validate",
        fatal=False,
    ),

    # ------------------------------------------------------------------
    # LIMITS / TRUNCATION
    # ------------------------------------------------------------------
    "VAL.CORE.ISSUES_TRUNCATED": _warn(
        "VAL.CORE.ISSUES_TRUNCATED",
        "El reporte fue truncado por max_issues={max_issues}; no se emitieron todos los hallazgos detectados.",
        details_keys=("check", "max_issues", "n_issues_emitted", "n_issues_detected_total", "issues_truncated", "action"),
        defaults={"check": "limits", "issues_truncated": True, "action": "truncate_report"},
    ),
}