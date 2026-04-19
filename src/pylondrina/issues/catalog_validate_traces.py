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
    "sample_rows",
)

VALIDATE_TRACES_ISSUES: dict[str, IssueSpec] = {
    # ------------------------------------------------------------------
    # INPUT / OPTIONS
    # ------------------------------------------------------------------
    "VAL.INPUT.INVALID_TRACES_OBJECT": _err(
        "VAL.INPUT.INVALID_TRACES_OBJECT",
        "El objeto entregado a validate_traces no es un TraceDataset utilizable.",
        details_keys=("received_type", "expected_type", "action"),
        defaults={"expected_type": "TraceDataset", "action": "abort"},
        exception="validate",
        fatal=True,
    ),
    "VAL.INPUT.MISSING_DATAFRAME": _err(
        "VAL.INPUT.MISSING_DATAFRAME",
        "El TraceDataset no expone una tabla usable en traces.data para ejecutar la validación.",
        details_keys=("attribute", "reason", "action"),
        defaults={"attribute": "data", "action": "abort"},
        exception="validate",
        fatal=True,
    ),
    "VAL.OPTIONS.INVALID_SAMPLE_ROWS_PER_ISSUE": _err(
        "VAL.OPTIONS.INVALID_SAMPLE_ROWS_PER_ISSUE",
        "sample_rows_per_issue debe ser un entero positivo; se recibió {value!r}.",
        details_keys=("option", "value", "expected", "action"),
        defaults={
            "option": "sample_rows_per_issue",
            "expected": "positive int",
            "action": "abort",
        },
        exception="validate",
        fatal=True,
    ),
    "VAL.OPTIONS.INVALID_FLAG_VALUE": _err(
        "VAL.OPTIONS.INVALID_FLAG_VALUE",
        "La opción {option!r} tiene un valor inválido para validate_traces: {value!r}.",
        details_keys=("option", "value", "expected", "action"),
        defaults={"expected": "bool", "action": "abort"},
        exception="validate",
        fatal=True,
    ),

    # ------------------------------------------------------------------
    # SCHEMA / PRECHECK
    # ------------------------------------------------------------------
    "VAL.SCHEMA.MISSING_SCHEMA": _err(
        "VAL.SCHEMA.MISSING_SCHEMA",
        "El TraceDataset no tiene un TraceSchema interpretable; no es posible ejecutar validate_traces.",
        details_keys=("schema_present", "action"),
        defaults={"schema_present": False, "action": "abort"},
        exception="schema",
        fatal=True,
    ),
    "VAL.SCHEMA.UNKNOWN_DTYPE": _err(
        "VAL.SCHEMA.UNKNOWN_DTYPE",
        "El campo {field!r} declara un dtype no soportado por validate_traces: {dtype!r}.",
        details_keys=("field", "dtype", "allowed_dtypes", "action"),
        defaults={
            "allowed_dtypes": ("string", "int", "float", "datetime", "bool"),
            "action": "abort",
        },
        exception="schema",
        fatal=True,
    ),
    "VAL.SCHEMA.CATEGORICAL_NOT_ALLOWED": _err(
        "VAL.SCHEMA.CATEGORICAL_NOT_ALLOWED",
        "El dtype 'categorical' no está permitido en TraceSchema v1.1 (campo {field!r}).",
        details_keys=("field", "dtype", "allowed_dtypes", "action"),
        defaults={
            "dtype": "categorical",
            "allowed_dtypes": ("string", "int", "float", "datetime", "bool"),
            "action": "abort",
        },
        exception="schema",
        fatal=True,
    ),
    "VAL.SCHEMA.REQUIRED_FIELD_UNKNOWN": _err(
        "VAL.SCHEMA.REQUIRED_FIELD_UNKNOWN",
        "TraceSchema.required contiene campos que no existen en schema.fields: {unknown_required}.",
        details_keys=("unknown_required", "required", "schema_fields_sample", "schema_fields_total", "action"),
        defaults={"action": "abort"},
        exception="schema",
        fatal=True,
    ),
    "VAL.SCHEMA.UNKNOWN_CONSTRAINT": _err(
        "VAL.SCHEMA.UNKNOWN_CONSTRAINT",
        "El campo {field!r} declara una constraint no soportada por validate_traces: {constraint!r}.",
        details_keys=("field", "constraint", "allowed_constraints", "action"),
        defaults={
            "allowed_constraints": ("nullable", "range", "datetime", "pattern", "length", "unique"),
            "action": "abort",
        },
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
    "VAL.SCHEMA.CONSTRAINT_INVALID_FORMAT": _warn(
        "VAL.SCHEMA.CONSTRAINT_INVALID_FORMAT",
        "La constraint {constraint!r} del campo {field!r} está mal formada o incompleta; se omitirá ese check.",
        details_keys=("field", "dtype", "constraint", "constraint_value", "reason", "action"),
        defaults={"action": "skip_constraint"},
    ),
    "VAL.SCHEMA.REQUIRED_NULLABLE_CONFLICT": _err(
        "VAL.SCHEMA.REQUIRED_NULLABLE_CONFLICT",
        "El campo {field!r} es required=True pero declara nullable=True; el TraceSchema es inconsistente.",
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

    # ------------------------------------------------------------------
    # REQUIRED FIELDS
    # ------------------------------------------------------------------
    "VAL.REQUIRED.MISSING_COLUMN": _err(
        "VAL.REQUIRED.MISSING_COLUMN",
        "Falta el campo obligatorio {field!r} en traces.data; no se cumple el mínimo requerido de traces.",
        details_keys=("check", "field", "required_fields", "present_fields", "action"),
        defaults={"check": "required_fields", "action": "report_error"},
        exception="validate",
        fatal=False,
    ),
    "VAL.REQUIRED.NULL_IN_REQUIRED": _err(
        "VAL.REQUIRED.NULL_IN_REQUIRED",
        "Se detectaron valores nulos en el campo obligatorio {field!r}.",
        details_keys=_COMMON_ROW_DETAILS + ("field", "expected", "action"),
        defaults={
            "check": "required_fields",
            "expected": "non-null",
            "action": "report_error",
        },
        exception="validate",
        fatal=False,
    ),

    # ------------------------------------------------------------------
    # TYPES / FORMATS
    # ------------------------------------------------------------------
    "VAL.TYPES.UNPARSEABLE_VALUE": _err(
        "VAL.TYPES.UNPARSEABLE_VALUE",
        "Se detectaron valores no parseables o no interpretables en {field!r}.",
        details_keys=_COMMON_ROW_DETAILS + ("field", "expected", "raw_values_sample", "action"),
        defaults={"check": "types_and_formats", "action": "report_error"},
        exception="validate",
        fatal=False,
    ),

    # ------------------------------------------------------------------
    # CONSTRAINTS
    # ------------------------------------------------------------------
    "VAL.CONSTRAINTS.VIOLATION": _err(
        "VAL.CONSTRAINTS.VIOLATION",
        "Se detectaron violaciones de la constraint {constraint!r} en el campo {field!r}.",
        details_keys=_COMMON_ROW_DETAILS + ("field", "constraint", "expected", "action"),
        defaults={"check": "constraints", "action": "report_error"},
        exception="validate",
        fatal=False,
    ),

    # ------------------------------------------------------------------
    # TEMPORAL
    # ------------------------------------------------------------------
    "VAL.TEMPORAL.NON_MONOTONIC_TIME": _warn(
        "VAL.TEMPORAL.NON_MONOTONIC_TIME",
        "Se detectaron secuencias temporales no monotónicas por usuario en {field!r}.",
        details_keys=(
            "check",
            "field",
            "n_users_affected",
            "user_ids_sample",
            "n_violations",
            "row_indices_sample",
            "sample_rows",
            "action",
        ),
        defaults={
            "check": "monotonic_time_per_user",
            "field": "time_utc",
            "action": "report_warning",
        },
        exception="validate",
        fatal=False,
    ),
}