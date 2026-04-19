# -------------------------
# file: pylondrina/validation_traces.py
# -------------------------
from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import numpy as np
import pandas as pd
from pandas.api import types as ptypes

from pylondrina.datasets import TraceDataset
from pylondrina.errors import SchemaError, ValidationError
from pylondrina.issues.catalog_validate_traces import VALIDATE_TRACES_ISSUES
from pylondrina.issues.core import emit_and_maybe_raise, emit_issue
from pylondrina.reports import ConsistencyReport, Issue
from pylondrina.schema import TraceSchema

EXCEPTION_MAP_VALIDATE_TRACE = {
    "schema": SchemaError,
    "validate": ValidationError,
}

TRACE_CORE_FIELDS = ("point_id", "user_id", "time_utc", "latitude", "longitude")
TRACE_ALLOWED_DTYPES = {"string", "int", "float", "datetime", "bool"}
TRACE_ALLOWED_CONSTRAINTS = {"nullable", "range", "datetime", "pattern", "length", "unique"}
TRACE_CONSTRAINTS_BY_DTYPE = {
    "string": {"nullable", "pattern", "length", "unique"},
    "int": {"nullable", "range", "unique"},
    "float": {"nullable", "range", "unique"},
    "datetime": {"nullable", "datetime", "unique"},
    "bool": {"nullable", "unique"},
}
BOOL_TRUE = {True, 1, "1", "true", "t", "yes", "y"}
BOOL_FALSE = {False, 0, "0", "false", "f", "no", "n"}


@dataclass(frozen=True)
class TraceValidationOptions:
    """
    Opciones de validación para `TraceDataset` según el contrato vigente de v1.1.

    Attributes
    ----------
    strict : bool, default=False
        Si True, los errores de datos se escalan después de construir evidencia.
    sample_rows_per_issue : int, default=5
        Máximo de filas ejemplo a guardar dentro de `Issue.details`.
    validate_required_fields : bool, default=True
        Verifica presencia y nulabilidad del núcleo mínimo de traces.
    validate_types_and_formats : bool, default=True
        Verifica parseabilidad de tiempos y numéricos básicos.
    validate_constraints : bool, default=True
        Ejecuta constraints simples declarativas del TraceSchema.
    validate_monotonic_time_per_user : bool, default=True
        Verifica que `time_utc` no decrezca dentro de cada `user_id`.
    """

    strict: bool = False
    sample_rows_per_issue: int = 5

    validate_required_fields: bool = True
    validate_types_and_formats: bool = True
    validate_constraints: bool = True
    validate_monotonic_time_per_user: bool = True


def validate_traces(
    traces: TraceDataset,
    *,
    options: Optional[TraceValidationOptions] = None,
) -> ConsistencyReport:
    """
    Valida un TraceDataset contra su TraceSchema y el mínimo canónico de traces.

    La operación no muta `traces.data`, pero sí actualiza `traces.metadata`
    con evento `validate_traces` e `is_validated` coherente con el resultado.
    """
    issues: List[Issue] = []

    # Se normalizan temprano las options para fijar la política efectiva del validate.
    options_eff = _normalize_trace_validation_options(options)

    # Se ejecuta el preflight fatal antes de construir evidencia o tocar metadata.
    skipped_constraints = _preflight_validate_traces_request(
        issues,
        traces=traces,
        options_eff=options_eff,
    )

    # Se asegura recién ahora que metadata sea usable para registrar el resultado retornable.
    if not isinstance(traces.metadata, dict):
        traces.metadata = {}
    if not isinstance(traces.metadata.get("events"), list):
        traces.metadata["events"] = []

    # Se arma el contexto mínimo de validación sin tocar traces.data.
    required_fields, checked_fields, checks_executed, effective_nullable = _resolve_trace_validation_targets(
        traces,
        options_eff=options_eff,
    )

    # Se ejecuta el bloque de required + tipos/formatos sobre el dataframe observado.
    _check_trace_required_and_types(
        issues,
        traces.data,
        schema=traces.schema,
        required_fields=required_fields,
        checked_fields=checked_fields,
        options_eff=options_eff,
    )

    # Se ejecutan las constraints simples del schema, respetando skips de precheck.
    _check_trace_constraints(
        issues,
        traces.data,
        schema=traces.schema,
        required_fields=required_fields,
        effective_nullable=effective_nullable,
        skipped_constraints=skipped_constraints,
        options_eff=options_eff,
    )

    # Se evalúa monotonicidad temporal por usuario sobre el orden actual del dataframe.
    _check_trace_monotonic_time_per_user(
        issues,
        traces.data,
        options_eff=options_eff,
    )

    # Se consolida el reporte final y se actualizan metadata + evento en la ruta retornable.
    report = _finalize_trace_validation(
        issues,
        traces,
        options_eff=options_eff,
        checked_fields=checked_fields,
        checks_executed=checks_executed,
    )

    # Si strict=True y quedaron errores de datos, se escala después de construir evidencia.
    if options_eff.strict:
        first_error = next((issue for issue in issues if issue.level == "error"), None)
        if first_error is not None:
            raise ValidationError(
                first_error.message,
                code=first_error.code,
                details={
                    "summary": report.summary,
                    "event": traces.metadata.get("events", [])[-1] if traces.metadata.get("events") else None,
                },
                issue=first_error,
                issues=issues,
            )

    return report


# -----------------------------------------------------------------------------
# Helpers internos principales del pipeline de validación
# -----------------------------------------------------------------------------

def _normalize_trace_validation_options(
    options: Optional[TraceValidationOptions],
) -> TraceValidationOptions:
    """
    Normaliza TraceValidationOptions a una forma efectiva y explícita.

    Emite: ninguno.
    """
    options_eff = options or TraceValidationOptions()
    return TraceValidationOptions(
        strict=options_eff.strict,
        sample_rows_per_issue=options_eff.sample_rows_per_issue,
        validate_required_fields=options_eff.validate_required_fields,
        validate_types_and_formats=options_eff.validate_types_and_formats,
        validate_constraints=options_eff.validate_constraints,
        validate_monotonic_time_per_user=options_eff.validate_monotonic_time_per_user,
    )


def _preflight_validate_traces_request(
    issues: List[Issue],
    *,
    traces: TraceDataset,
    options_eff: TraceValidationOptions,
) -> Dict[str, set[str]]:
    """
    Ejecuta el preflight fatal de validate_traces y marca constraints a omitir.

    Emite: VAL.INPUT.INVALID_TRACES_OBJECT, VAL.INPUT.MISSING_DATAFRAME,
            VAL.OPTIONS.INVALID_SAMPLE_ROWS_PER_ISSUE, VAL.OPTIONS.INVALID_FLAG_VALUE,
            VAL.SCHEMA.MISSING_SCHEMA, VAL.SCHEMA.UNKNOWN_DTYPE,
            VAL.SCHEMA.CATEGORICAL_NOT_ALLOWED, VAL.SCHEMA.REQUIRED_FIELD_UNKNOWN,
            VAL.SCHEMA.UNKNOWN_CONSTRAINT, VAL.SCHEMA.CONSTRAINT_NOT_ALLOWED_FOR_DTYPE,
            VAL.SCHEMA.CONSTRAINT_INVALID_FORMAT, VAL.SCHEMA.REQUIRED_NULLABLE_CONFLICT.
    """
    if not isinstance(traces, TraceDataset):
        # Se emite error fatal porque validate_traces solo acepta TraceDataset como contrato de entrada.
        emit_and_maybe_raise(
            issues,
            VALIDATE_TRACES_ISSUES,
            "VAL.INPUT.INVALID_TRACES_OBJECT",
            strict=False,
            exception_map=EXCEPTION_MAP_VALIDATE_TRACE,
            default_exception=ValidationError,
            received_type=type(traces).__name__,
        )

    if not hasattr(traces, "data") or not isinstance(traces.data, pd.DataFrame):
        # Se emite error fatal porque la operación valida siempre sobre traces.data.
        emit_and_maybe_raise(
            issues,
            VALIDATE_TRACES_ISSUES,
            "VAL.INPUT.MISSING_DATAFRAME",
            strict=False,
            exception_map=EXCEPTION_MAP_VALIDATE_TRACE,
            default_exception=ValidationError,
            reason="missing_or_not_dataframe",
        )

    if not isinstance(options_eff.sample_rows_per_issue, int) or options_eff.sample_rows_per_issue <= 0:
        # Se emite error fatal porque el muestreo por issue debe ser un entero positivo.
        emit_and_maybe_raise(
            issues,
            VALIDATE_TRACES_ISSUES,
            "VAL.OPTIONS.INVALID_SAMPLE_ROWS_PER_ISSUE",
            strict=False,
            exception_map=EXCEPTION_MAP_VALIDATE_TRACE,
            default_exception=ValidationError,
            option="sample_rows_per_issue",
            value=options_eff.sample_rows_per_issue,
        )

    for option_name in (
        "strict",
        "validate_required_fields",
        "validate_types_and_formats",
        "validate_constraints",
        "validate_monotonic_time_per_user",
    ):
        option_value = getattr(options_eff, option_name)
        if not isinstance(option_value, bool):
            # Se emite error fatal porque las banderas de ejecución deben ser booleanas y no ambiguas.
            emit_and_maybe_raise(
                issues,
                VALIDATE_TRACES_ISSUES,
                "VAL.OPTIONS.INVALID_FLAG_VALUE",
                strict=False,
                exception_map=EXCEPTION_MAP_VALIDATE_TRACE,
                default_exception=ValidationError,
                option=option_name,
                value=option_value,
            )

    schema = getattr(traces, "schema", None)
    if not isinstance(schema, TraceSchema):
        # Se emite error fatal porque validate_traces necesita un TraceSchema asociado.
        emit_and_maybe_raise(
            issues,
            VALIDATE_TRACES_ISSUES,
            "VAL.SCHEMA.MISSING_SCHEMA",
            strict=False,
            exception_map=EXCEPTION_MAP_VALIDATE_TRACE,
            default_exception=SchemaError,
            schema_present=schema is not None,
        )

    schema_fields = schema.fields if isinstance(schema.fields, dict) else {}
    unknown_required = [name for name in schema.required if name not in schema_fields]
    if unknown_required:
        # Se emite error fatal porque schema.required no puede apuntar a campos fuera de schema.fields.
        emit_and_maybe_raise(
            issues,
            VALIDATE_TRACES_ISSUES,
            "VAL.SCHEMA.REQUIRED_FIELD_UNKNOWN",
            strict=False,
            exception_map=EXCEPTION_MAP_VALIDATE_TRACE,
            default_exception=SchemaError,
            unknown_required=unknown_required,
            required=list(schema.required),
            schema_fields_sample=_sample_list(schema_fields.keys(), 20),
            schema_fields_total=len(schema_fields),
        )

    skipped_constraints: Dict[str, set[str]] = {}
    for field_name, field_spec in schema_fields.items():
        dtype = getattr(field_spec, "dtype", None)
        if dtype == "categorical":
            # Se emite error fatal porque categorical quedó fuera del contrato de traces v1.1.
            emit_and_maybe_raise(
                issues,
                VALIDATE_TRACES_ISSUES,
                "VAL.SCHEMA.CATEGORICAL_NOT_ALLOWED",
                strict=False,
                exception_map=EXCEPTION_MAP_VALIDATE_TRACE,
                default_exception=SchemaError,
                field=field_name,
                dtype=dtype,
            )
        if dtype not in TRACE_ALLOWED_DTYPES:
            # Se emite error fatal porque el dtype no pertenece al subconjunto soportado por validate_traces.
            emit_and_maybe_raise(
                issues,
                VALIDATE_TRACES_ISSUES,
                "VAL.SCHEMA.UNKNOWN_DTYPE",
                strict=False,
                exception_map=EXCEPTION_MAP_VALIDATE_TRACE,
                default_exception=SchemaError,
                field=field_name,
                dtype=dtype,
            )

        constraints = getattr(field_spec, "constraints", None) or {}
        nullable_declared = constraints.get("nullable") if isinstance(constraints, dict) else None
        if bool(field_spec.required) and nullable_declared is True:
            # Se emite error fatal porque required=True y nullable=True es inconsistente a nivel de contrato.
            emit_and_maybe_raise(
                issues,
                VALIDATE_TRACES_ISSUES,
                "VAL.SCHEMA.REQUIRED_NULLABLE_CONFLICT",
                strict=False,
                exception_map=EXCEPTION_MAP_VALIDATE_TRACE,
                default_exception=SchemaError,
                field=field_name,
                required=bool(field_spec.required),
                nullable=nullable_declared,
            )

        for constraint_name, constraint_value in constraints.items():
            if constraint_name not in TRACE_ALLOWED_CONSTRAINTS:
                # Se emite error fatal porque la constraint no existe en el contrato vigente de traces.
                emit_and_maybe_raise(
                    issues,
                    VALIDATE_TRACES_ISSUES,
                    "VAL.SCHEMA.UNKNOWN_CONSTRAINT",
                    strict=False,
                    exception_map=EXCEPTION_MAP_VALIDATE_TRACE,
                    default_exception=SchemaError,
                    field=field_name,
                    constraint=constraint_name,
                )
            if constraint_name not in TRACE_CONSTRAINTS_BY_DTYPE.get(dtype, set()):
                # Se emite error fatal porque la constraint no aplica al dtype declarado.
                emit_and_maybe_raise(
                    issues,
                    VALIDATE_TRACES_ISSUES,
                    "VAL.SCHEMA.CONSTRAINT_NOT_ALLOWED_FOR_DTYPE",
                    strict=False,
                    exception_map=EXCEPTION_MAP_VALIDATE_TRACE,
                    default_exception=SchemaError,
                    field=field_name,
                    dtype=dtype,
                    constraint=constraint_name,
                )
            if not _is_valid_constraint_payload(dtype, constraint_name, constraint_value):
                skipped_constraints.setdefault(field_name, set()).add(constraint_name)
                # Se emite warning porque la constraint es conocida, pero su payload no es usable y se omitirá.
                emit_issue(
                    issues,
                    VALIDATE_TRACES_ISSUES,
                    "VAL.SCHEMA.CONSTRAINT_INVALID_FORMAT",
                    field=field_name,
                    dtype=dtype,
                    constraint=constraint_name,
                    constraint_value=_json_safe(constraint_value),
                    reason="invalid_constraint_payload",
                )

    return skipped_constraints


def _resolve_trace_validation_targets(
    traces: TraceDataset,
    *,
    options_eff: TraceValidationOptions,
) -> tuple[List[str], List[str], Dict[str, bool], Dict[str, bool]]:
    """
    Construye el contexto mínimo de validación sin tocar traces.data.

    Emite: ninguno.
    """
    df = traces.data
    schema = traces.schema

    required_effective = list(dict.fromkeys(list(TRACE_CORE_FIELDS) + list(schema.required or [])))
    checked_fields = list(
        dict.fromkeys(
            [name for name in required_effective if name in df.columns]
            + [name for name in schema.fields.keys() if name in df.columns]
        )
    )
    checks_executed = {
        "required_fields": bool(options_eff.validate_required_fields),
        "types_and_formats": bool(options_eff.validate_types_and_formats),
        "constraints": bool(options_eff.validate_constraints),
        "monotonic_time_per_user": bool(options_eff.validate_monotonic_time_per_user),
    }

    effective_nullable: Dict[str, bool] = {}
    for field_name, field_spec in schema.fields.items():
        constraints = field_spec.constraints or {}
        if "nullable" in constraints:
            effective_nullable[field_name] = bool(constraints["nullable"])
        else:
            effective_nullable[field_name] = not bool(field_spec.required)

    for field_name in TRACE_CORE_FIELDS:
        effective_nullable.setdefault(field_name, field_name not in required_effective)

    return required_effective, checked_fields, checks_executed, effective_nullable


def _check_trace_required_and_types(
    issues: List[Issue],
    df: pd.DataFrame,
    *,
    schema: TraceSchema,
    required_fields: Sequence[str],
    checked_fields: Sequence[str],
    options_eff: TraceValidationOptions,
) -> None:
    """
    Ejecuta required fields y types/formats sobre traces.data.

    Emite: VAL.REQUIRED.MISSING_COLUMN, VAL.REQUIRED.NULL_IN_REQUIRED,
            VAL.TYPES.UNPARSEABLE_VALUE.
    """
    if options_eff.validate_required_fields:
        for field_name in required_fields:
            if field_name not in df.columns:
                # Se emite error porque un campo obligatorio del mínimo de traces no está presente.
                emit_issue(
                    issues,
                    VALIDATE_TRACES_ISSUES,
                    "VAL.REQUIRED.MISSING_COLUMN",
                    field=field_name,
                    required_fields=list(required_fields),
                    present_fields=list(df.columns),
                )
                continue

            null_mask = df[field_name].isna()
            if null_mask.any():
                # Se emite error porque un campo obligatorio no puede contener nulos en las filas observadas.
                emit_issue(
                    issues,
                    VALIDATE_TRACES_ISSUES,
                    "VAL.REQUIRED.NULL_IN_REQUIRED",
                    field=field_name,
                    row_count=int(null_mask.sum()),
                    n_rows_total=len(df),
                    n_violations=int(null_mask.sum()),
                    row_indices_sample=_sample_index_list(df.index[null_mask], options_eff.sample_rows_per_issue),
                    sample_rows=_sample_rows(df, null_mask, options_eff.sample_rows_per_issue),
                )

    if not options_eff.validate_types_and_formats:
        return

    core_dtype_map = {
        "point_id": "string",
        "user_id": "string",
        "time_utc": "datetime",
        "latitude": "float",
        "longitude": "float",
    }
    for field_name in checked_fields:
        if field_name not in df.columns:
            continue

        field_spec = schema.fields.get(field_name)
        dtype = field_spec.dtype if field_spec is not None else core_dtype_map.get(field_name)
        if dtype is None or dtype == "string":
            continue

        invalid_mask, raw_values_sample = _invalid_mask_for_dtype(df[field_name], dtype)
        if invalid_mask.any():
            # Se emite error porque el campo observado no es interpretable según el dtype lógico esperado.
            emit_issue(
                issues,
                VALIDATE_TRACES_ISSUES,
                "VAL.TYPES.UNPARSEABLE_VALUE",
                field=field_name,
                row_count=int(invalid_mask.sum()),
                n_rows_total=len(df),
                n_violations=int(invalid_mask.sum()),
                row_indices_sample=_sample_index_list(df.index[invalid_mask], options_eff.sample_rows_per_issue),
                sample_rows=_sample_rows(df, invalid_mask, options_eff.sample_rows_per_issue),
                expected=dtype,
                raw_values_sample=raw_values_sample,
            )


def _check_trace_constraints(
    issues: List[Issue],
    df: pd.DataFrame,
    *,
    schema: TraceSchema,
    required_fields: Sequence[str],
    effective_nullable: Mapping[str, bool],
    skipped_constraints: Mapping[str, set[str]],
    options_eff: TraceValidationOptions,
) -> None:
    """
    Ejecuta constraints simples declarativas por campo según el TraceSchema.

    Emite: VAL.CONSTRAINTS.VIOLATION.
    """
    if not options_eff.validate_constraints:
        return

    for field_name, field_spec in schema.fields.items():
        if field_name not in df.columns:
            continue

        constraints = field_spec.constraints or {}
        for constraint_name, constraint_value in constraints.items():
            if constraint_name in skipped_constraints.get(field_name, set()):
                continue

            if constraint_name == "nullable":
                if field_name in required_fields:
                    continue
                if not effective_nullable.get(field_name, True):
                    null_mask = df[field_name].isna()
                    if null_mask.any():
                        # Se emite error porque el campo declaró nullable=False y aun así contiene nulos.
                        emit_issue(
                            issues,
                            VALIDATE_TRACES_ISSUES,
                            "VAL.CONSTRAINTS.VIOLATION",
                            field=field_name,
                            row_count=int(null_mask.sum()),
                            n_rows_total=len(df),
                            n_violations=int(null_mask.sum()),
                            row_indices_sample=_sample_index_list(df.index[null_mask], options_eff.sample_rows_per_issue),
                            sample_rows=_sample_rows(df, null_mask, options_eff.sample_rows_per_issue),
                            constraint="nullable",
                            expected={"nullable": False},
                        )
                continue

            violation_mask = pd.Series(False, index=df.index)
            expected: Any = None

            if constraint_name == "range":
                if field_spec.dtype == "datetime":
                    parsed = pd.to_datetime(df[field_name], errors="coerce", utc=False)
                    min_value = pd.to_datetime(constraint_value.get("min"), errors="coerce", utc=False) if isinstance(constraint_value, Mapping) and constraint_value.get("min") is not None else None
                    max_value = pd.to_datetime(constraint_value.get("max"), errors="coerce", utc=False) if isinstance(constraint_value, Mapping) and constraint_value.get("max") is not None else None
                    if min_value is not None:
                        violation_mask |= parsed.notna() & (parsed < min_value)
                    if max_value is not None:
                        violation_mask |= parsed.notna() & (parsed > max_value)
                else:
                    numeric = pd.to_numeric(df[field_name], errors="coerce")
                    min_value = constraint_value.get("min") if isinstance(constraint_value, Mapping) else None
                    max_value = constraint_value.get("max") if isinstance(constraint_value, Mapping) else None
                    if min_value is not None:
                        violation_mask |= numeric.notna() & (numeric < min_value)
                    if max_value is not None:
                        violation_mask |= numeric.notna() & (numeric > max_value)
                expected = _json_safe(constraint_value)

            elif constraint_name == "datetime":
                parsed = pd.to_datetime(df[field_name], errors="coerce", utc=False)
                allow_naive = True
                if isinstance(constraint_value, Mapping):
                    allow_naive = bool(constraint_value.get("allow_naive", True))
                    min_value = pd.to_datetime(constraint_value.get("min"), errors="coerce", utc=False) if constraint_value.get("min") is not None else None
                    max_value = pd.to_datetime(constraint_value.get("max"), errors="coerce", utc=False) if constraint_value.get("max") is not None else None
                else:
                    min_value = None
                    max_value = None
                if min_value is not None:
                    violation_mask |= parsed.notna() & (parsed < min_value)
                if max_value is not None:
                    violation_mask |= parsed.notna() & (parsed > max_value)
                if not allow_naive and ptypes.is_datetime64_any_dtype(df[field_name]) and not ptypes.is_datetime64tz_dtype(df[field_name]):
                    violation_mask |= df[field_name].notna()
                expected = _json_safe(constraint_value)

            elif constraint_name == "pattern":
                pattern = str(constraint_value)
                compiled = re.compile(pattern)
                values = df[field_name].dropna().astype(str)
                violation_index = values.index[~values.str.fullmatch(compiled)]
                violation_mask.loc[violation_index] = True
                expected = {"pattern": pattern}

            elif constraint_name == "length":
                values = df[field_name].dropna().astype(str)
                lengths = values.str.len()
                min_value = constraint_value.get("min") if isinstance(constraint_value, Mapping) else None
                max_value = constraint_value.get("max") if isinstance(constraint_value, Mapping) else None
                violation_index = pd.Index([])
                if min_value is not None:
                    violation_index = violation_index.union(lengths.index[lengths < int(min_value)])
                if max_value is not None:
                    violation_index = violation_index.union(lengths.index[lengths > int(max_value)])
                violation_mask.loc[violation_index] = True
                expected = _json_safe(constraint_value)

            elif constraint_name == "unique":
                if bool(constraint_value if not isinstance(constraint_value, Mapping) else constraint_value.get("value", True)):
                    duplicated_mask = df[field_name].notna() & df[field_name].duplicated(keep=False)
                    violation_mask |= duplicated_mask
                expected = {"unique": True}

            if violation_mask.any():
                # Se emite error porque el campo violó una constraint declarativa del TraceSchema.
                emit_issue(
                    issues,
                    VALIDATE_TRACES_ISSUES,
                    "VAL.CONSTRAINTS.VIOLATION",
                    field=field_name,
                    row_count=int(violation_mask.sum()),
                    n_rows_total=len(df),
                    n_violations=int(violation_mask.sum()),
                    row_indices_sample=_sample_index_list(df.index[violation_mask], options_eff.sample_rows_per_issue),
                    sample_rows=_sample_rows(df, violation_mask, options_eff.sample_rows_per_issue),
                    constraint=constraint_name,
                    expected=expected,
                )


def _check_trace_monotonic_time_per_user(
    issues: List[Issue],
    df: pd.DataFrame,
    *,
    options_eff: TraceValidationOptions,
) -> None:
    """
    Ejecuta la regla temporal de monotonicidad por usuario sobre el orden observado.

    Emite: VAL.TEMPORAL.NON_MONOTONIC_TIME.
    """
    if not options_eff.validate_monotonic_time_per_user:
        return
    if "user_id" not in df.columns or "time_utc" not in df.columns:
        return

    parsed = pd.to_datetime(df["time_utc"], errors="coerce", utc=False)
    valid_mask = df["user_id"].notna() & parsed.notna()
    if not valid_mask.any():
        return

    work = pd.DataFrame(
        {
            "_user_id": df["user_id"],
            "_time_utc": parsed,
        },
        index=df.index,
    )
    work = work.loc[valid_mask]
    previous = work.groupby("_user_id", sort=False)["_time_utc"].shift(1)
    violation_mask = previous.notna() & (work["_time_utc"] < previous)
    if violation_mask.any():
        affected_index = work.index[violation_mask]
        affected_users = work.loc[affected_index, "_user_id"].astype(str)
        # Se emite warning porque el orden observado por usuario retrocede en el tiempo y eso afecta la trazabilidad temporal.
        emit_issue(
            issues,
            VALIDATE_TRACES_ISSUES,
            "VAL.TEMPORAL.NON_MONOTONIC_TIME",
            field="time_utc",
            row_count=int(violation_mask.sum()),
            n_users_affected=int(affected_users.nunique()),
            user_ids_sample=_sample_list(affected_users.tolist(), options_eff.sample_rows_per_issue),
            n_violations=int(violation_mask.sum()),
            row_indices_sample=_sample_index_list(affected_index, options_eff.sample_rows_per_issue),
            sample_rows=_sample_rows(df, df.index.isin(affected_index), options_eff.sample_rows_per_issue),
        )


def _finalize_trace_validation(
    issues: List[Issue],
    traces: TraceDataset,
    *,
    options_eff: TraceValidationOptions,
    checked_fields: Sequence[str],
    checks_executed: Mapping[str, bool],
) -> ConsistencyReport:
    """
    Consolida summary, actualiza metadata y registra el evento `validate_traces`.

    Emite: ninguno.
    """
    counts_by_level = {
        "error": sum(1 for issue in issues if issue.level == "error"),
        "warning": sum(1 for issue in issues if issue.level == "warning"),
        "info": sum(1 for issue in issues if issue.level == "info"),
    }
    counts_by_code = Counter(issue.code for issue in issues)
    ok = counts_by_level["error"] == 0

    summary = {
        "ok": ok,
        "n_rows": int(len(traces.data)),
        "n_issues": int(len(issues)),
        "n_errors": int(counts_by_level["error"]),
        "n_warnings": int(counts_by_level["warning"]),
        "n_info": int(counts_by_level["info"]),
        "counts_by_level": counts_by_level,
        "counts_by_code": dict(counts_by_code),
        "checked_fields": list(dict.fromkeys(checked_fields)),
        "checks_executed": dict(checks_executed),
        "schema_version": getattr(traces.schema, "version", None),
    }

    traces.metadata["is_validated"] = bool(ok)
    if not isinstance(traces.metadata.get("events"), list):
        traces.metadata["events"] = []
    event = {
        "op": "validate_traces",
        "ts_utc": _utc_now_iso(),
        "parameters": _json_safe(asdict(options_eff)),
        "summary": _json_safe(summary),
        "issues_summary": _build_issues_summary(issues),
    }
    traces.metadata["events"].append(event)

    return ConsistencyReport(
        issues=list(issues),
        summary=_json_safe(summary),
    )


# -----------------------------------------------------------------------------
# Helpers internos de uso general
# -----------------------------------------------------------------------------

def _is_valid_constraint_payload(dtype: str, constraint_name: str, constraint_value: Any) -> bool:
    """Indica si el payload de una constraint conocida tiene forma suficiente para ejecutarse."""
    if constraint_name == "nullable":
        return isinstance(constraint_value, bool)
    if constraint_name == "unique":
        return isinstance(constraint_value, bool) or (
            isinstance(constraint_value, Mapping) and isinstance(constraint_value.get("value", True), bool)
        )
    if constraint_name == "range":
        return isinstance(constraint_value, Mapping) and ("min" in constraint_value or "max" in constraint_value)
    if constraint_name == "datetime":
        if not isinstance(constraint_value, Mapping):
            return False
        allowed = {"timezone", "allow_naive", "min", "max"}
        return set(constraint_value.keys()).issubset(allowed)
    if constraint_name == "pattern":
        if not isinstance(constraint_value, str):
            return False
        try:
            re.compile(constraint_value)
            return True
        except re.error:
            return False
    if constraint_name == "length":
        return isinstance(constraint_value, Mapping) and ("min" in constraint_value or "max" in constraint_value)
    return True


def _invalid_mask_for_dtype(series: pd.Series, dtype: str) -> tuple[pd.Series, List[Any]]:
    """Devuelve la máscara de valores no interpretables según el dtype lógico esperado."""
    non_null = series.notna()
    if dtype == "int":
        parsed = pd.to_numeric(series, errors="coerce")
        invalid_mask = non_null & (parsed.isna() | (parsed % 1 != 0))
    elif dtype == "float":
        parsed = pd.to_numeric(series, errors="coerce")
        invalid_mask = non_null & parsed.isna()
    elif dtype == "datetime":
        parsed = pd.to_datetime(series, errors="coerce", utc=False)
        invalid_mask = non_null & parsed.isna()
    elif dtype == "bool":
        normalized = series.dropna().map(lambda value: str(value).strip().lower() if not isinstance(value, bool) else value)
        invalid_index = []
        true_text = {str(v).lower() for v in BOOL_TRUE}
        false_text = {str(v).lower() for v in BOOL_FALSE}
        for idx, value in normalized.items():
            if value in true_text or value in false_text or isinstance(value, bool):
                continue
            invalid_index.append(idx)
        invalid_mask = pd.Series(False, index=series.index)
        if invalid_index:
            invalid_mask.loc[invalid_index] = True
    else:
        invalid_mask = pd.Series(False, index=series.index)
    return invalid_mask, _sample_list(series[invalid_mask].tolist(), 10)


def _sample_rows(df: pd.DataFrame, mask: pd.Series | Sequence[bool], limit: int) -> List[Dict[str, Any]]:
    """Devuelve una muestra compacta de filas afectadas en una forma JSON-safe."""
    if not isinstance(mask, pd.Series):
        mask = pd.Series(mask, index=df.index)
    sampled_index = list(df.index[mask])[:limit]
    return [_json_safe_row(df.loc[idx].to_dict()) for idx in sampled_index]


def _sample_index_list(index_values: Sequence[Any], limit: int) -> List[Any]:
    """Devuelve una muestra estable de índices del DataFrame."""
    return [_json_safe_scalar(v) for v in list(index_values)[:limit]]


def _sample_list(values: Iterable[Any], limit: int) -> List[Any]:
    """Devuelve una muestra simple y serializable de cualquier secuencia."""
    out: List[Any] = []
    for value in values:
        out.append(_json_safe_scalar(value))
        if len(out) >= limit:
            break
    return out


def _build_issues_summary(issues: Sequence[Issue]) -> Dict[str, Any]:
    """Construye un resumen pequeño y estable de issues para el evento de validate."""
    level_counts = Counter(issue.level for issue in issues)
    code_counts = Counter(issue.code for issue in issues)
    top_codes = sorted(code_counts.items(), key=lambda item: (-item[1], item[0]))[:10]
    return {
        "counts": {
            "info": int(level_counts.get("info", 0)),
            "warning": int(level_counts.get("warning", 0)),
            "error": int(level_counts.get("error", 0)),
        },
        "top_codes": [{"code": code, "count": count} for code, count in top_codes],
    }


def _utc_now_iso() -> str:
    """Retorna un timestamp UTC ISO-8601 compacto con sufijo Z."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _json_safe_scalar(value: Any) -> Any:
    """Convierte escalares frecuentes de pandas/numpy a una forma JSON-safe."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    return value


def _json_safe(value: Any) -> Any:
    """Convierte recursivamente estructuras comunes a una forma JSON-safe."""
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    return _json_safe_scalar(value)


def _json_safe_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    """Convierte una fila dict-like a una forma JSON-safe y compacta."""
    return {str(key): _json_safe(value) for key, value in row.items()}