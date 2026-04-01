# -------------------------
# file: pylondrina/validation.py
# -------------------------
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Literal

import numpy as np
import pandas as pd
from pandas.api import types as ptypes
import h3

from pylondrina.datasets import TripDataset
from pylondrina.errors import SchemaError, ValidationError
from pylondrina.issues.catalog_validate_trips import VALIDATE_TRIPS_ISSUES
from pylondrina.issues.core import emit_and_maybe_raise, emit_issue
from pylondrina.reports import Issue, ValidationReport
from pylondrina.schema import (
    CONSTRAINTS_BY_DTYPE,
    VALID_CONSTRAINT_KEYS,
    VALID_DTYPES,
    DomainSpec,
    FieldSpec,
    TripSchema,
)

DomainValidationMode = Literal["off", "full", "sample"]
DuplicatesSubset = Optional[Tuple[str, ...]]

EXCEPTION_MAP_VALIDATE = {
    "schema": SchemaError,
    "validate": ValidationError,
}

CONSTRAINT_ALLOWED_PARAMS = {
    "nullable": ["value"],
    "range": ["min", "max"],
    "datetime": ["timezone", "allow_naive", "min", "max"],
    "h3": ["require_valid", "resolution", "allow_mixed_resolution"],
    "pattern": ["value"],
    "length": ["min", "max"],
    "unique": ["value"],
}

OD_COORDINATE_FIELDS = (
    "origin_latitude",
    "origin_longitude",
    "destination_latitude",
    "destination_longitude",
)


@dataclass(frozen=True)
class ValidationOptions:
    """
    Opciones de ejecución para `validate_trips`.

    Attributes
    ----------
    strict : bool, default=False
        Si es `True`, la operación lanza `ValidationError` cuando el reporte
        contiene al menos un issue de nivel `"error"`. Antes de lanzar, igual
        construye el `ValidationReport`, registra el evento `validate_trips`
        y actualiza `metadata["is_validated"]`.

        Si es `False`, la operación retorna normalmente un `ValidationReport`,
        aunque `report.ok` sea `False`.

    max_issues : int, default=500
        Máximo de issues que se emiten en el reporte final. Si se supera este
        límite, el reporte se trunca y se agrega el issue especial de
        truncamiento correspondiente.

    sample_rows_per_issue : int, default=5
        Cantidad máxima de ejemplos por issue que se guardan en
        `Issue.details`, por ejemplo índices de fila o valores de muestra.

    validate_required_fields : bool, default=True
        Si es `True`, verifica la presencia de columnas requeridas según
        `trips.schema.required`.

    validate_types_and_formats : bool, default=True
        Si es `True`, verifica tipos y formatos básicos según el `dtype`
        declarado en el schema, sin modificar el dataframe.

    validate_constraints : bool, default=True
        Si es `True`, ejecuta constraints simples declarativas definidas en
        el schema. Esto incluye:
        - nullabilidad efectiva por campo,
        - constraints como `range`, `pattern`, `length`, `datetime`, `h3`,
          `unique`,
        - y la regla especial de OD parcial si corresponde.

    validate_domains : {"off", "full", "sample"}, default="off"
        Modo de validación de dominios categóricos.

        - `"off"`: no valida dominios.
        - `"full"`: valida todos los valores no nulos.
        - `"sample"`: valida una muestra de valores no nulos.

    domains_sample_frac : float, default=0.01
        Fracción de filas a usar cuando `validate_domains="sample"`.

    domains_min_in_domain_ratio : float, default=1.0
        Proporción mínima de valores no nulos que deben pertenecer al dominio
        para considerar exitoso el check de dominio.

        - Si el ratio observado es menor, se emite issue de nivel `"error"`.
        - Si cumple el mínimo pero no alcanza cobertura total, se puede emitir
          un `"warning"`.

    validate_temporal_consistency : bool, default=False
        Si es `True`, ejecuta el check temporal v1.1 sobre datasets con
        temporalidad Tier 1, verificando que `origin_time_utc <= destination_time_utc`
        cuando ambos campos existen y son comparables.

    validate_duplicates : bool, default=False
        Si es `True`, ejecuta detección de duplicados usando el subconjunto
        explícito definido en `duplicates_subset`.

    duplicates_subset : tuple[str, ...] | None, default=None
        Subconjunto de columnas a usar para detectar duplicados.

        En v1.1, este parámetro es obligatorio cuando
        `validate_duplicates=True`. Si falta, está vacío o contiene columnas
        inexistentes, la operación aborta por config inválida.

    allow_partial_od_spatial : bool, default=True
        Si es `True`, aplica la regla especial de OD parcial sobre las
        coordenadas canónicas:

        - `origin_latitude`
        - `origin_longitude`
        - `destination_latitude`
        - `destination_longitude`

        Bajo esta regla, una fila es válida si tiene un origen completo
        o un destino completo. Solo se considera inválida si faltan ambos
        extremos espaciales.

        Esta excepción aplica solo a coordenadas, no a H3.
    """

    strict: bool = False
    max_issues: int = 500
    sample_rows_per_issue: int = 5

    validate_required_fields: bool = True
    validate_types_and_formats: bool = True
    validate_constraints: bool = True

    validate_domains: DomainValidationMode = "off"
    domains_sample_frac: float = 0.01
    domains_min_in_domain_ratio: float = 1.0

    validate_temporal_consistency: bool = False

    validate_duplicates: bool = False
    duplicates_subset: DuplicatesSubset = None

    allow_partial_od_spatial: bool = True


def validate_trips(
    trips: TripDataset,
    *,
    options: Optional[ValidationOptions] = None,
) -> ValidationReport:
    """
    Valida un `TripDataset` contra su `TripSchema` y reglas mínimas de conformidad.

    Parameters
    ----------
    trips : TripDataset
        Dataset en formato Golondrina.
    options : ValidationOptions, optional
        Opciones de validación. Si es None, se usan los defaults vigentes.

    Returns
    -------
    ValidationReport
        Reporte estructurado de validación.

    Raises
    ------
    SchemaError
        Si el schema es ausente, inconsistente o no interpretable para ejecutar
        la validación.

    ValidationError
        Si la configuración es inválida y la operación no puede ejecutarse,
        o si `strict=True` y la validación detecta errores de nivel `"error"`.

    Notes
    -----
    Reglas operativas relevantes de v1.1:

    - La operación no muta `trips.data`.
    - No crea ni regenera `dataset_id` ni `artifact_id`.
    - Registra un evento `validate_trips` en `trips.metadata["events"]`.
    - Actualiza `trips.metadata["is_validated"]` según `report.ok`.
    - Si `strict=True`, primero registra evidencia y luego lanza excepción.
    - Los abortos de schema/config ocurren antes del pipeline normal y no
      dependen de `strict`.
    """
    issues: List[Issue] = []
    options_eff = options if options is not None else ValidationOptions()

    # Se verifica temprano que el input sea realmente un TripDataset usable.
    if not isinstance(trips, TripDataset):
        # Se emite error fatal porque sin TripDataset no existe contrato interpretable.
        emit_and_maybe_raise(
            issues,
            VALIDATE_TRIPS_ISSUES,
            "VAL.INPUT.INVALID_TRIPS_OBJECT",
            strict=False,
            exception_map=EXCEPTION_MAP_VALIDATE,
            default_exception=ValidationError,
            received_type=type(trips).__name__,
        )

    # Se asegura que exista una tabla principal usable para validar.
    if not hasattr(trips, "data") or not isinstance(trips.data, pd.DataFrame):
        # Se emite error fatal porque la operación valida siempre sobre trips.data.
        emit_and_maybe_raise(
            issues,
            VALIDATE_TRIPS_ISSUES,
            "VAL.INPUT.MISSING_DATAFRAME",
            strict=False,
            exception_map=EXCEPTION_MAP_VALIDATE,
            default_exception=ValidationError,
            reason="missing_or_not_dataframe",
        )

    # Se asegura que metadata exista y sea mutable antes de registrar evidencia.
    if not isinstance(trips.metadata, dict):
        trips.metadata = {}
    if not isinstance(trips.metadata.get("events"), list):
        trips.metadata["events"] = []

    df = trips.data
    schema = getattr(trips, "schema", None)
    schema_effective = getattr(trips, "schema_effective", None)

    # Se valida que el schema base exista y sea interpretable.
    if not isinstance(schema, TripSchema):
        # Se emite error fatal porque la validación necesita un TripSchema base.
        emit_and_maybe_raise(
            issues,
            VALIDATE_TRIPS_ISSUES,
            "VAL.SCHEMA.MISSING_SCHEMA",
            strict=False,
            exception_map=EXCEPTION_MAP_VALIDATE,
            default_exception=SchemaError,
            schema_present=schema is not None,
        )

    # Se verifica que el catálogo de fields no venga vacío.
    if not isinstance(schema.fields, dict) or len(schema.fields) == 0:
        # Se emite error fatal porque no hay contrato ejecutable de campos.
        emit_and_maybe_raise(
            issues,
            VALIDATE_TRIPS_ISSUES,
            "VAL.SCHEMA.EMPTY_FIELDS",
            strict=False,
            exception_map=EXCEPTION_MAP_VALIDATE,
            default_exception=SchemaError,
            schema_version=getattr(schema, "version", None),
            fields_size=0 if not isinstance(getattr(schema, "fields", None), dict) else len(schema.fields),
        )

    schema_fields = set(schema.fields.keys())
    required_fields = list(getattr(schema, "required", []) or [])
    unknown_required = sorted([name for name in required_fields if name not in schema_fields])
    if unknown_required:
        # Se emite error fatal porque el schema se contradice a sí mismo.
        emit_and_maybe_raise(
            issues,
            VALIDATE_TRIPS_ISSUES,
            "VAL.SCHEMA.REQUIRED_FIELD_UNKNOWN",
            strict=False,
            exception_map=EXCEPTION_MAP_VALIDATE,
            default_exception=SchemaError,
            unknown_required=unknown_required,
            required=required_fields,
            schema_fields_sample=_sample_list(sorted(schema_fields), 10),
            schema_fields_total=len(schema_fields),
        )

    # Se endurecen los bordes críticos de options antes de ejecutar checks.
    _validate_options_or_abort(issues, options_eff)

    # Se valida la configuración de duplicados solo si el check quedó activado.
    if options_eff.validate_duplicates:
        if options_eff.duplicates_subset is None:
            # Se emite error fatal porque duplicates requiere subset explícito en v1.1.
            emit_and_maybe_raise(
                issues,
                VALIDATE_TRIPS_ISSUES,
                "VAL.CONFIG.DUPLICATES_SUBSET_NOT_PROVIDED",
                strict=False,
                exception_map=EXCEPTION_MAP_VALIDATE,
                default_exception=ValidationError,
                duplicates_subset=None,
            )
        if len(options_eff.duplicates_subset) == 0:
            # Se emite error fatal porque un subset vacío no tiene semántica útil.
            emit_and_maybe_raise(
                issues,
                VALIDATE_TRIPS_ISSUES,
                "VAL.CONFIG.DUPLICATES_SUBSET_EMPTY",
                strict=False,
                exception_map=EXCEPTION_MAP_VALIDATE,
                default_exception=ValidationError,
                duplicates_subset=list(options_eff.duplicates_subset),
            )
        unknown_dup_fields = sorted([name for name in options_eff.duplicates_subset if name not in df.columns])
        if unknown_dup_fields:
            # Se emite error fatal porque el subset apunta a columnas inexistentes del dataset.
            emit_and_maybe_raise(
                issues,
                VALIDATE_TRIPS_ISSUES,
                "VAL.CONFIG.DUPLICATES_SUBSET_UNKNOWN_FIELD",
                strict=False,
                exception_map=EXCEPTION_MAP_VALIDATE,
                default_exception=ValidationError,
                duplicates_subset=list(options_eff.duplicates_subset),
                unknown_fields=unknown_dup_fields,
                available_columns_sample=_sample_list(list(df.columns), 15),
                available_columns_total=len(df.columns),
            )

    # Se valida la sanidad declarativa del schema y, en paralelo, se identifican constraints a omitir.
    skipped_constraints = _validate_schema_and_collect_skips(
        issues,
        schema=schema,
        strict=options_eff.strict,
    )

    # Se resuelven reglas efectivas que usarán los checks posteriores.
    effective_nullable_by_field = _build_effective_nullable_by_field(schema)
    effective_domains_by_field = _build_effective_domains_by_field(
        schema=schema,
        schema_effective=schema_effective,
        metadata=trips.metadata,
    )
    temporal_context = _build_temporal_context(df=df, metadata=trips.metadata, schema_effective=schema_effective)

    checks_executed = {
        "required_fields": False,
        "types_and_formats": False,
        "constraints": False,
        "domains": False,
        "temporal_consistency": False,
        "duplicates": False,
    }
    checked_fields: set[str] = set()

    # Se ejecuta el bloque de columnas requeridas si quedó activo.
    if options_eff.validate_required_fields:
        schema_for_required = _build_schema_for_required_check(
            schema=schema,
            df_columns=df.columns,
            allow_partial_od_spatial=options_eff.allow_partial_od_spatial,
        )
        required_issues = check_required_columns(df, schema=schema_for_required)
        issues.extend(required_issues)
        checks_executed["required_fields"] = True
        checked_fields.update([f for f in schema_for_required.required if f in df.columns])

    # Se ejecuta el bloque de constraints simples y nullabilidad efectiva.
    if options_eff.validate_constraints:
        constraint_issues = check_constraints(
            df,
            schema=schema,
            effective_nullable_by_field=effective_nullable_by_field,
            allow_partial_od_spatial=options_eff.allow_partial_od_spatial,
            skipped_constraints=skipped_constraints,
            sample_rows_per_issue=options_eff.sample_rows_per_issue,
        )
        issues.extend(constraint_issues)
        checks_executed["constraints"] = True
        checked_fields.update([name for name in schema.fields if name in df.columns])

    # Se ejecuta el bloque de tipos y formatos básicos sin mutar el dataframe.
    if options_eff.validate_types_and_formats:
        type_issues = check_types_and_formats(
            df,
            schema=schema,
            sample_rows_per_issue=options_eff.sample_rows_per_issue,
        )
        issues.extend(type_issues)
        checks_executed["types_and_formats"] = True
        checked_fields.update([name for name in schema.fields if name in df.columns])

    # Se ejecuta la validación de dominios según el modo configurado.
    domains_block: Optional[dict[str, Any]] = None
    if options_eff.validate_domains != "off":
        domain_issues, domains_block = check_domains(
            df,
            schema=schema,
            effective_domains_by_field=effective_domains_by_field,
            mode=options_eff.validate_domains,
            sample_frac=options_eff.domains_sample_frac,
            min_in_domain_ratio=options_eff.domains_min_in_domain_ratio,
            sample_rows_per_issue=options_eff.sample_rows_per_issue,
        )
        issues.extend(domain_issues)
        checks_executed["domains"] = True
        checked_fields.update([name for name, fs in schema.fields.items() if fs.dtype == "categorical" and name in df.columns])

    # Se ejecuta la consistencia temporal solo cuando el contrato la habilita y hay tier 1 utilizable.
    temporal_block: Optional[dict[str, Any]] = None
    if options_eff.validate_temporal_consistency:
        temporal_issues, temporal_block = check_temporal_consistency(
            df,
            temporal_context=temporal_context,
            sample_rows_per_issue=options_eff.sample_rows_per_issue,
        )
        issues.extend(temporal_issues)
        checks_executed["temporal_consistency"] = True
        for field_name in ("origin_time_utc", "destination_time_utc"):
            if field_name in df.columns:
                checked_fields.add(field_name)

    # Se ejecuta el check de duplicados solo cuando existe subset ya validado.
    duplicates_block: Optional[dict[str, Any]] = None
    if options_eff.validate_duplicates:
        duplicate_issues, duplicates_block = check_duplicates(
            df,
            duplicates_subset=options_eff.duplicates_subset,
            sample_rows_per_issue=options_eff.sample_rows_per_issue,
        )
        issues.extend(duplicate_issues)
        checks_executed["duplicates"] = True
        checked_fields.update(options_eff.duplicates_subset or ())

    # Se aplica la política de truncamiento antes de consolidar el reporte final.
    issues_effective, limits_block = apply_issue_truncation(
        issues,
        max_issues=options_eff.max_issues,
    )

    # Se construye el summary estable usando solo los issues ya truncados.
    summary = build_validation_summary(
        n_rows=len(df),
        issues=issues_effective,
        schema=schema,
        checks_executed=checks_executed,
        checked_fields=sorted(checked_fields),
        domains_block=domains_block,
        temporal_block=temporal_block,
        duplicates_block=duplicates_block,
        limits_block=limits_block,
    )

    # Se calcula ok como ausencia de issues de nivel error en el reporte efectivo.
    ok = not any(issue.level == "error" for issue in issues_effective)
    summary["ok"] = ok

    report = ValidationReport(
        ok=ok,
        issues=issues_effective,
        summary=summary,
    )

    # Se serializan los parámetros efectivos para dejarlos en el evento del dataset.
    parameters = _options_to_event_parameters(options_eff)
    issues_summary = _build_issues_summary(issues_effective)

    # Se registra el evento append-only con la misma forma ya usada por OP-01.
    trips.metadata.setdefault("events", []).append(
        {
            "op": "validate_trips",
            "ts_utc": _utc_now_iso(),
            "parameters": parameters,
            "summary": summary,
            "issues_summary": issues_summary,
        }
    )

    # Se alinea el flag persistible de validación antes de cualquier raise por strict.
    trips._set_validated_flag(ok)

    # Se aplica strict recién al final, después de dejar toda la evidencia observable.
    if options_eff.strict and not ok:
        error_issue = next((issue for issue in issues_effective if issue.level == "error"), None)
        raise ValidationError(
            "validate_trips detectó errores de datos y strict=True exige abortar.",
            code=error_issue.code if error_issue is not None else None,
            details=error_issue.details if error_issue is not None else None,
            issue=error_issue,
            issues=issues_effective,
        )

    return report


def check_required_columns(
    df: pd.DataFrame,
    *,
    schema: TripSchema,
) -> List[Issue]:
    """
    Verifica presencia de columnas requeridas del schema.

    Emite
    -----
    - VAL.CORE.REQUIRED_COLUMNS_MISSING
    """
    issues: List[Issue] = []
    missing_required = sorted([name for name in schema.required if name not in df.columns])
    if not missing_required:
        return issues

    # Se emite error agregado porque faltan columnas mínimas del contrato base.
    emit_issue(
        issues,
        VALIDATE_TRIPS_ISSUES,
        "VAL.CORE.REQUIRED_COLUMNS_MISSING",
        row_count=len(missing_required),
        missing_required=missing_required,
        required=list(schema.required),
        available_columns_sample=_sample_list(list(df.columns), 15),
        available_columns_total=len(df.columns),
    )
    return issues


def check_types_and_formats(
    df: pd.DataFrame,
    *,
    schema: TripSchema,
    sample_rows_per_issue: int,
) -> List[Issue]:
    """
    Verifica interpretabilidad básica por dtype sin mutar el dataframe.

    Emite
    -----
    - VAL.CORE.TYPE_OR_FORMAT_INVALID
    """
    issues: List[Issue] = []

    for field_name, field_spec in schema.fields.items():
        if field_name not in df.columns:
            continue

        series = df[field_name]
        non_null_mask = series.notna()
        if not bool(non_null_mask.any()):
            continue

        invalid_mask = _invalid_mask_for_dtype(series, field_spec.dtype)
        invalid_mask &= non_null_mask
        n_invalid = int(invalid_mask.sum())
        if n_invalid == 0:
            continue

        row_indices_sample, values_sample, raw_values_sample = _sample_series_violations(
            series,
            invalid_mask,
            sample_rows_per_issue,
        )

        # Se emite error porque existen valores no interpretables para el dtype declarado.
        emit_issue(
            issues,
            VALIDATE_TRIPS_ISSUES,
            "VAL.CORE.TYPE_OR_FORMAT_INVALID",
            field=field_name,
            row_count=n_invalid,
            n_rows_total=len(df),
            n_violations=n_invalid,
            row_indices_sample=row_indices_sample,
            values_sample=values_sample,
            raw_values_sample=raw_values_sample,
            dtype_expected=field_spec.dtype,
            parse_fail_count=n_invalid,
            total_count=int(non_null_mask.sum()),
            fail_rate=(n_invalid / int(non_null_mask.sum())) if int(non_null_mask.sum()) > 0 else None,
        )

    return issues


def check_constraints(
    df: pd.DataFrame,
    *,
    schema: TripSchema,
    effective_nullable_by_field: Mapping[str, bool],
    allow_partial_od_spatial: bool,
    skipped_constraints: Mapping[str, set[str]],
    sample_rows_per_issue: int,
) -> List[Issue]:
    """
    Verifica nullabilidad efectiva y constraints simples declarativas.

    Emite
    -----
    - VAL.CORE.NULLABILITY_VIOLATION
    - VAL.CORE.OD_SPATIAL_BOTH_MISSING
    - VAL.CORE.CONSTRAINT_VIOLATION
    """
    issues: List[Issue] = []

    # Se aplica la excepción de OD parcial antes del resto, porque modifica la semántica espacial mínima.
    if allow_partial_od_spatial and _all_columns_present(df, OD_COORDINATE_FIELDS):
        origin_complete = df["origin_latitude"].notna() & df["origin_longitude"].notna()
        destination_complete = df["destination_latitude"].notna() & df["destination_longitude"].notna()
        invalid_od_mask = ~(origin_complete | destination_complete)
        n_invalid_od = int(invalid_od_mask.sum())
        if n_invalid_od > 0:
            row_indices_sample, values_sample, raw_values_sample = _sample_od_partial_violations(
                df,
                invalid_od_mask,
                sample_rows_per_issue,
            )
            # Se emite error porque no hay ni origen completo ni destino completo en coordenadas.
            emit_issue(
                issues,
                VALIDATE_TRIPS_ISSUES,
                "VAL.CORE.OD_SPATIAL_BOTH_MISSING",
                row_count=n_invalid_od,
                n_rows_total=len(df),
                n_violations=n_invalid_od,
                row_indices_sample=row_indices_sample,
                values_sample=values_sample,
                raw_values_sample=raw_values_sample,
                fields_checked=list(OD_COORDINATE_FIELDS),
                allow_partial_od_spatial=allow_partial_od_spatial,
            )

    for field_name, field_spec in schema.fields.items():
        if field_name not in df.columns:
            continue

        series = df[field_name]
        nullable_effective = effective_nullable_by_field.get(field_name, True)
        if allow_partial_od_spatial and field_name in OD_COORDINATE_FIELDS:
            nullable_effective = True

        if not nullable_effective:
            null_mask = series.isna()
            n_null = int(null_mask.sum())
            if n_null > 0:
                row_indices_sample, values_sample, raw_values_sample = _sample_series_violations(
                    series,
                    null_mask,
                    sample_rows_per_issue,
                )
                # Se emite error porque el campo quedó no-nullable en su semántica efectiva.
                emit_issue(
                    issues,
                    VALIDATE_TRIPS_ISSUES,
                    "VAL.CORE.NULLABILITY_VIOLATION",
                    field=field_name,
                    row_count=n_null,
                    n_rows_total=len(df),
                    n_violations=n_null,
                    row_indices_sample=row_indices_sample,
                    values_sample=values_sample,
                    raw_values_sample=raw_values_sample,
                    nullable_effective=False,
                )

        constraints = field_spec.constraints or {}
        for constraint_name, constraint_value in constraints.items():
            if constraint_name == "nullable":
                continue
            if constraint_name in skipped_constraints.get(field_name, set()):
                continue

            invalid_mask, expected, observed_sample = _evaluate_constraint(series, field_spec.dtype, constraint_name, constraint_value)
            if invalid_mask is None:
                continue
            n_invalid = int(invalid_mask.sum())
            if n_invalid == 0:
                continue

            row_indices_sample, values_sample, raw_values_sample = _sample_series_violations(
                series,
                invalid_mask,
                sample_rows_per_issue,
            )

            # Se emite error porque el campo viola una constraint declarativa ya aceptada por el precheck.
            emit_issue(
                issues,
                VALIDATE_TRIPS_ISSUES,
                "VAL.CORE.CONSTRAINT_VIOLATION",
                field=field_name,
                row_count=n_invalid,
                n_rows_total=len(df),
                n_violations=n_invalid,
                row_indices_sample=row_indices_sample,
                values_sample=values_sample,
                raw_values_sample=raw_values_sample,
                constraint=constraint_name,
                expected=expected,
                observed_sample=observed_sample,
            )

    return issues


def check_domains(
    df: pd.DataFrame,
    *,
    schema: TripSchema,
    effective_domains_by_field: Mapping[str, Optional[set[str]]],
    mode: DomainValidationMode,
    sample_frac: float,
    min_in_domain_ratio: float,
    sample_rows_per_issue: int,
) -> Tuple[List[Issue], Optional[Dict[str, Any]]]:
    """
    Valida campos categóricos contra dominio efectivo/base y devuelve bloque de summary.

    Emite
    -----
    - VAL.DOMAIN.MISSING_DOMAIN_INFO
    - VAL.DOMAIN.RATIO_BELOW_MIN
    - VAL.DOMAIN.PARTIAL_COVERAGE
    """
    issues: List[Issue] = []
    if mode == "off":
        return issues, None

    fields_summary: Dict[str, Any] = {}

    for field_name, field_spec in schema.fields.items():
        if field_spec.dtype != "categorical" or field_name not in df.columns:
            continue

        domain_values = effective_domains_by_field.get(field_name)
        if not domain_values:
            # Se emite warning porque sin dominio no hay base formal para este check en ese campo.
            emit_issue(
                issues,
                VALIDATE_TRIPS_ISSUES,
                "VAL.DOMAIN.MISSING_DOMAIN_INFO",
                field=field_name,
                domain_source_attempted=["schema_effective", "metadata", "schema"],
                reason="missing_domain_values",
            )
            continue

        series = df[field_name]
        non_null_series = series[series.notna()]
        n_checked_non_null_total = int(non_null_series.shape[0])
        if n_checked_non_null_total == 0:
            fields_summary[field_name] = {
                "mode": mode,
                "ratio_in_domain": None,
                "min_required_ratio": min_in_domain_ratio,
                "n_checked_non_null": 0,
                "n_in_domain": 0,
            }
            continue

        series_checked = non_null_series
        if mode == "sample":
            sample_n = max(1, int(np.ceil(n_checked_non_null_total * sample_frac)))
            sample_n = min(sample_n, n_checked_non_null_total)
            series_checked = non_null_series.sample(n=sample_n, random_state=42)

        as_text = series_checked.astype(str)
        in_domain_mask = as_text.isin(domain_values)
        n_checked_non_null = int(series_checked.shape[0])
        n_in_domain = int(in_domain_mask.sum())
        ratio_in_domain = (n_in_domain / n_checked_non_null) if n_checked_non_null > 0 else None
        out_of_domain_mask = ~in_domain_mask

        fields_summary[field_name] = {
            "mode": mode,
            "ratio_in_domain": ratio_in_domain,
            "min_required_ratio": min_in_domain_ratio,
            "n_checked_non_null": n_checked_non_null,
            "n_in_domain": n_in_domain,
        }

        if ratio_in_domain is None or ratio_in_domain == 1.0:
            continue

        row_indices_sample, values_sample, raw_values_sample = _sample_series_violations(
            series_checked,
            out_of_domain_mask,
            sample_rows_per_issue,
        )

        common_ctx = dict(
            field=field_name,
            row_count=int(out_of_domain_mask.sum()),
            n_rows_total=len(df),
            n_violations=int(out_of_domain_mask.sum()),
            row_indices_sample=row_indices_sample,
            values_sample=values_sample,
            raw_values_sample=raw_values_sample,
            mode=mode,
            ratio_in_domain=ratio_in_domain,
            min_required_ratio=min_in_domain_ratio,
            n_checked_non_null=n_checked_non_null,
            n_in_domain=n_in_domain,
            domain_values_sample=_sample_list(sorted(domain_values), 10),
        )

        if ratio_in_domain < min_in_domain_ratio:
            # Se emite error porque la cobertura en dominio quedó bajo el umbral exigido.
            emit_issue(
                issues,
                VALIDATE_TRIPS_ISSUES,
                "VAL.DOMAIN.RATIO_BELOW_MIN",
                **common_ctx,
            )
        else:
            # Se emite warning porque el campo supera el mínimo, pero no logra cobertura completa.
            emit_issue(
                issues,
                VALIDATE_TRIPS_ISSUES,
                "VAL.DOMAIN.PARTIAL_COVERAGE",
                **common_ctx,
            )

    block = {
        "mode": mode,
        "min_required_ratio": min_in_domain_ratio,
        "fields": fields_summary,
    }
    return issues, block


def check_temporal_consistency(
    df: pd.DataFrame,
    *,
    temporal_context: Mapping[str, Any],
    sample_rows_per_issue: int,
) -> Tuple[List[Issue], Optional[Dict[str, Any]]]:
    """
    Verifica la regla temporal v1.1 sobre `origin_time_utc <= destination_time_utc`.

    Emite
    -----
    - VAL.TEMPORAL.ORIGIN_AFTER_DESTINATION
    """
    issues: List[Issue] = []
    tier = temporal_context.get("tier")
    if tier != "tier_1":
        return issues, {
            "evaluated": False,
            "reason": "temporal_tier_not_1",
            "tier": tier,
        }

    if not _all_columns_present(df, ("origin_time_utc", "destination_time_utc")):
        return issues, {
            "evaluated": False,
            "reason": "missing_temporal_columns",
            "tier": tier,
        }

    origin_dt = pd.to_datetime(df["origin_time_utc"], errors="coerce", utc=False)
    destination_dt = pd.to_datetime(df["destination_time_utc"], errors="coerce", utc=False)
    comparable_mask = origin_dt.notna() & destination_dt.notna()
    invalid_mask = comparable_mask & (origin_dt > destination_dt)
    n_invalid = int(invalid_mask.sum())

    block = {
        "evaluated": True,
        "tier": tier,
        "n_checked": int(comparable_mask.sum()),
        "n_violations": n_invalid,
        "origin_field": "origin_time_utc",
        "destination_field": "destination_time_utc",
    }

    if n_invalid == 0:
        return issues, block

    row_indices_sample = _sample_index_list(df.index[invalid_mask].tolist(), sample_rows_per_issue)
    values_sample = [
        {
            "origin_time_utc": _json_safe_scalar(df.at[idx, "origin_time_utc"]),
            "destination_time_utc": _json_safe_scalar(df.at[idx, "destination_time_utc"]),
        }
        for idx in row_indices_sample
    ]

    # Se emite error porque hay filas donde el origen ocurre después del destino.
    emit_issue(
        issues,
        VALIDATE_TRIPS_ISSUES,
        "VAL.TEMPORAL.ORIGIN_AFTER_DESTINATION",
        row_count=n_invalid,
        n_rows_total=len(df),
        n_violations=n_invalid,
        row_indices_sample=row_indices_sample,
        values_sample=values_sample,
        raw_values_sample=values_sample,
    )
    return issues, block


def check_duplicates(
    df: pd.DataFrame,
    *,
    duplicates_subset: Tuple[str, ...],
    sample_rows_per_issue: int,
) -> Tuple[List[Issue], Optional[Dict[str, Any]]]:
    """
    Detecta duplicados según un subset explícito ya validado.

    Emite
    -----
    - VAL.DUPLICATES.ROWS_FOUND
    """
    issues: List[Issue] = []
    duplicated_mask = df.duplicated(subset=list(duplicates_subset), keep=False)
    n_duplicates = int(duplicated_mask.sum())

    block = {
        "evaluated": True,
        "duplicates_subset": list(duplicates_subset),
        "n_duplicate_rows": n_duplicates,
    }

    if n_duplicates == 0:
        return issues, block

    duplicate_rows = df.loc[duplicated_mask, list(duplicates_subset)]
    key_strings = duplicate_rows.astype(object).where(pd.notna(duplicate_rows), None).to_dict(orient="records")
    row_indices_sample = _sample_index_list(df.index[duplicated_mask].tolist(), sample_rows_per_issue)
    values_sample = [
        {col: _json_safe_scalar(df.at[idx, col]) for col in duplicates_subset}
        for idx in row_indices_sample
    ]

    # Se emite error porque existen filas repetidas bajo el subset lógico de duplicados.
    emit_issue(
        issues,
        VALIDATE_TRIPS_ISSUES,
        "VAL.DUPLICATES.ROWS_FOUND",
        row_count=n_duplicates,
        n_rows_total=len(df),
        n_violations=n_duplicates,
        row_indices_sample=row_indices_sample,
        values_sample=values_sample,
        raw_values_sample=values_sample,
        duplicates_subset=list(duplicates_subset),
        duplicate_keys_sample=_sample_list(key_strings, 10),
    )
    return issues, block


def apply_issue_truncation(
    issues_detected: List[Issue],
    *,
    max_issues: int,
) -> Tuple[List[Issue], Optional[Dict[str, Any]]]:
    """
    Aplica la política de truncamiento del reporte y devuelve el bloque `limits`.

    Emite
    -----
    - VAL.CORE.ISSUES_TRUNCATED
    """
    n_detected_total = len(issues_detected)
    if n_detected_total <= max_issues:
        return issues_detected, {
            "max_issues": max_issues,
            "issues_truncated": False,
            "n_issues_emitted": n_detected_total,
            "n_issues_detected_total": n_detected_total,
        }

    if max_issues <= 1:
        kept = []
    else:
        kept = list(issues_detected[: max_issues - 1])

    truncation_issues = list(kept)
    # Se emite warning final para dejar evidencia explícita del truncamiento aplicado.
    emit_issue(
        truncation_issues,
        VALIDATE_TRIPS_ISSUES,
        "VAL.CORE.ISSUES_TRUNCATED",
        max_issues=max_issues,
        n_issues_emitted=min(max_issues, n_detected_total),
        n_issues_detected_total=n_detected_total,
    )

    limits_block = {
        "max_issues": max_issues,
        "issues_truncated": True,
        "n_issues_emitted": len(truncation_issues),
        "n_issues_detected_total": n_detected_total,
    }
    return truncation_issues, limits_block


def build_validation_summary(
    *,
    n_rows: int,
    issues: List[Issue],
    schema: TripSchema,
    checks_executed: Mapping[str, bool],
    checked_fields: Sequence[str],
    domains_block: Optional[Dict[str, Any]] = None,
    temporal_block: Optional[Dict[str, Any]] = None,
    duplicates_block: Optional[Dict[str, Any]] = None,
    limits_block: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Construye el summary mínimo y estable del ValidationReport.
    """
    counts_by_level = {
        "error": sum(1 for issue in issues if issue.level == "error"),
        "warning": sum(1 for issue in issues if issue.level == "warning"),
        "info": sum(1 for issue in issues if issue.level == "info"),
    }
    counts_by_code: Dict[str, int] = {}
    for issue in issues:
        counts_by_code[issue.code] = counts_by_code.get(issue.code, 0) + 1

    summary: Dict[str, Any] = {
        "ok": False,
        "n_rows": int(n_rows),
        "n_issues": len(issues),
        "n_errors": counts_by_level["error"],
        "n_warnings": counts_by_level["warning"],
        "n_info": counts_by_level["info"],
        "counts_by_level": counts_by_level,
        "counts_by_code": counts_by_code,
        "checked_fields": list(checked_fields),
        "checks_executed": dict(checks_executed),
        "schema_version": getattr(schema, "version", None),
    }

    if domains_block is not None:
        summary["domains"] = domains_block
    if duplicates_block is not None:
        summary["duplicates"] = duplicates_block
    if temporal_block is not None:
        summary["temporal"] = temporal_block
    if limits_block is not None:
        summary["limits"] = limits_block

    return summary




def _build_schema_for_required_check(
    *,
    schema: TripSchema,
    df_columns: Sequence[str],
    allow_partial_od_spatial: bool,
) -> TripSchema:
    """Construye una vista del required-check respetando la excepción de OD parcial."""
    if not allow_partial_od_spatial:
        return schema

    required_effective = [name for name in schema.required if name not in OD_COORDINATE_FIELDS]
    origin_pair_present = {"origin_latitude", "origin_longitude"}.issubset(set(df_columns))
    destination_pair_present = {"destination_latitude", "destination_longitude"}.issubset(set(df_columns))

    if origin_pair_present or destination_pair_present:
        return TripSchema(
            version=schema.version,
            fields=schema.fields,
            required=required_effective,
            semantic_rules=schema.semantic_rules,
        )

    return schema


# -----------------------------------------------------------------------------
# Helpers internos de uso general
# -----------------------------------------------------------------------------

def _validate_options_or_abort(issues: List[Issue], options_eff: ValidationOptions) -> None:
    """Valida las opciones globales antes de ejecutar el pipeline."""
    if not isinstance(options_eff.max_issues, int) or options_eff.max_issues <= 0:
        # Se emite error fatal porque el límite de issues debe ser interpretable desde el inicio.
        emit_and_maybe_raise(
            issues,
            VALIDATE_TRIPS_ISSUES,
            "VAL.CONFIG.INVALID_MAX_ISSUES",
            strict=False,
            exception_map=EXCEPTION_MAP_VALIDATE,
            default_exception=ValidationError,
            max_issues=options_eff.max_issues,
        )

    if not isinstance(options_eff.sample_rows_per_issue, int) or options_eff.sample_rows_per_issue <= 0:
        # Se emite error fatal porque el muestreo por issue debe ser un entero positivo.
        emit_and_maybe_raise(
            issues,
            VALIDATE_TRIPS_ISSUES,
            "VAL.CONFIG.INVALID_SAMPLE_ROWS_PER_ISSUE",
            strict=False,
            exception_map=EXCEPTION_MAP_VALIDATE,
            default_exception=ValidationError,
            sample_rows_per_issue=options_eff.sample_rows_per_issue,
        )

    if options_eff.validate_domains not in {"off", "full", "sample"}:
        # Se emite error fatal porque el modo de dominios no pertenece al contrato vigente.
        emit_and_maybe_raise(
            issues,
            VALIDATE_TRIPS_ISSUES,
            "VAL.CONFIG.INVALID_VALIDATE_DOMAINS_MODE",
            strict=False,
            exception_map=EXCEPTION_MAP_VALIDATE,
            default_exception=ValidationError,
            validate_domains=options_eff.validate_domains,
        )

    try:
        domains_sample_frac = float(options_eff.domains_sample_frac)
    except Exception:
        domains_sample_frac = None
    if domains_sample_frac is None or not (0 < domains_sample_frac <= 1):
        # Se emite error fatal porque el muestreo de dominios debe quedar en (0, 1].
        emit_and_maybe_raise(
            issues,
            VALIDATE_TRIPS_ISSUES,
            "VAL.CONFIG.INVALID_DOMAINS_SAMPLE_FRAC",
            strict=False,
            exception_map=EXCEPTION_MAP_VALIDATE,
            default_exception=ValidationError,
            domains_sample_frac=options_eff.domains_sample_frac,
        )

    try:
        domains_min_ratio = float(options_eff.domains_min_in_domain_ratio)
    except Exception:
        domains_min_ratio = None
    if domains_min_ratio is None or not (0 <= domains_min_ratio <= 1):
        # Se emite error fatal porque el ratio mínimo debe quedar en [0, 1].
        emit_and_maybe_raise(
            issues,
            VALIDATE_TRIPS_ISSUES,
            "VAL.CONFIG.INVALID_DOMAINS_MIN_RATIO",
            strict=False,
            exception_map=EXCEPTION_MAP_VALIDATE,
            default_exception=ValidationError,
            domains_min_in_domain_ratio=options_eff.domains_min_in_domain_ratio,
        )


def _validate_schema_and_collect_skips(
    issues: List[Issue],
    *,
    schema: TripSchema,
    strict: bool,
) -> Dict[str, set[str]]:
    """Valida el schema para validate_trips y marca constraints a omitir sin abortar."""
    skipped_constraints: Dict[str, set[str]] = {}

    for field_name, field_spec in schema.fields.items():
        if field_spec.dtype not in VALID_DTYPES:
            # Se emite error fatal porque el dtype no pertenece al catálogo soportado.
            emit_and_maybe_raise(
                issues,
                VALIDATE_TRIPS_ISSUES,
                "VAL.SCHEMA.UNKNOWN_DTYPE",
                strict=False,
                exception_map=EXCEPTION_MAP_VALIDATE,
                default_exception=SchemaError,
                field=field_name,
                dtype=field_spec.dtype,
                supported_dtypes=sorted(VALID_DTYPES),
            )

        constraints = field_spec.constraints or {}
        for constraint_name, constraint_value in constraints.items():
            if constraint_name not in VALID_CONSTRAINT_KEYS:
                # Se emite error fatal porque la constraint ni siquiera existe en el catálogo del módulo.
                emit_and_maybe_raise(
                    issues,
                    VALIDATE_TRIPS_ISSUES,
                    "VAL.SCHEMA.UNKNOWN_CONSTRAINT",
                    strict=False,
                    exception_map=EXCEPTION_MAP_VALIDATE,
                    default_exception=SchemaError,
                    field=field_name,
                    constraint=constraint_name,
                    supported_constraints=sorted(VALID_CONSTRAINT_KEYS),
                )

            if constraint_name not in CONSTRAINTS_BY_DTYPE.get(field_spec.dtype, set()):
                # Se emite error fatal porque la constraint declarada no aplica a ese dtype lógico.
                emit_and_maybe_raise(
                    issues,
                    VALIDATE_TRIPS_ISSUES,
                    "VAL.SCHEMA.CONSTRAINT_NOT_ALLOWED_FOR_DTYPE",
                    strict=False,
                    exception_map=EXCEPTION_MAP_VALIDATE,
                    default_exception=SchemaError,
                    field=field_name,
                    dtype=field_spec.dtype,
                    constraint=constraint_name,
                    allowed_constraints=sorted(CONSTRAINTS_BY_DTYPE.get(field_spec.dtype, set())),
                )

            if constraint_name == "nullable" and field_spec.required and constraint_value is True:
                # Se emite error fatal porque required=True con nullable=True quedó prohibido en v1.1.
                emit_and_maybe_raise(
                    issues,
                    VALIDATE_TRIPS_ISSUES,
                    "VAL.SCHEMA.REQUIRED_NULLABLE_CONFLICT",
                    strict=False,
                    exception_map=EXCEPTION_MAP_VALIDATE,
                    default_exception=SchemaError,
                    field=field_name,
                    required=True,
                    nullable=True,
                )

            if not _constraint_params_are_valid(constraint_name, constraint_value):
                skipped_constraints.setdefault(field_name, set()).add(constraint_name)
                # Se emite warning porque la constraint es conocida, pero sus params no se pueden interpretar.
                emit_issue(
                    issues,
                    VALIDATE_TRIPS_ISSUES,
                    "VAL.SCHEMA.CONSTRAINT_PARAMS_INVALID",
                    field=field_name,
                    constraint=constraint_name,
                    expected_params=CONSTRAINT_ALLOWED_PARAMS.get(constraint_name),
                    received_params=_describe_received_params(constraint_value),
                    reason="invalid_or_incomplete_params",
                )

    return skipped_constraints


def _build_effective_nullable_by_field(schema: TripSchema) -> Dict[str, bool]:
    """Calcula la nulabilidad efectiva por campo a partir de required + constraint nullable."""
    result: Dict[str, bool] = {}
    for field_name, field_spec in schema.fields.items():
        constraints = field_spec.constraints or {}
        if "nullable" in constraints:
            result[field_name] = bool(constraints["nullable"])
        else:
            result[field_name] = not bool(field_spec.required)
    return result


def _build_effective_domains_by_field(
    *,
    schema: TripSchema,
    schema_effective: Any,
    metadata: Mapping[str, Any],
) -> Dict[str, Optional[set[str]]]:
    """Resuelve la precedencia schema_effective -> metadata -> schema para dominios."""
    result: Dict[str, Optional[set[str]]] = {}
    metadata_domains = metadata.get("domains_effective") if isinstance(metadata, Mapping) else None
    se_domains = getattr(schema_effective, "domains_effective", None)

    for field_name, field_spec in schema.fields.items():
        if field_spec.dtype != "categorical":
            continue

        domain_values: Optional[set[str]] = None
        if isinstance(se_domains, Mapping) and field_name in se_domains:
            domain_values = _extract_domain_values(se_domains[field_name])
        elif isinstance(metadata_domains, Mapping) and field_name in metadata_domains:
            domain_values = _extract_domain_values(metadata_domains[field_name])
        elif isinstance(field_spec.domain, DomainSpec):
            domain_values = {str(v) for v in field_spec.domain.values}

        result[field_name] = domain_values

    return result


def _build_temporal_context(
    *,
    df: pd.DataFrame,
    metadata: Mapping[str, Any],
    schema_effective: Any,
) -> Dict[str, Any]:
    """Construye el contexto temporal mínimo para el check temporal v1.1."""
    tier = None
    temporal_metadata = metadata.get("temporal") if isinstance(metadata, Mapping) else None
    temporal_effective = getattr(schema_effective, "temporal", None)

    if isinstance(temporal_effective, Mapping):
        tier = temporal_effective.get("tier")
    if tier is None and isinstance(temporal_metadata, Mapping):
        tier = temporal_metadata.get("tier")
    if tier is None:
        if _all_columns_present(df, ("origin_time_utc", "destination_time_utc")):
            tier = "tier_1"
        elif _all_columns_present(df, ("origin_time_local_hhmm", "destination_time_local_hhmm")):
            tier = "tier_2"
        else:
            tier = "tier_3"

    return {
        "tier": tier,
        "fields_present": [col for col in ("origin_time_utc", "destination_time_utc") if col in df.columns],
    }


def _invalid_mask_for_dtype(series: pd.Series, dtype: str) -> pd.Series:
    """Retorna una máscara de valores no interpretables para el dtype lógico."""
    non_null = series.notna()
    if dtype == "string":
        return pd.Series(False, index=series.index)
    if dtype == "categorical":
        return pd.Series(False, index=series.index)
    if dtype == "int":
        coerced = pd.to_numeric(series, errors="coerce")
        invalid = non_null & coerced.isna()
        valid_numeric = coerced[~invalid & non_null]
        if not valid_numeric.empty:
            invalid.loc[valid_numeric.index] = invalid.loc[valid_numeric.index] | (~np.isclose(valid_numeric % 1, 0))
        return invalid
    if dtype == "float":
        coerced = pd.to_numeric(series, errors="coerce")
        return non_null & coerced.isna()
    if dtype == "datetime":
        coerced = pd.to_datetime(series, errors="coerce", utc=False)
        return non_null & coerced.isna()
    if dtype == "bool":
        normalized = series.astype(str).str.strip().str.lower()
        valid = normalized.isin({"true", "false", "1", "0", "yes", "no", "y", "n", "t", "f"})
        valid = valid | series.map(lambda x: isinstance(x, (bool, np.bool_)))
        return non_null & ~valid
    return pd.Series(False, index=series.index)


def _evaluate_constraint(
    series: pd.Series,
    dtype: str,
    constraint_name: str,
    constraint_value: Any,
) -> Tuple[Optional[pd.Series], Optional[Any], Optional[list[Any]]]:
    """Evalúa una constraint declarativa y retorna máscara inválida + expected + muestra observada."""
    non_null = series.notna()

    if constraint_name == "range":
        numeric = pd.to_numeric(series, errors="coerce")
        invalid = pd.Series(False, index=series.index)
        if "min" in constraint_value:
            invalid = invalid | (non_null & numeric.notna() & (numeric < constraint_value["min"]))
        if "max" in constraint_value:
            invalid = invalid | (non_null & numeric.notna() & (numeric > constraint_value["max"]))
        return invalid, dict(constraint_value), _sample_list(series.loc[invalid].tolist(), 10)

    if constraint_name == "pattern":
        pattern = constraint_value
        as_text = series.astype(str)
        invalid = non_null & ~as_text.str.match(pattern, na=False)
        return invalid, pattern, _sample_list(series.loc[invalid].tolist(), 10)

    if constraint_name == "length":
        as_text = series.astype(str)
        lengths = as_text.str.len()
        invalid = pd.Series(False, index=series.index)
        if "min" in constraint_value:
            invalid = invalid | (non_null & (lengths < constraint_value["min"]))
        if "max" in constraint_value:
            invalid = invalid | (non_null & (lengths > constraint_value["max"]))
        return invalid, dict(constraint_value), _sample_list(series.loc[invalid].tolist(), 10)

    if constraint_name == "unique" and bool(constraint_value):
        invalid = series.duplicated(keep=False) & non_null
        return invalid, True, _sample_list(series.loc[invalid].tolist(), 10)

    if constraint_name == "datetime":
        parsed = pd.to_datetime(series, errors="coerce", utc=False)
        invalid = pd.Series(False, index=series.index)
        if "min" in constraint_value and constraint_value["min"] is not None:
            min_dt = pd.to_datetime(constraint_value["min"], errors="coerce", utc=False)
            if pd.notna(min_dt):
                invalid = invalid | (non_null & parsed.notna() & (parsed < min_dt))
        if "max" in constraint_value and constraint_value["max"] is not None:
            max_dt = pd.to_datetime(constraint_value["max"], errors="coerce", utc=False)
            if pd.notna(max_dt):
                invalid = invalid | (non_null & parsed.notna() & (parsed > max_dt))
        if bool(constraint_value.get("allow_naive") is False):
            tz_mask = parsed.map(lambda x: getattr(x, "tzinfo", None) is None if pd.notna(x) else False)
            invalid = invalid | (non_null & parsed.notna() & tz_mask)
        return invalid, dict(constraint_value), _sample_list(series.loc[invalid].tolist(), 10)

    if constraint_name == "h3":
        as_text = series.astype(str)
        invalid = pd.Series(False, index=series.index)
        if constraint_value.get("require_valid", False):
            invalid = invalid | (non_null & ~as_text.map(_is_valid_h3_value))
        resolution = constraint_value.get("resolution")
        if resolution is not None:
            resolution_mask = as_text.map(lambda x: _is_h3_resolution_mismatch(x, resolution))
            invalid = invalid | (non_null & resolution_mask)
        return invalid, dict(constraint_value), _sample_list(series.loc[invalid].tolist(), 10)

    return None, None, None


def _constraint_params_are_valid(constraint_name: str, constraint_value: Any) -> bool:
    """Verifica si la forma del payload de una constraint conocida es aceptable."""
    if constraint_name == "nullable":
        return isinstance(constraint_value, bool)
    if constraint_name == "pattern":
        return isinstance(constraint_value, str)
    if constraint_name == "unique":
        return isinstance(constraint_value, bool)
    if constraint_name == "range":
        if not isinstance(constraint_value, Mapping):
            return False
        keys = set(constraint_value.keys())
        if not keys or not keys.issubset({"min", "max"}):
            return False
        for value in constraint_value.values():
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                return False
        return True
    if constraint_name == "length":
        if not isinstance(constraint_value, Mapping):
            return False
        keys = set(constraint_value.keys())
        if not keys or not keys.issubset({"min", "max"}):
            return False
        return all(isinstance(v, int) and not isinstance(v, bool) for v in constraint_value.values())
    if constraint_name == "datetime":
        if not isinstance(constraint_value, Mapping):
            return False
        keys = set(constraint_value.keys())
        if not keys.issubset({"timezone", "allow_naive", "min", "max"}):
            return False
        if "allow_naive" in constraint_value and not isinstance(constraint_value["allow_naive"], bool):
            return False
        return True
    if constraint_name == "h3":
        if not isinstance(constraint_value, Mapping):
            return False
        keys = set(constraint_value.keys())
        if not keys.issubset({"require_valid", "resolution", "allow_mixed_resolution"}):
            return False
        if "require_valid" in constraint_value and not isinstance(constraint_value["require_valid"], bool):
            return False
        if "allow_mixed_resolution" in constraint_value and not isinstance(constraint_value["allow_mixed_resolution"], bool):
            return False
        if "resolution" in constraint_value and constraint_value["resolution"] is not None and not isinstance(constraint_value["resolution"], int):
            return False
        return True
    return False


def _describe_received_params(constraint_value: Any) -> Any:
    """Describe los params efectivamente recibidos para un warning de constraint."""
    if isinstance(constraint_value, Mapping):
        return list(constraint_value.keys())
    return type(constraint_value).__name__


def _extract_domain_values(raw: Any) -> Optional[set[str]]:
    """Extrae el conjunto de valores desde una estructura de dominio simple o rica."""
    if raw is None:
        return None
    if isinstance(raw, Mapping):
        values = raw.get("values")
        if values is None:
            return None
        return {str(v) for v in values}
    if isinstance(raw, (list, tuple, set)):
        return {str(v) for v in raw}
    return None


def _sample_series_violations(
    series: pd.Series,
    invalid_mask: pd.Series,
    sample_rows_per_issue: int,
) -> Tuple[List[Any], List[Any], List[Any]]:
    """Muestrea índices y valores para poblar Issue.details sin inflar el reporte."""
    idx = series.index[invalid_mask].tolist()
    sampled_idx = _sample_index_list(idx, sample_rows_per_issue)
    values = [_json_safe_scalar(series.loc[i]) for i in sampled_idx]
    return sampled_idx, values, values


def _sample_od_partial_violations(
    df: pd.DataFrame,
    invalid_mask: pd.Series,
    sample_rows_per_issue: int,
) -> Tuple[List[Any], List[Any], List[Any]]:
    """Muestrea filas de la regla OD parcial en forma compacta y serializable."""
    idx = df.index[invalid_mask].tolist()
    sampled_idx = _sample_index_list(idx, sample_rows_per_issue)
    values = [
        {
            "origin_latitude": _json_safe_scalar(df.at[i, "origin_latitude"]),
            "origin_longitude": _json_safe_scalar(df.at[i, "origin_longitude"]),
            "destination_latitude": _json_safe_scalar(df.at[i, "destination_latitude"]),
            "destination_longitude": _json_safe_scalar(df.at[i, "destination_longitude"]),
        }
        for i in sampled_idx
    ]
    return sampled_idx, values, values


def _build_issues_summary(issues: Sequence[Issue]) -> Dict[str, Any]:
    """Construye un resumen compacto de issues para dejarlo en el evento."""
    counts = {"info": 0, "warning": 0, "error": 0}
    counts_by_code: Dict[str, int] = {}
    for issue in issues:
        counts[issue.level] = counts.get(issue.level, 0) + 1
        counts_by_code[issue.code] = counts_by_code.get(issue.code, 0) + 1
    top_codes = sorted(counts_by_code.items(), key=lambda x: (-x[1], x[0]))[:10]
    return {
        "counts": counts,
        "top_codes": [{"code": code, "count": count} for code, count in top_codes],
    }


def _options_to_event_parameters(options_eff: ValidationOptions) -> Dict[str, Any]:
    """Serializa ValidationOptions a una forma estable y JSON-safe para el evento."""
    parameters = asdict(options_eff)
    if parameters.get("duplicates_subset") is not None:
        parameters["duplicates_subset"] = list(parameters["duplicates_subset"])
    return parameters


def _utc_now_iso() -> str:
    """Retorna timestamp UTC ISO-8601 compacto con sufijo Z."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sample_list(values: Iterable[Any], limit: int) -> List[Any]:
    """Devuelve una muestra simple y serializable de una secuencia cualquiera."""
    out: List[Any] = []
    for value in values:
        out.append(_json_safe_scalar(value))
        if len(out) >= limit:
            break
    return out


def _sample_index_list(index_values: Sequence[Any], limit: int) -> List[Any]:
    """Devuelve una muestra estable de índices del DataFrame."""
    return [_json_safe_scalar(v) for v in list(index_values)[:limit]]


def _json_safe_scalar(value: Any) -> Any:
    """Convierte escalares frecuentes de pandas/numpy a una forma JSON-safe."""
    if pd.isna(value):
        return None
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    return value


def _all_columns_present(df: pd.DataFrame, columns: Sequence[str]) -> bool:
    """Indica si todas las columnas solicitadas existen en el dataframe."""
    return all(name in df.columns for name in columns)


def _is_valid_h3_value(value: str) -> bool:
    """Valida un índice H3 textual de forma tolerante a excepciones."""
    try:
        return bool(h3.is_valid_cell(value))
    except Exception:
        return False


def _is_h3_resolution_mismatch(value: str, expected_resolution: int) -> bool:
    """Indica si un índice H3 válido no coincide con la resolución esperada."""
    try:
        if not h3.is_valid_cell(value):
            return True
        return h3.get_resolution(value) != expected_resolution
    except Exception:
        return True
