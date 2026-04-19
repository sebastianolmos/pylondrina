# -------------------------
# file: pylondrina/importing_traces.py
# -------------------------
from __future__ import annotations

import json
import re
import uuid
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from pandas.api import types as ptypes

from pylondrina.datasets import TraceDataset
from pylondrina.errors import ImportError as PylondrinaImportError, SchemaError
from pylondrina.issues.catalog_import_traces import IMPORT_TRACES_ISSUES
from pylondrina.issues.core import emit_and_maybe_raise, emit_issue
from pylondrina.reports import ImportReport, Issue
from pylondrina.schema import FieldSpec, TraceSchema
from pylondrina.types import FieldCorrespondence

EXCEPTION_MAP_IMPORT_TRACE = {
    "schema": SchemaError,
    "import": PylondrinaImportError,
}

TRACE_CORE_FIELDS = ("point_id", "user_id", "time_utc", "latitude", "longitude")
TRACE_CORE_REQUIRED_INPUT = ("user_id", "time_utc", "latitude", "longitude")
TRACE_ALLOWED_DTYPES = {"string", "int", "float", "datetime", "bool"}
TRACE_ALLOWED_CONSTRAINTS = {"nullable", "range", "datetime", "pattern", "length", "unique"}
TRACE_CONSTRAINTS_BY_DTYPE = {
    "string": {"nullable", "pattern", "length", "unique"},
    "int": {"nullable", "range", "unique"},
    "float": {"nullable", "range", "unique"},
    "datetime": {"nullable", "datetime", "unique"},
    "bool": {"nullable", "unique"},
}
OFFSET_RE = re.compile(r"^[+-](?:0\d|1\d|2[0-3]):[0-5]\d$")


@dataclass(frozen=True)
class ImportTraceOptions:
    """
    Opciones de importación/estandarización para construir un TraceDataset.

    Attributes
    ----------
    keep_extra_fields : bool, default=True
        Si True, conserva columnas no estándar como campos extendidos del dataset.
    selected_fields : sequence of str, optional
        Lista de campos a conservar. Si es None, se preservan todos los campos alcanzables
        sujetos a `keep_extra_fields`. Si es [], se conserva solo el núcleo canónico.
    strict : bool, default=False
        Si True, problemas recuperables de nivel error se escalan después de construir evidencia.
    source_timezone : str, optional
        Zona horaria de origen para interpretar timestamps naive.
    """

    keep_extra_fields: bool = True
    selected_fields: Optional[Sequence[str]] = None
    strict: bool = False
    source_timezone: Optional[str] = None

def import_traces_from_dataframe(
    df: pd.DataFrame,
    schema: TraceSchema,
    *,
    source_name: Optional[str] = None,
    options: Optional[ImportTraceOptions] = None,
    field_correspondence: Optional[FieldCorrespondence] = None,
    provenance: Optional[Dict[str, Any]] = None,
) -> Tuple[TraceDataset, ImportReport]:
    """
    Importa un DataFrame de puntos hacia un TraceDataset canónico y trazable.

    La operación asume que el input ya está "pointificado" y solo resuelve:
    alineación de columnas, núcleo mínimo de traces, temporalidad básica,
    metadata mínima, reporte y evento `import_traces`.
    """
    issues: List[Issue] = []
    rows_in = len(df) if isinstance(df, pd.DataFrame) else None

    # Se normalizan temprano las options para fijar la política efectiva del import.
    options_eff, parameters_effective = _normalize_import_trace_options(options)

    # Se ejecuta el preflight fatal antes de tocar el dataframe de trabajo.
    _preflight_import_traces_request(
        issues,
        df=df,
        schema=schema,
        field_correspondence=field_correspondence,
        options_eff=options_eff,
    )

    # Se trabaja siempre sobre una copia para no mutar el input tabular del usuario.
    work = df.copy(deep=True)

    # Se resuelve el mapeo efectivo de campos y la política de conservación de columnas.
    work, field_map_applied, n_fields_mapped = _resolve_trace_import_columns(
        issues,
        work,
        schema=schema,
        field_correspondence=field_correspondence,
        options_eff=options_eff,
    )

    # Se garantiza el núcleo canónico mínimo y se genera point_id cuando haga falta.
    work, point_id_generated = _materialize_trace_core(
        issues,
        work,
        strict=options_eff.strict,
    )

    # Se interpreta la temporalidad y se consolida time_utc con la mejor precedencia disponible.
    work, temporal_descriptor = _normalize_trace_time_utc(
        issues,
        work,
        schema=schema,
        options_eff=options_eff,
    )

    # Se cierra la salida pública con metadata mínima, evento y reporte del import.
    dataset, report = _finalize_import_traces_result(
        issues,
        work,
        schema=schema,
        source_name=source_name,
        options_eff=options_eff,
        parameters_effective=parameters_effective,
        field_map_applied=field_map_applied,
        n_fields_mapped=n_fields_mapped,
        point_id_generated=point_id_generated,
        temporal_descriptor=temporal_descriptor,
        provenance=provenance,
        rows_in=int(rows_in),
    )

    # Si strict=True y quedaron errores recuperables, se escala después de construir evidencia.
    if options_eff.strict:
        first_error = next((issue for issue in issues if issue.level == "error"), None)
        if first_error is not None:
            raise PylondrinaImportError(
                first_error.message,
                code=first_error.code,
                details={
                    "summary": report.summary,
                    "parameters": report.parameters,
                    "metadata": dataset.metadata,
                },
                issue=first_error,
                issues=issues,
            )

    return dataset, report


# -----------------------------------------------------------------------------
# Helpers internos principales del pipeline de importación
# -----------------------------------------------------------------------------

def _normalize_import_trace_options(
    options: Optional[ImportTraceOptions],
) -> tuple[ImportTraceOptions, Dict[str, Any]]:
    """
    Normaliza ImportTraceOptions a una forma efectiva y serializable.

    Emite: ninguno.
    """
    options_eff = options or ImportTraceOptions()

    selected_fields_raw = options_eff.selected_fields
    if selected_fields_raw is None:
        selected_fields = None
    elif isinstance(selected_fields_raw, (str, bytes)) or not isinstance(selected_fields_raw, Sequence):
        selected_fields = selected_fields_raw
    else:
        selected_fields = list(selected_fields_raw)

    options_eff = ImportTraceOptions(
        keep_extra_fields=bool(options_eff.keep_extra_fields),
        selected_fields=selected_fields,
        strict=bool(options_eff.strict),
        source_timezone=options_eff.source_timezone,
    )

    parameters_effective = {
        "source_name": None,
        "strict": options_eff.strict,
        "keep_extra_fields": options_eff.keep_extra_fields,
        "selected_fields": list(options_eff.selected_fields) if options_eff.selected_fields is not None else None,
        "source_timezone": options_eff.source_timezone,
    }
    return options_eff, parameters_effective


def _preflight_import_traces_request(
    issues: List[Issue],
    *,
    df: pd.DataFrame,
    schema: TraceSchema,
    field_correspondence: Optional[FieldCorrespondence],
    options_eff: ImportTraceOptions,
) -> None:
    """
    Ejecuta el preflight fatal del request antes del pipeline normal.

    Emite: IMP.INPUT.INVALID_DATAFRAME, IMP.INPUT.DUPLICATE_COLUMNS,
            IMP.INPUT.EMPTY_DATAFRAME, IMP.OPTIONS.INVALID_SELECTED_FIELDS_SPEC,
            IMP.OPTIONS.EMPTY_SELECTED_FIELDS, IMP.OPTIONS.INVALID_SOURCE_TIMEZONE,
            SCH.TRACE_SCHEMA.MISSING_SCHEMA, SCH.TRACE_SCHEMA.INVALID_VERSION,
            SCH.TRACE_SCHEMA.CATEGORICAL_NOT_ALLOWED, SCH.FIELD_SPEC.UNKNOWN_DTYPE,
            SCH.FIELD_SPEC.UNKNOWN_REQUIRED_FIELD, SCH.CONSTRAINTS.UNKNOWN_RULE,
            SCH.CONSTRAINTS.NOT_ALLOWED_FOR_DTYPE, SCH.TRACE_SCHEMA.INVALID_CRS,
            SCH.TRACE_SCHEMA.INVALID_TIMEZONE, MAP.FIELDS.INVALID_SPEC,
            MAP.FIELDS.UNKNOWN_CANONICAL_FIELD, MAP.FIELDS.COLLISION_DUPLICATE_TARGET,
            MAP.FIELDS.SOURCE_COLUMN_NOT_FOUND.
    """
    if not isinstance(df, pd.DataFrame):
        # Se emite error fatal porque sin DataFrame no existe superficie interpretable de import.
        emit_and_maybe_raise(
            issues,
            IMPORT_TRACES_ISSUES,
            "IMP.INPUT.INVALID_DATAFRAME",
            strict=False,
            exception_map=EXCEPTION_MAP_IMPORT_TRACE,
            default_exception=PylondrinaImportError,
            received_type=type(df).__name__,
        )

    duplicate_columns = _duplicated_columns(df.columns)
    if duplicate_columns:
        # Se emite error fatal porque columnas duplicadas vuelven ambiguo cualquier mapeo posterior.
        emit_and_maybe_raise(
            issues,
            IMPORT_TRACES_ISSUES,
            "IMP.INPUT.DUPLICATE_COLUMNS",
            strict=False,
            exception_map=EXCEPTION_MAP_IMPORT_TRACE,
            default_exception=PylondrinaImportError,
            duplicate_columns=duplicate_columns,
            columns_in=list(df.columns),
        )

    if len(df) == 0:
        # Se emite warning porque el import puede continuar, pero el resultado quedará vacío.
        emit_issue(
            issues,
            IMPORT_TRACES_ISSUES,
            "IMP.INPUT.EMPTY_DATAFRAME",
            rows_in=0,
            columns_in=list(df.columns),
        )

    if options_eff.selected_fields is not None and (
        isinstance(options_eff.selected_fields, (str, bytes)) or not isinstance(options_eff.selected_fields, list)
    ):
        # Se emite error fatal porque selected_fields debe quedar normalizado como secuencia serializable.
        emit_and_maybe_raise(
            issues,
            IMPORT_TRACES_ISSUES,
            "IMP.OPTIONS.INVALID_SELECTED_FIELDS_SPEC",
            strict=False,
            exception_map=EXCEPTION_MAP_IMPORT_TRACE,
            default_exception=PylondrinaImportError,
            received_type=type(options_eff.selected_fields).__name__,
            selected_fields=options_eff.selected_fields,
        )

    if options_eff.selected_fields == []:
        # Se emite info para dejar trazado que [] significa conservar solo el núcleo canónico.
        emit_issue(
            issues,
            IMPORT_TRACES_ISSUES,
            "IMP.OPTIONS.EMPTY_SELECTED_FIELDS",
            selected_fields=[],
            effective_selected_fields=list(TRACE_CORE_FIELDS),
        )

    _, tz_kind = _normalize_timezone_spec(options_eff.source_timezone)
    if options_eff.source_timezone is not None and tz_kind == "invalid":
        # Se emite error fatal porque la zona horaria explícita debe ser interpretable desde el inicio.
        emit_and_maybe_raise(
            issues,
            IMPORT_TRACES_ISSUES,
            "IMP.OPTIONS.INVALID_SOURCE_TIMEZONE",
            strict=False,
            exception_map=EXCEPTION_MAP_IMPORT_TRACE,
            default_exception=PylondrinaImportError,
            source_timezone=options_eff.source_timezone,
        )

    if not isinstance(schema, TraceSchema):
        # Se emite error fatal porque la operación necesita un TraceSchema interpretable.
        emit_and_maybe_raise(
            issues,
            IMPORT_TRACES_ISSUES,
            "SCH.TRACE_SCHEMA.MISSING_SCHEMA",
            strict=False,
            exception_map=EXCEPTION_MAP_IMPORT_TRACE,
            default_exception=SchemaError,
            schema_present=schema is not None,
        )

    if not isinstance(schema.version, str) or not schema.version.strip():
        # Se emite error fatal porque la versión del schema debe ser trazable y no vacía.
        emit_and_maybe_raise(
            issues,
            IMPORT_TRACES_ISSUES,
            "SCH.TRACE_SCHEMA.INVALID_VERSION",
            strict=False,
            exception_map=EXCEPTION_MAP_IMPORT_TRACE,
            default_exception=SchemaError,
            schema_version=getattr(schema, "version", None),
        )

    if schema.crs and not _is_probably_crs(schema.crs):
        # Se emite warning porque el CRS solo se conserva como metadata declarativa en traces.
        emit_issue(
            issues,
            IMPORT_TRACES_ISSUES,
            "SCH.TRACE_SCHEMA.INVALID_CRS",
            crs=schema.crs,
        )

    if schema.timezone:
        _, schema_tz_kind = _normalize_timezone_spec(schema.timezone)
        if schema_tz_kind == "invalid":
            # Se emite warning porque la timezone declarada puede omitirse y continuar con otras fuentes temporales.
            emit_issue(
                issues,
                IMPORT_TRACES_ISSUES,
                "SCH.TRACE_SCHEMA.INVALID_TIMEZONE",
                timezone=schema.timezone,
            )

    schema_fields = schema.fields if isinstance(schema.fields, dict) else {}
    for field_name, field_spec in schema_fields.items():
        dtype = getattr(field_spec, "dtype", None)
        if dtype == "categorical":
            # Se emite error fatal porque categorical quedó fuera del contrato de traces v1.1.
            emit_and_maybe_raise(
                issues,
                IMPORT_TRACES_ISSUES,
                "SCH.TRACE_SCHEMA.CATEGORICAL_NOT_ALLOWED",
                strict=False,
                exception_map=EXCEPTION_MAP_IMPORT_TRACE,
                default_exception=SchemaError,
                field=field_name,
                dtype=dtype,
            )
        if dtype not in TRACE_ALLOWED_DTYPES:
            # Se emite error fatal porque el dtype no pertenece al subconjunto soportado por traces.
            emit_and_maybe_raise(
                issues,
                IMPORT_TRACES_ISSUES,
                "SCH.FIELD_SPEC.UNKNOWN_DTYPE",
                strict=False,
                exception_map=EXCEPTION_MAP_IMPORT_TRACE,
                default_exception=SchemaError,
                field=field_name,
                dtype=dtype,
            )

        constraints = getattr(field_spec, "constraints", None) or {}
        for constraint_name in constraints.keys():
            if constraint_name not in TRACE_ALLOWED_CONSTRAINTS:
                # Se emite error fatal porque la constraint no pertenece al contrato vigente de traces.
                emit_and_maybe_raise(
                    issues,
                    IMPORT_TRACES_ISSUES,
                    "SCH.CONSTRAINTS.UNKNOWN_RULE",
                    strict=False,
                    exception_map=EXCEPTION_MAP_IMPORT_TRACE,
                    default_exception=SchemaError,
                    field=field_name,
                    constraint=constraint_name,
                )
            if constraint_name not in TRACE_CONSTRAINTS_BY_DTYPE.get(dtype, set()):
                # Se emite error fatal porque la constraint no aplica al dtype declarado.
                emit_and_maybe_raise(
                    issues,
                    IMPORT_TRACES_ISSUES,
                    "SCH.CONSTRAINTS.NOT_ALLOWED_FOR_DTYPE",
                    strict=False,
                    exception_map=EXCEPTION_MAP_IMPORT_TRACE,
                    default_exception=SchemaError,
                    field=field_name,
                    dtype=dtype,
                    constraint=constraint_name,
                )

    if any(field_name not in schema_fields for field_name in schema.required):
        unknown_required = [field_name for field_name in schema.required if field_name not in schema_fields]
        # Se emite error fatal porque schema.required no puede apuntar a campos ausentes del schema base.
        emit_and_maybe_raise(
            issues,
            IMPORT_TRACES_ISSUES,
            "SCH.FIELD_SPEC.UNKNOWN_REQUIRED_FIELD",
            strict=False,
            exception_map=EXCEPTION_MAP_IMPORT_TRACE,
            default_exception=SchemaError,
            unknown_required=unknown_required,
            required=list(schema.required),
            schema_fields_sample=_sample_list(schema_fields.keys(), 20),
            schema_fields_total=len(schema_fields),
        )

    if field_correspondence is not None and not isinstance(field_correspondence, Mapping):
        # Se emite error fatal porque field_correspondence debe ser un mapping interpretable.
        emit_and_maybe_raise(
            issues,
            IMPORT_TRACES_ISSUES,
            "MAP.FIELDS.INVALID_SPEC",
            strict=False,
            exception_map=EXCEPTION_MAP_IMPORT_TRACE,
            default_exception=PylondrinaImportError,
            received_type=type(field_correspondence).__name__,
            reason="not_mapping",
        )

    allowed_canonical_fields = set(TRACE_CORE_FIELDS) | set(schema_fields.keys())
    if field_correspondence is not None:
        seen_targets: Dict[str, List[str]] = {}
        for canonical_field, source_field in dict(field_correspondence).items():
            if canonical_field not in allowed_canonical_fields:
                # Se emite error fatal porque el mapping apunta a un campo canónico fuera del contrato vigente.
                emit_and_maybe_raise(
                    issues,
                    IMPORT_TRACES_ISSUES,
                    "MAP.FIELDS.UNKNOWN_CANONICAL_FIELD",
                    strict=False,
                    exception_map=EXCEPTION_MAP_IMPORT_TRACE,
                    default_exception=PylondrinaImportError,
                    field=canonical_field,
                    source_field=source_field,
                    allowed_fields_sample=_sample_list(sorted(allowed_canonical_fields), 20),
                    allowed_fields_total=len(allowed_canonical_fields),
                )
            if not isinstance(source_field, str) or not source_field:
                # Se emite error fatal porque cada entrada del mapping debe apuntar a un nombre de columna fuente usable.
                emit_and_maybe_raise(
                    issues,
                    IMPORT_TRACES_ISSUES,
                    "MAP.FIELDS.INVALID_SPEC",
                    strict=False,
                    exception_map=EXCEPTION_MAP_IMPORT_TRACE,
                    default_exception=PylondrinaImportError,
                    received_type=type(source_field).__name__,
                    reason="source_field_not_non_empty_string",
                )
            seen_targets.setdefault(canonical_field, []).append(source_field)
            if source_field not in df.columns:
                # Se emite warning porque esa entrada del mapping no podrá aplicarse y se omitirá.
                emit_issue(
                    issues,
                    IMPORT_TRACES_ISSUES,
                    "MAP.FIELDS.SOURCE_COLUMN_NOT_FOUND",
                    field=canonical_field,
                    source_field=source_field,
                    available_columns_sample=_sample_list(df.columns, 20),
                    available_columns_total=len(df.columns),
                )

            if canonical_field in df.columns and source_field != canonical_field:
                seen_targets.setdefault(canonical_field, []).append(canonical_field)

        for canonical_field, source_fields in seen_targets.items():
            unique_sources = sorted(set(source_fields))
            if len(unique_sources) > 1:
                # Se emite error fatal porque dos columnas competirían por el mismo destino canónico.
                emit_and_maybe_raise(
                    issues,
                    IMPORT_TRACES_ISSUES,
                    "MAP.FIELDS.COLLISION_DUPLICATE_TARGET",
                    strict=False,
                    exception_map=EXCEPTION_MAP_IMPORT_TRACE,
                    default_exception=PylondrinaImportError,
                    field=canonical_field,
                    source_fields=unique_sources,
                )


def _resolve_trace_import_columns(
    issues: List[Issue],
    df: pd.DataFrame,
    *,
    schema: TraceSchema,
    field_correspondence: Optional[FieldCorrespondence],
    options_eff: ImportTraceOptions,
) -> tuple[pd.DataFrame, Dict[str, str], int]:
    """
    Resuelve el mapeo efectivo y la política de conservación de columnas del import.

    Emite: IMP.OPTIONS.SELECTED_FIELDS_UNKNOWN, IMP.OPTIONS.EXTRA_FIELDS_DROPPED,
            MAP.FIELDS.SOURCE_COLUMN_NOT_FOUND.
    """
    work = df.copy(deep=True)
    applied: Dict[str, str] = {}

    # Primero se renombran solo las columnas del mapping que realmente existen en la fuente.
    rename_map: Dict[str, str] = {}
    if field_correspondence is not None:
        for canonical_field, source_field in dict(field_correspondence).items():
            if source_field in work.columns and canonical_field != source_field:
                rename_map[source_field] = canonical_field
                applied[canonical_field] = source_field
            elif source_field in work.columns and canonical_field == source_field:
                applied[canonical_field] = source_field
        if rename_map:
            work = work.rename(columns=rename_map)

    reachable_columns = list(work.columns)
    core_fields = list(TRACE_CORE_FIELDS)
    schema_fields = list(schema.fields.keys())

    if options_eff.selected_fields is None:
        if options_eff.keep_extra_fields:
            keep_columns = list(reachable_columns)
        else:
            keep_columns = [name for name in reachable_columns if name in set(core_fields) | set(schema_fields)]
    elif len(options_eff.selected_fields) == 0:
        keep_columns = [name for name in core_fields if name in reachable_columns]
    else:
        requested = list(dict.fromkeys(list(core_fields) + list(options_eff.selected_fields)))
        unknown_selected = [name for name in options_eff.selected_fields if name not in reachable_columns]
        if unknown_selected:
            # Se emite warning porque los selected_fields inexistentes se omiten, pero no rompen el import por sí solos.
            emit_issue(
                issues,
                IMPORT_TRACES_ISSUES,
                "IMP.OPTIONS.SELECTED_FIELDS_UNKNOWN",
                selected_fields=list(options_eff.selected_fields),
                unknown_fields=unknown_selected,
                n_unknown=len(unknown_selected),
                available_columns_sample=_sample_list(reachable_columns, 20),
                available_columns_total=len(reachable_columns),
            )
        keep_columns = [name for name in requested if name in reachable_columns]

    dropped_columns = [name for name in reachable_columns if name not in keep_columns]
    if dropped_columns:
        # Se emite info para dejar evidencia de que hubo descarte explícito por política efectiva de selección.
        emit_issue(
            issues,
            IMPORT_TRACES_ISSUES,
            "IMP.OPTIONS.EXTRA_FIELDS_DROPPED",
            keep_extra_fields=options_eff.keep_extra_fields,
            selected_fields=list(options_eff.selected_fields) if options_eff.selected_fields is not None else None,
            n_dropped=len(dropped_columns),
            dropped_columns_sample=_sample_list(dropped_columns, 20),
            dropped_columns_total=len(dropped_columns),
        )

    work = work.loc[:, keep_columns].copy()
    return work, applied, len(applied)


def _materialize_trace_core(
    issues: List[Issue],
    df: pd.DataFrame,
    *,
    strict: bool,
) -> tuple[pd.DataFrame, bool]:
    """
    Garantiza el núcleo mínimo de traces y genera point_id cuando haga falta.

    Emite: IMP.CORE.POINT_ID_GENERATED, IMP.CORE.MINIMUM_FIELDS_UNREACHABLE.
    """
    work = df.copy(deep=True)
    point_id_generated = False

    if "point_id" not in work.columns:
        point_id_generated = True
        point_values = [f"p{i}" for i in range(len(work))]
        work.insert(0, "point_id", pd.Series(point_values, index=work.index, dtype="string"))
        # Se emite info porque la operación resolvió técnicamente la ausencia de point_id.
        emit_issue(
            issues,
            IMPORT_TRACES_ISSUES,
            "IMP.CORE.POINT_ID_GENERATED",
            field="point_id",
            insert_position=0,
            rows_out=len(work),
        )

    missing_core = [name for name in TRACE_CORE_REQUIRED_INPUT if name not in work.columns]
    if missing_core:
        # Se emite error fatal porque ya no es posible construir el mínimo canónico de salida.
        emit_and_maybe_raise(
            issues,
            IMPORT_TRACES_ISSUES,
            "IMP.CORE.MINIMUM_FIELDS_UNREACHABLE",
            strict=False,
            exception_map=EXCEPTION_MAP_IMPORT_TRACE,
            default_exception=PylondrinaImportError,
            missing_fields=missing_core,
            available_columns_sample=_sample_list(work.columns, 20),
            available_columns_total=len(work.columns),
        )

    ordered = ["point_id"] + [name for name in work.columns if name != "point_id"]
    work = work.loc[:, ordered].copy()
    return work, point_id_generated


def _normalize_trace_time_utc(
    issues: List[Issue],
    df: pd.DataFrame,
    *,
    schema: TraceSchema,
    options_eff: ImportTraceOptions,
) -> tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Interpreta y consolida la columna time_utc según la precedencia temporal vigente.

    Emite: IMP.TIME.TIMEZONE_UNRESOLVED, IMP.TIME.NORMALIZATION_FAILED.
    """
    work = df.copy(deep=True)

    if "time_utc" not in work.columns:
        # Se emite error fatal porque el mínimo canónico exige una columna temporal interpretable.
        emit_and_maybe_raise(
            issues,
            IMPORT_TRACES_ISSUES,
            "IMP.CORE.MINIMUM_FIELDS_UNREACHABLE",
            strict=False,
            exception_map=EXCEPTION_MAP_IMPORT_TRACE,
            default_exception=PylondrinaImportError,
            missing_fields=["time_utc"],
            available_columns_sample=_sample_list(work.columns, 20),
            available_columns_total=len(work.columns),
        )

    series = work["time_utc"]
    parsed = pd.to_datetime(series, errors="coerce", utc=False)
    invalid_mask = series.notna() & parsed.isna()
    if invalid_mask.any():
        # Se emite error fatal porque la columna temporal no quedó interpretable para construir traces canónicas.
        emit_and_maybe_raise(
            issues,
            IMPORT_TRACES_ISSUES,
            "IMP.TIME.NORMALIZATION_FAILED",
            strict=False,
            exception_map=EXCEPTION_MAP_IMPORT_TRACE,
            default_exception=PylondrinaImportError,
            field="time_utc",
            reason="unparseable_values",
            n_invalid=int(invalid_mask.sum()),
            invalid_values_sample=_sample_list(series[invalid_mask].tolist(), 10),
            source_timezone=options_eff.source_timezone,
            schema_timezone=schema.timezone,
        )

    tz_source_used: Optional[str] = None
    timezone_resolution = "explicit_data_timezone"
    if ptypes.is_datetime64tz_dtype(parsed):
        normalized = parsed.dt.tz_convert("UTC").dt.tz_localize(None)
        tz_source_used = "data"
    else:
        source_tz, _ = _normalize_timezone_spec(options_eff.source_timezone)
        schema_tz, _ = _normalize_timezone_spec(schema.timezone)
        effective_tz = source_tz or schema_tz
        if effective_tz is not None:
            tz_source_used = options_eff.source_timezone if source_tz is not None else schema.timezone
            timezone_resolution = "options.source_timezone" if source_tz is not None else "schema.timezone"
            try:
                localized = parsed.dt.tz_localize(effective_tz)
                normalized = localized.dt.tz_convert("UTC").dt.tz_localize(None)
            except Exception:
                # Se emite error fatal porque la localización temporal explícita no pudo aplicarse correctamente.
                emit_and_maybe_raise(
                    issues,
                    IMPORT_TRACES_ISSUES,
                    "IMP.TIME.NORMALIZATION_FAILED",
                    strict=False,
                    exception_map=EXCEPTION_MAP_IMPORT_TRACE,
                    default_exception=PylondrinaImportError,
                    field="time_utc",
                    reason="tz_localization_failed",
                    n_invalid=int(series.notna().sum()),
                    invalid_values_sample=_sample_list(series.tolist(), 10),
                    source_timezone=options_eff.source_timezone,
                    schema_timezone=schema.timezone,
                )
        else:
            normalized = parsed
            timezone_resolution = "unresolved"
            # Se emite warning porque la columna quedó parseable, pero no fue posible desambiguar su zona horaria.
            emit_issue(
                issues,
                IMPORT_TRACES_ISSUES,
                "IMP.TIME.TIMEZONE_UNRESOLVED",
                precedence_tried=["options.source_timezone", "schema.timezone", "explicit_data_timezone"],
                time_field="time_utc",
            )

    work["time_utc"] = normalized
    descriptor = {
        "time_field": "time_utc",
        "timezone_resolution": timezone_resolution,
        "source_timezone_used": tz_source_used,
        "schema_timezone": schema.timezone,
        "normalized_to_utc": timezone_resolution != "unresolved",
    }
    return work, _json_safe(descriptor)


def _finalize_import_traces_result(
    issues: List[Issue],
    df: pd.DataFrame,
    *,
    schema: TraceSchema,
    source_name: Optional[str],
    options_eff: ImportTraceOptions,
    parameters_effective: Dict[str, Any],
    field_map_applied: Dict[str, str],
    n_fields_mapped: int,
    point_id_generated: bool,
    temporal_descriptor: Dict[str, Any],
    provenance: Optional[Dict[str, Any]],
    rows_in: int,
) -> tuple[TraceDataset, ImportReport]:
    """
    Construye TraceDataset, ImportReport, metadata mínima y evento `import_traces`.

    Emite: IMP.PROVENANCE.INVALID_STRUCTURE, IMP.META.NOT_JSON_SERIALIZABLE.
    """
    rows_out = len(df)
    metadata: Dict[str, Any] = {
        "dataset_id": _make_trace_dataset_id(),
        "schema_version": schema.version,
        "is_validated": False,
        "events": [],
        "temporal": temporal_descriptor,
        "point_id_generated": bool(point_id_generated),
    }
    if source_name is not None:
        metadata["source"] = {"name": source_name}
    if field_map_applied:
        metadata["field_correspondence_applied"] = dict(field_map_applied)

    provenance_eff: Dict[str, Any] = {}
    if provenance is not None:
        if not isinstance(provenance, Mapping):
            # Se emite warning porque provenance debe ser mapping serializable para conservarse.
            emit_issue(
                issues,
                IMPORT_TRACES_ISSUES,
                "IMP.PROVENANCE.INVALID_STRUCTURE",
                received_type=type(provenance).__name__,
                reason="not_mapping",
                exception_type=None,
                exception_message=None,
            )
        else:
            provenance_eff = _json_safe(dict(provenance))
            if not _json_is_serializable(provenance_eff):
                # Se emite warning porque provenance no quedó en una forma serializable segura y se omitirá.
                emit_issue(
                    issues,
                    IMPORT_TRACES_ISSUES,
                    "IMP.PROVENANCE.INVALID_STRUCTURE",
                    received_type=type(provenance).__name__,
                    reason="not_json_serializable_after_sanitize",
                    exception_type=None,
                    exception_message=None,
                )
                provenance_eff = {}

    summary = {
        "rows_in": int(rows_in),
        "rows_out": int(rows_out),
        "n_fields_mapped": int(n_fields_mapped),
        "point_id_generated": bool(point_id_generated),
    }

    event_parameters = dict(parameters_effective)
    event_parameters.update(
        {
            "source_name": source_name,
            "source_timezone": options_eff.source_timezone,
            "schema_version": schema.version,
            "crs": schema.crs,
            "timezone": schema.timezone,
            "has_field_correspondence": bool(field_map_applied),
        }
    )
    event = {
        "op": "import_traces",
        "ts_utc": _utc_now_iso(),
        "parameters": _json_safe(event_parameters),
        "summary": _json_safe(summary),
        "issues_summary": _build_issues_summary(issues),
    }

    payload = {
        "metadata": metadata,
        "event": event,
    }
    if not _json_is_serializable(_json_safe(payload)):
        # Se emite error recuperable porque metadata/evento requieren degradación adicional para quedar serializables.
        emit_issue(
            issues,
            IMPORT_TRACES_ISSUES,
            "IMP.META.NOT_JSON_SERIALIZABLE",
            reason="payload_not_json_serializable_after_first_pass",
            problematic_keys=["metadata", "event"],
            exception_type=None,
            exception_message=None,
        )

    metadata = _json_safe(metadata)
    metadata["events"] = [_json_safe(event)]
    provenance_eff = _json_safe(provenance_eff)

    dataset = TraceDataset(
        data=df.copy(deep=True),
        schema=schema,
        provenance=provenance_eff,
        metadata=metadata,
    )
    report = ImportReport(
        ok=not any(issue.level == "error" for issue in issues),
        issues=list(issues),
        summary=_json_safe(summary),
        parameters=_json_safe(event_parameters),
        field_correspondence=dict(field_map_applied),
        value_correspondence={},
        schema_version=schema.version,
        metadata=_json_safe({"source_name": source_name}),
    )
    return dataset, report


# -----------------------------------------------------------------------------
# Helpers internos de uso general
# -----------------------------------------------------------------------------

def _duplicated_columns(columns: Iterable[Any]) -> List[str]:
    """Devuelve nombres de columnas duplicadas preservando una sola ocurrencia por nombre."""
    seen: set[str] = set()
    duplicates: List[str] = []
    for value in columns:
        text = str(value)
        if text in seen and text not in duplicates:
            duplicates.append(text)
        seen.add(text)
    return duplicates


def _normalize_timezone_spec(value: Optional[str]) -> tuple[Optional[Any], str]:
    """Normaliza una timezone IANA u offset fijo a un objeto interpretable por pandas."""
    if value is None:
        return None, "missing"
    if not isinstance(value, str):
        return None, "invalid"
    text = value.strip()
    if not text:
        return None, "invalid"
    if text in {"UTC", "Z"}:
        return "UTC", "utc"
    if OFFSET_RE.match(text):
        sign = 1 if text[0] == "+" else -1
        hours = int(text[1:3])
        minutes = int(text[4:6])
        return timezone(sign * timedelta(hours=hours, minutes=minutes)), "offset"
    try:
        return ZoneInfo(text), "iana"
    except Exception:
        return None, "invalid"


def _is_probably_crs(value: str) -> bool:
    """Evalúa de forma laxa si un CRS textual tiene forma reconocible para metadata."""
    if not isinstance(value, str):
        return False
    text = value.strip()
    if not text:
        return False
    return bool(re.match(r"^(EPSG:\d+|epsg:\d+|[A-Za-z0-9_./:-]+)$", text))


def _make_trace_dataset_id() -> str:
    """Genera un identificador lógico simple para TraceDataset."""
    return f"traces_{uuid.uuid4().hex[:12]}"


def _utc_now_iso() -> str:
    """Retorna un timestamp UTC ISO-8601 compacto con sufijo Z."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _build_issues_summary(issues: Sequence[Issue]) -> Dict[str, Any]:
    """Construye un resumen pequeño y estable de issues para dejarlo en el evento."""
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


def _sample_list(values: Iterable[Any], limit: int) -> List[Any]:
    """Devuelve una muestra simple y JSON-safe de una secuencia cualquiera."""
    out: List[Any] = []
    for value in values:
        out.append(_json_safe_scalar(value))
        if len(out) >= limit:
            break
    return out


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
    if isinstance(value, (pd.Timedelta, timedelta)):
        return value.total_seconds()
    return value


def _json_safe(value: Any) -> Any:
    """Convierte recursivamente estructuras comunes a una forma JSON-safe."""
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    return _json_safe_scalar(value)


def _json_is_serializable(obj: Any) -> bool:
    """Indica si un objeto ya puede serializarse con json.dumps sin errores."""
    try:
        json.dumps(obj)
        return True
    except Exception:
        return False