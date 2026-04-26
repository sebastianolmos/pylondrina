# -------------------------
# file: pylondrina/importing.py
# -------------------------
from __future__ import annotations

import json
import re
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from pandas.api import types as ptypes
import h3

from pylondrina.datasets import TripDataset
from pylondrina.errors import ImportError as PylondrinaImportError, SchemaError
from pylondrina.issues.catalog_import_trips import IMPORT_ISSUES
from pylondrina.issues.core import emit_and_maybe_raise, emit_issue
from pylondrina.reports import ImportReport, Issue
from pylondrina.schema import (
    CONSTRAINTS_BY_DTYPE,
    VALID_CONSTRAINT_KEYS,
    VALID_DTYPES,
    DomainSpec,
    FieldSpec,
    TripSchema,
    TripSchemaEffective,
)
from pylondrina.types import FieldCorrespondence, ValueCorrespondence

EXCEPTION_MAP_IMPORT = {
    "schema": SchemaError,
    "import": PylondrinaImportError,
}

DEFAULT_UNKNOWN = "unknown"
TRUE_SET = {"true", "t", "1", "yes", "y"}
FALSE_SET = {"false", "f", "0", "no", "n"}
OFFSET_RE = re.compile(r"^[+-](?:0\d|1\d|2[0-3]):[0-5]\d$")
DM_PATTERN = re.compile(
    r"""^\s*(?P<deg>\d{1,3})[°\s]+(?P<minutes>\d{1,2}(?:\.\d+)?)\s*'?\s*(?P<hem>[NSEW])\s*$""",
    re.VERBOSE | re.IGNORECASE,
)
DMS_PATTERN = re.compile(
    r"""^\s*
    (?P<deg>\d{1,3})
    (?:[°\s]+)
    (?P<minutes>\d{1,2})
    (?:['\s]+)
    (?P<seconds>\d{1,2}(?:\.\d+)?)
    (?:(?:["\s]+))
    (?P<hem>[NSEW])
    \s*$
    """,
    re.VERBOSE | re.IGNORECASE,
)

TRIPDATASET_COLUMNS_SOFT_CAP = 256
TRIPDATASET_COLUMNS_HARD_CAP = 1024

CATEGORICAL_INFERENCE_K_MAX = 10_000
CATEGORICAL_INFERENCE_ALPHA_DECLARED = 0.05

_HHMM_RE = re.compile(r"^(?P<h>\d{2}):(?P<m>\d{2})$")
DATETIME_LOCALIZE_NONEXISTENT = "shift_forward"
DATETIME_LOCALIZE_AMBIGUOUS = "NaT"

@dataclass(frozen=True)
class ImportOptions:
    """
    Opciones de importación/estandarización para construir un TripDataset.

    Attributes
    ----------
    keep_extra_fields : bool, default=True
        Si True, conserva columnas que no están en el esquema como campos extendidos del dataset.
    selected_fields : sequence of str, optional
        Lista de campos estándar (Golondrina) que el usuario desea conservar explícitamente además de los obligatorios.
        Si None, se conservan todos los campos del esquema que existan en la fuente.
    strict : bool, default=False
        Si True, inconsistencias relevantes detienen el proceso (excepción) en vez de sólo reportarse.
    strict_domains : bool, default=False
        Si True, valores categóricos fuera del dominio base se consideran error.
        Si False, se permite extensión controlada del dominio a nivel de dataset (si el DomainSpec lo permite).
    single_stage : bool, default=False
        Si True, cada fila representa un viaje individual (no se repite trip_id y movement_seq=0).
        Útil para fuentes ya "tripificadas" donde no hay etapas múltiples por viaje.
    source_timezone : str, optional
        Zona horaria de origen para datetimes naive (solo aplicable cuando se alcanza Tier 1).
        Formatos aceptados por contrato de diseño:
        - IANA: "Area/City" (ej: "America/Santiago")
        - Offset fijo: "±HH:MM" (ej: "-03:00")
        - "UTC" o "Z"
    """
    keep_extra_fields: bool = True
    selected_fields: Optional[Sequence[str]] = None
    strict: bool = False
    strict_domains: bool = False
    single_stage: bool = False
    source_timezone: Optional[str] = None


def import_trips_from_dataframe(
    df: pd.DataFrame,
    schema: TripSchema,
    *,
    source_name: Optional[str] = None,
    options: Optional[ImportOptions] = None,
    field_correspondence: Optional[FieldCorrespondence] = None,
    value_correspondence: Optional[ValueCorrespondence] = None,
    provenance: Optional[Dict[str, Any]] = None,
    h3_resolution: int = 8,
) -> Tuple[TripDataset, ImportReport]:
    """
    Importa (convierte) un DataFrame de viajes desde un formato externo al formato Golondrina.

    Este proceso realiza la **estandarización** de:
    - nombres de campos (según correspondencias),
    - valores categóricos (según dominios del esquema y/o correspondencias),
    - tipos y formatos básicos (según FieldSpec),
    y genera un TripDataset con trazabilidad (metadatos + reportes).

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame fuente con viajes en el formato original.
    schema : TripSchema
        Esquema del formato Golondrina a aplicar.
    source_name : str, optional
        Nombre de la fuente (p. ej., "EOD", "XDR", "ADATRAP"). Se registra en metadatos.
    options : ImportOptions, optional
        Opciones de importación y política de errores.
    field_correspondence : mapping, optional
        Correspondencia: campo estándar Golondrina -> columna en el DataFrame fuente.
        Si None, se asume que el DataFrame ya usa nombres estándar (o se delega a perfiles de fuente).
    value_correspondence : mapping, optional
        Correspondencia de valores categóricos por campo: campo -> (valor_fuente -> valor_canónico).
    provenance : dict, optional
        Metadatos de procedencia adicionales (periodo, zona, versión del dataset, etc.).
        Debe ser JSON-serializable.
    h3_resolution : int, default=8
        Resolución H3 a utilizar para derivar índices de celdas (origen/destino) cuando sea aplicable.
        Debe estar en el rango permitido por H3 (típicamente 0..15). Esta resolución se registra en
        los metadatos del dataset para reproducibilidad.

    Returns
    -------
    dataset : TripDataset
        Conjunto de viajes en formato Golondrina.
    report : ImportReport
        Reporte de importación con hallazgos y trazabilidad.
    """
    # Se toman snapshots mínimos del input y se prepara una copia de trabajo.
    rows_in = len(df)
    cols_in = list(df.columns)
    work = df.copy(deep=True)
    issues: List[Issue] = []
    columns_added: List[str] = []
    columns_deleted: List[str] = []

    # Se normalizan las opciones y se fijan las políticas efectivas del import.
    options_eff, parameters_effective, option_issues = _normalize_options(
        options,
        schema=schema,
        source_name=source_name,
        h3_resolution=h3_resolution,
    )
    issues.extend(option_issues)

    # Se revisa la sanidad estructural del schema y se construye la vista efectiva.
    schema_issues, schema_effective, schema_version = _check_schema_for_import(schema)
    issues.extend(schema_issues)

    # Se aplica la correspondencia de campos hacia nombres canónicos Golondrina.
    work, field_correspondence_applied, field_issues = _apply_field_correspondence(
        work,
        schema=schema,
        field_correspondence=field_correspondence,
        strict=options_eff.strict,
    )
    issues.extend(field_issues)
    n_fields_mapped = len(field_correspondence_applied)

    # Se decide tempranamente qué campos del schema deben sobrevivir al resultado final.
    target_schema_fields = set([])
    schema_fields = set(schema.fields.keys())
    required_fields = set(schema.required)
    if options_eff.selected_fields is None:
        target_schema_fields = set(schema_fields)
    elif len(options_eff.selected_fields) == 0:
        target_schema_fields = set(required_fields)
    else:
        target_schema_fields = required_fields | (set(options_eff.selected_fields) & schema_fields )

    # Se hace el chequeo mínimo de requeridos y se detecta el tier temporal disponible.
    req_issues, temporal_tier_detected, temporal_fields_present = _first_required_check_and_temporal_tier(
        work,
        schema=schema,
        single_stage=options_eff.single_stage,
        strict=options_eff.strict,
    )
    issues.extend(req_issues)

    # Se normalizan los campos categóricos objetivo y se construyen domains_effective.
    work, domains_effective, value_correspondence_applied, domains_extended, n_domain_mappings_applied, cat_issues = _standardize_categorical_values(
        work,
        schema=schema,
        schema_effective=schema_effective,
        value_correspondence=value_correspondence,
        options=options_eff,
        target_schema_fields=target_schema_fields,
    )
    issues.extend(cat_issues)

    # Se hace coercion de los tipos básicos por dtype lógico, dejando datetime avanzado para el paso temporal.
    work, _, coercion_issues = _coerce_columns_by_dtype(
        work,
        schema=schema,
        schema_effective=schema_effective,
        target_schema_fields=target_schema_fields,
        strict=options_eff.strict,
    )
    issues.extend(coercion_issues)

    # Se normalizan datetimes solo cuando existe Tier 1 y se registra el estado por columna.
    work, datetime_normalization_status_by_field, datetime_issues = _normalize_datetime_columns(
        work,
        schema=schema,
        schema_effective=schema_effective,
        options=options_eff,
        temporal_tier=temporal_tier_detected,
        strict=options_eff.strict,
    )
    issues.extend(datetime_issues)

    # Se normalizan columnas HH:MM solo si el dataset está en tier 2
    work, hhmm_normalization_stats, tier_2_issues = _normalize_tier2_hhmm_columns(
        work,
        temporal_tier=temporal_tier_detected,
        schema=schema,
    )
    issues.extend(tier_2_issues)

    # Se parsean las coordenadas OD en grados decimales sin incorporar lógica CRS-aware en import.
    work, _, coord_issues = _parse_od_coordinate_columns(
        work,
        schema=schema,
        target_schema_fields=target_schema_fields,
        strict=options_eff.strict,
    )
    issues.extend(coord_issues)

    # Se derivan índices H3 cuando existen pares lat/lon utilizables y la resolución es válida.
    work, h3_meta, h3_columns_added, h3_issues = _derive_h3_indices(
        work,
        schema=schema,
        h3_resolution=h3_resolution,
        strict=options_eff.strict,
    )
    issues.extend(h3_issues)
    columns_added.extend(h3_columns_added)

    # Se garantiza movement_id como identificador único de fila.
    work, movement_columns_added, movement_issues = _ensure_movement_id(
        work,
        strict=options_eff.strict,
    )
    issues.extend(movement_issues)
    columns_added.extend(movement_columns_added)

    # Se derivan trip_id y movement_seq solo cuando se fijó single_stage=True.
    work, single_stage_columns_added, single_stage_issues = _ensure_single_stage_ids(
        work,
        schema=schema,
        single_stage=options_eff.single_stage,
        strict=options_eff.strict,
    )
    issues.extend(single_stage_issues)
    columns_added.extend(single_stage_columns_added)

    # Se aplica la selección final de columnas del schema y la política de extras.
    work, columns_deleted, extra_fields_kept, selection_issues = _select_final_columns(
        work,
        schema=schema,
        options=options_eff,
    )
    issues.extend(selection_issues)

    # Guardrails de ancho de tabla: soft cap / hard cap.
    n_columns_out = len(work.columns)

    if n_columns_out > TRIPDATASET_COLUMNS_HARD_CAP:
        emit_and_maybe_raise(
            issues,
            IMPORT_ISSUES,
            "IMP.COLUMNS.HARD_CAP_EXCEEDED",
            strict=options_eff.strict,
            exception_map=EXCEPTION_MAP_IMPORT,
            default_exception=PylondrinaImportError,
            n_columns=n_columns_out,
            soft_cap=TRIPDATASET_COLUMNS_SOFT_CAP,
            hard_cap=TRIPDATASET_COLUMNS_HARD_CAP,
            extra_fields_kept_sample=extra_fields_kept[:10],
            extra_fields_kept_total=len(extra_fields_kept),
        )
    elif n_columns_out > TRIPDATASET_COLUMNS_SOFT_CAP:
        emit_issue(
            issues,
            IMPORT_ISSUES,
            "IMP.COLUMNS.WIDE_TABLE",
            n_columns=n_columns_out,
            soft_cap=TRIPDATASET_COLUMNS_SOFT_CAP,
            hard_cap=TRIPDATASET_COLUMNS_HARD_CAP,
            extra_fields_kept_sample=extra_fields_kept[:10],
            extra_fields_kept_total=len(extra_fields_kept),
        )

    # Se hace el chequeo final de construibilidad mínima después de derivaciones y poda.
    _final_required_check(
        work,
        schema=schema,
        single_stage=options_eff.single_stage,
        strict=options_eff.strict,
    )
    
    # Se alinea el schema efectivo con los campos del schema que realmente quedaron en el dataframe final.
    schema_effective = _prune_schema_effective(
        schema_effective,
        df=work,
        schema=schema,
    )

    # Se construye provenance mínima si no fue entregada por el usuario.
    if provenance is not None:
        provenance_eff =  provenance
    else:
        provenance_eff =  {
            "source_name": source_name,
            "created_by_op": "import_trips",
            "created_at_utc": datetime.now(timezone.utc).isoformat() }
        
    # Nota sobre tier temporal detectado
    temporal_notes = None
    if temporal_tier_detected == "tier_2":
        temporal_notes = "Tier 2: solo HH:MM local; capacidades temporales limitadas"
    elif temporal_tier_detected == "tier_3":
        temporal_notes = "Tier 3: sin tiempos OD explícitos"

    # Se arma el evento de importación con parámetros efectivos y resumen cuantitativo.
    event_import = {
        "op": "import_trips",
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "parameters": parameters_effective,
        "summary": {
            "input_rows": rows_in,
            "output_rows": len(work),
            "rows_dropped": rows_in - len(work),
            "n_fields_mapped": n_fields_mapped,
            "n_domain_mappings_applied": n_domain_mappings_applied,
            "columns_added": columns_added,
            "columns_deleted": columns_deleted,
            "domains_extended_count": len(domains_extended),
            "temporal_tier": temporal_tier_detected,
            "temporal_notes": temporal_notes,
        },
        "issues_summary": _build_issues_summary(issues),
    }

    # Se construye metadata completa y serializable del dataset importado.
    metadata = _build_import_metadata(
        schema=schema,
        schema_effective=schema_effective,
        field_correspondence_applied=field_correspondence_applied,
        value_correspondence_applied=value_correspondence_applied,
        domains_effective=domains_effective,
        domains_extended=domains_extended,
        extra_fields_kept=extra_fields_kept,
        h3_meta=h3_meta,
        provenance=provenance_eff,
        temporal_tier_detected=temporal_tier_detected,
        temporal_fields_present=temporal_fields_present,
        datetime_normalization_status_by_field=datetime_normalization_status_by_field,
        datetime_normalization_stats_t2 = hhmm_normalization_stats,
        source_timezone_used=options_eff.source_timezone,
        event_import=event_import,
        issues=issues,
        strict=options_eff.strict,
    )

    # Se materializa el TripDataset final con schema y trazabilidad del import.
    trip_dataset = TripDataset(
        data=work,
        schema=schema,
        schema_version=schema_version,
        provenance=provenance_eff,
        field_correspondence=field_correspondence_applied,
        value_correspondence=value_correspondence_applied,
        metadata=metadata,
        schema_effective=schema_effective,
    )
    trip_dataset.metadata["is_validated"] = False

    # Se consolida el ImportReport con el resumen estable de la operación.
    import_report = _build_import_report(
        issues=issues,
        field_correspondence_applied=field_correspondence_applied,
        value_correspondence_applied=value_correspondence_applied,
        schema_version=schema_version,
        source_name=source_name,
        dataset_id=metadata.get("dataset_id"),
        parameters_effective=parameters_effective,
        rows_in=rows_in,
        rows_out=len(work),
        n_fields_mapped=n_fields_mapped,
        n_domain_mappings_applied=n_domain_mappings_applied,
        metadata=metadata,
    )

    return trip_dataset, import_report

# -----------------------------------------------------------------------------
# Helpers internos principales del pipeline de importación
# -----------------------------------------------------------------------------

def _normalize_options(
    options: Optional[ImportOptions],
    *,
    schema: TripSchema,
    source_name: Optional[str],
    h3_resolution: int,
) -> tuple[ImportOptions, Dict[str, Any], List[Issue]]:
    """
    Emite: IMP.OPTIONS.INVALID_SELECTED_FIELD,
            IMP.OPTIONS.EMPTY_SELECTED_FIELD,
            IMP.H3.INVALID_RESOLUTION,
            IMP.DATETIME.INVALID_SOURCE_TIMEZONE
    """
    issues: List[Issue] = []
    options_eff = options or ImportOptions()

    # Se revisa la selección de campos en ImportOptions
    selected_fields: Optional[List[str]]
    if options_eff.selected_fields is None:
        selected_fields = None
    else:
        selected_fields = list(options_eff.selected_fields)
        if len(selected_fields) == 0:
            emit_issue(
                issues,
                IMPORT_ISSUES,
                "IMP.OPTIONS.EMPTY_SELECTED_FIELD",
                selected_fields=[],
                effective_selected_fields=list(schema.required),
            )

    # Se revisa si los campos seleccionados estan definidos en el schema
    invalid_selected = []
    if selected_fields is not None:
        schema_fields = set(schema.fields.keys())
        invalid_selected = sorted(set(selected_fields) - schema_fields)
        if invalid_selected:
            emit_and_maybe_raise(
                issues,
                IMPORT_ISSUES,
                "IMP.OPTIONS.INVALID_SELECTED_FIELD",
                strict=options_eff.strict,
                exception_map=EXCEPTION_MAP_IMPORT,
                default_exception=PylondrinaImportError,
                selected_fields=selected_fields,
                invalid_fields=invalid_selected,
            )

    # Se revisa si se entrego una resolución h3 valida
    is_valid_h3_resolution = isinstance(h3_resolution, int) and 0 <= h3_resolution <= 15
    if not is_valid_h3_resolution:
        emit_and_maybe_raise(
            issues,
            IMPORT_ISSUES,
            "IMP.H3.INVALID_RESOLUTION",
            strict=options_eff.strict,
            exception_map=EXCEPTION_MAP_IMPORT,
            default_exception=PylondrinaImportError,
            h3_resolution=h3_resolution,
        )

    # Se revisa si se entrego una zona horaria valida
    tz_normalized, tz_kind = _normalize_source_timezone(options_eff.source_timezone)
    if options_eff.source_timezone is not None and tz_kind == "invalid":
        emit_issue(
            issues,
            IMPORT_ISSUES,
            "IMP.DATETIME.INVALID_SOURCE_TIMEZONE",
            source_timezone=options_eff.source_timezone,
        )

    options_eff = ImportOptions(
        keep_extra_fields=bool(options_eff.keep_extra_fields),
        selected_fields=selected_fields,
        strict=bool(options_eff.strict),
        strict_domains=bool(options_eff.strict_domains),
        single_stage=bool(options_eff.single_stage),
        source_timezone=tz_normalized,
    )

    parameters_effective = {
        "keep_extra_fields": options_eff.keep_extra_fields,
        "selected_fields": list(options_eff.selected_fields) if options_eff.selected_fields is not None else None,
        "strict": options_eff.strict,
        "strict_domains": options_eff.strict_domains,
        "single_stage": options_eff.single_stage,
        "h3_resolution": h3_resolution,
        "source_name": source_name,
        "source_timezone": options_eff.source_timezone,
    }
    return options_eff, parameters_effective, issues

def _check_schema_for_import(schema: TripSchema) -> tuple[List[Issue], TripSchemaEffective, str]:
    """
    Emite: SCH.TRIP_SCHEMA.INVALID_VERSION,
            SCH.TRIP_SCHEMA.EMPTY_FIELDS,
            SCH.TRIP_SCHEMA.EMPTY_REQUIRED,
            SCH.FIELD_SPEC.UNKNOWN_DTYPE,
            SCH.DOMAIN.MISSING_FOR_CATEGORICAL,
            SCH.DOMAIN.EMPTY_VALUES,
            SCH.DOMAIN.NON_STRING_VALUES,
            SCH.CONSTRAINTS.INVALID_FORMAT,
            SCH.CONSTRAINTS.UNKNOWN_RULE,
            SCH.CONSTRAINTS.INCOMPATIBLE_WITH_DTYPE
    """
    issues: List[Issue] = []

    if not isinstance(schema.version, str) or not schema.version.strip():
        emit_and_maybe_raise(
            issues,
            IMPORT_ISSUES,
            "SCH.TRIP_SCHEMA.INVALID_VERSION",
            strict=False,
            exception_map=EXCEPTION_MAP_IMPORT,
            default_exception=SchemaError,
            schema_version=schema.version,
            schema_name=None,
        )

    # Se revisa que schema tenga campos
    if not schema.fields:
        emit_and_maybe_raise(
            issues,
            IMPORT_ISSUES,
            "SCH.TRIP_SCHEMA.EMPTY_FIELDS",
            strict=False,
            exception_map=EXCEPTION_MAP_IMPORT,
            default_exception=SchemaError,
            schema_version=schema.version,
            fields_size=0,
        )

    # Se revisa que schema tenga campos requeridos
    if not schema.required:
        emit_and_maybe_raise(
            issues,
            IMPORT_ISSUES,
            "SCH.TRIP_SCHEMA.EMPTY_REQUIRED",
            strict=False,
            exception_map=EXCEPTION_MAP_IMPORT,
            default_exception=SchemaError,
            schema_version=schema.version,
            required_size=0,
        )

    # Se revisa que los campos requeridos esten en los campos definidos
    required_not_in_fields = sorted(set(schema.required) - set(schema.fields.keys()))
    if required_not_in_fields:
        emit_and_maybe_raise(
            issues,
            IMPORT_ISSUES,
            "IMP.INPUT.MISSING_REQUIRED_FIELD",
            strict=False,
            exception_map=EXCEPTION_MAP_IMPORT,
            default_exception=SchemaError,
            missing_required=required_not_in_fields,
            required=list(schema.required),
            source_columns=[],
            field_correspondence_keys=[],
            field_correspondence_values_sample=[],
        )

    schema_effective = TripSchemaEffective()

    # Se revisa por campo definido en schema
    for field_name, fs in schema.fields.items():
        dtype_eff = fs.dtype

        # Se revisa si es un tipo esperado por el modulo
        if fs.dtype not in VALID_DTYPES:
            emit_issue(
                issues,
                IMPORT_ISSUES,
                "SCH.FIELD_SPEC.UNKNOWN_DTYPE",
                field=field_name,
                dtype=fs.dtype,
                fallback_dtype="string",
            )
            dtype_eff = "string"
            schema_effective.overrides.setdefault(field_name, {}).setdefault("reasons", []).append("dtype_invalid")

        # Se revisa los campos categoricos si son validos
        if fs.dtype == "categorical":
            if fs.domain is None:
                emit_issue(
                    issues,
                    IMPORT_ISSUES,
                    "SCH.DOMAIN.MISSING_FOR_CATEGORICAL",
                    field=field_name,
                )
                dtype_eff = "string"
                schema_effective.overrides.setdefault(field_name, {}).setdefault("reasons", []).append("categorical_no_domain")
            else:
                if len(fs.domain.values) == 0:
                    emit_issue(
                        issues,
                        IMPORT_ISSUES,
                        "SCH.DOMAIN.EMPTY_VALUES",
                        field=field_name,
                        values_size=0,
                        extendable=bool(fs.domain.extendable),
                    )
                bad_vals = [v for v in fs.domain.values if not isinstance(v, str)]
                if bad_vals:
                    emit_issue(
                        issues,
                        IMPORT_ISSUES,
                        "SCH.DOMAIN.NON_STRING_VALUES",
                        field=field_name,
                        domain_values_sample=bad_vals[:5],
                        domain_values_total=len(bad_vals),
                    )

        # Se revisa si los constraints son esperados
        if fs.constraints is not None:
            if not isinstance(fs.constraints, dict):
                emit_and_maybe_raise(
                    issues,
                    IMPORT_ISSUES,
                    "SCH.CONSTRAINTS.INVALID_FORMAT",
                    strict=False,
                    exception_map=EXCEPTION_MAP_IMPORT,
                    default_exception=SchemaError,
                    field=field_name,
                    rule_raw=repr(fs.constraints),
                )
            else:
                keys = list(fs.constraints.keys())
                unknown = [k for k in keys if k not in VALID_CONSTRAINT_KEYS]
                if unknown:
                    emit_and_maybe_raise(
                        issues,
                        IMPORT_ISSUES,
                        "SCH.CONSTRAINTS.UNKNOWN_RULE",
                        strict=False,
                        exception_map=EXCEPTION_MAP_IMPORT,
                        default_exception=SchemaError,
                        field=field_name,
                        rule=unknown[0],
                        supported_rules=sorted(VALID_CONSTRAINT_KEYS),
                    )
                if "pattern" in fs.constraints:
                    pattern = fs.constraints["pattern"]
                    if not isinstance(pattern, str):
                        emit_and_maybe_raise(
                            issues,
                            IMPORT_ISSUES,
                            "SCH.CONSTRAINTS.INVALID_FORMAT",
                            strict=False,
                            exception_map=EXCEPTION_MAP_IMPORT,
                            default_exception=SchemaError,
                            field=field_name,
                            rule_raw=repr(pattern),
                        )
                    else:
                        try:
                            re.compile(pattern)
                        except re.error as exc:
                            emit_and_maybe_raise(
                                issues,
                                IMPORT_ISSUES,
                                "SCH.CONSTRAINTS.INVALID_FORMAT",
                                strict=False,
                                exception_map=EXCEPTION_MAP_IMPORT,
                                default_exception=SchemaError,
                                field=field_name,
                                rule_raw=str(exc),
                            )
                if fs.dtype in CONSTRAINTS_BY_DTYPE:
                    allowed = CONSTRAINTS_BY_DTYPE[fs.dtype]
                    incompatible = [k for k in keys if k in VALID_CONSTRAINT_KEYS and k not in allowed]
                    for rule in incompatible:
                        emit_issue(
                            issues,
                            IMPORT_ISSUES,
                            "SCH.CONSTRAINTS.INCOMPATIBLE_WITH_DTYPE",
                            field=field_name,
                            dtype=fs.dtype,
                            rule=rule,
                        )

        schema_effective.dtype_effective[field_name] = dtype_eff

    return issues, schema_effective, schema.version

def _apply_field_correspondence(
    df: pd.DataFrame,
    *,
    schema: TripSchema,
    field_correspondence: Optional[FieldCorrespondence],
    strict: bool,
) -> tuple[pd.DataFrame, Dict[str, str], List[Issue]]:
    """
    Emite: MAP.FIELDS.UNKNOWN_CANONICAL_FIELD,
            MAP.FIELDS.MISSING_SOURCE_COLUMN,
            MAP.FIELDS.COLLISION_DUPLICATE_TARGET,
            MAP.FIELDS.CANONICAL_ALREADY_PRESENT_CONFLICT,
            IMP.INPUT.OPTIONAL_FIELD_NOT_FOUND
    """
    issues: List[Issue] = []
    work = df
    schema_fields = set(schema.fields.keys())
    field_corr_input = dict(field_correspondence or {})

    # Se revisa que los campos indicados (al final del map) sean canonicos
    for canonical in field_corr_input.keys():
        if canonical not in schema_fields:
            emit_and_maybe_raise(
                issues,
                IMPORT_ISSUES,
                "MAP.FIELDS.UNKNOWN_CANONICAL_FIELD",
                strict=strict,
                exception_map=EXCEPTION_MAP_IMPORT,
                default_exception=PylondrinaImportError,
                field=canonical,
                schema_fields_sample=sorted(list(schema_fields))[:10],
                schema_fields_total=len(schema_fields),
            )

    source_to_canon: Dict[str, List[str]] = {}
    for canonical, source in field_corr_input.items():
        source_to_canon.setdefault(str(source), []).append(canonical)

    for source, canonicals in source_to_canon.items():
        if len(canonicals) > 1:
            emit_and_maybe_raise(
                issues,
                IMPORT_ISSUES,
                "MAP.FIELDS.COLLISION_DUPLICATE_TARGET",
                strict=strict,
                exception_map=EXCEPTION_MAP_IMPORT,
                default_exception=PylondrinaImportError,
                source_column=source,
                canonical_fields=canonicals,
                field_correspondence=field_corr_input,
            )

    rename_map: Dict[str, str] = {}
    applied: Dict[str, str] = {}

    for canonical, source in field_corr_input.items():
        if source == canonical:
            if source not in work.columns:
                if canonical in schema.required:
                    emit_and_maybe_raise(
                        issues,
                        IMPORT_ISSUES,
                        "MAP.FIELDS.MISSING_SOURCE_COLUMN",
                        strict=strict,
                        exception_map=EXCEPTION_MAP_IMPORT,
                        default_exception=PylondrinaImportError,
                        field=canonical,
                        source_field=source,
                        source_columns_sample=list(work.columns)[:10],
                        source_columns_total=len(work.columns),
                    )
                else:
                    emit_issue(
                        issues,
                        IMPORT_ISSUES,
                        "IMP.INPUT.OPTIONAL_FIELD_NOT_FOUND",
                        field=canonical,
                        source_columns=list(work.columns),
                        field_correspondence_used=False,
                    )
            continue

        if source not in work.columns:
            if canonical in schema.required:
                emit_and_maybe_raise(
                    issues,
                    IMPORT_ISSUES,
                    "MAP.FIELDS.MISSING_SOURCE_COLUMN",
                    strict=strict,
                    exception_map=EXCEPTION_MAP_IMPORT,
                    default_exception=PylondrinaImportError,
                    field=canonical,
                    source_field=source,
                    source_columns_sample=list(work.columns)[:10],
                    source_columns_total=len(work.columns),
                )
            else:
                emit_issue(
                    issues,
                    IMPORT_ISSUES,
                    "IMP.INPUT.OPTIONAL_FIELD_NOT_FOUND",
                    field=canonical,
                    source_columns=list(work.columns),
                    field_correspondence_used=True,
                )
            continue

        if canonical in work.columns and source != canonical:
            emit_and_maybe_raise(
                issues,
                IMPORT_ISSUES,
                "MAP.FIELDS.CANONICAL_ALREADY_PRESENT_CONFLICT",
                strict=strict,
                exception_map=EXCEPTION_MAP_IMPORT,
                default_exception=PylondrinaImportError,
                field=canonical,
                source_field=source,
            )

        rename_map[source] = canonical
        applied[canonical] = source

    if rename_map:
        work = work.rename(columns=rename_map, copy=False)

    if work.columns.duplicated().any():
        dup_cols = list(work.columns[work.columns.duplicated()])
        emit_and_maybe_raise(
            issues,
            IMPORT_ISSUES,
            "MAP.FIELDS.COLLISION_DUPLICATE_TARGET",
            strict=strict,
            exception_map=EXCEPTION_MAP_IMPORT,
            default_exception=PylondrinaImportError,
            source_column=dup_cols[0],
            canonical_fields=dup_cols,
            field_correspondence=field_corr_input,
        )

    derivable_optional = {"movement_id", "trip_id", "movement_seq", "origin_h3_index", "destination_h3_index"}
    for field_name, fs in schema.fields.items():
        if (
            field_name not in work.columns
            and not fs.required
            and field_name not in field_corr_input
            and field_name not in derivable_optional
        ):
            emit_issue(
                issues,
                IMPORT_ISSUES,
                "IMP.INPUT.OPTIONAL_FIELD_NOT_FOUND",
                field=field_name,
                source_columns=list(df.columns),
                field_correspondence_used=False,
            )

    return work, applied, issues

def _first_required_check_and_temporal_tier(
    df: pd.DataFrame,
    *,
    schema: TripSchema,
    single_stage: bool,
    strict: bool,
) -> tuple[List[Issue], str, List[str]]:
    """
    Emite: IMP.INPUT.EMPTY_DATAFRAME,
            IMP.INPUT.MISSING_REQUIRED_FIELD,
            IMP.TEMPORAL.TIER_LIMITED
    """
    issues: List[Issue] = []

    if len(df) == 0:
        emit_issue(
            issues,
            IMPORT_ISSUES,
            "IMP.INPUT.EMPTY_DATAFRAME",
            rows_in=0,
            columns_in=list(df.columns),
        )

    required_fields = set(schema.required)
    missing_required = sorted(required_fields - set(df.columns))
    derivable = {"movement_id", "origin_h3_index", "destination_h3_index"}
    if single_stage:
        derivable.update({"trip_id", "movement_seq"})
    missing_non_derivable = sorted([f for f in missing_required if f not in derivable])
    if missing_non_derivable:
        emit_and_maybe_raise(
            issues,
            IMPORT_ISSUES,
            "IMP.INPUT.MISSING_REQUIRED_FIELD",
            strict=strict,
            exception_map=EXCEPTION_MAP_IMPORT,
            default_exception=PylondrinaImportError,
            missing_required=missing_non_derivable,
            required=list(schema.required),
            source_columns=list(df.columns),
            field_correspondence_keys=[],
            field_correspondence_values_sample=[],
        )

    # Se detecta cual es el tier o nivel de prioridad con respecto a los campos temporales
    cols = set(df.columns)
    fields_present = [
        c
        for c in [
            "origin_time_utc",
            "destination_time_utc",
            "origin_time_local_hhmm",
            "destination_time_local_hhmm",
        ]
        if c in cols
    ]
    if {"origin_time_utc", "destination_time_utc"}.issubset(cols):
        temporal_tier, temporal_fields_present = "tier_1", fields_present
    elif {"origin_time_local_hhmm", "destination_time_local_hhmm"}.issubset(cols):
        temporal_tier, temporal_fields_present = "tier_2", fields_present
    else:
        temporal_tier, temporal_fields_present = "tier_3", fields_present

    if temporal_tier in {"tier_2", "tier_3"}:
        emit_issue(
            issues,
            IMPORT_ISSUES,
            "IMP.TEMPORAL.TIER_LIMITED",
            temporal_tier=temporal_tier,
            fields_present=temporal_fields_present,
            note="limited_temporal_capabilities",
        )
    return issues, temporal_tier, temporal_fields_present

def _standardize_categorical_values(
    df: pd.DataFrame,
    *,
    schema: TripSchema,
    schema_effective: TripSchemaEffective,
    value_correspondence: Optional[ValueCorrespondence],
    options: ImportOptions,
    target_schema_fields: set[str],
) -> tuple[pd.DataFrame, Dict[str, Any], Dict[str, Dict[str, str]], List[str], int, List[Issue]]:
    """
    Emite: MAP.VALUES.UNKNOWN_CANONICAL_FIELD,
            MAP.VALUES.NON_CATEGORICAL_FIELD,
            MAP.VALUES.UNKNOWN_CANONICAL_VALUE,
            DOM.POLICY.FIELD_NOT_EXTENDABLE,
            DOM.POLICY.MAPPING_REQUIRES_EXTENSION_BLOCKED,
            DOM.STRICT.OUT_OF_DOMAIN_ABORT,
            DOM.EXTENSION.APPLIED,
            DOM.INFERENCE.APPLIED,
            DOM.INFERENCE.DEGRADED_TO_STRING
    """
    issues: List[Issue] = []
    work = df
    vc_applied: Dict[str, Dict[str, str]] = {}
    domains_effective: Dict[str, Any] = {}
    domains_extended: List[str] = []
    n_domain_mappings_applied = 0
    value_correspondence = value_correspondence or {}

    for field_name, mapping in value_correspondence.items():
        # Se revisa que el campo del cual se aplicara correspondencia, exista en el schema
        if field_name not in schema.fields:
            emit_and_maybe_raise(
                issues,
                IMPORT_ISSUES,
                "MAP.VALUES.UNKNOWN_CANONICAL_FIELD",
                strict=options.strict,
                exception_map=EXCEPTION_MAP_IMPORT,
                default_exception=PylondrinaImportError,
                field=field_name,
                schema_fields_sample=sorted(list(schema.fields.keys()))[:10],
                schema_fields_total=len(schema.fields),
            )
        fs = schema.fields[field_name]
        effective_dtype = schema_effective.dtype_effective.get(field_name, fs.dtype)
        # Se revisa que el tipo del campo en el schema efectivo sea categorico
        if effective_dtype != "categorical":
            emit_issue(
                issues,
                IMPORT_ISSUES,
                "MAP.VALUES.NON_CATEGORICAL_FIELD",
                field=field_name,
                field_dtype=effective_dtype,
            )
            continue
        # Se revisa politica de extension, viendo si el flujo puede continuar
        if fs.domain is not None and fs.domain.values:
            base = set(str(v) for v in fs.domain.values)
            mapped_targets = {str(v) for v in mapping.values()}
            unknown_targets = sorted(mapped_targets - base)
            if unknown_targets:
                # Pasa si el dominio se puede extender, luego no pasa por politica de no extension o por que el dominion no aceptar extension (desconoce valores fuera de los canonicos)
                if fs.domain.extendable and not options.strict_domains:
                    pass
                elif fs.domain.extendable and options.strict_domains:
                    emit_and_maybe_raise(
                        issues,
                        IMPORT_ISSUES,
                        "DOM.POLICY.MAPPING_REQUIRES_EXTENSION_BLOCKED",
                        strict=options.strict,
                        exception_map=EXCEPTION_MAP_IMPORT,
                        default_exception=PylondrinaImportError,
                        field=field_name,
                        strict_domains=options.strict_domains,
                        domain_extendable=fs.domain.extendable,
                        unmapped_examples=unknown_targets[:5],
                        unmapped_count=len(unknown_targets),
                    )
                else:
                    emit_and_maybe_raise(
                        issues,
                        IMPORT_ISSUES,
                        "MAP.VALUES.UNKNOWN_CANONICAL_VALUE",
                        strict=options.strict,
                        exception_map=EXCEPTION_MAP_IMPORT,
                        default_exception=PylondrinaImportError,
                        field=field_name,
                        canonical_value=unknown_targets[0],
                        domain_values_sample=sorted(list(base))[:10],
                        domain_values_total=len(base),
                    )

    for field_name, fs in schema.fields.items():
        effective_dtype = schema_effective.dtype_effective.get(field_name, fs.dtype)
        # No se hace la correspondencia si el campo no es categorico o no esta en la especificacion del schema (y selected fields) o no esta en el dataframe
        if (
            effective_dtype != "categorical"
            or field_name not in target_schema_fields
            or field_name not in work.columns
        ):
            continue

        domain = fs.domain
        if domain is None:
            continue

        # Se fuerza el tipo de la columna y strings vacios se ponen nulos
        field_column = work[field_name]
        out = field_column.astype("string").str.strip()
        s = out.replace("", pd.NA)

        # Se aplica la correspondencia, guardando los pares realmente usados
        s_mapped, used_pairs = _apply_value_correspondence(s, dict(value_correspondence.get(field_name, {})) if value_correspondence else None)
        if used_pairs:
            vc_applied[field_name] = used_pairs
            n_domain_mappings_applied += len(used_pairs)

        base = set(str(v) for v in (domain.values or []))
        observed = set(str(v) for v in s_mapped.dropna().unique())
        observed_values_sorted = sorted(observed)
        unknown_token = _get_unknown_token(domain)
        added_values: set[str] = set()
        unknown_values: List[str] = []

        # Caso especial v1.1:
        # dtype='categorical' + DomainSpec.values vacío -> inferencia bootstrap controlada.
        if len(base) == 0:
            n_rows_non_null = int(s_mapped.notna().sum())
            n_unique_observed = len(observed)
            alpha = CATEGORICAL_INFERENCE_ALPHA_DECLARED
            cardinality_limit = min(
                float(CATEGORICAL_INFERENCE_K_MAX),
                alpha * float(n_rows_non_null),
            )

            inference_policy = {
                "mode": "declared_categorical_empty_domain",
                "alpha": alpha,
                "k_max": CATEGORICAL_INFERENCE_K_MAX,
                "cardinality_limit": cardinality_limit,
                "n_rows_non_null": n_rows_non_null,
            }

            if n_unique_observed <= cardinality_limit:
                emit_issue(
                    issues,
                    IMPORT_ISSUES,
                    "DOM.INFERENCE.APPLIED",
                    field=field_name,
                    n_rows_non_null=n_rows_non_null,
                    n_unique_observed=n_unique_observed,
                    alpha=alpha,
                    k_max=CATEGORICAL_INFERENCE_K_MAX,
                    cardinality_limit=cardinality_limit,
                    observed_values_sample=observed_values_sorted[:5],
                    observed_values_total=n_unique_observed,
                )

                work[field_name] = s_mapped.astype("string")

                domains_effective[field_name] = {
                    "base_values": [],
                    "observed_values": observed_values_sorted,
                    "extended_values": [],
                    "unknown_values": [],
                    "extendable": bool(domain.extendable),
                    "unknown_value": unknown_token,
                    "n_unique_observed": n_unique_observed,
                    "n_added": 0,
                    "value_correspondence_applied": vc_applied.get(field_name, {}),
                    "values": observed_values_sorted,
                    "extended": False,
                    "added_values": [],
                    "strict_applied": bool(options.strict_domains),
                    "inference_applied": True,
                    "inference_policy": inference_policy,
                }
                schema_effective.domains_effective[field_name] = domains_effective[field_name]
                schema_effective.overrides.setdefault(field_name, {}).setdefault("reasons", []).append(
                    "categorical_domain_inferred_from_observed_values"
                )
                schema_effective.overrides[field_name]["inference_policy"] = inference_policy

                continue

            emit_issue(
                issues,
                IMPORT_ISSUES,
                "DOM.INFERENCE.DEGRADED_TO_STRING",
                field=field_name,
                n_rows_non_null=n_rows_non_null,
                n_unique_observed=n_unique_observed,
                alpha=alpha,
                k_max=CATEGORICAL_INFERENCE_K_MAX,
                cardinality_limit=cardinality_limit,
                observed_values_sample=observed_values_sorted[:5],
                observed_values_total=n_unique_observed,
                fallback_dtype="string",
                reason="high_cardinality_for_categorical_inference",
            )

            work[field_name] = s_mapped.astype("string")
            schema_effective.dtype_effective[field_name] = "string"
            schema_effective.domains_effective.pop(field_name, None)

            schema_effective.overrides.setdefault(field_name, {}).setdefault("reasons", []).append(
                "categorical_inference_degraded_to_string_high_cardinality"
            )
            schema_effective.overrides[field_name]["fallback_dtype"] = "string"
            schema_effective.overrides[field_name]["inference_policy"] = inference_policy
            schema_effective.overrides[field_name]["observed_values_sample"] = observed_values_sorted[:5]
            schema_effective.overrides[field_name]["observed_values_total"] = n_unique_observed

            continue

        out_of_domain = observed - base

        if out_of_domain:
            # Si el dominio no de puede extender se avisa y los valores fuera del dominio pasar a ser unknown
            if not domain.extendable:
                emit_issue(
                    issues,
                    IMPORT_ISSUES,
                    "DOM.POLICY.FIELD_NOT_EXTENDABLE",
                    field=field_name,
                    strict_domains=options.strict_domains,
                    domain_extendable=domain.extendable,
                )
                unknown_values = sorted(out_of_domain)
                s_mapped = s_mapped.where(~s_mapped.isin(out_of_domain), other=unknown_token)
                base_with_unknown = set(base)
                base_with_unknown.add(unknown_token)
            # Si hay politica de no extender dominios (pero dominio si podia extenderse) se emite error
            elif options.strict_domains:
                emit_and_maybe_raise(
                    issues,
                    IMPORT_ISSUES,
                    "DOM.STRICT.OUT_OF_DOMAIN_ABORT",
                    strict=options.strict,
                    exception_map=EXCEPTION_MAP_IMPORT,
                    default_exception=PylondrinaImportError,
                    field=field_name,
                    unknown_count=len(out_of_domain),
                    total_count=len(observed),
                    unknown_rate=(len(out_of_domain) / len(observed)) if observed else 0.0,
                    unknown_examples=sorted(list(out_of_domain))[:5],
                    policy="strict_domains",
                )
                base_with_unknown = set(base)
            # Caso en que se puede extender dominios, se emite info
            else:
                added_values = set(out_of_domain)
                emit_issue(
                    issues,
                    IMPORT_ISSUES,
                    "DOM.EXTENSION.APPLIED",
                    field=field_name,
                    n_added=len(added_values),
                    added_values_sample=sorted(list(added_values))[:5],
                    added_values_total=len(added_values),
                    policy="extendable_non_strict",
                )
                base_with_unknown = set(base)
                base_with_unknown.add(unknown_token)
                domains_extended.append(field_name)
        else:
            base_with_unknown = set(base)
            base_with_unknown.add(unknown_token)

        work[field_name] = s_mapped.astype("string")
        domains_effective[field_name] = {
            "base_values": sorted(base),
            "observed_values": sorted(observed),
            "extended_values": sorted(added_values),
            "unknown_values": unknown_values,
            "extendable": bool(domain.extendable),
            "unknown_value": unknown_token,
            "n_unique_observed": len(observed),
            "n_added": len(added_values),
            "value_correspondence_applied": vc_applied.get(field_name, {}),
            "values": sorted(base_with_unknown | added_values),
            "extended": bool(added_values),
            "added_values": sorted(added_values),
            "strict_applied": bool(options.strict_domains),
        }
        schema_effective.domains_effective[field_name] = domains_effective[field_name]
        if added_values:
            schema_effective.overrides.setdefault(field_name, {}).setdefault("reasons", []).append("domain_extended")
            schema_effective.overrides[field_name]["added_values"] = sorted(added_values)
        if unknown_values:
            schema_effective.overrides.setdefault(field_name, {}).setdefault("reasons", []).append("out_of_domain_mapped_to_unknown")
            schema_effective.overrides[field_name]["unknown_values"] = unknown_values

    return work, domains_effective, vc_applied, domains_extended, n_domain_mappings_applied, issues

def _coerce_columns_by_dtype(
    df: pd.DataFrame,
    *,
    schema: TripSchema,
    schema_effective: TripSchemaEffective,
    target_schema_fields: set[str],
    strict: bool,
) -> tuple[pd.DataFrame, Dict[str, Any], List[Issue]]:
    """
    Emite: IMP.TYPE.COERCE_FAILED_REQUIRED,
            IMP.TYPE.COERCE_PARTIAL
    """
    COORD_FIELDS = {
        "origin_latitude",
        "origin_longitude",
        "destination_latitude",
        "destination_longitude",
    }
    issues: List[Issue] = []
    work = df
    coercion_stats: Dict[str, Any] = {}

    for field_name, fs in schema.fields.items():
        if field_name not in work.columns or field_name not in target_schema_fields:
            continue

        # Las coordenadas OD no se coercionan aquí.
        # Se procesan exclusivamente en _parse_od_coordinate_columns(...)
        if field_name in COORD_FIELDS:
            continue

        expected = schema_effective.dtype_effective.get(field_name, fs.dtype)
        original_series = work[field_name].copy()
        out, stats = _coerce_series_to_dtype(original_series, expected, parse_datetime=False)
        work[field_name] = out
        coercion_stats[field_name] = stats

        parse_fail_count = int(stats["na_delta"])
        if parse_fail_count <= 0:
            continue

        total_count = len(work)
        fail_rate = (parse_fail_count / total_count) if total_count else 0.0
        examples = original_series.loc[out.isna()].head(5).astype("string").tolist()
        usable_count = int(out.notna().sum()) if hasattr(out, "notna") else total_count - parse_fail_count

        if fs.required and usable_count == 0 and total_count > 0:
            emit_and_maybe_raise(
                issues,
                IMPORT_ISSUES,
                "IMP.TYPE.COERCE_FAILED_REQUIRED",
                strict=strict,
                exception_map=EXCEPTION_MAP_IMPORT,
                default_exception=PylondrinaImportError,
                field=field_name,
                dtype_expected=expected,
                parse_fail_count=parse_fail_count,
                rows_in=total_count,
                fail_rate=fail_rate,
                examples_sample=examples,
                row_count=parse_fail_count,
            )
        else:
            emit_issue(
                issues,
                IMPORT_ISSUES,
                "IMP.TYPE.COERCE_PARTIAL",
                field=field_name,
                dtype_expected=expected,
                parse_fail_count=parse_fail_count,
                total_count=total_count,
                fail_rate=fail_rate,
                row_count=parse_fail_count,
            )
    return work, coercion_stats, issues

def _normalize_datetime_columns(
    df: pd.DataFrame,
    *,
    schema: TripSchema,
    schema_effective: TripSchemaEffective,
    options: ImportOptions,
    temporal_tier: str,
    strict: bool,
) -> tuple[pd.DataFrame, Dict[str, Any], List[Issue]]:
    """
    Emite: IMP.DATETIME.NAIVE_WITHOUT_SOURCE_TZ,
            IMP.DATETIME.NUMERIC_NOT_PARSED,
            opcionalmente IMP.TYPE.COERCE_PARTIAL
    """
    issues: List[Issue] = []
    work = df
    status_by_field: Dict[str, Any] = {}

    # Si no hay campos temporales, no hay nada que hacer
    if temporal_tier != "tier_1":
        return work, status_by_field, issues

    for field_name, fs in schema.fields.items():
        effective_dtype = schema_effective.dtype_effective.get(field_name, fs.dtype)
        # Solo se trabaja con campos datetimes del schema
        if effective_dtype != "datetime" or field_name not in work.columns:
            continue

        out, info = _normalize_datetime_column(work[field_name], source_timezone=options.source_timezone)
        work[field_name] = out
        status_by_field[field_name] = info

        if info["status"] in {"naive_unconverted", "string_naive_unconverted"}:
            emit_issue(
                issues,
                IMPORT_ISSUES,
                "IMP.DATETIME.NAIVE_WITHOUT_SOURCE_TZ",
                field=field_name,
            )
        elif info["status"] == "not_parsed_numeric":
            emit_issue(
                issues,
                IMPORT_ISSUES,
                "IMP.DATETIME.NUMERIC_NOT_PARSED",
                field=field_name,
            )

        status = info.get("status")
        n_nat = int(info.get("n_nat", 0))

        if 0 < n_nat <= len(work) and info["status"].startswith("string"):
            emit_issue(
                issues,
                IMPORT_ISSUES,
                "IMP.TYPE.COERCE_PARTIAL",
                field=field_name,
                dtype_expected="datetime",
                parse_fail_count=n_nat,
                total_count=len(work),
                fail_rate=(n_nat / len(work)) if len(work) else 0.0,
                row_count=n_nat,
            )

    return work, status_by_field, issues

def _normalize_tier2_hhmm_columns(
    work: pd.DataFrame,
    *,
    temporal_tier: str,
    schema: TripSchema,
) -> tuple[pd.DataFrame, dict[str, dict[str, int]], list[Issue]]:
    """
    Normaliza columnas HH:MM cuando el dataset fue detectado como tier_2.

    Emite:
        - IMP.TYPE.COERCE_PARTIAL (si hubo valores inválidos convertidos a NA)
    """
    local_issues: list[Issue] = []

    if temporal_tier != "tier_2":
        return work, {}, local_issues

    hhmm_stats: dict[str, dict[str, int]] = {}

    for field in ("origin_time_local_hhmm", "destination_time_local_hhmm"):
        if field not in work.columns:
            continue

        normalized, stats = _normalize_hhmm_series(work[field])
        work[field] = normalized
        hhmm_stats[field] = stats

        if stats["n_invalid"] > 0:
            emit_issue(
                local_issues,
                IMPORT_ISSUES,
                "IMP.TYPE.COERCE_PARTIAL",
                field=field,
                dtype_expected="string_hhmm",
                parse_fail_count=stats["n_invalid"],
                total_count=stats["n_total"],
                fail_rate=(stats["n_invalid"] / stats["n_total"]) if stats["n_total"] else 0.0,
                fallback="set_null",
                action="continue",
            )

    return work, hhmm_stats, local_issues

def _parse_od_coordinate_columns(
    df: pd.DataFrame,
    *,
    schema: TripSchema,
    target_schema_fields: set[str],
    strict: bool,
) -> tuple[pd.DataFrame, Dict[str, Any], List[Issue]]:
    """
    Emite: IMP.TYPE.COERCE_PARTIAL,
            IMP.TYPE.COERCE_FAILED_REQUIRED
    """
    issues: List[Issue] = []
    work = df
    coord_stats: Dict[str, Any] = {}

    for field_name in [
        "origin_latitude",
        "origin_longitude",
        "destination_latitude",
        "destination_longitude",
    ]:
        if field_name not in work.columns or field_name not in target_schema_fields:
            continue

        original_series = work[field_name].copy()
        parsed = original_series.apply(_parse_coord_value)
        values = parsed.apply(lambda x: x[0]).astype(float)
        statuses = parsed.apply(lambda x: x[1]).astype("string")
        work[field_name] = values

        invalid_mask = statuses.eq("unparsed")
        parse_fail_count = int(invalid_mask.sum())
        total_count = len(work)
        coord_stats[field_name] = {
            "parse_fail_count": parse_fail_count,
            "total_count": total_count,
            "status_counts": statuses.value_counts(dropna=False).to_dict(),
        }

        if parse_fail_count <= 0:
            continue

        fail_rate = (parse_fail_count / total_count) if total_count else 0.0
        examples = original_series.loc[invalid_mask].head(5).astype("string").tolist()
        fs = schema.fields.get(field_name)

        if fs is not None and fs.required and values.notna().sum() == 0 and total_count > 0:
            emit_and_maybe_raise(
                issues,
                IMPORT_ISSUES,
                "IMP.TYPE.COERCE_FAILED_REQUIRED",
                strict=strict,
                exception_map=EXCEPTION_MAP_IMPORT,
                default_exception=PylondrinaImportError,
                field=field_name,
                dtype_expected="float",
                parse_fail_count=parse_fail_count,
                rows_in=total_count,
                fail_rate=fail_rate,
                examples_sample=examples,
                row_count=parse_fail_count,
            )
        else:
            emit_issue(
                issues,
                IMPORT_ISSUES,
                "IMP.TYPE.COERCE_PARTIAL",
                field=field_name,
                dtype_expected="float",
                parse_fail_count=parse_fail_count,
                total_count=total_count,
                fail_rate=fail_rate,
                row_count=parse_fail_count,
            )
    return work, coord_stats, issues

def _derive_h3_indices(
    df: pd.DataFrame,
    *,
    schema: TripSchema,
    h3_resolution: int,
    strict: bool,
) -> tuple[pd.DataFrame, Dict[str, Any], List[str], List[Issue]]:
    """
    Emite: IMP.H3.REQUIRED_FIELDS_UNAVAILABLE,
            IMP.H3.PARTIAL_DERIVATION
    """
    issues: List[Issue] = []
    work = df
    df_columns = df.columns
    columns_added: List[str] = []
    h3_meta: Dict[str, Any] = {
        "resolution": h3_resolution,
        "source_fields": [],
        "derived_fields": [],
    }

    required_h3_fields = [f for f in ["origin_h3_index", "destination_h3_index"] if f in schema.required]
    missing_pairs = []
    origin_pair = {"origin_latitude", "origin_longitude"}
    destination_pair = {"destination_latitude", "destination_longitude"}

    if ("origin_h3_index" in schema.fields or "origin_h3_index" in schema.required) and not origin_pair.issubset(work.columns):
        if "origin_h3_index" in required_h3_fields:
            missing_pairs.append(["origin_latitude", "origin_longitude"])
    if ("destination_h3_index" in schema.fields or "destination_h3_index" in schema.required) and not destination_pair.issubset(work.columns):
        if "destination_h3_index" in required_h3_fields:
            missing_pairs.append(["destination_latitude", "destination_longitude"])

    if missing_pairs:
        emit_and_maybe_raise(
            issues,
            IMPORT_ISSUES,
            "IMP.H3.REQUIRED_FIELDS_UNAVAILABLE",
            strict=strict,
            exception_map=EXCEPTION_MAP_IMPORT,
            default_exception=PylondrinaImportError,
            missing_pairs=missing_pairs,
            required_h3_fields=required_h3_fields,
        )

    if h3 is None:
        if required_h3_fields:
            emit_and_maybe_raise(
                issues,
                IMPORT_ISSUES,
                "IMP.H3.REQUIRED_FIELDS_UNAVAILABLE",
                strict=strict,
                exception_map=EXCEPTION_MAP_IMPORT,
                default_exception=PylondrinaImportError,
                missing_pairs=[["h3_python_package_missing"]],
                required_h3_fields=required_h3_fields,
            )
        return work, {}, columns_added, issues

    def _derive_pair(lat_col: str, lon_col: str, out_col: str) -> None:
        nonlocal work, columns_added, h3_meta, issues
        out_values: List[Any] = []
        null_count = 0
        for lat, lon in zip(work[lat_col], work[lon_col]):
            if pd.isna(lat) or pd.isna(lon):
                out_values.append(pd.NA)
                null_count += 1
                continue
            try:
                out_values.append(h3.latlng_to_cell(float(lat), float(lon), h3_resolution))
            except Exception:
                out_values.append(pd.NA)
                null_count += 1
        work[out_col] = pd.Series(out_values, index=work.index, dtype="string")
        if out_col not in columns_added and out_col not in df_columns:
            columns_added.append(out_col)
        h3_meta["source_fields"].append([lat_col, lon_col])
        h3_meta["derived_fields"].append(out_col)
        if null_count > 0:
            emit_issue(
                issues,
                IMPORT_ISSUES,
                "IMP.H3.PARTIAL_DERIVATION",
                derived_fields=[out_col],
                null_count=null_count,
                rows_in=len(work),
                row_count=null_count,
            )

    if origin_pair.issubset(work.columns):
        _derive_pair("origin_latitude", "origin_longitude", "origin_h3_index")
    if destination_pair.issubset(work.columns):
        _derive_pair("destination_latitude", "destination_longitude", "destination_h3_index")

    return work, (h3_meta if h3_meta["derived_fields"] else {}), columns_added, issues

def _ensure_movement_id(df: pd.DataFrame, *, strict: bool) -> tuple[pd.DataFrame, List[str], List[Issue]]:
    """
    Emite: IMP.ID.MOVEMENT_ID_DUPLICATE,
            IMP.ID.MOVEMENT_ID_CREATED
    """
    issues: List[Issue] = []
    work = df
    columns_added: List[str] = []

    if "movement_id" in work.columns:
        # Se detectan duplicados
        col = "movement_id"
        dup_mask = work[col].duplicated(keep=False)
        duplicated_rows = work.loc[dup_mask].copy()
        duplicated_ids = duplicated_rows[col].drop_duplicates().tolist()
        duplicated_counts = duplicated_rows[col].value_counts().to_dict()

        has_duplicates = bool(dup_mask.any())
        n_duplicated_rows = int(dup_mask.sum())

        if has_duplicates:
            emit_and_maybe_raise(
                issues,
                IMPORT_ISSUES,
                "IMP.ID.MOVEMENT_ID_DUPLICATE",
                strict=strict,
                exception_map=EXCEPTION_MAP_IMPORT,
                default_exception=PylondrinaImportError,
                duplicate_count=n_duplicated_rows,
                duplicate_examples=duplicated_ids[:5],
                row_count=n_duplicated_rows,
            )
        return work, columns_added, issues

    movement_ids = pd.Series([f"m{i}" for i in range(len(work))], index=work.index, dtype="string")
    work.insert(0, "movement_id", movement_ids)
    columns_added.append("movement_id")
    emit_issue(issues, IMPORT_ISSUES, "IMP.ID.MOVEMENT_ID_CREATED", field="movement_id")
    return work, columns_added, issues    

def _ensure_single_stage_ids(
    df: pd.DataFrame,
    *,
    schema: TripSchema,
    single_stage: bool,
    strict: bool,
) -> tuple[pd.DataFrame, List[str], List[Issue]]:
    """
    Emite: IMP.ID.TRIP_ID_CREATED,
            IMP.ID.MOVEMENT_SEQ_CREATED,
            IMP.INPUT.MISSING_REQUIRED_FIELD
    """
    issues: List[Issue] = []
    work = df.copy(deep=True)
    columns_added: List[str] = []

    if not single_stage:
        missing_if_required = [f for f in ["trip_id", "movement_seq"] if f in schema.required and f not in work.columns]
        if missing_if_required:
            emit_and_maybe_raise(
                issues,
                IMPORT_ISSUES,
                "IMP.INPUT.MISSING_REQUIRED_FIELD",
                strict=strict,
                exception_map=EXCEPTION_MAP_IMPORT,
                default_exception=PylondrinaImportError,
                missing_required=missing_if_required,
                required=list(schema.required),
                source_columns=list(work.columns),
                field_correspondence_keys=[],
                field_correspondence_values_sample=[],
            )
        return work, columns_added, issues

    # Ya deberia estar asegurado el campo movement_id
    if "movement_id" not in work.columns:
        emit_and_maybe_raise(
            issues,
            IMPORT_ISSUES,
            "IMP.INPUT.MISSING_REQUIRED_FIELD",
            strict=strict,
            exception_map=EXCEPTION_MAP_IMPORT,
            default_exception=PylondrinaImportError,
            missing_required=["movement_id"],
            required=list(schema.required),
            source_columns=list(work.columns),
            field_correspondence_keys=[],
            field_correspondence_values_sample=[],
        )

    if "trip_id" not in work.columns:
        insert_pos = 1 if len(work.columns) > 0 and work.columns[0] == "movement_id" else 0
        work.insert(insert_pos, "trip_id", pd.Series(work["movement_id"], index=work.index, dtype="string"))
        columns_added.append("trip_id")
        emit_issue(issues, IMPORT_ISSUES, "IMP.ID.TRIP_ID_CREATED", field="trip_id")

    if "movement_seq" not in work.columns:
        insert_pos = 2 if list(work.columns[:2]) == ["movement_id", "trip_id"] else len(work.columns)
        work.insert(insert_pos, "movement_seq", pd.Series([0] * len(work), index=work.index, dtype="Int64"))
        columns_added.append("movement_seq")
        emit_issue(issues, IMPORT_ISSUES, "IMP.ID.MOVEMENT_SEQ_CREATED", field="movement_seq")

    return work, columns_added, issues

def _select_final_columns(
    df: pd.DataFrame,
    *,
    schema: TripSchema,
    options: ImportOptions,
) -> tuple[pd.DataFrame, List[str], List[str], List[Issue]]:
    """
    Emite: IMP.OPTIONS.EXTRA_FIELDS_DROPPED
    """
    issues: List[Issue] = []
    cols_before = list(df.columns)
    res = _build_keep_schema_fields(schema, options.selected_fields)
    keep_schema_fields = set(res["keep_schema_fields"])
    schema_fields = set(schema.fields.keys())

    # Campos mínimos derivados que no deben perderse si fueron creados/asegurados.
    mandatory_runtime_fields = {"movement_id"}
    if options.single_stage:
        mandatory_runtime_fields |= {"trip_id", "movement_seq"}

    if options.keep_extra_fields:
        final_cols = [
            c for c in df.columns
            if (c in keep_schema_fields)
            or (c in mandatory_runtime_fields)
            or (c not in schema_fields)
        ]
    else:
        final_cols = [
            c for c in df.columns
            if (c in keep_schema_fields) or (c in mandatory_runtime_fields)
        ]

    columns_deleted = [c for c in df.columns if c not in final_cols]
    if columns_deleted:
        df.drop(columns=columns_deleted, inplace=True)
    extra_fields_kept = [c for c in df.columns if c not in schema_fields]    

    dropped_extras = [c for c in columns_deleted if c not in schema_fields]
    if dropped_extras:
        emit_issue(
            issues,
            IMPORT_ISSUES,
            "IMP.OPTIONS.EXTRA_FIELDS_DROPPED",
            keep_extra_fields=options.keep_extra_fields,
            n_dropped=len(dropped_extras),
            dropped_columns_sample=dropped_extras[:5],
            dropped_columns_total=dropped_extras,
            row_count=len(dropped_extras),
        )

    return df, columns_deleted, extra_fields_kept, issues

def _final_required_check(
    df: pd.DataFrame,
    *,
    schema: TripSchema,
    single_stage: bool,
    strict: bool,
) -> None:
    # required del schema
    missing_required_final = sorted(set(schema.required) - set(df.columns))

    # campos minimos de construibilidad del import
    extra_missing: List[str] = []
    if "movement_id" not in df.columns:
        extra_missing.append("movement_id")
    if single_stage:
        for field_name in ["trip_id", "movement_seq"]:
            if field_name not in df.columns:
                extra_missing.append(field_name)
    missing_total = sorted(set(missing_required_final + extra_missing))
    if missing_total:
        issues: List[Issue] = []
        emit_and_maybe_raise(
            issues,
            IMPORT_ISSUES,
            "IMP.INPUT.MISSING_REQUIRED_FIELD",
            strict=strict,
            exception_map=EXCEPTION_MAP_IMPORT,
            default_exception=PylondrinaImportError,
            missing_required=missing_total,
            required=list(schema.required),
            source_columns=list(df.columns),
            field_correspondence_keys=[],
            field_correspondence_values_sample=[],
        )

def _prune_schema_effective(
    schema_effective: TripSchemaEffective,
    *,
    df: pd.DataFrame,
    schema: TripSchema,
) -> TripSchemaEffective:
    final_schema_fields = [c for c in df.columns if c in schema.fields]
    schema_effective.fields_effective = list(final_schema_fields)
    schema_effective.dtype_effective = {
        k: v for k, v in schema_effective.dtype_effective.items() if k in final_schema_fields
    }
    schema_effective.overrides = {
        k: v for k, v in schema_effective.overrides.items() if k in final_schema_fields
    }
    schema_effective.domains_effective = {
        k: v for k, v in schema_effective.domains_effective.items() if k in final_schema_fields
    }
    return schema_effective
 
def _build_import_metadata(
    *,
    schema: TripSchema,
    schema_effective: TripSchemaEffective,
    field_correspondence_applied: Dict[str, str],
    value_correspondence_applied: Dict[str, Dict[str, str]],
    domains_effective: Dict[str, Any],
    domains_extended: List[str],
    extra_fields_kept: List[str],
    h3_meta: Optional[Dict[str, Any]],
    provenance: Dict[str, Any],
    temporal_tier_detected: str,
    temporal_fields_present: List[str],
    datetime_normalization_status_by_field: Dict[str, Any],
    datetime_normalization_stats_t2: Optional[Dict],
    source_timezone_used: Optional[str],
    event_import: Dict[str, Any],
    issues: List[Issue],
    strict: bool,
) -> Dict[str, Any]:
    """
    Emite: PRV.INPUT.NOT_JSON_SERIALIZABLE,
            IMP.METADATA.DATASET_ID_CREATED
    """
    local_issues: List[Issue] = []
    if not _json_is_serializable(provenance):
        emit_and_maybe_raise(
            local_issues,
            IMPORT_ISSUES,
            "PRV.INPUT.NOT_JSON_SERIALIZABLE",
            strict=strict,
            exception_map=EXCEPTION_MAP_IMPORT,
            default_exception=PylondrinaImportError,
            type=type(provenance).__name__,
            reason="not_json_serializable",
            example_repr=repr(provenance)[:200],
            suggestion="Entregar un dict/list/str/int/bool/None serializable a JSON.",
        )
    issues.extend(local_issues)

    dataset_id = f"tripds_{uuid.uuid4().hex}"
    emit_issue(
        issues,
        IMPORT_ISSUES,
        "IMP.METADATA.DATASET_ID_CREATED",
        dataset_id=dataset_id,
        generator="uuid4_hex",
        stored_in="metadata.dataset_id",
    )

    metadata = {
        "dataset_id": dataset_id,
        "is_validated": False,
        "schema": schema.to_dict(),
        "schema_effective": schema_effective.to_dict(),
        "mappings": {
            "field_correspondence": field_correspondence_applied,
            "value_correspondence": value_correspondence_applied,
        },
        "domains_effective": domains_effective,
        "domains_extended": domains_extended,
        "extra_fields_kept": extra_fields_kept,
        "events": [event_import],
        "provenance": provenance,
        "temporal": {
            "tier": temporal_tier_detected,
            "fields_present": temporal_fields_present,
        },
    }

    if h3_meta:
        metadata["h3"] = h3_meta
    if temporal_tier_detected == "tier_1":
        metadata["temporal"]["normalization"] = datetime_normalization_status_by_field
    if temporal_tier_detected == "tier_2":
        metadata["temporal"]["normalization"] = datetime_normalization_stats_t2
    if source_timezone_used is not None:
        metadata["temporal"]["source_timezone_used"] = source_timezone_used
    return metadata


def _build_import_report(
    *,
    issues: List[Issue],
    field_correspondence_applied: Dict[str, str],
    value_correspondence_applied: Dict[str, Dict[str, str]],
    schema_version: str,
    source_name: Optional[str],
    dataset_id: Optional[str],
    parameters_effective: Dict[str, Any],
    rows_in: int,
    rows_out: int,
    n_fields_mapped: int,
    n_domain_mappings_applied: int,
    metadata: Dict[str, Any],
) -> ImportReport:
    summary = {
        "rows_in": rows_in,
        "rows_out": rows_out,
        "n_fields_mapped": n_fields_mapped,
        "n_domain_mappings_applied": n_domain_mappings_applied,
    }
    report_metadata = {
        "schema_version": schema_version,
        "dataset_id": dataset_id,
        "source_name": source_name,
        "parameters_effective": parameters_effective,
        "summary": summary,
        "metadata": metadata,
    }
    ok = not any(issue.level == "error" for issue in issues)
    return ImportReport(
        ok=ok,
        issues=list(issues),
        summary=summary,
        parameters=parameters_effective,
        field_correspondence=field_correspondence_applied,
        value_correspondence=value_correspondence_applied,
        schema_version=schema_version,
        metadata=report_metadata,
    )


# -----------------------------------------------------------------------------
# Helpers internos de uso general
# -----------------------------------------------------------------------------

def _normalize_source_timezone(tz: Optional[str]) -> tuple[Optional[str], str]:
    if tz is None:
        return None, "none"
    tz = str(tz).strip()
    if tz == "":
        return None, "empty"
    if tz.upper() in {"UTC", "Z"}:
        return "UTC", "utc"
    if OFFSET_RE.match(tz):
        return tz, "offset"
    try:
        ZoneInfo(tz)
        return tz, "iana"
    except Exception:
        return None, "invalid"
    
def _apply_value_correspondence(s: pd.Series, vc_map: Optional[Dict[str, str]]) -> tuple[pd.Series, Dict[str, str]]:
    if not vc_map:
        return s, {}
    used_pairs: Dict[str, str] = {}
    observed = set(s.dropna().astype(str).tolist())
    for src_value, canonical_value in vc_map.items():
        if src_value in observed:
            used_pairs[str(src_value)] = str(canonical_value)
    if not used_pairs:
        return s, {}
    return s.replace(used_pairs), used_pairs

def _get_unknown_token(domain: Optional[DomainSpec]) -> str:
    if domain is None:
        return DEFAULT_UNKNOWN
    values = list(domain.values or [])
    for candidate in (DEFAULT_UNKNOWN, "other", "OTHER", "Unknown"):
        if candidate in values:
            return candidate
    return DEFAULT_UNKNOWN

def _coerce_series_to_dtype(
    s: pd.Series,
    expected: str,
    *,
    parse_datetime: bool = False,
    empty_string_as_na: bool = True,
) -> tuple[pd.Series, Dict[str, Any]]:
    before_dtype = str(s.dtype)
    na_before = int(pd.isna(s).sum())

    if expected in {"string", "categorical"}:
        out = s.astype("string")
        out = out.str.strip()
        if empty_string_as_na:
            out = out.replace("", pd.NA)
    elif expected == "int":
        out = pd.to_numeric(s, errors="coerce").astype("Int64")
    elif expected == "float":
        out = pd.to_numeric(s, errors="coerce")
    elif expected == "bool":
        s2 = s.astype("string").str.strip().str.lower().replace("", pd.NA)
        out = pd.Series([pd.NA] * len(s2), index=s2.index, dtype="boolean")
        out[s2.isin(TRUE_SET)] = True
        out[s2.isin(FALSE_SET)] = False
    elif expected == "datetime":
        if ptypes.is_datetime64_any_dtype(s.dtype):
            out = s
        elif parse_datetime:
            tmp = s.astype("string").str.strip().replace("", pd.NA)
            try:
                out = pd.to_datetime(tmp, format="mixed", errors="coerce")
            except TypeError:  # compatibilidad con pandas más antiguo
                out = pd.to_datetime(tmp, errors="coerce")
        else:
            out = s
    else:
        out = s.astype("string")

    after_dtype = str(out.dtype)
    na_after = int(pd.isna(out).sum())
    return out, {
        "expected": expected,
        "dtype_before": before_dtype,
        "dtype_after": after_dtype,
        "na_before": na_before,
        "na_after": na_after,
        "na_delta": max(0, na_after - na_before),
        "already_correct": _is_already_correct_dtype(s, expected),
    }

def _is_already_correct_dtype(s: pd.Series, expected: str) -> bool:
    dt = s.dtype
    if expected == "string":
        return str(dt) == "string"
    if expected == "int":
        return str(dt) == "Int64"
    if expected == "float":
        return ptypes.is_float_dtype(dt)
    if expected == "bool":
        return str(dt) == "boolean"
    if expected == "datetime":
        return ptypes.is_datetime64_any_dtype(dt)
    if expected == "categorical":
        return str(dt) == "string"
    return False

def _localize_naive_datetime_series_to_utc(s: pd.Series, tz_norm: str) -> pd.Series:
    """
    Localiza datetimes naive a la zona de origen y los convierte a UTC.

    Se manejan explícitamente bordes de DST:
    - nonexistent: horarios locales que no existen por salto horario.
    - ambiguous: horarios locales repetidos por cambio horario inverso.
    """
    return (
        s.dt.tz_localize(
            tz_norm,
            nonexistent=DATETIME_LOCALIZE_NONEXISTENT,
            ambiguous=DATETIME_LOCALIZE_AMBIGUOUS,
        )
        .dt.tz_convert("UTC")
    )


def _localize_naive_timestamp_to_utc(ts: pd.Timestamp, tz_norm: str) -> pd.Timestamp:
    """
    Localiza un Timestamp naive a la zona de origen y lo convierte a UTC,
    usando la misma política DST que la normalización vectorizada.
    """
    return (
        ts.tz_localize(
            tz_norm,
            nonexistent=DATETIME_LOCALIZE_NONEXISTENT,
            ambiguous=DATETIME_LOCALIZE_AMBIGUOUS,
        )
        .tz_convert("UTC")
    )

def _normalize_datetime_column(s: pd.Series, *, source_timezone: Optional[str]) -> tuple[pd.Series, Dict[str, Any]]:
    tz_norm, tz_kind = _normalize_source_timezone(source_timezone)

    # Si los valores son numéricos, no pueden ser datetimes
    if ptypes.is_numeric_dtype(s.dtype):
        out = pd.Series([pd.NaT] * len(s), index=s.index, dtype="datetime64[ns]")
        return out, {"status": "not_parsed_numeric", "tz_kind": tz_kind, "n_nat": int(out.isna().sum())}

    # Si ya viene como dtype datetime
    if ptypes.is_datetime64_any_dtype(s.dtype):
        if isinstance(s.dtype, pd.DatetimeTZDtype):
            tzname = str(getattr(s.dtype, "tz", ""))
            if tzname.upper() == "UTC":
                return s, {"status": "utc", "tz_kind": tz_kind, "n_nat": int(s.isna().sum())}
            return s.dt.tz_convert("UTC"), {
                "status": "tzaware_to_utc",
                "tz_kind": tz_kind,
                "n_nat": int(s.isna().sum()),
            }

        if tz_norm is None:
            return s, {"status": "naive_unconverted", "tz_kind": tz_kind, "n_nat": int(s.isna().sum())}

        localized = _localize_naive_datetime_series_to_utc(s, tz_norm)
        return localized, {
            "status": "naive_localized_to_utc",
            "tz_kind": tz_kind,
            "n_nat": int(localized.isna().sum()),
        }

    # Tratamiento como string
    tmp = s.astype("string").str.strip().replace("", pd.NA)

    try:
        parsed = pd.to_datetime(tmp, format="mixed", errors="coerce")
    except TypeError:  # compatibilidad con pandas más antiguo
        parsed = pd.to_datetime(tmp, errors="coerce")

    # Caso 1: parseo directo a datetime tz-aware
    if isinstance(parsed.dtype, pd.DatetimeTZDtype):
        out = parsed.dt.tz_convert("UTC")
        return out, {
            "status": "string_tzaware_to_utc",
            "tz_kind": tz_kind,
            "n_nat": int(out.isna().sum()),
        }

    # Caso 2: parseo directo a datetime naive
    if ptypes.is_datetime64_any_dtype(parsed.dtype):
        if tz_norm is None:
            return parsed, {
                "status": "string_naive_unconverted",
                "tz_kind": tz_kind,
                "n_nat": int(parsed.isna().sum()),
            }

        localized = _localize_naive_datetime_series_to_utc(parsed, tz_norm)
        return localized, {
            "status": "string_naive_localized_to_utc",
            "tz_kind": tz_kind,
            "n_nat": int(localized.isna().sum()),
        }

    # Caso 3: pandas devolvió object por mezcla de timestamps aware/naive parseables
    if parsed.dtype == "object":
        out_values = []
        parsed_ok = 0

        for val in parsed.tolist():
            if pd.isna(val):
                out_values.append(pd.NaT)
                continue

            if isinstance(val, (pd.Timestamp, datetime)):
                ts = pd.Timestamp(val)
                parsed_ok += 1

                if ts.tzinfo is not None:
                    out_values.append(ts.tz_convert("UTC"))
                else:
                    if tz_norm is None:
                        # Se conserva naive si no se declaró source_timezone
                        out_values.append(ts)
                    else:
                        out_values.append(_localize_naive_timestamp_to_utc(ts, tz_norm))
                continue

            # Cualquier otro caso raro se marca como NaT
            out_values.append(pd.NaT)

        # Intentamos usar un dtype consistente
        if tz_norm is not None or any(
            isinstance(v, pd.Timestamp) and v.tzinfo is not None
            for v in out_values if not pd.isna(v)
        ):
            try:
                out = pd.Series(out_values, index=s.index, dtype="datetime64[ns, UTC]")
            except Exception:
                out = pd.Series(out_values, index=s.index, dtype="object")
        else:
            try:
                out = pd.Series(out_values, index=s.index, dtype="datetime64[ns]")
            except Exception:
                out = pd.Series(out_values, index=s.index, dtype="object")

        status = "string_mixed_parsed"
        if tz_norm is not None:
            status = "string_mixed_localized_to_utc"

        return out, {
            "status": status,
            "tz_kind": tz_kind,
            "n_nat": int(pd.isna(out).sum()),
            "n_parsed": parsed_ok,
        }

    # No se pudo parsear nada útil
    out = pd.Series([pd.NaT] * len(s), index=s.index, dtype="datetime64[ns]")
    return out, {"status": "parse_failed", "tz_kind": tz_kind, "n_nat": int(out.isna().sum())}


def _normalize_hhmm_series(s: pd.Series) -> tuple[pd.Series, dict[str, int]]:
    """
    Normaliza una serie HH:MM:
    - convierte a StringDtype
    - hace strip
    - reemplaza vacíos por NA
    - valida formato HH:MM y rango 00:00..23:59
    - convierte inválidos a NA
    """
    s = s.astype("string").str.strip().replace("", pd.NA)

    out = []
    valid_flags = []

    for v in s.tolist():
        if pd.isna(v):
            out.append(pd.NA)
            valid_flags.append(True)
            continue

        m = _HHMM_RE.match(v)
        if not m:
            out.append(pd.NA)
            valid_flags.append(False)
            continue

        h = int(m.group("h"))
        mm = int(m.group("m"))

        if not (0 <= h <= 23 and 0 <= mm <= 59):
            out.append(pd.NA)
            valid_flags.append(False)
            continue

        out.append(v)
        valid_flags.append(True)

    out_s = pd.Series(out, index=s.index, dtype="string")
    stats = {
        "n_total": len(s),
        "n_invalid": valid_flags.count(False),
        "n_na": int(out_s.isna().sum()),
    }
    return out_s, stats

def _dm_to_dd(deg: Any, minutes: Any, hem: str) -> float:
    dd = float(deg) + float(minutes) / 60.0
    if hem.upper() in {"S", "W"}:
        dd = -dd
    return dd


def _dms_to_dd(deg: Any, minutes: Any, seconds: Any, hem: str) -> float:
    dd = float(deg) + float(minutes) / 60.0 + float(seconds) / 3600.0
    if hem.upper() in {"S", "W"}:
        dd = -dd
    return dd


def _parse_coord_value(v: Any) -> tuple[float, str]:
    if pd.isna(v):
        return np.nan, "null"
    if isinstance(v, (int, float, np.integer, np.floating)):
        return float(v), "numeric"

    s = str(v).strip()
    if s == "":
        return np.nan, "empty"

    try:
        return float(s), "dd_direct"
    except ValueError:
        pass

    try:
        return float(s.replace(",", ".")), "dd_comma_decimal"
    except ValueError:
        pass

    m = DM_PATTERN.match(s)
    if m:
        return _dm_to_dd(m.group("deg"), m.group("minutes"), m.group("hem")), "dm"

    m = DMS_PATTERN.match(s)
    if m:
        return _dms_to_dd(m.group("deg"), m.group("minutes"), m.group("seconds"), m.group("hem")), "dms"

    return np.nan, "unparsed"

def _build_keep_schema_fields(schema: TripSchema, selected_fields: Optional[Sequence[str]]) -> Dict[str, Any]:
    schema_fields = set(schema.fields.keys())
    required_fields = set(schema.required)
    if selected_fields is None:
        keep_schema_fields = set(schema_fields)
    elif len(selected_fields) == 0:
        keep_schema_fields = set(required_fields)
    else:
        selected_set = set(selected_fields)
        invalid_selected = sorted(selected_set - schema_fields)
        if invalid_selected:
            raise ValueError(f"selected_fields contiene campos fuera del schema: {invalid_selected}")
        keep_schema_fields = required_fields | (selected_set & schema_fields )
    return {
        "schema_fields": schema_fields,
        "required_fields": required_fields,
        "keep_schema_fields": keep_schema_fields,
    }

def _build_issues_summary(issues: Sequence[Issue]) -> Dict[str, Any]:
    level_counts = Counter()
    code_counts = Counter()
    for issue in issues:
        level_counts[issue.level] += 1
        code_counts[issue.code] += 1
    return {
        "counts": dict(level_counts),
        "by_code": dict(code_counts),
    }

def _json_is_serializable(obj: Any) -> bool:
    try:
        json.dumps(obj)
        return True
    except Exception:
        return False