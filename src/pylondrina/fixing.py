from __future__ import annotations

import copy
import json
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import pandas as pd

from pylondrina.datasets import TripDataset
from pylondrina.errors import FixError, PylondrinaError
from pylondrina.issues.catalog_fix_trips_cors import FIX_TRIPS_CORRESPONDENCE_ISSUES
from pylondrina.issues.core import emit_and_maybe_raise, emit_issue
from pylondrina.reports import Issue, OperationReport
from pylondrina.schema import TripSchemaEffective

EXCEPTION_MAP_FIX = {
    "fix": FixError,
}

FieldCorrections = Mapping[str, str]
"""
Mapeo de corrección de nombres de columnas.

- key: nombre actual de la columna en df
- value: nombre canónico objetivo
"""


ValueCorrections = Mapping[str, Mapping[Any, Any]]
"""
Mapeo de corrección de valores categóricos por campo.

Estructura:
- key: nombre de campo canónico
- value: dict que mapea valor_observado -> valor_canónico
"""

_ALLOWED_CONTEXT_KEYS = ("reason", "author", "source", "scope", "notes")


@dataclass(frozen=True)
class FixCorrespondenceOptions:
    """
    Opciones de control para la operación fix_trips_correspondence (API v1.1).

    Attributes
    ----------
    strict : bool, default=False
        Si True, Issues de nivel error pueden gatillar una excepción al finalizar la operación.
    max_issues : int, default=200
        Límite máximo de issues a registrar en el reporte.
    sample_rows_per_issue : int, default=50
        Tamaño máximo de muestra (filas/valores) a incluir en `Issue.details`.
    """

    strict: bool = False
    max_issues: int = 200
    sample_rows_per_issue: int = 50


def fix_trips_correspondence(
    trips: TripDataset,
    *,
    field_corrections: Optional[FieldCorrections] = None,
    value_corrections: Optional[ValueCorrections] = None,
    options: Optional[FixCorrespondenceOptions] = None,
    correspondence_context: Optional[Dict[str, Any]] = None,
) -> Tuple[TripDataset, OperationReport]:
    """
    Corrige correspondencias de un TripDataset Golondrina, soportando:
    (1) correspondencia de campos (renombrado de columnas) y
    (2) correspondencia de valores categóricos (recode por campo).

    Parameters
    ----------
    trips : TripDataset
        Dataset de viajes en formato Golondrina.
    field_corrections : FieldCorrections, optional
        Correcciones de nombres de columnas. Si es None, no se corrigen campos.
    value_corrections : ValueCorrections, optional
        Correcciones de valores categóricos por campo. Si es None, no se corrigen valores.
    options : FixCorrespondenceOptions, optional
        Opciones de ejecución (strict, límites de issues, muestreo de detalles).
    correspondence_context : dict, optional
        Metadatos adicionales para registrar en el evento de metadata.

    Returns
    -------
    fixed : TripDataset
        Nuevo TripDataset con correcciones aplicadas. Si hubo cambios efectivos,
        el dataset resultante queda marcado como no validado.
    report : OperationReport
        Reporte de la operación (issues + summary + parameters).
    """
    issues: List[Issue] = []
    options_eff, parameters_base = _normalize_fix_options(options)

    # ------------------------------------------------------------------
    # 1) Prechecks fatales y normalización del request
    # ------------------------------------------------------------------
    # Se asegura temprano que el input sea realmente un TripDataset usable.
    if not isinstance(trips, TripDataset):
        emit_and_maybe_raise(
            issues,
            FIX_TRIPS_CORRESPONDENCE_ISSUES,
            "FIX.INPUT.INVALID_TRIPS_OBJECT",
            strict=False,
            exception_map=EXCEPTION_MAP_FIX,
            default_exception=FixError,
            received_type=type(trips).__name__,
        )

    # Se asegura que la operación trabaje sobre trips.data y no sobre una superficie ambigua.
    if not hasattr(trips, "data") or not isinstance(trips.data, pd.DataFrame):
        emit_and_maybe_raise(
            issues,
            FIX_TRIPS_CORRESPONDENCE_ISSUES,
            "FIX.INPUT.MISSING_DATAFRAME",
            strict=False,
            exception_map=EXCEPTION_MAP_FIX,
            default_exception=FixError,
            reason="missing_or_not_dataframe",
        )

    # Se valida que field_corrections tenga forma interpretable antes de tocar el dataset.
    field_corrections_eff = _validate_field_corrections_or_abort(field_corrections)

    # Se valida que value_corrections tenga forma interpretable antes de tocar el dataset.
    value_corrections_eff = _validate_value_corrections_or_abort(value_corrections)

    # Se sanea el contexto para que el evento resultante quede completamente serializable.
    context_sanitized, context_issues = _sanitize_context_with_issues(
        correspondence_context,
        sample_rows_per_issue=options_eff.sample_rows_per_issue,
        n_rows_total=len(trips.data),
    )
    issues.extend(context_issues)

    # Se resuelven colisiones intrínsecamente ambiguas antes de iniciar el pipeline real.
    _check_ambiguous_field_targets_or_abort(field_corrections_eff)

    # ------------------------------------------------------------------
    # 2) Preparación del estado de trabajo transaccional
    # ------------------------------------------------------------------
    n_rows = len(trips.data)
    data_work: pd.DataFrame = trips.data
    metadata_work = _clone_metadata(trips.metadata)
    schema_effective_work = _clone_schema_effective(getattr(trips, "schema_effective", None))
    field_correspondence_final = _clone_field_correspondence(getattr(trips, "field_correspondence", None))
    value_correspondence_final = _clone_value_correspondence(getattr(trips, "value_correspondence", None))

    field_requested_count = len(field_corrections_eff)
    field_applied_count = 0
    value_requested_fields_count = len(value_corrections_eff)
    value_applied_fields_count = 0
    value_replacements_count = 0
    domains_effective_updated_fields: List[str] = []

    field_semantic_change = False
    value_semantic_change = False

    # ------------------------------------------------------------------
    # 3) Corrección de campos
    # ------------------------------------------------------------------
    applicable_field_corrections, field_correspondence_final, fields_effective_updated, field_issues, field_applied_count, field_semantic_change = _resolve_field_corrections(
        data_work,
        schema=getattr(trips, "schema", None),
        field_corrections=field_corrections_eff,
        field_correspondence_current=field_correspondence_final,
        sample_rows_per_issue=options_eff.sample_rows_per_issue,
        n_rows_total=n_rows,
    )
    issues.extend(field_issues)

    if applicable_field_corrections:
        # Se aplica el rename solo después de cerrar qué reglas son realmente válidas.
        data_work = apply_field_corrections(data_work, applicable_field_corrections, inplace=False)

    if field_semantic_change:
        schema_effective_work.fields_effective = fields_effective_updated

    # ------------------------------------------------------------------
    # 4) Corrección de valores
    # ------------------------------------------------------------------
    applicable_value_corrections, value_correspondence_final, value_issues, value_applied_fields_count, value_replacements_count, value_touched_fields, value_semantic_change = _resolve_value_corrections(
        data_work,
        schema=getattr(trips, "schema", None),
        value_corrections=value_corrections_eff,
        value_correspondence_current=value_correspondence_final,
        sample_rows_per_issue=options_eff.sample_rows_per_issue,
        n_rows_total=n_rows,
    )
    issues.extend(value_issues)

    if applicable_value_corrections:
        # Se aplica el recode solo después de cerrar qué campos/reglas son realmente utilizables.
        data_work = apply_value_corrections(data_work, applicable_value_corrections, inplace=False)

    # ------------------------------------------------------------------
    # 5) Consolidación del estado efectivo final
    # ------------------------------------------------------------------
    semantic_change_total = bool(field_semantic_change or value_semantic_change)
    noop = not semantic_change_total

    # Se consolida la trazabilidad efectiva final de mappings en metadata.
    metadata_work["mappings"] = {
        "field_correspondence": copy.deepcopy(field_correspondence_final),
        "value_correspondence": copy.deepcopy(value_correspondence_final),
    }

    if value_touched_fields:
        metadata_domains_updated, schema_domains_updated, domains_effective_updated_fields = _rebuild_domains_effective_for_fields(
            data_after_values=data_work,
            touched_fields=value_touched_fields,
            schema=getattr(trips, "schema", None),
            metadata_domains_effective=metadata_work.get("domains_effective"),
            schema_effective_domains=schema_effective_work.domains_effective,
        )
        metadata_work["domains_effective"] = metadata_domains_updated
        schema_effective_work.domains_effective = schema_domains_updated

        # Se deja evidencia informativa cuando sí hubo actualización de dominios efectivos.
        if domains_effective_updated_fields:
            emit_issue(
                issues,
                FIX_TRIPS_CORRESPONDENCE_ISSUES,
                "FIX.DOMAINS.UPDATED",
                updated_fields=domains_effective_updated_fields,
                added_values_by_field=_build_added_values_by_field(metadata_domains_updated, domains_effective_updated_fields),
                n_rows_total=n_rows,
            )

    # Se deja evidencia informativa resumida para las correcciones de campos realmente aplicadas.
    if field_applied_count > 0:
        emit_issue(
            issues,
            FIX_TRIPS_CORRESPONDENCE_ISSUES,
            "FIX.INFO.FIELD_CORRECTIONS_APPLIED",
            requested_count=field_requested_count,
            applied_count=field_applied_count,
            mapping_sample=_sample_mapping(applicable_field_corrections, limit=10),
            n_rows_total=n_rows,
        )

    # Se deja evidencia informativa resumida para las correcciones de valores realmente aplicadas.
    if value_applied_fields_count > 0:
        emit_issue(
            issues,
            FIX_TRIPS_CORRESPONDENCE_ISSUES,
            "FIX.INFO.VALUE_CORRECTIONS_APPLIED",
            requested_fields_count=value_requested_fields_count,
            applied_fields_count=value_applied_fields_count,
            replacements_count=value_replacements_count,
            mapping_sample=_sample_nested_mapping(applicable_value_corrections, field_limit=5, value_limit=5),
            n_rows_total=n_rows,
        )

    # Se registra el caso explícito sin correcciones entregadas como operación sin cambios efectivos.
    if field_requested_count == 0 and value_requested_fields_count == 0:
        emit_issue(
            issues,
            FIX_TRIPS_CORRESPONDENCE_ISSUES,
            "FIX.NO_EFFECTIVE_CHANGES.NO_CORRECTIONS",
            field_corrections_provided=False,
            value_corrections_provided=False,
            n_rows_total=n_rows,
        )
    elif noop:
        # Se registra el caso donde hubo request pero no se concretó ningún cambio efectivo.
        emit_issue(
            issues,
            FIX_TRIPS_CORRESPONDENCE_ISSUES,
            "FIX.NO_EFFECTIVE_CHANGES.NO_EFFECTIVE_CHANGES",
            field_corrections_provided=field_requested_count > 0,
            value_corrections_provided=value_requested_fields_count > 0,
            requested_field_rules=field_requested_count,
            requested_value_rules=sum(len(v) for v in value_corrections_eff.values()),
            n_rows_total=n_rows,
        )

    # Se alinea is_validated solo si hubo cambio semántico real.
    if semantic_change_total:
        metadata_work["is_validated"] = False
    else:
        metadata_work["is_validated"] = bool(_extract_validated_flag(trips.metadata))

    # Se mantiene el snapshot serializable de schema_effective alineado con el objeto resultante.
    metadata_work["schema_effective"] = schema_effective_work.to_dict()

    # ------------------------------------------------------------------
    # 6) Reporte, evento y política strict
    # ------------------------------------------------------------------
    issues_effective, limits_block = _apply_issue_truncation(
        issues,
        max_issues=options_eff.max_issues,
    )

    summary: Dict[str, Any] = {
        "n_rows": n_rows,
        "n_field_corrections_requested": field_requested_count,
        "n_field_corrections_applied": field_applied_count,
        "n_value_corrections_fields_requested": value_requested_fields_count,
        "n_value_corrections_fields_applied": value_applied_fields_count,
        "n_value_replacements_applied": value_replacements_count,
        "domains_effective_updated_fields": domains_effective_updated_fields,
        "noop": noop,
    }
    if limits_block is not None:
        summary["limits"] = limits_block

    parameters = {
        "field_corrections": _to_json_serializable_or_none(field_corrections_eff) if field_requested_count > 0 else None,
        "value_corrections": _to_json_serializable_or_none(value_corrections_eff) if value_requested_fields_count > 0 else None,
        **parameters_base,
    }

    ok = not any(issue.level == "error" for issue in issues_effective)
    report = OperationReport(
        ok=ok,
        issues=issues_effective,
        summary=summary,
        parameters=parameters,
    )

    event = {
        "op": "fix_trips_correspondence",
        "ts_utc": _utc_now_iso(),
        "parameters": parameters,
        "summary": summary,
        "issues_summary": _build_issues_summary(issues_effective),
        "context": context_sanitized,
    }

    if not isinstance(metadata_work.get("events"), list):
        metadata_work["events"] = []
    metadata_work["events"].append(event)

    # ------------------------------------------------------------------
    # 7) Commit final y strict
    # ------------------------------------------------------------------
    if data_work is trips.data:
        # Se evita aliasing del dataframe cuando la operación terminó sin cambios efectivos.
        data_out = trips.data.copy(deep=True)
    else:
        data_out = data_work

    fixed = TripDataset(
        data=data_out,
        schema=trips.schema,
        schema_version=trips.schema_version,
        provenance=copy.deepcopy(trips.provenance),
        field_correspondence=field_correspondence_final,
        value_correspondence=value_correspondence_final,
        metadata=metadata_work,
        schema_effective=schema_effective_work,
    )

    if options_eff.strict and not ok:
        error_issue = next((issue for issue in issues_effective if issue.level == "error"), None)
        raise FixError(
            "fix_trips_correspondence detectó errores de datos y strict=True exige abortar.",
            code=error_issue.code if error_issue is not None else None,
            details=error_issue.details if error_issue is not None else None,
            issue=error_issue,
            issues=issues_effective,
        )

    return fixed, report


# -----------------------------------------------------------------------------
# Helpers internos principales del pipeline de la operación
# -----------------------------------------------------------------------------


def _normalize_fix_options(
    options: Optional[FixCorrespondenceOptions],
) -> Tuple[FixCorrespondenceOptions, Dict[str, Any]]:
    """Normaliza opciones efectivas de OP-03."""
    options_eff = options or FixCorrespondenceOptions()
    max_issues = int(options_eff.max_issues)
    sample_rows_per_issue = int(options_eff.sample_rows_per_issue)

    if max_issues <= 0:
        max_issues = 1
    if sample_rows_per_issue <= 0:
        sample_rows_per_issue = 1

    normalized = FixCorrespondenceOptions(
        strict=bool(options_eff.strict),
        max_issues=max_issues,
        sample_rows_per_issue=sample_rows_per_issue,
    )
    parameters = {
        "strict": normalized.strict,
        "max_issues": normalized.max_issues,
        "sample_rows_per_issue": normalized.sample_rows_per_issue,
    }
    return normalized, parameters


def _validate_field_corrections_or_abort(
    field_corrections: Optional[FieldCorrections],
) -> Dict[str, str]:
    """
    Valida la estructura de `field_corrections` antes del pipeline.

    Emite
    -----
    - FIX.CORRECTIONS.INVALID_FIELD_STRUCTURE
    - FIX.CORRECTIONS.INVALID_RULE_STRUCTURE
    """
    issues: List[Issue] = []
    if field_corrections is None:
        return {}

    if not isinstance(field_corrections, Mapping):
        # Se aborta porque sin un Mapping interpretable no existe contrato ejecutable de corrección.
        emit_and_maybe_raise(
            issues,
            FIX_TRIPS_CORRESPONDENCE_ISSUES,
            "FIX.CORRECTIONS.INVALID_FIELD_STRUCTURE",
            strict=False,
            exception_map=EXCEPTION_MAP_FIX,
            default_exception=FixError,
            received_type=type(field_corrections).__name__,
        )

    normalized: Dict[str, str] = {}
    for input_key, rule_value in field_corrections.items():
        if not isinstance(input_key, str) or not isinstance(rule_value, str):
            # Se aborta porque cada regla debe poder interpretarse como source_column -> target_column.
            emit_and_maybe_raise(
                issues,
                FIX_TRIPS_CORRESPONDENCE_ISSUES,
                "FIX.CORRECTIONS.INVALID_RULE_STRUCTURE",
                strict=False,
                exception_map=EXCEPTION_MAP_FIX,
                default_exception=FixError,
                kind="field_corrections",
                input_key=input_key,
                rule_value=rule_value,
                reason="expected_str_to_str_mapping",
            )
        normalized[input_key] = rule_value
    return normalized


def _validate_value_corrections_or_abort(
    value_corrections: Optional[ValueCorrections],
) -> Dict[str, Dict[Any, Any]]:
    """
    Valida la estructura de `value_corrections` antes del pipeline.

    Emite
    -----
    - FIX.CORRECTIONS.INVALID_VALUE_STRUCTURE
    - FIX.CORRECTIONS.INVALID_RULE_STRUCTURE
    """
    issues: List[Issue] = []
    if value_corrections is None:
        return {}

    if not isinstance(value_corrections, Mapping):
        # Se aborta porque sin un Mapping interpretable no existe contrato ejecutable de recodificación.
        emit_and_maybe_raise(
            issues,
            FIX_TRIPS_CORRESPONDENCE_ISSUES,
            "FIX.CORRECTIONS.INVALID_VALUE_STRUCTURE",
            strict=False,
            exception_map=EXCEPTION_MAP_FIX,
            default_exception=FixError,
            received_type=type(value_corrections).__name__,
        )

    normalized: Dict[str, Dict[Any, Any]] = {}
    for input_key, rule_value in value_corrections.items():
        if not isinstance(input_key, str) or not isinstance(rule_value, Mapping):
            # Se aborta porque cada bloque debe poder interpretarse como field -> mapping origen/canónico.
            emit_and_maybe_raise(
                issues,
                FIX_TRIPS_CORRESPONDENCE_ISSUES,
                "FIX.CORRECTIONS.INVALID_RULE_STRUCTURE",
                strict=False,
                exception_map=EXCEPTION_MAP_FIX,
                default_exception=FixError,
                kind="value_corrections",
                input_key=input_key,
                rule_value=rule_value,
                reason="expected_field_to_mapping",
            )

        inner_normalized: Dict[Any, Any] = {}
        for source_value, target_value in rule_value.items():
            if not _json_is_serializable(source_value) or not _json_is_serializable(target_value):
                # Se aborta porque los parámetros del evento/report deben quedar JSON-safe.
                emit_and_maybe_raise(
                    issues,
                    FIX_TRIPS_CORRESPONDENCE_ISSUES,
                    "FIX.CORRECTIONS.INVALID_RULE_STRUCTURE",
                    strict=False,
                    exception_map=EXCEPTION_MAP_FIX,
                    default_exception=FixError,
                    kind="value_corrections",
                    input_key=input_key,
                    rule_value={source_value: target_value},
                    reason="non_json_serializable_value_rule",
                )
            inner_normalized[source_value] = target_value
        normalized[input_key] = inner_normalized
    return normalized


def _sanitize_context_with_issues(
    correspondence_context: Optional[Dict[str, Any]],
    *,
    sample_rows_per_issue: int,
    n_rows_total: int,
) -> Tuple[Optional[Dict[str, Any]], List[Issue]]:
    """
    Sanea `correspondence_context` y emite warnings degradables.

    Emite
    -----
    - FIX.CONTEXT.INVALID_ROOT
    - FIX.CONTEXT.UNKNOWN_KEYS_DROPPED
    - FIX.CONTEXT.NON_SERIALIZABLE_DROPPED
    """
    issues: List[Issue] = []
    try:
        context_sanitized, unknown_keys, dropped_paths = sanitize_correspondence_context(correspondence_context)
    except TypeError:
        # Se aborta porque un root no-dict vuelve no interpretable el contexto contractual del evento.
        emit_and_maybe_raise(
            issues,
            FIX_TRIPS_CORRESPONDENCE_ISSUES,
            "FIX.CONTEXT.INVALID_ROOT",
            strict=False,
            exception_map=EXCEPTION_MAP_FIX,
            default_exception=FixError,
            received_type=type(correspondence_context).__name__,
        )
        raise AssertionError("unreachable")

    if unknown_keys:
        # Se avisa porque esas keys no pueden preservarse dentro del whitelist top-level vigente.
        emit_issue(
            issues,
            FIX_TRIPS_CORRESPONDENCE_ISSUES,
            "FIX.CONTEXT.UNKNOWN_KEYS_DROPPED",
            unknown_keys=_sample_list(unknown_keys, sample_rows_per_issue),
            allowed_keys=["reason", "author", "source", "scope", "notes"],
            sample_rows_per_issue=sample_rows_per_issue,
            n_rows_total=n_rows_total,
        )

    if dropped_paths:
        # Se avisa porque algunos fragmentos del contexto no eran serializables y debieron descartarse.
        emit_issue(
            issues,
            FIX_TRIPS_CORRESPONDENCE_ISSUES,
            "FIX.CONTEXT.NON_SERIALIZABLE_DROPPED",
            dropped_paths=_sample_list(dropped_paths, sample_rows_per_issue),
            sample_rows_per_issue=sample_rows_per_issue,
            n_rows_total=n_rows_total,
        )

    return context_sanitized, issues


def _check_ambiguous_field_targets_or_abort(field_corrections: Mapping[str, str]) -> None:
    """
    Verifica colisiones fatales source->same_target imposibles de resolver con seguridad.

    Emite
    -----
    - FIX.FIELD.AMBIGUOUS_MULTI_SOURCE_TO_SAME_TARGET
    """
    issues: List[Issue] = []
    inverse: Dict[str, List[str]] = {}
    for source_column, target_column in field_corrections.items():
        inverse.setdefault(target_column, []).append(source_column)

    for target_column, source_columns in inverse.items():
        if len(source_columns) <= 1:
            continue
        # Se aborta porque varias columnas compiten por el mismo target canónico y el resultado sería ambiguo.
        emit_and_maybe_raise(
            issues,
            FIX_TRIPS_CORRESPONDENCE_ISSUES,
            "FIX.FIELD.AMBIGUOUS_MULTI_SOURCE_TO_SAME_TARGET",
            strict=False,
            exception_map=EXCEPTION_MAP_FIX,
            default_exception=FixError,
            target_column=target_column,
            source_columns=sorted(source_columns),
        )


def _resolve_field_corrections(
    df: pd.DataFrame,
    *,
    schema: Any,
    field_corrections: Mapping[str, str],
    field_correspondence_current: Dict[str, str],
    sample_rows_per_issue: int,
    n_rows_total: int,
) -> Tuple[Dict[str, str], Dict[str, str], List[str], List[Issue], int, bool]:
    """
    Resuelve qué field_corrections pueden aplicarse y actualiza la vista efectiva final.

    Emite
    -----
    - FIX.FIELD.SOURCE_COLUMN_MISSING
    - FIX.FIELD.TARGET_NOT_IN_SCHEMA
    - FIX.FIELD.TARGET_ALREADY_EXISTS
    - FIX.FIELD.RULE_NOT_ALLOWED
    - FIX.FIELD.PARTIAL_APPLY
    """
    issues: List[Issue] = []
    if not field_corrections:
        return {}, field_correspondence_current, list(getattr(schema, "fields", {}).keys()), issues, 0, False

    schema_fields = set(getattr(schema, "fields", {}).keys())
    existing_columns = list(df.columns)
    existing_columns_set = set(existing_columns)

    applicable: Dict[str, str] = {}
    applied_count = 0
    omitted_count = 0

    for source_column, target_column in field_corrections.items():
        if source_column not in existing_columns_set:
            omitted_count += 1
            # Se avisa porque la regla apunta a una columna que ya no existe en la superficie real del dataset.
            emit_issue(
                issues,
                FIX_TRIPS_CORRESPONDENCE_ISSUES,
                "FIX.FIELD.SOURCE_COLUMN_MISSING",
                source_column=source_column,
                target_column=target_column,
                available_columns_sample=_sample_list(existing_columns, 15),
                available_columns_total=len(existing_columns),
                sample_rows_per_issue=sample_rows_per_issue,
                n_rows_total=n_rows_total,
                field=target_column if isinstance(target_column, str) else None,
                source_field=source_column,
            )
            continue

        if target_column not in schema_fields:
            omitted_count += 1
            # Se avisa porque el target no pertenece al contrato base del schema.
            emit_issue(
                issues,
                FIX_TRIPS_CORRESPONDENCE_ISSUES,
                "FIX.FIELD.TARGET_NOT_IN_SCHEMA",
                source_column=source_column,
                target_column=target_column,
                allowed_targets_sample=_sample_list(sorted(schema_fields), 15),
                allowed_targets_total=len(schema_fields),
                reason="target_not_in_trip_schema",
                sample_rows_per_issue=sample_rows_per_issue,
                n_rows_total=n_rows_total,
                field=target_column,
                source_field=source_column,
            )
            continue

        if source_column in schema_fields:
            omitted_count += 1
            # Se marca error porque OP-03 no permite renombres entre canónicos ni sobre canónicos ya instalados.
            emit_issue(
                issues,
                FIX_TRIPS_CORRESPONDENCE_ISSUES,
                "FIX.FIELD.RULE_NOT_ALLOWED",
                source_column=source_column,
                target_column=target_column,
                reason="canonical_to_canonical_not_allowed",
                sample_rows_per_issue=sample_rows_per_issue,
                n_rows_total=n_rows_total,
                field=target_column,
                source_field=source_column,
            )
            continue

        if target_column in existing_columns_set and target_column != source_column:
            omitted_count += 1
            # Se marca error porque el target ya existe y la regla implicaría sobrescribir una columna canónica presente.
            emit_issue(
                issues,
                FIX_TRIPS_CORRESPONDENCE_ISSUES,
                "FIX.FIELD.TARGET_ALREADY_EXISTS",
                source_column=source_column,
                target_column=target_column,
                reason="target_already_exists_in_dataset",
                sample_rows_per_issue=sample_rows_per_issue,
                n_rows_total=n_rows_total,
                field=target_column,
                source_field=source_column,
            )
            continue

        applicable[source_column] = target_column
        applied_count += 1

        # Se actualiza la vista final canónica -> origen usando la mejor referencia disponible del source.
        source_origin = field_correspondence_current.get(source_column, source_column)
        field_correspondence_current[target_column] = source_origin
        field_correspondence_current.pop(source_column, None)

    semantic_change = applied_count > 0
    resulting_columns = [applicable.get(col, col) for col in existing_columns]
    fields_effective_updated = [name for name in schema_fields if name in resulting_columns]

    if omitted_count > 0 and applied_count > 0:
        # Se resume la aplicación parcial para que el reporte no dependa solo de issues atómicos.
        emit_issue(
            issues,
            FIX_TRIPS_CORRESPONDENCE_ISSUES,
            "FIX.FIELD.PARTIAL_APPLY",
            requested_count=len(field_corrections),
            applied_count=applied_count,
            omitted_count=omitted_count,
            mapping_sample=_sample_mapping(field_corrections, limit=10),
            sample_rows_per_issue=sample_rows_per_issue,
            n_rows_total=n_rows_total,
        )

    return applicable, field_correspondence_current, fields_effective_updated, issues, applied_count, semantic_change


def _resolve_value_corrections(
    df: pd.DataFrame,
    *,
    schema: Any,
    value_corrections: Mapping[str, Mapping[Any, Any]],
    value_correspondence_current: Dict[str, Dict[str, str]],
    sample_rows_per_issue: int,
    n_rows_total: int,
) -> Tuple[Dict[str, Dict[Any, Any]], Dict[str, Dict[str, str]], List[Issue], int, int, List[str], bool]:
    """
    Resuelve qué value_corrections pueden aplicarse y actualiza la vista efectiva final.

    Emite
    -----
    - FIX.VALUE.FIELD_MISSING
    - FIX.VALUE.FIELD_NOT_COMPATIBLE
    - FIX.VALUE.SOURCE_VALUES_NOT_FOUND
    - FIX.VALUE.TARGET_ALREADY_PRESENT
    - FIX.VALUE.PARTIAL_APPLY
    """
    issues: List[Issue] = []
    if not value_corrections:
        return {}, value_correspondence_current, issues, 0, 0, [], False

    schema_fields = getattr(schema, "fields", {})
    applicable: Dict[str, Dict[Any, Any]] = {}
    applied_fields_count = 0
    total_replacements = 0
    touched_fields: List[str] = []
    omitted_fields = 0

    for field_name, mapping in value_corrections.items():
        if field_name not in df.columns:
            omitted_fields += 1
            # Se avisa porque el campo objetivo no existe en el dataset tras la etapa fields.
            emit_issue(
                issues,
                FIX_TRIPS_CORRESPONDENCE_ISSUES,
                "FIX.VALUE.FIELD_MISSING",
                field=field_name,
                available_columns_sample=_sample_list(list(df.columns), 15),
                available_columns_total=len(df.columns),
                sample_rows_per_issue=sample_rows_per_issue,
                n_rows_total=n_rows_total,
            )
            continue

        field_spec = schema_fields.get(field_name)
        if field_spec is None or getattr(field_spec, "dtype", None) != "categorical":
            omitted_fields += 1
            # Se marca error porque OP-03 solo recodifica campos compatibles con corrección categórica.
            emit_issue(
                issues,
                FIX_TRIPS_CORRESPONDENCE_ISSUES,
                "FIX.VALUE.FIELD_NOT_COMPATIBLE",
                field=field_name,
                field_type=getattr(field_spec, "dtype", None),
                reason="field_not_categorical_or_not_in_schema",
                sample_rows_per_issue=sample_rows_per_issue,
                n_rows_total=n_rows_total,
            )
            continue

        series = df[field_name]
        available_values = set(series.dropna().tolist())
        target_values_present = set(series.dropna().tolist())

        matched_mapping: Dict[Any, Any] = {}
        missing_values: List[Any] = []
        for source_value, target_value in mapping.items():
            if source_value not in available_values:
                missing_values.append(source_value)
                continue

            if target_value in target_values_present and source_value != target_value:
                # Se avisa porque la regla colapsa hacia un target ya presente y puede ser redundante.
                emit_issue(
                    issues,
                    FIX_TRIPS_CORRESPONDENCE_ISSUES,
                    "FIX.VALUE.TARGET_ALREADY_PRESENT",
                    field=field_name,
                    source_value=source_value,
                    target_value=target_value,
                    sample_rows_per_issue=sample_rows_per_issue,
                    n_rows_total=n_rows_total,
                )

            matched_mapping[source_value] = target_value
            total_replacements += int((series == source_value).sum())

        if missing_values:
            # Se avisa porque algunas reglas no encontraron valores observados sobre los cuales actuar.
            emit_issue(
                issues,
                FIX_TRIPS_CORRESPONDENCE_ISSUES,
                "FIX.VALUE.SOURCE_VALUES_NOT_FOUND",
                field=field_name,
                missing_values_sample=_sample_list(missing_values, sample_rows_per_issue),
                n_missing_values=len(missing_values),
                sample_rows_per_issue=sample_rows_per_issue,
                n_rows_total=n_rows_total,
            )

        if not matched_mapping:
            omitted_fields += 1
            continue

        applicable[field_name] = matched_mapping
        touched_fields.append(field_name)
        applied_fields_count += 1

        final_mapping = dict(value_correspondence_current.get(field_name, {}))
        for source_value, target_value in matched_mapping.items():
            final_mapping[str(source_value)] = str(target_value) if target_value is not None else target_value
        value_correspondence_current[field_name] = final_mapping

    if omitted_fields > 0 and applied_fields_count > 0:
        # Se resume la aplicación parcial para no depender solo del detalle por campo/regla.
        emit_issue(
            issues,
            FIX_TRIPS_CORRESPONDENCE_ISSUES,
            "FIX.VALUE.PARTIAL_APPLY",
            requested_fields_count=len(value_corrections),
            applied_fields_count=applied_fields_count,
            replacements_count=total_replacements,
            mapping_sample=_sample_nested_mapping(value_corrections, field_limit=5, value_limit=5),
            sample_rows_per_issue=sample_rows_per_issue,
            n_rows_total=n_rows_total,
        )

    semantic_change = total_replacements > 0
    return applicable, value_correspondence_current, issues, applied_fields_count, total_replacements, touched_fields, semantic_change


def _rebuild_domains_effective_for_fields(
    *,
    data_after_values: pd.DataFrame,
    touched_fields: Sequence[str],
    schema: Any,
    metadata_domains_effective: Any,
    schema_effective_domains: Any,
) -> Tuple[Dict[str, Any], Dict[str, Any], List[str]]:
    """Reconstruye `domains_effective` solo para los campos realmente tocados por value_corrections."""
    metadata_domains_updated = copy.deepcopy(metadata_domains_effective) if isinstance(metadata_domains_effective, dict) else {}
    schema_domains_updated = copy.deepcopy(schema_effective_domains) if isinstance(schema_effective_domains, dict) else {}
    updated_fields: List[str] = []

    schema_fields = getattr(schema, "fields", {}) if schema is not None else {}

    for field_name in touched_fields:
        if field_name not in data_after_values.columns:
            continue

        series = data_after_values[field_name]
        observed_values = [value for value in series.dropna().tolist()]
        observed_values_unique = sorted({_json_safe_scalar(value) for value in observed_values}, key=lambda x: str(x))

        field_spec = schema_fields.get(field_name)
        domain_spec = getattr(field_spec, "domain", None) if field_spec is not None else None
        base_values = set(getattr(domain_spec, "values", []) or []) if domain_spec is not None else set()

        prev_entry = metadata_domains_updated.get(field_name)
        unknown_value_prev = None
        strict_applied_prev = None
        if isinstance(prev_entry, dict):
            unknown_value_prev = prev_entry.get("unknown_value")
            strict_applied_prev = prev_entry.get("strict_applied")

        added_values = sorted([value for value in observed_values_unique if value not in base_values], key=lambda x: str(x))
        entry: Dict[str, Any] = {
            "values": observed_values_unique,
            "extended": bool(len(added_values) > 0),
        }
        if added_values:
            entry["added_values"] = added_values
        if unknown_value_prev is not None and unknown_value_prev in observed_values_unique:
            entry["unknown_value"] = unknown_value_prev
        if strict_applied_prev is not None:
            entry["strict_applied"] = strict_applied_prev

        metadata_domains_updated[field_name] = entry
        schema_domains_updated[field_name] = copy.deepcopy(entry)
        updated_fields.append(field_name)

    return metadata_domains_updated, schema_domains_updated, updated_fields


# -----------------------------------------------------------------------------
# Helpers internos de uso general
# -----------------------------------------------------------------------------


def _clone_metadata(metadata: Any) -> Dict[str, Any]:
    """Copia metadata de forma controlada y garantiza una estructura dict usable."""
    if not isinstance(metadata, dict):
        return {}
    return copy.deepcopy(metadata)


def _clone_schema_effective(schema_effective: Any) -> TripSchemaEffective:
    """Copia `schema_effective` o crea una vista vacía si el input no es interpretable."""
    if isinstance(schema_effective, TripSchemaEffective):
        return copy.deepcopy(schema_effective)
    if isinstance(schema_effective, dict):
        return TripSchemaEffective(
            dtype_effective=copy.deepcopy(schema_effective.get("dtype_effective", {})),
            overrides=copy.deepcopy(schema_effective.get("overrides", {})),
            domains_effective=copy.deepcopy(schema_effective.get("domains_effective", {})),
            temporal=copy.deepcopy(schema_effective.get("temporal", {})),
            fields_effective=copy.deepcopy(schema_effective.get("fields_effective", [])),
        )
    return TripSchemaEffective()


def _clone_field_correspondence(field_correspondence: Any) -> Dict[str, str]:
    """Copia la vista efectiva final de correspondencias de campo."""
    if not isinstance(field_correspondence, dict):
        return {}
    return copy.deepcopy(field_correspondence)


def _clone_value_correspondence(value_correspondence: Any) -> Dict[str, Dict[str, str]]:
    """Copia la vista efectiva final de correspondencias de valores."""
    if not isinstance(value_correspondence, dict):
        return {}
    return copy.deepcopy(value_correspondence)


def _apply_issue_truncation(
    issues_detected: List[Issue],
    *,
    max_issues: int,
) -> Tuple[List[Issue], Optional[Dict[str, Any]]]:
    """Aplica la política de truncamiento del reporte de OP-03."""
    n_detected_total = len(issues_detected)
    if n_detected_total <= max_issues:
        return issues_detected, {
            "max_issues": max_issues,
            "issues_truncated": False,
            "n_issues_emitted": n_detected_total,
            "n_issues_detected_total": n_detected_total,
        }

    kept = list(issues_detected[: max(max_issues - 1, 0)])
    truncation_issues = list(kept)

    # Se agrega el issue final de truncamiento para que el reporte refleje explícitamente el recorte aplicado.
    emit_issue(
        truncation_issues,
        FIX_TRIPS_CORRESPONDENCE_ISSUES,
        "FIX.CORE.ISSUES_TRUNCATED",
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


def _build_issues_summary(issues: Sequence[Issue]) -> Dict[str, Any]:
    """Resume issues por severidad y por code para incorporarlos en el evento."""
    level_counts = Counter()
    code_counts = Counter()
    for issue in issues:
        level_counts[issue.level] += 1
        code_counts[issue.code] += 1
    return {
        "counts": dict(level_counts),
        "by_code": dict(code_counts),
    }


def _extract_validated_flag(metadata: Any) -> bool:
    """Lee el estado validado desde metadata, tolerando trazas antiguas del core."""
    if not isinstance(metadata, dict):
        return False
    if "is_validated" in metadata:
        return bool(metadata.get("is_validated", False))
    flags = metadata.get("flags", {})
    if isinstance(flags, dict):
        return bool(flags.get("validated", False))
    return False


def _build_added_values_by_field(domains_effective: Mapping[str, Any], fields: Sequence[str]) -> Dict[str, List[Any]]:
    """Extrae `added_values` por campo para dejar evidencia resumida de dominios."""
    out: Dict[str, List[Any]] = {}
    for field_name in fields:
        entry = domains_effective.get(field_name)
        if not isinstance(entry, dict):
            continue
        added_values = entry.get("added_values")
        if isinstance(added_values, list):
            out[field_name] = copy.deepcopy(added_values)
    return out


def _sample_list(values: Iterable[Any], limit: int) -> List[Any]:
    """Toma una muestra simple y JSON-safe de una secuencia."""
    out: List[Any] = []
    for value in list(values)[:limit]:
        out.append(_json_safe_scalar(value))
    return out


def _sample_mapping(mapping: Mapping[Any, Any], *, limit: int) -> Dict[str, Any]:
    """Devuelve una muestra pequeña de un mapping simple para details/reportes."""
    out: Dict[str, Any] = {}
    for idx, (key, value) in enumerate(mapping.items()):
        if idx >= limit:
            break
        out[str(key)] = _json_safe_scalar(value)
    return out


def _sample_nested_mapping(
    mapping: Mapping[Any, Mapping[Any, Any]],
    *,
    field_limit: int,
    value_limit: int,
) -> Dict[str, Dict[str, Any]]:
    """Devuelve una muestra pequeña de un mapping anidado para details/reportes."""
    out: Dict[str, Dict[str, Any]] = {}
    for field_idx, (field_name, inner) in enumerate(mapping.items()):
        if field_idx >= field_limit:
            break
        inner_out: Dict[str, Any] = {}
        for value_idx, (source_value, target_value) in enumerate(inner.items()):
            if value_idx >= value_limit:
                break
            inner_out[str(source_value)] = _json_safe_scalar(target_value)
        out[str(field_name)] = inner_out
    return out


def _to_json_serializable_or_none(obj: Any) -> Any:
    """Convierte dict/list anidados a una forma JSON-safe sin hacer fallback silencioso."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return {str(k): _to_json_serializable_or_none(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_json_serializable_or_none(v) for v in obj]
    if _json_is_serializable(obj):
        return obj
    raise TypeError(f"Object is not JSON-serializable: {type(obj).__name__}")


def _json_is_serializable(obj: Any) -> bool:
    """Chequea si un objeto puede serializarse directamente a JSON."""
    try:
        json.dumps(obj)
        return True
    except Exception:
        return False


def _json_safe_scalar(value: Any) -> Any:
    """Normaliza un escalar a una representación JSON-friendly y estable."""
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _utc_now_iso() -> str:
    """Retorna timestamp UTC en ISO-8601 para eventos del módulo."""
    return datetime.now(timezone.utc).isoformat()


def sanitize_correspondence_context(
    correspondence_context: Optional[Dict[str, Any]],
) -> Tuple[Optional[Dict[str, Any]], List[str], List[str]]:
    """
    Sanea `correspondence_context` y descarta fragmentos no permitidos o no serializables.

    Returns
    -------
    tuple
        `(context_sanitized, unknown_keys, dropped_paths)`.

    Notes
    -----
    - Si el root no es `dict | None`, esta función levanta `TypeError`.
    - Solo conserva las keys top-level permitidas por el contrato de OP-03.
    """
    if correspondence_context is None:
        return None, [], []

    if not isinstance(correspondence_context, dict):
        raise TypeError("correspondence_context must be dict or None")

    unknown_keys: List[str] = []
    dropped_paths: List[str] = []
    sanitized: Dict[str, Any] = {}

    for key, value in correspondence_context.items():
        # Se descartan keys fuera del whitelist top-level vigente.
        if key not in _ALLOWED_CONTEXT_KEYS:
            unknown_keys.append(str(key))
            continue

        safe_value, value_dropped_paths = _sanitize_json_value(value, path=key)
        if safe_value is not None:
            sanitized[key] = safe_value
        dropped_paths.extend(value_dropped_paths)

    return sanitized, sorted(unknown_keys), dropped_paths


def apply_field_corrections(
    df: pd.DataFrame,
    corrections: FieldCorrections,
    *,
    inplace: bool = False,
) -> pd.DataFrame:
    """
    Aplica correcciones de nombres de columnas (renombrado) sobre un DataFrame.

    Notes
    -----
    - Esta función es deliberadamente pura y no emite issues.
    - Se asume que la política de qué correcciones son válidas ya fue resuelta por OP-03.
    """
    out = df if inplace else df.copy(deep=True)
    if not corrections:
        return out
    return out.rename(columns=dict(corrections), inplace=False)


def apply_value_corrections(
    df: pd.DataFrame,
    corrections: ValueCorrections,
    *,
    inplace: bool = False,
) -> pd.DataFrame:
    """
    Aplica correcciones de valores categóricos (recode) por campo.

    Notes
    -----
    - Esta función es deliberadamente pura y no emite issues.
    - Se asume que la política de qué campos/reglas son válidos ya fue resuelta por OP-03.
    """
    out = df if inplace else df.copy(deep=True)
    if not corrections:
        return out

    for field_name, mapping in corrections.items():
        if field_name not in out.columns:
            continue
        if not mapping:
            continue

        # Se aplica replace para preservar nulos y dtype lo mejor posible.
        out[field_name] = out[field_name].replace(dict(mapping))

    return out


def _sanitize_json_value(value: Any, *, path: str) -> Tuple[Any, List[str]]:
    """Devuelve una versión JSON-safe del valor o `None` si debe descartarse."""
    dropped_paths: List[str] = []

    if value is None or isinstance(value, (str, int, float, bool)):
        if _json_is_serializable(value):
            return value, dropped_paths
        dropped_paths.append(path)
        return None, dropped_paths

    if isinstance(value, dict):
        sanitized_dict: Dict[str, Any] = {}
        for sub_key, sub_value in value.items():
            sub_key_str = str(sub_key)
            safe_sub_value, sub_dropped = _sanitize_json_value(sub_value, path=f"{path}.{sub_key_str}")
            if safe_sub_value is not None:
                sanitized_dict[sub_key_str] = safe_sub_value
            dropped_paths.extend(sub_dropped)
        if sanitized_dict:
            return sanitized_dict, dropped_paths
        dropped_paths.append(path)
        return None, dropped_paths

    if isinstance(value, (list, tuple)):
        sanitized_list: List[Any] = []
        for idx, item in enumerate(value):
            safe_item, item_dropped = _sanitize_json_value(item, path=f"{path}[{idx}]")
            if safe_item is not None:
                sanitized_list.append(safe_item)
            dropped_paths.extend(item_dropped)
        if sanitized_list:
            return sanitized_list, dropped_paths
        dropped_paths.append(path)
        return None, dropped_paths

    dropped_paths.append(path)
    return None, dropped_paths


def _json_is_serializable(obj: Any) -> bool:
    """Chequea si un objeto puede serializarse directamente a JSON."""
    try:
        json.dumps(obj)
        return True
    except Exception:
        return False
