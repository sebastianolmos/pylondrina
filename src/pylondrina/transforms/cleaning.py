# -------------------------
# file: pylondrina/transforms/cleaning.py
# -------------------------
from __future__ import annotations

import copy
import json
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence

import h3
import pandas as pd
from pandas.api import types as ptypes

from pylondrina.datasets import TripDataset
from pylondrina.errors import PylondrinaError
from pylondrina.issues.catalog_clean_trips import CLEAN_TRIPS_ISSUES
from pylondrina.issues.core import emit_and_maybe_raise, emit_issue
from pylondrina.reports import Issue, OperationReport
from pylondrina.schema import TripSchemaEffective


class _CleanTypeError(TypeError, PylondrinaError):
    """
    Adaptador interno para errores fatales que deben comportarse como TypeError.
    """

    def __init__(
        self,
        message: str,
        *,
        code: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
        issue: Optional[Issue] = None,
        issues: Optional[Sequence[Issue]] = None,
    ) -> None:
        PylondrinaError.__init__(
            self,
            message,
            code=code,
            details=details,
            issue=issue,
            issues=issues,
        )


class _CleanValueError(ValueError, PylondrinaError):
    """
    Adaptador interno para errores fatales que deben comportarse como ValueError.
    """

    def __init__(
        self,
        message: str,
        *,
        code: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
        issue: Optional[Issue] = None,
        issues: Optional[Sequence[Issue]] = None,
    ) -> None:
        PylondrinaError.__init__(
            self,
            message,
            code=code,
            details=details,
            issue=issue,
            issues=issues,
        )


EXCEPTION_MAP_CLEAN = {
    "type": _CleanTypeError,
    "value": _CleanValueError,
}

_LATLON_FIELDS = (
    "origin_latitude",
    "origin_longitude",
    "destination_latitude",
    "destination_longitude",
)
_H3_FIELDS = ("origin_h3_index", "destination_h3_index")
_TIME_FIELDS = ("origin_time_utc", "destination_time_utc")

_SUMMARY_RULE_KEYS = (
    "nulls_required",
    "nulls_fields",
    "invalid_latlon",
    "invalid_h3",
    "origin_after_destination",
    "duplicates",
    "categorical_values",
)


@dataclass(frozen=True)
class CleanOptions:
    """
    Opciones de limpieza drop-only para `clean_trips`.

    Attributes
    ----------
    drop_rows_with_nulls_in_required_fields : bool, default=False
        Si True, elimina filas con nulos en cualquier campo requerido del schema.
    drop_rows_with_nulls_in_fields : sequence of str, optional
        Lista de campos para eliminar filas con nulos en cualquiera de ellos.
    drop_rows_with_invalid_latlon : bool, default=False
        Si True, elimina filas con geografía OD inválida según la semántica cerrada de OP-04.
    drop_rows_with_invalid_h3 : bool, default=False
        Si True, elimina filas con H3 faltante o inválido en origen/destino.
    drop_rows_with_origin_after_destination : bool, default=False
        Si True, elimina filas con origen posterior a destino cuando la regla es evaluable.
    drop_duplicates : bool, default=False
        Si True, elimina filas duplicadas conservando la primera ocurrencia.
    duplicates_subset : sequence of str, optional
        Subconjunto de columnas para detectar duplicados.
    drop_rows_by_categorical_values : mapping, optional
        Mapeo campo -> lista de valores a eliminar. `None` dentro de la lista
        significa eliminar también nulos/NaN en ese campo.
    """

    drop_rows_with_nulls_in_required_fields: bool = False
    drop_rows_with_nulls_in_fields: Optional[Sequence[str]] = None

    drop_rows_with_invalid_latlon: bool = False
    drop_rows_with_invalid_h3: bool = False
    drop_rows_with_origin_after_destination: bool = False

    drop_duplicates: bool = False
    duplicates_subset: Optional[Sequence[str]] = None

    drop_rows_by_categorical_values: Optional[Mapping[str, Sequence[Any]]] = None


def clean_trips(
    trips: TripDataset,
    *,
    options: Optional[CleanOptions] = None,
) -> tuple[TripDataset, OperationReport]:
    """
    Limpia un `TripDataset` eliminando filas según reglas explícitas (solo drops).

    Parameters
    ----------
    trips : TripDataset
        Dataset de viajes en formato Golondrina.
    options : CleanOptions, optional
        Reglas de limpieza. Si es None, se usan defaults efectivos (sin reglas activas).

    Returns
    -------
    tuple[TripDataset, OperationReport]
        Nuevo dataset derivado y reporte de la operación.
    """
    issues: List[Issue] = []

    # ------------------------------------------------------------------
    # 1) Precondiciones fatales y normalización básica del request
    # ------------------------------------------------------------------
    # Se asegura temprano que la operación reciba un TripDataset usable.
    if not isinstance(trips, TripDataset):
        emit_and_maybe_raise(
            issues,
            CLEAN_TRIPS_ISSUES,
            "CLN.CONFIG.INVALID_TRIPS_OBJECT",
            strict=False,
            exception_map=EXCEPTION_MAP_CLEAN,
            default_exception=_CleanTypeError,
            received_type=type(trips).__name__,
        )

    # Se fija la superficie normativa de trabajo: TripDataset.data.
    if not hasattr(trips, "data") or not isinstance(trips.data, pd.DataFrame):
        emit_and_maybe_raise(
            issues,
            CLEAN_TRIPS_ISSUES,
            "CLN.CONFIG.MISSING_DATAFRAME",
            strict=False,
            exception_map=EXCEPTION_MAP_CLEAN,
            default_exception=_CleanTypeError,
            received_type=type(getattr(trips, "data", None)).__name__,
            reason="missing_or_not_dataframe",
        )

    # Se asegura que options tenga la forma esperable para construir parámetros efectivos.
    if options is not None and not isinstance(options, CleanOptions):
        emit_and_maybe_raise(
            issues,
            CLEAN_TRIPS_ISSUES,
            "CLN.CONFIG.INVALID_OPTIONS_OBJECT",
            strict=False,
            exception_map=EXCEPTION_MAP_CLEAN,
            default_exception=_CleanTypeError,
            received_type=type(options).__name__,
        )

    options_eff = options or CleanOptions()

    # Se valida la estructura de la lista explícita de campos nulos antes del pipeline principal.
    if options_eff.drop_rows_with_nulls_in_fields is None:
        null_fields_raw: Optional[Sequence[str]] = None
    else:
        if isinstance(options_eff.drop_rows_with_nulls_in_fields, (str, bytes)) or not isinstance(
            options_eff.drop_rows_with_nulls_in_fields,
            Sequence,
        ):
            emit_and_maybe_raise(
                issues,
                CLEAN_TRIPS_ISSUES,
                "CLN.CONFIG.INVALID_NULL_FIELDS",
                strict=False,
                exception_map=EXCEPTION_MAP_CLEAN,
                default_exception=_CleanValueError,
                received_type=type(options_eff.drop_rows_with_nulls_in_fields).__name__,
                reason="expected_sequence_of_str",
            )

        null_fields_raw = list(options_eff.drop_rows_with_nulls_in_fields)
        if any(not isinstance(field_name, str) for field_name in null_fields_raw):
            emit_and_maybe_raise(
                issues,
                CLEAN_TRIPS_ISSUES,
                "CLN.CONFIG.INVALID_NULL_FIELDS",
                strict=False,
                exception_map=EXCEPTION_MAP_CLEAN,
                default_exception=_CleanValueError,
                invalid_fields_sample=[
                    _json_safe_scalar(field_name)
                    for field_name in null_fields_raw
                    if not isinstance(field_name, str)
                ][:5],
                reason="expected_sequence_of_str",
            )

    # Se valida la estructura del subset de duplicados aunque luego la regla quede inactiva.
    if options_eff.duplicates_subset is None:
        duplicates_subset_raw: Optional[Sequence[str]] = None
    else:
        if isinstance(options_eff.duplicates_subset, (str, bytes)) or not isinstance(
            options_eff.duplicates_subset,
            Sequence,
        ):
            emit_and_maybe_raise(
                issues,
                CLEAN_TRIPS_ISSUES,
                "CLN.CONFIG.INVALID_DUPLICATES_SUBSET",
                strict=False,
                exception_map=EXCEPTION_MAP_CLEAN,
                default_exception=_CleanValueError,
                received_type=type(options_eff.duplicates_subset).__name__,
                reason="expected_sequence_of_str",
            )

        duplicates_subset_raw = list(options_eff.duplicates_subset)
        if any(not isinstance(field_name, str) for field_name in duplicates_subset_raw):
            emit_and_maybe_raise(
                issues,
                CLEAN_TRIPS_ISSUES,
                "CLN.CONFIG.INVALID_DUPLICATES_SUBSET",
                strict=False,
                exception_map=EXCEPTION_MAP_CLEAN,
                default_exception=_CleanValueError,
                invalid_fields_sample=[
                    _json_safe_scalar(field_name)
                    for field_name in duplicates_subset_raw
                    if not isinstance(field_name, str)
                ][:5],
                reason="expected_sequence_of_str",
            )

    # Se valida la estructura del mapping categórico antes de intentar evaluarlo por campo.
    if options_eff.drop_rows_by_categorical_values is None:
        categorical_drop_raw: Optional[Mapping[str, Sequence[Any]]] = None
    else:
        if not isinstance(options_eff.drop_rows_by_categorical_values, Mapping):
            emit_and_maybe_raise(
                issues,
                CLEAN_TRIPS_ISSUES,
                "CLN.CONFIG.INVALID_CATEGORICAL_MAPPING",
                strict=False,
                exception_map=EXCEPTION_MAP_CLEAN,
                default_exception=_CleanValueError,
                received_type=type(options_eff.drop_rows_by_categorical_values).__name__,
                reason="expected_mapping_field_to_sequence",
            )

        categorical_drop_raw = options_eff.drop_rows_by_categorical_values
        for field_name, raw_values in categorical_drop_raw.items():
            if not isinstance(field_name, str):
                emit_and_maybe_raise(
                    issues,
                    CLEAN_TRIPS_ISSUES,
                    "CLN.CONFIG.INVALID_CATEGORICAL_MAPPING",
                    strict=False,
                    exception_map=EXCEPTION_MAP_CLEAN,
                    default_exception=_CleanValueError,
                    received_type=type(field_name).__name__,
                    reason="expected_str_field_name",
                )

            if isinstance(raw_values, (str, bytes)) or not isinstance(raw_values, Sequence):
                emit_and_maybe_raise(
                    issues,
                    CLEAN_TRIPS_ISSUES,
                    "CLN.CONFIG.INVALID_CATEGORICAL_MAPPING",
                    strict=False,
                    exception_map=EXCEPTION_MAP_CLEAN,
                    default_exception=_CleanValueError,
                    field=field_name,
                    received_type=type(raw_values).__name__,
                    reason="expected_sequence_of_json_safe_values",
                )

            for raw_value in raw_values:
                if raw_value is not None and not isinstance(raw_value, (str, int, float, bool)):
                    emit_and_maybe_raise(
                        issues,
                        CLEAN_TRIPS_ISSUES,
                        "CLN.CONFIG.NON_SERIALIZABLE_PARAMETER",
                        strict=False,
                        exception_map=EXCEPTION_MAP_CLEAN,
                        default_exception=_CleanValueError,
                        field=field_name,
                        option_name="drop_rows_by_categorical_values",
                        value_repr=repr(raw_value)[:200],
                    )

    rows_in = len(trips.data)
    required_fields = [field_name for field_name in getattr(trips.schema, "required", []) if isinstance(field_name, str)]

    # ------------------------------------------------------------------
    # 2) Resolución de parámetros efectivos y reglas activas
    # ------------------------------------------------------------------
    # Se normaliza la lista explícita de nulos para que el contrato use null cuando la regla está inactiva.
    null_fields_effective = _normalize_string_list_or_none(null_fields_raw)

    # Se normaliza el mapping categórico para que el contrato use null cuando la regla está inactiva.
    categorical_drop_effective = _normalize_categorical_drop_map_or_none(categorical_drop_raw)

    # Se resuelve el subset efectivo de duplicados antes de decidir si la regla es utilizable.
    duplicates_subset_effective = resolve_duplicates_subset_effective(
        trips,
        drop_duplicates=bool(options_eff.drop_duplicates),
        duplicates_subset=duplicates_subset_raw,
    )

    # Se aborta si el subset explícito de duplicados apunta a columnas inexistentes y la regla está activa.
    if bool(options_eff.drop_duplicates) and duplicates_subset_raw is not None:
        missing_duplicates_fields = [
            field_name for field_name in duplicates_subset_effective or [] if field_name not in trips.data.columns
        ]
        if missing_duplicates_fields:
            emit_and_maybe_raise(
                issues,
                CLEAN_TRIPS_ISSUES,
                "CLN.CONFIG.INVALID_DUPLICATES_SUBSET",
                strict=False,
                exception_map=EXCEPTION_MAP_CLEAN,
                default_exception=_CleanValueError,
                duplicates_subset=list(duplicates_subset_raw),
                missing_fields=missing_duplicates_fields,
                reason="subset_fields_missing_in_dataframe",
            )

    parameters = {
        "drop_rows_with_nulls_in_required_fields": bool(options_eff.drop_rows_with_nulls_in_required_fields),
        "drop_rows_with_nulls_in_fields": null_fields_effective,
        "drop_rows_with_invalid_latlon": bool(options_eff.drop_rows_with_invalid_latlon),
        "drop_rows_with_invalid_h3": bool(options_eff.drop_rows_with_invalid_h3),
        "drop_rows_with_origin_after_destination": bool(options_eff.drop_rows_with_origin_after_destination),
        "drop_duplicates": bool(options_eff.drop_duplicates),
        "duplicates_subset": list(duplicates_subset_raw) if duplicates_subset_raw is not None else None,
        "duplicates_subset_effective": duplicates_subset_effective,
        "drop_rows_by_categorical_values": categorical_drop_effective,
    }

    dropped_by_rule: Dict[str, int] = {rule_name: 0 for rule_name in _SUMMARY_RULE_KEYS}
    survivor_mask = pd.Series(True, index=trips.data.index, dtype=bool)
    applied_rules: List[str] = []
    omitted_rules: List[str] = []

    temporal_meta = trips.metadata.get("temporal", {}) if isinstance(trips.metadata, dict) else {}
    temporal_tier = _extract_temporal_tier(temporal_meta, trips.data)
    temporal_fields_present = temporal_meta.get("fields_present") if isinstance(temporal_meta, dict) else None

    # ------------------------------------------------------------------
    # 3) Evaluación de reglas y acumulación de supervivencia
    # ------------------------------------------------------------------
    # Se aplica nulls_required solo si el request está activo y las columnas requeridas existen.
    if bool(options_eff.drop_rows_with_nulls_in_required_fields):
        missing_required_columns = [field_name for field_name in required_fields if field_name not in trips.data.columns]
        if missing_required_columns:
            # Se omite la regla porque el dataset no expone todas las columnas requeridas para evaluarla.
            emit_issue(
                issues,
                CLEAN_TRIPS_ISSUES,
                "CLN.RULE.FIELDS_MISSING",
                rule="nulls_required",
                missing_fields=missing_required_columns,
                available_fields_sample=_sample_list(list(trips.data.columns), limit=10),
                available_fields_total=len(trips.data.columns),
            )
            omitted_rules.append("nulls_required")
        elif required_fields:
            current = trips.data.loc[survivor_mask]
            drop_mask = mask_nulls_in_fields(current, required_fields)
            dropped_count = int(drop_mask.sum())
            dropped_by_rule["nulls_required"] = dropped_count
            if dropped_count > 0:
                survivor_mask.loc[current.index[drop_mask]] = False
            applied_rules.append("nulls_required")

    # Se aplica nulls_fields solo cuando la lista explícita quedó activa y totalmente evaluable.
    if null_fields_effective is not None:
        missing_null_fields = [field_name for field_name in null_fields_effective if field_name not in trips.data.columns]
        if missing_null_fields:
            # Se omite la regla completa para no reinterpretar parcialmente el request del usuario.
            emit_issue(
                issues,
                CLEAN_TRIPS_ISSUES,
                "CLN.RULE.FIELDS_MISSING",
                rule="nulls_fields",
                missing_fields=missing_null_fields,
                available_fields_sample=_sample_list(list(trips.data.columns), limit=10),
                available_fields_total=len(trips.data.columns),
            )
            omitted_rules.append("nulls_fields")
        else:
            current = trips.data.loc[survivor_mask]
            drop_mask = mask_nulls_in_fields(current, null_fields_effective)
            dropped_count = int(drop_mask.sum())
            dropped_by_rule["nulls_fields"] = dropped_count
            if dropped_count > 0:
                survivor_mask.loc[current.index[drop_mask]] = False
            applied_rules.append("nulls_fields")

    # Se aplica invalid_latlon solo si están las columnas OD mínimas para evaluar la regla.
    if bool(options_eff.drop_rows_with_invalid_latlon):
        missing_latlon_fields = [field_name for field_name in _LATLON_FIELDS if field_name not in trips.data.columns]
        if missing_latlon_fields:
            # Se omite la regla porque no existe superficie espacial suficiente para evaluarla.
            emit_issue(
                issues,
                CLEAN_TRIPS_ISSUES,
                "CLN.RULE.FIELDS_MISSING",
                rule="invalid_latlon",
                missing_fields=missing_latlon_fields,
                available_fields_sample=_sample_list(list(trips.data.columns), limit=10),
                available_fields_total=len(trips.data.columns),
            )
            omitted_rules.append("invalid_latlon")
        else:
            current = trips.data.loc[survivor_mask]
            drop_mask = mask_invalid_latlon(current)
            dropped_count = int(drop_mask.sum())
            dropped_by_rule["invalid_latlon"] = dropped_count
            if dropped_count > 0:
                survivor_mask.loc[current.index[drop_mask]] = False
            applied_rules.append("invalid_latlon")

    # Se aplica invalid_h3 solo si ambos índices H3 existen; no se admite semántica parcial.
    if bool(options_eff.drop_rows_with_invalid_h3):
        missing_h3_fields = [field_name for field_name in _H3_FIELDS if field_name not in trips.data.columns]
        if missing_h3_fields:
            # Se omite la regla porque sin ambos H3 no existe contrato evaluable para esta validación drop-only.
            emit_issue(
                issues,
                CLEAN_TRIPS_ISSUES,
                "CLN.RULE.FIELDS_MISSING",
                rule="invalid_h3",
                missing_fields=missing_h3_fields,
                available_fields_sample=_sample_list(list(trips.data.columns), limit=10),
                available_fields_total=len(trips.data.columns),
            )
            omitted_rules.append("invalid_h3")
        else:
            current = trips.data.loc[survivor_mask]
            drop_mask = mask_invalid_h3(current)
            dropped_count = int(drop_mask.sum())
            dropped_by_rule["invalid_h3"] = dropped_count
            if dropped_count > 0:
                survivor_mask.loc[current.index[drop_mask]] = False
            applied_rules.append("invalid_h3")

    # Se aplica origin_after_destination solo si el dataset es Tier 1 y la comparación es interpretable.
    if bool(options_eff.drop_rows_with_origin_after_destination):
        if temporal_tier != "tier_1":
            # Se omite la regla porque el dataset no ofrece temporalidad absoluta evaluable para este check.
            emit_issue(
                issues,
                CLEAN_TRIPS_ISSUES,
                "CLN.RULE.TEMPORAL_RULE_NOT_EVALUABLE",
                rule="origin_after_destination",
                temporal_tier=temporal_tier,
                fields_present=_to_json_serializable_or_none(temporal_fields_present),
                missing_fields=[field_name for field_name in _TIME_FIELDS if field_name not in trips.data.columns],
            )
            omitted_rules.append("origin_after_destination")
        else:
            missing_time_fields = [field_name for field_name in _TIME_FIELDS if field_name not in trips.data.columns]
            if missing_time_fields:
                # Se omite la regla porque la metadata temporal declara Tier 1, pero faltan columnas efectivas para compararla.
                emit_issue(
                    issues,
                    CLEAN_TRIPS_ISSUES,
                    "CLN.RULE.TEMPORAL_RULE_NOT_EVALUABLE",
                    rule="origin_after_destination",
                    temporal_tier=temporal_tier,
                    fields_present=_to_json_serializable_or_none(temporal_fields_present),
                    missing_fields=missing_time_fields,
                    reason="missing_required_columns",
                )
                omitted_rules.append("origin_after_destination")
            else:
                current = trips.data.loc[survivor_mask]
                drop_mask = mask_origin_after_destination(current)
                dropped_count = int(drop_mask.sum())
                dropped_by_rule["origin_after_destination"] = dropped_count
                if dropped_count > 0:
                    survivor_mask.loc[current.index[drop_mask]] = False
                applied_rules.append("origin_after_destination")

    # Se aplica duplicates según el subset efectivo resuelto y el contrato de preservación de la primera ocurrencia.
    if bool(options_eff.drop_duplicates):
        if duplicates_subset_effective == []:
            # Se omite la regla porque el default no produjo ninguna columna utilizable.
            emit_issue(
                issues,
                CLEAN_TRIPS_ISSUES,
                "CLN.RULE.DEFAULT_DUPLICATES_SUBSET_UNAVAILABLE",
                subset_default=list(required_fields),
                subset_effective=[],
                schema_required=list(required_fields),
                available_fields_sample=_sample_list(list(trips.data.columns), limit=10),
                available_fields_total=len(trips.data.columns),
            )
            omitted_rules.append("duplicates")
        elif duplicates_subset_effective is not None:
            current = trips.data.loc[survivor_mask]
            drop_mask = mask_duplicates(current, duplicates_subset_effective)
            dropped_count = int(drop_mask.sum())
            dropped_by_rule["duplicates"] = dropped_count
            if dropped_count > 0:
                survivor_mask.loc[current.index[drop_mask]] = False
            applied_rules.append("duplicates")

    # Se aplica categorical_values por entrada de campo, omitiendo solo las entradas no utilizables.
    if categorical_drop_effective is not None:
        current = trips.data.loc[survivor_mask]
        categorical_union = pd.Series(False, index=current.index, dtype=bool)
        categorical_applied = False

        for field_name, banned_values in categorical_drop_effective.items():
            if field_name not in current.columns:
                # Se omite solo esta entrada porque el campo pedido no existe en el dataset actual.
                emit_issue(
                    issues,
                    CLEAN_TRIPS_ISSUES,
                    "CLN.RULE.FIELDS_MISSING",
                    rule="categorical_values",
                    missing_fields=[field_name],
                    available_fields_sample=_sample_list(list(current.columns), limit=10),
                    available_fields_total=len(current.columns),
                    details={
                        "rule": "categorical_values",
                        "missing_fields": [field_name],
                        "available_fields_sample": _sample_list(list(current.columns), limit=10),
                        "available_fields_total": len(current.columns),
                        "reason": "missing_required_columns",
                        "action": "rule_entry_omitted",
                    },
                )
                continue

            if not _is_categorical_field(trips, field_name, current[field_name]):
                # Se omite esta entrada porque el campo no es categórico ni interpretable como tal.
                emit_issue(
                    issues,
                    CLEAN_TRIPS_ISSUES,
                    "CLN.RULE.FIELD_NOT_CATEGORICAL",
                    field=field_name,
                    dtype_observed=str(current[field_name].dtype),
                )
                continue

            categorical_union = categorical_union | mask_categorical_values(current[[field_name]], {field_name: banned_values})
            categorical_applied = True

        if categorical_applied:
            dropped_count = int(categorical_union.sum())
            dropped_by_rule["categorical_values"] = dropped_count
            if dropped_count > 0:
                survivor_mask.loc[current.index[categorical_union]] = False
            applied_rules.append("categorical_values")
        elif categorical_drop_effective:
            omitted_rules.append("categorical_values")

    # ------------------------------------------------------------------
    # 4) Commit único del dataset derivado
    # ------------------------------------------------------------------
    rows_out = int(survivor_mask.sum())
    data_out = trips.data.loc[survivor_mask].copy(deep=False)

    metadata_out = _clone_metadata(trips.metadata)
    if not isinstance(metadata_out.get("events"), list):
        metadata_out["events"] = []
    metadata_out["is_validated"] = bool(_extract_validated_flag(trips.metadata))

    schema_effective_out = _clone_schema_effective(getattr(trips, "schema_effective", None))
    field_correspondence_out = copy.deepcopy(getattr(trips, "field_correspondence", {}))
    value_correspondence_out = copy.deepcopy(getattr(trips, "value_correspondence", {}))
    provenance_out = copy.deepcopy(getattr(trips, "provenance", {}))

    # ------------------------------------------------------------------
    # 5) Evidencia final, evento y reporte
    # ------------------------------------------------------------------
    summary = build_clean_summary(
        rows_in=rows_in,
        rows_out=rows_out,
        dropped_by_rule=dropped_by_rule,
    )

    if not applied_rules:
        # Se deja evidencia explícita cuando la operación retorna sin ninguna regla realmente activa.
        emit_issue(
            issues,
            CLEAN_TRIPS_ISSUES,
            "CLN.NO_CHANGES.NO_RULES_ACTIVE",
            rows_in=rows_in,
            rows_out=rows_out,
            dropped_total=summary["dropped_total"],
            active_rules=[],
            omitted_rules=omitted_rules,
        )
    elif summary["dropped_total"] == 0:
        # Se deja evidencia explícita cuando la operación retorna sin cambios efectivos sobre las filas.
        emit_issue(
            issues,
            CLEAN_TRIPS_ISSUES,
            "CLN.NO_CHANGES.NO_ROWS_DROPPED",
            rows_in=rows_in,
            rows_out=rows_out,
            dropped_total=summary["dropped_total"],
            active_rules=applied_rules,
            omitted_rules=omitted_rules,
        )

    if rows_out == 0:
        # Se advierte el vaciamiento total porque puede ser legítimo, pero operativamente importante.
        emit_issue(
            issues,
            CLEAN_TRIPS_ISSUES,
            "CLN.RESULT.EMPTY_DATASET",
            rows_in=rows_in,
            rows_out=rows_out,
            dropped_total=summary["dropped_total"],
            active_rules=applied_rules,
            dropped_by_rule=summary["dropped_by_rule"],
        )

    ok = not any(issue.level == "error" for issue in issues)
    report = OperationReport(
        ok=ok,
        issues=issues,
        summary=summary,
        parameters=parameters,
    )

    level_counts = Counter(issue.level for issue in issues)
    code_counts = Counter(issue.code for issue in issues)
    issues_summary = {
        "counts": {
            "info": int(level_counts.get("info", 0)),
            "warning": int(level_counts.get("warning", 0)),
            "error": int(level_counts.get("error", 0)),
        },
        "top_codes": [
            {"code": code, "count": int(count)}
            for code, count in sorted(code_counts.items(), key=lambda item: (-item[1], item[0]))[:10]
        ],
    }

    event = {
        "op": "clean_trips",
        "ts_utc": _utc_now_iso(),
        "parameters": parameters,
        "summary": summary,
        "issues_summary": issues_summary,
    }
    metadata_out["events"].append(event)

    cleaned = TripDataset(
        data=data_out,
        schema=trips.schema,
        schema_version=trips.schema_version,
        provenance=provenance_out,
        field_correspondence=field_correspondence_out,
        value_correspondence=value_correspondence_out,
        metadata=metadata_out,
        schema_effective=schema_effective_out,
    )

    return cleaned, report


# -----------------------------------------------------------------------------
# Helpers internos principales del pipeline de la operación
# -----------------------------------------------------------------------------


def resolve_duplicates_subset_effective(
    trips: TripDataset,
    *,
    drop_duplicates: bool,
    duplicates_subset: Optional[Sequence[str]],
) -> Optional[List[str]]:
    """
    Resuelve `duplicates_subset_effective` según el contrato de OP-04.

    Emite
    -----
    No emite issues directamente.
    """
    # Se devuelve null contractual cuando la regla de duplicados no está activa.
    if not drop_duplicates:
        return None

    # Se normaliza el subset explícito preservando orden y unicidad.
    if duplicates_subset is not None:
        return _deduplicate_preserving_order([str(field_name) for field_name in duplicates_subset])

    # Se usa el default desde schema.required intersectado con columnas presentes en data.
    required_fields = [field_name for field_name in getattr(trips.schema, "required", []) if isinstance(field_name, str)]
    effective = [field_name for field_name in required_fields if field_name in trips.data.columns]
    return _deduplicate_preserving_order(effective)


def mask_nulls_in_fields(data: pd.DataFrame, fields: Sequence[str]) -> pd.Series:
    """
    Construye la máscara de filas con nulos en cualquiera de los campos dados.

    Emite
    -----
    No emite issues directamente.
    """
    # Se usa OR por columnas para marcar cualquier fila con al menos un nulo en los campos objetivo.
    if len(fields) == 0:
        return pd.Series(False, index=data.index, dtype=bool)
    return data.loc[:, list(fields)].isna().any(axis=1)


def mask_invalid_latlon(data: pd.DataFrame) -> pd.Series:
    """
    Construye la máscara de `invalid_latlon` según la semántica cerrada de OP-04.

    Emite
    -----
    No emite issues directamente.
    """
    origin_lat_raw = data["origin_latitude"]
    origin_lon_raw = data["origin_longitude"]
    dest_lat_raw = data["destination_latitude"]
    dest_lon_raw = data["destination_longitude"]

    origin_lat = pd.to_numeric(origin_lat_raw, errors="coerce")
    origin_lon = pd.to_numeric(origin_lon_raw, errors="coerce")
    dest_lat = pd.to_numeric(dest_lat_raw, errors="coerce")
    dest_lon = pd.to_numeric(dest_lon_raw, errors="coerce")

    origin_present_lat = origin_lat_raw.notna()
    origin_present_lon = origin_lon_raw.notna()
    dest_present_lat = dest_lat_raw.notna()
    dest_present_lon = dest_lon_raw.notna()

    origin_absent = ~origin_present_lat & ~origin_present_lon
    dest_absent = ~dest_present_lat & ~dest_present_lon

    origin_partial = origin_present_lat ^ origin_present_lon
    dest_partial = dest_present_lat ^ dest_present_lon

    origin_complete = origin_present_lat & origin_present_lon
    dest_complete = dest_present_lat & dest_present_lon

    origin_complete_invalid = origin_complete & (
        origin_lat.isna()
        | origin_lon.isna()
        | ~origin_lat.between(-90.0, 90.0)
        | ~origin_lon.between(-180.0, 180.0)
    )
    dest_complete_invalid = dest_complete & (
        dest_lat.isna()
        | dest_lon.isna()
        | ~dest_lat.between(-90.0, 90.0)
        | ~dest_lon.between(-180.0, 180.0)
    )

    both_absent = origin_absent & dest_absent
    return origin_partial | dest_partial | origin_complete_invalid | dest_complete_invalid | both_absent


def mask_invalid_h3(data: pd.DataFrame) -> pd.Series:
    """
    Construye la máscara de `invalid_h3` exigiendo ambos índices H3 presentes y válidos.

    Emite
    -----
    No emite issues directamente.
    """
    origin = data["origin_h3_index"].map(_is_valid_h3_value)
    destination = data["destination_h3_index"].map(_is_valid_h3_value)
    return ~(origin & destination)


def mask_origin_after_destination(data: pd.DataFrame) -> pd.Series:
    """
    Construye la máscara temporal cuando la regla ya fue declarada evaluable por la función principal.

    Emite
    -----
    No emite issues directamente.
    """
    origin = pd.to_datetime(data["origin_time_utc"], errors="coerce", utc=True)
    destination = pd.to_datetime(data["destination_time_utc"], errors="coerce", utc=True)
    comparable = origin.notna() & destination.notna()
    return comparable & (origin > destination)


def mask_duplicates(data: pd.DataFrame, subset: Sequence[str]) -> pd.Series:
    """
    Construye la máscara de filas duplicadas conservando la primera ocurrencia.

    Emite
    -----
    No emite issues directamente.
    """
    # Se usa la semántica estándar de pandas: conservar la primera, eliminar las siguientes.
    return data.duplicated(subset=list(subset), keep="first")


def mask_categorical_values(
    data: pd.DataFrame,
    drop_map: Mapping[str, Sequence[Any]],
) -> pd.Series:
    """
    Construye la máscara de `categorical_values` combinando por OR todas las entradas del mapping.

    Emite
    -----
    No emite issues directamente.
    """
    # Se acumulan coincidencias por OR, tratando None como sentinel de drop de nulos/NaN.
    mask = pd.Series(False, index=data.index, dtype=bool)
    for field_name, banned_values in drop_map.items():
        series = data[field_name]
        non_null_values = [value for value in banned_values if value is not None]
        drop_nulls = any(value is None for value in banned_values)

        field_mask = pd.Series(False, index=data.index, dtype=bool)
        if non_null_values:
            field_mask = field_mask | series.isin(non_null_values)
        if drop_nulls:
            field_mask = field_mask | series.isna()
        mask = mask | field_mask
    return mask


def build_clean_summary(
    *,
    rows_in: int,
    rows_out: int,
    dropped_by_rule: Mapping[str, int],
) -> dict:
    """
    Construye el summary canónico y estable de `clean_trips`.

    Emite
    -----
    No emite issues directamente.
    """
    # Se preserva exactamente la familia rows_in/rows_out/dropped_total/dropped_by_rule cerrada para OP-04.
    summary_dropped_by_rule = {
        rule_name: int(dropped_by_rule.get(rule_name, 0)) for rule_name in _SUMMARY_RULE_KEYS
    }
    return {
        "rows_in": int(rows_in),
        "rows_out": int(rows_out),
        "dropped_total": int(rows_in) - int(rows_out),
        "dropped_by_rule": summary_dropped_by_rule,
    }


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


def _normalize_string_list_or_none(values: Optional[Sequence[str]]) -> Optional[List[str]]:
    """Normaliza una secuencia textual a lista única o devuelve None si queda inactiva."""
    if values is None:
        return None
    normalized = _deduplicate_preserving_order([str(value) for value in values])
    return normalized or None


def _normalize_categorical_drop_map_or_none(
    drop_map: Optional[Mapping[str, Sequence[Any]]],
) -> Optional[Dict[str, List[Any]]]:
    """Normaliza el mapping categórico a una forma JSON-safe y devuelve None si queda inactivo."""
    if not drop_map:
        return None

    normalized: Dict[str, List[Any]] = {}
    for field_name, banned_values in drop_map.items():
        seen: set[str] = set()
        values_out: List[Any] = []
        for raw_value in banned_values:
            value_norm = None if raw_value is None else _json_safe_scalar(raw_value)
            dedup_key = json.dumps(value_norm, sort_keys=True, ensure_ascii=False)
            if dedup_key in seen:
                continue
            seen.add(dedup_key)
            values_out.append(value_norm)
        normalized[str(field_name)] = values_out
    return normalized or None


def _extract_temporal_tier(temporal_meta: Any, data: pd.DataFrame) -> str:
    """Extrae el tier temporal desde metadata o lo infiere de forma conservadora desde las columnas."""
    if isinstance(temporal_meta, dict):
        tier = temporal_meta.get("tier")
        if isinstance(tier, str) and tier in {"tier_1", "tier_2", "tier_3"}:
            return tier
    if all(field_name in data.columns for field_name in _TIME_FIELDS):
        return "tier_1"
    if all(field_name in data.columns for field_name in ("origin_time_local_hhmm", "destination_time_local_hhmm")):
        return "tier_2"
    return "tier_3"


def _is_categorical_field(trips: TripDataset, field_name: str, series: pd.Series) -> bool:
    """Decide si un campo puede tratarse como categórico sin recalcular dominios ni schema_effective."""
    schema_fields = getattr(getattr(trips, "schema", None), "fields", {})
    field_spec = schema_fields.get(field_name) if isinstance(schema_fields, dict) else None
    if field_spec is not None:
        if getattr(field_spec, "dtype", None) == "categorical":
            return True
        if getattr(field_spec, "domain", None) is not None:
            return True

    metadata_domains = trips.metadata.get("domains_effective") if isinstance(trips.metadata, dict) else None
    if isinstance(metadata_domains, dict) and field_name in metadata_domains:
        return True

    schema_effective_domains = getattr(getattr(trips, "schema_effective", None), "domains_effective", None)
    if isinstance(schema_effective_domains, dict) and field_name in schema_effective_domains:
        return True

    return bool(ptypes.is_categorical_dtype(series))


def _deduplicate_preserving_order(values: Sequence[str]) -> List[str]:
    """Elimina duplicados preservando el orden original de la secuencia."""
    seen = set()
    out: List[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _sample_list(values: Sequence[Any], *, limit: int) -> List[Any]:
    """Toma una muestra JSON-safe pequeña de una secuencia para details de issues."""
    return [_json_safe_scalar(value) for value in list(values)[:limit]]


def _json_safe_scalar(value: Any) -> Any:
    """Normaliza un escalar a una forma JSON-friendly estable."""
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if pd.isna(value):
        return None
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    return str(value)


def _to_json_serializable_or_none(obj: Any) -> Any:
    """Convierte dict/list anidados a una forma JSON-safe sin hacer fallback silencioso."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return {str(key): _to_json_serializable_or_none(value) for key, value in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_json_serializable_or_none(value) for value in obj]
    if _json_is_serializable(obj):
        return obj
    return _json_safe_scalar(obj)


def _json_is_serializable(obj: Any) -> bool:
    """Chequea si un objeto puede serializarse directamente a JSON."""
    try:
        json.dumps(obj)
        return True
    except Exception:
        return False


def _is_valid_h3_value(value: Any) -> bool:
    """Valida un índice H3 textual de forma tolerante a nulos, vacíos y excepciones."""
    if value is None or pd.isna(value):
        return False
    try:
        value_text = str(value).strip()
        if value_text == "":
            return False
        return bool(h3.is_valid_cell(value_text))
    except Exception:
        return False


def _utc_now_iso() -> str:
    """Retorna timestamp UTC ISO-8601 para eventos del módulo."""
    return datetime.now(timezone.utc).isoformat()