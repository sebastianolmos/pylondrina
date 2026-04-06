# -------------------------
# file: pylondrina/transforms/flows.py
# -------------------------
from __future__ import annotations

import copy
import json
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Literal

import h3
import numpy as np
import pandas as pd
from pandas.api import types as ptypes

from pylondrina.datasets import FlowDataset, TripDataset
from pylondrina.errors import SchemaError, ValidationError
from pylondrina.issues.catalog_build_flows import BUILD_FLOWS_ISSUES
from pylondrina.issues.core import emit_and_maybe_raise, emit_issue
from pylondrina.reports import FlowBuildReport, Issue

EXCEPTION_MAP_BUILD = {
    "schema": SchemaError,
    "validate": ValidationError,
}

TimeAggregation = Literal["none", "hour", "day", "week"]
"""
Granularidad temporal para construir flujos.

- "none": no particiona temporalmente (un flujo por OD y segmentación).
- "hour"/"day"/"week": agrega además por “bin” temporal según `time_basis`.
"""

TimeBasis = Literal["origin", "destination"]
"""
Campo temporal base para ubicar el viaje en el bin cuando `time_aggregation != "none"`.

- "origin": usa `origin_time_utc` para asignar el viaje al bin.
- "destination": usa `destination_time_utc` para asignar el viaje al bin.
"""



_REQUIRED_OD_H3_FIELDS = ("origin_h3_index", "destination_h3_index")
_REQUIRED_TIER1_FIELDS = ("origin_time_utc", "destination_time_utc")


@dataclass(frozen=True)
class FlowBuildOptions:
    """
    Opciones para construir un FlowDataset a partir de un TripDataset.

    Parameters
    ----------
    h3_resolution : int, default=8
        Resolución H3 objetivo de agregación.
    group_by : sequence of str, optional
        Campos adicionales por los que se segmenta el flujo (p. ej. `mode`, `purpose`, etc.).
        Si es None, se agregan flujos solo por OD (y por tiempo si aplica).
    time_aggregation : {"none", "hour", "day", "week"}, default="none"
        Granularidad temporal de agregación. Si es None, no se agrega dimensión temporal.
    time_basis : {"origin", "destination"}, default="origin"
        Campo temporal base para construir la ventana temporal del flujo.
    min_trips_per_flow : int, default=1
        Umbral mínimo de movements para conservar un flujo.
    keep_flow_to_trips : bool, default=False
        Si True, construye la tabla auxiliar `flow_to_trips`.
    require_validated : bool, default=True
        Si True, exige `trips.metadata["is_validated"] is True`.
    strict : bool, default=False
        Reserva de política para degradaciones recuperables explícitas.
    max_issues : int, default=1000
        Guardarraíl del tamaño del reporte.
    """

    h3_resolution: int = 8
    group_by: Optional[Sequence[str]] = None
    time_aggregation: TimeAggregation = "none"
    time_basis: TimeBasis = "origin"
    min_trips_per_flow: int = 1
    keep_flow_to_trips: bool = False
    require_validated: bool = True
    strict: bool = False
    max_issues: int = 1000


# -----------------------------------------------------------------------------
# Función pública principal
# -----------------------------------------------------------------------------

def build_flows(
    trips: TripDataset,
    *,
    options: Optional[FlowBuildOptions] = None,
) -> Tuple[FlowDataset, FlowBuildReport]:
    """
    Construye un FlowDataset mínimo, estable y exportable desde un TripDataset.

    Parameters
    ----------
    trips : TripDataset
        Dataset de trips en formato Golondrina.
    options : FlowBuildOptions, optional
        Opciones efectivas de agregación. Si es None, se usan defaults.

    Returns
    -------
    tuple[FlowDataset, FlowBuildReport]
        Dataset de flujos derivado y reporte estructurado de la operación.
    """
    issues: List[Issue] = []

    # Se normaliza el request y se cierran las precondiciones fatales antes de tocar la agregación.
    options_eff, parameters = _resolve_and_precheck_build_request(trips, options)

    # Se prepara el subconjunto buildable y se deja explícita la evidencia de descartes.
    prepared_df, prep_issues, prep_info = _prepare_buildable_movements(trips, options_eff)
    issues.extend(prep_issues)

    # Se agregan los flujos usando el esquema canónico interno del FlowDataset.
    flows_df = _aggregate_flows(
        prepared_df,
        prep_info["effective_flow_keys"],
        options_eff,
    )

    # Se construye el auxiliar flow_to_trips solo si el usuario lo pidió explícitamente.
    flow_to_trips = _build_flow_to_trips(prepared_df, flows_df, options_eff)

    # Se construye el dataset derivado con metadata/provenance propias y sin copiar el historial de trips.
    flow_dataset = _build_flow_dataset(
        trips,
        flows_df,
        flow_to_trips,
        options_eff,
        prep_info,
    )

    # Se arma el reporte y el evento reproducible de build a partir de la evidencia acumulada.
    report, event = _build_flow_report_and_event(
        issues,
        options_eff,
        prep_info,
        flows_df,
        flow_to_trips,
    )

    # Se anexa el evento del pipeline propio de flows al dataset de salida.
    _ensure_events_list(flow_dataset.metadata).append(event)

    return flow_dataset, report


# -----------------------------------------------------------------------------
# Helpers internos principales del pipeline de la operación
# -----------------------------------------------------------------------------

def _resolve_and_precheck_build_request(
    trips: TripDataset,
    options: FlowBuildOptions | None,
) -> tuple[FlowBuildOptions, dict[str, Any]]:
    """
    Normaliza opciones y cierra precondiciones fatales del request.

    Emite
    -----
    - FLOW.INPUT.INVALID_TRIPDATASET
    - FLOW.INPUT.INVALID_DATA_SURFACE
    - FLOW.INPUT.NO_TRIPS
    - FLOW.VALIDATION.REQUIRED_NOT_VALIDATED
    - FLOW.CONFIG.INVALID_H3_RESOLUTION
    - FLOW.CONFIG.INVALID_TIME_AGGREGATION
    - FLOW.CONFIG.INVALID_TIME_BASIS
    - FLOW.CONFIG.INVALID_MIN_TRIPS_PER_FLOW
    - FLOW.CONFIG.INVALID_MAX_ISSUES
    - FLOW.CONFIG.GROUP_BY_INVALID_FIELDS
    - FLOW.BACKLINK.MOVEMENT_ID_REQUIRED
    - FLOW.TEMPORAL.TIER_NOT_SUPPORTED
    - FLOW.TEMPORAL.REQUIRED_TIME_FIELDS_MISSING
    """
    issues: List[Issue] = []

    if not isinstance(trips, TripDataset):
        # Se aborta porque sin TripDataset no existe contrato interpretable para la operación.
        emit_and_maybe_raise(
            issues,
            BUILD_FLOWS_ISSUES,
            "FLOW.INPUT.INVALID_TRIPDATASET",
            strict=False,
            exception_map=EXCEPTION_MAP_BUILD,
            default_exception=ValidationError,
            received_type=type(trips).__name__,
        )

    if not hasattr(trips, "data") or not isinstance(trips.data, pd.DataFrame):
        # Se aborta porque build_flows trabaja normativamente sobre TripDataset.data.
        emit_and_maybe_raise(
            issues,
            BUILD_FLOWS_ISSUES,
            "FLOW.INPUT.INVALID_DATA_SURFACE",
            strict=False,
            exception_map=EXCEPTION_MAP_BUILD,
            default_exception=ValidationError,
            data_type=type(getattr(trips, "data", None)).__name__,
            reason="data_not_dataframe",
        )

    if len(trips.data) == 0:
        # Se aborta porque no hay movements sobre los que agregar flujos.
        emit_and_maybe_raise(
            issues,
            BUILD_FLOWS_ISSUES,
            "FLOW.INPUT.NO_TRIPS",
            strict=False,
            exception_map=EXCEPTION_MAP_BUILD,
            default_exception=ValidationError,
            n_trips_in=0,
        )

    options_raw = options or FlowBuildOptions()
    group_by = None if options_raw.group_by is None else [str(name) for name in options_raw.group_by]
    time_aggregation = options_raw.time_aggregation or "none"
    time_basis = options_raw.time_basis or "origin"

    if not isinstance(options_raw.h3_resolution, int) or not (0 <= options_raw.h3_resolution <= 15):
        # Se aborta porque la resolución H3 objetivo debe ser interpretable y válida.
        emit_and_maybe_raise(
            issues,
            BUILD_FLOWS_ISSUES,
            "FLOW.CONFIG.INVALID_H3_RESOLUTION",
            strict=False,
            exception_map=EXCEPTION_MAP_BUILD,
            default_exception=SchemaError,
            h3_resolution_target=options_raw.h3_resolution,
        )

    if time_aggregation not in {"none", "hour", "day", "week"}:
        # Se aborta porque la granularidad temporal quedó cerrada por contrato v1.1.
        emit_and_maybe_raise(
            issues,
            BUILD_FLOWS_ISSUES,
            "FLOW.CONFIG.INVALID_TIME_AGGREGATION",
            strict=False,
            exception_map=EXCEPTION_MAP_BUILD,
            default_exception=SchemaError,
            time_aggregation=time_aggregation,
        )

    if time_basis not in {"origin", "destination"}:
        # Se aborta porque la base temporal debe ser origin o destination.
        emit_and_maybe_raise(
            issues,
            BUILD_FLOWS_ISSUES,
            "FLOW.CONFIG.INVALID_TIME_BASIS",
            strict=False,
            exception_map=EXCEPTION_MAP_BUILD,
            default_exception=SchemaError,
            time_basis=time_basis,
        )

    if not isinstance(options_raw.min_trips_per_flow, int) or options_raw.min_trips_per_flow <= 0:
        # Se aborta porque el umbral mínimo debe ser un entero positivo.
        emit_and_maybe_raise(
            issues,
            BUILD_FLOWS_ISSUES,
            "FLOW.CONFIG.INVALID_MIN_TRIPS_PER_FLOW",
            strict=False,
            exception_map=EXCEPTION_MAP_BUILD,
            default_exception=SchemaError,
            min_trips_per_flow=options_raw.min_trips_per_flow,
        )

    if not isinstance(options_raw.max_issues, int) or options_raw.max_issues <= 0:
        # Se aborta porque max_issues funciona como guardarraíl y debe ser válido.
        emit_and_maybe_raise(
            issues,
            BUILD_FLOWS_ISSUES,
            "FLOW.CONFIG.INVALID_MAX_ISSUES",
            strict=False,
            exception_map=EXCEPTION_MAP_BUILD,
            default_exception=SchemaError,
            max_issues=options_raw.max_issues,
        )

    missing_group_fields = [name for name in (group_by or []) if name not in trips.data.columns]
    if missing_group_fields:
        # Se aborta porque group_by no puede apuntar a campos inexistentes en el input.
        emit_and_maybe_raise(
            issues,
            BUILD_FLOWS_ISSUES,
            "FLOW.CONFIG.GROUP_BY_INVALID_FIELDS",
            strict=False,
            exception_map=EXCEPTION_MAP_BUILD,
            default_exception=SchemaError,
            group_by_invalid_fields=missing_group_fields,
            available_fields_sample=[str(col) for col in list(trips.data.columns)[:20]],
            available_fields_total=int(len(trips.data.columns)),
            n_trips_in=int(len(trips.data)),
            group_by=group_by,
        )

    if options_raw.require_validated and not _extract_validated_flag(getattr(trips, "metadata", None)):
        # Se aborta porque el contrato vigente exige trips validados cuando require_validated=True.
        emit_and_maybe_raise(
            issues,
            BUILD_FLOWS_ISSUES,
            "FLOW.VALIDATION.REQUIRED_NOT_VALIDATED",
            strict=False,
            exception_map=EXCEPTION_MAP_BUILD,
            default_exception=ValidationError,
            require_validated=True,
            validated_flag=_extract_validated_flag(getattr(trips, "metadata", None)),
        )

    if options_raw.keep_flow_to_trips and "movement_id" not in trips.data.columns:
        # Se aborta porque flow_to_trips requiere movement_id como clave mínima de backlink.
        emit_and_maybe_raise(
            issues,
            BUILD_FLOWS_ISSUES,
            "FLOW.BACKLINK.MOVEMENT_ID_REQUIRED",
            strict=False,
            exception_map=EXCEPTION_MAP_BUILD,
            default_exception=ValidationError,
        )

    if time_aggregation != "none":
        tier = _extract_temporal_tier(getattr(trips, "metadata", None))
        if tier != "tier_1":
            # Se aborta porque la agregación temporal solo quedó soportada para Tier 1.
            emit_and_maybe_raise(
                issues,
                BUILD_FLOWS_ISSUES,
                "FLOW.TEMPORAL.TIER_NOT_SUPPORTED",
                strict=False,
                exception_map=EXCEPTION_MAP_BUILD,
                default_exception=ValidationError,
                tier=tier,
                time_aggregation=time_aggregation,
            )

        missing_time_fields = [field for field in _REQUIRED_TIER1_FIELDS if field not in trips.data.columns]
        if missing_time_fields:
            # Se aborta porque sin los campos Tier 1 no se puede construir la dimensión temporal.
            emit_and_maybe_raise(
                issues,
                BUILD_FLOWS_ISSUES,
                "FLOW.TEMPORAL.REQUIRED_TIME_FIELDS_MISSING",
                strict=False,
                exception_map=EXCEPTION_MAP_BUILD,
                default_exception=ValidationError,
                missing_fields=missing_time_fields,
                available_fields_sample=[str(col) for col in list(trips.data.columns)[:20]],
                available_fields_total=int(len(trips.data.columns)),
                n_trips_in=int(len(trips.data)),
            )

    options_eff = FlowBuildOptions(
        h3_resolution=int(options_raw.h3_resolution),
        group_by=group_by,
        time_aggregation=time_aggregation,
        time_basis=time_basis,
        min_trips_per_flow=int(options_raw.min_trips_per_flow),
        keep_flow_to_trips=bool(options_raw.keep_flow_to_trips),
        require_validated=bool(options_raw.require_validated),
        strict=bool(options_raw.strict),
        max_issues=int(options_raw.max_issues),
    )

    parameters = {
        "h3_resolution": options_eff.h3_resolution,
        "group_by": list(options_eff.group_by) if options_eff.group_by is not None else None,
        "time_aggregation": options_eff.time_aggregation,
        "time_basis": options_eff.time_basis,
        "min_trips_per_flow": options_eff.min_trips_per_flow,
        "keep_flow_to_trips": options_eff.keep_flow_to_trips,
        "require_validated": options_eff.require_validated,
        "strict": options_eff.strict,
        "max_issues": options_eff.max_issues,
    }
    return options_eff, parameters


def _prepare_buildable_movements(
    trips: TripDataset,
    options: FlowBuildOptions,
) -> tuple[pd.DataFrame, list[Issue], dict[str, Any]]:
    """
    Prepara el subconjunto buildable y las claves efectivas de agregación.

    Emite
    -----
    - FLOW.INPUT.REQUIRED_FIELDS_MISSING
    - FLOW.AGG.H3_INVALID_OR_MIXED
    - FLOW.AGG.H3_RESOLUTION_TOO_FINE
    - FLOW.OUTPUT.MOVEMENTS_DROPPED_MISSING_OD_H3
    - FLOW.OUTPUT.NO_BUILDABLE_MOVEMENTS
    - FLOW.TEMPORAL.REQUIRED_TIME_FIELDS_MISSING
    """
    issues: List[Issue] = []
    work = trips.data.copy(deep=True)

    missing_h3_fields = [field for field in _REQUIRED_OD_H3_FIELDS if field not in work.columns]
    if missing_h3_fields:
        # Se aborta porque build_flows necesita H3 OD canónicos en el dataset de entrada.
        emit_and_maybe_raise(
            issues,
            BUILD_FLOWS_ISSUES,
            "FLOW.INPUT.REQUIRED_FIELDS_MISSING",
            strict=False,
            exception_map=EXCEPTION_MAP_BUILD,
            default_exception=ValidationError,
            missing_fields=missing_h3_fields,
            available_fields_sample=[str(col) for col in list(work.columns)[:20]],
            available_fields_total=int(len(work.columns)),
            n_trips_in=int(len(work)),
        )

    n_trips_in = int(len(work))

    origin_norm, origin_missing_mask, origin_invalid = _normalize_h3_series(work["origin_h3_index"])
    dest_norm, dest_missing_mask, dest_invalid = _normalize_h3_series(work["destination_h3_index"])
    invalid_values = origin_invalid + dest_invalid
    if invalid_values:
        # Se aborta porque índices H3 inválidos vuelven ambigua la agregación espacial.
        emit_and_maybe_raise(
            issues,
            BUILD_FLOWS_ISSUES,
            "FLOW.AGG.H3_INVALID_OR_MIXED",
            strict=False,
            exception_map=EXCEPTION_MAP_BUILD,
            default_exception=ValidationError,
            n_violations=int(len(invalid_values)),
            values_sample=invalid_values[:10],
            row_indices_sample=_collect_invalid_h3_row_indices(origin_invalid, dest_invalid)[:10],
        )

    work = work.copy()
    work["origin_h3_index"] = origin_norm
    work["destination_h3_index"] = dest_norm

    h3_resolution_input, mixed_h3_resolutions = _infer_h3_resolution_from_columns(
        work["origin_h3_index"],
        work["destination_h3_index"],
    )

    if mixed_h3_resolutions:
        # Se aborta porque v1.1 no permite mezclar resoluciones H3 en el input de agregación.
        emit_and_maybe_raise(
            issues,
            BUILD_FLOWS_ISSUES,
            "FLOW.AGG.H3_INVALID_OR_MIXED",
            strict=False,
            exception_map=EXCEPTION_MAP_BUILD,
            default_exception=ValidationError,
            h3_resolution_input=None,
            h3_resolution_target=options.h3_resolution,
            reason="mixed_h3_resolutions",
        )

    if h3_resolution_input is not None and options.h3_resolution > h3_resolution_input:
        # Se aborta porque v1.1 no permite refinar H3 desde build_flows.
        emit_and_maybe_raise(
            issues,
            BUILD_FLOWS_ISSUES,
            "FLOW.AGG.H3_RESOLUTION_TOO_FINE",
            strict=False,
            exception_map=EXCEPTION_MAP_BUILD,
            default_exception=ValidationError,
            h3_resolution_input=h3_resolution_input,
            h3_resolution_target=options.h3_resolution,
        )

    missing_od_mask = origin_missing_mask | dest_missing_mask
    n_trips_dropped_missing_od = int(missing_od_mask.sum())
    if n_trips_dropped_missing_od > 0:
        # Se deja evidencia agregada de los movements que no pueden entrar a un flujo OD completo.
        emit_issue(
            issues,
            BUILD_FLOWS_ISSUES,
            "FLOW.OUTPUT.MOVEMENTS_DROPPED_MISSING_OD_H3",
            row_count=n_trips_dropped_missing_od,
            n_trips_in=n_trips_in,
            n_trips_eligible=int((~missing_od_mask).sum()),
            n_trips_dropped=n_trips_dropped_missing_od,
            n_violations=n_trips_dropped_missing_od,
            row_indices_sample=_sample_indices_from_mask(missing_od_mask)[:10],
            values_sample=_sample_missing_od_values(work, missing_od_mask),
        )

    buildable_mask = ~missing_od_mask
    prepared = work.loc[buildable_mask].copy()
    n_trips_eligible = int(len(prepared))

    if n_trips_eligible == 0:
        # Se aborta porque ningún movement quedó con ambos H3 OD para agregarlo.
        emit_and_maybe_raise(
            issues,
            BUILD_FLOWS_ISSUES,
            "FLOW.OUTPUT.NO_BUILDABLE_MOVEMENTS",
            strict=False,
            exception_map=EXCEPTION_MAP_BUILD,
            default_exception=ValidationError,
            n_trips_in=n_trips_in,
            n_trips_eligible=0,
            n_trips_dropped=n_trips_dropped_missing_od,
        )

    if h3_resolution_input is not None and options.h3_resolution < h3_resolution_input:
        # Se hace roll-up explícito a la resolución padre porque el contrato sí lo permite.
        prepared["origin_h3_index"] = prepared["origin_h3_index"].map(
            lambda cell: h3.cell_to_parent(cell, options.h3_resolution)
        )
        prepared["destination_h3_index"] = prepared["destination_h3_index"].map(
            lambda cell: h3.cell_to_parent(cell, options.h3_resolution)
        )

    effective_flow_keys: List[str] = ["origin_h3_index", "destination_h3_index"]

    if options.time_aggregation != "none":
        time_field = "origin_time_utc" if options.time_basis == "origin" else "destination_time_utc"
        time_series = _coerce_datetime_series(prepared[time_field])
        if bool(time_series.isna().any()):
            # Se aborta porque la dimensión temporal quedó pedida pero no es usable de forma estable.
            emit_and_maybe_raise(
                issues,
                BUILD_FLOWS_ISSUES,
                "FLOW.TEMPORAL.REQUIRED_TIME_FIELDS_MISSING",
                strict=False,
                exception_map=EXCEPTION_MAP_BUILD,
                default_exception=ValidationError,
                missing_fields=[time_field],
                available_fields_sample=[str(col) for col in list(prepared.columns)[:20]],
                available_fields_total=int(len(prepared.columns)),
                n_trips_in=n_trips_in,
                reason="time_values_not_usable",
            )
        window_start = _make_window_start(time_series, options.time_aggregation)
        prepared["window_start_utc"] = window_start
        prepared["window_end_utc"] = _make_window_end(window_start, options.time_aggregation)
        effective_flow_keys.extend(["window_start_utc", "window_end_utc"])

    if options.group_by:
        effective_flow_keys.extend(list(options.group_by))

    prep_info = {
        "effective_flow_keys": effective_flow_keys,
        "n_trips_in": n_trips_in,
        "n_trips_eligible": n_trips_eligible,
        "n_trips_dropped": int(n_trips_in - n_trips_eligible),
        "h3_resolution_input": h3_resolution_input,
        "h3_resolution_target": options.h3_resolution,
    }
    return prepared, issues, prep_info


def _aggregate_flows(
    prepared_df: pd.DataFrame,
    effective_flow_keys: list[str],
    options: FlowBuildOptions,
) -> pd.DataFrame:
    """
    Agrega el working set buildable y arma la tabla canónica `flows`.

    Emite
    -----
    No emite issues directamente; la evidencia se construye en el reporte final.
    """
    if prepared_df.empty:
        base_columns = ["flow_id", *effective_flow_keys, "flow_count", "flow_value"]
        return pd.DataFrame(columns=base_columns)

    groupby_obj = prepared_df.groupby(effective_flow_keys, dropna=False, observed=True)
    flows_df = groupby_obj.size().rename("flow_count").reset_index()

    if "trip_weight" in prepared_df.columns:
        weight_series = pd.to_numeric(prepared_df["trip_weight"], errors="coerce").fillna(0.0)
        value_df = prepared_df[effective_flow_keys].copy()
        value_df["trip_weight"] = weight_series
        flow_value_df = (
            value_df.groupby(effective_flow_keys, dropna=False, observed=True)["trip_weight"]
            .sum()
            .reset_index(name="flow_value")
        )
        flows_df = flows_df.merge(flow_value_df, on=effective_flow_keys, how="left")
    else:
        flows_df["flow_value"] = flows_df["flow_count"].astype(float)

    flows_df = flows_df.loc[flows_df["flow_count"] >= int(options.min_trips_per_flow)].copy()
    flows_df = flows_df.sort_values(effective_flow_keys).reset_index(drop=True)
    flows_df.insert(0, "flow_id", _make_flow_ids(len(flows_df)))

    ordered_columns = ["flow_id", *effective_flow_keys, "flow_count", "flow_value"]
    return flows_df.loc[:, ordered_columns]


def _build_flow_to_trips(
    prepared_df: pd.DataFrame,
    flows_df: pd.DataFrame,
    options: FlowBuildOptions,
) -> pd.DataFrame | None:
    """
    Construye la tabla mínima `flow_to_trips` cuando el request la habilita.

    Emite
    -----
    No emite issues directamente; la precondición de `movement_id` se resuelve antes.
    """
    if not options.keep_flow_to_trips:
        return None

    if flows_df.empty:
        return pd.DataFrame(columns=["flow_id", "movement_id"])

    key_columns = [
        column
        for column in flows_df.columns
        if column in prepared_df.columns and column not in {"flow_id", "flow_count", "flow_value"}
    ]
    mapping_df = flows_df.loc[:, ["flow_id", *key_columns]].copy()
    joined = prepared_df.loc[:, ["movement_id", *key_columns]].merge(
        mapping_df,
        on=key_columns,
        how="inner",
    )
    return joined.loc[:, ["flow_id", "movement_id"]].reset_index(drop=True)


def _build_flow_dataset(
    trips: TripDataset,
    flows_df: pd.DataFrame,
    flow_to_trips: pd.DataFrame | None,
    options: FlowBuildOptions,
    prep_info: dict[str, Any],
) -> FlowDataset:
    """
    Construye el estado vivo del FlowDataset derivado.

    Emite
    -----
    No emite issues directamente; arma metadata/provenance/aggregation_spec del resultado.
    """
    effective_flow_keys = list(prep_info.get("effective_flow_keys", []))
    aggregation_spec = {
        "h3_resolution": int(options.h3_resolution),
        "group_by": list(options.group_by) if options.group_by is not None else [],
        "time_aggregation": options.time_aggregation,
        "time_basis": options.time_basis,
        "min_trips_per_flow": int(options.min_trips_per_flow),
        "keep_flow_to_trips": bool(options.keep_flow_to_trips),
        "require_validated": bool(options.require_validated),
        "strict": bool(options.strict),
        "max_issues": int(options.max_issues),
        "effective_flow_keys": effective_flow_keys,
    }

    source_dataset_id = None
    if isinstance(getattr(trips, "metadata", None), dict):
        source_dataset_id = trips.metadata.get("dataset_id")

    provenance = {
        "derived_from": [
            {
                "source_type": "trips",
                "dataset_id": source_dataset_id,
                "schema_version": getattr(trips, "schema_version", None),
                "n_rows": int(len(trips.data)),
            }
        ],
        "prior_events_summary": _summarize_prior_events(getattr(trips, "metadata", {}).get("events")),
    }

    metadata = {
        "dataset_id": f"flows_{uuid.uuid4().hex[:12]}",
        "artifact_id": None,
        "is_validated": False,
        "events": [],
        "h3": {"resolution": int(options.h3_resolution)},
    }

    return FlowDataset(
        flows=flows_df,
        flow_to_trips=flow_to_trips,
        aggregation_spec=aggregation_spec,
        source_trips=trips,
        metadata=metadata,
        provenance=provenance,
    )


def _build_flow_report_and_event(
    issues: list[Issue],
    options: FlowBuildOptions,
    prep_info: dict[str, Any],
    flows_df: pd.DataFrame,
    flow_to_trips: pd.DataFrame | None,
) -> tuple[FlowBuildReport, dict[str, Any]]:
    """
    Construye el FlowBuildReport y el evento `build_flows`.

    Emite
    -----
    - FLOW.OUTPUT.EMPTY_AFTER_THRESHOLD
    - FLOW.REPORT.ISSUES_TRUNCATED
    """
    issues_detected = list(issues)

    if int(prep_info.get("n_trips_eligible", 0)) > 0 and len(flows_df) == 0:
        # Se deja warning explícito cuando la agregación quedó vacía por el umbral mínimo.
        emit_issue(
            issues_detected,
            BUILD_FLOWS_ISSUES,
            "FLOW.OUTPUT.EMPTY_AFTER_THRESHOLD",
            min_trips_per_flow=options.min_trips_per_flow,
            n_trips_in=int(prep_info.get("n_trips_in", 0)),
            n_trips_eligible=int(prep_info.get("n_trips_eligible", 0)),
            n_flows_out=0,
        )

    issues_effective, limits_block = _truncate_build_issues(issues_detected, options.max_issues)

    summary = {
        "n_trips_in": int(prep_info.get("n_trips_in", 0)),
        "n_trips_eligible": int(prep_info.get("n_trips_eligible", 0)),
        "n_trips_dropped": int(prep_info.get("n_trips_dropped", 0)),
        "n_flows_out": int(len(flows_df)),
        "n_flow_to_trips_rows": None if flow_to_trips is None else int(len(flow_to_trips)),
    }
    if limits_block is not None:
        summary["limits"] = limits_block

    parameters = {
        "h3_resolution": int(options.h3_resolution),
        "group_by": list(options.group_by) if options.group_by is not None else None,
        "time_aggregation": options.time_aggregation,
        "time_basis": options.time_basis,
        "min_trips_per_flow": int(options.min_trips_per_flow),
        "keep_flow_to_trips": bool(options.keep_flow_to_trips),
        "require_validated": bool(options.require_validated),
        "strict": bool(options.strict),
        "max_issues": int(options.max_issues),
    }

    report = FlowBuildReport(
        ok=not any(issue.level == "error" for issue in issues_effective),
        issues=issues_effective,
        summary=summary,
        parameters=parameters,
        metadata={},
    )

    event = {
        "op": "build_flows",
        "ts_utc": _utc_now_iso(),
        "parameters": parameters,
        "summary": summary,
        "issues_summary": _build_issues_summary(issues_effective),
    }
    return report, event


# -----------------------------------------------------------------------------
# Helpers internos de uso general
# -----------------------------------------------------------------------------

def _normalize_h3_series(series: pd.Series) -> tuple[pd.Series, pd.Series, list[str]]:
    """Normaliza una serie H3 a strings o null y separa valores inválidos."""
    normalized_values: List[Optional[str]] = []
    missing_mask: List[bool] = []
    invalid_values: List[str] = []

    for value in series.tolist():
        value_norm = _normalize_h3_value(value)
        if value_norm is None:
            normalized_values.append(None)
            missing_mask.append(True)
            continue
        if not _is_valid_h3_value(value_norm):
            normalized_values.append(None)
            missing_mask.append(False)
            invalid_values.append(value_norm)
            continue
        normalized_values.append(value_norm)
        missing_mask.append(False)

    normalized = pd.Series(normalized_values, index=series.index, dtype="object")
    missing = pd.Series(missing_mask, index=series.index, dtype="bool")
    return normalized, missing, invalid_values


def _collect_invalid_h3_row_indices(origin_invalid: list[str], dest_invalid: list[str]) -> list[int]:
    """Retorna una muestra vacía porque el catálogo solo requiere que el índice exista si aporta."""
    return []


def _infer_h3_resolution_from_columns(origin_series: pd.Series, destination_series: pd.Series) -> tuple[Optional[int], bool]:
    """Infiera una resolución H3 desde las columnas OD y detecte mezcla de resoluciones."""
    resolutions = set()
    for series in (origin_series, destination_series):
        for value in series.dropna().astype(str).tolist():
            if _is_valid_h3_value(value):
                resolutions.add(int(h3.get_resolution(value)))
    if not resolutions:
        return None, False
    if len(resolutions) > 1:
        return None, True
    return int(next(iter(resolutions))), False


def _coerce_datetime_series(series: pd.Series) -> pd.Series:
    """Normaliza una serie temporal a pandas datetime UTC naive."""
    coerced = pd.to_datetime(series, errors="coerce", utc=True)
    if isinstance(coerced.dtype, pd.DatetimeTZDtype):
        return coerced.dt.tz_convert(None)
    return coerced


def _make_window_start(series_utc: pd.Series, time_aggregation: str) -> pd.Series:
    """Calcula el inicio de la ventana temporal del flujo."""
    if isinstance(series_utc, pd.DatetimeIndex):
        series_utc = pd.Series(series_utc)

    if time_aggregation == "hour":
        return series_utc.dt.floor("h")
    if time_aggregation == "day":
        return series_utc.dt.floor("D")
    if time_aggregation == "week":
        day_start = series_utc.dt.floor("D")
        return day_start - pd.to_timedelta(day_start.dt.weekday, unit="D")
    return series_utc


def _make_window_end(window_start: pd.Series, time_aggregation: str) -> pd.Series:
    """Calcula el fin de la ventana temporal del flujo."""
    if isinstance(window_start, pd.DatetimeIndex):
        window_start = pd.Series(window_start)

    if time_aggregation == "hour":
        return window_start + pd.Timedelta(hours=1)
    if time_aggregation == "day":
        return window_start + pd.Timedelta(days=1)
    if time_aggregation == "week":
        return window_start + pd.Timedelta(days=7)
    return window_start


def _make_flow_ids(n_rows: int) -> list[str]:
    """Genera identificadores estables y simples para las filas de flows."""
    return [f"flow_{idx:07d}" for idx in range(n_rows)]


def _extract_validated_flag(metadata: Any) -> bool:
    """Lee `metadata["is_validated"]` tolerando trazas antiguas del core."""
    if not isinstance(metadata, dict):
        return False
    if "is_validated" in metadata:
        return bool(metadata.get("is_validated", False))
    flags = metadata.get("flags", {})
    if isinstance(flags, dict):
        return bool(flags.get("validated", False))
    return False


def _extract_temporal_tier(metadata: Any) -> str:
    """Extrae el tier temporal del dataset, con fallback conservador a tier_3."""
    if not isinstance(metadata, dict):
        return "tier_3"
    temporal = metadata.get("temporal", {})
    if not isinstance(temporal, dict):
        return "tier_3"
    tier = temporal.get("tier", "tier_3")
    return str(tier)


def _summarize_prior_events(events: Any) -> list[dict[str, Any]] | None:
    """Construye un resumen mínimo y serializable de los eventos previos del dataset origen."""
    if not isinstance(events, list):
        return None
    summarized: list[dict[str, Any]] = []
    for event in events:
        if not isinstance(event, dict):
            continue
        summary_event = {
            "op": event.get("op"),
            "ts_utc": event.get("ts_utc"),
            "summary": _to_json_serializable_or_none(event.get("summary")),
        }
        summarized.append(summary_event)
    return summarized or None


def _sample_indices_from_mask(mask: pd.Series, limit: int = 10) -> list[int]:
    """Devuelve una muestra acotada de índices donde la máscara es verdadera."""
    return [int(idx) if isinstance(idx, (int, np.integer)) else idx for idx in mask[mask].index[:limit].tolist()]


def _sample_missing_od_values(df: pd.DataFrame, mask: pd.Series, limit: int = 10) -> list[dict[str, Any]]:
    """Devuelve una muestra compacta de pares OD faltantes para Issue.details."""
    sample_df = df.loc[mask, ["origin_h3_index", "destination_h3_index"]].head(limit)
    return [
        {
            "origin_h3_index": _json_safe_scalar(row.origin_h3_index),
            "destination_h3_index": _json_safe_scalar(row.destination_h3_index),
        }
        for row in sample_df.itertuples(index=False)
    ]


def _truncate_build_issues(issues_all: Sequence[Issue], max_issues: int) -> tuple[list[Issue], Optional[dict[str, Any]]]:
    """Aplica el guardarraíl max_issues y deja evidencia explícita del truncamiento."""
    total_detected = len(issues_all)
    if total_detected <= max_issues:
        return list(issues_all), None

    retained = list(issues_all[: max(max_issues - 1, 0)])
    # Se agrega un último issue explícito para que el truncamiento quede visible en el reporte.
    emit_issue(
        retained,
        BUILD_FLOWS_ISSUES,
        "FLOW.REPORT.ISSUES_TRUNCATED",
        max_issues=max_issues,
        n_issues_emitted=max_issues,
        n_issues_detected_total=total_detected,
    )
    limits = {
        "max_issues": int(max_issues),
        "issues_truncated": True,
        "n_issues_emitted": len(retained),
        "n_issues_detected_total": int(total_detected),
    }
    return retained, limits


def _ensure_events_list(metadata: dict[str, Any]) -> list[dict[str, Any]]:
    """Asegura que metadata['events'] exista como lista append-only."""
    if not isinstance(metadata.get("events"), list):
        metadata["events"] = []
    return metadata["events"]


def _build_issues_summary(issues: Sequence[Issue]) -> dict[str, Any]:
    """Resume issues por severidad y por code para el evento de build."""
    level_counts = Counter(issue.level for issue in issues)
    code_counts = Counter(issue.code for issue in issues)
    return {
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


def _to_json_serializable_or_none(obj: Any) -> Any:
    """Convierte dict/list anidados a una forma JSON-safe sin fallback silencioso complejo."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return {str(key): _to_json_serializable_or_none(value) for key, value in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_json_serializable_or_none(value) for value in obj]
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    if pd.isna(obj):
        return None
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


def _json_safe_scalar(value: Any) -> Any:
    """Normaliza un escalar a una forma JSON-friendly y estable."""
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    return str(value)


def _normalize_h3_value(value: Any) -> Optional[str]:
    """Normaliza un valor H3 a string o None."""
    if value is None or pd.isna(value):
        return None
    value_text = str(value).strip()
    if value_text == "":
        return None
    return value_text


def _is_valid_h3_value(value: str) -> bool:
    """Valida un índice H3 textual de forma tolerante a excepciones."""
    try:
        return bool(h3.is_valid_cell(value))
    except Exception:
        return False


def _utc_now_iso() -> str:
    """Retorna timestamp UTC ISO-8601 para eventos del módulo."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
