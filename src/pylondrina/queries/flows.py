from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import h3
import pandas as pd

from pylondrina.datasets import FlowDataset, TripDataset
from pylondrina.errors import PylondrinaError
from pylondrina.issues.catalog_trips_from_flows import GET_TRIPS_FROM_FLOWS_ISSUES
from pylondrina.issues.core import emit_and_maybe_raise, emit_issue
from pylondrina.reports import Issue, OperationReport


EXCEPTION_MAP_GET_TRIPS = {
    "type": PylondrinaError,
    "value": PylondrinaError,
}

_MIN_FLOW_FIELDS = ("flow_id",)
_MIN_FLOW_TO_TRIPS_FIELDS = ("flow_id", "movement_id")
_BASE_TRIP_FIELDS = ("movement_id", "origin_h3_index", "destination_h3_index")
_TEMPORAL_WINDOW_FIELDS = ("window_start_utc", "window_end_utc")
_SUPPORTED_TIME_AGGREGATIONS = {"none", "hour", "day", "week"}
_SUPPORTED_TIME_BASIS = {"origin", "destination"}
_FLOW_RESERVED_FIELDS = {"flow_id", "flow_count", "flow_value"}


# -----------------------------------------------------------------------------
# OP-13 no define dataclass de opciones; el contrato vigente usa solo `max_issues`.
# -----------------------------------------------------------------------------


def get_trips_from_flows(
    flows: FlowDataset,
    trips: Optional[TripDataset] = None,
    *,
    max_issues: int = 1000,
) -> Tuple[pd.DataFrame, OperationReport]:
    """
    Obtiene una tabla de correspondencia flujo-viajes desde un FlowDataset.

    Parameters
    ----------
    flows : FlowDataset
        Dataset de flujos Golondrina. Debe exponer `flows.flows` como DataFrame
        e incluir la columna `flow_id`.
    trips : TripDataset, optional
        Dataset de trips para reconstrucción cuando `flows.flow_to_trips` no es
        usable. Si es None, se intenta usar `flows.source_trips`.
    max_issues : int, default=1000
        Guardarraíl del tamaño del reporte.

    Returns
    -------
    tuple[pd.DataFrame, OperationReport]
        Tabla de correspondencia flujo-viajes y reporte estructurado.

    Notes
    -----
    - OP-13 es una query pura: no modifica metadata, no registra eventos y no
      muta los datasets de entrada.
    - La prioridad de fuentes es: `flows.flow_to_trips` -> `trips` -> `flows.source_trips`.
    - La unidad contractual mínima de salida es `flow_id + movement_id`.
    """
    issues_all: List[Issue] = []

    # ------------------------------------------------------------------
    # 1) Se validan precondiciones fatales del request antes de resolver fuentes.
    # ------------------------------------------------------------------
    if not isinstance(flows, FlowDataset):
        # Se aborta porque sin FlowDataset no hay superficie contractual interpretable.
        emit_and_maybe_raise(
            issues_all,
            GET_TRIPS_FROM_FLOWS_ISSUES,
            "GET_TRIPS_FROM_FLOWS.CONFIG.INVALID_FLOWS_INPUT",
            strict=False,
            exception_map=EXCEPTION_MAP_GET_TRIPS,
            default_exception=PylondrinaError,
            received_type=type(flows).__name__,
            has_flows_attr=hasattr(flows, "flows"),
            flows_attr_type=type(getattr(flows, "flows", None)).__name__,
        )

    if not hasattr(flows, "flows") or not isinstance(flows.flows, pd.DataFrame):
        # Se aborta porque OP-13 trabaja normativamente sobre `flows.flows` como DataFrame.
        emit_and_maybe_raise(
            issues_all,
            GET_TRIPS_FROM_FLOWS_ISSUES,
            "GET_TRIPS_FROM_FLOWS.CONFIG.INVALID_FLOWS_INPUT",
            strict=False,
            exception_map=EXCEPTION_MAP_GET_TRIPS,
            default_exception=PylondrinaError,
            received_type=type(flows).__name__,
            has_flows_attr=hasattr(flows, "flows"),
            flows_attr_type=type(getattr(flows, "flows", None)).__name__,
        )

    if not isinstance(max_issues, int) or max_issues <= 0:
        # Se aborta porque `max_issues` forma parte del contrato observable del reporte.
        emit_and_maybe_raise(
            issues_all,
            GET_TRIPS_FROM_FLOWS_ISSUES,
            "GET_TRIPS_FROM_FLOWS.PARAM.INVALID_MAX_ISSUES",
            strict=False,
            exception_map=EXCEPTION_MAP_GET_TRIPS,
            default_exception=PylondrinaError,
            value=max_issues,
        )

    flows_df = flows.flows
    missing_flow_fields = [field for field in _MIN_FLOW_FIELDS if field not in flows_df.columns]
    if missing_flow_fields:
        # Se aborta porque el FlowDataset no cumple el contrato mínimo de entrada.
        emit_and_maybe_raise(
            issues_all,
            GET_TRIPS_FROM_FLOWS_ISSUES,
            "GET_TRIPS_FROM_FLOWS.DATA.MISSING_FLOW_ID",
            strict=False,
            exception_map=EXCEPTION_MAP_GET_TRIPS,
            default_exception=PylondrinaError,
            max_issues=max_issues,
            n_flows_input=int(len(flows_df)),
            n_trips_input=None,
            used_source=None,
            reconstruction_attempted=False,
            available_fields_sample=_sample_list(list(flows_df.columns), limit=20),
            available_fields_total=int(len(flows_df.columns)),
        )

    request_ctx = {
        "max_issues": int(max_issues),
        "n_flows_input": int(len(flows_df)),
        "n_trips_input": None,
        "used_source": None,
        "reconstruction_attempted": False,
    }

    # ------------------------------------------------------------------
    # 2) Se resuelve la fuente efectiva y se fija el contexto del reporte.
    # ------------------------------------------------------------------
    used_source, source_obj, reconstruction_attempted, n_trips_input = _resolve_correspondence_source(
        flows,
        trips,
        issues=issues_all,
        request_ctx=request_ctx,
    )
    request_ctx["used_source"] = used_source
    request_ctx["reconstruction_attempted"] = bool(reconstruction_attempted)
    request_ctx["n_trips_input"] = n_trips_input

    # ------------------------------------------------------------------
    # 3) Se consume la tabla auxiliar o se reconstruye la correspondencia exacta.
    # ------------------------------------------------------------------
    if used_source == "flow_to_trips":
        provisional_df = _extract_correspondence_from_flow_to_trips(
            source_obj,
            issues=issues_all,
            request_ctx=request_ctx,
        )
        movement_universe = _unique_non_null_values(source_obj.get("movement_id")) if "movement_id" in source_obj.columns else []
        join_info = {
            "join_key_columns": ["flow_id", "movement_id"],
            "group_by": [],
            "window_columns": [],
        }
    else:
        provisional_df, movement_universe, join_info = _reconstruct_correspondence_from_trips(
            flows_df,
            source_obj,
            aggregation_spec=getattr(flows, "aggregation_spec", None),
            issues=issues_all,
            request_ctx=request_ctx,
        )

    # ------------------------------------------------------------------
    # 4) Se normaliza la salida final y se cuantifica la cobertura observable.
    # ------------------------------------------------------------------
    flow_trip_correspondence_df, summary = _finalize_flow_trip_correspondence(
        provisional_df,
        flows_df=flows_df,
        movement_universe=movement_universe,
        issues=issues_all,
        request_ctx=request_ctx,
        join_info=join_info,
    )

    # ------------------------------------------------------------------
    # 5) Se aplica el guardarraíl del reporte y se retorna sin side effects.
    # ------------------------------------------------------------------
    issues_effective, limits_block = _truncate_query_issues(
        issues_all,
        max_issues=max_issues,
    )
    if limits_block is not None:
        summary["limits"] = limits_block

    ok = not any(issue.level == "error" for issue in issues_effective)
    report = OperationReport(
        ok=ok,
        issues=issues_effective,
        summary=summary,
        parameters={
            "max_issues": int(max_issues),
            "used_source": used_source,
            "reconstruction_attempted": bool(reconstruction_attempted),
            "n_flows_input": int(len(flows_df)),
            "n_trips_input": n_trips_input,
        },
    )

    return flow_trip_correspondence_df, report


# -----------------------------------------------------------------------------
# Helpers internos principales del pipeline de la operación
# -----------------------------------------------------------------------------


def _resolve_correspondence_source(
    flows: FlowDataset,
    trips: TripDataset | None,
    *,
    issues: List[Issue],
    request_ctx: Dict[str, Any],
) -> tuple[str, Any, bool, int | None]:
    """
    Resuelve la fuente efectiva de correspondencia flujo-viajes.

    Emite
    -----
    - GET_TRIPS_FROM_FLOWS.SOURCE.PREFERRED_SOURCE_UNUSABLE
    - GET_TRIPS_FROM_FLOWS.SOURCE.NO_USABLE_SOURCE
    """
    checked_sources: List[str] = []
    source_failures: Dict[str, Dict[str, Any]] = {}

    preferred_source_unusable: str | None = None
    required_columns = list(_MIN_FLOW_TO_TRIPS_FIELDS)

    # Primero se intenta consumir la tabla auxiliar ya materializada en flows.
    checked_sources.append("flow_to_trips")
    flow_to_trips = getattr(flows, "flow_to_trips", None)
    if isinstance(flow_to_trips, pd.DataFrame):
        missing_columns = [field for field in _MIN_FLOW_TO_TRIPS_FIELDS if field not in flow_to_trips.columns]
        if not missing_columns:
            return "flow_to_trips", flow_to_trips, False, None
        preferred_source_unusable = "flow_to_trips"
        source_failures["flow_to_trips"] = {
            "reason": "missing_required_columns",
            "required_columns": list(_MIN_FLOW_TO_TRIPS_FIELDS),
            "missing_columns": missing_columns,
        }
    elif flow_to_trips is not None:
        preferred_source_unusable = "flow_to_trips"
        source_failures["flow_to_trips"] = {
            "reason": "not_dataframe",
            "required_columns": list(_MIN_FLOW_TO_TRIPS_FIELDS),
            "missing_columns": list(_MIN_FLOW_TO_TRIPS_FIELDS),
        }
    else:
        source_failures["flow_to_trips"] = {
            "reason": "missing",
            "required_columns": list(_MIN_FLOW_TO_TRIPS_FIELDS),
            "missing_columns": list(_MIN_FLOW_TO_TRIPS_FIELDS),
        }

    # Luego se intenta el argumento explícito `trips`, si fue entregado.
    checked_sources.append("trips_argument")
    if trips is not None:
        if isinstance(trips, TripDataset) and isinstance(getattr(trips, "data", None), pd.DataFrame):
            if preferred_source_unusable is not None:
                # Se deja warning porque hubo que degradar desde la fuente prioritaria.
                warn_ctx = dict(request_ctx)
                warn_ctx.update(
                    {
                        "preferred_source": preferred_source_unusable,
                        "used_source": "trips_argument",
                        "checked_sources": list(checked_sources),
                        "source_failures": _to_json_serializable_or_none(source_failures),
                        "required_columns": required_columns,
                        "missing_columns": source_failures.get(preferred_source_unusable, {}).get("missing_columns"),
                    }
                )
                emit_issue(
                    issues,
                    GET_TRIPS_FROM_FLOWS_ISSUES,
                    "GET_TRIPS_FROM_FLOWS.SOURCE.PREFERRED_SOURCE_UNUSABLE",
                    **warn_ctx,
                )
            return "trips_argument", trips, True, int(len(trips.data))
        preferred_source_unusable = "trips_argument"
        source_failures["trips_argument"] = {
            "reason": "invalid_tripdataset_input",
            "required_columns": list(_BASE_TRIP_FIELDS),
            "missing_columns": [],
        }
    else:
        source_failures["trips_argument"] = {
            "reason": "missing",
            "required_columns": list(_BASE_TRIP_FIELDS),
            "missing_columns": list(_BASE_TRIP_FIELDS),
        }

    # Finalmente se intenta la referencia viva en memoria que dejó el pipeline de flows.
    checked_sources.append("flows.source_trips")
    source_trips = getattr(flows, "source_trips", None)
    if isinstance(source_trips, TripDataset) and isinstance(getattr(source_trips, "data", None), pd.DataFrame):
        if preferred_source_unusable is not None:
            # Se deja warning porque hubo que caer a un fallback posterior.
            warn_ctx = dict(request_ctx)
            warn_ctx.update(
                {
                    "preferred_source": preferred_source_unusable,
                    "used_source": "flows.source_trips",
                    "checked_sources": list(checked_sources),
                    "source_failures": _to_json_serializable_or_none(source_failures),
                    "required_columns": required_columns,
                    "missing_columns": source_failures.get(preferred_source_unusable, {}).get("missing_columns"),
                }
            )
            emit_issue(
                issues,
                GET_TRIPS_FROM_FLOWS_ISSUES,
                "GET_TRIPS_FROM_FLOWS.SOURCE.PREFERRED_SOURCE_UNUSABLE",
                **warn_ctx,
            )
        return "flows.source_trips", source_trips, True, int(len(source_trips.data))

    if source_trips is not None:
        source_failures["flows.source_trips"] = {
            "reason": "invalid_source_trips_reference",
            "required_columns": list(_BASE_TRIP_FIELDS),
            "missing_columns": [],
        }
    else:
        source_failures["flows.source_trips"] = {
            "reason": "missing",
            "required_columns": list(_BASE_TRIP_FIELDS),
            "missing_columns": list(_BASE_TRIP_FIELDS),
        }

    # Se aborta porque no quedó ninguna fuente usable para construir la tabla de salida.
    emit_and_maybe_raise(
        issues,
        GET_TRIPS_FROM_FLOWS_ISSUES,
        "GET_TRIPS_FROM_FLOWS.SOURCE.NO_USABLE_SOURCE",
        strict=False,
        exception_map=EXCEPTION_MAP_GET_TRIPS,
        default_exception=PylondrinaError,
        **request_ctx,
        preferred_source="flow_to_trips",
        checked_sources=list(checked_sources),
        source_failures=_to_json_serializable_or_none(source_failures),
        required_columns=list(_MIN_FLOW_TO_TRIPS_FIELDS),
        missing_columns=None,
    )
    raise AssertionError("Unreachable after NO_USABLE_SOURCE")


def _extract_correspondence_from_flow_to_trips(
    flow_to_trips_df: pd.DataFrame,
    *,
    issues: List[Issue],
    request_ctx: Dict[str, Any],
) -> pd.DataFrame:
    """
    Consume `flows.flow_to_trips` como fuente directa de correspondencia.

    Emite
    -----
    - GET_TRIPS_FROM_FLOWS.SOURCE.DUPLICATE_PAIRS_NORMALIZED
    """
    # Se toma solo la estructura contractual mínima del auxiliar directo.
    provisional = flow_to_trips_df.loc[:, list(_MIN_FLOW_TO_TRIPS_FIELDS)].copy()

    duplicated_mask = provisional.duplicated(subset=list(_MIN_FLOW_TO_TRIPS_FIELDS), keep="first")
    n_duplicate_pairs = int(duplicated_mask.sum())
    if n_duplicate_pairs > 0:
        # Se deja warning porque el auxiliar venía con pares exactos repetidos y se normalizó.
        emit_issue(
            issues,
            GET_TRIPS_FROM_FLOWS_ISSUES,
            "GET_TRIPS_FROM_FLOWS.SOURCE.DUPLICATE_PAIRS_NORMALIZED",
            **request_ctx,
            source="flow_to_trips",
            n_rows_in=int(len(provisional)),
            n_rows_out=int(len(provisional) - n_duplicate_pairs),
            n_duplicate_pairs=n_duplicate_pairs,
        )
        provisional = provisional.loc[~duplicated_mask].copy()

    return provisional.reset_index(drop=True)


def _reconstruct_correspondence_from_trips(
    flows_df: pd.DataFrame,
    trips: TripDataset,
    *,
    aggregation_spec: Any,
    issues: List[Issue],
    request_ctx: Dict[str, Any],
) -> tuple[pd.DataFrame, list[Any], dict[str, Any]]:
    """
    Reconstruye la correspondencia flujo-viajes desde un TripDataset.

    Emite
    -----
    - GET_TRIPS_FROM_FLOWS.RECON.MISSING_REQUIRED_COLUMNS
    - GET_TRIPS_FROM_FLOWS.RECON.AGGREGATION_KEYS_UNRECOVERABLE
    """
    source_name = str(request_ctx.get("used_source"))
    trips_df = trips.data

    # Se resuelven las llaves efectivas de agregación que deben reproducirse exactamente.
    join_info = _resolve_reconstruction_join_info(
        flows_df,
        aggregation_spec=aggregation_spec,
        issues=issues,
        request_ctx=request_ctx,
    )
    join_key_columns = list(join_info["join_key_columns"])
    group_by = list(join_info["group_by"])
    window_columns = list(join_info["window_columns"])
    h3_resolution_target = join_info.get("h3_resolution_target")
    time_aggregation = str(join_info.get("time_aggregation", "none"))
    time_basis = str(join_info.get("time_basis", "origin"))

    required_trip_columns = list(_BASE_TRIP_FIELDS)
    required_trip_columns.extend([field for field in group_by if field not in required_trip_columns])
    if window_columns:
        time_field = "origin_time_utc" if time_basis == "origin" else "destination_time_utc"
        if time_field not in required_trip_columns:
            required_trip_columns.append(time_field)

    missing_trip_columns = [field for field in required_trip_columns if field not in trips_df.columns]
    if missing_trip_columns:
        # Se aborta porque sin esas columnas no se puede reproducir la llave de agregación.
        emit_and_maybe_raise(
            issues,
            GET_TRIPS_FROM_FLOWS_ISSUES,
            "GET_TRIPS_FROM_FLOWS.RECON.MISSING_REQUIRED_COLUMNS",
            strict=False,
            exception_map=EXCEPTION_MAP_GET_TRIPS,
            default_exception=PylondrinaError,
            **request_ctx,
            source=source_name,
            required_columns=required_trip_columns,
            missing_columns=missing_trip_columns,
            join_key_columns=join_key_columns,
            group_by=group_by,
            window_columns=window_columns,
        )

    # Se prepara una copia de trabajo para no mutar `trips.data` durante la reconstrucción.
    work_columns = list(dict.fromkeys(required_trip_columns + (["trip_id"] if "trip_id" in trips_df.columns else [])))
    work = trips_df.loc[:, work_columns].copy()

    # Se replica el roll-up H3 solo cuando la resolución del flujo es más gruesa que la de trips.
    work = _apply_h3_rollup_if_needed(
        work,
        target_resolution=h3_resolution_target,
        issues=issues,
        request_ctx=request_ctx,
    )

    # Se replica la ventana temporal efectiva solo cuando el FlowDataset es temporal.
    if window_columns:
        time_field = "origin_time_utc" if time_basis == "origin" else "destination_time_utc"
        time_series = _coerce_datetime_series(work[time_field])
        work["window_start_utc"] = _make_window_start(time_series, time_aggregation)
        work["window_end_utc"] = _make_window_end(work["window_start_utc"], time_aggregation)

    missing_join_fields_in_work = [field for field in join_key_columns if field not in work.columns]
    if missing_join_fields_in_work:
        # Se aborta porque ni siquiera la copia de trabajo pudo materializar las llaves de join.
        emit_and_maybe_raise(
            issues,
            GET_TRIPS_FROM_FLOWS_ISSUES,
            "GET_TRIPS_FROM_FLOWS.RECON.AGGREGATION_KEYS_UNRECOVERABLE",
            strict=False,
            exception_map=EXCEPTION_MAP_GET_TRIPS,
            default_exception=PylondrinaError,
            **request_ctx,
            source=source_name,
            aggregation_spec_keys_present=list(aggregation_spec.keys()) if isinstance(aggregation_spec, dict) else None,
            group_by=group_by,
            window_columns=window_columns,
            join_key_columns=join_key_columns,
            reason="join_keys_not_materializable",
        )

    mapping_df = flows_df.loc[:, ["flow_id", *join_key_columns]].copy()
    duplicate_flow_keys = mapping_df.duplicated(subset=join_key_columns, keep=False)
    if bool(duplicate_flow_keys.any()):
        # Se aborta porque el join sería ambiguo: varias filas de flows comparten la misma llave efectiva.
        emit_and_maybe_raise(
            issues,
            GET_TRIPS_FROM_FLOWS_ISSUES,
            "GET_TRIPS_FROM_FLOWS.RECON.AGGREGATION_KEYS_UNRECOVERABLE",
            strict=False,
            exception_map=EXCEPTION_MAP_GET_TRIPS,
            default_exception=PylondrinaError,
            **request_ctx,
            source=source_name,
            aggregation_spec_keys_present=list(aggregation_spec.keys()) if isinstance(aggregation_spec, dict) else None,
            group_by=group_by,
            window_columns=window_columns,
            join_key_columns=join_key_columns,
            reason="non_unique_flow_join_keys",
        )

    trip_columns_out = ["movement_id", *join_key_columns]
    if "trip_id" in work.columns:
        trip_columns_out.append("trip_id")

    joined = work.loc[:, trip_columns_out].merge(
        mapping_df,
        on=join_key_columns,
        how="inner",
    )

    ordered_columns = ["flow_id", "movement_id"]
    if "trip_id" in joined.columns:
        ordered_columns.append("trip_id")

    provisional = joined.loc[:, ordered_columns].reset_index(drop=True)
    movement_universe = _unique_non_null_values(work["movement_id"])
    return provisional, movement_universe, join_info


def _finalize_flow_trip_correspondence(
    provisional_df: pd.DataFrame,
    *,
    flows_df: pd.DataFrame,
    movement_universe: Sequence[Any],
    issues: List[Issue],
    request_ctx: Dict[str, Any],
    join_info: Dict[str, Any],
) -> tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Normaliza la tabla final y calcula la cobertura observable de la query.

    Emite
    -----
    - GET_TRIPS_FROM_FLOWS.OUTPUT.PARTIAL_COVERAGE
    - GET_TRIPS_FROM_FLOWS.OUTPUT.EMPTY_RESULT
    """
    valid_flow_ids = set(_unique_non_null_values(flows_df["flow_id"]))
    correspondence = provisional_df.copy()

    # Se restringe la salida a los flow_id vigentes del dataset consultado.
    if "flow_id" in correspondence.columns:
        correspondence = correspondence.loc[correspondence["flow_id"].isin(valid_flow_ids)].copy()

    # Se garantizan columnas mínimas y se remueven filas obviamente inválidas.
    keep_columns = ["flow_id", "movement_id"]
    if "trip_id" in correspondence.columns:
        keep_columns.append("trip_id")
    correspondence = correspondence.loc[:, [col for col in keep_columns if col in correspondence.columns]].copy()
    correspondence = correspondence.loc[
        correspondence["flow_id"].notna() & correspondence["movement_id"].notna()
    ].copy()

    # Se aplica la normalización final de pares exactos y orden estable contractual.
    correspondence = correspondence.drop_duplicates(subset=["flow_id", "movement_id"], keep="first")
    correspondence = _safe_sort_correspondence_df(correspondence)

    matched_flow_ids = set(_unique_non_null_values(correspondence["flow_id"]))
    matched_movement_ids = set(_unique_non_null_values(correspondence["movement_id"]))
    all_movement_ids = set(_unique_non_null_values(movement_universe))

    n_rows_out = int(len(correspondence))
    n_unique_flows_out = int(len(matched_flow_ids))
    n_unique_movements_out = int(len(matched_movement_ids))
    n_unmatched_flows = int(max(len(valid_flow_ids) - n_unique_flows_out, 0))
    n_unmatched_movements = int(max(len(all_movement_ids) - n_unique_movements_out, 0))

    example_values = {
        "flow_id_sample_unmatched": _sample_set_difference(valid_flow_ids, matched_flow_ids, limit=10),
        "movement_id_sample_unmatched": _sample_set_difference(all_movement_ids, matched_movement_ids, limit=10),
    }

    if n_rows_out == 0:
        # Se deja warning porque la operación fue interpretable, pero la salida quedó vacía.
        emit_issue(
            issues,
            GET_TRIPS_FROM_FLOWS_ISSUES,
            "GET_TRIPS_FROM_FLOWS.OUTPUT.EMPTY_RESULT",
            **request_ctx,
            source=request_ctx.get("used_source"),
            join_key_columns=list(join_info.get("join_key_columns", [])),
            group_by=list(join_info.get("group_by", [])),
            window_columns=list(join_info.get("window_columns", [])),
            n_rows_out=n_rows_out,
            n_unmatched_movements=n_unmatched_movements,
            n_unmatched_flows=n_unmatched_flows,
            example_values=_to_json_serializable_or_none(example_values),
        )
    elif n_unmatched_movements > 0 or n_unmatched_flows > 0:
        # Se deja warning porque la correspondencia quedó parcial respecto del universo observado.
        emit_issue(
            issues,
            GET_TRIPS_FROM_FLOWS_ISSUES,
            "GET_TRIPS_FROM_FLOWS.OUTPUT.PARTIAL_COVERAGE",
            **request_ctx,
            source=request_ctx.get("used_source"),
            join_key_columns=list(join_info.get("join_key_columns", [])),
            group_by=list(join_info.get("group_by", [])),
            window_columns=list(join_info.get("window_columns", [])),
            n_rows_out=n_rows_out,
            n_unmatched_movements=n_unmatched_movements,
            n_unmatched_flows=n_unmatched_flows,
            example_values=_to_json_serializable_or_none(example_values),
        )

    summary = {
        "n_rows_out": n_rows_out,
        "n_unique_flows_out": n_unique_flows_out,
        "n_unique_movements_out": n_unique_movements_out,
        "n_unmatched_flows": n_unmatched_flows,
        "n_unmatched_movements": n_unmatched_movements,
    }
    return correspondence.reset_index(drop=True), summary


# -----------------------------------------------------------------------------
# Helpers internos de uso general
# -----------------------------------------------------------------------------


def _resolve_reconstruction_join_info(
    flows_df: pd.DataFrame,
    *,
    aggregation_spec: Any,
    issues: List[Issue],
    request_ctx: Dict[str, Any],
) -> Dict[str, Any]:
    """Resuelve las llaves efectivas de agregación requeridas para reconstrucción."""
    if not isinstance(aggregation_spec, dict):
        # Se aborta porque sin aggregation_spec no hay forma fiable de reconstruir la llave efectiva.
        emit_and_maybe_raise(
            issues,
            GET_TRIPS_FROM_FLOWS_ISSUES,
            "GET_TRIPS_FROM_FLOWS.RECON.AGGREGATION_KEYS_UNRECOVERABLE",
            strict=False,
            exception_map=EXCEPTION_MAP_GET_TRIPS,
            default_exception=PylondrinaError,
            **request_ctx,
            source=request_ctx.get("used_source"),
            aggregation_spec_keys_present=None,
            group_by=None,
            window_columns=None,
            join_key_columns=None,
            reason="aggregation_spec_not_dict",
        )

    aggregation_spec_keys = list(aggregation_spec.keys())
    group_by_raw = aggregation_spec.get("group_by", [])
    if group_by_raw is None:
        group_by = []
    elif isinstance(group_by_raw, (list, tuple)):
        group_by = [str(value) for value in group_by_raw]
    else:
        # Se aborta porque `group_by` debe ser una lista interpretable y serializable.
        emit_and_maybe_raise(
            issues,
            GET_TRIPS_FROM_FLOWS_ISSUES,
            "GET_TRIPS_FROM_FLOWS.RECON.AGGREGATION_KEYS_UNRECOVERABLE",
            strict=False,
            exception_map=EXCEPTION_MAP_GET_TRIPS,
            default_exception=PylondrinaError,
            **request_ctx,
            source=request_ctx.get("used_source"),
            aggregation_spec_keys_present=aggregation_spec_keys,
            group_by=_to_json_serializable_or_none(group_by_raw),
            window_columns=None,
            join_key_columns=None,
            reason="group_by_not_sequence",
        )

    time_aggregation = str(aggregation_spec.get("time_aggregation", "none") or "none")
    time_basis = str(aggregation_spec.get("time_basis", "origin") or "origin")
    if time_aggregation not in _SUPPORTED_TIME_AGGREGATIONS or time_basis not in _SUPPORTED_TIME_BASIS:
        # Se aborta porque la dimensión temporal del flujo no es interpretable bajo el contrato vigente.
        emit_and_maybe_raise(
            issues,
            GET_TRIPS_FROM_FLOWS_ISSUES,
            "GET_TRIPS_FROM_FLOWS.RECON.AGGREGATION_KEYS_UNRECOVERABLE",
            strict=False,
            exception_map=EXCEPTION_MAP_GET_TRIPS,
            default_exception=PylondrinaError,
            **request_ctx,
            source=request_ctx.get("used_source"),
            aggregation_spec_keys_present=aggregation_spec_keys,
            group_by=group_by,
            window_columns=None,
            join_key_columns=None,
            reason="invalid_time_configuration",
        )

    effective_keys_raw = aggregation_spec.get("effective_flow_keys")
    derived_keys = ["origin_h3_index", "destination_h3_index"]
    window_columns: List[str] = []
    if time_aggregation != "none":
        window_columns = list(_TEMPORAL_WINDOW_FIELDS)
        derived_keys.extend(window_columns)
    derived_keys.extend(group_by)

    if effective_keys_raw is None:
        join_key_columns = list(derived_keys)
    elif isinstance(effective_keys_raw, (list, tuple)) and len(effective_keys_raw) > 0:
        join_key_columns = [str(value) for value in effective_keys_raw]
    else:
        # Se aborta porque `effective_flow_keys` existe pero no es interpretable.
        emit_and_maybe_raise(
            issues,
            GET_TRIPS_FROM_FLOWS_ISSUES,
            "GET_TRIPS_FROM_FLOWS.RECON.AGGREGATION_KEYS_UNRECOVERABLE",
            strict=False,
            exception_map=EXCEPTION_MAP_GET_TRIPS,
            default_exception=PylondrinaError,
            **request_ctx,
            source=request_ctx.get("used_source"),
            aggregation_spec_keys_present=aggregation_spec_keys,
            group_by=group_by,
            window_columns=window_columns,
            join_key_columns=_to_json_serializable_or_none(effective_keys_raw),
            reason="effective_flow_keys_not_sequence",
        )

    missing_join_fields = [field for field in join_key_columns if field not in flows_df.columns]
    if missing_join_fields:
        # Se aborta porque `flows.flows` no expone todas las llaves requeridas para el join exacto.
        emit_and_maybe_raise(
            issues,
            GET_TRIPS_FROM_FLOWS_ISSUES,
            "GET_TRIPS_FROM_FLOWS.RECON.AGGREGATION_KEYS_UNRECOVERABLE",
            strict=False,
            exception_map=EXCEPTION_MAP_GET_TRIPS,
            default_exception=PylondrinaError,
            **request_ctx,
            source=request_ctx.get("used_source"),
            aggregation_spec_keys_present=aggregation_spec_keys,
            group_by=group_by,
            window_columns=window_columns,
            join_key_columns=join_key_columns,
            reason="missing_join_fields_in_flows",
        )

    return {
        "join_key_columns": join_key_columns,
        "group_by": group_by,
        "window_columns": window_columns,
        "time_aggregation": time_aggregation,
        "time_basis": time_basis,
        "h3_resolution_target": aggregation_spec.get("h3_resolution"),
    }


def _apply_h3_rollup_if_needed(
    trips_df: pd.DataFrame,
    *,
    target_resolution: Any,
    issues: List[Issue],
    request_ctx: Dict[str, Any],
) -> pd.DataFrame:
    """Replica el roll-up H3 mínimo necesario para igualar la resolución del flujo."""
    if target_resolution is None:
        return trips_df
    if not isinstance(target_resolution, int) or not (0 <= target_resolution <= 15):
        # Se aborta porque la resolución H3 objetivo no es interpretable.
        emit_and_maybe_raise(
            issues,
            GET_TRIPS_FROM_FLOWS_ISSUES,
            "GET_TRIPS_FROM_FLOWS.RECON.AGGREGATION_KEYS_UNRECOVERABLE",
            strict=False,
            exception_map=EXCEPTION_MAP_GET_TRIPS,
            default_exception=PylondrinaError,
            **request_ctx,
            source=request_ctx.get("used_source"),
            aggregation_spec_keys_present=["h3_resolution"],
            group_by=None,
            window_columns=None,
            join_key_columns=["origin_h3_index", "destination_h3_index"],
            reason="invalid_h3_resolution_target",
        )

    input_resolution, mixed_resolution = _infer_pair_h3_resolution(
        trips_df["origin_h3_index"],
        trips_df["destination_h3_index"],
    )
    if mixed_resolution:
        # Se aborta porque la reconstrucción exacta no puede apoyarse en resoluciones H3 mezcladas.
        emit_and_maybe_raise(
            issues,
            GET_TRIPS_FROM_FLOWS_ISSUES,
            "GET_TRIPS_FROM_FLOWS.RECON.AGGREGATION_KEYS_UNRECOVERABLE",
            strict=False,
            exception_map=EXCEPTION_MAP_GET_TRIPS,
            default_exception=PylondrinaError,
            **request_ctx,
            source=request_ctx.get("used_source"),
            aggregation_spec_keys_present=["h3_resolution"],
            group_by=None,
            window_columns=None,
            join_key_columns=["origin_h3_index", "destination_h3_index"],
            reason="mixed_input_h3_resolution",
        )

    if input_resolution is not None and target_resolution > input_resolution:
        # Se aborta porque OP-13 no recalcula celdas H3 más finas que las del dataset de trips.
        emit_and_maybe_raise(
            issues,
            GET_TRIPS_FROM_FLOWS_ISSUES,
            "GET_TRIPS_FROM_FLOWS.RECON.AGGREGATION_KEYS_UNRECOVERABLE",
            strict=False,
            exception_map=EXCEPTION_MAP_GET_TRIPS,
            default_exception=PylondrinaError,
            **request_ctx,
            source=request_ctx.get("used_source"),
            aggregation_spec_keys_present=["h3_resolution"],
            group_by=None,
            window_columns=None,
            join_key_columns=["origin_h3_index", "destination_h3_index"],
            reason="target_h3_resolution_finer_than_input",
        )

    if input_resolution is None or target_resolution == input_resolution:
        return trips_df

    work = trips_df.copy()
    work["origin_h3_index"] = work["origin_h3_index"].map(
        lambda value: _rollup_h3_value(value, target_resolution)
    )
    work["destination_h3_index"] = work["destination_h3_index"].map(
        lambda value: _rollup_h3_value(value, target_resolution)
    )
    return work


def _truncate_query_issues(
    issues_all: Sequence[Issue],
    *,
    max_issues: int,
) -> tuple[list[Issue], dict[str, Any] | None]:
    """Aplica el guardarraíl `max_issues` y deja evidencia explícita del truncamiento."""
    total_detected = len(issues_all)
    if total_detected <= max_issues:
        return list(issues_all), None

    retained = list(issues_all[: max(max_issues - 1, 0)])
    # Se agrega un issue final para que el truncamiento no quede implícito.
    emit_issue(
        retained,
        GET_TRIPS_FROM_FLOWS_ISSUES,
        "GET_TRIPS_FROM_FLOWS.REPORT.ISSUES_TRUNCATED",
        max_issues=max_issues,
        n_issues_emitted=max_issues,
        n_issues_detected_total=total_detected,
    )
    limits = {
        "max_issues": int(max_issues),
        "issues_truncated": True,
        "n_issues_emitted": int(len(retained)),
        "n_issues_detected_total": int(total_detected),
    }
    return retained, limits


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


def _infer_pair_h3_resolution(
    origin_series: pd.Series,
    destination_series: pd.Series,
) -> tuple[int | None, bool]:
    """Infiera la resolución H3 de trips y detecta mezcla de resoluciones."""
    resolutions = set()
    for series in (origin_series, destination_series):
        for value in series.dropna().tolist():
            value_text = _normalize_h3_value(value)
            if value_text is not None and _is_valid_h3_value(value_text):
                resolutions.add(int(h3.get_resolution(value_text)))
    if not resolutions:
        return None, False
    if len(resolutions) > 1:
        return None, True
    return int(next(iter(resolutions))), False


def _rollup_h3_value(value: Any, target_resolution: int) -> Any:
    """Hace roll-up H3 de un valor individual cuando corresponde."""
    value_text = _normalize_h3_value(value)
    if value_text is None or not _is_valid_h3_value(value_text):
        return value
    current_resolution = int(h3.get_resolution(value_text))
    if target_resolution >= current_resolution:
        return value_text
    return h3.cell_to_parent(value_text, target_resolution)


def _safe_sort_correspondence_df(df: pd.DataFrame) -> pd.DataFrame:
    """Ordena la tabla final de manera estable sin forzar cast permanente de IDs."""
    if df.empty:
        return df.reset_index(drop=True)

    work = df.copy()
    work["__flow_sort__"] = work["flow_id"].map(lambda value: "" if value is None else str(value))
    work["__movement_sort__"] = work["movement_id"].map(lambda value: "" if value is None else str(value))
    work = work.sort_values(["__flow_sort__", "__movement_sort__", "flow_id", "movement_id"], kind="stable")
    work = work.drop(columns=["__flow_sort__", "__movement_sort__"])
    return work.reset_index(drop=True)


def _unique_non_null_values(values: Iterable[Any]) -> list[Any]:
    """Devuelve valores únicos no nulos preservando el primer orden de aparición."""
    seen = set()
    ordered: list[Any] = []
    for value in values:
        if pd.isna(value):
            continue
        marker = _hashable_marker(value)
        if marker in seen:
            continue
        seen.add(marker)
        ordered.append(value)
    return ordered


def _sample_set_difference(left: Iterable[Any], right: Iterable[Any], *, limit: int = 10) -> list[Any]:
    """Muestra una diferencia de conjuntos en una forma compacta y JSON-friendly."""
    right_markers = {_hashable_marker(value) for value in right}
    sample: list[Any] = []
    for value in left:
        if _hashable_marker(value) in right_markers:
            continue
        sample.append(_json_safe_scalar(value))
        if len(sample) >= limit:
            break
    return sample


def _sample_list(values: Sequence[Any], *, limit: int = 20) -> list[Any]:
    """Devuelve una muestra compacta de una secuencia para Issue.details."""
    return [_json_safe_scalar(value) for value in list(values)[:limit]]


def _hashable_marker(value: Any) -> tuple[str, Any]:
    """Normaliza un valor a una clave hashable y estable para comparaciones internas."""
    if isinstance(value, (str, int, float, bool)):
        return (type(value).__name__, value)
    if isinstance(value, (pd.Timestamp, datetime)):
        return ("datetime", value.isoformat())
    return (type(value).__name__, str(value))


def _to_json_serializable_or_none(obj: Any) -> Any:
    """Convierte dict/list anidados a una forma JSON-safe sin fallback silencioso complejo."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return {str(key): _to_json_serializable_or_none(value) for key, value in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_to_json_serializable_or_none(value) for value in obj]
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    if pd.isna(obj):
        return None
    return _json_safe_scalar(obj)


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
    """Normaliza un índice H3 a string o None."""
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
