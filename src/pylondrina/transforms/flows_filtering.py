from __future__ import annotations

import copy
import json
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

import h3
import pandas as pd
from pandas.api import types as ptypes

from pylondrina.datasets import FlowDataset
from pylondrina.errors import FilterError
from pylondrina.issues.catalog_filter_flows import FILTER_FLOWS_ISSUES
from pylondrina.issues.core import emit_and_maybe_raise, emit_issue
from pylondrina.reports import Issue, OperationReport
from pylondrina.transforms.filtering import SpatialPredicate, WhereClause


WhereOp = str
WhereValue = Union[Any, Sequence[Any], Mapping[WhereOp, Any]]

EXCEPTION_MAP_FILTER = {
    "filter": FilterError,
}

_ALLOWED_SPATIAL_PREDICATES = {"origin", "destination", "both", "either"}
_ALLOWED_WHERE_OPS = {
    "eq",
    "ne",
    "in",
    "not_in",
    "is_null",
    "not_null",
    "gt",
    "gte",
    "lt",
    "lte",
    "between",
}
_ALLOWED_WHERE_OPS_BY_DTYPE = {
    "string": {"eq", "ne", "in", "not_in", "is_null", "not_null"},
    "categorical": {"eq", "ne", "in", "not_in", "is_null", "not_null"},
    "int": {"eq", "ne", "in", "not_in", "is_null", "not_null", "gt", "gte", "lt", "lte", "between"},
    "float": {"eq", "ne", "in", "not_in", "is_null", "not_null", "gt", "gte", "lt", "lte", "between"},
    "datetime": {"eq", "ne", "in", "not_in", "is_null", "not_null", "gt", "gte", "lt", "lte", "between"},
    "bool": {"eq", "ne", "is_null", "not_null"},
}
_CANONICAL_MIN_COLUMNS = (
    "flow_id",
    "origin_h3_index",
    "destination_h3_index",
    "flow_count",
    "flow_value",
)
_SUMMARY_FILTER_KEYS = ("where", "h3_cells")
_H3_FIELDS = ("origin_h3_index", "destination_h3_index")
_DEFAULT_SAMPLE_ROWS_REMOVED = 10


@dataclass(frozen=True)
class FlowFilterOptions:
    """
    Opciones para filtrar un FlowDataset por criterios declarativos (`where`) y/o por celdas H3.

    Attributes
    ----------
    where : Optional[WhereClause]
        Filtro declarativo por columnas de `FlowDataset.flows`.
    h3_cells : Optional[Iterable[str]]
        Conjunto de celdas H3 para el eje espacial.
    spatial_predicate : SpatialPredicate
        Sobre qué extremo(s) del flujo se evalúa `h3_cells`.
    keep_flow_to_trips : bool
        Si True, intenta sincronizar `flow_to_trips` con los `flow_id` retenidos.
    keep_metadata : bool
        Si True, agrega evento `filter_flows` en `metadata["events"]` del resultado.
    strict : bool
        Si True, errores recuperables por eje escalan después de construir evidencia.
    """

    where: Optional[WhereClause] = None
    h3_cells: Optional[Iterable[str]] = None
    spatial_predicate: SpatialPredicate = "origin"
    keep_flow_to_trips: bool = True
    keep_metadata: bool = True
    strict: bool = False


def filter_flows(
    flows: FlowDataset,
    *,
    options: Optional[FlowFilterOptions] = None,
    max_issues: int = 1000,
) -> Tuple[FlowDataset, OperationReport]:
    """
    Filtra un FlowDataset combinando criterios por atributos (`where`) y/o por celdas H3.

    Parameters
    ----------
    flows:
        Dataset de flujos de entrada.
    options:
        Request declarativo del filtrado. Si es None, se usan defaults efectivos.
    max_issues:
        Límite máximo de issues retenidos en el reporte final.

    Returns
    -------
    (FlowDataset, OperationReport)
        Nuevo dataset filtrado y reporte estructurado de la operación.
    """
    issues_all: List[Issue] = []

    # ------------------------------------------------------------------
    # 1) Se normaliza el request efectivo y se resuelven abortos fatales.
    # ------------------------------------------------------------------
    options_eff, parameters, filters_requested, request_ctx = _normalize_filter_flows_request(
        flows,
        options=options,
        max_issues=max_issues,
        issues=issues_all,
    )

    flows_df = flows.flows

    # ------------------------------------------------------------------
    # 2) Se evalúan los ejes de filtrado sin materializar subsets todavía.
    # ------------------------------------------------------------------
    where_mask, where_info = _evaluate_where_mask_on_flows_df(
        flows_df,
        where=options_eff.where,
        issues=issues_all,
        request_ctx=request_ctx,
    )
    h3_mask, h3_info = _evaluate_h3_mask_on_flows_df(
        flows_df,
        h3_cells=options_eff.h3_cells,
        spatial_predicate=options_eff.spatial_predicate,
        issues=issues_all,
        request_ctx=request_ctx,
    )

    filters_applied: List[str] = []
    filters_omitted: List[str] = []
    if bool(where_info.get("applied", False)):
        filters_applied.append("where")
    elif options_eff.where is not None:
        filters_omitted.append("where")

    if bool(h3_info.get("applied", False)):
        filters_applied.append("h3_cells")
    elif options_eff.h3_cells is not None:
        filters_omitted.append("h3_cells")

    # ------------------------------------------------------------------
    # 3) Se combinan las máscaras con AND global y se cuantifica el efecto.
    # ------------------------------------------------------------------
    rows_in = int(len(flows_df))
    survivor_mask = pd.Series(True, index=flows_df.index, dtype=bool)
    dropped_by_filter = {filter_name: 0 for filter_name in _SUMMARY_FILTER_KEYS}

    if where_mask is not None:
        current_index = survivor_mask[survivor_mask].index
        current_mask = where_mask.loc[current_index].fillna(False).astype(bool)
        dropped_now = int((~current_mask).sum())
        dropped_by_filter["where"] = dropped_now
        if dropped_now > 0:
            survivor_mask.loc[current_index[~current_mask]] = False

        removed_evidence = _build_removed_rows_evidence(
            flows_df.loc[current_index],
            ~current_mask,
            value_fields=list(where_info.get("fields_evaluated", [])),
        )
        # Se deja evidencia agregada del eje where con su descarte incremental real.
        emit_issue(
            issues_all,
            FILTER_FLOWS_ISSUES,
            "FLT_FLOW.WHERE.APPLIED",
            row_count=dropped_now,
            **request_ctx,
            n_flows_in=int(len(current_index)),
            n_flows_out=int(current_mask.sum()),
            rows_in=int(len(current_index)),
            rows_out=int(current_mask.sum()),
            dropped_total=dropped_now,
            filters_requested=list(filters_requested),
            filters_applied=list(filters_applied),
            filters_omitted=list(filters_omitted),
            dropped_by_filter=int(dropped_now),
            flow_id_sample_removed=removed_evidence.get("flow_id_sample_removed"),
            rows_sample_removed=removed_evidence.get("rows_sample_removed"),
            fields_evaluated=list(where_info.get("fields_evaluated", [])),
            rules_evaluated=int(where_info.get("rules_evaluated", 0)),
        )

    if h3_mask is not None:
        current_index = survivor_mask[survivor_mask].index
        current_mask = h3_mask.loc[current_index].fillna(False).astype(bool)
        dropped_now = int((~current_mask).sum())
        dropped_by_filter["h3_cells"] = dropped_now
        if dropped_now > 0:
            survivor_mask.loc[current_index[~current_mask]] = False

        removed_evidence = _build_removed_rows_evidence(
            flows_df.loc[current_index],
            ~current_mask,
            value_fields=list(_H3_FIELDS),
        )
        # Se deja evidencia agregada del eje H3 con su descarte incremental real.
        emit_issue(
            issues_all,
            FILTER_FLOWS_ISSUES,
            "FLT_FLOW.H3.APPLIED",
            row_count=dropped_now,
            **request_ctx,
            n_flows_in=int(len(current_index)),
            n_flows_out=int(current_mask.sum()),
            rows_in=int(len(current_index)),
            rows_out=int(current_mask.sum()),
            dropped_total=dropped_now,
            filters_requested=list(filters_requested),
            filters_applied=list(filters_applied),
            filters_omitted=list(filters_omitted),
            dropped_by_filter=int(dropped_now),
            flow_id_sample_removed=removed_evidence.get("flow_id_sample_removed"),
            rows_sample_removed=removed_evidence.get("rows_sample_removed"),
            h3_cells_count=int(h3_info.get("valid_cells_count", 0)),
        )

    rows_out = int(survivor_mask.sum())
    dropped_total = int(rows_in - rows_out)

    if not filters_requested:
        # Se informa que la operación derivó un nuevo dataset sin filtros efectivos.
        emit_issue(
            issues_all,
            FILTER_FLOWS_ISSUES,
            "FLT_FLOW.NO_CHANGES.NO_FILTERS_DEFINED",
            **request_ctx,
            n_flows_in=rows_in,
            n_flows_out=rows_out,
            rows_in=rows_in,
            rows_out=rows_out,
            dropped_total=dropped_total,
            filters_requested=list(filters_requested),
            filters_applied=list(filters_applied),
            filters_omitted=list(filters_omitted),
        )
    elif dropped_total == 0:
        # Se informa que los filtros se evaluaron, pero no generaron cambios efectivos.
        emit_issue(
            issues_all,
            FILTER_FLOWS_ISSUES,
            "FLT_FLOW.NO_CHANGES.FILTER_WITHOUT_EFFECT",
            **request_ctx,
            n_flows_in=rows_in,
            n_flows_out=rows_out,
            rows_in=rows_in,
            rows_out=rows_out,
            dropped_total=dropped_total,
            filters_requested=list(filters_requested),
            filters_applied=list(filters_applied),
            filters_omitted=list(filters_omitted),
        )

    # ------------------------------------------------------------------
    # 4) Se materializa el subset final y se resuelven auxiliares/trazabilidad.
    # ------------------------------------------------------------------
    filtered_flows_df = flows_df.loc[survivor_mask].copy(deep=True)
    kept_flow_ids = set(filtered_flows_df["flow_id"].tolist()) if "flow_id" in filtered_flows_df.columns else set()

    filtered_flow_to_trips, flow_to_trips_status = _resolve_filtered_flow_to_trips(
        flows.flow_to_trips,
        kept_flow_ids=kept_flow_ids,
        keep_flow_to_trips=bool(options_eff.keep_flow_to_trips),
        issues=issues_all,
        request_ctx={
            **request_ctx,
            "n_flows_in": rows_in,
            "n_flows_out": rows_out,
            "rows_in": rows_in,
            "rows_out": rows_out,
            "dropped_total": dropped_total,
        },
    )

    if filters_requested and rows_out == 0:
        # Se deja warning porque el resultado vacío es válido, pero importante de interpretar.
        emit_issue(
            issues_all,
            FILTER_FLOWS_ISSUES,
            "FLT_FLOW.RESULT.EMPTY_DATASET",
            **request_ctx,
            n_flows_in=rows_in,
            n_flows_out=rows_out,
            rows_in=rows_in,
            rows_out=rows_out,
            dropped_total=dropped_total,
            filters_requested=list(filters_requested),
            filters_applied=list(filters_applied),
            filters_omitted=list(filters_omitted),
            dropped_by_filter={key: int(value) for key, value in dropped_by_filter.items()},
            flow_to_trips_status=flow_to_trips_status,
            row_count=rows_in,
        )

    metadata_out = _build_metadata_out(getattr(flows, "metadata", {}), keep_metadata=bool(options_eff.keep_metadata))
    metadata_out["is_validated"] = _extract_validated_flag(getattr(flows, "metadata", {}))
    provenance_out = _build_derived_flow_provenance(flows)

    filtered_dataset = FlowDataset(
        flows=filtered_flows_df,
        flow_to_trips=filtered_flow_to_trips,
        aggregation_spec=copy.deepcopy(getattr(flows, "aggregation_spec", {})),
        source_trips=getattr(flows, "source_trips", None),
        metadata=metadata_out,
        provenance=provenance_out,
    )

    # ------------------------------------------------------------------
    # 5) Se construyen el summary, el reporte y el payload del evento.
    # ------------------------------------------------------------------
    issues_effective, limits_block = _truncate_issues_with_limit(
        issues_all,
        max_issues=max_issues,
    )
    summary = _build_filter_flows_summary(
        rows_in=rows_in,
        rows_out=rows_out,
        dropped_by_filter=dropped_by_filter,
        filters_requested=filters_requested,
        filters_applied=filters_applied,
        filters_omitted=filters_omitted,
        flow_to_trips_status=flow_to_trips_status,
        limits=limits_block,
    )
    issues_summary = _build_issues_summary(issues_effective)

    event_payload = {
        "op": "filter_flows",
        "ts_utc": _utc_now_iso(),
        "parameters": parameters,
        "summary": summary,
        "issues_summary": issues_summary,
    }

    # ------------------------------------------------------------------
    # 6) Se registra el evento si corresponde y recién después se resuelve strict.
    # ------------------------------------------------------------------
    if bool(options_eff.keep_metadata):
        try:
            _ensure_events_list(filtered_dataset.metadata).append(event_payload)
        except Exception as exc:
            # Se degrada el fallo de append del evento para no perder el dataset filtrado.
            emit_issue(
                issues_all,
                FILTER_FLOWS_ISSUES,
                "FLT_FLOW.EVENT.APPEND_FAILED",
                **request_ctx,
                reason="event_append_failed",
                recovered=True,
                recovery_action="return_dataset_without_event_append",
                exception_type=type(exc).__name__,
            )

    # Si el append del evento agregó issues, se recompone el reporte final.
    issues_effective, limits_block = _truncate_issues_with_limit(
        issues_all,
        max_issues=max_issues,
    )
    summary = _build_filter_flows_summary(
        rows_in=rows_in,
        rows_out=rows_out,
        dropped_by_filter=dropped_by_filter,
        filters_requested=filters_requested,
        filters_applied=filters_applied,
        filters_omitted=filters_omitted,
        flow_to_trips_status=flow_to_trips_status,
        limits=limits_block,
    )
    ok = not any(issue.level == "error" for issue in issues_effective)
    report = OperationReport(
        ok=ok,
        issues=issues_effective,
        summary=summary,
        parameters=parameters,
    )

    if bool(options_eff.strict) and not ok:
        error_issue = next((issue for issue in issues_effective if issue.level == "error"), None)
        raise FilterError(
            "filter_flows detectó errores de datos y strict=True exige abortar.",
            code=error_issue.code if error_issue is not None else None,
            details=error_issue.details if error_issue is not None else None,
            issue=error_issue,
            issues=issues_effective,
        )

    return filtered_dataset, report


def _build_filter_flows_summary(
    *,
    rows_in: int,
    rows_out: int,
    dropped_by_filter: Mapping[str, int],
    filters_requested: Sequence[str],
    filters_applied: Sequence[str],
    filters_omitted: Sequence[str],
    flow_to_trips_status: str,
    limits: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Construye el summary canónico y estable de `filter_flows`.

    Emite
    ------
    No emite issues directamente.
    """
    summary = {
        "rows_in": int(rows_in),
        "rows_out": int(rows_out),
        "dropped_total": int(rows_in) - int(rows_out),
        "dropped_by_filter": {
            filter_name: int(dropped_by_filter.get(filter_name, 0))
            for filter_name in _SUMMARY_FILTER_KEYS
        },
        "filters_requested": list(filters_requested),
        "filters_applied": list(filters_applied),
        "filters_omitted": list(filters_omitted),
        "flow_to_trips_status": str(flow_to_trips_status),
    }
    if limits is not None:
        summary["limits"] = limits
    return summary


# -----------------------------------------------------------------------------
# Helpers internos principales del pipeline de la operación
# -----------------------------------------------------------------------------


def _normalize_filter_flows_request(
    flows: Any,
    *,
    options: Optional[FlowFilterOptions],
    max_issues: int,
    issues: List[Issue],
) -> Tuple[FlowFilterOptions, Dict[str, Any], List[str], Dict[str, Any]]:
    """
    Normaliza el request efectivo y resuelve abortos fatales de configuración.

    Emite
    ------
    FLT_FLOW.CONFIG.INVALID_FLOWS_OBJECT
    FLT_FLOW.CONFIG.MISSING_FLOWS_DATAFRAME
    FLT_FLOW.CONTRACT.MISSING_CANONICAL_COLUMNS
    FLT_FLOW.CONFIG.INVALID_OPTIONS_OBJECT
    FLT_FLOW.CONFIG.INVALID_MAX_ISSUES
    FLT_FLOW.CONFIG.WHERE_NOT_INTERPRETABLE
    FLT_FLOW.CONFIG.INVALID_H3_CELLS
    FLT_FLOW.CONFIG.H3_CELLS_EMPTY_AFTER_NORMALIZATION
    FLT_FLOW.CONFIG.INVALID_SPATIAL_PREDICATE
    FLT_FLOW.CONFIG.NON_SERIALIZABLE_PARAMETER
    """
    # Se asegura temprano que el input sea realmente un FlowDataset usable.
    if not isinstance(flows, FlowDataset):
        # Se aborta porque sin FlowDataset no existe superficie contractual interpretable.
        emit_and_maybe_raise(
            issues,
            FILTER_FLOWS_ISSUES,
            "FLT_FLOW.CONFIG.INVALID_FLOWS_OBJECT",
            strict=False,
            exception_map=EXCEPTION_MAP_FILTER,
            default_exception=FilterError,
            received_type=type(flows).__name__,
        )

    # Se fija la superficie normativa de trabajo: FlowDataset.flows.
    if not hasattr(flows, "flows") or not isinstance(flows.flows, pd.DataFrame):
        # Se aborta porque OP-12 siempre opera sobre flows.flows como DataFrame.
        emit_and_maybe_raise(
            issues,
            FILTER_FLOWS_ISSUES,
            "FLT_FLOW.CONFIG.MISSING_FLOWS_DATAFRAME",
            strict=False,
            exception_map=EXCEPTION_MAP_FILTER,
            default_exception=FilterError,
            received_type=type(getattr(flows, "flows", None)).__name__,
        )

    flows_df = flows.flows

    missing_canonical = [field_name for field_name in _CANONICAL_MIN_COLUMNS if field_name not in flows_df.columns]
    if missing_canonical:
        # Se aborta porque el contrato canónico mínimo de flujos no está presente.
        emit_and_maybe_raise(
            issues,
            FILTER_FLOWS_ISSUES,
            "FLT_FLOW.CONTRACT.MISSING_CANONICAL_COLUMNS",
            strict=False,
            exception_map=EXCEPTION_MAP_FILTER,
            default_exception=FilterError,
            missing_columns=missing_canonical,
            available_columns_sample=_sample_list(list(flows_df.columns), limit=20),
            available_columns_total=len(flows_df.columns),
        )

    # Se valida que `options` tenga forma interpretable antes de construir parámetros.
    if options is not None and not isinstance(options, FlowFilterOptions):
        # Se aborta porque el request no puede normalizarse sin un FlowFilterOptions válido.
        emit_and_maybe_raise(
            issues,
            FILTER_FLOWS_ISSUES,
            "FLT_FLOW.CONFIG.INVALID_OPTIONS_OBJECT",
            strict=False,
            exception_map=EXCEPTION_MAP_FILTER,
            default_exception=FilterError,
            received_type=type(options).__name__,
        )

    # Se valida el guardarraíl de issues como entero positivo.
    if not isinstance(max_issues, int) or max_issues <= 0:
        # Se aborta porque el límite de issues forma parte del contrato de reporte.
        emit_and_maybe_raise(
            issues,
            FILTER_FLOWS_ISSUES,
            "FLT_FLOW.CONFIG.INVALID_MAX_ISSUES",
            strict=False,
            exception_map=EXCEPTION_MAP_FILTER,
            default_exception=FilterError,
            max_issues=max_issues,
        )

    options_eff = options or FlowFilterOptions()

    if str(options_eff.spatial_predicate) not in _ALLOWED_SPATIAL_PREDICATES:
        # Se aborta porque el predicado espacial no pertenece al contrato público vigente.
        emit_and_maybe_raise(
            issues,
            FILTER_FLOWS_ISSUES,
            "FLT_FLOW.CONFIG.INVALID_SPATIAL_PREDICATE",
            strict=False,
            exception_map=EXCEPTION_MAP_FILTER,
            default_exception=FilterError,
            spatial_predicate=options_eff.spatial_predicate,
        )

    # Se valida sólo la legibilidad top-level de `where`; la semántica fina se resuelve por eje.
    if options_eff.where is not None and not isinstance(options_eff.where, Mapping):
        # Se aborta porque `where` debe ser un mapping campo -> cláusula.
        emit_and_maybe_raise(
            issues,
            FILTER_FLOWS_ISSUES,
            "FLT_FLOW.CONFIG.WHERE_NOT_INTERPRETABLE",
            strict=False,
            exception_map=EXCEPTION_MAP_FILTER,
            default_exception=FilterError,
            where_repr=repr(options_eff.where)[:200],
            received_type=type(options_eff.where).__name__,
        )

    h3_cells_normalized: Optional[List[str]] = None
    if options_eff.h3_cells is not None:
        raw_h3 = options_eff.h3_cells
        if isinstance(raw_h3, (str, bytes)) or not isinstance(raw_h3, Iterable):
            # Se aborta porque `h3_cells` debe ser un iterable real de celdas saneables.
            emit_and_maybe_raise(
                issues,
                FILTER_FLOWS_ISSUES,
                "FLT_FLOW.CONFIG.INVALID_H3_CELLS",
                strict=False,
                exception_map=EXCEPTION_MAP_FILTER,
                default_exception=FilterError,
                received_type=type(raw_h3).__name__,
                invalid_items_sample=[repr(raw_h3)[:120]],
            )

        normalized_items: List[str] = []
        invalid_items_sample: List[str] = []
        for raw_value in raw_h3:
            normalized = _normalize_h3_value(raw_value)
            if normalized is None:
                if len(invalid_items_sample) < 10:
                    invalid_items_sample.append(repr(raw_value)[:120])
                continue
            normalized_items.append(normalized)

        h3_cells_normalized = sorted(set(normalized_items))
        if len(h3_cells_normalized) == 0:
            # Se aborta porque el eje espacial no puede construirse con una whitelist vacía.
            emit_and_maybe_raise(
                issues,
                FILTER_FLOWS_ISSUES,
                "FLT_FLOW.CONFIG.H3_CELLS_EMPTY_AFTER_NORMALIZATION",
                strict=False,
                exception_map=EXCEPTION_MAP_FILTER,
                default_exception=FilterError,
                h3_cells_count_raw=len(normalized_items) + len(invalid_items_sample),
                h3_cells_count_normalized=0,
            )

    options_eff = FlowFilterOptions(
        where=options_eff.where,
        h3_cells=h3_cells_normalized,
        spatial_predicate=str(options_eff.spatial_predicate),
        keep_flow_to_trips=bool(options_eff.keep_flow_to_trips),
        keep_metadata=bool(options_eff.keep_metadata),
        strict=bool(options_eff.strict),
    )

    metadata = getattr(flows, "metadata", {}) if isinstance(getattr(flows, "metadata", {}), dict) else {}
    request_ctx = {
        "dataset_id": metadata.get("dataset_id"),
        "artifact_id": metadata.get("artifact_id"),
        "strict": bool(options_eff.strict),
        "where_provided": options_eff.where is not None,
        "h3_cells_provided": options_eff.h3_cells is not None,
        "spatial_predicate": options_eff.spatial_predicate,
        "keep_flow_to_trips": bool(options_eff.keep_flow_to_trips),
        "keep_metadata": bool(options_eff.keep_metadata),
        "max_issues": int(max_issues),
    }

    parameters = {
        "where": _normalize_where_for_parameters(options_eff.where) if options_eff.where is not None else None,
        "h3_cells": list(options_eff.h3_cells) if options_eff.h3_cells is not None else None,
        "spatial_predicate": options_eff.spatial_predicate,
        "keep_flow_to_trips": bool(options_eff.keep_flow_to_trips),
        "keep_metadata": bool(options_eff.keep_metadata),
        "strict": bool(options_eff.strict),
        "max_issues": int(max_issues),
    }

    for option_name, option_value in parameters.items():
        if not _json_is_serializable(option_value):
            # Se aborta porque los parámetros efectivos forman parte del contrato serializable.
            emit_and_maybe_raise(
                issues,
                FILTER_FLOWS_ISSUES,
                "FLT_FLOW.CONFIG.NON_SERIALIZABLE_PARAMETER",
                strict=False,
                exception_map=EXCEPTION_MAP_FILTER,
                default_exception=FilterError,
                option_name=str(option_name),
                value_repr=repr(option_value)[:200],
                **request_ctx,
            )

    filters_requested: List[str] = []
    if options_eff.where is not None:
        filters_requested.append("where")
    if options_eff.h3_cells is not None:
        filters_requested.append("h3_cells")

    return options_eff, parameters, filters_requested, request_ctx


def _evaluate_where_mask_on_flows_df(
    flows_df: pd.DataFrame,
    *,
    where: Optional[WhereClause],
    issues: List[Issue],
    request_ctx: Mapping[str, Any],
) -> Tuple[Optional[pd.Series], Dict[str, Any]]:
    """
    Evalúa el DSL `where` sobre `FlowDataset.flows` y devuelve la máscara del eje.

    Emite
    ------
    FLT_FLOW.WHERE.FIELD_MISSING
    FLT_FLOW.WHERE.OPERATOR_UNKNOWN
    FLT_FLOW.WHERE.OPERATOR_INCOMPATIBLE
    FLT_FLOW.WHERE.INVALID_VALUE_SHAPE
    FLT_FLOW.WHERE.EMPTY_SEQUENCE
    FLT_FLOW.WHERE.DATETIME_PARSE_FAILED
    """
    info = {
        "requested": where is not None,
        "applied": False,
        "fields_evaluated": [],
        "rules_evaluated": 0,
    }
    if where is None:
        return None, info

    field_masks: List[pd.Series] = []

    # Se procesa cada campo del DSL como una cláusula AND independiente.
    for raw_field_name, raw_clause in where.items():
        field_name = str(raw_field_name)

        if field_name not in flows_df.columns:
            # Se omite la cláusula porque el campo no existe en la tabla canónica de flujos.
            emit_issue(
                issues,
                FILTER_FLOWS_ISSUES,
                "FLT_FLOW.WHERE.FIELD_MISSING",
                field=field_name,
                **request_ctx,
                n_flows_in=int(len(flows_df)),
                n_flows_out=int(len(flows_df)),
                rows_in=int(len(flows_df)),
                rows_out=int(len(flows_df)),
                dropped_total=0,
                operator=None,
                value=None,
                value_repr=None,
                expected_type="existing column in FlowDataset.flows",
                dtype_observed=None,
                available_fields_sample=_sample_list(list(flows_df.columns), limit=20),
                available_fields_total=len(flows_df.columns),
                row_count=int(len(flows_df)),
            )
            continue

        normalized_clause, clause_kind = _normalize_where_clause(raw_clause)
        if clause_kind == "empty_sequence":
            # Se omite la cláusula porque una secuencia vacía no define selección alguna.
            emit_issue(
                issues,
                FILTER_FLOWS_ISSUES,
                "FLT_FLOW.WHERE.EMPTY_SEQUENCE",
                field=field_name,
                **request_ctx,
                n_flows_in=int(len(flows_df)),
                n_flows_out=int(len(flows_df)),
                rows_in=int(len(flows_df)),
                rows_out=int(len(flows_df)),
                dropped_total=0,
                operator="in",
                value=_to_json_serializable_or_none(raw_clause),
                value_repr=repr(raw_clause)[:200],
                expected_type="non-empty sequence",
                dtype_observed=str(flows_df[field_name].dtype),
                available_fields_sample=_sample_list(list(flows_df.columns), limit=20),
                available_fields_total=len(flows_df.columns),
                row_count=int(len(flows_df)),
            )
            continue

        if normalized_clause is None:
            # Se omite la cláusula porque la forma entregada no es interpretable para el DSL.
            emit_issue(
                issues,
                FILTER_FLOWS_ISSUES,
                "FLT_FLOW.WHERE.INVALID_VALUE_SHAPE",
                field=field_name,
                **request_ctx,
                n_flows_in=int(len(flows_df)),
                n_flows_out=int(len(flows_df)),
                rows_in=int(len(flows_df)),
                rows_out=int(len(flows_df)),
                dropped_total=0,
                operator=None,
                value=_to_json_serializable_or_none(raw_clause),
                value_repr=repr(raw_clause)[:200],
                expected_type="scalar | non-empty sequence | dict[op, value]",
                expected_shape="scalar | sequence | operator mapping",
                dtype_observed=str(flows_df[field_name].dtype),
                available_fields_sample=_sample_list(list(flows_df.columns), limit=20),
                available_fields_total=len(flows_df.columns),
                row_count=int(len(flows_df)),
            )
            continue

        series = flows_df[field_name]
        dtype_effective = _resolve_flow_field_dtype(field_name, series)
        allowed_ops = _allowed_ops_for_dtype(dtype_effective)
        field_mask = pd.Series(True, index=flows_df.index, dtype=bool)
        clause_invalid = False

        # Se combinan con AND todos los operadores válidos del mismo campo.
        for op_name, raw_value in normalized_clause.items():
            if op_name not in _ALLOWED_WHERE_OPS:
                # Se omite la cláusula porque el operador no pertenece al catálogo soportado.
                emit_issue(
                    issues,
                    FILTER_FLOWS_ISSUES,
                    "FLT_FLOW.WHERE.OPERATOR_UNKNOWN",
                    field=field_name,
                    **request_ctx,
                    n_flows_in=int(len(flows_df)),
                    n_flows_out=int(len(flows_df)),
                    rows_in=int(len(flows_df)),
                    rows_out=int(len(flows_df)),
                    dropped_total=0,
                    operator=str(op_name),
                    value=_to_json_serializable_or_none(raw_value),
                    value_repr=repr(raw_value)[:200],
                    expected_type=f"operator in {sorted(_ALLOWED_WHERE_OPS)}",
                    dtype_observed=dtype_effective,
                    available_fields_sample=_sample_list(list(flows_df.columns), limit=20),
                    available_fields_total=len(flows_df.columns),
                    row_count=int(len(flows_df)),
                )
                clause_invalid = True
                break

            if op_name not in allowed_ops:
                # Se omite la cláusula porque el operador no es compatible con el dtype efectivo.
                emit_issue(
                    issues,
                    FILTER_FLOWS_ISSUES,
                    "FLT_FLOW.WHERE.OPERATOR_INCOMPATIBLE",
                    field=field_name,
                    **request_ctx,
                    n_flows_in=int(len(flows_df)),
                    n_flows_out=int(len(flows_df)),
                    rows_in=int(len(flows_df)),
                    rows_out=int(len(flows_df)),
                    dropped_total=0,
                    operator=str(op_name),
                    value=_to_json_serializable_or_none(raw_value),
                    value_repr=repr(raw_value)[:200],
                    expected_type=f"operator in {sorted(allowed_ops)}",
                    dtype_observed=dtype_effective,
                    available_fields_sample=_sample_list(list(flows_df.columns), limit=20),
                    available_fields_total=len(flows_df.columns),
                    row_count=int(len(flows_df)),
                )
                clause_invalid = True
                break

            is_valid_value, expected_shape = _validate_where_operator_value(op_name, raw_value, dtype_effective)
            if not is_valid_value:
                code = "FLT_FLOW.WHERE.EMPTY_SEQUENCE" if op_name in {"in", "not_in"} and _is_empty_sequence(raw_value) else "FLT_FLOW.WHERE.INVALID_VALUE_SHAPE"
                # Se omite la cláusula porque el valor no respeta la forma esperada por el operador.
                emit_issue(
                    issues,
                    FILTER_FLOWS_ISSUES,
                    code,
                    field=field_name,
                    **request_ctx,
                    n_flows_in=int(len(flows_df)),
                    n_flows_out=int(len(flows_df)),
                    rows_in=int(len(flows_df)),
                    rows_out=int(len(flows_df)),
                    dropped_total=0,
                    operator=str(op_name),
                    value=_to_json_serializable_or_none(raw_value),
                    value_repr=repr(raw_value)[:200],
                    expected_type=dtype_effective,
                    expected_shape=expected_shape,
                    dtype_observed=dtype_effective,
                    available_fields_sample=_sample_list(list(flows_df.columns), limit=20),
                    available_fields_total=len(flows_df.columns),
                    row_count=int(len(flows_df)),
                )
                clause_invalid = True
                break

            try:
                op_mask = _evaluate_where_operator_mask(series, dtype_effective, op_name, raw_value)
            except Exception as exc:
                if dtype_effective == "datetime":
                    # Se omite la cláusula porque el valor temporal no pudo interpretarse de forma estable.
                    emit_issue(
                        issues,
                        FILTER_FLOWS_ISSUES,
                        "FLT_FLOW.WHERE.DATETIME_PARSE_FAILED",
                        field=field_name,
                        **request_ctx,
                        n_flows_in=int(len(flows_df)),
                        n_flows_out=int(len(flows_df)),
                        rows_in=int(len(flows_df)),
                        rows_out=int(len(flows_df)),
                        dropped_total=0,
                        operator=str(op_name),
                        value=_to_json_serializable_or_none(raw_value),
                        value_repr=repr(raw_value)[:200],
                        expected_type="datetime-comparable value",
                        dtype_observed=dtype_effective,
                        available_fields_sample=_sample_list(list(flows_df.columns), limit=20),
                        available_fields_total=len(flows_df.columns),
                        parse_error=str(exc),
                        row_count=int(len(flows_df)),
                    )
                    clause_invalid = True
                    break
                raise

            field_mask &= op_mask.fillna(False).astype(bool)
            info["rules_evaluated"] += 1

        if clause_invalid:
            continue

        info["fields_evaluated"].append(field_name)
        field_masks.append(field_mask)

    if not field_masks:
        return None, info

    # Se consolida el eje where respetando AND entre campos.
    where_mask = pd.Series(True, index=flows_df.index, dtype=bool)
    for current_mask in field_masks:
        where_mask &= current_mask.fillna(False).astype(bool)

    info["applied"] = True
    return where_mask, info


def _evaluate_h3_mask_on_flows_df(
    flows_df: pd.DataFrame,
    *,
    h3_cells: Optional[Iterable[str]],
    spatial_predicate: SpatialPredicate,
    issues: List[Issue],
    request_ctx: Mapping[str, Any],
) -> Tuple[Optional[pd.Series], Dict[str, Any]]:
    """
    Evalúa `h3_cells` + `spatial_predicate` sobre `origin_h3_index` / `destination_h3_index`.

    Emite
    ------
    FLT_FLOW.H3.INVALID_CELL_VALUES
    FLT_FLOW.H3.COLUMNS_MISSING
    """
    info = {
        "requested": h3_cells is not None,
        "applied": False,
        "valid_cells_count": 0,
    }
    if h3_cells is None:
        return None, info

    missing_fields = [field_name for field_name in _required_h3_fields_for_predicate(str(spatial_predicate)) if field_name not in flows_df.columns]
    if missing_fields:
        # Se omite el eje H3 porque faltan columnas requeridas por el predicado espacial.
        emit_issue(
            issues,
            FILTER_FLOWS_ISSUES,
            "FLT_FLOW.H3.COLUMNS_MISSING",
            **request_ctx,
            n_flows_in=int(len(flows_df)),
            n_flows_out=int(len(flows_df)),
            rows_in=int(len(flows_df)),
            rows_out=int(len(flows_df)),
            dropped_total=0,
            missing_fields=missing_fields,
            invalid_cells_sample=None,
            invalid_cells_count=0,
            valid_cells_count=0,
            row_count=int(len(flows_df)),
        )
        return None, info

    h3_cells_list = list(h3_cells)
    valid_cells: List[str] = []
    invalid_cells: List[str] = []
    for cell in h3_cells_list:
        normalized = _normalize_h3_value(cell)
        if normalized is None or not _is_valid_h3_value(normalized):
            if len(invalid_cells) < 10:
                invalid_cells.append(str(cell))
            continue
        valid_cells.append(normalized)

    valid_cells = sorted(set(valid_cells))
    info["valid_cells_count"] = len(valid_cells)

    if invalid_cells:
        # Se deja evidencia de celdas inválidas y se continúa solo con las válidas.
        emit_issue(
            issues,
            FILTER_FLOWS_ISSUES,
            "FLT_FLOW.H3.INVALID_CELL_VALUES",
            **request_ctx,
            n_flows_in=int(len(flows_df)),
            n_flows_out=int(len(flows_df)),
            rows_in=int(len(flows_df)),
            rows_out=int(len(flows_df)),
            dropped_total=0,
            invalid_cells_sample=invalid_cells,
            invalid_cells_count=int(len(h3_cells_list) - len(valid_cells)),
            valid_cells_count=int(len(valid_cells)),
            missing_fields=None,
            row_count=int(len(flows_df)),
        )

    if not valid_cells:
        return None, info

    whitelist = set(valid_cells)
    origin_mask = pd.Series(False, index=flows_df.index, dtype=bool)
    destination_mask = pd.Series(False, index=flows_df.index, dtype=bool)

    if str(spatial_predicate) in {"origin", "both", "either"}:
        origin_mask = flows_df["origin_h3_index"].map(
            lambda value: _normalize_h3_value(value) in whitelist if _normalize_h3_value(value) is not None else False
        ).fillna(False).astype(bool)
    if str(spatial_predicate) in {"destination", "both", "either"}:
        destination_mask = flows_df["destination_h3_index"].map(
            lambda value: _normalize_h3_value(value) in whitelist if _normalize_h3_value(value) is not None else False
        ).fillna(False).astype(bool)

    if str(spatial_predicate) == "origin":
        h3_mask = origin_mask
    elif str(spatial_predicate) == "destination":
        h3_mask = destination_mask
    elif str(spatial_predicate) == "both":
        h3_mask = origin_mask & destination_mask
    else:
        h3_mask = origin_mask | destination_mask

    info["applied"] = True
    return h3_mask, info


def _resolve_filtered_flow_to_trips(
    flow_to_trips_df: Any,
    *,
    kept_flow_ids: set[Any],
    keep_flow_to_trips: bool,
    issues: List[Issue],
    request_ctx: Mapping[str, Any],
) -> Tuple[Optional[pd.DataFrame], str]:
    """
    Resuelve la política cerrada de `flow_to_trips` para el dataset filtrado.

    Emite
    ------
    FLT_FLOW.AUX.FLOW_TO_TRIPS_SYNCED
    FLT_FLOW.AUX.FLOW_TO_TRIPS_REQUESTED_BUT_MISSING
    FLT_FLOW.AUX.FLOW_TO_TRIPS_INVALID
    """
    if not keep_flow_to_trips:
        return None, "not_requested"

    if flow_to_trips_df is None:
        # Se deja constancia de que el auxiliar fue solicitado pero no estaba disponible.
        emit_issue(
            issues,
            FILTER_FLOWS_ISSUES,
            "FLT_FLOW.AUX.FLOW_TO_TRIPS_REQUESTED_BUT_MISSING",
            **request_ctx,
            flow_to_trips_status="missing",
            flow_to_trips_rows_in=None,
            flow_to_trips_rows_out=None,
            missing_fields=None,
        )
        return None, "missing"

    if not isinstance(flow_to_trips_df, pd.DataFrame):
        # Se descarta el auxiliar porque su estructura ni siquiera es DataFrame.
        emit_issue(
            issues,
            FILTER_FLOWS_ISSUES,
            "FLT_FLOW.AUX.FLOW_TO_TRIPS_INVALID",
            **request_ctx,
            flow_to_trips_status="discarded_invalid",
            flow_to_trips_rows_in=None,
            flow_to_trips_rows_out=None,
            missing_fields=["flow_id", "movement_id"],
            reason="aux_not_dataframe",
        )
        return None, "discarded_invalid"

    missing_fields = [field_name for field_name in ("flow_id", "movement_id") if field_name not in flow_to_trips_df.columns]
    if missing_fields:
        # Se descarta el auxiliar porque no tiene la estructura mínima para sincronización.
        emit_issue(
            issues,
            FILTER_FLOWS_ISSUES,
            "FLT_FLOW.AUX.FLOW_TO_TRIPS_INVALID",
            **request_ctx,
            flow_to_trips_status="discarded_invalid",
            flow_to_trips_rows_in=int(len(flow_to_trips_df)),
            flow_to_trips_rows_out=None,
            missing_fields=missing_fields,
        )
        return None, "discarded_invalid"

    # Se filtra el auxiliar por flow_id retenidos para mantener consistencia del drill-down.
    filtered = flow_to_trips_df.loc[flow_to_trips_df["flow_id"].isin(list(kept_flow_ids))].copy(deep=True)
    emit_issue(
        issues,
        FILTER_FLOWS_ISSUES,
        "FLT_FLOW.AUX.FLOW_TO_TRIPS_SYNCED",
        row_count=int(len(flow_to_trips_df) - len(filtered)),
        **request_ctx,
        flow_to_trips_status="synced",
        flow_to_trips_rows_in=int(len(flow_to_trips_df)),
        flow_to_trips_rows_out=int(len(filtered)),
        missing_fields=None,
    )
    return filtered, "synced"


# -----------------------------------------------------------------------------
# Helpers internos de uso general
# -----------------------------------------------------------------------------


def _truncate_issues_with_limit(
    issues_all: Sequence[Issue],
    *,
    max_issues: int,
) -> Tuple[List[Issue], Optional[Dict[str, Any]]]:
    """Aplica el límite de issues y agrega el issue final de truncamiento si corresponde."""
    total_detected = len(issues_all)
    if total_detected <= max_issues:
        return list(issues_all), None

    retained = list(issues_all[: max(max_issues - 1, 0)])
    # Se agrega un último issue explícito para que el truncamiento quede visible en el reporte.
    emit_issue(
        retained,
        FILTER_FLOWS_ISSUES,
        "FLT_FLOW.REPORT.ISSUES_TRUNCATED",
        max_issues=max_issues,
        n_issues_emitted=max_issues,
        n_issues_detected_total=total_detected,
        action="truncate_issues",
    )
    limits = {
        "max_issues": int(max_issues),
        "issues_truncated": True,
        "n_issues_emitted": len(retained),
        "n_issues_detected_total": int(total_detected),
    }
    return retained, limits


def _build_issues_summary(issues: Sequence[Issue]) -> Dict[str, Any]:
    """Resume issues por severidad y por code para el evento de metadata."""
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


def _normalize_where_for_parameters(where: Mapping[str, Any]) -> Dict[str, Any]:
    """Convierte `where` a una forma serializable sin reinterpretar su semántica recuperable."""
    normalized: Dict[str, Any] = {}
    for field_name, raw_clause in where.items():
        if isinstance(raw_clause, Mapping):
            normalized[str(field_name)] = {
                str(op_name): _to_json_serializable_or_none(op_value)
                for op_name, op_value in raw_clause.items()
            }
        elif isinstance(raw_clause, set):
            normalized[str(field_name)] = [_to_json_serializable_or_none(value) for value in sorted(raw_clause, key=lambda value: repr(value))]
        elif isinstance(raw_clause, (list, tuple)):
            normalized[str(field_name)] = [_to_json_serializable_or_none(value) for value in raw_clause]
        else:
            normalized[str(field_name)] = _to_json_serializable_or_none(raw_clause)
    return normalized


def _build_removed_rows_evidence(
    data: pd.DataFrame,
    removed_mask: pd.Series,
    *,
    value_fields: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """Construye una muestra compacta de filas descartadas para `Issue.details`."""
    removed_index = list(data.index[removed_mask])[:_DEFAULT_SAMPLE_ROWS_REMOVED]
    removed_frame = data.loc[removed_index] if removed_index else data.iloc[0:0]

    flow_ids = None
    if "flow_id" in removed_frame.columns:
        flow_ids = [_json_safe_scalar(value) for value in removed_frame["flow_id"].tolist()[:_DEFAULT_SAMPLE_ROWS_REMOVED]]

    rows_sample = None
    if value_fields:
        available_fields = [field_name for field_name in value_fields if field_name in removed_frame.columns]
        if available_fields:
            fields_for_sample = ["flow_id", *available_fields] if "flow_id" in removed_frame.columns else list(available_fields)
            rows_sample = [
                {str(column_name): _json_safe_scalar(value) for column_name, value in row.items()}
                for row in removed_frame[fields_for_sample].head(_DEFAULT_SAMPLE_ROWS_REMOVED).to_dict(orient="records")
            ]

    return {
        "flow_id_sample_removed": flow_ids,
        "rows_sample_removed": rows_sample,
    }


def _build_metadata_out(metadata: Any, *, keep_metadata: bool) -> Dict[str, Any]:
    """Reconstruye metadata según la política cerrada de `keep_metadata`."""
    if not isinstance(metadata, dict):
        return {}
    if keep_metadata:
        metadata_out = copy.deepcopy(metadata)
        if not isinstance(metadata_out.get("events"), list):
            metadata_out["events"] = []
        return metadata_out

    metadata_out: Dict[str, Any] = {}
    for key_name, value in metadata.items():
        if key_name == "events":
            continue
        metadata_out[key_name] = copy.deepcopy(value)
    return metadata_out


def _build_derived_flow_provenance(flows: FlowDataset) -> Optional[Dict[str, Any]]:
    """Reconstruye provenance como dataset derivado del FlowDataset de entrada."""
    provenance_out = copy.deepcopy(dict(flows.provenance)) if isinstance(flows.provenance, Mapping) else {}
    source_dataset_id = None
    source_artifact_id = None
    source_metadata = getattr(flows, "metadata", {})
    if isinstance(source_metadata, dict):
        source_dataset_id = source_metadata.get("dataset_id")
        source_artifact_id = source_metadata.get("artifact_id")

    provenance_out["derived_from"] = [
        {
            "source_type": "flows",
            "dataset_id": source_dataset_id,
            "artifact_id": source_artifact_id,
            "n_rows": int(len(getattr(flows, "flows", pd.DataFrame()))),
        }
    ]
    provenance_out["prior_events_summary"] = _summarize_prior_events(source_metadata.get("events") if isinstance(source_metadata, dict) else None)
    return provenance_out or None


def _summarize_prior_events(events: Any) -> Optional[List[Dict[str, Any]]]:
    """Construye un resumen compacto de eventos previos sin copiar toda la historia."""
    if not isinstance(events, list):
        return None

    summary: List[Dict[str, Any]] = []
    for event in events[-10:]:
        if not isinstance(event, dict):
            continue
        summary.append(
            {
                "op": event.get("op"),
                "ts_utc": event.get("ts_utc"),
            }
        )
    return summary or None


def _ensure_events_list(metadata: dict[str, Any]) -> list[dict[str, Any]]:
    """Asegura que metadata['events'] exista como lista append-only."""
    if not isinstance(metadata.get("events"), list):
        metadata["events"] = []
    return metadata["events"]


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


def _normalize_where_clause(raw_clause: Any) -> Tuple[Optional[Dict[str, Any]], str]:
    """Normaliza una cláusula individual del DSL `where`."""
    if isinstance(raw_clause, Mapping):
        return {str(key): value for key, value in raw_clause.items()}, "mapping"
    if isinstance(raw_clause, set):
        if len(raw_clause) == 0:
            return None, "empty_sequence"
        return {"in": list(raw_clause)}, "implicit_in"
    if isinstance(raw_clause, (list, tuple)) and not isinstance(raw_clause, (str, bytes)):
        if len(raw_clause) == 0:
            return None, "empty_sequence"
        return {"in": list(raw_clause)}, "implicit_in"
    return {"eq": raw_clause}, "scalar"


def _resolve_flow_field_dtype(field_name: str, series: pd.Series) -> str:
    """Resuelve el dtype lógico efectivo de un campo de flows bajo el contrato v1.1."""
    if field_name in {"flow_id", "origin_h3_index", "destination_h3_index"}:
        return "string"
    if field_name == "flow_count":
        return "int"
    if field_name == "flow_value":
        return "float"
    if field_name in {"window_start_utc", "window_end_utc"}:
        return "datetime"
    if ptypes.is_bool_dtype(series):
        return "bool"
    if ptypes.is_datetime64_any_dtype(series) or ptypes.is_datetime64tz_dtype(series):
        return "datetime"
    if ptypes.is_integer_dtype(series):
        return "int"
    if ptypes.is_numeric_dtype(series):
        return "float"
    if ptypes.is_categorical_dtype(series):
        return "categorical"
    return "string"


def _allowed_ops_for_dtype(dtype_effective: Optional[str]) -> set[str]:
    """Retorna el subconjunto de operadores permitido para el dtype lógico efectivo."""
    return set(_ALLOWED_WHERE_OPS_BY_DTYPE.get(dtype_effective or "string", _ALLOWED_WHERE_OPS_BY_DTYPE["string"]))


def _validate_where_operator_value(op_name: str, op_value: Any, dtype_effective: Optional[str]) -> Tuple[bool, str]:
    """Valida la forma esperada del valor según operador y dtype lógico."""
    if op_name in {"eq", "ne"}:
        return True, "scalar JSON-safe value"

    if op_name in {"in", "not_in"}:
        valid = isinstance(op_value, (list, tuple, set)) and len(op_value) > 0 and not isinstance(op_value, (str, bytes))
        return valid, "in/not_in expect non-empty sequence"

    if op_name in {"gt", "gte", "lt", "lte"}:
        if dtype_effective == "datetime":
            return True, f"{op_name} expects datetime-comparable scalar"
        return isinstance(op_value, (int, float)) and not isinstance(op_value, bool), f"{op_name} expects numeric scalar"

    if op_name == "between":
        if not isinstance(op_value, (list, tuple)) or len(op_value) != 2:
            return False, "between expects list/tuple of length 2"
        low, high = op_value[0], op_value[1]
        if dtype_effective == "datetime":
            return True, "between expects (start, end) datetime-comparable pair"
        valid_numeric = all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in (low, high))
        return valid_numeric, "between expects (min, max) numeric pair"

    if op_name in {"is_null", "not_null"}:
        return op_value is True, f"{op_name} expects literal True"

    return False, "unsupported operator"


def _evaluate_where_operator_mask(series: pd.Series, dtype_effective: Optional[str], op_name: str, op_value: Any) -> pd.Series:
    """Evalúa un operador individual del DSL `where` sobre una serie de flows."""
    if op_name == "eq":
        if dtype_effective == "datetime":
            scalar = _coerce_datetime_scalar(op_value)
            return pd.to_datetime(series, errors="coerce", utc=True).eq(scalar)
        return series.eq(op_value)

    if op_name == "ne":
        if dtype_effective == "datetime":
            scalar = _coerce_datetime_scalar(op_value)
            return pd.to_datetime(series, errors="coerce", utc=True).ne(scalar)
        return series.ne(op_value)

    if op_name == "in":
        if dtype_effective == "datetime":
            values = [_coerce_datetime_scalar(value) for value in list(op_value)]
            return pd.to_datetime(series, errors="coerce", utc=True).isin(values)
        return series.isin(list(op_value))

    if op_name == "not_in":
        if dtype_effective == "datetime":
            values = [_coerce_datetime_scalar(value) for value in list(op_value)]
            return ~pd.to_datetime(series, errors="coerce", utc=True).isin(values)
        return ~series.isin(list(op_value))

    if op_name == "is_null":
        return series.isna()

    if op_name == "not_null":
        return series.notna()

    if dtype_effective == "datetime":
        series_dt = pd.to_datetime(series, errors="coerce", utc=True)
        if op_name == "gt":
            return series_dt.gt(_coerce_datetime_scalar(op_value))
        if op_name == "gte":
            return series_dt.ge(_coerce_datetime_scalar(op_value))
        if op_name == "lt":
            return series_dt.lt(_coerce_datetime_scalar(op_value))
        if op_name == "lte":
            return series_dt.le(_coerce_datetime_scalar(op_value))
        if op_name == "between":
            low, high = op_value
            low_ts = _coerce_datetime_scalar(low)
            high_ts = _coerce_datetime_scalar(high)
            return series_dt.ge(low_ts) & series_dt.le(high_ts)

    series_num = pd.to_numeric(series, errors="coerce")
    if op_name == "gt":
        return series_num.gt(op_value)
    if op_name == "gte":
        return series_num.ge(op_value)
    if op_name == "lt":
        return series_num.lt(op_value)
    if op_name == "lte":
        return series_num.le(op_value)
    if op_name == "between":
        low, high = op_value
        return series_num.ge(low) & series_num.le(high)

    return pd.Series(False, index=series.index, dtype=bool)


def _required_h3_fields_for_predicate(spatial_predicate: str) -> List[str]:
    """Retorna las columnas H3 requeridas según el predicado espacial."""
    if spatial_predicate == "origin":
        return ["origin_h3_index"]
    if spatial_predicate == "destination":
        return ["destination_h3_index"]
    return ["origin_h3_index", "destination_h3_index"]


def _coerce_datetime_scalar(value: Any) -> pd.Timestamp:
    """Normaliza un escalar temporal a Timestamp UTC-aware para comparaciones estables."""
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


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


def _sample_list(values: Sequence[Any], *, limit: int) -> List[Any]:
    """Construye una muestra compacta y JSON-safe desde una secuencia."""
    return [_json_safe_scalar(value) for value in list(values)[:limit]]


def _is_empty_sequence(value: Any) -> bool:
    """Indica si un valor corresponde a una secuencia vacía relevante para el DSL."""
    return isinstance(value, (list, tuple, set)) and len(value) == 0 and not isinstance(value, (str, bytes))


def _to_json_serializable_or_none(obj: Any) -> Any:
    """Convierte dict/list anidados a una forma JSON-safe sin fallback silencioso complejo."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return {str(key): _to_json_serializable_or_none(value) for key, value in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        values = list(obj)
        if isinstance(obj, set):
            values = sorted(values, key=lambda value: repr(value))
        return [_to_json_serializable_or_none(value) for value in values]
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
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if pd.isna(value):
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _utc_now_iso() -> str:
    """Retorna timestamp UTC ISO-8601 para eventos del módulo."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")