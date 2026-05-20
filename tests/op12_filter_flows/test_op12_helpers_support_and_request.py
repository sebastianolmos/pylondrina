from __future__ import annotations

from copy import deepcopy

import numpy as np
import pandas as pd
import pytest

from pylondrina.errors import FilterError
from pylondrina.reports import Issue
from pylondrina.transforms.flows_filtering import (
    FlowFilterOptions,
    _allowed_ops_for_dtype,
    _build_derived_flow_provenance,
    _build_issues_summary,
    _build_metadata_out,
    _build_removed_rows_evidence,
    _coerce_datetime_scalar,
    _evaluate_where_operator_mask,
    _extract_validated_flag,
    _is_empty_sequence,
    _is_valid_h3_value,
    _json_is_serializable,
    _json_safe_scalar,
    _normalize_filter_flows_request,
    _normalize_h3_value,
    _normalize_where_clause,
    _required_h3_fields_for_predicate,
    _resolve_flow_field_dtype,
    _sample_list,
    _to_json_serializable_or_none,
    _truncate_issues_with_limit,
    _utc_now_iso,
    _validate_where_operator_value,
)


# -----------------------------------------------------------------------------
# Bloque 1. Utilidades internas de uso general
# -----------------------------------------------------------------------------


def test_json_serialization_helpers_normalize_scalars_and_nested_payloads():
    """Verifica normalización JSON-safe de escalares y estructuras anidadas."""
    ts = pd.Timestamp("2026-01-01T07:00:00Z")

    assert _json_safe_scalar(None) is None
    assert _json_safe_scalar(np.nan) is None
    assert _json_safe_scalar(True) is True
    assert _json_safe_scalar(ts).startswith("2026-01-01T07:00:00")

    payload = {
        "a": 1,
        "b": [ts, np.nan, {"x": (1, 2), "y": None}],
    }

    normalized = _to_json_serializable_or_none(payload)

    assert normalized["a"] == 1
    assert normalized["b"][0].startswith("2026-01-01T07:00:00")
    assert normalized["b"][1] is None
    assert normalized["b"][2]["x"] == [1, 2]
    assert _json_is_serializable(normalized) is True


def test_h3_helpers_normalize_validate_and_resolve_required_fields(
    small_flowdataset_factory,
):
    """Verifica normalización básica de H3 y columnas requeridas por predicado espacial."""
    _, cells = small_flowdataset_factory()
    valid_cell = cells["origin_a"]

    assert _normalize_h3_value(valid_cell) == valid_cell
    assert _normalize_h3_value(f"  {valid_cell}  ") == valid_cell
    assert _normalize_h3_value("") is None
    assert _normalize_h3_value("   ") is None
    assert _normalize_h3_value(None) is None

    assert _is_valid_h3_value(valid_cell) is True
    assert _is_valid_h3_value("h3_invalido_total") is False

    assert _required_h3_fields_for_predicate("origin") == [
        "origin_h3_index",
    ]
    assert _required_h3_fields_for_predicate("destination") == [
        "destination_h3_index",
    ]
    assert _required_h3_fields_for_predicate("both") == [
        "origin_h3_index",
        "destination_h3_index",
    ]
    assert _required_h3_fields_for_predicate("either") == [
        "origin_h3_index",
        "destination_h3_index",
    ]


def test_flow_dtype_and_allowed_operator_helpers_follow_contract(
    small_flowdataset_factory,
):
    """Verifica resolución de dtype lógico y matriz de operadores permitidos."""
    flows, _ = small_flowdataset_factory()
    df = flows.flows

    assert _resolve_flow_field_dtype("flow_id", df["flow_id"]) == "string"
    assert (
        _resolve_flow_field_dtype(
            "origin_h3_index",
            df["origin_h3_index"],
        )
        == "string"
    )
    assert _resolve_flow_field_dtype("flow_count", df["flow_count"]) == "int"
    assert _resolve_flow_field_dtype("flow_value", df["flow_value"]) == "float"
    assert (
        _resolve_flow_field_dtype(
            "window_start_utc",
            df["window_start_utc"],
        )
        == "datetime"
    )
    assert _resolve_flow_field_dtype("mode", df["mode"]) == "string"

    categorical_ops = _allowed_ops_for_dtype("categorical")
    assert "eq" in categorical_ops
    assert "in" in categorical_ops
    assert "gt" not in categorical_ops

    numeric_ops = _allowed_ops_for_dtype("float")
    assert "between" in numeric_ops
    assert "gte" in numeric_ops

    boolean_ops = _allowed_ops_for_dtype("bool")
    assert "eq" in boolean_ops
    assert "between" not in boolean_ops


def test_validate_where_operator_value_accepts_and_rejects_expected_shapes():
    """Verifica la forma válida o inválida de valores según operador y dtype."""
    ok, _ = _validate_where_operator_value("eq", "bus", "string")
    assert ok is True

    ok, _ = _validate_where_operator_value(
        "in",
        ["bus", "metro"],
        "categorical",
    )
    assert ok is True

    ok, _ = _validate_where_operator_value("in", [], "categorical")
    assert ok is False

    ok, _ = _validate_where_operator_value("gte", 3, "int")
    assert ok is True

    ok, _ = _validate_where_operator_value("gte", "3", "int")
    assert ok is False

    ok, _ = _validate_where_operator_value("between", [1, 10], "float")
    assert ok is True

    ok, _ = _validate_where_operator_value("between", [1], "float")
    assert ok is False

    ok, _ = _validate_where_operator_value("is_null", True, "string")
    assert ok is True

    ok, _ = _validate_where_operator_value("is_null", False, "string")
    assert ok is False


def test_where_clause_and_small_sequence_helpers_normalize_dsl_inputs():
    """Verifica normalización del DSL `where`, detección de secuencias vacías y muestreo."""
    clause, shape = _normalize_where_clause("bus")
    assert clause == {"eq": "bus"}
    assert shape == "scalar"

    clause, shape = _normalize_where_clause(["bus", "metro"])
    assert clause == {"in": ["bus", "metro"]}
    assert shape == "implicit_in"

    clause, shape = _normalize_where_clause(("bus", "metro"))
    assert clause == {"in": ["bus", "metro"]}
    assert shape == "implicit_in"

    clause, shape = _normalize_where_clause({"gte": 5})
    assert clause == {"gte": 5}
    assert shape == "mapping"

    assert _is_empty_sequence([]) is True
    assert _is_empty_sequence(()) is True
    assert _is_empty_sequence(set()) is True
    assert _is_empty_sequence("abc") is False

    values = ["a", "b", "c", "d"]
    sample = _sample_list(values, limit=2)

    assert sample == values[:2]


def test_datetime_coercion_and_where_operator_masks_follow_expected_semantics(
    small_flowdataset_factory,
):
    """Verifica coerción UTC y evaluación de máscaras numéricas, categóricas, temporales y nulas."""
    flows, _ = small_flowdataset_factory()
    df = flows.flows

    ts = _coerce_datetime_scalar("2026-01-01T08:00:00-03:00")
    assert ts == pd.Timestamp("2026-01-01T11:00:00Z")

    mask_num = _evaluate_where_operator_mask(
        df["flow_count"],
        "int",
        "gte",
        5,
    )
    assert mask_num.tolist() == [True, True, False, False]

    mask_in = _evaluate_where_operator_mask(
        df["mode"],
        "string",
        "in",
        ["bus", "metro"],
    )
    assert mask_in.tolist() == [True, True, False, True]

    mask_dt = _evaluate_where_operator_mask(
        df["window_start_utc"],
        "datetime",
        "between",
        ["2026-01-01T10:00:00Z", "2026-01-01T12:00:00Z"],
    )
    assert mask_dt.tolist() == [True, True, False, False]

    mask_null = _evaluate_where_operator_mask(
        df["gender"],
        "string",
        "is_null",
        True,
    )
    assert mask_null.tolist() == [False, False, False, True]


def test_build_removed_rows_evidence_collects_compact_samples_from_removed_rows(
    small_flowdataset_factory,
):
    """Verifica construcción de evidencia compacta para filas descartadas."""
    flows, _ = small_flowdataset_factory()

    removed_mask = pd.Series(
        [False, True, True, False],
        index=flows.flows.index,
    )

    removed_rows = flows.flows.loc[removed_mask]
    expected_flow_ids = removed_rows["flow_id"].tolist()

    evidence = _build_removed_rows_evidence(
        flows.flows,
        removed_mask,
        value_fields=["mode", "flow_count"],
    )

    assert evidence["flow_id_sample_removed"] == expected_flow_ids
    assert len(evidence["rows_sample_removed"]) == len(expected_flow_ids)
    assert (
        evidence["rows_sample_removed"][0]["flow_id"]
        == expected_flow_ids[0]
    )
    assert "mode" in evidence["rows_sample_removed"][0]
    assert "flow_count" in evidence["rows_sample_removed"][0]


def test_metadata_provenance_and_timestamp_helpers_preserve_traceability_contract(
    small_flowdataset_factory,
):
    """Verifica metadata derivada, estado validado, provenance y formato UTC de timestamps."""
    flows, _ = small_flowdataset_factory()

    md_keep = _build_metadata_out(flows.metadata, keep_metadata=True)
    md_drop = _build_metadata_out(flows.metadata, keep_metadata=False)

    assert md_keep is not flows.metadata
    assert "events" in md_keep
    assert len(md_keep["events"]) == len(flows.metadata["events"])

    assert "events" not in md_drop
    assert md_drop["dataset_id"] == flows.metadata["dataset_id"]
    assert md_drop["artifact_id"] == flows.metadata["artifact_id"]

    assert _extract_validated_flag(flows.metadata) is False
    assert _extract_validated_flag({"flags": {"validated": True}}) is True
    assert _extract_validated_flag({}) is False

    provenance = _build_derived_flow_provenance(flows)

    assert provenance is not None
    assert "derived_from" in provenance
    assert (
        provenance["derived_from"][0]["dataset_id"]
        == flows.metadata["dataset_id"]
    )
    assert "prior_events_summary" in provenance
    assert all(
        set(item.keys()) == {"op", "ts_utc"}
        for item in provenance["prior_events_summary"]
    )

    ts_utc = _utc_now_iso()
    assert ts_utc.endswith("Z")


def test_issue_truncation_and_summary_helpers_report_limits_and_counts():
    """Verifica truncamiento de issues y resumen compacto por severidad y código."""
    issues_all = [
        Issue(level="info", code="X.INFO", message="i1"),
        Issue(level="warning", code="X.WARN", message="w1"),
        Issue(level="error", code="X.ERR", message="e1"),
        Issue(level="warning", code="X.WARN", message="w2"),
    ]

    issues_truncated, limits = _truncate_issues_with_limit(
        issues_all,
        max_issues=3,
    )

    assert len(issues_truncated) == 3
    assert issues_truncated[-1].code == "FLT_FLOW.REPORT.ISSUES_TRUNCATED"

    assert limits is not None
    assert limits["max_issues"] == 3
    assert limits["issues_truncated"] is True

    issues_summary = _build_issues_summary(issues_truncated)

    assert issues_summary["counts"]["warning"] >= 1
    assert issues_summary["counts"]["error"] >= 0
    assert isinstance(issues_summary["top_codes"], list)


# -----------------------------------------------------------------------------
# Bloque 2. Helper principal _normalize_filter_flows_request
# -----------------------------------------------------------------------------


def test_normalize_filter_flows_request_uses_defaults_when_options_are_absent(
    small_flowdataset_factory,
):
    """Verifica normalización por defecto con `options=None`."""
    flows, _ = small_flowdataset_factory()
    issues = []

    options_eff, parameters, filters_requested, request_ctx = (
        _normalize_filter_flows_request(
            flows,
            options=None,
            max_issues=1000,
            issues=issues,
        )
    )

    assert isinstance(options_eff, FlowFilterOptions)
    assert options_eff.where is None
    assert options_eff.h3_cells is None
    assert options_eff.spatial_predicate == "origin"

    assert filters_requested == []
    assert parameters["where"] is None
    assert parameters["h3_cells"] is None
    assert parameters["keep_flow_to_trips"] is True

    assert request_ctx["dataset_id"] == flows.metadata["dataset_id"]
    assert request_ctx["artifact_id"] == flows.metadata["artifact_id"]

    assert issues == []


def test_normalize_filter_flows_request_deduplicates_h3_cells_and_serializes_options(
    small_flowdataset_factory,
):
    """Verifica normalización válida de `where`, H3 deduplicado y parámetros efectivos."""
    flows, cells = small_flowdataset_factory()
    issues = []

    options_in = FlowFilterOptions(
        where={
            "mode": ["bus", "metro"],
            "flow_count": {"gte": 2},
        },
        h3_cells=[
            cells["origin_a"],
            cells["origin_a"],
            cells["origin_b"],
        ],
        spatial_predicate="either",
        keep_flow_to_trips=False,
        keep_metadata=False,
        strict=True,
    )

    options_eff, parameters, filters_requested, request_ctx = (
        _normalize_filter_flows_request(
            flows,
            options=options_in,
            max_issues=50,
            issues=issues,
        )
    )

    assert sorted(options_eff.h3_cells) == sorted(
        {cells["origin_a"], cells["origin_b"]}
    )
    assert filters_requested == ["where", "h3_cells"]

    assert parameters["spatial_predicate"] == "either"
    assert parameters["keep_flow_to_trips"] is False
    assert parameters["keep_metadata"] is False

    assert request_ctx["strict"] is True
    assert issues == []


def test_normalize_filter_flows_request_raises_for_invalid_max_issues(
    small_flowdataset_factory,
):
    """Verifica error fatal por `max_issues` inválido."""
    flows, _ = small_flowdataset_factory()
    issues = []

    with pytest.raises(FilterError) as excinfo:
        _normalize_filter_flows_request(
            flows,
            options=None,
            max_issues=0,
            issues=issues,
        )

    assert excinfo.value.code == "FLT_FLOW.CONFIG.INVALID_MAX_ISSUES"


def test_normalize_filter_flows_request_raises_when_h3_cells_become_empty_after_normalization(
    small_flowdataset_factory,
):
    """Verifica error fatal si `h3_cells` queda vacío tras saneamiento."""
    flows, _ = small_flowdataset_factory()
    issues = []

    options_in = FlowFilterOptions(
        h3_cells=[None, "", "   ", np.nan],
    )

    with pytest.raises(FilterError) as excinfo:
        _normalize_filter_flows_request(
            flows,
            options=options_in,
            max_issues=100,
            issues=issues,
        )

    assert (
        excinfo.value.code
        == "FLT_FLOW.CONFIG.H3_CELLS_EMPTY_AFTER_NORMALIZATION"
    )


def test_normalize_filter_flows_request_raises_when_canonical_flow_columns_are_missing(
    small_flowdataset_factory,
):
    """Verifica error fatal cuando falta una columna canónica mínima de flows."""
    flows, _ = small_flowdataset_factory()

    flows_bad = deepcopy(flows)
    flows_bad.flows = flows_bad.flows.drop(columns=["flow_value"])

    issues = []

    with pytest.raises(FilterError) as excinfo:
        _normalize_filter_flows_request(
            flows_bad,
            options=None,
            max_issues=100,
            issues=issues,
        )

    assert excinfo.value.code == "FLT_FLOW.CONTRACT.MISSING_CANONICAL_COLUMNS"


def test_normalize_filter_flows_request_raises_for_invalid_spatial_predicate(
    small_flowdataset_factory,
):
    """Verifica error fatal por `spatial_predicate` no soportado."""
    flows, _ = small_flowdataset_factory()
    issues = []

    options_in = FlowFilterOptions(
        h3_cells=["dummy"],
        spatial_predicate="sideways",
    )

    with pytest.raises(FilterError) as excinfo:
        _normalize_filter_flows_request(
            flows,
            options=options_in,
            max_issues=100,
            issues=issues,
        )

    assert excinfo.value.code == "FLT_FLOW.CONFIG.INVALID_SPATIAL_PREDICATE"