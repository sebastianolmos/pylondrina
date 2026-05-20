from __future__ import annotations

from pylondrina.transforms.flows_filtering import (
    _evaluate_h3_mask_on_flows_df,
    _evaluate_where_mask_on_flows_df,
)


# -----------------------------------------------------------------------------
# Bloque 3. Helper principal _evaluate_where_mask_on_flows_df
# -----------------------------------------------------------------------------


def test_evaluate_where_mask_applies_implicit_eq_on_categorical_field(
    small_flowdataset_factory,
    request_ctx_factory,
):
    """Verifica la forma básica del DSL `where` con escalar implícito como `eq`."""
    flows, _ = small_flowdataset_factory()
    issues = []
    request_ctx = request_ctx_factory(
        flows,
        where_provided=True,
        h3_cells_provided=False,
    )

    mask, info = _evaluate_where_mask_on_flows_df(
        flows.flows,
        where={"mode": "bus"},
        issues=issues,
        request_ctx=request_ctx,
    )

    assert mask.tolist() == [True, False, False, True]
    assert info["applied"] is True
    assert info["fields_evaluated"] == ["mode"]
    assert issues == []


def test_evaluate_where_mask_combines_multiple_fields_with_global_and(
    small_flowdataset_factory,
    request_ctx_factory,
):
    """Verifica AND entre campos al combinar pertenencia categórica y umbral numérico."""
    flows, _ = small_flowdataset_factory()
    issues = []
    request_ctx = request_ctx_factory(
        flows,
        where_provided=True,
        h3_cells_provided=False,
    )

    mask, info = _evaluate_where_mask_on_flows_df(
        flows.flows,
        where={
            "mode": ["bus", "metro"],
            "flow_count": {"gte": 5},
        },
        issues=issues,
        request_ctx=request_ctx,
    )

    assert mask.tolist() == [True, True, False, False]
    assert info["applied"] is True
    assert set(info["fields_evaluated"]) == {"mode", "flow_count"}
    assert issues == []


def test_evaluate_where_mask_applies_datetime_between_filter(
    small_flowdataset_factory,
    request_ctx_factory,
):
    """Verifica filtrado temporal mediante `between` sobre un campo datetime."""
    flows, _ = small_flowdataset_factory()
    issues = []
    request_ctx = request_ctx_factory(
        flows,
        where_provided=True,
        h3_cells_provided=False,
    )

    mask, info = _evaluate_where_mask_on_flows_df(
        flows.flows,
        where={
            "window_start_utc": {
                "between": [
                    "2026-01-01T10:00:00Z",
                    "2026-01-01T12:00:00Z",
                ]
            }
        },
        issues=issues,
        request_ctx=request_ctx,
    )

    assert mask.tolist() == [True, True, False, False]
    assert info["applied"] is True
    assert info["fields_evaluated"] == ["window_start_utc"]
    assert issues == []


def test_evaluate_where_mask_omits_missing_field_clause_and_reports_issue(
    small_flowdataset_factory,
    request_ctx_factory,
    assert_issue_codes,
):
    """Verifica omisión completa del eje si todas las cláusulas usan campos inexistentes."""
    flows, _ = small_flowdataset_factory()
    issues = []
    request_ctx = request_ctx_factory(
        flows,
        where_provided=True,
        h3_cells_provided=False,
    )

    mask, info = _evaluate_where_mask_on_flows_df(
        flows.flows,
        where={"campo_inexistente": "x"},
        issues=issues,
        request_ctx=request_ctx,
    )

    assert mask is None
    assert info["applied"] is False

    assert_issue_codes(
        issues,
        expected_present=[
            "FLT_FLOW.WHERE.FIELD_MISSING",
        ],
    )


def test_evaluate_where_mask_omits_incompatible_operator_clause_and_reports_issue(
    small_flowdataset_factory,
    request_ctx_factory,
    assert_issue_codes,
):
    """Verifica issue y omisión cuando el operador no es compatible con el dtype."""
    flows, _ = small_flowdataset_factory()
    issues = []
    request_ctx = request_ctx_factory(
        flows,
        where_provided=True,
        h3_cells_provided=False,
    )

    mask, info = _evaluate_where_mask_on_flows_df(
        flows.flows,
        where={"mode": {"gte": 3}},
        issues=issues,
        request_ctx=request_ctx,
    )

    assert mask is None
    assert info["applied"] is False

    assert_issue_codes(
        issues,
        expected_present=[
            "FLT_FLOW.WHERE.OPERATOR_INCOMPATIBLE",
        ],
    )


def test_evaluate_where_mask_omits_datetime_clause_when_parse_fails(
    small_flowdataset_factory,
    request_ctx_factory,
    assert_issue_codes,
):
    """Verifica issue y omisión si un valor temporal no puede parsearse."""
    flows, _ = small_flowdataset_factory()
    issues = []
    request_ctx = request_ctx_factory(
        flows,
        where_provided=True,
        h3_cells_provided=False,
    )

    mask, info = _evaluate_where_mask_on_flows_df(
        flows.flows,
        where={"window_start_utc": {"gte": "no_es_fecha"}},
        issues=issues,
        request_ctx=request_ctx,
    )

    assert mask is None
    assert info["applied"] is False

    assert_issue_codes(
        issues,
        expected_present=[
            "FLT_FLOW.WHERE.DATETIME_PARSE_FAILED",
        ],
    )


def test_evaluate_where_mask_keeps_valid_clause_when_another_clause_is_invalid(
    small_flowdataset_factory,
    request_ctx_factory,
    assert_issue_codes,
):
    """Verifica aplicación parcial de `where` cuando una cláusula válida coexiste con otra inválida."""
    flows, _ = small_flowdataset_factory()
    issues = []
    request_ctx = request_ctx_factory(
        flows,
        where_provided=True,
        h3_cells_provided=False,
    )

    mask, info = _evaluate_where_mask_on_flows_df(
        flows.flows,
        where={
            "mode": "bus",
            "campo_inexistente": "x",
        },
        issues=issues,
        request_ctx=request_ctx,
    )

    assert mask.tolist() == [True, False, False, True]
    assert info["applied"] is True
    assert info["fields_evaluated"] == ["mode"]

    assert_issue_codes(
        issues,
        expected_present=[
            "FLT_FLOW.WHERE.FIELD_MISSING",
        ],
    )


# -----------------------------------------------------------------------------
# Bloque 4. Helper principal _evaluate_h3_mask_on_flows_df
# -----------------------------------------------------------------------------


def test_evaluate_h3_mask_filters_by_origin_predicate(
    small_flowdataset_factory,
    request_ctx_factory,
):
    """Verifica filtrado H3 básico sobre el extremo origen."""
    flows, cells = small_flowdataset_factory()
    issues = []
    request_ctx = request_ctx_factory(
        flows,
        where_provided=False,
        h3_cells_provided=True,
        spatial_predicate="origin",
    )

    mask, info = _evaluate_h3_mask_on_flows_df(
        flows.flows,
        h3_cells=[cells["origin_a"]],
        spatial_predicate="origin",
        issues=issues,
        request_ctx=request_ctx,
    )

    assert mask.tolist() == [True, True, False, False]
    assert info["applied"] is True
    assert info["valid_cells_count"] == 1
    assert issues == []


def test_evaluate_h3_mask_filters_with_both_predicate(
    small_flowdataset_factory,
    request_ctx_factory,
):
    """Verifica filtrado H3 con predicado `both` sobre origen y destino."""
    flows, cells = small_flowdataset_factory()
    issues = []
    request_ctx = request_ctx_factory(
        flows,
        where_provided=False,
        h3_cells_provided=True,
        spatial_predicate="both",
    )

    mask, info = _evaluate_h3_mask_on_flows_df(
        flows.flows,
        h3_cells=[cells["origin_a"], cells["dest_a"]],
        spatial_predicate="both",
        issues=issues,
        request_ctx=request_ctx,
    )

    assert mask.tolist() == [True, False, False, False]
    assert info["applied"] is True
    assert issues == []


def test_evaluate_h3_mask_keeps_valid_cells_and_reports_invalid_values(
    small_flowdataset_factory,
    request_ctx_factory,
    assert_issue_codes,
):
    """Verifica que celdas H3 inválidas se reporten sin impedir usar las válidas."""
    flows, cells = small_flowdataset_factory()
    issues = []
    request_ctx = request_ctx_factory(
        flows,
        where_provided=False,
        h3_cells_provided=True,
        spatial_predicate="origin",
    )

    mask, info = _evaluate_h3_mask_on_flows_df(
        flows.flows,
        h3_cells=[cells["origin_a"], "celda_h3_invalida_total"],
        spatial_predicate="origin",
        issues=issues,
        request_ctx=request_ctx,
    )

    assert mask.tolist() == [True, True, False, False]
    assert info["applied"] is True

    assert_issue_codes(
        issues,
        expected_present=[
            "FLT_FLOW.H3.INVALID_CELL_VALUES",
        ],
    )


def test_evaluate_h3_mask_omits_axis_when_required_columns_are_missing(
    small_flowdataset_factory,
    request_ctx_factory,
    assert_issue_codes,
):
    """Verifica omisión del eje H3 si faltan columnas requeridas por el predicado espacial."""
    flows, cells = small_flowdataset_factory()
    flows_df_bad = flows.flows.drop(columns=["destination_h3_index"])

    issues = []
    request_ctx = request_ctx_factory(
        flows,
        where_provided=False,
        h3_cells_provided=True,
        spatial_predicate="both",
    )

    mask, info = _evaluate_h3_mask_on_flows_df(
        flows_df_bad,
        h3_cells=[cells["origin_a"]],
        spatial_predicate="both",
        issues=issues,
        request_ctx=request_ctx,
    )

    assert mask is None
    assert info["applied"] is False

    assert_issue_codes(
        issues,
        expected_present=[
            "FLT_FLOW.H3.COLUMNS_MISSING",
        ],
    )


def test_evaluate_h3_mask_omits_axis_when_all_requested_cells_are_invalid(
    small_flowdataset_factory,
    request_ctx_factory,
    assert_issue_codes,
):
    """Verifica omisión del eje H3 si ninguna celda solicitada resulta válida."""
    flows, _ = small_flowdataset_factory()
    issues = []
    request_ctx = request_ctx_factory(
        flows,
        where_provided=False,
        h3_cells_provided=True,
        spatial_predicate="origin",
    )

    mask, info = _evaluate_h3_mask_on_flows_df(
        flows.flows,
        h3_cells=["bad_1", "bad_2"],
        spatial_predicate="origin",
        issues=issues,
        request_ctx=request_ctx,
    )

    assert mask is None
    assert info["applied"] is False

    assert_issue_codes(
        issues,
        expected_present=[
            "FLT_FLOW.H3.INVALID_CELL_VALUES",
        ],
    )