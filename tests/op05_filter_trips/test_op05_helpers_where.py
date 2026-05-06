from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from pylondrina.datasets import TripDataset
from pylondrina.reports import Issue
from pylondrina.schema import DomainSpec, FieldSpec, TripSchema, TripSchemaEffective
from pylondrina.transforms.filtering import (
    _allowed_ops_for_dtype,
    _build_where_mask,
    _normalize_where_field_clause,
    _validate_where_operator_value,
)


def make_filter_field(
    name: str,
    dtype: str,
    *,
    required: bool = False,
    domain: DomainSpec | None = None,
) -> FieldSpec:
    """Construye un FieldSpec mínimo para fixtures de OP-05."""
    return FieldSpec(
        name=name,
        dtype=dtype,
        required=required,
        domain=domain,
    )


@pytest.fixture()
def base_filter_schema() -> TripSchema:
    """Schema mínimo para probar el DSL where de OP-05."""
    fields = {
        "movement_id": make_filter_field("movement_id", "string", required=True),
        "user_id": make_filter_field("user_id", "string", required=True),
        "mode": make_filter_field(
            "mode",
            "categorical",
            domain=DomainSpec(values=["bus", "metro", "car", "walk"], extendable=True),
        ),
        "purpose": make_filter_field(
            "purpose",
            "categorical",
            domain=DomainSpec(values=["work", "study", "leisure"], extendable=True),
        ),
        "distance_km": make_filter_field("distance_km", "float"),
        "is_peak": make_filter_field("is_peak", "bool"),
    }

    return TripSchema(
        version="test-1.0",
        fields=fields,
        required=["movement_id", "user_id"],
    )


@pytest.fixture()
def make_filter_tripdataset(base_filter_schema: TripSchema):
    """Factory mínima de TripDataset para probar máscaras atributivas."""

    def _make(data: pd.DataFrame | None = None) -> TripDataset:
        df = (
            data.copy(deep=True)
            if data is not None
            else pd.DataFrame(
                {
                    "movement_id": ["m0", "m1", "m2", "m3", "m4"],
                    "user_id": ["u0", "u1", "u2", "u3", "u4"],
                    "mode": ["bus", "metro", "car", "walk", "bus"],
                    "purpose": ["work", "study", "work", None, "leisure"],
                    "distance_km": [5.0, 12.5, 1.2, 0.4, 25.0],
                    "is_peak": [True, True, False, False, True],
                }
            )
        )

        schema_effective = TripSchemaEffective(
            dtype_effective={
                "movement_id": "string",
                "user_id": "string",
                "mode": "categorical",
                "purpose": "categorical",
                "distance_km": "float",
                "is_peak": "bool",
            },
            domains_effective={
                "mode": {"values": ["bus", "metro", "car", "walk"]},
                "purpose": {"values": ["work", "study", "leisure"]},
            },
            fields_effective=list(df.columns),
        )

        return TripDataset(
            data=df,
            schema=base_filter_schema,
            schema_version=base_filter_schema.version,
            provenance={"source": {"name": "synthetic_filter_where_tests"}},
            field_correspondence={},
            value_correspondence={},
            metadata={
                "dataset_id": "ds_filter_where_small",
                "is_validated": True,
                "events": [],
                "domains_effective": schema_effective.domains_effective,
            },
            schema_effective=schema_effective,
        )

    return _make


def _issue_codes(issues: list[Issue]) -> list[str]:
    """Retorna códigos de issues en el orden emitido."""
    return [issue.code for issue in issues]


def _kept_ids(mask: pd.Series, trips: TripDataset) -> list[str]:
    """Retorna movement_id retenidos por una máscara booleana."""
    return trips.data.loc[mask, "movement_id"].tolist()


def test_normalize_where_field_clause_accepts_scalar_sequence_and_mapping() -> None:
    """Verifica las formas válidas del DSL where: escalar, secuencia y mapping."""
    scalar_clause, scalar_shape = _normalize_where_field_clause("work")
    assert scalar_clause == {"eq": "work"}
    assert scalar_shape == "scalar"

    list_clause, list_shape = _normalize_where_field_clause(["bus", "metro"])
    assert list_clause == {"in": ["bus", "metro"]}
    assert list_shape == "sequence"

    tuple_clause, tuple_shape = _normalize_where_field_clause(("bus", "metro"))
    assert tuple_clause == {"in": ["bus", "metro"]}
    assert tuple_shape == "sequence"

    mapping_clause, mapping_shape = _normalize_where_field_clause({"gte": 1.0, "lt": 10.0})
    assert mapping_clause == {"gte": 1.0, "lt": 10.0}
    assert mapping_shape == "mapping"


def test_normalize_where_field_clause_rejects_set_shape() -> None:
    """Verifica que set no sea aceptado como forma válida del DSL where."""
    clause, shape = _normalize_where_field_clause({"work", "study"})

    assert clause is None
    assert shape == "set"


def test_allowed_ops_for_dtype_matches_where_contract() -> None:
    """Verifica la matriz básica dtype-operador usada por el DSL where."""
    categorical_ops = _allowed_ops_for_dtype("categorical")
    assert {"eq", "ne", "in", "not_in", "is_null", "not_null"}.issubset(categorical_ops)
    assert "gt" not in categorical_ops
    assert "between" not in categorical_ops

    numeric_ops = _allowed_ops_for_dtype("float")
    assert {"eq", "in", "gt", "gte", "lt", "lte", "between"}.issubset(numeric_ops)

    bool_ops = _allowed_ops_for_dtype("bool")
    assert {"eq", "ne", "is_null", "not_null"}.issubset(bool_ops)
    assert "in" not in bool_ops
    assert "gt" not in bool_ops

    fallback_ops = _allowed_ops_for_dtype(None)
    assert {"eq", "ne", "in", "not_in", "is_null", "not_null"}.issubset(fallback_ops)


def test_validate_where_operator_value_accepts_valid_shapes_by_operator() -> None:
    """Verifica shapes válidos para operadores atributivos soportados."""
    ok, _ = _validate_where_operator_value("eq", "work", "categorical")
    assert ok is True

    ok, _ = _validate_where_operator_value("gt", 3.5, "float")
    assert ok is True

    ok, _ = _validate_where_operator_value("between", [1.0, 5.0], "float")
    assert ok is True

    ok, _ = _validate_where_operator_value("in", ["bus", "metro"], "categorical")
    assert ok is True

    ok, _ = _validate_where_operator_value("is_null", True, "string")
    assert ok is True

    ok, _ = _validate_where_operator_value("eq", "2026-01-01T07:00:00Z", "datetime")
    assert ok is True


def test_validate_where_operator_value_rejects_invalid_shapes_by_operator() -> None:
    """Verifica shapes inválidos para operadores donde el contrato exige forma específica."""
    ok, _ = _validate_where_operator_value("gt", "3.5", "float")
    assert ok is False

    ok, _ = _validate_where_operator_value("between", [1.0], "float")
    assert ok is False

    ok, _ = _validate_where_operator_value("in", [], "categorical")
    assert ok is False

    ok, _ = _validate_where_operator_value("is_null", False, "string")
    assert ok is False

    ok, _ = _validate_where_operator_value(
        "eq",
        pd.Timestamp("2026-01-01T07:00:00Z"),
        "datetime",
    )
    assert ok is False


def test_build_where_mask_applies_and_between_fields_and_operators(make_filter_tripdataset) -> None:
    """Verifica máscara where con AND entre campos y operadores del mismo campo."""
    trips = make_filter_tripdataset()
    issues: list[Issue] = []

    where = {
        "mode": ["bus", "metro"],
        "distance_km": {"gte": 1.0, "lt": 15.0},
        "is_peak": True,
    }

    mask, applied, omitted = _build_where_mask(
        trips,
        where=where,
        sample_rows_per_issue=3,
        issues=issues,
    )

    expected_mask = (
        trips.data["mode"].isin(where["mode"])
        & trips.data["distance_km"].ge(where["distance_km"]["gte"])
        & trips.data["distance_km"].lt(where["distance_km"]["lt"])
        & trips.data["is_peak"].eq(where["is_peak"])
    )

    assert applied is True
    assert omitted is False
    pd.testing.assert_series_equal(mask, expected_mask)

    assert _kept_ids(mask, trips) == _kept_ids(expected_mask, trips)
    assert "FLT.INFO.WHERE_APPLIED" in _issue_codes(issues)


def test_build_where_mask_omits_non_applicable_clauses_with_recoverable_issues(
    make_filter_tripdataset,
) -> None:
    """Verifica degradación controlada por campo inexistente, operador incompatible y shape inválido."""
    trips = make_filter_tripdataset()
    issues: list[Issue] = []

    where = {
        "does_not_exist": "x",
        "mode": {"gt": "bus"},
        "purpose": {"in": {"work"}},
    }

    mask, applied, omitted = _build_where_mask(
        trips,
        where=where,
        sample_rows_per_issue=3,
        issues=issues,
    )

    assert mask is None
    assert applied is False
    assert omitted is True

    assert set(_issue_codes(issues)) == {
        "FLT.WHERE.FIELD_NOT_FOUND",
        "FLT.WHERE.OP_INCOMPATIBLE",
        "FLT.WHERE.INVALID_VALUE_SHAPE",
    }