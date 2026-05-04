from __future__ import annotations

import pandas as pd
import pytest

from pylondrina.fixing import apply_field_corrections, _resolve_field_corrections
from pylondrina.reports import Issue
from pylondrina.schema import DomainSpec, FieldSpec, TripSchema


def make_field(
    name: str,
    dtype: str,
    *,
    required: bool = False,
    constraints: dict | None = None,
    domain: DomainSpec | None = None,
) -> FieldSpec:
    """Construye un FieldSpec mínimo equivalente al usado en el notebook helper-level."""
    return FieldSpec(
        name=name,
        dtype=dtype,
        required=required,
        constraints=constraints,
        domain=domain,
    )


@pytest.fixture()
def base_fix_schema() -> TripSchema:
    """Schema mínimo usado para resolver correcciones de campos en OP-03."""
    fields = [
        make_field("movement_id", "string", required=True),
        make_field("user_id", "string", required=True),
        make_field(
            "mode",
            "categorical",
            domain=DomainSpec(
                values=["walk", "bus", "metro", "car", "unknown"],
                extendable=True,
            ),
        ),
        make_field(
            "purpose",
            "categorical",
            domain=DomainSpec(
                values=["work", "study", "shopping", "unknown"],
                extendable=True,
            ),
        ),
        make_field("trip_weight", "float"),
    ]
    return TripSchema(
        version="1.1",
        fields={field.name: field for field in fields},
        required=[field.name for field in fields if field.required],
        semantic_rules=None,
    )


def _issue_codes(issues: list[Issue]) -> list[str]:
    """Retorna los códigos de issues en el orden emitido por el helper."""
    return [issue.code for issue in issues]


def _assert_issue_present(issues: list[Issue], code: str) -> None:
    """Falla explícitamente si no aparece el código de issue esperado."""
    assert code in _issue_codes(issues)


def test_apply_field_corrections_renames_columns_without_mutating_source() -> None:
    """Verifica que el renombrado puro preserve datos, índice y no mute el DataFrame fuente."""
    df_source = pd.DataFrame(
        {
            "movement_id": ["m1", "m2"],
            "user_id": ["u1", "u2"],
            "modo": ["BUS", "walk"],
            "proposito": ["work", "study"],
        }
    )
    df_source_before = df_source.copy(deep=True)
    corrections = {"modo": "mode", "proposito": "purpose"}

    df_out = apply_field_corrections(df_source, corrections)

    expected = df_source_before.rename(columns=corrections)
    pd.testing.assert_frame_equal(df_out, expected)
    pd.testing.assert_frame_equal(df_source, df_source_before)

    assert list(df_out.columns) == ["movement_id", "user_id", "mode", "purpose"]
    assert list(df_out.index) == list(df_source_before.index)


def test_resolve_field_corrections_identifies_applicable_rules_and_effective_mapping(
    base_fix_schema: TripSchema,
) -> None:
    """Verifica que reglas válidas produzcan mapping aplicable, campos efectivos y cero issues."""
    df_source = pd.DataFrame(
        {
            "movement_id": ["m1", "m2"],
            "user_id": ["u1", "u2"],
            "modo": ["BUS", "walk"],
            "proposito": ["work", "study"],
        }
    )
    requested_corrections = {"modo": "mode", "proposito": "purpose"}
    current_correspondence = {"movement_id": "movement_id", "user_id": "user_id"}

    (
        applicable,
        field_corr_final,
        fields_effective,
        issues,
        applied_count,
        semantic_change,
    ) = _resolve_field_corrections(
        df_source,
        schema=base_fix_schema,
        field_corrections=requested_corrections,
        field_correspondence_current=current_correspondence.copy(),
        sample_rows_per_issue=5,
        n_rows_total=len(df_source),
    )

    assert applicable == requested_corrections
    assert field_corr_final["mode"] == "modo"
    assert field_corr_final["purpose"] == "proposito"
    assert "modo" not in field_corr_final
    assert "proposito" not in field_corr_final
    assert {"movement_id", "user_id", "mode", "purpose"}.issubset(set(fields_effective))
    assert issues == []
    assert applied_count == len(applicable)
    assert semantic_change is True


def test_resolve_field_corrections_degrades_missing_source_and_applies_valid_rules(
    base_fix_schema: TripSchema,
) -> None:
    """Verifica aplicación parcial cuando una columna origen falta y otra regla sí es aplicable."""
    df_source = pd.DataFrame(
        {
            "movement_id": ["m1", "m2"],
            "user_id": ["u1", "u2"],
            "modo": ["BUS", "walk"],
        }
    )
    requested_corrections = {
        "modo": "mode",
        "missing_col": "purpose",
    }

    (
        applicable,
        field_corr_final,
        fields_effective,
        issues,
        applied_count,
        semantic_change,
    ) = _resolve_field_corrections(
        df_source,
        schema=base_fix_schema,
        field_corrections=requested_corrections,
        field_correspondence_current={"movement_id": "movement_id", "user_id": "user_id"},
        sample_rows_per_issue=5,
        n_rows_total=len(df_source),
    )

    assert applicable == {"modo": "mode"}
    assert field_corr_final["mode"] == "modo"
    assert "missing_col" not in field_corr_final
    assert "mode" in set(fields_effective)
    assert "purpose" not in set(fields_effective)
    assert applied_count == len(applicable)
    assert semantic_change is True

    _assert_issue_present(issues, "FIX.FIELD.SOURCE_COLUMN_MISSING")
    _assert_issue_present(issues, "FIX.FIELD.PARTIAL_APPLY")


def test_resolve_field_corrections_rejects_canonical_to_canonical_rule(
    base_fix_schema: TripSchema,
) -> None:
    """Verifica que OP-03 no permita renombrar una columna canónica hacia otra canónica."""
    df_source = pd.DataFrame(
        {
            "movement_id": ["m1"],
            "user_id": ["u1"],
            "mode": ["bus"],
            "purpose": ["work"],
        }
    )

    (
        applicable,
        field_corr_final,
        fields_effective,
        issues,
        applied_count,
        semantic_change,
    ) = _resolve_field_corrections(
        df_source,
        schema=base_fix_schema,
        field_corrections={"mode": "purpose"},
        field_correspondence_current={},
        sample_rows_per_issue=5,
        n_rows_total=len(df_source),
    )

    assert applicable == {}
    assert field_corr_final == {}
    assert set(fields_effective) == {"movement_id", "user_id", "mode", "purpose"}
    assert applied_count == 0
    assert semantic_change is False
    _assert_issue_present(issues, "FIX.FIELD.RULE_NOT_ALLOWED")