from __future__ import annotations

import pandas as pd
import pytest

from pylondrina.fixing import apply_value_corrections, _resolve_value_corrections
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
    """Schema mínimo usado para resolver correcciones de valores en OP-03."""
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


def test_apply_value_corrections_recodes_values_preserves_nulls_and_source() -> None:
    """Verifica que la recodificación pura preserve nulos y no mute el DataFrame fuente."""
    df_source = pd.DataFrame(
        {
            "mode": ["BUS", "WALK", None, "BUS"],
            "purpose": ["work", "study", "work", "shopping"],
            "trip_weight": [1.0, 2.0, 3.0, 4.0],
        }
    )
    df_source_before = df_source.copy(deep=True)
    corrections = {
        "mode": {"BUS": "bus", "WALK": "walk"},
        "purpose": {"shopping": "shopping"},
    }

    df_out = apply_value_corrections(df_source, corrections)

    expected = df_source_before.copy(deep=True)
    expected["mode"] = expected["mode"].replace(corrections["mode"])
    expected["purpose"] = expected["purpose"].replace(corrections["purpose"])

    pd.testing.assert_frame_equal(df_out, expected)
    pd.testing.assert_frame_equal(df_source, df_source_before)
    assert df_out["mode"].isna().sum() == df_source_before["mode"].isna().sum()


def test_resolve_value_corrections_identifies_applicable_rules_and_effective_mapping(
    base_fix_schema: TripSchema,
) -> None:
    """Verifica que reglas válidas produzcan mapping aplicable, conteos y campos tocados."""
    df_source = pd.DataFrame(
        {
            "mode": ["BUS", "WALK", None, "BUS"],
            "purpose": ["work", "study", "work", "shopping"],
        }
    )
    requested_corrections = {"mode": {"BUS": "bus", "WALK": "walk"}}
    current_correspondence = {"mode": {"METRO": "metro"}}
    expected_replacements = int(df_source["mode"].isin(requested_corrections["mode"].keys()).sum())

    (
        applicable,
        value_corr_final,
        issues,
        applied_fields_count,
        replacements_count,
        touched_fields,
        semantic_change,
    ) = _resolve_value_corrections(
        df_source,
        schema=base_fix_schema,
        value_corrections=requested_corrections,
        value_correspondence_current=current_correspondence.copy(),
        sample_rows_per_issue=5,
        n_rows_total=len(df_source),
    )

    assert applicable == requested_corrections
    assert value_corr_final["mode"]["METRO"] == current_correspondence["mode"]["METRO"]
    assert value_corr_final["mode"]["BUS"] == requested_corrections["mode"]["BUS"]
    assert value_corr_final["mode"]["WALK"] == requested_corrections["mode"]["WALK"]
    assert issues == []
    assert applied_fields_count == len(applicable)
    assert replacements_count == expected_replacements
    assert touched_fields == list(applicable.keys())
    assert semantic_change is True


def test_resolve_value_corrections_degrades_missing_field_and_applies_valid_rules(
    base_fix_schema: TripSchema,
) -> None:
    """Verifica aplicación parcial cuando falta un campo y otro mapping sí es aplicable."""
    df_source = pd.DataFrame({"mode": ["BUS", "WALK", None, "BUS"]})
    requested_corrections = {
        "mode": {"BUS": "bus"},
        "missing_field": {"x": "y"},
    }
    expected_replacements = int(df_source["mode"].isin(requested_corrections["mode"].keys()).sum())

    (
        applicable,
        value_corr_final,
        issues,
        applied_fields_count,
        replacements_count,
        touched_fields,
        semantic_change,
    ) = _resolve_value_corrections(
        df_source,
        schema=base_fix_schema,
        value_corrections=requested_corrections,
        value_correspondence_current={},
        sample_rows_per_issue=5,
        n_rows_total=len(df_source),
    )

    assert applicable == {"mode": requested_corrections["mode"]}
    assert "missing_field" not in value_corr_final
    assert value_corr_final["mode"]["BUS"] == requested_corrections["mode"]["BUS"]
    assert applied_fields_count == len(applicable)
    assert replacements_count == expected_replacements
    assert touched_fields == list(applicable.keys())
    assert semantic_change is True
    _assert_issue_present(issues, "FIX.VALUE.FIELD_MISSING")
    _assert_issue_present(issues, "FIX.VALUE.PARTIAL_APPLY")


def test_resolve_value_corrections_rejects_non_categorical_field(
    base_fix_schema: TripSchema,
) -> None:
    """Verifica que OP-03 no aplique value corrections sobre campos no categóricos."""
    df_source = pd.DataFrame({"trip_weight": [1.0, 2.0, 3.0]})
    requested_corrections = {"trip_weight": {1.0: 10.0}}

    (
        applicable,
        value_corr_final,
        issues,
        applied_fields_count,
        replacements_count,
        touched_fields,
        semantic_change,
    ) = _resolve_value_corrections(
        df_source,
        schema=base_fix_schema,
        value_corrections=requested_corrections,
        value_correspondence_current={},
        sample_rows_per_issue=5,
        n_rows_total=len(df_source),
    )

    assert applicable == {}
    assert value_corr_final == {}
    assert applied_fields_count == 0
    assert replacements_count == 0
    assert touched_fields == []
    assert semantic_change is False
    _assert_issue_present(issues, "FIX.VALUE.FIELD_NOT_COMPATIBLE")


def test_resolve_value_corrections_warns_for_present_target_and_missing_source_values(
    base_fix_schema: TripSchema,
) -> None:
    """Verifica warnings por target ya presente y source values sin match, aplicando lo válido."""
    df_source = pd.DataFrame({"mode": ["BUS", "bus", "walk", None]})
    requested_corrections = {
        "mode": {
            "BUS": "bus",
            "TAXI": "taxi",
        }
    }
    expected_applicable = {"mode": {"BUS": requested_corrections["mode"]["BUS"]}}
    expected_replacements = int(df_source["mode"].isin(expected_applicable["mode"].keys()).sum())

    (
        applicable,
        value_corr_final,
        issues,
        applied_fields_count,
        replacements_count,
        touched_fields,
        semantic_change,
    ) = _resolve_value_corrections(
        df_source,
        schema=base_fix_schema,
        value_corrections=requested_corrections,
        value_correspondence_current={},
        sample_rows_per_issue=5,
        n_rows_total=len(df_source),
    )

    assert applicable == expected_applicable
    assert value_corr_final["mode"] == expected_applicable["mode"]
    assert applied_fields_count == len(applicable)
    assert replacements_count == expected_replacements
    assert touched_fields == list(applicable.keys())
    assert semantic_change is True
    _assert_issue_present(issues, "FIX.VALUE.TARGET_ALREADY_PRESENT")
    _assert_issue_present(issues, "FIX.VALUE.SOURCE_VALUES_NOT_FOUND")