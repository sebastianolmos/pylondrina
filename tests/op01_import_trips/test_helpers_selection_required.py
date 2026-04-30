import json

import pandas as pd
import pytest

from pylondrina.errors import ImportError as PylondrinaImportError
from pylondrina.importing import (
    ImportOptions,
    _final_required_check,
    _prune_schema_effective,
    _select_final_columns,
)
from pylondrina.schema import FieldSpec, TripSchema, TripSchemaEffective


# ---------------------------------------------------------------------
# Helpers locales de test
# ---------------------------------------------------------------------


def assert_json_safe(obj, label: str = "object") -> None:
    try:
        json.dumps(obj, default=str)
    except Exception as exc:
        raise AssertionError(f"{label} no es JSON-safe: {exc}") from exc


def assert_columns_equal(
    df: pd.DataFrame,
    expected_columns: list[str],
    label: str = "columns",
) -> None:
    assert list(df.columns) == expected_columns, f"{label}: columnas inesperadas"


def issue_codes(issues) -> list[str]:
    return [issue.code for issue in issues]


def assert_issue_present(issues, code: str) -> None:
    codes = issue_codes(issues)
    assert code in codes, f"No se encontró issue {code!r}. Issues encontrados: {codes!r}"


def assert_issue_absent(issues, code: str) -> None:
    codes = issue_codes(issues)
    assert code not in codes, f"No se esperaba issue {code!r}. Issues encontrados: {codes!r}"


# ---------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------


@pytest.fixture
def schema_g9() -> TripSchema:
    return TripSchema(
        version="0.1.0",
        fields={
            "movement_id": FieldSpec(name="movement_id", dtype="string", required=True),
            "trip_id": FieldSpec(name="trip_id", dtype="string", required=True),
            "movement_seq": FieldSpec(name="movement_seq", dtype="int", required=True),
            "purpose": FieldSpec(name="purpose", dtype="categorical", required=False),
            "mode": FieldSpec(name="mode", dtype="categorical", required=False),
            "origin_latitude": FieldSpec(name="origin_latitude", dtype="float", required=False),
            "origin_longitude": FieldSpec(name="origin_longitude", dtype="float", required=False),
            "destination_latitude": FieldSpec(name="destination_latitude", dtype="float", required=False),
            "destination_longitude": FieldSpec(name="destination_longitude", dtype="float", required=False),
        },
        required=["movement_id", "trip_id", "movement_seq"],
        semantic_rules=None,
    )


@pytest.fixture
def df_g9() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "movement_id": ["m0", "m1", "m2"],
            "trip_id": ["t0", "t1", "t2"],
            "movement_seq": pd.Series([0, 0, 0], dtype="Int64"),
            "purpose": ["work", "study", "home"],
            "mode": ["bus", "metro", "walk"],
            "origin_latitude": [-33.45, -33.46, -33.47],
            "origin_longitude": [-70.60, -70.61, -70.62],
            "destination_latitude": [-33.40, -33.41, -33.42],
            "destination_longitude": [-70.50, -70.51, -70.52],
            "raw_source_col": ["A", "B", "C"],
            "debug_flag": [True, False, True],
        }
    )


@pytest.fixture
def schema_effective_g9() -> TripSchemaEffective:
    return TripSchemaEffective(
        dtype_effective={
            "movement_id": "string",
            "trip_id": "string",
            "movement_seq": "int",
            "purpose": "categorical",
            "mode": "categorical",
            "origin_latitude": "float",
            "origin_longitude": "float",
            "destination_latitude": "float",
            "destination_longitude": "float",
            "raw_source_col": "string",
        },
        overrides={
            "purpose": {"reasons": ["domain_extended"], "added_values": ["home"]},
            "mode": {"reasons": ["domain_extended"], "added_values": ["tram"]},
            "raw_source_col": {"reasons": ["debug_only"]},
        },
        domains_effective={
            "purpose": {
                "values": ["unknown", "work", "study", "home"],
                "extended": True,
                "added_values": ["home"],
                "unknown_value": "unknown",
                "unknown_values": [],
            },
            "mode": {
                "values": ["unknown", "bus", "metro", "walk", "tram"],
                "extended": True,
                "added_values": ["tram"],
                "unknown_value": "unknown",
                "unknown_values": [],
            },
            "raw_source_col": {
                "values": ["A", "B", "C"],
                "extended": False,
                "added_values": [],
                "unknown_value": "unknown",
                "unknown_values": [],
            },
        },
        temporal={},
        fields_effective=[],
    )


def copy_schema_effective(schema_effective: TripSchemaEffective) -> TripSchemaEffective:
    return TripSchemaEffective(
        dtype_effective=dict(schema_effective.dtype_effective),
        overrides={k: dict(v) for k, v in schema_effective.overrides.items()},
        domains_effective={k: dict(v) for k, v in schema_effective.domains_effective.items()},
        temporal=dict(schema_effective.temporal),
        fields_effective=list(schema_effective.fields_effective),
    )


# ---------------------------------------------------------------------
# Tests de _select_final_columns
# ---------------------------------------------------------------------


def test_select_final_columns_none_selected_fields_drops_extra_fields(schema_g9, df_g9):
    """Verifica que selected_fields=None conserve todos los campos del schema y elimine extras si keep_extra_fields=False."""
    options = ImportOptions(
        selected_fields=None,
        keep_extra_fields=False,
    )

    work, columns_deleted, extra_fields_kept, issues = _select_final_columns(
        df_g9.copy(deep=True),
        schema=schema_g9,
        options=options,
    )

    assert_columns_equal(
        work,
        [
            "movement_id",
            "trip_id",
            "movement_seq",
            "purpose",
            "mode",
            "origin_latitude",
            "origin_longitude",
            "destination_latitude",
            "destination_longitude",
        ],
        "selected_fields=None + keep_extra_fields=False",
    )

    assert set(columns_deleted) == {"raw_source_col", "debug_flag"}
    assert extra_fields_kept == []
    assert_issue_present(issues, "IMP.OPTIONS.EXTRA_FIELDS_DROPPED")


def test_select_final_columns_empty_selected_fields_keeps_required_only(schema_g9, df_g9):
    """Verifica que selected_fields=[] conserve solo campos required y elimine el resto si keep_extra_fields=False."""
    options = ImportOptions(
        selected_fields=[],
        keep_extra_fields=False,
    )

    work, columns_deleted, extra_fields_kept, issues = _select_final_columns(
        df_g9.copy(deep=True),
        schema=schema_g9,
        options=options,
    )

    assert_columns_equal(
        work,
        ["movement_id", "trip_id", "movement_seq"],
        "selected_fields=[] + keep_extra_fields=False",
    )

    assert set(columns_deleted) == {
        "purpose",
        "mode",
        "origin_latitude",
        "origin_longitude",
        "destination_latitude",
        "destination_longitude",
        "raw_source_col",
        "debug_flag",
    }
    assert extra_fields_kept == []
    assert_issue_present(issues, "IMP.OPTIONS.EXTRA_FIELDS_DROPPED")


def test_select_final_columns_subset_keeps_extra_fields(schema_g9, df_g9):
    """Verifica que un subconjunto de selected_fields conserve required, selected y extras cuando keep_extra_fields=True."""
    options = ImportOptions(
        selected_fields=["purpose", "origin_latitude", "origin_longitude"],
        keep_extra_fields=True,
    )

    work, columns_deleted, extra_fields_kept, issues = _select_final_columns(
        df_g9.copy(deep=True),
        schema=schema_g9,
        options=options,
    )

    assert_columns_equal(
        work,
        [
            "movement_id",
            "trip_id",
            "movement_seq",
            "purpose",
            "origin_latitude",
            "origin_longitude",
            "raw_source_col",
            "debug_flag",
        ],
        "subset + keep_extra_fields=True",
    )

    assert set(columns_deleted) == {"mode", "destination_latitude", "destination_longitude"}
    assert set(extra_fields_kept) == {"raw_source_col", "debug_flag"}
    assert_issue_absent(issues, "IMP.OPTIONS.EXTRA_FIELDS_DROPPED")


def test_select_final_columns_subset_drops_extra_fields(schema_g9, df_g9):
    """Verifica que un subconjunto de selected_fields conserve required y selected, pero elimine extras si keep_extra_fields=False."""
    options = ImportOptions(
        selected_fields=["purpose", "origin_latitude", "origin_longitude"],
        keep_extra_fields=False,
    )

    work, columns_deleted, extra_fields_kept, issues = _select_final_columns(
        df_g9.copy(deep=True),
        schema=schema_g9,
        options=options,
    )

    assert_columns_equal(
        work,
        [
            "movement_id",
            "trip_id",
            "movement_seq",
            "purpose",
            "origin_latitude",
            "origin_longitude",
        ],
        "subset + keep_extra_fields=False",
    )

    assert set(columns_deleted) == {
        "mode",
        "destination_latitude",
        "destination_longitude",
        "raw_source_col",
        "debug_flag",
    }
    assert extra_fields_kept == []
    assert_issue_present(issues, "IMP.OPTIONS.EXTRA_FIELDS_DROPPED")


# ---------------------------------------------------------------------
# Tests de _final_required_check
# ---------------------------------------------------------------------


def test_final_required_check_happy_path(schema_g9):
    """Verifica que el chequeo final no falle cuando todos los required del schema están presentes."""
    df = pd.DataFrame(
        {
            "movement_id": ["m0", "m1"],
            "trip_id": ["t0", "t1"],
            "movement_seq": pd.Series([0, 0], dtype="Int64"),
            "purpose": ["work", "study"],
        }
    )

    _final_required_check(
        df,
        schema=schema_g9,
        single_stage=True,
        strict=False,
    )


def test_final_required_check_missing_required_field_raises(schema_g9):
    """Verifica que el chequeo final aborte cuando desaparece un campo required del schema."""
    df = pd.DataFrame(
        {
            "movement_id": ["m0", "m1"],
            "trip_id": ["t0", "t1"],
            "purpose": ["work", "study"],
        }
    )

    with pytest.raises(PylondrinaImportError) as exc_info:
        _final_required_check(
            df,
            schema=schema_g9,
            single_stage=True,
            strict=False,
        )

    assert_issue_present(exc_info.value.issues, "IMP.INPUT.MISSING_REQUIRED_FIELD")

    issue = exc_info.value.issue
    assert issue.code == "IMP.INPUT.MISSING_REQUIRED_FIELD"
    assert issue.details["missing_required"] == ["movement_seq"]
    assert issue.details["required"] == ["movement_id", "trip_id", "movement_seq"]


def test_final_required_check_single_stage_false_still_respects_schema_required(schema_g9):
    """Verifica que single_stage=False no agregue exigencias extra, pero sí respete los required declarados por el schema."""
    df = pd.DataFrame(
        {
            "movement_id": ["m0", "m1"],
            "trip_id": ["t0", "t1"],
            "movement_seq": pd.Series([0, 0], dtype="Int64"),
        }
    )

    _final_required_check(
        df,
        schema=schema_g9,
        single_stage=False,
        strict=False,
    )


def test_final_required_check_single_stage_true_requires_runtime_trip_fields():
    """Verifica que single_stage=True exija trip_id y movement_seq aunque no estén declarados como required en el schema."""
    schema = TripSchema(
        version="0.1.0",
        fields={
            "movement_id": FieldSpec(name="movement_id", dtype="string", required=True),
        },
        required=["movement_id"],
        semantic_rules=None,
    )

    df = pd.DataFrame(
        {
            "movement_id": ["m0", "m1"],
        }
    )

    with pytest.raises(PylondrinaImportError) as exc_info:
        _final_required_check(
            df,
            schema=schema,
            single_stage=True,
            strict=False,
        )

    assert_issue_present(exc_info.value.issues, "IMP.INPUT.MISSING_REQUIRED_FIELD")

    issue = exc_info.value.issue
    assert issue.code == "IMP.INPUT.MISSING_REQUIRED_FIELD"
    assert issue.details["missing_required"] == ["movement_seq", "trip_id"]
    assert issue.details["required"] == ["movement_id"]


# ---------------------------------------------------------------------
# Tests de _prune_schema_effective
# ---------------------------------------------------------------------


def test_prune_schema_effective_keeps_only_schema_fields_present_in_dataframe(
    schema_g9,
    schema_effective_g9,
):
    """Verifica que schema_effective se pode según los campos finales presentes y definidos en el schema."""
    df = pd.DataFrame(
        {
            "movement_id": ["m0", "m1"],
            "trip_id": ["t0", "t1"],
            "movement_seq": pd.Series([0, 0], dtype="Int64"),
            "purpose": ["work", "home"],
            "origin_latitude": [-33.45, -33.46],
            "origin_longitude": [-70.60, -70.61],
            "raw_source_col": ["A", "B"],
        }
    )

    pruned = _prune_schema_effective(
        copy_schema_effective(schema_effective_g9),
        df=df,
        schema=schema_g9,
    )

    assert pruned.fields_effective == [
        "movement_id",
        "trip_id",
        "movement_seq",
        "purpose",
        "origin_latitude",
        "origin_longitude",
    ]

    assert set(pruned.dtype_effective.keys()) == {
        "movement_id",
        "trip_id",
        "movement_seq",
        "purpose",
        "origin_latitude",
        "origin_longitude",
    }

    assert set(pruned.overrides.keys()) == {"purpose"}
    assert set(pruned.domains_effective.keys()) == {"purpose"}

    assert_json_safe(pruned.to_dict(), "pruned_schema_effective")


# ---------------------------------------------------------------------
# Test integrado pequeño
# ---------------------------------------------------------------------


def test_selection_and_prune_integrated_small_case(
    schema_g9,
    df_g9,
    schema_effective_g9,
):
    """Verifica el encadenamiento select + prune con selected_fields parcial y eliminación de extras."""
    options = ImportOptions(
        selected_fields=["purpose", "origin_latitude"],
        keep_extra_fields=False,
    )

    work, columns_deleted, extra_fields_kept, issues = _select_final_columns(
        df_g9.copy(deep=True),
        schema=schema_g9,
        options=options,
    )

    pruned = _prune_schema_effective(
        copy_schema_effective(schema_effective_g9),
        df=work,
        schema=schema_g9,
    )

    assert_columns_equal(
        work,
        ["movement_id", "trip_id", "movement_seq", "purpose", "origin_latitude"],
        "integrated select+prune columns",
    )

    assert set(columns_deleted) == {
        "mode",
        "origin_longitude",
        "destination_latitude",
        "destination_longitude",
        "raw_source_col",
        "debug_flag",
    }
    assert extra_fields_kept == []
    assert_issue_present(issues, "IMP.OPTIONS.EXTRA_FIELDS_DROPPED")

    assert pruned.fields_effective == [
        "movement_id",
        "trip_id",
        "movement_seq",
        "purpose",
        "origin_latitude",
    ]

    assert set(pruned.domains_effective.keys()) == {"purpose"}
    assert set(pruned.overrides.keys()) == {"purpose"}