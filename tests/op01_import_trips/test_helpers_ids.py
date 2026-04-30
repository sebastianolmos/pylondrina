import pandas as pd
import pytest

from pylondrina.errors import ImportError as PylondrinaImportError
from pylondrina.importing import _ensure_movement_id, _ensure_single_stage_ids
from pylondrina.schema import FieldSpec, TripSchema


# ---------------------------------------------------------------------
# Helpers locales de test
# ---------------------------------------------------------------------


def assert_columns_equal(
    df: pd.DataFrame,
    expected_columns: list[str],
    label: str = "columns",
) -> None:
    assert list(df.columns) == expected_columns, f"{label}: columnas inesperadas"


def assert_dtype(df: pd.DataFrame, column: str, expected_dtype: str) -> None:
    assert str(df[column].dtype) == expected_dtype


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
def schema_ids_required() -> TripSchema:
    return TripSchema(
        version="0.1.0",
        fields={
            "movement_id": FieldSpec(name="movement_id", dtype="string", required=True),
            "trip_id": FieldSpec(name="trip_id", dtype="string", required=True),
            "movement_seq": FieldSpec(name="movement_seq", dtype="int", required=True),
            "purpose": FieldSpec(name="purpose", dtype="categorical", required=False),
        },
        required=["movement_id", "trip_id", "movement_seq"],
        semantic_rules=None,
    )


@pytest.fixture
def schema_ids_not_required() -> TripSchema:
    return TripSchema(
        version="0.1.0",
        fields={
            "movement_id": FieldSpec(name="movement_id", dtype="string", required=True),
            "trip_id": FieldSpec(name="trip_id", dtype="string", required=False),
            "movement_seq": FieldSpec(name="movement_seq", dtype="int", required=False),
            "purpose": FieldSpec(name="purpose", dtype="categorical", required=False),
        },
        required=["movement_id"],
        semantic_rules=None,
    )


# ---------------------------------------------------------------------
# Tests de _ensure_movement_id
# ---------------------------------------------------------------------


def test_ensure_movement_id_existing_unique_is_passthrough():
    """Verifica que un movement_id existente y único no se modifique ni genere issues."""
    df = pd.DataFrame(
        {
            "movement_id": pd.Series(["m0", "m1", "m2"], dtype="string"),
            "purpose": ["work", "study", "home"],
        }
    )

    work, columns_added, issues = _ensure_movement_id(
        df.copy(deep=True),
        strict=False,
    )

    assert_columns_equal(work, ["movement_id", "purpose"], "movement_id existente sin duplicados")
    assert_dtype(work, "movement_id", "string")
    assert work["movement_id"].tolist() == ["m0", "m1", "m2"]
    assert columns_added == []
    assert issues == []


def test_ensure_movement_id_duplicate_raises_import_error():
    """Verifica que movement_id duplicado aborte porque rompe la unicidad de fila del TripDataset."""
    df = pd.DataFrame(
        {
            "movement_id": pd.Series(["m0", "m1", "m1", "m2"], dtype="string"),
            "purpose": ["work", "study", "home", "other"],
        }
    )

    with pytest.raises(PylondrinaImportError) as exc_info:
        _ensure_movement_id(
            df.copy(deep=True),
            strict=False,
        )

    assert_issue_present(exc_info.value.issues, "IMP.ID.MOVEMENT_ID_DUPLICATE")

    issue = exc_info.value.issue
    assert issue.code == "IMP.ID.MOVEMENT_ID_DUPLICATE"
    assert issue.details["duplicate_count"] == 2
    assert issue.details["duplicate_examples"] == ["m1"]
    assert issue.details["action"] == "abort"


def test_ensure_movement_id_missing_generates_sequential_ids():
    """Verifica que, si falta movement_id, se cree al inicio con IDs secuenciales m0, m1, ..."""
    df = pd.DataFrame(
        {
            "purpose": ["work", "study", "home"],
            "mode": ["bus", "metro", "walk"],
        }
    )

    work, columns_added, issues = _ensure_movement_id(
        df.copy(deep=True),
        strict=False,
    )

    assert_columns_equal(work, ["movement_id", "purpose", "mode"], "movement_id generado")
    assert_dtype(work, "movement_id", "string")
    assert work["movement_id"].tolist() == ["m0", "m1", "m2"]
    assert columns_added == ["movement_id"]
    assert_issue_present(issues, "IMP.ID.MOVEMENT_ID_CREATED")

    issue = issues[0]
    assert issue.code == "IMP.ID.MOVEMENT_ID_CREATED"
    assert issue.field == "movement_id"
    assert issue.details["field"] == "movement_id"
    assert issue.details["action"] == "generated"


def test_ensure_movement_id_missing_on_empty_dataframe_creates_empty_string_column():
    """Verifica el borde de DataFrame vacío: se agrega movement_id vacío con dtype string."""
    df = pd.DataFrame(
        {
            "purpose": pd.Series([], dtype="object"),
        }
    )

    work, columns_added, issues = _ensure_movement_id(
        df.copy(deep=True),
        strict=False,
    )

    assert_columns_equal(work, ["movement_id", "purpose"], "movement_id generado en DataFrame vacío")
    assert_dtype(work, "movement_id", "string")
    assert len(work) == 0
    assert columns_added == ["movement_id"]
    assert_issue_present(issues, "IMP.ID.MOVEMENT_ID_CREATED")


# ---------------------------------------------------------------------
# Tests de _ensure_single_stage_ids
# ---------------------------------------------------------------------


def test_ensure_single_stage_ids_true_generates_trip_id_and_movement_seq(schema_ids_required):
    """Verifica que single_stage=True genere trip_id desde movement_id y movement_seq=0 si faltan."""
    df = pd.DataFrame(
        {
            "movement_id": pd.Series(["m0", "m1", "m2"], dtype="string"),
            "purpose": ["work", "study", "home"],
        }
    )

    work, columns_added, issues = _ensure_single_stage_ids(
        df.copy(deep=True),
        schema=schema_ids_required,
        single_stage=True,
        strict=False,
    )

    assert_columns_equal(
        work,
        ["movement_id", "trip_id", "movement_seq", "purpose"],
        "single_stage=True genera IDs",
    )
    assert_dtype(work, "trip_id", "string")
    assert_dtype(work, "movement_seq", "Int64")

    assert work["trip_id"].tolist() == ["m0", "m1", "m2"]
    assert work["movement_seq"].tolist() == [0, 0, 0]

    assert columns_added == ["trip_id", "movement_seq"]
    assert_issue_present(issues, "IMP.ID.TRIP_ID_CREATED")
    assert_issue_present(issues, "IMP.ID.MOVEMENT_SEQ_CREATED")

    trip_issue = [issue for issue in issues if issue.code == "IMP.ID.TRIP_ID_CREATED"][0]
    seq_issue = [issue for issue in issues if issue.code == "IMP.ID.MOVEMENT_SEQ_CREATED"][0]
    assert trip_issue.field == "trip_id"
    assert trip_issue.details["action"] == "generated_from_movement_id"
    assert seq_issue.field == "movement_seq"
    assert seq_issue.details["action"] == "generated_zero"


def test_ensure_single_stage_ids_true_preserves_existing_trip_id_and_movement_seq(schema_ids_required):
    """Verifica que single_stage=True no sobrescriba trip_id ni movement_seq cuando ya existen."""
    df = pd.DataFrame(
        {
            "movement_id": pd.Series(["m0", "m1"], dtype="string"),
            "trip_id": pd.Series(["t0", "t1"], dtype="string"),
            "movement_seq": pd.Series([0, 1], dtype="Int64"),
            "purpose": ["work", "study"],
        }
    )

    work, columns_added, issues = _ensure_single_stage_ids(
        df.copy(deep=True),
        schema=schema_ids_required,
        single_stage=True,
        strict=False,
    )

    assert_columns_equal(
        work,
        ["movement_id", "trip_id", "movement_seq", "purpose"],
        "single_stage=True con campos existentes",
    )
    assert work["trip_id"].tolist() == ["t0", "t1"]
    assert work["movement_seq"].tolist() == [0, 1]
    assert columns_added == []
    assert issues == []


def test_ensure_single_stage_ids_true_without_movement_id_raises_import_error(schema_ids_required):
    """Verifica que single_stage=True aborte si movement_id todavía no existe."""
    df = pd.DataFrame(
        {
            "purpose": ["work", "study"],
        }
    )

    with pytest.raises(PylondrinaImportError) as exc_info:
        _ensure_single_stage_ids(
            df.copy(deep=True),
            schema=schema_ids_required,
            single_stage=True,
            strict=False,
        )

    assert_issue_present(exc_info.value.issues, "IMP.INPUT.MISSING_REQUIRED_FIELD")

    issue = exc_info.value.issue
    assert issue.code == "IMP.INPUT.MISSING_REQUIRED_FIELD"
    assert issue.details["missing_required"] == ["movement_id"]
    assert issue.details["source_columns"] == ["purpose"]


def test_ensure_single_stage_ids_false_does_not_generate_optional_trip_fields(schema_ids_not_required):
    """Verifica que single_stage=False no genere trip_id ni movement_seq cuando no son requeridos."""
    df = pd.DataFrame(
        {
            "movement_id": pd.Series(["m0", "m1"], dtype="string"),
            "purpose": ["work", "study"],
        }
    )

    work, columns_added, issues = _ensure_single_stage_ids(
        df.copy(deep=True),
        schema=schema_ids_not_required,
        single_stage=False,
        strict=False,
    )

    assert_columns_equal(
        work,
        ["movement_id", "purpose"],
        "single_stage=False no genera",
    )
    assert columns_added == []
    assert issues == []


def test_ensure_single_stage_ids_false_missing_required_trip_fields_raises_import_error(schema_ids_required):
    """Verifica que single_stage=False aborte si el schema exige trip_id y movement_seq pero faltan."""
    df = pd.DataFrame(
        {
            "movement_id": pd.Series(["m0", "m1"], dtype="string"),
            "purpose": ["work", "study"],
        }
    )

    with pytest.raises(PylondrinaImportError) as exc_info:
        _ensure_single_stage_ids(
            df.copy(deep=True),
            schema=schema_ids_required,
            single_stage=False,
            strict=False,
        )

    assert_issue_present(exc_info.value.issues, "IMP.INPUT.MISSING_REQUIRED_FIELD")

    issue = exc_info.value.issue
    assert issue.code == "IMP.INPUT.MISSING_REQUIRED_FIELD"
    assert issue.details["missing_required"] == ["trip_id", "movement_seq"]
    assert issue.details["required"] == ["movement_id", "trip_id", "movement_seq"]


def test_ensure_single_stage_ids_true_generates_only_missing_trip_id(schema_ids_required):
    """Verifica que single_stage=True genere solo trip_id si movement_seq ya existe."""
    df = pd.DataFrame(
        {
            "movement_id": pd.Series(["m0", "m1"], dtype="string"),
            "movement_seq": pd.Series([0, 0], dtype="Int64"),
            "purpose": ["work", "study"],
        }
    )

    work, columns_added, issues = _ensure_single_stage_ids(
        df.copy(deep=True),
        schema=schema_ids_required,
        single_stage=True,
        strict=False,
    )

    assert_columns_equal(
        work,
        ["movement_id", "trip_id", "movement_seq", "purpose"],
        "single_stage=True genera solo trip_id",
    )
    assert work["trip_id"].tolist() == ["m0", "m1"]
    assert work["movement_seq"].tolist() == [0, 0]
    assert columns_added == ["trip_id"]
    assert_issue_present(issues, "IMP.ID.TRIP_ID_CREATED")
    assert_issue_absent(issues, "IMP.ID.MOVEMENT_SEQ_CREATED")


def test_ensure_single_stage_ids_true_generates_only_missing_movement_seq(schema_ids_required):
    """Verifica que single_stage=True genere solo movement_seq si trip_id ya existe."""
    df = pd.DataFrame(
        {
            "movement_id": pd.Series(["m0", "m1"], dtype="string"),
            "trip_id": pd.Series(["t0", "t1"], dtype="string"),
            "purpose": ["work", "study"],
        }
    )

    work, columns_added, issues = _ensure_single_stage_ids(
        df.copy(deep=True),
        schema=schema_ids_required,
        single_stage=True,
        strict=False,
    )

    assert_columns_equal(
        work,
        ["movement_id", "trip_id", "movement_seq", "purpose"],
        "single_stage=True genera solo movement_seq",
    )
    assert work["trip_id"].tolist() == ["t0", "t1"]
    assert work["movement_seq"].tolist() == [0, 0]
    assert_dtype(work, "movement_seq", "Int64")
    assert columns_added == ["movement_seq"]
    assert_issue_absent(issues, "IMP.ID.TRIP_ID_CREATED")
    assert_issue_present(issues, "IMP.ID.MOVEMENT_SEQ_CREATED")


def test_ids_integrated_ensure_movement_id_then_single_stage_ids(schema_ids_required):
    """Verifica el encadenamiento mínimo: crear movement_id y luego derivar trip_id/movement_seq para single_stage."""
    df = pd.DataFrame(
        {
            "purpose": ["work", "study", "home"],
        }
    )

    work, columns_added_mid, issues_mid = _ensure_movement_id(
        df.copy(deep=True),
        strict=False,
    )

    work_2, columns_added_stage, issues_stage = _ensure_single_stage_ids(
        work,
        schema=schema_ids_required,
        single_stage=True,
        strict=False,
    )

    assert_columns_equal(
        work_2,
        ["movement_id", "trip_id", "movement_seq", "purpose"],
        "integrated IDs",
    )

    assert work_2["movement_id"].tolist() == ["m0", "m1", "m2"]
    assert work_2["trip_id"].tolist() == ["m0", "m1", "m2"]
    assert work_2["movement_seq"].tolist() == [0, 0, 0]

    assert columns_added_mid == ["movement_id"]
    assert columns_added_stage == ["trip_id", "movement_seq"]

    assert_issue_present(issues_mid, "IMP.ID.MOVEMENT_ID_CREATED")
    assert_issue_present(issues_stage, "IMP.ID.TRIP_ID_CREATED")
    assert_issue_present(issues_stage, "IMP.ID.MOVEMENT_SEQ_CREATED")