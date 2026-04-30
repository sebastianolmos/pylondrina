import pandas as pd
import pytest

from pylondrina.errors import ImportError as PylondrinaImportError
from pylondrina.importing import _apply_field_correspondence
from pylondrina.schema import FieldSpec, TripSchema


# ---------------------------------------------------------------------
# Helpers locales de test
# ---------------------------------------------------------------------


def assert_columns_equal(df: pd.DataFrame, expected_columns: list[str]) -> None:
    assert list(df.columns) == expected_columns


def issue_codes(issues) -> list[str]:
    return [issue.code for issue in issues]


def assert_issue_present(issues, code: str) -> None:
    codes = issue_codes(issues)
    assert code in codes, f"No se encontró issue {code!r}. Issues encontrados: {codes!r}"


def assert_issue_absent(issues, code: str) -> None:
    codes = issue_codes(issues)
    assert code not in codes, f"No se esperaba issue {code!r}. Issues encontrados: {codes!r}"


def get_issue(issues, code: str):
    for issue in issues:
        if issue.code == code:
            return issue
    raise AssertionError(f"No se encontró issue {code!r}. Issues encontrados: {issue_codes(issues)!r}")


# ---------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------


@pytest.fixture
def schema_g4() -> TripSchema:
    return TripSchema(
        version="0.1.0",
        fields={
            "user_id": FieldSpec(name="user_id", dtype="string", required=True),
            "purpose": FieldSpec(name="purpose", dtype="categorical", required=False),
            "mode": FieldSpec(name="mode", dtype="categorical", required=False),
            "origin_latitude": FieldSpec(name="origin_latitude", dtype="float", required=False),
            "origin_longitude": FieldSpec(name="origin_longitude", dtype="float", required=False),
            "dummy_id": FieldSpec(name="dummy_id", dtype="string", required=False),
            "movement_id": FieldSpec(name="movement_id", dtype="string", required=False),
            "movement_seq": FieldSpec(name="movement_seq", dtype="int", required=False),
            "origin_h3_index": FieldSpec(name="origin_h3_index", dtype="string", required=False),
            "destination_h3_index": FieldSpec(name="destination_h3_index", dtype="string", required=False),
        },
        required=["user_id"],
        semantic_rules=None,
    )


@pytest.fixture
def df_g4_base() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "uid": ["u1", "u2"],
            "motivo": ["trabajo", "estudio"],
            "modo": ["bus", "metro"],
            "o_lat": [-33.45, -33.46],
            "o_lon": [-70.60, -70.61],
            "raw_extra": ["A", "B"],
        }
    )


# ---------------------------------------------------------------------
# Tests de _apply_field_correspondence
# ---------------------------------------------------------------------


def test_apply_field_correspondence_valid_mapping(schema_g4, df_g4_base):
    """Verifica que un mapping válido renombre columnas fuente a campos canónicos y registre solo mappings realmente aplicados."""
    field_corr_valid = {
        "user_id": "uid",
        "purpose": "motivo",
        "mode": "modo",
        "origin_latitude": "o_lat",
        "origin_longitude": "o_lon",
    }

    work_valid, applied_valid, issues_valid = _apply_field_correspondence(
        df_g4_base.copy(deep=True),
        schema=schema_g4,
        field_correspondence=field_corr_valid,
        strict=False,
    )

    assert_columns_equal(
        work_valid,
        [
            "user_id",
            "purpose",
            "mode",
            "origin_latitude",
            "origin_longitude",
            "raw_extra",
        ],
    )

    assert applied_valid == {
        "user_id": "uid",
        "purpose": "motivo",
        "mode": "modo",
        "origin_latitude": "o_lat",
        "origin_longitude": "o_lon",
    }

    assert work_valid["user_id"].tolist() == ["u1", "u2"]
    assert work_valid["purpose"].tolist() == ["trabajo", "estudio"]
    assert work_valid["mode"].tolist() == ["bus", "metro"]
    assert work_valid["origin_latitude"].tolist() == [-33.45, -33.46]
    assert work_valid["origin_longitude"].tolist() == [-70.60, -70.61]

    # dummy_id está en el schema, es opcional, no es derivable y no aparece en la fuente.
    assert_issue_present(issues_valid, "IMP.INPUT.OPTIONAL_FIELD_NOT_FOUND")

    optional_issue = get_issue(issues_valid, "IMP.INPUT.OPTIONAL_FIELD_NOT_FOUND")
    assert optional_issue.field == "dummy_id"
    assert optional_issue.details["field"] == "dummy_id"
    assert optional_issue.details["field_correspondence_used"] is False
    assert optional_issue.details["action"] == "omit_optional"


def test_apply_field_correspondence_canonical_identity_is_not_reported_as_applied(schema_g4):
    """Verifica que mappings identidad, como user_id -> user_id, no se contabilicen como correspondencias aplicadas."""
    df_g4_identity = pd.DataFrame(
        {
            "user_id": ["u1", "u2"],
            "purpose": ["work", "study"],
            "raw_extra": ["A", "B"],
        }
    )

    field_corr_identity = {
        "user_id": "user_id",
        "purpose": "purpose",
    }

    work_identity, applied_identity, issues_identity = _apply_field_correspondence(
        df_g4_identity.copy(deep=True),
        schema=schema_g4,
        field_correspondence=field_corr_identity,
        strict=False,
    )

    assert_columns_equal(
        work_identity,
        ["user_id", "purpose", "raw_extra"],
    )

    # Las identidades canónico -> canónico no se registran como mappings aplicados.
    assert applied_identity == {}

    assert work_identity["user_id"].tolist() == ["u1", "u2"]
    assert work_identity["purpose"].tolist() == ["work", "study"]

    # Faltan opcionales no derivables: mode, origin_latitude, origin_longitude, dummy_id.
    assert_issue_present(issues_identity, "IMP.INPUT.OPTIONAL_FIELD_NOT_FOUND")


def test_apply_field_correspondence_missing_required_source_column_raises(schema_g4, df_g4_base):
    """Verifica que un campo requerido mapeado a una columna fuente inexistente produzca abort con ImportError."""
    field_corr_missing_required = {
        "user_id": "uid_missing",
    }

    with pytest.raises(PylondrinaImportError) as exc_info:
        _apply_field_correspondence(
            df_g4_base.copy(deep=True),
            schema=schema_g4,
            field_correspondence=field_corr_missing_required,
            strict=False,
        )

    assert_issue_present(exc_info.value.issues, "MAP.FIELDS.MISSING_SOURCE_COLUMN")

    issue = exc_info.value.issue
    assert issue.code == "MAP.FIELDS.MISSING_SOURCE_COLUMN"
    assert issue.field == "user_id"
    assert issue.source_field == "uid_missing"
    assert issue.details["field"] == "user_id"
    assert issue.details["source_field"] == "uid_missing"
    assert issue.details["action"] == "abort"


def test_apply_field_correspondence_missing_optional_source_column_is_recoverable(schema_g4, df_g4_base):
    """Verifica que un campo opcional mapeado a una fuente inexistente se omita con issue recuperable."""
    field_corr_missing_optional = {
        "user_id": "uid",
        "purpose": "motivo_missing",
    }

    work_missing_optional, applied_missing_optional, issues_missing_optional = _apply_field_correspondence(
        df_g4_base.copy(deep=True),
        schema=schema_g4,
        field_correspondence=field_corr_missing_optional,
        strict=False,
    )

    assert "user_id" in work_missing_optional.columns
    assert "purpose" not in work_missing_optional.columns

    assert applied_missing_optional == {
        "user_id": "uid",
    }

    assert work_missing_optional["user_id"].tolist() == ["u1", "u2"]

    assert_issue_present(issues_missing_optional, "IMP.INPUT.OPTIONAL_FIELD_NOT_FOUND")

    issue_for_purpose = None
    for issue in issues_missing_optional:
        if issue.code == "IMP.INPUT.OPTIONAL_FIELD_NOT_FOUND" and issue.field == "purpose":
            issue_for_purpose = issue
            break

    assert issue_for_purpose is not None
    assert issue_for_purpose.details["field"] == "purpose"
    assert issue_for_purpose.details["field_correspondence_used"] is True
    assert issue_for_purpose.details["action"] == "omit_optional"


def test_apply_field_correspondence_unknown_canonical_field_raises(schema_g4, df_g4_base):
    """Verifica que un mapping hacia un campo canónico inexistente en el schema aborte la operación."""
    field_corr_unknown_canonical = {
        "fake_field": "uid",
    }

    with pytest.raises(PylondrinaImportError) as exc_info:
        _apply_field_correspondence(
            df_g4_base.copy(deep=True),
            schema=schema_g4,
            field_correspondence=field_corr_unknown_canonical,
            strict=False,
        )

    assert_issue_present(exc_info.value.issues, "MAP.FIELDS.UNKNOWN_CANONICAL_FIELD")

    issue = exc_info.value.issue
    assert issue.code == "MAP.FIELDS.UNKNOWN_CANONICAL_FIELD"
    assert issue.field == "fake_field"
    assert issue.details["field"] == "fake_field"
    assert issue.details["action"] == "abort"


def test_apply_field_correspondence_two_canonicals_to_same_source_column_raises(schema_g4, df_g4_base):
    """Verifica que dos campos canónicos no puedan mapearse a la misma columna fuente."""
    field_corr_duplicate_target = {
        "purpose": "motivo",
        "mode": "motivo",
    }

    with pytest.raises(PylondrinaImportError) as exc_info:
        _apply_field_correspondence(
            df_g4_base.copy(deep=True),
            schema=schema_g4,
            field_correspondence=field_corr_duplicate_target,
            strict=False,
        )

    assert_issue_present(exc_info.value.issues, "MAP.FIELDS.COLLISION_DUPLICATE_TARGET")

    issue = exc_info.value.issue
    assert issue.code == "MAP.FIELDS.COLLISION_DUPLICATE_TARGET"
    assert issue.details["source_column"] == "motivo"
    assert issue.details["canonical_fields"] == ["purpose", "mode"]
    assert issue.details["field_correspondence"] == field_corr_duplicate_target
    assert issue.details["action"] == "abort"


def test_apply_field_correspondence_canonical_already_present_conflict_raises(schema_g4):
    """Verifica que haya abort cuando un campo canónico ya existe y además se intenta mapear desde otra columna."""
    df_g4_conflict = pd.DataFrame(
        {
            "user_id": ["u_can_1", "u_can_2"],
            "uid": ["u_src_1", "u_src_2"],
            "raw_extra": ["A", "B"],
        }
    )

    field_corr_conflict = {
        "user_id": "uid",
    }

    with pytest.raises(PylondrinaImportError) as exc_info:
        _apply_field_correspondence(
            df_g4_conflict.copy(deep=True),
            schema=schema_g4,
            field_correspondence=field_corr_conflict,
            strict=False,
        )

    assert_issue_present(exc_info.value.issues, "MAP.FIELDS.CANONICAL_ALREADY_PRESENT_CONFLICT")

    issue = exc_info.value.issue
    assert issue.code == "MAP.FIELDS.CANONICAL_ALREADY_PRESENT_CONFLICT"
    assert issue.field == "user_id"
    assert issue.source_field == "uid"
    assert issue.details["field"] == "user_id"
    assert issue.details["source_field"] == "uid"
    assert issue.details["action"] == "abort"


def test_apply_field_correspondence_optional_fields_not_present_without_mapping_emit_issue(schema_g4):
    """Verifica que campos opcionales no derivables ausentes se reporten, pero no impidan el mapping mínimo requerido."""
    df_g4_minimal = pd.DataFrame(
        {
            "uid": ["u1", "u2"],
        }
    )

    field_corr_minimal = {
        "user_id": "uid",
    }

    work_minimal, applied_minimal, issues_minimal = _apply_field_correspondence(
        df_g4_minimal.copy(deep=True),
        schema=schema_g4,
        field_correspondence=field_corr_minimal,
        strict=False,
    )

    assert_columns_equal(work_minimal, ["user_id"])
    assert applied_minimal == {"user_id": "uid"}
    assert work_minimal["user_id"].tolist() == ["u1", "u2"]

    assert_issue_present(issues_minimal, "IMP.INPUT.OPTIONAL_FIELD_NOT_FOUND")

    missing_optional_fields = {
        issue.field
        for issue in issues_minimal
        if issue.code == "IMP.INPUT.OPTIONAL_FIELD_NOT_FOUND"
    }

    assert {"purpose", "mode", "origin_latitude", "origin_longitude", "dummy_id"}.issubset(
        missing_optional_fields
    )

    # Estos campos son derivables en etapas posteriores del import, por eso no deben reportarse aquí.
    assert "movement_id" not in missing_optional_fields
    assert "movement_seq" not in missing_optional_fields
    assert "origin_h3_index" not in missing_optional_fields
    assert "destination_h3_index" not in missing_optional_fields


def test_apply_field_correspondence_none_keeps_dataframe_and_reports_missing_optional_fields(schema_g4, df_g4_base):
    """Verifica que field_correspondence=None deje el DataFrame intacto y reporte opcionales ausentes respecto del schema."""
    work_none, applied_none, issues_none = _apply_field_correspondence(
        df_g4_base.copy(deep=True),
        schema=schema_g4,
        field_correspondence=None,
        strict=False,
    )

    assert_columns_equal(
        work_none,
        ["uid", "motivo", "modo", "o_lat", "o_lon", "raw_extra"],
    )

    assert applied_none == {}
    assert_issue_present(issues_none, "IMP.INPUT.OPTIONAL_FIELD_NOT_FOUND")

    missing_optional_fields = {
        issue.field
        for issue in issues_none
        if issue.code == "IMP.INPUT.OPTIONAL_FIELD_NOT_FOUND"
    }

    assert {"purpose", "mode", "origin_latitude", "origin_longitude", "dummy_id"}.issubset(
        missing_optional_fields
    )


def test_apply_field_correspondence_empty_mapping_behaves_like_no_mapping(schema_g4, df_g4_base):
    """Verifica que un mapping vacío se comporte igual que no entregar field_correspondence."""
    work_empty, applied_empty, issues_empty = _apply_field_correspondence(
        df_g4_base.copy(deep=True),
        schema=schema_g4,
        field_correspondence={},
        strict=False,
    )

    assert_columns_equal(
        work_empty,
        ["uid", "motivo", "modo", "o_lat", "o_lon", "raw_extra"],
    )

    assert applied_empty == {}
    assert_issue_present(issues_empty, "IMP.INPUT.OPTIONAL_FIELD_NOT_FOUND")


def test_apply_field_correspondence_integrated_small_case(schema_g4):
    """Verifica un caso mixto con identidad canónica, mappings aplicados y opcional faltante recuperable."""
    df_g4_integrated = pd.DataFrame(
        {
            "user_id": ["u1", "u2"],
            "motivo": ["trabajo", "estudio"],
            "modo": ["bus", "metro"],
            "raw_extra": ["A", "B"],
        }
    )

    field_corr_integrated = {
        "user_id": "user_id",
        "purpose": "motivo",
        "mode": "modo",
        "origin_latitude": "o_lat_missing",
    }

    work_integrated, applied_integrated, issues_integrated = _apply_field_correspondence(
        df_g4_integrated.copy(deep=True),
        schema=schema_g4,
        field_correspondence=field_corr_integrated,
        strict=False,
    )

    assert_columns_equal(
        work_integrated,
        ["user_id", "purpose", "mode", "raw_extra"],
    )

    # user_id -> user_id es identidad, por eso no aparece en applied.
    # origin_latitude -> o_lat_missing es opcional faltante, por eso tampoco aparece.
    assert applied_integrated == {
        "purpose": "motivo",
        "mode": "modo",
    }

    assert work_integrated["user_id"].tolist() == ["u1", "u2"]
    assert work_integrated["purpose"].tolist() == ["trabajo", "estudio"]
    assert work_integrated["mode"].tolist() == ["bus", "metro"]

    assert_issue_present(issues_integrated, "IMP.INPUT.OPTIONAL_FIELD_NOT_FOUND")

    missing_optional_fields = {
        issue.field
        for issue in issues_integrated
        if issue.code == "IMP.INPUT.OPTIONAL_FIELD_NOT_FOUND"
    }

    assert "origin_latitude" in missing_optional_fields
    assert "origin_longitude" in missing_optional_fields
    assert "dummy_id" in missing_optional_fields