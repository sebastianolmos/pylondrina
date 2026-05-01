import pandas as pd

from pylondrina.schema import FieldSpec, TripSchema
from pylondrina.validation import (
    _build_effective_nullable_by_field,
    check_constraints,
    check_required_columns,
    check_types_and_formats,
)


# ---------------------------------------------------------------------
# Helpers locales de test
# ---------------------------------------------------------------------


def make_field(
    name: str,
    dtype: str,
    *,
    required: bool = False,
    constraints: dict | None = None,
) -> FieldSpec:
    return FieldSpec(
        name=name,
        dtype=dtype,
        required=required,
        constraints=constraints,
    )


def make_trip_schema(fields: list[FieldSpec], *, version: str = "1.1") -> TripSchema:
    return TripSchema(
        version=version,
        fields={field.name: field for field in fields},
        required=[field.name for field in fields if field.required],
        semantic_rules=None,
    )


def issue_codes(issues) -> list[str]:
    return [issue.code for issue in issues]


def assert_issue_present(issues, code: str) -> None:
    codes = issue_codes(issues)
    assert code in codes, f"No se encontró issue {code!r}. Issues encontrados: {codes!r}"


def assert_issue_absent(issues, code: str) -> None:
    codes = issue_codes(issues)
    assert code not in codes, f"No se esperaba issue {code!r}. Issues encontrados: {codes!r}"


def issues_with_code(issues, code: str):
    return [issue for issue in issues if issue.code == code]


def assert_counts_by_level(
    issues,
    *,
    errors: int = 0,
    warnings: int = 0,
    info: int = 0,
) -> None:
    levels = [issue.level for issue in issues]
    assert levels.count("error") == errors
    assert levels.count("warning") == warnings
    assert levels.count("info") == info


# ---------------------------------------------------------------------
# Fixtures locales simples
# ---------------------------------------------------------------------


def make_required_schema() -> TripSchema:
    return make_trip_schema(
        [
            make_field("movement_id", "string", required=True),
            make_field("user_id", "string", required=True),
            make_field("mode", "categorical", required=False),
        ]
    )


def make_types_schema() -> TripSchema:
    return make_trip_schema(
        [
            make_field("int_field", "int"),
            make_field("float_field", "float"),
            make_field("dt_field", "datetime"),
            make_field("bool_field", "bool"),
            make_field("cat_field", "categorical"),
            make_field("str_field", "string"),
        ]
    )


def make_types_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "int_field": [1, "2", "3.5", "abc", None],
            "float_field": [1.2, "2.3", "abc", None, 7],
            "dt_field": [
                "2026-01-01T08:00:00Z",
                "not-a-date",
                None,
                "2026-01-02",
                "13/99/2026",
            ],
            "bool_field": [True, "false", "YES", "maybe", None],
            "cat_field": ["walk", "bus", None, "xxx", "metro"],
            "str_field": ["a", 2, None, "b", object()],
        }
    )


# ---------------------------------------------------------------------
# Tests de check_required_columns
# ---------------------------------------------------------------------


def test_check_required_columns_returns_no_issues_when_required_columns_are_present():
    """Verifica que no se emitan issues cuando todas las columnas requeridas están presentes."""
    schema = make_required_schema()
    df = pd.DataFrame(
        {
            "movement_id": ["m1", "m2"],
            "user_id": ["u1", "u2"],
            "mode": ["walk", "bus"],
        }
    )

    issues = check_required_columns(df, schema=schema)

    assert issues == []


def test_check_required_columns_reports_missing_required_columns():
    """Verifica que columnas requeridas ausentes se reporten con VAL.CORE.REQUIRED_COLUMNS_MISSING."""
    schema = make_required_schema()
    df = pd.DataFrame(
        {
            "movement_id": ["m1", "m2"],
            "mode": ["walk", "bus"],
        }
    )

    issues = check_required_columns(df, schema=schema)

    assert len(issues) == 1
    assert_issue_present(issues, "VAL.CORE.REQUIRED_COLUMNS_MISSING")
    assert_counts_by_level(issues, errors=1, warnings=0, info=0)

    issue = issues[0]
    assert issue.level == "error"
    assert issue.code == "VAL.CORE.REQUIRED_COLUMNS_MISSING"
    assert issue.row_count == 1
    assert issue.details["missing_required"] == ["user_id"]
    assert issue.details["required"] == ["movement_id", "user_id"]
    assert issue.details["available_columns_total"] == len(df.columns)
    assert set(issue.details["available_columns_sample"]) == set(df.columns)


# ---------------------------------------------------------------------
# Tests de check_types_and_formats
# ---------------------------------------------------------------------


def test_check_types_and_formats_reports_one_issue_per_invalid_typed_field():
    """Verifica que tipos no interpretables se reporten por campo afectado sin evaluar string/categorical."""
    schema = make_types_schema()
    df = make_types_dataframe()

    issues = check_types_and_formats(
        df,
        schema=schema,
        sample_rows_per_issue=3,
    )

    invalid_type_issues = issues_with_code(issues, "VAL.CORE.TYPE_OR_FORMAT_INVALID")

    assert len(issues) == len(invalid_type_issues)
    assert_counts_by_level(issues, errors=len(issues), warnings=0, info=0)

    fields_with_issues = {issue.field for issue in invalid_type_issues}
    assert fields_with_issues == {
        "int_field",
        "float_field",
        "dt_field",
        "bool_field",
    }

    fields_without_issues = {"cat_field", "str_field"}
    assert fields_without_issues.isdisjoint(fields_with_issues)

    for issue in invalid_type_issues:
        assert issue.level == "error"
        assert issue.row_count >= 1
        assert issue.details["field"] == issue.field
        assert issue.details["dtype_expected"] == schema.fields[issue.field].dtype
        assert issue.details["parse_fail_count"] == issue.row_count
        assert issue.details["total_count"] <= len(df)
        assert issue.details["action"] == "report_error"


def test_check_types_and_formats_does_not_mutate_dataframe():
    """Verifica que el helper de tipos/formatos inspeccione el DataFrame sin modificarlo."""
    schema = make_types_schema()
    df = make_types_dataframe()
    df_before = df.copy(deep=True)

    _ = check_types_and_formats(
        df,
        schema=schema,
        sample_rows_per_issue=3,
    )

    pd.testing.assert_frame_equal(df, df_before)


# ---------------------------------------------------------------------
# Tests de check_constraints
# ---------------------------------------------------------------------


def test_check_constraints_reports_nullability_range_and_length_violations():
    """Verifica nullabilidad efectiva y constraints declarativas range/length en un mismo schema."""
    schema = make_trip_schema(
        [
            make_field("movement_id", "string", required=True),
            make_field(
                "score",
                "float",
                required=False,
                constraints={"nullable": False, "range": {"min": 0, "max": 10}},
            ),
            make_field(
                "comment",
                "string",
                required=False,
                constraints={"length": {"min": 2, "max": 5}},
            ),
        ]
    )
    df = pd.DataFrame(
        {
            "movement_id": ["m1", "m2", "m3", "m4"],
            "score": [5, None, -1, 12],
            "comment": ["ok", "x", "123456", None],
        }
    )

    issues = check_constraints(
        df,
        schema=schema,
        effective_nullable_by_field=_build_effective_nullable_by_field(schema),
        allow_partial_od_spatial=False,
        skipped_constraints={},
        sample_rows_per_issue=5,
    )

    assert len(issues) == 3
    assert_counts_by_level(issues, errors=3, warnings=0, info=0)

    assert_issue_present(issues, "VAL.CORE.NULLABILITY_VIOLATION")
    assert_issue_present(issues, "VAL.CORE.CONSTRAINT_VIOLATION")

    nullability_issues = issues_with_code(issues, "VAL.CORE.NULLABILITY_VIOLATION")
    constraint_issues = issues_with_code(issues, "VAL.CORE.CONSTRAINT_VIOLATION")

    assert len(nullability_issues) == 1
    assert len(constraint_issues) == 2

    nullability_issue = nullability_issues[0]
    assert nullability_issue.field == "score"
    assert nullability_issue.row_count == 1
    assert nullability_issue.details["nullable_effective"] is False
    assert nullability_issue.details["action"] == "report_error"

    constraints_by_field = {
        (issue.field, issue.details["constraint"])
        for issue in constraint_issues
    }
    assert constraints_by_field == {
        ("score", "range"),
        ("comment", "length"),
    }

    for issue in constraint_issues:
        assert issue.level == "error"
        assert issue.row_count >= 1
        assert issue.details["field"] == issue.field
        assert issue.details["n_violations"] == issue.row_count
        assert issue.details["action"] == "report_error"


def test_check_constraints_allows_partial_od_spatial_but_reports_rows_without_origin_or_destination():
    """Verifica que OD parcial permita origen o destino completo, pero reporte filas sin ambos pares espaciales."""
    schema = make_trip_schema(
        [
            make_field("origin_latitude", "float", required=True),
            make_field("origin_longitude", "float", required=True),
            make_field("destination_latitude", "float", required=True),
            make_field("destination_longitude", "float", required=True),
        ]
    )
    df = pd.DataFrame(
        {
            "origin_latitude": [-33.45, None, None],
            "origin_longitude": [-70.66, None, None],
            "destination_latitude": [None, -33.44, None],
            "destination_longitude": [None, -70.62, None],
        }
    )

    issues = check_constraints(
        df,
        schema=schema,
        effective_nullable_by_field=_build_effective_nullable_by_field(schema),
        allow_partial_od_spatial=True,
        skipped_constraints={},
        sample_rows_per_issue=5,
    )

    assert len(issues) == 1
    assert_issue_present(issues, "VAL.CORE.OD_SPATIAL_BOTH_MISSING")
    assert_issue_absent(issues, "VAL.CORE.NULLABILITY_VIOLATION")
    assert_counts_by_level(issues, errors=1, warnings=0, info=0)

    issue = issues[0]
    assert issue.level == "error"
    assert issue.code == "VAL.CORE.OD_SPATIAL_BOTH_MISSING"
    assert issue.row_count == 1
    assert issue.details["n_rows_total"] == len(df)
    assert issue.details["n_violations"] == issue.row_count
    assert set(issue.details["fields_checked"]) == {
        "origin_latitude",
        "origin_longitude",
        "destination_latitude",
        "destination_longitude",
    }
    assert issue.details["allow_partial_od_spatial"] is True
    assert issue.details["action"] == "report_error"


def test_check_constraints_respects_skipped_constraints():
    """Verifica que una constraint conocida marcada como skipped no sea evaluada ni genere issue."""
    schema = make_trip_schema(
        [
            make_field(
                "score",
                "float",
                constraints={"range": {"min": 0, "max": 10}},
            ),
        ]
    )
    df = pd.DataFrame(
        {
            "score": [-5, 99, 4],
        }
    )

    issues = check_constraints(
        df,
        schema=schema,
        effective_nullable_by_field=_build_effective_nullable_by_field(schema),
        allow_partial_od_spatial=False,
        skipped_constraints={"score": {"range"}},
        sample_rows_per_issue=5,
    )

    assert issues == []