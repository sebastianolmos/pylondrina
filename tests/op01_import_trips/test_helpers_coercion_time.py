import math

import numpy as np
import pandas as pd
import pytest

from pylondrina.errors import ImportError as PylondrinaImportError
from pylondrina.importing import (
    ImportOptions,
    _coerce_columns_by_dtype,
    _coerce_series_to_dtype,
    _normalize_datetime_column,
    _normalize_datetime_columns,
    _normalize_tier2_hhmm_columns,
)
from pylondrina.schema import FieldSpec, TripSchema, TripSchemaEffective


# ---------------------------------------------------------------------
# Helpers locales de test
# ---------------------------------------------------------------------


def assert_columns_equal(df: pd.DataFrame, expected_columns: list[str]) -> None:
    assert list(df.columns) == expected_columns


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


def get_issues(issues, code: str):
    return [issue for issue in issues if issue.code == code]


def assert_list_with_na(actual, expected) -> None:
    assert len(actual) == len(expected)

    for actual_value, expected_value in zip(actual, expected):
        if pd.isna(expected_value):
            assert pd.isna(actual_value)
        else:
            assert actual_value == expected_value


# ---------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------


@pytest.fixture
def schema_datetime() -> TripSchema:
    return TripSchema(
        version="0.1.0",
        fields={
            "origin_time_utc": FieldSpec(name="origin_time_utc", dtype="datetime", required=False),
            "destination_time_utc": FieldSpec(name="destination_time_utc", dtype="datetime", required=False),
            "trip_id": FieldSpec(name="trip_id", dtype="string", required=False),
        },
        required=[],
        semantic_rules=None,
    )


@pytest.fixture
def schema_effective_datetime() -> TripSchemaEffective:
    return TripSchemaEffective(
        dtype_effective={
            "origin_time_utc": "datetime",
            "destination_time_utc": "datetime",
            "trip_id": "string",
        },
        overrides={},
        domains_effective={},
        temporal={},
        fields_effective=[],
    )


@pytest.fixture
def schema_hhmm() -> TripSchema:
    return TripSchema(
        version="0.1.0",
        fields={
            "origin_time_local_hhmm": FieldSpec(
                name="origin_time_local_hhmm",
                dtype="string",
                required=False,
            ),
            "destination_time_local_hhmm": FieldSpec(
                name="destination_time_local_hhmm",
                dtype="string",
                required=False,
            ),
        },
        required=[],
        semantic_rules=None,
    )


@pytest.fixture
def schema_coercion() -> TripSchema:
    return TripSchema(
        version="0.1.0",
        fields={
            "user_id": FieldSpec(name="user_id", dtype="string", required=True),
            "trip_count": FieldSpec(name="trip_count", dtype="int", required=False),
            "distance_km": FieldSpec(name="distance_km", dtype="float", required=False),
            "is_student": FieldSpec(name="is_student", dtype="bool", required=False),
            "required_int": FieldSpec(name="required_int", dtype="int", required=True),
            "origin_latitude": FieldSpec(name="origin_latitude", dtype="float", required=False),
        },
        required=["user_id", "required_int"],
        semantic_rules=None,
    )


@pytest.fixture
def schema_effective_coercion() -> TripSchemaEffective:
    return TripSchemaEffective(
        dtype_effective={
            "user_id": "string",
            "trip_count": "int",
            "distance_km": "float",
            "is_student": "bool",
            "required_int": "int",
            "origin_latitude": "float",
        },
        overrides={},
        domains_effective={},
        temporal={},
        fields_effective=[],
    )


@pytest.fixture
def target_fields_coercion() -> set[str]:
    return {
        "user_id",
        "trip_count",
        "distance_km",
        "is_student",
        "required_int",
        "origin_latitude",
    }


# ---------------------------------------------------------------------
# Tests de _normalize_datetime_column
# ---------------------------------------------------------------------


def test_normalize_datetime_column_keeps_utc_tzaware_series():
    """Verifica que una serie datetime tz-aware en UTC se conserve como UTC sin conversión adicional."""
    s_utc = pd.Series(pd.to_datetime(["2026-03-01T08:00:00Z", None], utc=True))

    out, info = _normalize_datetime_column(s_utc, source_timezone=None)

    assert str(out.dtype) == "datetime64[ns, UTC]"
    assert info["status"] == "utc"
    assert info["tz_kind"] == "none"
    assert info["n_nat"] == 1
    assert pd.notna(out.iloc[0])
    assert pd.isna(out.iloc[1])


def test_normalize_datetime_column_localizes_naive_datetime_with_source_timezone():
    """Verifica que datetimes naive se localicen usando source_timezone y queden normalizados a UTC."""
    s_naive = pd.Series(
        pd.to_datetime(["2026-03-01 08:00:00", "2026-03-01 09:30:00"])
    )

    out, info = _normalize_datetime_column(s_naive, source_timezone="America/Santiago")

    assert str(out.dtype) == "datetime64[ns, UTC]"
    assert info["status"] == "naive_localized_to_utc"
    assert info["tz_kind"] == "iana"
    assert info["n_nat"] == 0


def test_normalize_datetime_column_parses_string_with_explicit_utc_timezone():
    """Verifica que strings ISO con zona explícita se parseen y normalicen a datetime UTC."""
    s_str_utc = pd.Series(
        ["2026-03-01T08:00:00Z", "2026-03-01T09:40:00Z"],
        dtype="object",
    )

    out, info = _normalize_datetime_column(s_str_utc, source_timezone=None)

    assert str(out.dtype) == "datetime64[ns, UTC]"
    assert info["status"] == "string_tzaware_to_utc"
    assert info["tz_kind"] == "none"
    assert info["n_nat"] == 0


def test_normalize_datetime_column_keeps_string_naive_without_source_timezone():
    """Verifica que strings parseables pero sin zona se conserven naive cuando no se declara source_timezone."""
    s_str_naive = pd.Series(
        ["2026-03-01 08:00:00", "2026-03-01 09:40:00"],
        dtype="object",
    )

    out, info = _normalize_datetime_column(s_str_naive, source_timezone=None)

    assert "datetime64[ns]" in str(out.dtype)
    assert "UTC" not in str(out.dtype)
    assert info["status"] == "string_naive_unconverted"
    assert info["tz_kind"] == "none"
    assert info["n_nat"] == 0


def test_normalize_datetime_column_rejects_numeric_values_as_not_parsed():
    """Verifica que series numéricas no se interpreten como timestamps y queden como NaT."""
    s_num = pd.Series([1700000000, 1700003600], dtype="int64")

    out, info = _normalize_datetime_column(s_num, source_timezone="UTC")

    assert "datetime64[ns]" in str(out.dtype)
    assert int(out.isna().sum()) == 2
    assert info["status"] == "not_parsed_numeric"
    assert info["tz_kind"] == "utc"
    assert info["n_nat"] == 2


# ---------------------------------------------------------------------
# Tests de _normalize_datetime_columns
# ---------------------------------------------------------------------


def test_normalize_datetime_columns_tier1_with_explicit_utc_strings(
    schema_datetime,
    schema_effective_datetime,
):
    """Verifica que Tier 1 normalice ambos campos UTC desde strings con zona explícita."""
    df = pd.DataFrame(
        {
            "origin_time_utc": ["2026-03-01T08:00:00Z", "2026-03-01T09:00:00Z"],
            "destination_time_utc": ["2026-03-01T08:30:00Z", "2026-03-01T09:40:00Z"],
            "trip_id": ["t1", "t2"],
        }
    )

    work, status, issues = _normalize_datetime_columns(
        df.copy(deep=True),
        schema=schema_datetime,
        schema_effective=schema_effective_datetime,
        options=ImportOptions(source_timezone=None),
        temporal_tier="tier_1",
        strict=False,
    )

    assert str(work["origin_time_utc"].dtype) == "datetime64[ns, UTC]"
    assert str(work["destination_time_utc"].dtype) == "datetime64[ns, UTC]"
    assert work["trip_id"].tolist() == ["t1", "t2"]

    assert status["origin_time_utc"]["status"] == "string_tzaware_to_utc"
    assert status["destination_time_utc"]["status"] == "string_tzaware_to_utc"

    assert_issue_absent(issues, "IMP.DATETIME.NUMERIC_NOT_PARSED")
    assert_issue_absent(issues, "IMP.DATETIME.NAIVE_WITHOUT_SOURCE_TZ")
    assert_issue_absent(issues, "IMP.TYPE.COERCE_PARTIAL")


def test_normalize_datetime_columns_tier1_with_naive_strings_and_source_timezone(
    schema_datetime,
    schema_effective_datetime,
):
    """Verifica que Tier 1 localice strings naive usando source_timezone y no emita warning de naive sin zona."""
    df = pd.DataFrame(
        {
            "origin_time_utc": ["2026-03-01 08:00:00", "2026-03-01 09:00:00"],
            "destination_time_utc": ["2026-03-01 08:30:00", "2026-03-01 09:40:00"],
        }
    )

    work, status, issues = _normalize_datetime_columns(
        df.copy(deep=True),
        schema=schema_datetime,
        schema_effective=schema_effective_datetime,
        options=ImportOptions(source_timezone="America/Santiago"),
        temporal_tier="tier_1",
        strict=False,
    )

    assert str(work["origin_time_utc"].dtype) == "datetime64[ns, UTC]"
    assert str(work["destination_time_utc"].dtype) == "datetime64[ns, UTC]"

    assert status["origin_time_utc"]["status"] == "string_naive_localized_to_utc"
    assert status["destination_time_utc"]["status"] == "string_naive_localized_to_utc"

    assert_issue_absent(issues, "IMP.DATETIME.NAIVE_WITHOUT_SOURCE_TZ")


def test_normalize_datetime_columns_tier1_with_naive_strings_without_source_timezone_emits_issue(
    schema_datetime,
    schema_effective_datetime,
):
    """Verifica que Tier 1 con strings naive sin source_timezone emita issue de temporalidad no desambiguada."""
    df = pd.DataFrame(
        {
            "origin_time_utc": ["2026-03-01 08:00:00", "2026-03-01 09:00:00"],
            "destination_time_utc": ["2026-03-01 08:30:00", "2026-03-01 09:40:00"],
        }
    )

    work, status, issues = _normalize_datetime_columns(
        df.copy(deep=True),
        schema=schema_datetime,
        schema_effective=schema_effective_datetime,
        options=ImportOptions(source_timezone=None),
        temporal_tier="tier_1",
        strict=False,
    )

    assert "datetime64[ns]" in str(work["origin_time_utc"].dtype)
    assert "UTC" not in str(work["origin_time_utc"].dtype)

    assert status["origin_time_utc"]["status"] == "string_naive_unconverted"
    assert status["destination_time_utc"]["status"] == "string_naive_unconverted"

    assert_issue_present(issues, "IMP.DATETIME.NAIVE_WITHOUT_SOURCE_TZ")
    assert len(get_issues(issues, "IMP.DATETIME.NAIVE_WITHOUT_SOURCE_TZ")) == 2


def test_normalize_datetime_columns_tier1_numeric_fields_emit_numeric_not_parsed(
    schema_datetime,
    schema_effective_datetime,
):
    """Verifica que campos datetime numéricos queden como NaT y emitan IMP.DATETIME.NUMERIC_NOT_PARSED."""
    df = pd.DataFrame(
        {
            "origin_time_utc": [1700000000, 1700003600],
            "destination_time_utc": [1700001800, 1700005400],
        }
    )

    work, status, issues = _normalize_datetime_columns(
        df.copy(deep=True),
        schema=schema_datetime,
        schema_effective=schema_effective_datetime,
        options=ImportOptions(source_timezone="UTC"),
        temporal_tier="tier_1",
        strict=False,
    )

    assert int(work["origin_time_utc"].isna().sum()) == 2
    assert int(work["destination_time_utc"].isna().sum()) == 2

    assert status["origin_time_utc"]["status"] == "not_parsed_numeric"
    assert status["destination_time_utc"]["status"] == "not_parsed_numeric"

    assert_issue_present(issues, "IMP.DATETIME.NUMERIC_NOT_PARSED")
    assert len(get_issues(issues, "IMP.DATETIME.NUMERIC_NOT_PARSED")) == 2


def test_normalize_datetime_columns_tier1_partially_invalid_strings_emit_coerce_partial(
    schema_datetime,
    schema_effective_datetime,
):
    """Verifica que strings datetime parcialmente inválidos se conviertan a NaT y emitan IMP.TYPE.COERCE_PARTIAL."""
    df = pd.DataFrame(
        {
            "origin_time_utc": ["2026-03-01T08:00:00Z", "bad"],
            "destination_time_utc": ["2026-03-01T08:30:00Z", "2026-03-01T09:40:00Z"],
        }
    )

    work, status, issues = _normalize_datetime_columns(
        df.copy(deep=True),
        schema=schema_datetime,
        schema_effective=schema_effective_datetime,
        options=ImportOptions(source_timezone=None),
        temporal_tier="tier_1",
        strict=False,
    )

    assert str(work["origin_time_utc"].dtype) == "datetime64[ns, UTC]"
    assert pd.notna(work.loc[0, "origin_time_utc"])
    assert pd.isna(work.loc[1, "origin_time_utc"])

    assert status["origin_time_utc"]["status"] == "string_tzaware_to_utc"
    assert status["origin_time_utc"]["n_nat"] == 1

    assert_issue_present(issues, "IMP.TYPE.COERCE_PARTIAL")


def test_normalize_datetime_columns_non_tier1_is_passthrough(
    schema_datetime,
    schema_effective_datetime,
):
    """Verifica que la normalización datetime no actúe cuando el tier temporal no es tier_1."""
    df = pd.DataFrame(
        {
            "origin_time_local_hhmm": ["08:00", "09:00"],
            "destination_time_local_hhmm": ["08:30", "09:40"],
        }
    )

    work, status, issues = _normalize_datetime_columns(
        df.copy(deep=True),
        schema=schema_datetime,
        schema_effective=schema_effective_datetime,
        options=ImportOptions(source_timezone=None),
        temporal_tier="tier_2",
        strict=False,
    )

    assert_columns_equal(work, ["origin_time_local_hhmm", "destination_time_local_hhmm"])
    assert work.equals(df)
    assert status == {}
    assert issues == []


# ---------------------------------------------------------------------
# Tests de _normalize_tier2_hhmm_columns
# ---------------------------------------------------------------------


def test_normalize_tier2_hhmm_columns_clean_values(schema_hhmm):
    """Verifica que Tier 2 preserve valores HH:MM válidos y retorne estadísticas sin inválidos."""
    df = pd.DataFrame(
        {
            "origin_time_local_hhmm": ["08:23", "12:30", "23:59"],
            "destination_time_local_hhmm": ["09:10", "13:00", "00:00"],
        }
    )

    work, stats, issues = _normalize_tier2_hhmm_columns(
        df.copy(deep=True),
        temporal_tier="tier_2",
        schema=schema_hhmm,
    )

    assert_dtype(work, "origin_time_local_hhmm", "string")
    assert_dtype(work, "destination_time_local_hhmm", "string")

    assert work["origin_time_local_hhmm"].tolist() == ["08:23", "12:30", "23:59"]
    assert work["destination_time_local_hhmm"].tolist() == ["09:10", "13:00", "00:00"]

    assert stats["origin_time_local_hhmm"]["n_total"] == 3
    assert stats["origin_time_local_hhmm"]["n_invalid"] == 0
    assert stats["destination_time_local_hhmm"]["n_total"] == 3
    assert stats["destination_time_local_hhmm"]["n_invalid"] == 0

    assert issues == []


def test_normalize_tier2_hhmm_columns_invalid_values_emit_coerce_partial(schema_hhmm):
    """Verifica que HH:MM inválidos pasen a NA y generen issues de coerción parcial por columna afectada."""
    df = pd.DataFrame(
        {
            "origin_time_local_hhmm": ["08:23", "24:00", "ab:cd", "", None, "07:05"],
            "destination_time_local_hhmm": ["09:10", "13:00", "99:99", "  ", np.nan, "00:00"],
        }
    )

    work, stats, issues = _normalize_tier2_hhmm_columns(
        df.copy(deep=True),
        temporal_tier="tier_2",
        schema=schema_hhmm,
    )

    assert_dtype(work, "origin_time_local_hhmm", "string")
    assert_dtype(work, "destination_time_local_hhmm", "string")

    assert work.loc[0, "origin_time_local_hhmm"] == "08:23"
    assert pd.isna(work.loc[1, "origin_time_local_hhmm"])
    assert pd.isna(work.loc[2, "origin_time_local_hhmm"])
    assert pd.isna(work.loc[3, "origin_time_local_hhmm"])
    assert pd.isna(work.loc[4, "origin_time_local_hhmm"])
    assert work.loc[5, "origin_time_local_hhmm"] == "07:05"

    assert work.loc[0, "destination_time_local_hhmm"] == "09:10"
    assert work.loc[1, "destination_time_local_hhmm"] == "13:00"
    assert pd.isna(work.loc[2, "destination_time_local_hhmm"])
    assert pd.isna(work.loc[3, "destination_time_local_hhmm"])
    assert pd.isna(work.loc[4, "destination_time_local_hhmm"])
    assert work.loc[5, "destination_time_local_hhmm"] == "00:00"

    assert stats["origin_time_local_hhmm"]["n_total"] == 6
    assert stats["origin_time_local_hhmm"]["n_invalid"] == 2
    assert stats["destination_time_local_hhmm"]["n_total"] == 6
    assert stats["destination_time_local_hhmm"]["n_invalid"] == 1

    assert_issue_present(issues, "IMP.TYPE.COERCE_PARTIAL")
    assert len(get_issues(issues, "IMP.TYPE.COERCE_PARTIAL")) == 2


def test_normalize_tier2_hhmm_columns_non_tier2_is_passthrough(schema_hhmm):
    """Verifica que las columnas HH:MM no se modifiquen cuando el tier temporal no es tier_2."""
    df = pd.DataFrame(
        {
            "origin_time_local_hhmm": ["08:00"],
            "destination_time_local_hhmm": ["08:30"],
        }
    )

    work, stats, issues = _normalize_tier2_hhmm_columns(
        df.copy(deep=True),
        temporal_tier="tier_3",
        schema=schema_hhmm,
    )

    assert work.equals(df)
    assert stats == {}
    assert issues == []


# ---------------------------------------------------------------------
# Tests de _coerce_series_to_dtype
# ---------------------------------------------------------------------


def test_coerce_series_to_dtype_string_trims_and_turns_empty_into_na():
    """Verifica que la coerción a string haga strip y convierta strings vacíos en NA."""
    s = pd.Series(["  a  ", "", None, "b"], dtype="object")

    out, stats = _coerce_series_to_dtype(s, "string")

    assert str(out.dtype) == "string"
    assert_list_with_na(out.tolist(), ["a", pd.NA, pd.NA, "b"])

    assert stats["expected"] == "string"
    assert stats["dtype_before"] == "object"
    assert stats["dtype_after"] == "string"
    assert stats["na_before"] == 1
    assert stats["na_after"] == 2
    assert stats["na_delta"] == 1
    assert stats["already_correct"] is False


def test_coerce_series_to_dtype_int_sets_unparseable_values_to_na():
    """Verifica que la coerción a int nullable convierta basura a NA sin perder enteros válidos."""
    s = pd.Series(["1", "2", "abc", None], dtype="object")

    out, stats = _coerce_series_to_dtype(s, "int")

    assert str(out.dtype) == "Int64"
    assert_list_with_na(out.tolist(), [1, 2, pd.NA, pd.NA])
    assert stats["expected"] == "int"
    assert stats["na_delta"] == 1


def test_coerce_series_to_dtype_float_sets_unparseable_values_to_nan():
    """Verifica que la coerción a float convierta basura a NaN y preserve valores numéricos."""
    s = pd.Series(["1.5", "2", "abc", None], dtype="object")

    out, stats = _coerce_series_to_dtype(s, "float")

    assert str(out.dtype) == "float64"
    assert math.isclose(out.iloc[0], 1.5, rel_tol=0, abs_tol=1e-12)
    assert math.isclose(out.iloc[1], 2.0, rel_tol=0, abs_tol=1e-12)
    assert pd.isna(out.iloc[2])
    assert pd.isna(out.iloc[3])
    assert stats["expected"] == "float"
    assert stats["na_delta"] == 1


def test_coerce_series_to_dtype_bool_supports_common_boolean_tokens():
    """Verifica que la coerción booleana reconozca tokens comunes y convierta valores no interpretables a NA."""
    s = pd.Series(["true", "FALSE", "1", "0", "yes", "no", "abc", "", None], dtype="object")

    out, stats = _coerce_series_to_dtype(s, "bool")

    assert str(out.dtype) == "boolean"
    assert_list_with_na(out.tolist(), [True, False, True, False, True, False, pd.NA, pd.NA, pd.NA])

    assert stats["expected"] == "bool"
    assert stats["na_before"] == 1
    assert stats["na_after"] == 3
    assert stats["na_delta"] == 2


def test_coerce_series_to_dtype_datetime_without_parse_does_not_touch_strings():
    """Verifica que datetime con parse_datetime=False no parsee strings ni cambie el dtype."""
    s = pd.Series(["2026-03-01 08:00:00", "2026-03-01 09:00:00"], dtype="object")

    out, stats = _coerce_series_to_dtype(s, "datetime", parse_datetime=False)

    assert out.equals(s)
    assert stats["expected"] == "datetime"
    assert stats["dtype_before"] == "object"
    assert stats["dtype_after"] == "object"
    assert stats["na_delta"] == 0


def test_coerce_series_to_dtype_datetime_with_parse_converts_invalid_strings_to_nat():
    """Verifica que datetime con parse_datetime=True parsee valores válidos y convierta inválidos a NaT."""
    s = pd.Series(["2026-03-01 08:00:00", "bad", None], dtype="object")

    out, stats = _coerce_series_to_dtype(s, "datetime", parse_datetime=True)

    assert "datetime64[ns]" in str(out.dtype)
    assert pd.notna(out.iloc[0])
    assert pd.isna(out.iloc[1])
    assert pd.isna(out.iloc[2])
    assert stats["expected"] == "datetime"
    assert stats["na_delta"] == 1


# ---------------------------------------------------------------------
# Tests de _coerce_columns_by_dtype
# ---------------------------------------------------------------------


def test_coerce_columns_by_dtype_coerces_partial_failures_and_emits_issues(
    schema_coercion,
    schema_effective_coercion,
    target_fields_coercion,
):
    """Verifica coerción por columnas, preservando lo convertible y reportando fallas parciales."""
    df = pd.DataFrame(
        {
            "user_id": ["u1", "u2", "u3"],
            "trip_count": ["1", "abc", "3"],
            "distance_km": ["1.5", "2.0", "bad"],
            "is_student": ["true", "no", "abc"],
            "required_int": ["10", "20", "30"],
        }
    )

    work, stats, issues = _coerce_columns_by_dtype(
        df.copy(deep=True),
        schema=schema_coercion,
        schema_effective=schema_effective_coercion,
        target_schema_fields=target_fields_coercion,
        strict=False,
    )

    assert_dtype(work, "user_id", "string")
    assert_dtype(work, "trip_count", "Int64")
    assert_dtype(work, "distance_km", "float64")
    assert_dtype(work, "is_student", "boolean")
    assert_dtype(work, "required_int", "Int64")

    assert_list_with_na(work["trip_count"].tolist(), [1, pd.NA, 3])
    assert math.isclose(work.loc[0, "distance_km"], 1.5, rel_tol=0, abs_tol=1e-12)
    assert math.isclose(work.loc[1, "distance_km"], 2.0, rel_tol=0, abs_tol=1e-12)
    assert pd.isna(work.loc[2, "distance_km"])
    assert_list_with_na(work["is_student"].tolist(), [True, False, pd.NA])

    assert stats["trip_count"]["na_delta"] == 1
    assert stats["distance_km"]["na_delta"] == 1
    assert stats["is_student"]["na_delta"] == 1

    assert_issue_present(issues, "IMP.TYPE.COERCE_PARTIAL")
    assert len(get_issues(issues, "IMP.TYPE.COERCE_PARTIAL")) == 3


def test_coerce_columns_by_dtype_required_field_fully_unusable_raises(
    schema_coercion,
    schema_effective_coercion,
    target_fields_coercion,
):
    """Verifica que un campo requerido sin ningún valor usable después de coerción aborte."""
    df = pd.DataFrame(
        {
            "user_id": ["u1", "u2", "u3"],
            "trip_count": ["1", "2", "3"],
            "distance_km": ["1.5", "2.0", "3.1"],
            "is_student": ["true", "false", "true"],
            "required_int": ["bad", "worse", "nope"],
        }
    )

    with pytest.raises(PylondrinaImportError) as exc_info:
        _coerce_columns_by_dtype(
            df.copy(deep=True),
            schema=schema_coercion,
            schema_effective=schema_effective_coercion,
            target_schema_fields=target_fields_coercion,
            strict=False,
        )

    assert_issue_present(exc_info.value.issues, "IMP.TYPE.COERCE_FAILED_REQUIRED")

    issue = exc_info.value.issue
    assert issue.code == "IMP.TYPE.COERCE_FAILED_REQUIRED"
    assert issue.field == "required_int"
    assert issue.details["dtype_expected"] == "int"
    assert issue.details["parse_fail_count"] == 3
    assert issue.details["rows_in"] == 3


def test_coerce_columns_by_dtype_skips_fields_outside_target_schema_fields(
    schema_coercion,
    schema_effective_coercion,
):
    """Verifica que solo se coercionen los campos incluidos en target_schema_fields."""
    df = pd.DataFrame(
        {
            "user_id": ["u1", "u2"],
            "trip_count": ["1", "bad"],
            "required_int": ["10", "20"],
        }
    )

    work, stats, issues = _coerce_columns_by_dtype(
        df.copy(deep=True),
        schema=schema_coercion,
        schema_effective=schema_effective_coercion,
        target_schema_fields={"user_id", "required_int"},
        strict=False,
    )

    assert_dtype(work, "user_id", "string")
    assert_dtype(work, "required_int", "Int64")

    # trip_count no fue parte de target_schema_fields, por eso queda intacto.
    assert work["trip_count"].tolist() == ["1", "bad"]
    assert str(work["trip_count"].dtype) == "object"
    assert "trip_count" not in stats
    assert issues == []


def test_coerce_columns_by_dtype_skips_coordinate_fields_for_dedicated_parser(
    schema_coercion,
    schema_effective_coercion,
    target_fields_coercion,
):
    """Verifica que coordenadas OD no se coercionen aquí porque se procesan en el helper espacial dedicado."""
    df = pd.DataFrame(
        {
            "user_id": ["u1", "u2"],
            "required_int": ["10", "20"],
            "origin_latitude": ["-33.45", "bad"],
        }
    )

    work, stats, issues = _coerce_columns_by_dtype(
        df.copy(deep=True),
        schema=schema_coercion,
        schema_effective=schema_effective_coercion,
        target_schema_fields=target_fields_coercion,
        strict=False,
    )

    assert work["origin_latitude"].tolist() == ["-33.45", "bad"]
    assert str(work["origin_latitude"].dtype) == "object"
    assert "origin_latitude" not in stats
    assert_issue_absent(issues, "IMP.TYPE.COERCE_PARTIAL")