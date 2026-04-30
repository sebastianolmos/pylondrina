import json
import math

import numpy as np
import pandas as pd
import pytest

from pylondrina.importing import (
    _build_issues_summary,
    _build_keep_schema_fields,
    _dm_to_dd,
    _dms_to_dd,
    _get_unknown_token,
    _is_already_correct_dtype,
    _normalize_hhmm_series,
    _normalize_source_timezone,
    _parse_coord_value,
)
from pylondrina.reports import Issue
from pylondrina.schema import DomainSpec, FieldSpec, TripSchema


def assert_json_safe(obj, label: str = "object") -> None:
    try:
        json.dumps(obj, default=str)
    except Exception as exc:
        raise AssertionError(f"{label} no es JSON-safe: {exc}") from exc


def assert_series_dtype(series: pd.Series, expected: str) -> None:
    assert str(series.dtype) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, (None, "none")),
        ("", (None, "empty")),
        ("   ", (None, "empty")),
        ("UTC", ("UTC", "utc")),
        ("utc", ("UTC", "utc")),
        ("Z", ("UTC", "utc")),
        ("-03:00", ("-03:00", "offset")),
        ("+00:00", ("+00:00", "offset")),
        ("America/Santiago", ("America/Santiago", "iana")),
        ("  America/Santiago  ", ("America/Santiago", "iana")),
        ("Santiago/Chile", (None, "invalid")),
        ("abc", (None, "invalid")),
    ],
)
def test_normalize_source_timezone(raw, expected):
    assert _normalize_source_timezone(raw) == expected


def test_get_unknown_token():
    domain_none = None
    domain_unknown = DomainSpec(values=["bus", "unknown", "metro"], extendable=False, aliases=None)
    domain_other = DomainSpec(values=["bus", "other", "metro"], extendable=False, aliases=None)
    domain_upper_other = DomainSpec(values=["bus", "OTHER", "metro"], extendable=False, aliases=None)
    domain_title_unknown = DomainSpec(values=["bus", "Unknown", "metro"], extendable=False, aliases=None)
    domain_no_candidate = DomainSpec(values=["bus", "metro"], extendable=False, aliases=None)
    domain_empty = DomainSpec(values=[], extendable=False, aliases=None)

    assert _get_unknown_token(domain_none) == "unknown"
    assert _get_unknown_token(domain_unknown) == "unknown"
    assert _get_unknown_token(domain_other) == "other"
    assert _get_unknown_token(domain_upper_other) == "OTHER"
    assert _get_unknown_token(domain_title_unknown) == "Unknown"
    assert _get_unknown_token(domain_no_candidate) == "unknown"
    assert _get_unknown_token(domain_empty) == "unknown"


def test_is_already_correct_dtype():
    s_string = pd.Series(["a", "b", pd.NA], dtype="string")
    s_int = pd.Series([1, 2, pd.NA], dtype="Int64")
    s_float = pd.Series([1.5, 2.5, np.nan], dtype="float64")
    s_bool = pd.Series([True, False, pd.NA], dtype="boolean")
    s_dt_naive = pd.Series(pd.to_datetime(["2026-03-01 08:00:00", None]))
    s_dt_utc = pd.Series(pd.to_datetime(["2026-03-01T08:00:00Z", None], utc=True))
    s_obj = pd.Series(["a", "b", None], dtype="object")

    assert _is_already_correct_dtype(s_string, "string") is True
    assert _is_already_correct_dtype(s_string, "categorical") is True
    assert _is_already_correct_dtype(s_int, "int") is True
    assert _is_already_correct_dtype(s_float, "float") is True
    assert _is_already_correct_dtype(s_bool, "bool") is True
    assert _is_already_correct_dtype(s_dt_naive, "datetime") is True
    assert _is_already_correct_dtype(s_dt_utc, "datetime") is True

    assert _is_already_correct_dtype(s_obj, "string") is False
    assert _is_already_correct_dtype(s_string, "int") is False
    assert _is_already_correct_dtype(s_int, "float") is False
    assert _is_already_correct_dtype(s_float, "bool") is False
    assert _is_already_correct_dtype(s_bool, "datetime") is False


def test_dm_to_dd():
    assert _dm_to_dd(33, 30, "N") == pytest.approx(33.5)
    assert _dm_to_dd(70, 30, "W") == pytest.approx(-70.5)
    assert _dm_to_dd(33, 27.6, "S") == pytest.approx(-(33 + 27.6 / 60.0))


def test_dms_to_dd():
    assert _dms_to_dd(33, 30, 0, "N") == pytest.approx(33.5)
    assert _dms_to_dd(70, 30, 0, "W") == pytest.approx(-70.5)
    assert _dms_to_dd(33, 27, 36, "S") == pytest.approx(-(33 + 27 / 60.0 + 36 / 3600.0))


@pytest.mark.parametrize(
    ("raw", "expected_value", "expected_status"),
    [
        (np.nan, np.nan, "null"),
        (None, np.nan, "null"),
        (10, 10.0, "numeric"),
        (-33.45, -33.45, "numeric"),
        ("-33.446160", -33.446160, "dd_direct"),
        ("335208,7188", 335208.7188, "dd_comma_decimal"),
        ("33 27.0000 S", -33.45, "dm"),
        ('33° 27\' 00" S', -33.45, "dms"),
        ("33 27 00 S", -33.45, "dms"),
        ("70 30 00 W", -70.5, "dms"),
        ("12 05 30.5 N", 12.091805556, "dms"),
        ("", np.nan, "empty"),
        ("   ", np.nan, "empty"),
        ("abc", np.nan, "unparsed"),
    ],
)
def test_parse_coord_value(raw, expected_value, expected_status):
    value, status = _parse_coord_value(raw)

    assert status == expected_status

    if pd.isna(expected_value):
        assert pd.isna(value)
    else:
        assert value == pytest.approx(expected_value, abs=1e-9)


def test_normalize_hhmm_series():
    s_hhmm = pd.Series(
        ["08:23", "12:30", "24:00", "ab:cd", "", None, "7:05", "09:60", " 07:05 ", "00:00", "23:59"],
        dtype="object",
    )

    out_hhmm, stats_hhmm = _normalize_hhmm_series(s_hhmm)

    assert_series_dtype(out_hhmm, "string")

    assert out_hhmm.iloc[0] == "08:23"
    assert out_hhmm.iloc[1] == "12:30"
    assert out_hhmm.iloc[8] == "07:05"
    assert out_hhmm.iloc[9] == "00:00"
    assert out_hhmm.iloc[10] == "23:59"

    assert pd.isna(out_hhmm.iloc[2])
    assert pd.isna(out_hhmm.iloc[3])
    assert pd.isna(out_hhmm.iloc[4])
    assert pd.isna(out_hhmm.iloc[5])
    assert pd.isna(out_hhmm.iloc[6])
    assert pd.isna(out_hhmm.iloc[7])

    assert stats_hhmm["n_total"] == 11
    assert stats_hhmm["n_invalid"] == 4
    assert stats_hhmm["n_na"] == 6


def test_build_keep_schema_fields_with_selected_fields_none_keeps_all_schema_fields():
    schema = TripSchema(
        version="0.1.0",
        fields={
            "movement_id": FieldSpec(name="movement_id", dtype="string", required=True),
            "trip_id": FieldSpec(name="trip_id", dtype="string", required=False),
            "movement_seq": FieldSpec(name="movement_seq", dtype="int", required=False),
            "purpose": FieldSpec(name="purpose", dtype="categorical", required=False),
            "mode": FieldSpec(name="mode", dtype="categorical", required=False),
        },
        required=["movement_id", "trip_id"],
        semantic_rules=None,
    )

    result = _build_keep_schema_fields(schema, None)

    assert result["schema_fields"] == {"movement_id", "trip_id", "movement_seq", "purpose", "mode"}
    assert result["required_fields"] == {"movement_id", "trip_id"}
    assert result["keep_schema_fields"] == {"movement_id", "trip_id", "movement_seq", "purpose", "mode"}


def test_build_keep_schema_fields_with_empty_selected_fields_keeps_required_only():
    schema = TripSchema(
        version="0.1.0",
        fields={
            "movement_id": FieldSpec(name="movement_id", dtype="string", required=True),
            "trip_id": FieldSpec(name="trip_id", dtype="string", required=False),
            "purpose": FieldSpec(name="purpose", dtype="categorical", required=False),
        },
        required=["movement_id", "trip_id"],
        semantic_rules=None,
    )

    result = _build_keep_schema_fields(schema, [])

    assert result["keep_schema_fields"] == {"movement_id", "trip_id"}


def test_build_keep_schema_fields_with_valid_subset_keeps_required_union_selected():
    schema = TripSchema(
        version="0.1.0",
        fields={
            "movement_id": FieldSpec(name="movement_id", dtype="string", required=True),
            "trip_id": FieldSpec(name="trip_id", dtype="string", required=False),
            "purpose": FieldSpec(name="purpose", dtype="categorical", required=False),
            "mode": FieldSpec(name="mode", dtype="categorical", required=False),
        },
        required=["movement_id", "trip_id"],
        semantic_rules=None,
    )

    result = _build_keep_schema_fields(schema, ["purpose", "mode"])

    assert result["keep_schema_fields"] == {"movement_id", "trip_id", "purpose", "mode"}


def test_build_keep_schema_fields_with_invalid_selected_fields_raises_value_error():
    schema = TripSchema(
        version="0.1.0",
        fields={
            "movement_id": FieldSpec(name="movement_id", dtype="string", required=True),
            "purpose": FieldSpec(name="purpose", dtype="categorical", required=False),
        },
        required=["movement_id"],
        semantic_rules=None,
    )

    with pytest.raises(ValueError):
        _build_keep_schema_fields(schema, ["purpose", "fake_field"])


def test_build_issues_summary_empty():
    assert _build_issues_summary([]) == {"counts": {}, "by_code": {}}


def test_build_issues_summary_counts_by_level_and_code():
    issues = [
        Issue(level="warning", code="CODE_A", message="a"),
        Issue(level="warning", code="CODE_A", message="a2"),
        Issue(level="info", code="CODE_B", message="b"),
        Issue(level="error", code="CODE_C", message="c"),
    ]

    summary = _build_issues_summary(issues)

    assert summary["counts"] == {
        "warning": 2,
        "info": 1,
        "error": 1,
    }

    assert summary["by_code"] == {
        "CODE_A": 2,
        "CODE_B": 1,
        "CODE_C": 1,
    }

    assert_json_safe(summary, "issues_summary")