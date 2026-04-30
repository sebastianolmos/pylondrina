import json

import pandas as pd
import pytest

from pylondrina.errors import ImportError as PylondrinaImportError
from pylondrina.errors import SchemaError
from pylondrina.importing import (
    ImportOptions,
    _check_schema_for_import,
    _first_required_check_and_temporal_tier,
    _normalize_options,
)
from pylondrina.schema import DomainSpec, FieldSpec, TripSchema


def assert_json_safe(obj, label: str = "object") -> None:
    try:
        json.dumps(obj, default=str)
    except Exception as exc:
        raise AssertionError(f"{label} no es JSON-safe: {exc}") from exc


def issue_codes(issues) -> set[str]:
    assert issues is not None
    return {issue.code for issue in issues}


def assert_issue_present(issues, code: str) -> None:
    assert code in issue_codes(issues), f"No se encontró issue {code!r}. Issues: {issues!r}"


def assert_issue_absent(issues, code: str) -> None:
    assert code not in issue_codes(issues), f"No se esperaba issue {code!r}. Issues: {issues!r}"


def make_minimal_schema() -> TripSchema:
    return TripSchema(
        version="0.1.0",
        fields={
            "movement_id": FieldSpec(name="movement_id", dtype="string", required=True),
            "user_id": FieldSpec(name="user_id", dtype="string", required=True),
            "purpose": FieldSpec(name="purpose", dtype="categorical", required=False),
            "mode": FieldSpec(name="mode", dtype="categorical", required=False),
        },
        required=["movement_id", "user_id"],
        semantic_rules=None,
    )


def make_temporal_schema() -> TripSchema:
    return TripSchema(
        version="0.1.0",
        fields={
            "user_id": FieldSpec(name="user_id", dtype="string", required=True),
            "movement_id": FieldSpec(name="movement_id", dtype="string", required=True),
            "origin_time_utc": FieldSpec(name="origin_time_utc", dtype="datetime", required=False),
            "destination_time_utc": FieldSpec(name="destination_time_utc", dtype="datetime", required=False),
            "origin_time_local_hhmm": FieldSpec(name="origin_time_local_hhmm", dtype="string", required=False),
            "destination_time_local_hhmm": FieldSpec(name="destination_time_local_hhmm", dtype="string", required=False),
        },
        required=["user_id", "movement_id"],
        semantic_rules=None,
    )


def test_normalize_options_defaults():
    schema = make_minimal_schema()

    options, parameters, issues = _normalize_options(
        None,
        schema=schema,
        source_name="unit_test_source",
        h3_resolution=8,
    )

    assert isinstance(options, ImportOptions)
    assert options.keep_extra_fields is True
    assert options.selected_fields is None
    assert options.strict is False
    assert options.strict_domains is False
    assert options.single_stage is False
    assert options.source_timezone is None

    assert parameters == {
        "keep_extra_fields": True,
        "selected_fields": None,
        "strict": False,
        "strict_domains": False,
        "single_stage": False,
        "h3_resolution": 8,
        "source_name": "unit_test_source",
        "source_timezone": None,
    }

    assert issues == []


def test_normalize_options_empty_selected_fields_emits_issue_and_keeps_empty_list():
    schema = make_minimal_schema()

    options, parameters, issues = _normalize_options(
        ImportOptions(selected_fields=[]),
        schema=schema,
        source_name=None,
        h3_resolution=8,
    )

    assert options.selected_fields == []
    assert parameters["selected_fields"] == []
    assert_issue_present(issues, "IMP.OPTIONS.EMPTY_SELECTED_FIELD")


@pytest.mark.parametrize("strict", [False, True])
def test_normalize_options_invalid_selected_fields_always_raises(strict):
    schema = make_minimal_schema()

    with pytest.raises(PylondrinaImportError) as exc_info:
        _normalize_options(
            ImportOptions(selected_fields=["user_id", "fake_field"], strict=strict),
            schema=schema,
            source_name=None,
            h3_resolution=8,
        )

    assert_issue_present(exc_info.value.issues, "IMP.OPTIONS.INVALID_SELECTED_FIELD")

    issue = exc_info.value.issue
    assert issue.code == "IMP.OPTIONS.INVALID_SELECTED_FIELD"
    assert issue.level == "error"
    assert issue.details["selected_fields"] == ["user_id", "fake_field"]
    assert issue.details["invalid_fields"] == ["fake_field"]
    assert issue.details["reason"] == "not_in_schema"
    assert issue.details["action"] == "abort"
    

@pytest.mark.parametrize("bad_resolution", [-1, 16, 8.5, "8"])
def test_normalize_options_invalid_h3_resolution_non_strict_still_raises_if_issue_is_fatal(bad_resolution):
    schema = make_minimal_schema()

    with pytest.raises(PylondrinaImportError) as exc_info:
        _normalize_options(
            ImportOptions(strict=False),
            schema=schema,
            source_name=None,
            h3_resolution=bad_resolution,
        )

    assert_issue_present(exc_info.value.issues, "IMP.H3.INVALID_RESOLUTION")


def test_normalize_options_invalid_source_timezone_emits_warning_and_normalizes_to_none():
    schema = make_minimal_schema()

    options, parameters, issues = _normalize_options(
        ImportOptions(source_timezone="Santiago/Chile"),
        schema=schema,
        source_name=None,
        h3_resolution=8,
    )

    assert options.source_timezone is None
    assert parameters["source_timezone"] is None
    assert_issue_present(issues, "IMP.DATETIME.INVALID_SOURCE_TIMEZONE")


def test_normalize_options_valid_source_timezone_is_normalized():
    schema = make_minimal_schema()

    options, parameters, issues = _normalize_options(
        ImportOptions(source_timezone="utc"),
        schema=schema,
        source_name="source_a",
        h3_resolution=8,
    )

    assert options.source_timezone == "UTC"
    assert parameters["source_timezone"] == "UTC"
    assert parameters["source_name"] == "source_a"
    assert issues == []


def test_first_required_check_detects_tier_1_with_utc_fields():
    schema = make_temporal_schema()
    df = pd.DataFrame(
        {
            "user_id": ["u1", "u2"],
            "origin_time_utc": ["2026-03-01T08:00:00Z", "2026-03-01T09:00:00Z"],
            "destination_time_utc": ["2026-03-01T08:30:00Z", "2026-03-01T09:40:00Z"],
        }
    )

    issues, tier, fields_present = _first_required_check_and_temporal_tier(
        df,
        schema=schema,
        single_stage=False,
        strict=False,
    )

    assert tier == "tier_1"
    assert set(fields_present) == {"origin_time_utc", "destination_time_utc"}
    assert_issue_absent(issues, "IMP.TEMPORAL.TIER_LIMITED")
    assert_issue_absent(issues, "IMP.INPUT.MISSING_REQUIRED_FIELD")


def test_first_required_check_detects_tier_2_with_hhmm_fields():
    schema = make_temporal_schema()
    df = pd.DataFrame(
        {
            "user_id": ["u1", "u2"],
            "origin_time_local_hhmm": ["08:00", "09:00"],
            "destination_time_local_hhmm": ["08:30", "09:40"],
        }
    )

    issues, tier, fields_present = _first_required_check_and_temporal_tier(
        df,
        schema=schema,
        single_stage=False,
        strict=False,
    )

    assert tier == "tier_2"
    assert set(fields_present) == {"origin_time_local_hhmm", "destination_time_local_hhmm"}
    assert_issue_present(issues, "IMP.TEMPORAL.TIER_LIMITED")
    assert_issue_absent(issues, "IMP.INPUT.MISSING_REQUIRED_FIELD")


def test_first_required_check_detects_tier_3_without_od_time_fields():
    schema = make_temporal_schema()
    df = pd.DataFrame(
        {
            "user_id": ["u1", "u2"],
            "trip_id": ["t1", "t2"],
        }
    )

    issues, tier, fields_present = _first_required_check_and_temporal_tier(
        df,
        schema=schema,
        single_stage=False,
        strict=False,
    )

    assert tier == "tier_3"
    assert fields_present == []
    assert_issue_present(issues, "IMP.TEMPORAL.TIER_LIMITED")
    assert_issue_absent(issues, "IMP.INPUT.MISSING_REQUIRED_FIELD")


def test_first_required_check_empty_dataframe_emits_empty_dataframe_issue():
    schema = make_temporal_schema()
    df = pd.DataFrame(columns=["user_id", "origin_time_utc", "destination_time_utc"])

    issues, tier, fields_present = _first_required_check_and_temporal_tier(
        df,
        schema=schema,
        single_stage=False,
        strict=False,
    )

    assert tier == "tier_1"
    assert set(fields_present) == {"origin_time_utc", "destination_time_utc"}
    assert_issue_present(issues, "IMP.INPUT.EMPTY_DATAFRAME")


def test_first_required_check_missing_non_derivable_required_field_raises():
    schema = make_temporal_schema()
    df = pd.DataFrame(
        {
            "origin_time_utc": ["2026-03-01T08:00:00Z"],
            "destination_time_utc": ["2026-03-01T08:30:00Z"],
        }
    )

    with pytest.raises(PylondrinaImportError) as exc_info:
        _first_required_check_and_temporal_tier(
            df,
            schema=schema,
            single_stage=False,
            strict=False,
        )

    assert_issue_present(exc_info.value.issues, "IMP.INPUT.MISSING_REQUIRED_FIELD")


def test_check_schema_for_import_happy_path_minimal():
    schema = TripSchema(
        version="0.1.0",
        fields={
            "user_id": FieldSpec(name="user_id", dtype="string", required=True),
            "purpose": FieldSpec(
                name="purpose",
                dtype="categorical",
                required=False,
                domain=DomainSpec(values=["work", "study"], extendable=True, aliases=None),
            ),
        },
        required=["user_id"],
        semantic_rules=None,
    )

    issues, schema_effective, schema_version = _check_schema_for_import(schema)

    assert issues == []
    assert schema_version == "0.1.0"
    assert schema_effective.dtype_effective == {
        "user_id": "string",
        "purpose": "categorical",
    }
    assert schema_effective.overrides == {}


def test_check_schema_for_import_empty_fields_raises_schema_error():
    schema = TripSchema(
        version="0.1.0",
        fields={},
        required=["user_id"],
        semantic_rules=None,
    )

    with pytest.raises(SchemaError) as exc_info:
        _check_schema_for_import(schema)

    assert_issue_present(exc_info.value.issues, "SCH.TRIP_SCHEMA.EMPTY_FIELDS")


def test_check_schema_for_import_empty_required_raises_schema_error():
    schema = TripSchema(
        version="0.1.0",
        fields={
            "user_id": FieldSpec(name="user_id", dtype="string", required=True),
        },
        required=[],
        semantic_rules=None,
    )

    with pytest.raises(SchemaError) as exc_info:
        _check_schema_for_import(schema)

    assert_issue_present(exc_info.value.issues, "SCH.TRIP_SCHEMA.EMPTY_REQUIRED")


def test_check_schema_for_import_required_outside_fields_raises_import_error():
    schema = TripSchema(
        version="0.1.0",
        fields={
            "user_id": FieldSpec(name="user_id", dtype="string", required=True),
        },
        required=["user_id", "trip_id"],
        semantic_rules=None,
    )

    with pytest.raises(PylondrinaImportError) as exc_info:
        _check_schema_for_import(schema)

    assert_issue_present(exc_info.value.issues, "IMP.INPUT.MISSING_REQUIRED_FIELD")


def test_check_schema_for_import_unknown_dtype_falls_back_to_string():
    schema = TripSchema(
        version="0.1.0",
        fields={
            "user_id": FieldSpec(name="user_id", dtype="string", required=True),
            "weird_field": FieldSpec(name="weird_field", dtype="vector", required=False),
        },
        required=["user_id"],
        semantic_rules=None,
    )

    issues, schema_effective, _ = _check_schema_for_import(schema)

    assert_issue_present(issues, "SCH.FIELD_SPEC.UNKNOWN_DTYPE")
    assert schema_effective.dtype_effective["user_id"] == "string"
    assert schema_effective.dtype_effective["weird_field"] == "string"
    assert "weird_field" in schema_effective.overrides
    assert "dtype_invalid" in schema_effective.overrides["weird_field"]["reasons"]


def test_check_schema_for_import_categorical_without_domain_falls_back_to_string():
    schema = TripSchema(
        version="0.1.0",
        fields={
            "user_id": FieldSpec(name="user_id", dtype="string", required=True),
            "purpose": FieldSpec(name="purpose", dtype="categorical", required=False, domain=None),
        },
        required=["user_id"],
        semantic_rules=None,
    )

    issues, schema_effective, _ = _check_schema_for_import(schema)

    assert_issue_present(issues, "SCH.DOMAIN.MISSING_FOR_CATEGORICAL")
    assert schema_effective.dtype_effective["purpose"] == "string"
    assert "purpose" in schema_effective.overrides
    assert "categorical_no_domain" in schema_effective.overrides["purpose"]["reasons"]


def test_check_schema_for_import_empty_domain_emits_issue_but_keeps_categorical():
    schema = TripSchema(
        version="0.1.0",
        fields={
            "user_id": FieldSpec(name="user_id", dtype="string", required=True),
            "purpose": FieldSpec(
                name="purpose",
                dtype="categorical",
                required=False,
                domain=DomainSpec(values=[], extendable=True, aliases=None),
            ),
        },
        required=["user_id"],
        semantic_rules=None,
    )

    issues, schema_effective, _ = _check_schema_for_import(schema)

    assert_issue_present(issues, "SCH.DOMAIN.EMPTY_VALUES")
    assert schema_effective.dtype_effective["purpose"] == "categorical"
    assert schema_effective.overrides == {}


def test_check_schema_for_import_non_string_domain_values_emit_issue():
    schema = TripSchema(
        version="0.1.0",
        fields={
            "user_id": FieldSpec(name="user_id", dtype="string", required=True),
            "purpose": FieldSpec(
                name="purpose",
                dtype="categorical",
                required=False,
                domain=DomainSpec(values=["work", 123, None], extendable=True, aliases=None),
            ),
        },
        required=["user_id"],
        semantic_rules=None,
    )

    issues, schema_effective, _ = _check_schema_for_import(schema)

    assert_issue_present(issues, "SCH.DOMAIN.NON_STRING_VALUES")
    assert schema_effective.dtype_effective["purpose"] == "categorical"


def test_check_schema_for_import_unknown_constraint_raises_schema_error():
    schema = TripSchema(
        version="0.1.0",
        fields={
            "user_id": FieldSpec(
                name="user_id",
                dtype="string",
                required=True,
                constraints={"min_foo": 10},
            ),
        },
        required=["user_id"],
        semantic_rules=None,
    )

    with pytest.raises(SchemaError) as exc_info:
        _check_schema_for_import(schema)

    assert_issue_present(exc_info.value.issues, "SCH.CONSTRAINTS.UNKNOWN_RULE")


def test_check_schema_for_import_invalid_pattern_raises_schema_error():
    schema = TripSchema(
        version="0.1.0",
        fields={
            "user_id": FieldSpec(
                name="user_id",
                dtype="string",
                required=True,
                constraints={"pattern": "["},
            ),
        },
        required=["user_id"],
        semantic_rules=None,
    )

    with pytest.raises(SchemaError) as exc_info:
        _check_schema_for_import(schema)

    assert_issue_present(exc_info.value.issues, "SCH.CONSTRAINTS.INVALID_FORMAT")


def test_check_schema_for_import_invalid_constraints_container_raises_schema_error():
    schema = TripSchema(
        version="0.1.0",
        fields={
            "user_id": FieldSpec(
                name="user_id",
                dtype="string",
                required=True,
                constraints=["not", "a", "dict"],
            ),
        },
        required=["user_id"],
        semantic_rules=None,
    )

    with pytest.raises(SchemaError) as exc_info:
        _check_schema_for_import(schema)

    assert_issue_present(exc_info.value.issues, "SCH.CONSTRAINTS.INVALID_FORMAT")


def test_check_schema_for_import_incompatible_constraint_emits_issue_but_keeps_dtype():
    schema = TripSchema(
        version="0.1.0",
        fields={
            "trip_count": FieldSpec(
                name="trip_count",
                dtype="int",
                required=True,
                constraints={"pattern": r"^\d+$"},
            ),
        },
        required=["trip_count"],
        semantic_rules=None,
    )

    issues, schema_effective, _ = _check_schema_for_import(schema)

    assert_issue_present(issues, "SCH.CONSTRAINTS.INCOMPATIBLE_WITH_DTYPE")
    assert schema_effective.dtype_effective["trip_count"] == "int"


def test_check_schema_for_import_integrated_small_case():
    schema = TripSchema(
        version="0.1.0",
        fields={
            "user_id": FieldSpec(name="user_id", dtype="string", required=True),
            "weird_field": FieldSpec(name="weird_field", dtype="vector", required=False),
            "cat_no_domain": FieldSpec(name="cat_no_domain", dtype="categorical", required=False, domain=None),
            "cat_empty_domain": FieldSpec(
                name="cat_empty_domain",
                dtype="categorical",
                required=False,
                domain=DomainSpec(values=[], extendable=True, aliases=None),
            ),
            "cat_non_string": FieldSpec(
                name="cat_non_string",
                dtype="categorical",
                required=False,
                domain=DomainSpec(values=["ok", 1, None], extendable=True, aliases=None),
            ),
        },
        required=["user_id"],
        semantic_rules=None,
    )

    issues, schema_effective, schema_version = _check_schema_for_import(schema)

    assert schema_version == "0.1.0"

    assert_issue_present(issues, "SCH.FIELD_SPEC.UNKNOWN_DTYPE")
    assert_issue_present(issues, "SCH.DOMAIN.MISSING_FOR_CATEGORICAL")
    assert_issue_present(issues, "SCH.DOMAIN.EMPTY_VALUES")
    assert_issue_present(issues, "SCH.DOMAIN.NON_STRING_VALUES")

    assert schema_effective.dtype_effective["weird_field"] == "string"
    assert schema_effective.dtype_effective["cat_no_domain"] == "string"
    assert schema_effective.dtype_effective["cat_empty_domain"] == "categorical"
    assert schema_effective.dtype_effective["cat_non_string"] == "categorical"

    assert "dtype_invalid" in schema_effective.overrides["weird_field"]["reasons"]
    assert "categorical_no_domain" in schema_effective.overrides["cat_no_domain"]["reasons"]

    assert_json_safe(schema_effective.to_dict(), "schema_effective")