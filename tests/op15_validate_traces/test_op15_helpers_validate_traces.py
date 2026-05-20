from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

import numpy as np
import pandas as pd
import pytest

from pylondrina.datasets import TraceDataset
from pylondrina.errors import SchemaError, ValidationError
from pylondrina.reports import ConsistencyReport, Issue
from pylondrina.schema import FieldSpec, TraceSchema
from pylondrina.validation_traces import (
    TraceValidationOptions,
    _build_issues_summary,
    _check_trace_constraints,
    _check_trace_monotonic_time_per_user,
    _check_trace_required_and_types,
    _finalize_trace_validation,
    _invalid_mask_for_dtype,
    _is_valid_constraint_payload,
    _json_safe,
    _json_safe_row,
    _json_safe_scalar,
    _normalize_trace_validation_options,
    _preflight_validate_traces_request,
    _resolve_trace_validation_targets,
    _sample_index_list,
    _sample_list,
    _sample_rows,
)


def _codes(issues: Sequence[Issue]) -> list[str]:
    return [issue.code for issue in issues]


def test_validate_trace_general_helpers_handle_constraints_sampling_json_and_summary(
    assert_json_safe: Callable[[Any, str], None],
) -> None:
    """Verifica helpers generales de constraints, dtype masks, sampling, JSON e issues."""
    assert _is_valid_constraint_payload("string", "nullable", True) is True
    assert _is_valid_constraint_payload("float", "range", {"min": 0, "max": 10}) is True
    assert _is_valid_constraint_payload(
        "datetime",
        "datetime",
        {"allow_naive": False, "timezone": "UTC"},
    ) is True
    assert _is_valid_constraint_payload("string", "pattern", r"^[A-Z]+$") is True
    assert _is_valid_constraint_payload("string", "length", {"min": 1, "max": 5}) is True
    assert _is_valid_constraint_payload("bool", "unique", {"value": True}) is True

    assert _is_valid_constraint_payload("float", "range", {"minimum": 0}) is False
    assert _is_valid_constraint_payload("datetime", "datetime", {"foo": "bar"}) is False
    assert _is_valid_constraint_payload("string", "pattern", "[") is False

    mask_int, sample_int = _invalid_mask_for_dtype(
        pd.Series(["1", "2", "2.5", None]),
        "int",
    )
    assert mask_int.tolist() == [False, False, True, False]
    assert sample_int == ["2.5"]

    mask_float, sample_float = _invalid_mask_for_dtype(
        pd.Series(["1.2", "abc", None]),
        "float",
    )
    assert mask_float.tolist() == [False, True, False]
    assert sample_float == ["abc"]

    mask_dt, sample_dt = _invalid_mask_for_dtype(
        pd.Series(["2026-01-01", "not-a-date", None]),
        "datetime",
    )
    assert mask_dt.tolist() == [False, True, False]
    assert sample_dt == ["not-a-date"]

    mask_bool, sample_bool = _invalid_mask_for_dtype(
        pd.Series(["true", "0", "maybe", None]),
        "bool",
    )
    assert mask_bool.tolist() == [False, False, True, False]
    assert sample_bool == ["maybe"]

    df_sample = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": ["x", "y", "z"],
            "ts": pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03"]),
        },
        index=[10, 11, 12],
    )
    mask = pd.Series([True, False, True], index=df_sample.index)

    rows = _sample_rows(df_sample, mask, limit=2)
    assert len(rows) == min(int(mask.sum()), 2)
    assert_json_safe(rows, "sample_rows")

    idxs = _sample_index_list(df_sample.index, limit=2)
    assert idxs == list(df_sample.index[:2])

    vals = _sample_list([np.int64(1), pd.Timestamp("2026-01-01"), None], limit=3)
    assert_json_safe(vals, "sample_list_validate")

    assert _json_safe_scalar(np.int64(7)) == 7
    assert isinstance(_json_safe_scalar(np.int64(7)), int)
    assert _json_safe_scalar(np.bool_(True)) is True
    assert isinstance(_json_safe_scalar(pd.Timestamp("2026-01-01")), str)

    safe_payload = _json_safe(
        {
            "x": np.int64(1),
            "ts": pd.Timestamp("2026-01-01"),
            "items": {"b", "a"},
        }
    )
    assert_json_safe(safe_payload, "json_safe_payload")

    safe_row = _json_safe_row(
        {
            "x": np.int64(1),
            "ts": pd.Timestamp("2026-01-01"),
        }
    )
    assert_json_safe(safe_row, "json_safe_row")

    issues = [
        Issue(level="warning", code="VAL.TEST.WARN_A", message="warn A1"),
        Issue(level="warning", code="VAL.TEST.WARN_A", message="warn A2"),
        Issue(level="error", code="VAL.TEST.ERR_B", message="err B1"),
    ]
    summary = _build_issues_summary(issues)

    assert summary["counts"] == {"info": 0, "warning": 2, "error": 1}
    assert summary["top_codes"][0] == {"code": "VAL.TEST.WARN_A", "count": 2}
    assert summary["top_codes"][1] == {"code": "VAL.TEST.ERR_B", "count": 1}


def test_normalize_trace_validation_options_preserves_defaults_and_explicit_values() -> None:
    """Verifica defaults y valores explícitos de TraceValidationOptions efectivas."""
    opts = _normalize_trace_validation_options(None)

    assert isinstance(opts, TraceValidationOptions)
    assert opts.strict is False
    assert opts.sample_rows_per_issue == 5
    assert opts.validate_required_fields is True
    assert opts.validate_types_and_formats is True
    assert opts.validate_constraints is True
    assert opts.validate_monotonic_time_per_user is True

    opts = _normalize_trace_validation_options(
        TraceValidationOptions(
            strict=True,
            sample_rows_per_issue=3,
            validate_required_fields=True,
            validate_types_and_formats=False,
            validate_constraints=False,
            validate_monotonic_time_per_user=True,
        )
    )

    assert opts.strict is True
    assert opts.sample_rows_per_issue == 3
    assert opts.validate_required_fields is True
    assert opts.validate_types_and_formats is False
    assert opts.validate_constraints is False
    assert opts.validate_monotonic_time_per_user is True


def test_preflight_validate_traces_accepts_valid_request_skips_bad_constraint_and_rejects_categorical(
    valid_traces: TraceDataset,
    valid_trace_df: pd.DataFrame,
    validate_trace_fields: list[FieldSpec],
    make_trace_field: Callable[..., FieldSpec],
    make_trace_schema: Callable[..., TraceSchema],
    make_trace_dataset: Callable[..., TraceDataset],
    assert_issue_present: Callable[[Sequence[Issue], str], None],
) -> None:
    """Verifica preflight feliz, skip por constraint mal formada y fatal por categorical."""
    issues: list[Issue] = []
    skipped = _preflight_validate_traces_request(
        issues,
        traces=valid_traces,
        options_eff=TraceValidationOptions(),
    )

    assert skipped == {}
    assert _codes(issues) == []

    schema_skip = make_trace_schema(
        validate_trace_fields
        + [
            make_trace_field(
                "score",
                "float",
                required=False,
                constraints={"range": {"minimum": 0}},
            )
        ],
        required=["point_id", "user_id", "time_utc", "latitude", "longitude"],
        timezone=valid_traces.schema.timezone,
    )
    traces_skip = make_trace_dataset(
        valid_trace_df.assign(score=np.arange(len(valid_trace_df), dtype=float)),
        schema_skip,
        metadata={"events": [], "is_validated": False},
    )

    issues = []
    skipped = _preflight_validate_traces_request(
        issues,
        traces=traces_skip,
        options_eff=TraceValidationOptions(),
    )

    assert skipped == {"score": {"range"}}
    assert_issue_present(issues, "VAL.SCHEMA.CONSTRAINT_INVALID_FORMAT")

    schema_bad = make_trace_schema(
        [
            make_trace_field("point_id", "string", required=True),
            make_trace_field("user_id", "string", required=True),
            make_trace_field("time_utc", "datetime", required=True),
            make_trace_field("latitude", "float", required=True),
            make_trace_field("longitude", "float", required=True),
            make_trace_field("poi_type", "categorical", required=False),
        ],
        required=["point_id", "user_id", "time_utc", "latitude", "longitude"],
        timezone=valid_traces.schema.timezone,
    )
    traces_bad = make_trace_dataset(
        valid_trace_df[["point_id", "user_id", "time_utc", "latitude", "longitude"]],
        schema_bad,
        metadata={"events": [], "is_validated": False},
    )

    with pytest.raises(SchemaError) as exc_info:
        _preflight_validate_traces_request(
            [],
            traces=traces_bad,
            options_eff=TraceValidationOptions(),
        )

    assert exc_info.value.code == "VAL.SCHEMA.CATEGORICAL_NOT_ALLOWED"


def test_preflight_validate_traces_rejects_invalid_options_and_input_shape(
    valid_traces: TraceDataset,
    clone_tracedataset: Callable[[TraceDataset], TraceDataset],
) -> None:
    """Verifica abortos fatales de preflight por options y entrada no interpretable."""
    with pytest.raises(ValidationError) as sample_exc:
        _preflight_validate_traces_request(
            [],
            traces=valid_traces,
            options_eff=TraceValidationOptions(sample_rows_per_issue=0),
        )
    assert sample_exc.value.code == "VAL.OPTIONS.INVALID_SAMPLE_ROWS_PER_ISSUE"

    with pytest.raises(ValidationError) as flag_exc:
        _preflight_validate_traces_request(
            [],
            traces=valid_traces,
            options_eff=TraceValidationOptions(validate_constraints="yes"),  # type: ignore[arg-type]
        )
    assert flag_exc.value.code == "VAL.OPTIONS.INVALID_FLAG_VALUE"

    traces_missing_data = clone_tracedataset(valid_traces)
    traces_missing_data.data = None  # type: ignore[assignment]

    with pytest.raises(ValidationError) as data_exc:
        _preflight_validate_traces_request(
            [],
            traces=traces_missing_data,
            options_eff=TraceValidationOptions(),
        )
    assert data_exc.value.code == "VAL.INPUT.MISSING_DATAFRAME"


def test_resolve_trace_validation_targets_builds_required_checked_flags_and_nullable(
    valid_traces: TraceDataset,
    trace_core_columns: tuple[str, ...],
) -> None:
    """Verifica targets efectivos: required, checked_fields, checks y nulabilidad."""
    required_fields, checked_fields, checks_executed, effective_nullable = (
        _resolve_trace_validation_targets(
            valid_traces,
            options_eff=TraceValidationOptions(),
        )
    )

    assert required_fields == list(trace_core_columns)
    assert set(trace_core_columns).issubset(set(checked_fields))
    assert {"visit_code", "battery_pct", "speed_mps", "is_home"}.issubset(
        set(checked_fields)
    )
    assert checks_executed == {
        "required_fields": True,
        "types_and_formats": True,
        "constraints": True,
        "monotonic_time_per_user": True,
    }

    for field in trace_core_columns:
        assert effective_nullable[field] is False
    assert effective_nullable["visit_code"] is True
    assert effective_nullable["battery_pct"] is True
    assert effective_nullable["speed_mps"] is True
    assert effective_nullable["is_home"] is False


def test_check_trace_required_and_types_reports_missing_null_and_unparseable_without_mutating_data(
    valid_trace_df: pd.DataFrame,
    validate_trace_schema_base: TraceSchema,
    trace_core_columns: tuple[str, ...],
    assert_issue_present: Callable[[Sequence[Issue], str], None],
) -> None:
    """Verifica required faltante, nulos required, tipos no parseables y no mutación."""
    checked_fields = list(validate_trace_schema_base.fields.keys())

    issues: list[Issue] = []
    df_missing = valid_trace_df.drop(columns=["longitude"])

    _check_trace_required_and_types(
        issues,
        df_missing,
        schema=validate_trace_schema_base,
        required_fields=trace_core_columns,
        checked_fields=checked_fields,
        options_eff=TraceValidationOptions(),
    )

    assert_issue_present(issues, "VAL.REQUIRED.MISSING_COLUMN")

    issues = []
    df_bad = valid_trace_df.copy(deep=True)
    df_bad["latitude"] = df_bad["latitude"].astype("object")
    df_bad["time_utc"] = df_bad["time_utc"].astype("object")
    df_bad.loc[df_bad.index[0], "user_id"] = None
    df_bad.loc[df_bad.index[1], "latitude"] = "not-a-float"
    df_bad.loc[df_bad.index[1], "time_utc"] = "not-a-datetime"
    before = df_bad.copy(deep=True)

    _check_trace_required_and_types(
        issues,
        df_bad,
        schema=validate_trace_schema_base,
        required_fields=trace_core_columns,
        checked_fields=checked_fields,
        options_eff=TraceValidationOptions(sample_rows_per_issue=2),
    )

    assert_issue_present(issues, "VAL.REQUIRED.NULL_IN_REQUIRED")
    assert_issue_present(issues, "VAL.TYPES.UNPARSEABLE_VALUE")
    assert {issue.field for issue in issues}.issuperset(
        {"user_id", "latitude", "time_utc"}
    )
    pd.testing.assert_frame_equal(df_bad, before)


def test_check_trace_constraints_reports_nullable_range_pattern_length_and_unique_violations(
    make_trace_field: Callable[..., FieldSpec],
    make_trace_schema: Callable[..., TraceSchema],
    make_trace_dataset: Callable[..., TraceDataset],
    assert_issue_present: Callable[[Sequence[Issue], str], None],
) -> None:
    """Verifica violations agregadas por nullable, range, pattern, length y unique."""
    schema_constraints = make_trace_schema(
        [
            make_trace_field(
                "point_id",
                "string",
                required=True,
                constraints={"unique": True},
            ),
            make_trace_field("user_id", "string", required=True),
            make_trace_field("time_utc", "datetime", required=True),
            make_trace_field(
                "latitude",
                "float",
                required=True,
                constraints={"range": {"min": -90, "max": 90}},
            ),
            make_trace_field(
                "longitude",
                "float",
                required=True,
                constraints={"range": {"min": -180, "max": 180}},
            ),
            make_trace_field(
                "accuracy",
                "float",
                required=False,
                constraints={"nullable": False, "range": {"min": 0, "max": 50}},
            ),
            make_trace_field(
                "label",
                "string",
                required=False,
                constraints={"pattern": r"^[A-Z]{2}$", "length": {"min": 2, "max": 2}},
            ),
            make_trace_field(
                "sensor_id",
                "string",
                required=False,
                constraints={"unique": True},
            ),
        ],
        required=["point_id", "user_id", "time_utc", "latitude", "longitude"],
    )
    df_constraints = pd.DataFrame(
        {
            "point_id": ["p0", "p1"],
            "user_id": ["u1", "u2"],
            "time_utc": ["2026-01-01T08:00:00", "2026-01-01T09:00:00"],
            "latitude": [-33.45, -33.46],
            "longitude": [-70.66, -70.67],
            "accuracy": [None, 80.0],
            "label": ["A1", "TOO_LONG"],
            "sensor_id": ["s1", "s1"],
        }
    )
    before = df_constraints.copy(deep=True)

    required_fields, _, _, effective_nullable = _resolve_trace_validation_targets(
        make_trace_dataset(df_constraints, schema_constraints, metadata={"events": []}),
        options_eff=TraceValidationOptions(),
    )

    issues: list[Issue] = []
    _check_trace_constraints(
        issues,
        df_constraints,
        schema=schema_constraints,
        required_fields=required_fields,
        effective_nullable=effective_nullable,
        skipped_constraints={},
        options_eff=TraceValidationOptions(sample_rows_per_issue=2),
    )

    assert _codes(issues).count("VAL.CONSTRAINTS.VIOLATION") >= 4
    assert_issue_present(issues, "VAL.CONSTRAINTS.VIOLATION")
    fields_with_violations = {issue.field for issue in issues}
    assert {"accuracy", "label", "sensor_id"}.issubset(fields_with_violations)
    pd.testing.assert_frame_equal(df_constraints, before)


def test_check_trace_monotonic_time_per_user_reports_warning_only_for_temporal_regression(
    assert_issue_present: Callable[[Sequence[Issue], str], None],
) -> None:
    """Verifica ausencia de issue en orden válido y warning cuando hay retroceso temporal."""
    df_ok = pd.DataFrame(
        {
            "point_id": ["p0", "p1", "p2"],
            "user_id": ["u1", "u1", "u2"],
            "time_utc": [
                "2026-01-01T08:00:00",
                "2026-01-01T08:00:00",
                "2026-01-01T09:00:00",
            ],
            "latitude": [-33.45, -33.45, -33.46],
            "longitude": [-70.66, -70.66, -70.67],
        }
    )
    issues: list[Issue] = []

    _check_trace_monotonic_time_per_user(
        issues,
        df_ok,
        options_eff=TraceValidationOptions(),
    )

    assert _codes(issues) == []

    df_bad = pd.DataFrame(
        {
            "point_id": ["p0", "p1", "p2", "p3"],
            "user_id": ["u1", "u1", "u2", "u2"],
            "time_utc": [
                "2026-01-01T08:00:00",
                "2026-01-01T07:59:00",
                "2026-01-01T09:00:00",
                "2026-01-01T09:05:00",
            ],
            "latitude": [-33.45, -33.45, -33.46, -33.46],
            "longitude": [-70.66, -70.66, -70.67, -70.67],
        }
    )
    before = df_bad.copy(deep=True)
    issues = []

    _check_trace_monotonic_time_per_user(
        issues,
        df_bad,
        options_eff=TraceValidationOptions(sample_rows_per_issue=2),
    )

    assert_issue_present(issues, "VAL.TEMPORAL.NON_MONOTONIC_TIME")
    assert issues[0].level == "warning"
    pd.testing.assert_frame_equal(df_bad, before)


def test_finalize_trace_validation_builds_report_event_and_updates_validation_state(
    valid_trace_df: pd.DataFrame,
    validate_trace_schema_base: TraceSchema,
    make_trace_dataset: Callable[..., TraceDataset],
    assert_json_safe: Callable[[Any, str], None],
) -> None:
    """Verifica ConsistencyReport, summary, metadata is_validated y evento final."""
    checks_executed = {
        "required_fields": True,
        "types_and_formats": True,
        "constraints": True,
        "monotonic_time_per_user": True,
    }
    checked_fields = [
        "point_id",
        "user_id",
        "time_utc",
        "latitude",
        "longitude",
        "battery_pct",
    ]

    traces_ok = make_trace_dataset(
        valid_trace_df,
        validate_trace_schema_base,
        metadata={"dataset_id": "traces_001", "events": [], "is_validated": False},
    )
    data_ok_before = traces_ok.data.copy(deep=True)
    issues_ok = [
        Issue(level="warning", code="VAL.TEST.WARNING", message="warning de prueba")
    ]

    report_ok = _finalize_trace_validation(
        issues_ok,
        traces_ok,
        options_eff=TraceValidationOptions(sample_rows_per_issue=3),
        checked_fields=checked_fields,
        checks_executed=checks_executed,
    )

    assert isinstance(report_ok, ConsistencyReport)
    assert report_ok.summary["ok"] is True
    assert report_ok.summary["n_rows"] == len(data_ok_before)
    assert report_ok.summary["n_warnings"] == 1
    assert report_ok.summary["counts_by_code"] == {"VAL.TEST.WARNING": 1}
    assert traces_ok.metadata["is_validated"] is True
    assert len(traces_ok.metadata["events"]) == 1

    event_ok = traces_ok.metadata["events"][0]
    assert event_ok["op"] == "validate_traces"
    assert event_ok["summary"] == report_ok.summary
    assert event_ok["parameters"] == {
        "strict": False,
        "sample_rows_per_issue": 3,
        "validate_required_fields": True,
        "validate_types_and_formats": True,
        "validate_constraints": True,
        "validate_monotonic_time_per_user": True,
    }
    assert getattr(report_ok, "parameters", {}) == {}
    assert_json_safe(event_ok, "validate_trace_event_ok")
    pd.testing.assert_frame_equal(traces_ok.data, data_ok_before)

    traces_err = make_trace_dataset(
        valid_trace_df,
        validate_trace_schema_base,
        metadata={"dataset_id": "traces_002", "events": [], "is_validated": False},
    )
    data_err_before = traces_err.data.copy(deep=True)
    issues_err = [
        Issue(
            level="error",
            code="VAL.REQUIRED.MISSING_COLUMN",
            message="error de prueba",
        )
    ]

    report_err = _finalize_trace_validation(
        issues_err,
        traces_err,
        options_eff=TraceValidationOptions(sample_rows_per_issue=3),
        checked_fields=checked_fields,
        checks_executed=checks_executed,
    )

    assert report_err.summary["ok"] is False
    assert report_err.summary["n_errors"] == 1
    assert report_err.summary["counts_by_code"] == {"VAL.REQUIRED.MISSING_COLUMN": 1}
    assert traces_err.metadata["is_validated"] is False
    assert len(traces_err.metadata["events"]) == 1
    assert traces_err.metadata["events"][0]["op"] == "validate_traces"
    assert traces_err.metadata["events"][0]["summary"] == report_err.summary
    assert_json_safe(traces_err.metadata["events"][0], "validate_trace_event_err")
    pd.testing.assert_frame_equal(traces_err.data, data_err_before)