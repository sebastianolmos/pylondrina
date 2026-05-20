from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from typing import Any

import numpy as np
import pandas as pd
import pytest

from pylondrina.datasets import TraceDataset
from pylondrina.errors import ImportError as PylondrinaImportError
from pylondrina.errors import SchemaError
from pylondrina.importing_traces import (
    ImportTraceOptions,
    _build_issues_summary,
    _duplicated_columns,
    _finalize_import_traces_result,
    _json_is_serializable,
    _json_safe,
    _json_safe_scalar,
    _materialize_trace_core,
    _normalize_import_trace_options,
    _normalize_timezone_spec,
    _normalize_trace_time_utc,
    _preflight_import_traces_request,
    _resolve_trace_import_columns,
)
from pylondrina.reports import ImportReport, Issue
from pylondrina.schema import FieldSpec, TraceSchema


def _issue_codes(issues: list[Issue] | tuple[Issue, ...]) -> list[str]:
    return [issue.code for issue in issues]


def test_import_trace_general_helpers_are_json_safe_and_summarize_issues(
    assert_json_safe,
) -> None:
    """Verifica utilidades generales de OP-14 para duplicados, timezones, JSON e issues."""
    assert _duplicated_columns(["a", "b", "a", "c", "b", "a"]) == ["a", "b"]

    tz_obj, tz_kind = _normalize_timezone_spec("UTC")
    assert tz_obj == "UTC"
    assert tz_kind == "utc"

    tz_obj, tz_kind = _normalize_timezone_spec("-03:00")
    assert tz_kind == "offset"
    assert tz_obj.utcoffset(None) == -timedelta(hours=3)

    tz_obj, tz_kind = _normalize_timezone_spec("America/Santiago")
    assert tz_kind == "iana"
    assert tz_obj is not None

    tz_obj, tz_kind = _normalize_timezone_spec("not/a_real_timezone")
    assert tz_obj is None
    assert tz_kind == "invalid"

    assert _json_safe_scalar(np.int64(7)) == 7
    assert isinstance(_json_safe_scalar(np.int64(7)), int)
    assert _json_safe_scalar(np.float64(2.5)) == 2.5
    assert isinstance(_json_safe_scalar(np.float64(2.5)), float)
    assert _json_safe_scalar(np.bool_(True)) is True
    assert isinstance(_json_safe_scalar(pd.Timestamp("2026-01-01T08:00:00Z")), str)

    payload_safe = _json_safe(
        {
            "num": np.int64(5),
            "flag": np.bool_(False),
            "ts": pd.Timestamp("2026-01-01T08:00:00Z"),
            "vals": {np.int64(1), np.float64(2.5)},
        }
    )
    assert_json_safe(payload_safe, "payload_safe_import")
    assert _json_is_serializable(payload_safe) is True

    summary = _build_issues_summary(
        [
            Issue(level="warning", code="IMP.TEST.WARN_A", message="warn A1"),
            Issue(level="warning", code="IMP.TEST.WARN_A", message="warn A2"),
            Issue(level="error", code="IMP.TEST.ERR_B", message="err B1"),
        ]
    )
    assert summary["counts"] == {"info": 0, "warning": 2, "error": 1}
    assert summary["top_codes"][0] == {"code": "IMP.TEST.WARN_A", "count": 2}
    assert summary["top_codes"][1] == {"code": "IMP.TEST.ERR_B", "count": 1}


def test_normalize_import_trace_options_builds_effective_options_and_parameters(
    assert_json_safe,
) -> None:
    """Verifica defaults, normalización de selected_fields y parameters JSON-safe de OP-14."""
    options_eff, params = _normalize_import_trace_options(None)

    assert isinstance(options_eff, ImportTraceOptions)
    assert options_eff.keep_extra_fields is True
    assert options_eff.selected_fields is None
    assert options_eff.strict is False
    assert options_eff.source_timezone is None
    assert params["selected_fields"] is None
    assert_json_safe(params, "import_trace_parameters_default")

    options_eff, params = _normalize_import_trace_options(
        ImportTraceOptions(
            keep_extra_fields=False,
            selected_fields=("location_category", "noise"),
            strict=True,
            source_timezone="-03:00",
        )
    )

    assert options_eff.keep_extra_fields is False
    assert options_eff.selected_fields == ["location_category", "noise"]
    assert options_eff.strict is True
    assert options_eff.source_timezone == "-03:00"
    assert params["selected_fields"] == options_eff.selected_fields
    assert params["strict"] is True
    assert_json_safe(params, "import_trace_parameters_custom")


def test_preflight_import_traces_accepts_valid_request_and_rejects_invalid_configs(
    raw_points_helper_df: pd.DataFrame,
    base_import_schema: TraceSchema,
    raw_to_canonical_helper: dict[str, str],
    make_trace_field: Callable[..., FieldSpec],
    make_trace_schema: Callable[..., TraceSchema],
) -> None:
    """Verifica preflight feliz y abortos fatales públicos de configuración de OP-14."""
    issues: list[Issue] = []
    _preflight_import_traces_request(
        issues,
        df=raw_points_helper_df,
        schema=base_import_schema,
        field_correspondence=raw_to_canonical_helper,
        options_eff=ImportTraceOptions(),
    )
    assert _issue_codes(issues) == []

    with pytest.raises(PylondrinaImportError) as selected_exc:
        _preflight_import_traces_request(
            [],
            df=raw_points_helper_df,
            schema=base_import_schema,
            field_correspondence=raw_to_canonical_helper,
            options_eff=ImportTraceOptions(selected_fields="location_category"),
        )
    assert selected_exc.value.code == "IMP.OPTIONS.INVALID_SELECTED_FIELDS_SPEC"

    schema_bad_categorical = make_trace_schema(
        [
            make_trace_field("point_id", "string", required=True),
            make_trace_field("user_id", "string", required=True),
            make_trace_field("time_utc", "datetime", required=True),
            make_trace_field("latitude", "float", required=True),
            make_trace_field("longitude", "float", required=True),
            make_trace_field("poi_type", "categorical", required=False),
        ],
        required=["point_id", "user_id", "time_utc", "latitude", "longitude"],
    )
    with pytest.raises(SchemaError) as categorical_exc:
        _preflight_import_traces_request(
            [],
            df=raw_points_helper_df,
            schema=schema_bad_categorical,
            field_correspondence=raw_to_canonical_helper,
            options_eff=ImportTraceOptions(),
        )
    assert categorical_exc.value.code == "SCH.TRACE_SCHEMA.CATEGORICAL_NOT_ALLOWED"

    with pytest.raises(PylondrinaImportError) as timezone_exc:
        _preflight_import_traces_request(
            [],
            df=raw_points_helper_df,
            schema=base_import_schema,
            field_correspondence=raw_to_canonical_helper,
            options_eff=ImportTraceOptions(source_timezone="Not/A_Real_Timezone"),
        )
    assert timezone_exc.value.code == "IMP.OPTIONS.INVALID_SOURCE_TIMEZONE"


def test_resolve_trace_import_columns_applies_mapping_and_extra_field_policy(
    raw_points_helper_df: pd.DataFrame,
    base_import_schema: TraceSchema,
    raw_to_canonical_helper: dict[str, str],
    assert_issue_present,
) -> None:
    """Verifica renombrado efectivo, preservación/descarte de extras y unknown selected fields."""
    source_before = raw_points_helper_df.copy(deep=True)
    issues: list[Issue] = []

    work, applied, n_fields_mapped = _resolve_trace_import_columns(
        issues,
        raw_points_helper_df,
        schema=base_import_schema,
        field_correspondence=raw_to_canonical_helper,
        options_eff=ImportTraceOptions(keep_extra_fields=True),
    )

    expected_core_and_schema_fields = [
        "user_id",
        "time_utc",
        "latitude",
        "longitude",
        "location_category",
    ]
    assert work.columns[: len(expected_core_and_schema_fields)].tolist() == (
        expected_core_and_schema_fields
    )
    assert "noise" in work.columns
    assert applied == raw_to_canonical_helper
    assert n_fields_mapped == len(raw_to_canonical_helper)
    assert _issue_codes(issues) == []

    for canonical_field, source_field in raw_to_canonical_helper.items():
        pd.testing.assert_series_equal(
            work[canonical_field].reset_index(drop=True),
            source_before[source_field].reset_index(drop=True),
            check_names=False,
        )
    pd.testing.assert_series_equal(
        work["noise"].reset_index(drop=True),
        source_before["noise"].reset_index(drop=True),
        check_names=False,
    )

    issues = []
    work, applied, n_fields_mapped = _resolve_trace_import_columns(
        issues,
        raw_points_helper_df,
        schema=base_import_schema,
        field_correspondence=raw_to_canonical_helper,
        options_eff=ImportTraceOptions(
            keep_extra_fields=False,
            selected_fields=["location_category", "missing_field"],
        ),
    )

    assert set(work.columns) == {
        "user_id",
        "time_utc",
        "latitude",
        "longitude",
        "location_category",
    }
    assert "noise" not in work.columns
    assert "missing_field" not in work.columns
    assert applied == raw_to_canonical_helper
    assert n_fields_mapped == len(raw_to_canonical_helper)
    assert_issue_present(issues, "IMP.OPTIONS.SELECTED_FIELDS_UNKNOWN")
    assert_issue_present(issues, "IMP.OPTIONS.EXTRA_FIELDS_DROPPED")

    pd.testing.assert_frame_equal(raw_points_helper_df, source_before)


def test_materialize_trace_core_generates_point_id_and_rejects_missing_required_core(
    assert_issue_present,
) -> None:
    """Verifica generación de point_id y abort fatal cuando falta núcleo no derivable."""
    df_no_point_id = pd.DataFrame(
        {
            "user_id": ["u1", "u2"],
            "time_utc": ["2026-01-01 08:00:00", "2026-01-01 09:30:00"],
            "latitude": [-33.45, -33.46],
            "longitude": [-70.66, -70.67],
        }
    )
    source_before = df_no_point_id.copy(deep=True)
    issues: list[Issue] = []

    work, point_id_generated = _materialize_trace_core(issues, df_no_point_id, strict=False)

    assert point_id_generated is True
    assert work.columns[0] == "point_id"
    assert work["point_id"].notna().all()
    assert work["point_id"].is_unique
    assert len(work) == len(source_before)
    assert_issue_present(issues, "IMP.CORE.POINT_ID_GENERATED")
    pd.testing.assert_frame_equal(df_no_point_id, source_before)

    df_missing_core = pd.DataFrame(
        {
            "user_id": ["u1"],
            "time_utc": ["2026-01-01 08:00:00"],
            "latitude": [-33.45],
        }
    )
    with pytest.raises(PylondrinaImportError) as exc_info:
        _materialize_trace_core([], df_missing_core, strict=False)
    assert exc_info.value.code == "IMP.CORE.MINIMUM_FIELDS_UNREACHABLE"


def test_normalize_trace_time_utc_resolves_timezone_warning_and_bad_values(
    base_import_fields: list[FieldSpec],
    make_trace_schema: Callable[..., TraceSchema],
    assert_issue_present,
) -> None:
    """Verifica normalización temporal, warning por timezone no resuelta y error por parseo."""
    schema_no_tz = make_trace_schema(
        base_import_fields,
        required=["point_id", "user_id", "time_utc", "latitude", "longitude"],
        timezone=None,
    )
    df_time = pd.DataFrame(
        {
            "point_id": ["p0", "p1"],
            "user_id": ["u1", "u2"],
            "time_utc": ["2026-01-01 08:00:00", "2026-01-01 09:30:00"],
            "latitude": [-33.45, -33.46],
            "longitude": [-70.66, -70.67],
        }
    )
    source_before = df_time.copy(deep=True)

    issues: list[Issue] = []
    norm, descriptor = _normalize_trace_time_utc(
        issues,
        df_time,
        schema=schema_no_tz,
        options_eff=ImportTraceOptions(source_timezone="-03:00"),
    )
    expected = (
        pd.to_datetime(source_before["time_utc"])
        .dt.tz_localize("Etc/GMT+3")
        .dt.tz_convert("UTC")
    )
    assert _issue_codes(issues) == []
    assert descriptor["timezone_resolution"] == "options.source_timezone"
    assert descriptor["source_timezone_used"] == "-03:00"
    assert descriptor["normalized_to_utc"] is True
    pd.testing.assert_series_equal(
        norm["time_utc"].reset_index(drop=True),
        expected.reset_index(drop=True),
        check_names=False,
    )

    issues = []
    norm_unresolved, descriptor_unresolved = _normalize_trace_time_utc(
        issues,
        df_time,
        schema=schema_no_tz,
        options_eff=ImportTraceOptions(source_timezone=None),
    )
    assert_issue_present(issues, "IMP.TIME.TIMEZONE_UNRESOLVED")
    assert descriptor_unresolved["timezone_resolution"] == "unresolved"
    assert descriptor_unresolved["normalized_to_utc"] is False
    assert len(norm_unresolved) == len(source_before)

    df_bad_time = df_time.copy()
    df_bad_time.loc[df_bad_time.index[-1], "time_utc"] = "not-a-datetime"
    with pytest.raises(PylondrinaImportError) as exc_info:
        _normalize_trace_time_utc(
            [],
            df_bad_time,
            schema=schema_no_tz,
            options_eff=ImportTraceOptions(source_timezone="UTC"),
        )
    assert exc_info.value.code == "IMP.TIME.NORMALIZATION_FAILED"

    pd.testing.assert_frame_equal(df_time, source_before)


def test_finalize_import_traces_result_builds_dataset_report_metadata_and_event(
    base_import_schema: TraceSchema,
    raw_to_canonical_helper: dict[str, str],
    assert_json_safe,
) -> None:
    """Verifica cierre de OP-14: TraceDataset, ImportReport, metadata, evento y provenance."""
    df_final = pd.DataFrame(
        {
            "point_id": ["p0", "p1"],
            "user_id": ["u1", "u2"],
            "time_utc": pd.to_datetime(["2026-01-01 11:00:00", "2026-01-01 12:30:00"]),
            "latitude": [-33.45, -33.46],
            "longitude": [-70.66, -70.67],
            "location_category": ["home", "work"],
        }
    )
    issues = [
        Issue(level="info", code="IMP.CORE.POINT_ID_GENERATED", message="point_id generado"),
    ]
    options_eff, parameters_effective = _normalize_import_trace_options(
        ImportTraceOptions(source_timezone="-03:00")
    )
    temporal_descriptor = {
        "time_field": "time_utc",
        "timezone_resolution": "options.source_timezone",
        "source_timezone_used": "-03:00",
        "schema_timezone": None,
        "normalized_to_utc": True,
    }
    provenance = {"notebook": "helper_level_tests"}

    dataset, report = _finalize_import_traces_result(
        issues,
        df_final,
        schema=base_import_schema,
        source_name="raw_checkins",
        options_eff=options_eff,
        parameters_effective=parameters_effective,
        field_map_applied=raw_to_canonical_helper,
        n_fields_mapped=len(raw_to_canonical_helper),
        point_id_generated=True,
        temporal_descriptor=temporal_descriptor,
        provenance=provenance,
        rows_in=len(df_final),
    )

    assert isinstance(dataset, TraceDataset)
    assert isinstance(report, ImportReport)
    pd.testing.assert_frame_equal(dataset.data, df_final)

    assert dataset.metadata["is_validated"] is False
    assert dataset.metadata["schema_version"] == base_import_schema.version
    assert dataset.metadata["point_id_generated"] is True
    assert dataset.metadata["source"] == {"name": "raw_checkins"}
    assert dataset.metadata["field_correspondence_applied"] == raw_to_canonical_helper
    assert dataset.metadata["temporal"] == temporal_descriptor
    assert len(dataset.metadata["events"]) == 1

    event = dataset.metadata["events"][0]
    assert event["op"] == "import_traces"
    assert event["summary"] == report.summary
    assert event["parameters"] == report.parameters
    assert event["issues_summary"]["counts"] == {"info": 1, "warning": 0, "error": 0}

    assert report.ok is True
    assert report.summary == {
        "rows_in": len(df_final),
        "rows_out": len(df_final),
        "n_fields_mapped": len(raw_to_canonical_helper),
        "point_id_generated": True,
    }
    assert report.field_correspondence == raw_to_canonical_helper
    assert report.value_correspondence == {}
    assert report.schema_version == base_import_schema.version
    assert dataset.provenance == provenance

    assert_json_safe(dataset.metadata, "import_trace_metadata")
    assert_json_safe(report.summary, "import_trace_report_summary")
    assert_json_safe(report.parameters, "import_trace_report_parameters")
    assert_json_safe(event, "import_trace_event")