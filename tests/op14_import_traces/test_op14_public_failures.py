from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pandas as pd
import pytest

from pylondrina.errors import ImportError as PylondrinaImportError
from pylondrina.errors import SchemaError
from pylondrina.importing_traces import ImportTraceOptions, import_traces_from_dataframe
from pylondrina.reports import ImportReport
from pylondrina.schema import FieldSpec, TraceSchema


def test_import_traces_degrades_invalid_provenance_without_aborting(
    raw_points_df: pd.DataFrame,
    trace_schema_base: TraceSchema,
    field_correspondence_base: dict[str, str],
    assert_issue_present: Callable[[list[Any], str], None],
    assert_json_safe: Callable[[Any, str], None],
) -> None:
    """Verifica que provenance no mapping se omita con warning sin abortar OP-14."""
    source_before = raw_points_df.copy(deep=True)

    traces, report = import_traces_from_dataframe(
        raw_points_df,
        trace_schema_base,
        options=ImportTraceOptions(source_timezone="UTC"),
        field_correspondence=field_correspondence_base,
        provenance=["this", "is", "not", "a", "mapping"],
    )

    assert isinstance(report, ImportReport)
    assert report.ok is True
    assert report.summary["rows_in"] == len(source_before)
    assert report.summary["rows_out"] == len(traces.data) == len(source_before)

    assert_issue_present(report.issues, "IMP.PROVENANCE.INVALID_STRUCTURE")
    assert traces.provenance == {}
    assert traces.metadata["is_validated"] is False
    assert traces.metadata["events"][-1]["op"] == "import_traces"

    assert_json_safe(traces.metadata, "traces.metadata")
    assert_json_safe(report.summary, "report.summary")
    assert_json_safe(report.parameters, "report.parameters")

    pd.testing.assert_frame_equal(raw_points_df, source_before)


def test_import_traces_raises_when_canonical_minimum_is_unreachable(
    raw_points_df: pd.DataFrame,
    trace_schema_base: TraceSchema,
    field_correspondence_base: dict[str, str],
    assert_issue_present: Callable[[tuple[Any, ...], str], None],
) -> None:
    """Verifica abort fatal cuando no se puede materializar el núcleo canónico."""
    raw_points_missing_lat = raw_points_df.drop(columns=["lat_src"])

    with pytest.raises(PylondrinaImportError) as exc_info:
        import_traces_from_dataframe(
            raw_points_missing_lat,
            trace_schema_base,
            options=ImportTraceOptions(source_timezone="UTC"),
            field_correspondence=field_correspondence_base,
        )

    exc = exc_info.value
    assert exc.code == "IMP.CORE.MINIMUM_FIELDS_UNREACHABLE"
    assert exc.issues is not None
    assert_issue_present(exc.issues, "MAP.FIELDS.SOURCE_COLUMN_NOT_FOUND")
    assert_issue_present(exc.issues, "IMP.CORE.POINT_ID_GENERATED")
    assert_issue_present(exc.issues, "IMP.CORE.MINIMUM_FIELDS_UNREACHABLE")

    assert "lat_src" not in raw_points_missing_lat.columns


def test_import_traces_raises_for_invalid_source_timezone(
    raw_points_df: pd.DataFrame,
    trace_schema_base: TraceSchema,
    field_correspondence_base: dict[str, str],
) -> None:
    """Verifica abort fatal cuando source_timezone explícita no es interpretable."""
    source_before = raw_points_df.copy(deep=True)

    with pytest.raises(PylondrinaImportError) as exc_info:
        import_traces_from_dataframe(
            raw_points_df,
            trace_schema_base,
            options=ImportTraceOptions(source_timezone="Not/A_Real_Timezone"),
            field_correspondence=field_correspondence_base,
        )

    exc = exc_info.value
    assert exc.code == "IMP.OPTIONS.INVALID_SOURCE_TIMEZONE"
    assert exc.issues is not None
    assert exc.issues[-1].code == "IMP.OPTIONS.INVALID_SOURCE_TIMEZONE"

    pd.testing.assert_frame_equal(raw_points_df, source_before)


def test_import_traces_raises_for_categorical_dtype_in_trace_schema(
    raw_points_df: pd.DataFrame,
    trace_schema_base: TraceSchema,
    field_correspondence_base: dict[str, str],
    make_trace_field: Callable[..., FieldSpec],
    make_trace_schema: Callable[..., TraceSchema],
) -> None:
    """Verifica abort fatal cuando TraceSchema declara dtype categorical en OP-14."""
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
        timezone=trace_schema_base.timezone,
        crs=trace_schema_base.crs,
    )

    source_before = raw_points_df.copy(deep=True)

    with pytest.raises(SchemaError) as exc_info:
        import_traces_from_dataframe(
            raw_points_df,
            schema_bad_categorical,
            options=ImportTraceOptions(source_timezone="UTC"),
            field_correspondence=field_correspondence_base,
        )

    exc = exc_info.value
    assert exc.code == "SCH.TRACE_SCHEMA.CATEGORICAL_NOT_ALLOWED"
    assert exc.issues is not None
    assert exc.issues[-1].code == "SCH.TRACE_SCHEMA.CATEGORICAL_NOT_ALLOWED"

    pd.testing.assert_frame_equal(raw_points_df, source_before)