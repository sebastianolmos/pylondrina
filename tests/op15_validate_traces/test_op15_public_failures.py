from __future__ import annotations

from collections.abc import Callable

import pandas as pd
import pytest

from pylondrina.datasets import TraceDataset
from pylondrina.errors import SchemaError, ValidationError
from pylondrina.schema import FieldSpec, TraceSchema
from pylondrina.validation_traces import TraceValidationOptions, validate_traces


def test_validate_traces_strict_raises_after_recording_validation_evidence(
    valid_traces: TraceDataset,
    clone_tracedataset: Callable[[TraceDataset], TraceDataset],
) -> None:
    """Verifica que strict=True eleva ValidationError después de registrar evidencia."""
    traces = clone_tracedataset(valid_traces)
    data_before = traces.data.copy(deep=True)
    events_before = list(traces.metadata.get("events", []))

    first_index = traces.data.index[0]
    traces.data.loc[first_index, "battery_pct"] = 500
    data_with_error = traces.data.copy(deep=True)

    with pytest.raises(ValidationError) as exc_info:
        validate_traces(traces, options=TraceValidationOptions(strict=True))

    exc = exc_info.value
    assert exc.code == "VAL.CONSTRAINTS.VIOLATION"
    assert exc.issue is not None
    assert exc.issue.code == "VAL.CONSTRAINTS.VIOLATION"
    assert exc.issues is not None
    assert any(issue.code == "VAL.CONSTRAINTS.VIOLATION" for issue in exc.issues)

    assert exc.details is not None
    assert exc.details["summary"]["ok"] is False
    assert exc.details["event"]["op"] == "validate_traces"

    assert traces.metadata["is_validated"] is False
    assert len(traces.metadata["events"]) == len(events_before) + 1
    assert traces.metadata["events"][-1]["op"] == "validate_traces"
    assert traces.metadata["events"][-1]["summary"]["ok"] is False

    pd.testing.assert_frame_equal(traces.data, data_with_error)
    assert not traces.data.equals(data_before)


def test_validate_traces_categorical_schema_raises_before_metadata_is_touched(
    valid_traces: TraceDataset,
    make_trace_field: Callable[..., FieldSpec],
    make_trace_schema: Callable[..., TraceSchema],
) -> None:
    """Verifica que dtype categorical en TraceSchema aborta antes de tocar metadata."""
    schema_bad_categorical = make_trace_schema(
        [
            make_trace_field("point_id", "string", required=True),
            make_trace_field("user_id", "categorical", required=True),
            make_trace_field("time_utc", "datetime", required=True),
            make_trace_field("latitude", "float", required=True),
            make_trace_field("longitude", "float", required=True),
        ],
        required=["point_id", "user_id", "time_utc", "latitude", "longitude"],
        timezone=valid_traces.schema.timezone,
        crs=valid_traces.schema.crs,
        version="bad-trace-schema",
    )

    traces = TraceDataset(
        data=valid_traces.data.copy(deep=True),
        schema=schema_bad_categorical,
        metadata={},
        provenance={},
    )
    data_before = traces.data.copy(deep=True)
    metadata_before = dict(traces.metadata)

    with pytest.raises(SchemaError) as exc_info:
        validate_traces(traces)

    exc = exc_info.value
    assert exc.code == "VAL.SCHEMA.CATEGORICAL_NOT_ALLOWED"
    assert exc.issue is not None
    assert exc.issue.code == "VAL.SCHEMA.CATEGORICAL_NOT_ALLOWED"
    assert exc.issues is not None
    assert exc.issues[-1].code == "VAL.SCHEMA.CATEGORICAL_NOT_ALLOWED"

    assert traces.metadata == metadata_before == {}
    pd.testing.assert_frame_equal(traces.data, data_before)