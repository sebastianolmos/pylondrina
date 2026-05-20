from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pandas as pd

from pylondrina.datasets import TraceDataset
from pylondrina.reports import ConsistencyReport
from pylondrina.validation_traces import TraceValidationOptions, validate_traces


def test_validate_traces_smoke_happy_path_updates_metadata_without_mutating_data(
    valid_traces: TraceDataset,
    clone_tracedataset: Callable[[TraceDataset], TraceDataset],
    assert_json_safe: Callable[[Any, str], None],
) -> None:
    """Verifica el camino feliz mínimo de OP-15 y su evidencia observable."""
    traces = clone_tracedataset(valid_traces)
    data_before = traces.data.copy(deep=True)
    events_before = list(traces.metadata.get("events", []))

    report = validate_traces(traces, options=TraceValidationOptions())

    assert isinstance(report, ConsistencyReport)
    assert report.summary["ok"] is True
    assert report.summary["n_rows"] == len(data_before)
    assert report.summary["n_errors"] == 0
    assert report.summary["counts_by_level"]["error"] == 0
    assert report.summary["schema_version"] == traces.schema.version

    assert traces.metadata["is_validated"] is True
    assert len(traces.metadata["events"]) == len(events_before) + 1

    event = traces.metadata["events"][-1]
    assert event["op"] == "validate_traces"
    assert event["summary"] == report.summary
    assert event["parameters"] == {
        "strict": False,
        "sample_rows_per_issue": 5,
        "validate_required_fields": True,
        "validate_types_and_formats": True,
        "validate_constraints": True,
        "validate_monotonic_time_per_user": True,
    }
    assert event["issues_summary"]["counts"] == {
        "info": 0,
        "warning": 0,
        "error": 0,
    }
    assert event["issues_summary"]["top_codes"] == []

    assert_json_safe(report.summary, "validate_traces report.summary")
    assert_json_safe(event, "validate_traces event")
    assert_json_safe(traces.metadata, "traces.metadata")

    pd.testing.assert_frame_equal(traces.data, data_before)