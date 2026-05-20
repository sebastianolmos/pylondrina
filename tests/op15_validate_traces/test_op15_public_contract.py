from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

import pandas as pd

from pylondrina.datasets import TraceDataset
from pylondrina.reports import Issue
from pylondrina.validation_traces import validate_traces


def _assert_common_returnable_validation_contract(
    *,
    traces: TraceDataset,
    report,
    data_before: pd.DataFrame,
    assert_json_safe: Callable[[Any, str], None],
) -> None:
    """Verifica invariantes comunes de una validación retornable de OP-15."""
    assert report.summary["n_rows"] == len(data_before)
    assert report.summary["schema_version"] == traces.schema.version

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

    assert_json_safe(report.summary, "validate_traces report.summary")
    assert_json_safe(event, "validate_traces event")
    assert_json_safe(traces.metadata, "traces.metadata")

    pd.testing.assert_frame_equal(traces.data, data_before)


def test_validate_traces_reports_missing_required_column_without_raising(
    valid_traces: TraceDataset,
    clone_tracedataset: Callable[[TraceDataset], TraceDataset],
    assert_issue_present: Callable[[Sequence[Issue], str], None],
    assert_json_safe: Callable[[Any, str], None],
) -> None:
    """Verifica que una columna required ausente produce error retornable en strict=False."""
    traces = clone_tracedataset(valid_traces)
    traces.data = traces.data.drop(columns=["user_id"])
    data_before = traces.data.copy(deep=True)

    report = validate_traces(traces)

    assert report.summary["ok"] is False
    assert report.summary["n_errors"] >= 1
    assert report.summary["counts_by_code"].get("VAL.REQUIRED.MISSING_COLUMN", 0) >= 1
    assert_issue_present(report.issues, "VAL.REQUIRED.MISSING_COLUMN")

    assert traces.metadata["is_validated"] is False

    _assert_common_returnable_validation_contract(
        traces=traces,
        report=report,
        data_before=data_before,
        assert_json_safe=assert_json_safe,
    )


def test_validate_traces_reports_mixed_type_and_constraint_errors(
    valid_traces: TraceDataset,
    clone_tracedataset: Callable[[TraceDataset], TraceDataset],
    assert_issue_present: Callable[[Sequence[Issue], str], None],
    assert_json_safe: Callable[[Any, str], None],
) -> None:
    """Verifica errores retornables de tipos/formatos y constraints en una corrida pública."""
    traces = clone_tracedataset(valid_traces)
    row_index = list(traces.data.index)

    traces.data["time_utc"] = traces.data["time_utc"].astype("object")
    traces.data["latitude"] = traces.data["latitude"].astype("object")
    traces.data["is_home"] = traces.data["is_home"].astype("object")

    traces.data.loc[row_index[1], "time_utc"] = "not-a-time"
    traces.data.loc[row_index[2], "latitude"] = "north"
    traces.data.loc[row_index[3], "visit_code"] = "BAD"
    traces.data.loc[row_index[4], "battery_pct"] = 150
    traces.data.loc[row_index[5], "is_home"] = "maybe"

    data_before = traces.data.copy(deep=True)

    report = validate_traces(traces)

    assert report.summary["ok"] is False
    assert report.summary["n_errors"] >= 1
    assert report.summary["counts_by_code"].get("VAL.TYPES.UNPARSEABLE_VALUE", 0) >= 1
    assert report.summary["counts_by_code"].get("VAL.CONSTRAINTS.VIOLATION", 0) >= 1
    assert_issue_present(report.issues, "VAL.TYPES.UNPARSEABLE_VALUE")
    assert_issue_present(report.issues, "VAL.CONSTRAINTS.VIOLATION")

    assert traces.metadata["is_validated"] is False

    _assert_common_returnable_validation_contract(
        traces=traces,
        report=report,
        data_before=data_before,
        assert_json_safe=assert_json_safe,
    )


def test_validate_traces_reports_nullable_and_unique_constraint_violations(
    valid_traces: TraceDataset,
    clone_tracedataset: Callable[[TraceDataset], TraceDataset],
    assert_issue_present: Callable[[Sequence[Issue], str], None],
    assert_json_safe: Callable[[Any, str], None],
) -> None:
    """Verifica violaciones retornables de nullable=False y unique=True."""
    traces = clone_tracedataset(valid_traces)
    row_index = list(traces.data.index)

    traces.data["is_home"] = traces.data["is_home"].astype("object")
    traces.data.loc[row_index[0], "is_home"] = None
    traces.data.loc[row_index[1], "point_id"] = traces.data.loc[row_index[0], "point_id"]

    data_before = traces.data.copy(deep=True)

    report = validate_traces(traces)

    assert report.summary["ok"] is False
    assert report.summary["n_errors"] >= 1
    assert report.summary["counts_by_code"].get("VAL.CONSTRAINTS.VIOLATION", 0) >= 2
    assert_issue_present(report.issues, "VAL.CONSTRAINTS.VIOLATION")

    assert traces.metadata["is_validated"] is False

    _assert_common_returnable_validation_contract(
        traces=traces,
        report=report,
        data_before=data_before,
        assert_json_safe=assert_json_safe,
    )


def test_validate_traces_reports_non_monotonic_time_as_warning_only(
    valid_traces: TraceDataset,
    clone_tracedataset: Callable[[TraceDataset], TraceDataset],
    assert_issue_present: Callable[[Sequence[Issue], str], None],
    assert_json_safe: Callable[[Any, str], None],
) -> None:
    """Verifica que la monotonicidad rota por usuario emite warning y mantiene ok=True."""
    traces = clone_tracedataset(valid_traces)

    user_id = traces.data["user_id"].iloc[0]
    user_index = traces.data.index[traces.data["user_id"] == user_id].tolist()
    assert len(user_index) >= 2

    traces.data.loc[user_index[-1], "time_utc"] = (
        traces.data.loc[user_index[0], "time_utc"] - pd.Timedelta(hours=1)
    )

    data_before = traces.data.copy(deep=True)

    report = validate_traces(traces)

    assert report.summary["ok"] is True
    assert report.summary["n_errors"] == 0
    assert report.summary["n_warnings"] >= 1
    assert report.summary["counts_by_code"].get("VAL.TEMPORAL.NON_MONOTONIC_TIME", 0) >= 1
    assert_issue_present(report.issues, "VAL.TEMPORAL.NON_MONOTONIC_TIME")

    assert traces.metadata["is_validated"] is True

    _assert_common_returnable_validation_contract(
        traces=traces,
        report=report,
        data_before=data_before,
        assert_json_safe=assert_json_safe,
    )