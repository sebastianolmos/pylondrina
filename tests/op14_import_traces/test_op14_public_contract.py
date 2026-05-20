from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pandas as pd

from pylondrina.datasets import TraceDataset
from pylondrina.importing_traces import ImportTraceOptions, import_traces_from_dataframe
from pylondrina.reports import ImportReport


def _assert_common_successful_import_contract(
    *,
    traces: TraceDataset,
    report: ImportReport,
    source_df: pd.DataFrame,
    field_correspondence_base: dict[str, str],
    trace_core_columns: tuple[str, ...],
    assert_json_safe: Callable[[Any, str], None],
) -> None:
    """Verifica invariantes comunes del contrato público de un import exitoso de OP-14."""
    assert isinstance(traces, TraceDataset)
    assert isinstance(report, ImportReport)

    assert report.ok is True
    assert report.summary["rows_in"] == len(source_df)
    assert report.summary["rows_out"] == len(traces.data) == len(source_df)
    assert report.summary["n_fields_mapped"] == len(field_correspondence_base)
    assert report.summary["point_id_generated"] is True

    assert report.field_correspondence == field_correspondence_base
    assert report.value_correspondence == {}

    assert traces.data.columns[: len(trace_core_columns)].tolist() == list(trace_core_columns)
    assert traces.data["point_id"].notna().all()
    assert traces.data["point_id"].is_unique

    assert traces.metadata["is_validated"] is False
    assert traces.metadata["point_id_generated"] is True

    event = traces.metadata["events"][-1]
    assert event["op"] == "import_traces"
    assert event["summary"] == report.summary
    assert event["parameters"] == report.parameters

    assert_json_safe(traces.metadata, "traces.metadata")
    assert_json_safe(report.summary, "report.summary")
    assert_json_safe(report.parameters, "report.parameters")
    assert_json_safe(event, "import_traces event")


def test_import_traces_preserves_reachable_extras_and_records_public_contract(
    raw_points_df_large: pd.DataFrame,
    trace_schema_base,
    field_correspondence_base: dict[str, str],
    trace_core_columns: tuple[str, ...],
    assert_issue_present: Callable[[list[Any], str], None],
    assert_json_safe: Callable[[Any, str], None],
) -> None:
    """Verifica el contrato público del camino principal de OP-14 con extras preservados."""
    source_before = raw_points_df_large.copy(deep=True)

    traces, report = import_traces_from_dataframe(
        raw_points_df_large,
        trace_schema_base,
        source_name="pytest_contract_raw_points_large",
        options=ImportTraceOptions(source_timezone="America/Santiago"),
        field_correspondence=field_correspondence_base,
        provenance={"stage": "pytest", "case": "op14_public_contract_large"},
    )

    _assert_common_successful_import_contract(
        traces=traces,
        report=report,
        source_df=source_before,
        field_correspondence_base=field_correspondence_base,
        trace_core_columns=trace_core_columns,
        assert_json_safe=assert_json_safe,
    )

    assert report.parameters["source_name"] == "pytest_contract_raw_points_large"
    assert report.parameters["source_timezone"] == "America/Santiago"
    assert report.parameters["keep_extra_fields"] is True
    assert report.parameters["selected_fields"] is None
    assert report.parameters["has_field_correspondence"] is True

    assert traces.metadata["source"]["name"] == "pytest_contract_raw_points_large"
    assert traces.metadata["field_correspondence_applied"] == field_correspondence_base
    assert traces.metadata["temporal"]["source_timezone_used"] == "America/Santiago"
    assert traces.metadata["temporal"]["normalized_to_utc"] is True

    assert_issue_present(report.issues, "IMP.CORE.POINT_ID_GENERATED")

    for canonical_field, source_field in field_correspondence_base.items():
        assert canonical_field in traces.data.columns
        assert source_field not in traces.data.columns

    preserved_extra_fields = {"device_vendor", "poi_name", "sample_weight"}
    assert preserved_extra_fields.issubset(set(traces.data.columns))

    for field in preserved_extra_fields:
        pd.testing.assert_series_equal(
            traces.data[field].reset_index(drop=True),
            source_before[field].reset_index(drop=True),
            check_names=False,
        )

    pd.testing.assert_frame_equal(raw_points_df_large, source_before)


def test_import_traces_with_empty_selected_fields_keeps_only_canonical_core(
    raw_points_df: pd.DataFrame,
    trace_schema_base,
    field_correspondence_base: dict[str, str],
    trace_core_columns: tuple[str, ...],
    assert_issue_present: Callable[[list[Any], str], None],
    assert_json_safe: Callable[[Any, str], None],
) -> None:
    """Verifica que selected_fields vacío conserve solo el núcleo canónico de traces."""
    source_before = raw_points_df.copy(deep=True)

    traces, report = import_traces_from_dataframe(
        raw_points_df,
        trace_schema_base,
        options=ImportTraceOptions(
            selected_fields=[],
            keep_extra_fields=True,
            source_timezone="UTC",
        ),
        field_correspondence=field_correspondence_base,
    )

    _assert_common_successful_import_contract(
        traces=traces,
        report=report,
        source_df=source_before,
        field_correspondence_base=field_correspondence_base,
        trace_core_columns=trace_core_columns,
        assert_json_safe=assert_json_safe,
    )

    assert traces.data.columns.tolist() == list(trace_core_columns)

    assert report.parameters["selected_fields"] == []
    assert report.parameters["keep_extra_fields"] is True
    assert report.parameters["source_timezone"] == "UTC"

    assert traces.metadata["temporal"]["source_timezone_used"] == "UTC"
    assert traces.metadata["temporal"]["normalized_to_utc"] is True

    assert_issue_present(report.issues, "IMP.OPTIONS.EMPTY_SELECTED_FIELDS")
    assert_issue_present(report.issues, "IMP.OPTIONS.EXTRA_FIELDS_DROPPED")
    assert_issue_present(report.issues, "IMP.CORE.POINT_ID_GENERATED")

    source_non_core_fields = set(source_before.columns) - set(field_correspondence_base.values())
    assert source_non_core_fields.isdisjoint(set(traces.data.columns))

    pd.testing.assert_frame_equal(raw_points_df, source_before)


def test_import_traces_with_selected_fields_omits_unknowns_and_drops_unselected_extras(
    raw_points_df: pd.DataFrame,
    trace_schema_base,
    field_correspondence_base: dict[str, str],
    trace_core_columns: tuple[str, ...],
    assert_issue_present: Callable[[list[Any], str], None],
    assert_json_safe: Callable[[Any, str], None],
) -> None:
    """Verifica selección explícita, omisión recuperable de unknowns y descarte de extras."""
    source_before = raw_points_df.copy(deep=True)
    selected_fields = ["visit_code", "battery_pct", "missing_extra"]
    expected_selected_fields = {"visit_code", "battery_pct"}

    traces, report = import_traces_from_dataframe(
        raw_points_df,
        trace_schema_base,
        options=ImportTraceOptions(
            keep_extra_fields=False,
            selected_fields=selected_fields,
            source_timezone="UTC",
        ),
        field_correspondence=field_correspondence_base,
    )

    _assert_common_successful_import_contract(
        traces=traces,
        report=report,
        source_df=source_before,
        field_correspondence_base=field_correspondence_base,
        trace_core_columns=trace_core_columns,
        assert_json_safe=assert_json_safe,
    )

    assert traces.data.columns[: len(trace_core_columns)].tolist() == list(trace_core_columns)
    assert set(traces.data.columns) == set(trace_core_columns) | expected_selected_fields

    assert "missing_extra" not in traces.data.columns
    assert expected_selected_fields.issubset(set(traces.data.columns))

    for field in expected_selected_fields:
        pd.testing.assert_series_equal(
            traces.data[field].reset_index(drop=True),
            source_before[field].reset_index(drop=True),
            check_names=False,
        )

    assert report.parameters["selected_fields"] == selected_fields
    assert report.parameters["keep_extra_fields"] is False
    assert report.parameters["source_timezone"] == "UTC"

    assert traces.metadata["temporal"]["source_timezone_used"] == "UTC"
    assert traces.metadata["temporal"]["normalized_to_utc"] is True

    assert_issue_present(report.issues, "IMP.OPTIONS.SELECTED_FIELDS_UNKNOWN")
    assert_issue_present(report.issues, "IMP.OPTIONS.EXTRA_FIELDS_DROPPED")
    assert_issue_present(report.issues, "IMP.CORE.POINT_ID_GENERATED")

    dropped_extra_fields = {"speed_mps", "is_home", "device_vendor", "poi_name", "sample_weight"}
    assert dropped_extra_fields.isdisjoint(set(traces.data.columns))

    pd.testing.assert_frame_equal(raw_points_df, source_before)