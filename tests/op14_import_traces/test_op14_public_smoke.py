from __future__ import annotations

import pandas as pd

from pylondrina.datasets import TraceDataset
from pylondrina.importing_traces import ImportTraceOptions, import_traces_from_dataframe
from pylondrina.reports import ImportReport


def test_import_traces_from_dataframe_smoke_happy_path(
    raw_points_df: pd.DataFrame,
    trace_schema_base,
    field_correspondence_base: dict[str, str],
    trace_core_columns: tuple[str, ...],
    assert_json_safe,
) -> None:
    """Verifica el camino feliz mínimo de OP-14 usando la API pública."""
    source_before = raw_points_df.copy(deep=True)

    traces, report = import_traces_from_dataframe(
        raw_points_df,
        trace_schema_base,
        source_name="pytest_smoke_raw_points",
        options=ImportTraceOptions(source_timezone="America/Santiago"),
        field_correspondence=field_correspondence_base,
        provenance={"stage": "pytest", "case": "op14_smoke"},
    )

    assert isinstance(traces, TraceDataset)
    assert isinstance(report, ImportReport)

    assert report.ok is True
    assert report.summary["rows_in"] == len(source_before)
    assert report.summary["rows_out"] == len(traces.data) == len(source_before)
    assert report.summary["n_fields_mapped"] == len(field_correspondence_base)
    assert report.summary["point_id_generated"] is True

    assert set(trace_core_columns).issubset(traces.data.columns)
    assert traces.data["point_id"].notna().all()
    assert traces.data["point_id"].is_unique

    assert traces.metadata["is_validated"] is False
    assert traces.metadata["point_id_generated"] is True
    assert traces.metadata["events"][-1]["op"] == "import_traces"

    assert report.field_correspondence == field_correspondence_base
    assert report.value_correspondence == {}

    assert_json_safe(traces.metadata, "traces.metadata")
    assert_json_safe(report.summary, "report.summary")
    assert_json_safe(report.parameters, "report.parameters")

    pd.testing.assert_frame_equal(raw_points_df, source_before)