from __future__ import annotations

from pathlib import Path

import pandas as pd

from pylondrina.export.flows import (
    FlowExportResult,
    _append_export_event_or_warning,
    _build_flowmap_tables,
    _materialize_flowmap_export,
)
from pylondrina.reports import OperationReport


def test_build_flowmap_tables_maps_internal_flows_to_external_layout_and_locations(
    make_flowdataset_for_export,
) -> None:
    """Verifica mapping a origin/dest/count y construcción de locations desde H3."""
    flows = make_flowdataset_for_export(
        with_extra_fields=True,
    )

    flows_out_df, locations_df = _build_flowmap_tables(
        flows.flows,
        extra_flow_fields=["mode", "purpose"],
        count_source="flow_value",
    )

    assert list(flows_out_df.columns) == [
        "origin",
        "dest",
        "count",
        "mode",
        "purpose",
    ]

    assert len(flows_out_df) == len(flows.flows)

    pd.testing.assert_series_equal(
        flows_out_df["origin"].reset_index(drop=True),
        flows.flows["origin_h3_index"].astype(str).reset_index(drop=True),
        check_names=False,
    )
    pd.testing.assert_series_equal(
        flows_out_df["dest"].reset_index(drop=True),
        flows.flows["destination_h3_index"].astype(str).reset_index(drop=True),
        check_names=False,
    )
    pd.testing.assert_series_equal(
        flows_out_df["count"].reset_index(drop=True),
        pd.to_numeric(flows.flows["flow_value"], errors="coerce").reset_index(drop=True),
        check_names=False,
        check_dtype=False,
    )

    assert list(locations_df.columns) == ["id", "name", "lat", "lon"]

    expected_location_ids = set(flows.flows["origin_h3_index"]).union(
        set(flows.flows["destination_h3_index"])
    )

    assert set(locations_df["id"]) == expected_location_ids
    assert locations_df["lat"].notna().all()
    assert locations_df["lon"].notna().all()


def test_materialize_flowmap_export_writes_artifacts_sidecar_report_and_event_payload(
    make_flowdataset_for_export,
    make_export_case_dir,
    load_export_sidecar,
    assert_json_safe,
) -> None:
    """Verifica materialización completa de archivos, sidecar, reporte y evento."""
    flows = make_flowdataset_for_export(
        with_extra_fields=True,
    )

    case_dir = make_export_case_dir(
        "case_04_06_materialize_flowmap_export_happy"
    )
    export_dir = case_dir / "artifact_export"

    flows_out_df, locations_df = _build_flowmap_tables(
        flows.flows,
        extra_flow_fields=["mode", "purpose"],
        count_source="flow_value",
    )

    parameters = {
        "output_root": str(case_dir),
        "export_dir": str(export_dir),
        "format": "flowmap_blue",
        "mode": "error_if_exists",
        "folder_name": "artifact_export",
        "extra_flow_fields": ["mode", "purpose"],
    }

    result, report, event_dict = _materialize_flowmap_export(
        flows,
        str(export_dir),
        flows_out_df,
        locations_df,
        parameters,
        "flow_value",
    )

    assert isinstance(result, FlowExportResult)
    assert isinstance(report, OperationReport)

    assert Path(result.export_dir).exists()
    assert Path(result.artifacts["flows"]).exists()
    assert Path(result.artifacts["locations"]).exists()
    assert Path(result.artifacts["metadata"]).exists()

    assert report.ok is True
    assert report.summary["n_flows"] == len(flows_out_df)
    assert report.summary["n_locations"] == len(locations_df)
    assert report.summary["files_written"] == [
        "flows.csv",
        "locations.csv",
        "metadata.json",
    ]

    assert event_dict["op"] == "export_flows"
    assert event_dict["parameters"] == report.parameters
    assert event_dict["summary"] == report.summary
    assert "ts_utc" in event_dict
    assert "issues_summary" in event_dict

    sidecar = load_export_sidecar(result)

    assert sidecar["artifact_type"] == "flow_export"
    assert sidecar["format"] == "flowmap_blue"
    assert sidecar["files"]["flows"] == "flows.csv"
    assert sidecar["files"]["locations"] == "locations.csv"
    assert sidecar["files"]["metadata"] == "metadata.json"
    assert sidecar["flow_dataset_ref"]["dataset_id"] == "flows_case_001"
    assert sidecar["export"]["count_source"] == "flow_value"

    assert_json_safe(sidecar, "flow_export_sidecar")
    assert_json_safe(event_dict, "export_flows_event")


def test_append_export_event_or_warning_appends_event_to_flowdataset_metadata(
    make_flowdataset_for_export,
) -> None:
    """Verifica append normal del evento export_flows en metadata del FlowDataset."""
    flows = make_flowdataset_for_export()

    report = OperationReport(
        ok=True,
        issues=[],
        summary={"n_flows": len(flows.flows)},
        parameters={"format": "flowmap_blue"},
    )

    event_dict = {
        "op": "export_flows",
        "ts_utc": "2026-04-01T12:00:00Z",
        "parameters": {"format": "flowmap_blue"},
        "summary": {"n_flows": len(flows.flows)},
    }

    report_out = _append_export_event_or_warning(
        flows,
        event_dict,
        report,
    )

    assert report_out.ok is True
    assert report_out is report
    assert flows.metadata["events"][-1] == event_dict
    assert flows.metadata["events"][-1]["op"] == "export_flows"


def test_append_export_event_or_warning_keeps_report_ok_and_adds_warning_when_append_fails(
    make_flowdataset_for_export,
    assert_issue_present,
) -> None:
    """Verifica recovery con warning si falla el append del evento posterior al export."""
    class BrokenEventsDict(dict):
        def __setitem__(self, key, value):
            if key == "events":
                raise RuntimeError("broken events append")
            return super().__setitem__(key, value)

    flows = make_flowdataset_for_export()

    base_metadata = dict(flows.metadata)
    base_metadata["events"] = None
    flows.metadata = BrokenEventsDict(base_metadata)

    report = OperationReport(
        ok=True,
        issues=[],
        summary={"n_flows": len(flows.flows)},
        parameters={"format": "flowmap_blue"},
    )

    event_dict = {
        "op": "export_flows",
        "ts_utc": "2026-04-01T12:00:00Z",
        "parameters": {"format": "flowmap_blue"},
        "summary": {"n_flows": len(flows.flows)},
    }

    report_out = _append_export_event_or_warning(
        flows,
        event_dict,
        report,
    )

    assert report_out.ok is True
    assert report_out is report

    assert_issue_present(
        report_out.issues,
        "EXPORT_FLOWS.EVENT.APPEND_FAILED",
    )