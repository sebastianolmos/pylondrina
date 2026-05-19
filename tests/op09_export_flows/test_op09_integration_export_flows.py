from __future__ import annotations

from pathlib import Path

import pytest

from pylondrina.errors import ExportError
from pylondrina.export.flows import (
    ExportFlowsOptions,
    FlowExportResult,
    export_flows,
)
from pylondrina.reports import OperationReport


def test_export_flows_happy_path_from_segmented_build_preserves_extras_artifacts_and_sidecar(
    make_flowdataset_segmented,
    make_export_case_dir,
    load_export_sidecar,
    read_export_flows_csv,
    read_export_locations_csv,
) -> None:
    """Verifica export feliz desde flows segmentados construidos por OP-08."""
    case_dir = make_export_case_dir(
        "test_02_export_happy_from_build"
    )

    flow_ds, build_report = make_flowdataset_segmented(
        h3_res=5,
        g_by=["user_gender"],
    )

    export_result, export_report = export_flows(
        flow_ds,
        output_root=str(case_dir),
        options=ExportFlowsOptions(
            format="flowmap_blue",
            mode="error_if_exists",
            folder_name="segmented_export_case",
            extra_flow_fields=[
                "user_gender",
                "window_start_utc",
            ],
        ),
    )

    assert build_report.ok is True
    assert isinstance(export_result, FlowExportResult)
    assert isinstance(export_report, OperationReport)
    assert export_report.ok is True

    export_dir = Path(export_result.export_dir)
    flows_csv_path = Path(export_result.artifacts["flows"])
    locations_csv_path = Path(export_result.artifacts["locations"])
    metadata_json_path = Path(export_result.artifacts["metadata"])

    assert export_dir.exists()
    assert flows_csv_path.exists()
    assert locations_csv_path.exists()
    assert metadata_json_path.exists()

    flows_csv = read_export_flows_csv(export_result)
    locations_csv = read_export_locations_csv(export_result)
    sidecar = load_export_sidecar(export_result)

    assert {
        "origin",
        "dest",
        "count",
        "user_gender",
        "window_start_utc",
    }.issubset(flows_csv.columns)

    assert {
        "id",
        "name",
        "lat",
        "lon",
    }.issubset(locations_csv.columns)

    assert export_report.summary["n_flows"] == len(flows_csv)
    assert export_report.summary["n_locations"] == len(locations_csv)
    assert export_report.summary["files_written"] == [
        "flows.csv",
        "locations.csv",
        "metadata.json",
    ]

    assert sidecar["artifact_type"] == "flow_export"
    assert sidecar["format"] == "flowmap_blue"
    assert sidecar["flow_dataset_ref"]["aggregation_spec"]["group_by"] == [
        "user_gender"
    ]
    assert sidecar["export"]["count_source"] == "flow_value"
    assert sidecar["export"]["parameters"]["extra_flow_fields"] == [
        "user_gender",
        "window_start_utc",
    ]

    assert flow_ds.metadata["events"][-1]["op"] == "export_flows"
    assert flow_ds.metadata["events"][-1]["summary"] == export_report.summary
    assert flow_ds.metadata["events"][-1]["parameters"] == export_report.parameters


def test_export_flows_raises_for_flowdataset_missing_required_flow_value(
    make_flowdataset_small,
    clone_flowdataset,
    make_export_case_dir,
) -> None:
    """Verifica error fatal cuando el FlowDataset no conserva el núcleo exportable."""
    case_dir = make_export_case_dir(
        "test_05_export_fatal_non_exportable"
    )

    flow_ds, build_report = make_flowdataset_small()
    assert build_report.ok is True

    bad_flow_ds = clone_flowdataset(flow_ds)
    bad_flow_ds.flows = bad_flow_ds.flows.drop(
        columns=["flow_value"]
    )

    with pytest.raises(ExportError) as excinfo:
        export_flows(
            bad_flow_ds,
            output_root=str(case_dir),
            options=ExportFlowsOptions(
                format="flowmap_blue",
                mode="error_if_exists",
                folder_name="bad_export_case",
                extra_flow_fields=None,
            ),
        )

    assert excinfo.value.code == "EXPORT_FLOWS.DATA.REQUIRED_FIELDS_MISSING"


def test_export_flows_appends_event_and_sidecar_references_prior_flowdataset_state(
    make_flowdataset_segmented,
    make_export_case_dir,
    load_export_sidecar,
    assert_json_safe,
) -> None:
    """Verifica evento export_flows, consistencia del reporte y sidecar trazable."""
    case_dir = make_export_case_dir(
        "test_06_metadata_events_summaries"
    )

    flow_ds, build_report = make_flowdataset_segmented(
        h3_res=8,
        g_by=["mode"],
        t_agg="week",
        t_basis="origin",
    )

    assert build_report.ok is True
    assert flow_ds.metadata["events"][-1]["op"] == "build_flows"

    export_result, export_report = export_flows(
        flow_ds,
        output_root=str(case_dir),
        options=ExportFlowsOptions(
            format="flowmap_blue",
            mode="error_if_exists",
            folder_name="metadata_chain_case",
            extra_flow_fields=[
                "mode",
                "window_start_utc",
            ],
        ),
    )

    assert export_report.ok is True

    assert flow_ds.metadata["events"][-2]["op"] == "build_flows"
    assert flow_ds.metadata["events"][-1]["op"] == "export_flows"
    assert flow_ds.metadata["events"][-1]["summary"] == export_report.summary
    assert flow_ds.metadata["events"][-1]["parameters"] == export_report.parameters
    assert "issues_summary" in flow_ds.metadata["events"][-1]

    sidecar = load_export_sidecar(export_result)

    assert sidecar["flow_dataset_ref"]["dataset_id"] == flow_ds.metadata["dataset_id"]
    assert sidecar["flow_dataset_ref"]["metadata"]["events"][-1]["op"] == "build_flows"

    assert_json_safe(sidecar, "flow export sidecar")


def test_export_flows_does_not_materialize_flow_to_trips_as_separate_artifact(
    make_flowdataset_for_export,
    make_export_case_dir,
) -> None:
    """Verifica que flow_to_trips no se exporte como archivo separado en v1.1."""
    case_dir = make_export_case_dir(
        "test_07_flow_to_trips_not_exported"
    )

    flow_ds = make_flowdataset_for_export(
        with_extra_fields=True,
    )

    assert flow_ds.flow_to_trips is not None

    export_result, export_report = export_flows(
        flow_ds,
        output_root=str(case_dir),
        options=ExportFlowsOptions(
            format="flowmap_blue",
            mode="error_if_exists",
            folder_name="flow_to_trips_case",
            extra_flow_fields=["mode"],
        ),
    )

    assert export_report.ok is True
    assert set(export_result.artifacts.keys()) == {
        "flows",
        "locations",
        "metadata",
    }

    assert not (
        Path(export_result.export_dir) / "flow_to_trips.csv"
    ).exists()