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


def test_export_flows_happy_path_minimal_writes_basic_flowmap_artifacts(
    make_flowdataset_for_export,
    export_root: Path,
    load_export_sidecar,
    read_export_flows_csv,
    read_export_locations_csv,
) -> None:
    """Verifica export mínimo a flowmap_blue con artefactos y sidecar básicos."""
    flows = make_flowdataset_for_export(
        with_extra_fields=False,
    )

    output_root = export_root / "export_minimal_root"
    output_root.mkdir(parents=True, exist_ok=True)

    result, report = export_flows(
        flows,
        output_root=str(output_root),
        options=ExportFlowsOptions(
            format="flowmap_blue",
            mode="error_if_exists",
            folder_name="case_export_minimal",
            extra_flow_fields=None,
        ),
    )

    assert isinstance(result, FlowExportResult)
    assert isinstance(report, OperationReport)
    assert report.ok is True

    assert set(result.artifacts.keys()) == {
        "flows",
        "locations",
        "metadata",
    }

    assert Path(result.artifacts["flows"]).exists()
    assert Path(result.artifacts["locations"]).exists()
    assert Path(result.artifacts["metadata"]).exists()

    flows_csv = read_export_flows_csv(result)
    locations_csv = read_export_locations_csv(result)
    metadata_json = load_export_sidecar(result)

    assert list(flows_csv.columns) == [
        "origin",
        "dest",
        "count",
    ]

    assert {
        "id",
        "name",
        "lat",
        "lon",
    }.issubset(locations_csv.columns)

    assert metadata_json["export"]["count_source"] == "flow_value"


def test_export_flows_preserves_explicit_extra_flow_fields_in_csv_and_sidecar(
    make_flowdataset_for_export,
    export_root: Path,
    load_export_sidecar,
    read_export_flows_csv,
) -> None:
    """Verifica export con extras explícitas preservadas en flows.csv y metadata.json."""
    flows = make_flowdataset_for_export(
        with_extra_fields=True,
    )

    output_root = export_root / "export_with_extras_root"
    output_root.mkdir(parents=True, exist_ok=True)

    result, report = export_flows(
        flows,
        output_root=str(output_root),
        options=ExportFlowsOptions(
            format="flowmap_blue",
            mode="error_if_exists",
            folder_name="case_export_with_extras",
            extra_flow_fields=[
                "mode",
                "purpose",
                "window_start_utc",
            ],
        ),
    )

    assert report.ok is True

    flows_csv = read_export_flows_csv(result)
    metadata_json = load_export_sidecar(result)

    assert {
        "origin",
        "dest",
        "count",
        "mode",
        "purpose",
        "window_start_utc",
    }.issubset(flows_csv.columns)

    assert metadata_json["export"]["parameters"]["extra_flow_fields"] == [
        "mode",
        "purpose",
        "window_start_utc",
    ]


def test_export_flows_raises_when_target_directory_already_exists_and_mode_is_error_if_exists(
    make_flowdataset_for_export,
    export_root: Path,
) -> None:
    """Verifica error fatal por colisión de carpeta con mode='error_if_exists'."""
    flows = make_flowdataset_for_export(
        with_extra_fields=False,
    )

    output_root = export_root / "export_collision_root"
    output_root.mkdir(parents=True, exist_ok=True)

    export_flows(
        flows,
        output_root=str(output_root),
        options=ExportFlowsOptions(
            format="flowmap_blue",
            mode="error_if_exists",
            folder_name="case_collision",
        ),
    )

    with pytest.raises(ExportError) as excinfo:
        export_flows(
            flows,
            output_root=str(output_root),
            options=ExportFlowsOptions(
                format="flowmap_blue",
                mode="error_if_exists",
                folder_name="case_collision",
            ),
        )

    assert excinfo.value.code == "EXPORT_FLOWS.LAYOUT.EXPORT_DIR_EXISTS_ABORT"