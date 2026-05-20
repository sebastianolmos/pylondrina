from __future__ import annotations

import copy
import json
from pathlib import Path

import pandas as pd

from pylondrina.datasets import FlowDataset
from pylondrina.io.flows import (
    WriteFlowsOptions,
    write_flows,
)


def _read_json(path: Path) -> dict:
    """Lee un archivo JSON y retorna su contenido como diccionario."""
    return json.loads(path.read_text(encoding="utf-8"))


def test_write_flows_happy_path_uses_default_feather_backend(
    tmp_path: Path,
    flowdataset_minimal: FlowDataset,
) -> None:
    """Verifica el camino público mínimo de write_flows con Feather por defecto."""
    artifact_dir = tmp_path / "artifact_write_happy"
    expected_bundle_dir = tmp_path / "artifact_write_happy.golondrina"

    flows = flowdataset_minimal
    flows_before = flows.flows.copy(deep=True)
    metadata_before = copy.deepcopy(flows.metadata)

    report = write_flows(
        flows,
        artifact_dir,
        options=WriteFlowsOptions(
            mode="error_if_exists",
            normalize_artifact_dir=True,
            write_flow_to_trips=False,
        ),
    )

    assert report.ok is True

    # Bundle mínimo formal.
    assert expected_bundle_dir.exists()
    assert (expected_bundle_dir / "flows.feather").exists()
    assert (expected_bundle_dir / "flows.metadata.json").exists()
    assert not (expected_bundle_dir / "flow_to_trips.feather").exists()

    # Summary mínimo de la operación.
    assert report.summary["n_flows"] == len(flows.flows)
    assert report.summary["n_flow_to_trips"] is None
    assert report.summary["dataset_id"] == flows.metadata["dataset_id"]
    assert report.summary["artifact_id"] == flows.metadata["artifact_id"]
    assert set(report.summary["files_written"]) == {
        "flows.feather",
        "flows.metadata.json",
    }

    # Parámetros efectivos.
    assert report.parameters["path"] == str(expected_bundle_dir)
    assert report.parameters["storage_format"] == "feather"
    assert report.parameters["mode"] == "error_if_exists"
    assert report.parameters["normalize_artifact_dir"] is True
    assert report.parameters["write_flow_to_trips"] is False

    # Metadata viva alineada tras commit exitoso.
    assert flows.metadata["dataset_id"] == metadata_before["dataset_id"]
    assert "artifact_id" in flows.metadata
    assert flows.metadata["is_validated"] is True
    assert flows.metadata["events"][-1]["op"] == "write_flows"

    # La tabla principal no debe mutarse.
    pd.testing.assert_frame_equal(
        flows.flows,
        flows_before,
    )

    # Sidecar mínimo coherente con el backend por defecto.
    sidecar = _read_json(expected_bundle_dir / "flows.metadata.json")

    assert sidecar["dataset_type"] == "flows"
    assert sidecar["format"] == "golondrina"
    assert sidecar["layout_version"] == "1.1"
    assert sidecar["storage"]["format"] == "feather"
    assert sidecar["files"]["data"] == "flows.feather"
    assert sidecar["files"]["metadata"] == "flows.metadata.json"
    assert sidecar["files"]["flow_to_trips"] is None


def test_write_flows_persists_flow_to_trips_when_requested_and_available(
    tmp_path: Path,
    flowdataset_with_aux: FlowDataset,
) -> None:
    """Verifica la persistencia pública del auxiliar flow_to_trips con backend Feather."""
    artifact_dir = tmp_path / "artifact_with_aux"

    flows = flowdataset_with_aux

    report = write_flows(
        flows,
        artifact_dir,
        options=WriteFlowsOptions(
            mode="error_if_exists",
            normalize_artifact_dir=False,
            write_flow_to_trips=True,
        ),
    )

    assert report.ok is True

    # Layout físico esperado.
    assert artifact_dir.exists()
    assert (artifact_dir / "flows.feather").exists()
    assert (artifact_dir / "flows.metadata.json").exists()
    assert (artifact_dir / "flow_to_trips.feather").exists()

    # Summary de persistencia con auxiliar.
    assert report.summary["n_flows"] == len(flows.flows)
    assert report.summary["n_flow_to_trips"] == len(flows.flow_to_trips)
    assert set(report.summary["files_written"]) == {
        "flows.feather",
        "flows.metadata.json",
        "flow_to_trips.feather",
    }

    # Parámetros efectivos.
    assert report.parameters["storage_format"] == "feather"
    assert report.parameters["write_flow_to_trips"] is True
    assert report.parameters["normalize_artifact_dir"] is False

    # Sidecar mínimo coherente con auxiliar persistido.
    sidecar = _read_json(artifact_dir / "flows.metadata.json")

    assert sidecar["storage"]["format"] == "feather"
    assert sidecar["files"]["data"] == "flows.feather"
    assert sidecar["files"]["metadata"] == "flows.metadata.json"
    assert sidecar["files"]["flow_to_trips"] == "flow_to_trips.feather"

    assert sidecar["tables"]["flow_to_trips"] is not None
    assert sidecar["tables"]["flow_to_trips"]["n_rows"] == len(
        flows.flow_to_trips
    )

    # Metadata final alineada con el reporte.
    assert flows.metadata["events"][-1]["op"] == "write_flows"
    assert flows.metadata["artifact_id"] == report.summary["artifact_id"]