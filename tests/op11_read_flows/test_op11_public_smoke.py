from __future__ import annotations

import pandas as pd
import pytest

from pylondrina.errors import ExportError
from pylondrina.io.flows import (
    ReadFlowsOptions,
    WriteFlowsOptions,
    read_flows,
    write_flows,
)


# -----------------------------------------------------------------------------
# Smoke tests públicos de OP-11 read_flows
# -----------------------------------------------------------------------------


def test_read_flows_happy_path_uses_golondrina_fallback_and_reconstructs_minimal_bundle(
    tmp_path,
    minimal_flowdataset_factory,
):
    """Verifica lectura feliz mínima usando fallback automático al sufijo `.golondrina`."""
    case_dir = tmp_path / "case_read_happy_from_sidecar"
    artifact_dir = case_dir / "artifact"
    true_artifact_dir = case_dir / "artifact.golondrina"

    flows = minimal_flowdataset_factory(
        validated=True,
        with_flow_to_trips=False,
    )

    write_report = write_flows(
        flows,
        artifact_dir,
        options=WriteFlowsOptions(
            mode="error_if_exists",
            normalize_artifact_dir=True,
            write_flow_to_trips=False,
        ),
    )

    loaded, read_report = read_flows(
        artifact_dir,
        options=ReadFlowsOptions(
            strict=False,
            keep_metadata=True,
            read_flow_to_trips=False,
        ),
    )

    assert write_report.ok is True
    assert read_report.ok is True
    assert true_artifact_dir.exists()

    assert read_report.summary["n_flows"] == len(flows.flows)
    assert read_report.summary["flow_to_trips_loaded"] is False
    assert read_report.summary["n_flow_to_trips"] is None

    assert "flows.feather" in read_report.summary["files_read"]
    assert "flows.metadata.json" in read_report.summary["files_read"]

    assert read_report.parameters["path"] == str(true_artifact_dir)
    assert read_report.parameters["strict"] is False
    assert read_report.parameters["keep_metadata"] is True
    assert read_report.parameters["read_flow_to_trips"] is False

    assert loaded.metadata["dataset_id"] == flows.metadata["dataset_id"]
    assert loaded.metadata["artifact_id"] == flows.metadata["artifact_id"]
    assert loaded.metadata["is_validated"] is False
    assert loaded.source_trips is None

    loaded_ops = [event["op"] for event in loaded.metadata["events"]]
    assert "write_flows" in loaded_ops
    assert loaded_ops[-1] == "read_flows"

    pd.testing.assert_frame_equal(
        loaded.flows.reset_index(drop=True),
        flows.flows.reset_index(drop=True),
        check_dtype=False,
        check_categorical=False,
    )


def test_read_flows_loads_existing_flow_to_trips_auxiliary(
    tmp_path,
    minimal_flowdataset_factory,
):
    """Verifica lectura pública con auxiliar `flow_to_trips` efectivamente persistido."""
    case_dir = tmp_path / "case_with_aux_present"
    artifact_dir = case_dir / "artifact"

    flows = minimal_flowdataset_factory(
        validated=True,
        with_flow_to_trips=True,
    )

    write_report = write_flows(
        flows,
        artifact_dir,
        options=WriteFlowsOptions(
            mode="error_if_exists",
            normalize_artifact_dir=False,
            write_flow_to_trips=True,
        ),
    )

    loaded, read_report = read_flows(
        artifact_dir,
        options=ReadFlowsOptions(
            strict=False,
            keep_metadata=True,
            read_flow_to_trips=True,
        ),
    )

    assert write_report.ok is True
    assert read_report.ok is True

    assert (artifact_dir / "flow_to_trips.feather").exists()
    assert "flow_to_trips.feather" in write_report.summary["files_written"]

    assert read_report.summary["flow_to_trips_loaded"] is True
    assert read_report.summary["n_flow_to_trips"] == len(flows.flow_to_trips)
    assert "flow_to_trips.feather" in read_report.summary["files_read"]

    assert loaded.flow_to_trips is not None

    pd.testing.assert_frame_equal(
        loaded.flow_to_trips.reset_index(drop=True),
        flows.flow_to_trips.reset_index(drop=True),
        check_dtype=False,
        check_categorical=False,
    )


def test_read_flows_degrades_when_requested_flow_to_trips_file_is_missing(
    tmp_path,
    minimal_flowdataset_factory,
    issue_codes,
):
    """Verifica degradación recuperable si el auxiliar solicitado fue removido del bundle."""
    case_dir = tmp_path / "case_read_degraded_missing_aux"
    artifact_dir = case_dir / "artifact"

    flows = minimal_flowdataset_factory(
        validated=True,
        with_flow_to_trips=True,
    )

    write_flows(
        flows,
        artifact_dir,
        options=WriteFlowsOptions(
            mode="error_if_exists",
            normalize_artifact_dir=False,
            write_flow_to_trips=True,
        ),
    )

    (artifact_dir / "flow_to_trips.feather").unlink()

    loaded, report = read_flows(
        artifact_dir,
        options=ReadFlowsOptions(
            strict=False,
            keep_metadata=True,
            read_flow_to_trips=True,
        ),
    )

    codes = issue_codes(report)

    assert report.ok is True
    assert "READ_FLOWS.FLOW_TO_TRIPS.REQUESTED_BUT_MISSING" in codes

    assert loaded.flow_to_trips is None
    assert report.summary["flow_to_trips_loaded"] is False
    assert report.summary["n_flow_to_trips"] is None


def test_read_flows_with_keep_metadata_false_preserves_persisted_metadata_without_appending_read_event(
    tmp_path,
    minimal_flowdataset_factory,
):
    """Verifica lectura sin append de evento `read_flows` cuando `keep_metadata=False`."""
    case_dir = tmp_path / "case_read_keep_metadata_false"
    artifact_dir = case_dir / "artifact"

    flows = minimal_flowdataset_factory(
        validated=True,
        with_flow_to_trips=False,
    )

    write_flows(
        flows,
        artifact_dir,
        options=WriteFlowsOptions(
            mode="error_if_exists",
            normalize_artifact_dir=False,
            write_flow_to_trips=False,
        ),
    )

    loaded, report = read_flows(
        artifact_dir,
        options=ReadFlowsOptions(
            strict=False,
            keep_metadata=False,
            read_flow_to_trips=False,
        ),
    )

    assert report.ok is True

    assert loaded.metadata["dataset_id"] == flows.metadata["dataset_id"]
    assert loaded.metadata["artifact_id"] == flows.metadata["artifact_id"]
    assert loaded.metadata["is_validated"] is False
    assert loaded.provenance == flows.provenance

    loaded_ops = [event["op"] for event in loaded.metadata["events"]]
    assert "read_flows" not in loaded_ops
    assert loaded_ops[-1] == "write_flows"


def test_read_flows_raises_when_formal_sidecar_is_missing(
    tmp_path,
    minimal_flows_df,
):
    """Verifica fatalidad pública cuando el bundle no contiene `flows.metadata.json`."""
    case_dir = tmp_path / "case_read_fatal_missing_sidecar"
    artifact_dir = case_dir / "artifact"
    artifact_dir.mkdir(parents=True, exist_ok=True)

    minimal_flows_df.to_parquet(
        artifact_dir / "flows.parquet",
        index=False,
        compression="snappy",
        engine="pyarrow",
    )

    with pytest.raises(ExportError) as excinfo:
        read_flows(
            artifact_dir,
            options=ReadFlowsOptions(
                strict=False,
                keep_metadata=True,
                read_flow_to_trips=False,
            ),
        )

    assert excinfo.value.code == "READ_FLOWS.LAYOUT.MISSING_SIDECAR"