from __future__ import annotations

import copy
from pathlib import Path

import pandas as pd

from pylondrina.io.trips import WriteTripsOptions, write_trips

from conftest import artifact_data_file_path, clone_tripdataset, load_sidecar


def test_write_trips_rich_validated_dataset_to_parquet_with_normalized_bundle(
    tmp_path: Path,
    rich_tripdataset_validated,
):
    """Verifica escritura Parquet de un TripDataset rico validado con bundle normalizado."""
    trips = clone_tripdataset(rich_tripdataset_validated)

    data_before = trips.data.copy(deep=True)
    metadata_before = copy.deepcopy(trips.metadata)
    provenance_before = copy.deepcopy(trips.provenance)
    schema_before = copy.deepcopy(trips.schema)
    schema_effective_before = copy.deepcopy(trips.schema_effective)

    base_path = tmp_path / "sample_bundle"
    artifact_dir = tmp_path / "sample_bundle.golondrina"

    write_report = write_trips(
        trips,
        base_path,
        options=WriteTripsOptions(
            mode="overwrite",
            require_validated=True,
            storage_format="parquet",
            parquet_compression="snappy",
            normalize_artifact_dir=True,
        ),
    )

    assert write_report.ok is True

    assert artifact_dir.exists()
    assert artifact_dir.is_dir()
    assert artifact_data_file_path(artifact_dir, "parquet").exists()
    assert (artifact_dir / "trips.metadata.json").exists()
    assert not artifact_data_file_path(artifact_dir, "feather").exists()

    assert write_report.summary["n_rows"] == len(data_before)
    assert Path(write_report.summary["path"]) == artifact_dir
    assert write_report.summary["storage_format"] == "parquet"
    assert write_report.summary["files_written"] == ["trips.parquet", "trips.metadata.json"]
    assert write_report.summary["dataset_id_status"] == "preserved"
    assert write_report.summary["dataset_id"] == metadata_before["dataset_id"]
    assert write_report.summary["artifact_id"] == trips.metadata["artifact_id"]

    assert Path(write_report.parameters["path"]) == artifact_dir
    assert write_report.parameters["mode"] == "overwrite"
    assert write_report.parameters["require_validated"] is True
    assert write_report.parameters["storage_format"] == "parquet"
    assert write_report.parameters["parquet_compression"] == "snappy"
    assert write_report.parameters["normalize_artifact_dir"] is True

    assert trips.metadata["dataset_id"] == metadata_before["dataset_id"]
    assert trips.metadata["artifact_id"] == write_report.summary["artifact_id"]
    assert trips.metadata["artifact_id"].startswith("art_")
    assert trips.metadata["is_validated"] is True
    assert len(trips.metadata["events"]) == len(metadata_before["events"]) + 1

    write_event = trips.metadata["events"][-1]
    assert write_event["op"] == "write_trips"
    assert write_event["parameters"] == write_report.parameters
    assert write_event["summary"] == write_report.summary

    pd.testing.assert_frame_equal(trips.data, data_before)

    sidecar = load_sidecar(artifact_dir)

    assert sidecar["dataset_type"] == "trips"
    assert sidecar["format"] == "golondrina"
    assert sidecar["layout_version"] == "1.1"

    assert sidecar["storage"]["format"] == "parquet"
    assert sidecar["storage"]["options"]["compression"] == "snappy"

    assert sidecar["files"]["data"] == "trips.parquet"
    assert sidecar["files"]["metadata"] == "trips.metadata.json"

    assert sidecar["dataset_id"] == trips.metadata["dataset_id"]
    assert sidecar["artifact_id"] == trips.metadata["artifact_id"]

    assert sidecar["metadata"]["dataset_id"] == trips.metadata["dataset_id"]
    assert sidecar["metadata"]["artifact_id"] == trips.metadata["artifact_id"]
    assert sidecar["metadata"]["is_validated"] is True
    assert sidecar["metadata"]["events"][-1] == write_event

    assert sidecar["provenance"] == provenance_before
    assert sidecar["schema"]["version"] == schema_before.version
    assert "fields" in sidecar["schema"]
    assert "schema_effective" in sidecar
    assert sidecar["schema_effective"] == schema_effective_before.to_dict()


def test_write_trips_rich_validated_dataset_to_feather_with_normalized_bundle(
    tmp_path: Path,
    rich_tripdataset_validated,
):
    """Verifica escritura Feather de un TripDataset rico validado con bundle normalizado."""
    trips = clone_tripdataset(rich_tripdataset_validated)

    data_before = trips.data.copy(deep=True)
    metadata_before = copy.deepcopy(trips.metadata)
    provenance_before = copy.deepcopy(trips.provenance)
    schema_before = copy.deepcopy(trips.schema)
    schema_effective_before = copy.deepcopy(trips.schema_effective)

    base_path = tmp_path / "sample_bundle"
    artifact_dir = tmp_path / "sample_bundle.golondrina"

    write_report = write_trips(
        trips,
        base_path,
        options=WriteTripsOptions(
            mode="overwrite",
            require_validated=True,
            storage_format="feather",
            feather_compression="lz4",
            normalize_artifact_dir=True,
        ),
    )

    assert write_report.ok is True

    assert artifact_dir.exists()
    assert artifact_dir.is_dir()
    assert artifact_data_file_path(artifact_dir, "feather").exists()
    assert (artifact_dir / "trips.metadata.json").exists()
    assert not artifact_data_file_path(artifact_dir, "parquet").exists()

    assert write_report.summary["n_rows"] == len(data_before)
    assert Path(write_report.summary["path"]) == artifact_dir
    assert write_report.summary["storage_format"] == "feather"
    assert write_report.summary["files_written"] == ["trips.feather", "trips.metadata.json"]
    assert write_report.summary["dataset_id_status"] == "preserved"
    assert write_report.summary["dataset_id"] == metadata_before["dataset_id"]
    assert write_report.summary["artifact_id"] == trips.metadata["artifact_id"]

    assert Path(write_report.parameters["path"]) == artifact_dir
    assert write_report.parameters["mode"] == "overwrite"
    assert write_report.parameters["require_validated"] is True
    assert write_report.parameters["storage_format"] == "feather"
    assert write_report.parameters["feather_compression"] == "lz4"
    assert write_report.parameters["normalize_artifact_dir"] is True

    assert trips.metadata["dataset_id"] == metadata_before["dataset_id"]
    assert trips.metadata["artifact_id"] == write_report.summary["artifact_id"]
    assert trips.metadata["artifact_id"].startswith("art_")
    assert trips.metadata["is_validated"] is True
    assert len(trips.metadata["events"]) == len(metadata_before["events"]) + 1

    write_event = trips.metadata["events"][-1]
    assert write_event["op"] == "write_trips"
    assert write_event["parameters"] == write_report.parameters
    assert write_event["summary"] == write_report.summary

    pd.testing.assert_frame_equal(
        trips.data.reset_index(drop=True),
        data_before.reset_index(drop=True),
        check_dtype=False,
        check_categorical=False,
    )

    sidecar = load_sidecar(artifact_dir)

    assert sidecar["dataset_type"] == "trips"
    assert sidecar["format"] == "golondrina"
    assert sidecar["layout_version"] == "1.1"

    assert sidecar["storage"]["format"] == "feather"
    assert sidecar["storage"]["options"]["compression"] == "lz4"
    assert sidecar["storage"]["options"]["version"] == 2

    assert sidecar["files"]["data"] == "trips.feather"
    assert sidecar["files"]["metadata"] == "trips.metadata.json"

    assert sidecar["dataset_id"] == trips.metadata["dataset_id"]
    assert sidecar["artifact_id"] == trips.metadata["artifact_id"]

    assert sidecar["metadata"]["dataset_id"] == trips.metadata["dataset_id"]
    assert sidecar["metadata"]["artifact_id"] == trips.metadata["artifact_id"]
    assert sidecar["metadata"]["is_validated"] is True
    assert sidecar["metadata"]["events"][-1] == write_event

    assert sidecar["provenance"] == provenance_before
    assert sidecar["schema"]["version"] == schema_before.version
    assert "fields" in sidecar["schema"]
    assert "schema_effective" in sidecar
    assert sidecar["schema_effective"] == schema_effective_before.to_dict()


def test_write_trips_feather_sidecar_backend_and_data_file_are_coherent(
    tmp_path: Path,
    rich_tripdataset_validated,
):
    """Verifica coherencia integrada entre sidecar, backend Feather y archivo tabular."""
    trips = clone_tripdataset(rich_tripdataset_validated)

    base_path = tmp_path / "bundle"
    artifact_dir = tmp_path / "bundle.golondrina"

    write_report = write_trips(
        trips,
        base_path,
        options=WriteTripsOptions(
            mode="error_if_exists",
            require_validated=True,
            storage_format="feather",
            feather_compression="lz4",
            normalize_artifact_dir=True,
        ),
    )

    data_path = artifact_data_file_path(artifact_dir, "feather")
    sidecar = load_sidecar(artifact_dir)

    assert write_report.ok is True

    assert artifact_dir.exists()
    assert data_path.exists()
    assert data_path.name == "trips.feather"
    assert (artifact_dir / sidecar["files"]["metadata"]).exists()

    assert sidecar["storage"]["format"] == "feather"
    assert sidecar["storage"]["options"]["compression"] == "lz4"
    assert sidecar["storage"]["options"]["version"] == 2

    assert sidecar["files"]["data"] == data_path.name
    assert sidecar["files"]["metadata"] == "trips.metadata.json"

    assert write_report.summary["storage_format"] == "feather"
    assert write_report.summary["files_written"] == ["trips.feather", "trips.metadata.json"]
    assert write_report.summary["artifact_id"] == sidecar["artifact_id"]
    assert write_report.summary["dataset_id"] == sidecar["dataset_id"]

    assert trips.metadata["events"][-1]["op"] == "write_trips"
    assert trips.metadata["events"][-1]["parameters"] == write_report.parameters
    assert trips.metadata["events"][-1]["summary"] == write_report.summary


def test_write_trips_allows_rich_unvalidated_dataset_when_validation_requirement_is_disabled(
    tmp_path: Path,
    rich_tripdataset_unvalidated,
):
    """Verifica escritura integrada de dataset rico no validado con `require_validated=False`."""
    trips = clone_tripdataset(rich_tripdataset_unvalidated)

    data_before = trips.data.copy(deep=True)
    metadata_before = copy.deepcopy(trips.metadata)

    base_path = tmp_path / "unvalidated_allowed_bundle"
    artifact_dir = tmp_path / "unvalidated_allowed_bundle.golondrina"

    assert trips.metadata["is_validated"] is False

    write_report = write_trips(
        trips,
        base_path,
        options=WriteTripsOptions(
            mode="error_if_exists",
            require_validated=False,
            storage_format="parquet",
            parquet_compression="snappy",
            normalize_artifact_dir=True,
        ),
    )

    assert write_report.ok is True

    assert artifact_dir.exists()
    assert artifact_data_file_path(artifact_dir, "parquet").exists()
    assert (artifact_dir / "trips.metadata.json").exists()
    assert not artifact_data_file_path(artifact_dir, "feather").exists()

    assert write_report.parameters["require_validated"] is False
    assert write_report.parameters["storage_format"] == "parquet"
    assert Path(write_report.parameters["path"]) == artifact_dir

    assert write_report.summary["n_rows"] == len(data_before)
    assert write_report.summary["storage_format"] == "parquet"
    assert write_report.summary["files_written"] == ["trips.parquet", "trips.metadata.json"]
    assert write_report.summary["dataset_id"] == trips.metadata["dataset_id"]
    assert write_report.summary["artifact_id"] == trips.metadata["artifact_id"]

    assert trips.metadata["dataset_id"] == metadata_before["dataset_id"]
    assert trips.metadata["is_validated"] is False
    assert trips.metadata["events"][-1]["op"] == "write_trips"
    assert trips.metadata["events"][-1]["parameters"] == write_report.parameters
    assert trips.metadata["events"][-1]["summary"] == write_report.summary

    pd.testing.assert_frame_equal(
        trips.data.reset_index(drop=True),
        data_before.reset_index(drop=True),
        check_dtype=False,
        check_categorical=False,
    )

    sidecar = load_sidecar(artifact_dir)

    assert sidecar["dataset_type"] == "trips"
    assert sidecar["format"] == "golondrina"
    assert sidecar["storage"]["format"] == "parquet"
    assert sidecar["files"]["data"] == "trips.parquet"

    assert sidecar["dataset_id"] == trips.metadata["dataset_id"]
    assert sidecar["artifact_id"] == trips.metadata["artifact_id"]

    assert sidecar["metadata"]["is_validated"] is False
    assert sidecar["metadata"]["events"][-1]["op"] == "write_trips"
    assert sidecar["metadata"]["events"][-1]["parameters"] == write_report.parameters
    assert sidecar["metadata"]["events"][-1]["summary"] == write_report.summary