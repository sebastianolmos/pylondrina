from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from pylondrina.errors import ExportError, ValidationError
from pylondrina.io.trips import WriteTripsOptions, write_trips

from conftest import assert_issue_absent, assert_issue_present, load_sidecar


def test_write_trips_parquet_happy_path_with_golondrina_suffix(
    tmp_path: Path,
    trip_dataset_validated,
):
    """Verifica escritura pública exitosa en Parquet con normalización `.golondrina`."""
    artifact_base_path = tmp_path / "artifact_write_parquet_happy"
    artifact_dir = tmp_path / "artifact_write_parquet_happy.golondrina"

    trips = trip_dataset_validated
    data_before = trips.data.copy(deep=True)
    metadata_before = trips.metadata.copy()

    report = write_trips(
        trips,
        artifact_base_path,
        options=WriteTripsOptions(
            mode="error_if_exists",
            require_validated=True,
            storage_format="parquet",
            parquet_compression="snappy",
            normalize_artifact_dir=True,
        ),
    )

    assert report.ok is True
    assert report.issues == []

    assert artifact_dir.exists()
    assert artifact_dir.is_dir()
    assert (artifact_dir / "trips.parquet").exists()
    assert (artifact_dir / "trips.metadata.json").exists()
    assert not (artifact_dir / "trips.feather").exists()

    assert report.summary["n_rows"] == len(data_before)
    assert Path(report.summary["path"]) == artifact_dir
    assert report.summary["storage_format"] == "parquet"
    assert report.summary["files_written"] == ["trips.parquet", "trips.metadata.json"]
    assert report.summary["dataset_id_status"] == "preserved"
    assert report.summary["dataset_id"] == metadata_before["dataset_id"]
    assert report.summary["artifact_id"] == trips.metadata["artifact_id"]

    assert Path(report.parameters["path"]) == artifact_dir
    assert report.parameters["mode"] == "error_if_exists"
    assert report.parameters["require_validated"] is True
    assert report.parameters["storage_format"] == "parquet"
    assert report.parameters["parquet_compression"] == "snappy"
    assert report.parameters["normalize_artifact_dir"] is True

    assert trips.metadata["dataset_id"] == metadata_before["dataset_id"]
    assert trips.metadata["artifact_id"] == report.summary["artifact_id"]
    assert trips.metadata["artifact_id"].startswith("art_")
    assert trips.metadata["is_validated"] is True
    assert len(trips.metadata["events"]) == len(metadata_before["events"]) + 1
    assert trips.metadata["events"][-1]["op"] == "write_trips"
    assert trips.metadata["events"][-1]["parameters"] == report.parameters
    assert trips.metadata["events"][-1]["summary"] == report.summary

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

    assert "schema" in sidecar
    assert "schema_effective" in sidecar
    assert "provenance" in sidecar
    assert "metadata" in sidecar

    assert sidecar["metadata"]["dataset_id"] == trips.metadata["dataset_id"]
    assert sidecar["metadata"]["artifact_id"] == trips.metadata["artifact_id"]
    assert sidecar["metadata"]["is_validated"] is True
    assert sidecar["metadata"]["events"][-1]["op"] == "write_trips"
    assert sidecar["metadata"]["events"][-1]["parameters"] == report.parameters
    assert sidecar["metadata"]["events"][-1]["summary"] == report.summary


def test_write_trips_feather_happy_path_without_suffix_normalization(
    tmp_path: Path,
    trip_dataset_validated,
):
    """Verifica escritura pública exitosa en Feather sin normalizar el directorio."""
    artifact_dir = tmp_path / "artifact_write_feather_happy"

    trips = trip_dataset_validated
    data_before = trips.data.copy(deep=True)
    metadata_before = trips.metadata.copy()

    report = write_trips(
        trips,
        artifact_dir,
        options=WriteTripsOptions(
            mode="error_if_exists",
            require_validated=True,
            storage_format="feather",
            feather_compression="lz4",
            normalize_artifact_dir=False,
        ),
    )

    assert report.ok is True
    assert report.issues == []

    assert artifact_dir.exists()
    assert artifact_dir.is_dir()
    assert (artifact_dir / "trips.feather").exists()
    assert (artifact_dir / "trips.metadata.json").exists()
    assert not (artifact_dir / "trips.parquet").exists()

    assert report.summary["n_rows"] == len(data_before)
    assert Path(report.summary["path"]) == artifact_dir
    assert report.summary["storage_format"] == "feather"
    assert report.summary["files_written"] == ["trips.feather", "trips.metadata.json"]
    assert report.summary["dataset_id_status"] == "preserved"
    assert report.summary["dataset_id"] == metadata_before["dataset_id"]
    assert report.summary["artifact_id"] == trips.metadata["artifact_id"]

    assert Path(report.parameters["path"]) == artifact_dir
    assert report.parameters["mode"] == "error_if_exists"
    assert report.parameters["require_validated"] is True
    assert report.parameters["storage_format"] == "feather"
    assert report.parameters["feather_compression"] == "lz4"
    assert report.parameters["normalize_artifact_dir"] is False

    assert trips.metadata["dataset_id"] == metadata_before["dataset_id"]
    assert trips.metadata["artifact_id"] == report.summary["artifact_id"]
    assert trips.metadata["artifact_id"].startswith("art_")
    assert trips.metadata["is_validated"] is True
    assert len(trips.metadata["events"]) == len(metadata_before["events"]) + 1
    assert trips.metadata["events"][-1]["op"] == "write_trips"

    pd.testing.assert_frame_equal(trips.data, data_before)

    sidecar = load_sidecar(artifact_dir)

    assert sidecar["storage"]["format"] == "feather"
    assert sidecar["storage"]["options"]["compression"] == "lz4"
    assert sidecar["storage"]["options"]["version"] == 2

    assert sidecar["files"]["data"] == "trips.feather"
    assert sidecar["files"]["metadata"] == "trips.metadata.json"

    assert sidecar["dataset_id"] == trips.metadata["dataset_id"]
    assert sidecar["artifact_id"] == trips.metadata["artifact_id"]
    assert sidecar["metadata"]["events"][-1]["op"] == "write_trips"


def test_write_trips_creates_missing_dataset_id(
    tmp_path: Path,
    trip_dataset_without_dataset_id,
):
    """Verifica que `write_trips` cree `dataset_id` cuando falta en metadata."""
    artifact_dir = tmp_path / "artifact_dataset_id_created"

    trips = trip_dataset_without_dataset_id
    data_before = trips.data.copy(deep=True)

    assert "dataset_id" not in trips.metadata

    report = write_trips(
        trips,
        artifact_dir,
        options=WriteTripsOptions(
            mode="error_if_exists",
            require_validated=True,
            storage_format="parquet",
            parquet_compression="snappy",
            normalize_artifact_dir=False,
        ),
    )

    assert report.ok is True
    assert_issue_present(report.issues, "WRT.METADATA.DATASET_ID_CREATED")

    assert artifact_dir.exists()
    assert (artifact_dir / "trips.parquet").exists()
    assert (artifact_dir / "trips.metadata.json").exists()

    assert isinstance(trips.metadata["dataset_id"], str)
    assert trips.metadata["dataset_id"].startswith("dset_")
    assert isinstance(trips.metadata["artifact_id"], str)
    assert trips.metadata["artifact_id"].startswith("art_")

    assert report.summary["dataset_id"] == trips.metadata["dataset_id"]
    assert report.summary["artifact_id"] == trips.metadata["artifact_id"]
    assert report.summary["dataset_id_status"] == "created"
    assert report.summary["storage_format"] == "parquet"

    pd.testing.assert_frame_equal(trips.data, data_before)

    sidecar = load_sidecar(artifact_dir)

    assert sidecar["dataset_id"] == trips.metadata["dataset_id"]
    assert sidecar["artifact_id"] == trips.metadata["artifact_id"]
    assert sidecar["metadata"]["dataset_id"] == trips.metadata["dataset_id"]
    assert sidecar["metadata"]["artifact_id"] == trips.metadata["artifact_id"]
    assert sidecar["metadata"]["events"][-1]["op"] == "write_trips"


def test_write_trips_overwrite_replaces_existing_artifact(
    tmp_path: Path,
    trip_dataset_validated,
):
    """Verifica que `mode='overwrite'` reemplace el artefacto y regenere `artifact_id`."""
    artifact_dir = tmp_path / "artifact_overwrite"

    trips = trip_dataset_validated
    dataset_id_before = trips.metadata["dataset_id"]

    first_report = write_trips(
        trips,
        artifact_dir,
        options=WriteTripsOptions(
            mode="error_if_exists",
            require_validated=True,
            storage_format="parquet",
            parquet_compression="snappy",
            normalize_artifact_dir=False,
        ),
    )

    first_artifact_id = trips.metadata["artifact_id"]

    assert first_report.ok is True
    assert artifact_dir.exists()
    assert (artifact_dir / "trips.parquet").exists()
    assert (artifact_dir / "trips.metadata.json").exists()

    sentinel = artifact_dir / "old_residual.txt"
    sentinel.write_text("old", encoding="utf-8")
    assert sentinel.exists()

    second_report = write_trips(
        trips,
        artifact_dir,
        options=WriteTripsOptions(
            mode="overwrite",
            require_validated=True,
            storage_format="feather",
            feather_compression="lz4",
            normalize_artifact_dir=False,
        ),
    )

    second_artifact_id = trips.metadata["artifact_id"]

    assert second_report.ok is True
    assert trips.metadata["dataset_id"] == dataset_id_before
    assert second_artifact_id != first_artifact_id

    assert artifact_dir.exists()
    assert not sentinel.exists()

    assert not (artifact_dir / "trips.parquet").exists()
    assert (artifact_dir / "trips.feather").exists()
    assert (artifact_dir / "trips.metadata.json").exists()

    assert second_report.summary["dataset_id"] == dataset_id_before
    assert second_report.summary["artifact_id"] == second_artifact_id
    assert second_report.summary["dataset_id_status"] == "preserved"
    assert second_report.summary["storage_format"] == "feather"
    assert second_report.summary["files_written"] == ["trips.feather", "trips.metadata.json"]

    sidecar = load_sidecar(artifact_dir)

    assert sidecar["storage"]["format"] == "feather"
    assert sidecar["files"]["data"] == "trips.feather"
    assert sidecar["dataset_id"] == dataset_id_before
    assert sidecar["artifact_id"] == second_artifact_id
    assert sidecar["metadata"]["events"][-1]["op"] == "write_trips"


def test_write_trips_raises_validation_error_when_dataset_is_not_validated(
    tmp_path: Path,
    trip_dataset_unvalidated,
):
    """Verifica que `require_validated=True` rechace datasets no validados."""
    artifact_dir = tmp_path / "artifact_not_validated"

    with pytest.raises(ValidationError) as excinfo:
        write_trips(
            trip_dataset_unvalidated,
            artifact_dir,
            options=WriteTripsOptions(
                mode="error_if_exists",
                require_validated=True,
                storage_format="parquet",
                parquet_compression="snappy",
                normalize_artifact_dir=False,
            ),
        )

    assert excinfo.value.code == "WRT.VALIDATION.REQUIRED_NOT_VALIDATED"
    assert not artifact_dir.exists()


def test_write_trips_raises_export_error_when_destination_exists(
    tmp_path: Path,
    trip_dataset_validated,
):
    """Verifica que `mode='error_if_exists'` no sobrescriba un destino existente."""
    artifact_dir = tmp_path / "artifact_existing"
    artifact_dir.mkdir(parents=True, exist_ok=True)

    sentinel = artifact_dir / "sentinel.txt"
    sentinel.write_text("do not delete", encoding="utf-8")

    with pytest.raises(ExportError) as excinfo:
        write_trips(
            trip_dataset_validated,
            artifact_dir,
            options=WriteTripsOptions(
                mode="error_if_exists",
                require_validated=True,
                storage_format="parquet",
                parquet_compression="snappy",
                normalize_artifact_dir=False,
            ),
        )

    assert excinfo.value.code == "WRT.DEST.ALREADY_EXISTS"
    assert artifact_dir.exists()
    assert sentinel.exists()
    assert sentinel.read_text(encoding="utf-8") == "do not delete"


def test_write_trips_allows_unvalidated_dataset_when_requirement_is_disabled(
    tmp_path: Path,
    trip_dataset_unvalidated,
):
    """Verifica escritura de dataset no validado cuando `require_validated=False`."""
    artifact_dir = tmp_path / "artifact_require_validated_false"

    trips = trip_dataset_unvalidated
    data_before = trips.data.copy(deep=True)

    report = write_trips(
        trips,
        artifact_dir,
        options=WriteTripsOptions(
            mode="error_if_exists",
            require_validated=False,
            storage_format="parquet",
            parquet_compression="snappy",
            normalize_artifact_dir=False,
        ),
    )

    assert report.ok is True
    assert_issue_absent(report.issues, "WRT.VALIDATION.REQUIRED_NOT_VALIDATED")

    assert artifact_dir.exists()
    assert (artifact_dir / "trips.parquet").exists()
    assert (artifact_dir / "trips.metadata.json").exists()
    assert not (artifact_dir / "trips.feather").exists()

    assert report.parameters["require_validated"] is False
    assert report.summary["storage_format"] == "parquet"
    assert report.summary["files_written"] == ["trips.parquet", "trips.metadata.json"]

    assert trips.metadata["is_validated"] is False
    assert trips.metadata["events"][-1]["op"] == "write_trips"

    pd.testing.assert_frame_equal(trips.data, data_before)

    sidecar = load_sidecar(artifact_dir)

    assert sidecar["metadata"]["is_validated"] is False
    assert sidecar["metadata"]["events"][-1]["op"] == "write_trips"
    assert sidecar["storage"]["format"] == "parquet"
    assert sidecar["files"]["data"] == "trips.parquet"