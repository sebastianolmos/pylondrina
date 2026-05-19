from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from pylondrina.datasets import TripDataset
from pylondrina.errors import ExportError
from pylondrina.io.trips import ReadTripsOptions, read_trips
from pylondrina.schema import TripSchemaEffective


def test_read_trips_parquet_happy_path_uses_schema_snapshot(
    make_case_dir,
    write_valid_artifact_with_backend,
    assert_issue_present,
    assert_data_equivalent,
) -> None:
    """Verifica lectura happy path de un bundle Parquet usando el schema del sidecar."""
    case_dir = make_case_dir("case_01_read_from_snapshot_parquet")

    written_trips, artifact_dir, _, write_report = write_valid_artifact_with_backend(
        case_dir,
        artifact_name="artifact",
        storage_format="parquet",
    )

    loaded, read_report = read_trips(
        artifact_dir,
        options=ReadTripsOptions(
            schema=None,
            strict=False,
            keep_metadata=True,
        ),
    )

    assert write_report.ok is True
    assert read_report.ok is True

    assert isinstance(loaded, TripDataset)
    assert loaded.schema.version == written_trips.schema.version
    assert loaded.schema.to_dict() == written_trips.schema.to_dict()
    assert loaded.schema_effective.to_dict() == written_trips.schema_effective.to_dict()

    assert read_report.parameters["schema"]["source"] == "metadata"
    assert read_report.summary["schema_source"] == "metadata"
    assert read_report.summary["storage_format"] == "parquet"
    assert read_report.summary["n_rows"] == len(written_trips.data)
    assert read_report.summary["n_columns"] == len(written_trips.data.columns)

    # La lectura formal no equivale a certificación.
    assert loaded.metadata["is_validated"] is False
    assert_issue_present(read_report.issues, "READ.METADATA.VALIDATED_FORCED_FALSE")

    # Identidad lógica y de artefacto preservada.
    assert loaded.metadata["dataset_id"] == written_trips.metadata["dataset_id"]
    assert loaded.metadata["artifact_id"] == written_trips.metadata["artifact_id"]
    assert read_report.summary["dataset_id"] == written_trips.metadata["dataset_id"]
    assert read_report.summary["artifact_id"] == written_trips.metadata["artifact_id"]

    # Evento de lectura.
    assert loaded.metadata["events"][-1]["op"] == "read_trips"
    assert loaded.metadata["events"][-1]["summary"] == read_report.summary

    # Datos equivalentes.
    assert_data_equivalent(loaded.data, written_trips.data)


def test_read_trips_feather_happy_path_reports_feather_storage(
    make_case_dir,
    write_valid_artifact_with_backend,
    load_sidecar,
    assert_issue_present,
    assert_data_equivalent,
) -> None:
    """Verifica lectura happy path de un bundle Feather y su storage observable."""
    case_dir = make_case_dir("case_02_read_from_snapshot_feather")

    written_trips, artifact_dir, data_path, write_report = write_valid_artifact_with_backend(
        case_dir,
        artifact_name="artifact",
        storage_format="feather",
    )

    loaded, read_report = read_trips(
        artifact_dir,
        options=ReadTripsOptions(
            schema=None,
            strict=False,
            keep_metadata=True,
        ),
    )

    assert write_report.ok is True
    assert read_report.ok is True

    assert data_path.exists()
    assert data_path.name == "trips.feather"
    assert not (artifact_dir / "trips.parquet").exists()

    assert read_report.parameters["schema"]["source"] == "metadata"
    assert read_report.summary["schema_source"] == "metadata"
    assert read_report.summary["storage_format"] == "feather"

    assert loaded.schema.version == written_trips.schema.version
    assert loaded.schema_effective.to_dict() == written_trips.schema_effective.to_dict()

    assert loaded.metadata["is_validated"] is False
    assert_issue_present(read_report.issues, "READ.METADATA.VALIDATED_FORCED_FALSE")

    assert loaded.metadata["dataset_id"] == written_trips.metadata["dataset_id"]
    assert loaded.metadata["artifact_id"] == written_trips.metadata["artifact_id"]
    assert loaded.metadata["events"][-1]["op"] == "read_trips"

    sidecar = load_sidecar(artifact_dir)
    assert sidecar["storage"]["format"] == "feather"
    assert sidecar["files"]["data"] == "trips.feather"

    assert_data_equivalent(loaded.data, written_trips.data)


def test_read_trips_uses_explicit_schema_and_reports_mismatch(
    make_case_dir,
    trip_schema_minimal,
    write_valid_artifact_with_backend,
    assert_issue_present,
) -> None:
    """Verifica que options.schema tenga precedencia y reporte mismatch observable."""
    case_dir = make_case_dir("case_03_read_with_schema_option")

    _, artifact_dir, _, write_report = write_valid_artifact_with_backend(
        case_dir,
        artifact_name="artifact",
        storage_format="parquet",
    )

    schema_override = copy.deepcopy(trip_schema_minimal)
    schema_override.version = "1.1-override"

    loaded, read_report = read_trips(
        artifact_dir,
        options=ReadTripsOptions(
            schema=schema_override,
            strict=False,
            keep_metadata=True,
        ),
    )

    assert write_report.ok is True
    assert read_report.ok is True

    assert loaded.schema.version == schema_override.version
    assert read_report.parameters["schema"]["source"] == "options"
    assert read_report.parameters["schema"]["version"] == schema_override.version
    assert read_report.summary["schema_source"] == "options"
    assert read_report.summary["schema_mismatch"] is True

    assert_issue_present(read_report.issues, "READ.SCHEMA.MISMATCH")
    assert_issue_present(read_report.issues, "READ.METADATA.VALIDATED_FORCED_FALSE")

    assert loaded.metadata["is_validated"] is False
    assert loaded.metadata["events"][-1]["op"] == "read_trips"


def test_read_trips_keep_metadata_false_does_not_append_read_event(
    make_case_dir,
    write_valid_artifact_with_backend,
    load_sidecar,
    assert_issue_present,
) -> None:
    """Verifica que keep_metadata=False preserve eventos previos sin agregar read_trips."""
    case_dir = make_case_dir("case_04_read_keep_metadata_false")

    _, artifact_dir, _, write_report = write_valid_artifact_with_backend(
        case_dir,
        artifact_name="artifact",
        storage_format="parquet",
    )

    sidecar = load_sidecar(artifact_dir)
    events_before_read = copy.deepcopy(sidecar["metadata"]["events"])

    loaded, read_report = read_trips(
        artifact_dir,
        options=ReadTripsOptions(
            schema=None,
            strict=False,
            keep_metadata=False,
        ),
    )

    assert write_report.ok is True
    assert read_report.ok is True

    assert loaded.metadata["is_validated"] is False
    assert_issue_present(read_report.issues, "READ.METADATA.VALIDATED_FORCED_FALSE")

    # No debe agregarse evento read_trips.
    assert loaded.metadata["events"] == events_before_read
    assert all(event["op"] != "read_trips" for event in loaded.metadata["events"])

    # El reporte sí existe y conserva la política efectiva.
    assert read_report.parameters["keep_metadata"] is False
    assert read_report.summary["storage_format"] == "parquet"


def test_read_trips_falls_back_to_golondrina_suffix_when_base_path_is_missing(
    make_case_dir,
    write_valid_artifact_with_backend,
) -> None:
    """Verifica resolución automática del bundle con sufijo .golondrina."""
    case_dir = make_case_dir("case_05_read_auto_suffix")
    base_path = case_dir / "artifact_auto_suffix"
    expected_artifact_dir = case_dir / "artifact_auto_suffix.golondrina"

    written_trips, artifact_dir, _, write_report = write_valid_artifact_with_backend(
        case_dir,
        artifact_name="artifact_auto_suffix",
        storage_format="parquet",
    )

    assert write_report.ok is True
    assert artifact_dir == expected_artifact_dir
    assert expected_artifact_dir.exists()
    assert not base_path.exists()

    loaded, read_report = read_trips(
        base_path,
        options=ReadTripsOptions(
            schema=None,
            strict=False,
            keep_metadata=True,
        ),
    )

    assert read_report.ok is True
    assert Path(read_report.summary["path"]) == expected_artifact_dir
    assert read_report.summary["storage_format"] == "parquet"

    assert loaded.metadata["dataset_id"] == written_trips.metadata["dataset_id"]
    assert loaded.metadata["artifact_id"] == written_trips.metadata["artifact_id"]
    assert loaded.metadata["events"][-1]["op"] == "read_trips"


def test_read_trips_raises_when_formal_sidecar_is_missing(
    make_case_dir,
    materialize_minimal_formal_artifact,
) -> None:
    """Verifica error fatal cuando falta trips.metadata.json."""
    case_dir = make_case_dir("case_06_read_fatal_missing_sidecar")
    artifact_dir = case_dir / "artifact"

    materialize_minimal_formal_artifact(
        artifact_dir,
        storage_format="parquet",
    )

    formal_sidecar = artifact_dir / "trips.metadata.json"
    formal_sidecar.unlink()

    with pytest.raises(ExportError) as excinfo:
        read_trips(
            artifact_dir,
            options=ReadTripsOptions(
                schema=None,
                strict=False,
                keep_metadata=True,
            ),
        )

    assert excinfo.value.code == "READ.LAYOUT.MISSING_SIDECAR"
    assert not formal_sidecar.exists()


def test_read_trips_raises_when_only_legacy_sidecar_exists(
    make_case_dir,
    materialize_minimal_formal_artifact,
) -> None:
    """Verifica error fatal cuando existe metadata.json legacy sin sidecar formal."""
    case_dir = make_case_dir("case_07_read_fatal_legacy_sidecar")
    artifact_dir = case_dir / "artifact"

    materialize_minimal_formal_artifact(
        artifact_dir,
        storage_format="parquet",
    )

    formal_sidecar = artifact_dir / "trips.metadata.json"
    formal_sidecar.unlink()

    (artifact_dir / "metadata.json").write_text(
        json.dumps({"legacy": True}, ensure_ascii=False),
        encoding="utf-8",
    )

    with pytest.raises(ExportError) as excinfo:
        read_trips(
            artifact_dir,
            options=ReadTripsOptions(
                schema=None,
                strict=False,
                keep_metadata=True,
            ),
        )

    assert excinfo.value.code == "READ.LAYOUT.LEGACY_SIDECAR_DETECTED"


def test_read_trips_recovers_missing_schema_effective_and_artifact_id_when_not_strict(
    make_case_dir,
    write_valid_artifact_with_backend,
    load_sidecar,
    write_sidecar,
    assert_issue_present,
) -> None:
    """Verifica recovery degradado con strict=False para sidecar parcialmente incompleto."""
    case_dir = make_case_dir("case_08_read_degraded_recovery")

    _, artifact_dir, _, write_report = write_valid_artifact_with_backend(
        case_dir,
        artifact_name="artifact",
        storage_format="parquet",
    )

    assert write_report.ok is True

    payload_bad = load_sidecar(artifact_dir)
    payload_bad.pop("schema_effective", None)
    payload_bad["artifact_id"] = None
    payload_bad["metadata"]["artifact_id"] = None

    write_sidecar(artifact_dir, payload_bad)

    loaded, read_report = read_trips(
        artifact_dir,
        options=ReadTripsOptions(
            schema=None,
            strict=False,
            keep_metadata=True,
        ),
    )

    assert read_report.ok is True

    assert_issue_present(read_report.issues, "READ.SCHEMA_EFFECTIVE.DEFAULTED")
    assert_issue_present(read_report.issues, "READ.METADATA.ARTIFACT_ID_SET_NONE")
    assert_issue_present(read_report.issues, "READ.METADATA.VALIDATED_FORCED_FALSE")

    assert loaded.metadata["is_validated"] is False
    assert loaded.metadata["artifact_id"] is None
    assert loaded.schema_effective.to_dict() == TripSchemaEffective().to_dict()
    assert loaded.metadata["events"][-1]["op"] == "read_trips"


def test_read_trips_raises_on_missing_schema_effective_when_strict(
    make_case_dir,
    write_valid_artifact_with_backend,
    load_sidecar,
    write_sidecar,
) -> None:
    """Verifica que strict=True vuelva fatal la ausencia de schema_effective."""
    case_dir = make_case_dir("case_09_read_degraded_strict_true")

    _, artifact_dir, _, write_report = write_valid_artifact_with_backend(
        case_dir,
        artifact_name="artifact",
        storage_format="parquet",
    )

    assert write_report.ok is True

    payload_bad = load_sidecar(artifact_dir)
    payload_bad.pop("schema_effective", None)

    write_sidecar(artifact_dir, payload_bad)

    with pytest.raises(ExportError) as excinfo:
        read_trips(
            artifact_dir,
            options=ReadTripsOptions(
                schema=None,
                strict=True,
                keep_metadata=True,
            ),
        )

    assert excinfo.value.code == "READ.SCHEMA_EFFECTIVE.DEFAULTED"


def test_read_trips_raises_when_storage_backend_and_declared_data_file_mismatch(
    make_case_dir,
    write_valid_artifact_with_backend,
    load_sidecar,
    write_sidecar,
) -> None:
    """Verifica error fatal cuando storage.format y files.data son inconsistentes."""
    case_dir = make_case_dir("case_10_read_fatal_backend_file_mismatch")

    _, artifact_dir, data_path, write_report = write_valid_artifact_with_backend(
        case_dir,
        artifact_name="artifact",
        storage_format="feather",
    )

    assert write_report.ok is True
    assert data_path.exists()
    assert data_path.name == "trips.feather"

    payload_bad = load_sidecar(artifact_dir)
    payload_bad["files"]["data"] = "trips.parquet"

    write_sidecar(artifact_dir, payload_bad)

    with pytest.raises(ExportError) as excinfo:
        read_trips(
            artifact_dir,
            options=ReadTripsOptions(
                schema=None,
                strict=False,
                keep_metadata=True,
            ),
        )

    assert excinfo.value.code == "READ.LAYOUT.DATA_FILE_MISMATCH"


def test_read_trips_roundtrip_minimal_preserves_data_schema_and_traceability(
    make_case_dir,
    write_valid_artifact_with_backend,
    assert_issue_present,
    assert_data_equivalent,
) -> None:
    """Verifica round-trip mínimo write/read sobre datos, schema y eventos."""
    case_dir = make_case_dir("case_11_roundtrip_minimal")

    written_trips, artifact_dir, _, write_report = write_valid_artifact_with_backend(
        case_dir,
        artifact_name="artifact",
        storage_format="parquet",
    )

    expected_data = written_trips.data.copy(deep=True)
    expected_schema = copy.deepcopy(written_trips.schema)
    expected_schema_effective = copy.deepcopy(written_trips.schema_effective)

    loaded, read_report = read_trips(
        artifact_dir,
        options=ReadTripsOptions(
            schema=None,
            strict=False,
            keep_metadata=True,
        ),
    )

    assert write_report.ok is True
    assert read_report.ok is True

    assert loaded.metadata["dataset_id"] == written_trips.metadata["dataset_id"]
    assert loaded.metadata["artifact_id"] == written_trips.metadata["artifact_id"]
    assert loaded.metadata["is_validated"] is False

    assert_data_equivalent(loaded.data, expected_data)
    assert loaded.schema.to_dict() == expected_schema.to_dict()
    assert loaded.schema_effective.to_dict() == expected_schema_effective.to_dict()

    ops_loaded = [event["op"] for event in loaded.metadata["events"]]
    assert "write_trips" in ops_loaded
    assert ops_loaded[-1] == "read_trips"

    assert_issue_present(read_report.issues, "READ.METADATA.VALIDATED_FORCED_FALSE")