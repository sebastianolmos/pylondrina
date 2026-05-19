from __future__ import annotations

import copy
from pathlib import Path

from pylondrina.io.trips import ReadTripsOptions, read_trips


def test_read_trips_parquet_happy_path_with_suffix_fallback_and_metadata_schema(
    make_case_dir,
    rich_tripdataset_validated,
    write_valid_artifact_with_backend,
    assert_issue_present,
    assert_data_equivalent,
) -> None:
    """Verifica lectura feliz Parquet con path sin sufijo y schema desde metadata."""
    case_dir = make_case_dir("test_01_read_parquet_fallback_metadata_schema")

    written_trips, artifact_dir, _, write_report = write_valid_artifact_with_backend(
        case_dir,
        artifact_name="bundle",
        trips=rich_tripdataset_validated,
        storage_format="parquet",
        parquet_compression="snappy",
    )

    loaded, read_report = read_trips(
        case_dir / "bundle",
        options=ReadTripsOptions(
            schema=None,
            strict=False,
            keep_metadata=True,
        ),
    )

    assert write_report.ok is True
    assert read_report.ok is True

    assert Path(read_report.parameters["path"]) == artifact_dir
    assert read_report.parameters["schema"]["source"] == "metadata"
    assert read_report.summary["path"] == str(artifact_dir)
    assert read_report.summary["schema_source"] == "metadata"
    assert read_report.summary["storage_format"] == "parquet"

    assert loaded.metadata["is_validated"] is False
    assert_issue_present(read_report.issues, "READ.METADATA.VALIDATED_FORCED_FALSE")

    assert loaded.metadata["dataset_id"] == written_trips.metadata["dataset_id"]
    assert loaded.metadata["artifact_id"] == written_trips.metadata["artifact_id"]

    assert loaded.metadata["events"][-1]["op"] == "read_trips"
    assert loaded.metadata["events"][-1]["parameters"] == read_report.parameters
    assert loaded.metadata["events"][-1]["summary"] == read_report.summary

    assert_data_equivalent(loaded.data, written_trips.data)


def test_read_trips_feather_happy_path_with_suffix_fallback_and_metadata_schema(
    make_case_dir,
    rich_tripdataset_validated,
    write_valid_artifact_with_backend,
    assert_issue_present,
    assert_data_equivalent,
) -> None:
    """Verifica lectura feliz Feather con path sin sufijo y schema desde metadata."""
    case_dir = make_case_dir("test_02_read_feather_fallback_metadata_schema")

    written_trips, artifact_dir, data_path, write_report = write_valid_artifact_with_backend(
        case_dir,
        artifact_name="bundle",
        trips=rich_tripdataset_validated,
        storage_format="feather",
        feather_compression="lz4",
    )

    loaded, read_report = read_trips(
        case_dir / "bundle",
        options=ReadTripsOptions(
            schema=None,
            strict=False,
            keep_metadata=True,
        ),
    )

    assert write_report.ok is True
    assert read_report.ok is True

    assert data_path.name == "trips.feather"
    assert data_path.exists()

    assert Path(read_report.parameters["path"]) == artifact_dir
    assert read_report.parameters["schema"]["source"] == "metadata"
    assert read_report.summary["schema_source"] == "metadata"
    assert read_report.summary["storage_format"] == "feather"

    assert loaded.metadata["is_validated"] is False
    assert_issue_present(read_report.issues, "READ.METADATA.VALIDATED_FORCED_FALSE")

    assert loaded.metadata["dataset_id"] == written_trips.metadata["dataset_id"]
    assert loaded.metadata["artifact_id"] == written_trips.metadata["artifact_id"]

    assert loaded.metadata["events"][-1]["op"] == "read_trips"
    assert loaded.metadata["events"][-1]["parameters"] == read_report.parameters
    assert loaded.metadata["events"][-1]["summary"] == read_report.summary

    assert_data_equivalent(loaded.data, written_trips.data)


def test_read_trips_full_roundtrip_parquet_preserves_dataset_contract(
    make_case_dir,
    rich_tripdataset_validated,
    write_valid_artifact_with_backend,
    assert_data_equivalent,
) -> None:
    """Verifica round-trip completo Parquet sobre datos, schema, provenance y mappings."""
    case_dir = make_case_dir("test_05_roundtrip_basic_parquet")

    trips_before_write = copy.deepcopy(rich_tripdataset_validated)

    data_original = trips_before_write.data.copy(deep=True)
    schema_original = copy.deepcopy(trips_before_write.schema)
    schema_effective_original = copy.deepcopy(trips_before_write.schema_effective)
    provenance_original = copy.deepcopy(trips_before_write.provenance)
    field_corr_original = copy.deepcopy(trips_before_write.field_correspondence)
    value_corr_original = copy.deepcopy(trips_before_write.value_correspondence)

    written_trips, artifact_dir, _, write_report = write_valid_artifact_with_backend(
        case_dir,
        artifact_name="roundtrip_bundle",
        trips=trips_before_write,
        storage_format="parquet",
        parquet_compression="snappy",
    )

    loaded, read_report = read_trips(
        case_dir / "roundtrip_bundle",
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

    assert_data_equivalent(loaded.data, data_original)

    assert loaded.schema.to_dict() == schema_original.to_dict()
    assert loaded.schema_effective.to_dict() == schema_effective_original.to_dict()
    assert loaded.provenance == provenance_original
    assert loaded.field_correspondence == field_corr_original
    assert loaded.value_correspondence == value_corr_original

    ops_loaded = [event["op"] for event in loaded.metadata["events"]]
    assert "write_trips" in ops_loaded
    assert ops_loaded[-1] == "read_trips"


def test_read_trips_full_roundtrip_feather_preserves_dataset_contract(
    make_case_dir,
    rich_tripdataset_validated,
    write_valid_artifact_with_backend,
    assert_data_equivalent,
) -> None:
    """Verifica round-trip completo Feather sobre datos, schema, provenance y mappings."""
    case_dir = make_case_dir("test_06_roundtrip_basic_feather")

    trips_before_write = copy.deepcopy(rich_tripdataset_validated)

    data_original = trips_before_write.data.copy(deep=True)
    schema_original = copy.deepcopy(trips_before_write.schema)
    schema_effective_original = copy.deepcopy(trips_before_write.schema_effective)
    provenance_original = copy.deepcopy(trips_before_write.provenance)
    field_corr_original = copy.deepcopy(trips_before_write.field_correspondence)
    value_corr_original = copy.deepcopy(trips_before_write.value_correspondence)

    written_trips, artifact_dir, _, write_report = write_valid_artifact_with_backend(
        case_dir,
        artifact_name="roundtrip_bundle",
        trips=trips_before_write,
        storage_format="feather",
        feather_compression="lz4",
    )

    loaded, read_report = read_trips(
        case_dir / "roundtrip_bundle",
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

    assert_data_equivalent(loaded.data, data_original)

    assert loaded.schema.to_dict() == schema_original.to_dict()
    assert loaded.schema_effective.to_dict() == schema_effective_original.to_dict()
    assert loaded.provenance == provenance_original
    assert loaded.field_correspondence == field_corr_original
    assert loaded.value_correspondence == value_corr_original

    ops_loaded = [event["op"] for event in loaded.metadata["events"]]
    assert "write_trips" in ops_loaded
    assert ops_loaded[-1] == "read_trips"


def test_read_trips_keep_metadata_false_preserves_state_without_appending_read_event(
    make_case_dir,
    rich_tripdataset_validated,
    write_valid_artifact_with_backend,
    load_sidecar,
    assert_issue_present,
) -> None:
    """Verifica que keep_metadata=False no agregue read_trips, pero conserve metadata útil."""
    case_dir = make_case_dir("test_12_read_keep_metadata_false")

    written_trips, artifact_dir, _, write_report = write_valid_artifact_with_backend(
        case_dir,
        artifact_name="bundle",
        trips=rich_tripdataset_validated,
        storage_format="feather",
        feather_compression="lz4",
    )

    assert write_report.ok is True

    sidecar = load_sidecar(artifact_dir)
    events_before = copy.deepcopy(sidecar["metadata"]["events"])

    loaded, read_report = read_trips(
        artifact_dir,
        options=ReadTripsOptions(
            schema=None,
            strict=False,
            keep_metadata=False,
        ),
    )

    assert read_report.ok is True
    assert loaded.metadata["is_validated"] is False
    assert_issue_present(read_report.issues, "READ.METADATA.VALIDATED_FORCED_FALSE")

    ops_loaded = [event["op"] for event in loaded.metadata["events"]]
    assert ops_loaded == [event["op"] for event in events_before]
    assert "read_trips" not in ops_loaded

    assert "dataset_id" in loaded.metadata
    assert loaded.provenance == written_trips.provenance