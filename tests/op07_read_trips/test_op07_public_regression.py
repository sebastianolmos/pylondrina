from __future__ import annotations

import copy

import pandas as pd
import pytest

from pylondrina.errors import ExportError
from pylondrina.io.trips import ReadTripsOptions, read_trips
from pylondrina.schema import TripSchemaEffective


def _series_as_string_with_na(series: pd.Series) -> pd.Series:
    """Normaliza una serie a string preservando el patrón de NA."""
    return series.astype("string")


def test_read_trips_recovers_degraded_feather_sidecar_when_not_strict(
    make_case_dir,
    write_valid_artifact_with_backend,
    load_sidecar,
    write_sidecar,
    assert_issue_present,
) -> None:
    """Verifica recovery público con strict=False ante sidecar Feather degradado."""
    case_dir = make_case_dir("test_10_read_degraded_recovery_strict_false")

    _, artifact_dir, _, write_report = write_valid_artifact_with_backend(
        case_dir,
        artifact_name="bundle",
        storage_format="feather",
        feather_compression="lz4",
    )

    assert write_report.ok is True

    payload = load_sidecar(artifact_dir)
    payload.pop("schema_effective", None)
    payload["dataset_id"] = ""
    payload["artifact_id"] = None
    payload["metadata"]["dataset_id"] = ""
    payload["metadata"]["artifact_id"] = None
    payload["metadata"]["is_validated"] = True

    write_sidecar(artifact_dir, payload)

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
    assert_issue_present(read_report.issues, "READ.METADATA.DATASET_ID_REGENERATED")
    assert_issue_present(read_report.issues, "READ.METADATA.ARTIFACT_ID_SET_NONE")
    assert_issue_present(read_report.issues, "READ.METADATA.VALIDATED_FORCED_FALSE")

    assert loaded.schema_effective.to_dict() == TripSchemaEffective().to_dict()

    assert isinstance(loaded.metadata["dataset_id"], str)
    assert loaded.metadata["dataset_id"]
    assert loaded.metadata["artifact_id"] is None
    assert loaded.metadata["is_validated"] is False

    assert read_report.summary["dataset_id_status"] == "regenerated"
    assert read_report.summary["artifact_id_status"] == "missing_or_invalid"
    assert read_report.summary["storage_format"] == "feather"

    assert loaded.metadata["events"][-1]["op"] == "read_trips"


def test_read_trips_raises_for_degraded_sidecar_when_strict(
    make_case_dir,
    write_valid_artifact_with_backend,
    load_sidecar,
    write_sidecar,
) -> None:
    """Verifica que strict=True vuelva fatal un sidecar sin schema_effective."""
    case_dir = make_case_dir("test_11_read_degraded_strict_true")

    _, artifact_dir, _, write_report = write_valid_artifact_with_backend(
        case_dir,
        artifact_name="bundle",
        storage_format="feather",
        feather_compression="lz4",
    )

    assert write_report.ok is True

    payload = load_sidecar(artifact_dir)
    payload.pop("schema_effective", None)

    write_sidecar(artifact_dir, payload)

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


def test_read_trips_feather_sidecar_backend_and_data_file_are_coherent(
    make_case_dir,
    write_valid_artifact_with_backend,
    load_sidecar,
) -> None:
    """Verifica coherencia pública entre sidecar, backend Feather y archivo tabular."""
    case_dir = make_case_dir("test_13_sidecar_backend_coherence_feather")

    written_trips, artifact_dir, data_path, write_report = write_valid_artifact_with_backend(
        case_dir,
        artifact_name="bundle",
        storage_format="feather",
        feather_compression="lz4",
    )

    assert write_report.ok is True

    sidecar = load_sidecar(artifact_dir)

    assert sidecar["storage"]["format"] == "feather"
    assert sidecar["files"]["data"] == "trips.feather"
    assert data_path.name == "trips.feather"
    assert data_path.exists()
    assert write_report.summary["storage_format"] == "feather"

    loaded, read_report = read_trips(
        artifact_dir,
        options=ReadTripsOptions(
            schema=None,
            strict=False,
            keep_metadata=True,
        ),
    )

    assert read_report.ok is True
    assert read_report.summary["storage_format"] == "feather"
    assert loaded.metadata["artifact_id"] == written_trips.metadata["artifact_id"]
    assert loaded.metadata["is_validated"] is False


def test_read_trips_raises_when_feather_sidecar_declares_parquet_data_file(
    make_case_dir,
    write_valid_artifact_with_backend,
    load_sidecar,
    write_sidecar,
) -> None:
    """Verifica error fatal por mismatch entre storage.format y files.data."""
    case_dir = make_case_dir("test_14_data_file_mismatch")

    _, artifact_dir, data_path, write_report = write_valid_artifact_with_backend(
        case_dir,
        artifact_name="bundle",
        storage_format="feather",
        feather_compression="lz4",
    )

    assert write_report.ok is True
    assert data_path.name == "trips.feather"
    assert data_path.exists()

    payload = load_sidecar(artifact_dir)
    payload["storage"]["format"] = "feather"
    payload["files"]["data"] = "trips.parquet"

    write_sidecar(artifact_dir, payload)

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
        parquet_compression="snappy",
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


def test_read_trips_feather_roundtrip_preserves_logical_categorical_integrity(
    make_case_dir,
    write_valid_artifact_with_backend,
    rich_tripdataset_validated,
    selected_categorical_columns,
    observed_non_null_values,
) -> None:
    """Verifica integridad lógica de columnas categóricas tras round-trip Feather."""
    case_dir = make_case_dir("test_15_categorical_integrity_roundtrip_feather")

    written_trips, artifact_dir, _, write_report = write_valid_artifact_with_backend(
        case_dir,
        artifact_name="integrity_bundle",
        trips=rich_tripdataset_validated,
        storage_format="feather",
        feather_compression="lz4",
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
    assert read_report.summary["storage_format"] == "feather"
    assert loaded.metadata["is_validated"] is False

    checked_cols: list[str] = []

    for column_name in selected_categorical_columns(written_trips.data):
        if column_name not in loaded.data.columns:
            continue

        checked_cols.append(column_name)

        original_series = _series_as_string_with_na(
            written_trips.data[column_name]
        ).reset_index(drop=True)

        loaded_series = _series_as_string_with_na(
            loaded.data[column_name]
        ).reset_index(drop=True)

        pd.testing.assert_series_equal(
            loaded_series,
            original_series,
            check_names=False,
            check_dtype=False,
        )

        pd.testing.assert_series_equal(
            loaded.data[column_name].isna().reset_index(drop=True),
            written_trips.data[column_name].isna().reset_index(drop=True),
            check_names=False,
        )

        original_counts = original_series.value_counts(dropna=False).sort_index()
        loaded_counts = loaded_series.value_counts(dropna=False).sort_index()

        pd.testing.assert_series_equal(
            loaded_counts,
            original_counts,
            check_names=False,
            check_dtype=False,
        )

        assert observed_non_null_values(loaded.data[column_name]) == observed_non_null_values(
            written_trips.data[column_name]
        )

    assert checked_cols, "No se encontró ninguna columna categórica observable para verificar."