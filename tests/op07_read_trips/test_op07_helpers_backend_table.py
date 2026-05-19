from __future__ import annotations

from pathlib import Path

import pytest

from pylondrina.errors import ExportError
from pylondrina.io.trips import (
    _extract_storage_format,
    _read_trips_table_from_storage,
    _resolve_trip_data_path_from_sidecar,
)


def test_extract_storage_format_accepts_supported_backends_and_rejects_unsupported_format(
    make_sidecar_payload,
    assert_issue_present,
) -> None:
    """Verifica extracción de backends soportados y rechazo de formatos no soportados."""
    payload_parquet = make_sidecar_payload(storage_format="parquet")
    payload_feather = make_sidecar_payload(storage_format="feather")

    issues = []

    assert _extract_storage_format(payload_parquet, strict=False, issues=issues) == "parquet"
    assert _extract_storage_format(payload_feather, strict=False, issues=issues) == "feather"
    assert issues == []

    payload_bad = make_sidecar_payload(storage_format="parquet")
    payload_bad["storage"]["format"] = "csv"

    issues_bad = []

    with pytest.raises(ExportError):
        _extract_storage_format(
            payload_bad,
            strict=False,
            issues=issues_bad,
        )

    assert_issue_present(issues_bad, "READ.STORAGE.UNSUPPORTED_FORMAT")


def test_resolve_trip_data_path_from_sidecar_returns_parquet_file_for_parquet_storage(
    make_case_dir,
    materialize_minimal_formal_artifact,
) -> None:
    """Verifica resolución de trips.parquet desde files.data y storage.format."""
    case_dir = make_case_dir("test_03_02_resolve_data_path_parquet")
    paths, payload = materialize_minimal_formal_artifact(
        case_dir / "artifact",
        storage_format="parquet",
    )

    issues = []

    data_path = _resolve_trip_data_path_from_sidecar(
        paths.root_dir,
        payload,
        storage_format="parquet",
        strict=False,
        issues=issues,
    )

    assert data_path == paths.root_dir / "trips.parquet"
    assert data_path.exists()
    assert issues == []


def test_resolve_trip_data_path_from_sidecar_returns_feather_file_for_feather_storage(
    make_case_dir,
    materialize_minimal_formal_artifact,
) -> None:
    """Verifica resolución de trips.feather desde files.data y storage.format."""
    case_dir = make_case_dir("test_03_03_resolve_data_path_feather")
    paths, payload = materialize_minimal_formal_artifact(
        case_dir / "artifact",
        storage_format="feather",
    )

    issues = []

    data_path = _resolve_trip_data_path_from_sidecar(
        paths.root_dir,
        payload,
        storage_format="feather",
        strict=False,
        issues=issues,
    )

    assert data_path == paths.root_dir / "trips.feather"
    assert data_path.exists()
    assert issues == []


def test_resolve_trip_data_path_from_sidecar_uses_expected_filename_when_files_data_is_missing(
    make_case_dir,
    materialize_minimal_formal_artifact,
) -> None:
    """Verifica fallback al nombre tabular esperado cuando falta files.data."""
    case_dir = make_case_dir("test_03_04_resolve_data_path_default_filename")
    paths, payload = materialize_minimal_formal_artifact(
        case_dir / "artifact",
        storage_format="parquet",
    )

    payload["files"].pop("data")

    issues = []

    data_path = _resolve_trip_data_path_from_sidecar(
        paths.root_dir,
        payload,
        storage_format="parquet",
        strict=False,
        issues=issues,
    )

    assert data_path == paths.root_dir / "trips.parquet"
    assert data_path.exists()
    assert issues == []


def test_resolve_trip_data_path_from_sidecar_raises_on_backend_data_file_mismatch(
    make_case_dir,
    materialize_minimal_formal_artifact,
    assert_issue_present,
) -> None:
    """Verifica error fatal cuando files.data no coincide con storage.format."""
    case_dir = make_case_dir("test_03_05_data_file_mismatch")
    paths, payload = materialize_minimal_formal_artifact(
        case_dir / "artifact",
        storage_format="feather",
    )

    payload["files"]["data"] = "trips.parquet"

    issues = []

    with pytest.raises(ExportError):
        _resolve_trip_data_path_from_sidecar(
            paths.root_dir,
            payload,
            storage_format="feather",
            strict=False,
            issues=issues,
        )

    assert_issue_present(issues, "READ.LAYOUT.DATA_FILE_MISMATCH")


def test_resolve_trip_data_path_from_sidecar_raises_when_data_file_is_missing(
    make_case_dir,
    materialize_minimal_formal_artifact,
    assert_issue_present,
) -> None:
    """Verifica error fatal cuando el archivo tabular esperado no existe."""
    case_dir = make_case_dir("test_03_06_missing_data_file")
    paths, payload = materialize_minimal_formal_artifact(
        case_dir / "artifact",
        storage_format="parquet",
    )

    data_file = paths.root_dir / "trips.parquet"
    data_file.unlink()

    issues = []

    with pytest.raises(ExportError):
        _resolve_trip_data_path_from_sidecar(
            paths.root_dir,
            payload,
            storage_format="parquet",
            strict=False,
            issues=issues,
        )

    assert_issue_present(issues, "READ.LAYOUT.MISSING_DATA_FILE")


def test_read_trips_table_from_storage_loads_parquet_table(
    make_case_dir,
    materialize_minimal_formal_artifact,
    trip_df_minimal,
    assert_data_equivalent,
) -> None:
    """Verifica lectura física de trips.parquet desde el backend Parquet."""
    case_dir = make_case_dir("test_03_07_read_table_parquet")
    paths, _ = materialize_minimal_formal_artifact(
        case_dir / "artifact",
        storage_format="parquet",
    )

    issues = []

    df_loaded = _read_trips_table_from_storage(
        paths.root_dir / "trips.parquet",
        storage_format="parquet",
        issues=issues,
        destination_path=paths.root_dir,
    )

    assert len(df_loaded) == len(trip_df_minimal)
    assert list(df_loaded.columns) == list(trip_df_minimal.columns)
    assert_data_equivalent(df_loaded, trip_df_minimal)
    assert issues == []


def test_read_trips_table_from_storage_loads_feather_table(
    make_case_dir,
    materialize_minimal_formal_artifact,
    trip_df_minimal,
    assert_data_equivalent,
) -> None:
    """Verifica lectura física de trips.feather desde el backend Feather."""
    case_dir = make_case_dir("test_03_08_read_table_feather")
    paths, _ = materialize_minimal_formal_artifact(
        case_dir / "artifact",
        storage_format="feather",
    )

    issues = []

    df_loaded = _read_trips_table_from_storage(
        paths.root_dir / "trips.feather",
        storage_format="feather",
        issues=issues,
        destination_path=paths.root_dir,
    )

    assert len(df_loaded) == len(trip_df_minimal)
    assert list(df_loaded.columns) == list(trip_df_minimal.columns)
    assert_data_equivalent(df_loaded, trip_df_minimal)
    assert issues == []


def test_read_trips_table_from_storage_reports_empty_dataframe(
    make_case_dir,
    materialize_minimal_formal_artifact,
    trip_df_minimal,
    assert_issue_present,
) -> None:
    """Verifica evidencia informativa cuando el archivo reconstruye una tabla vacía."""
    case_dir = make_case_dir("test_03_09_read_empty_dataframe")
    empty_df = trip_df_minimal.iloc[0:0].copy()

    paths, _ = materialize_minimal_formal_artifact(
        case_dir / "artifact",
        df=empty_df,
        storage_format="parquet",
    )

    issues = []

    df_loaded = _read_trips_table_from_storage(
        paths.root_dir / "trips.parquet",
        storage_format="parquet",
        issues=issues,
        destination_path=paths.root_dir,
    )

    assert len(df_loaded) == 0
    assert list(df_loaded.columns) == list(empty_df.columns)
    assert_issue_present(issues, "READ.CORE.EMPTY_DATAFRAME")


def test_read_trips_table_from_storage_raises_for_corrupt_parquet_file(
    make_case_dir,
    assert_issue_present,
) -> None:
    """Verifica error fatal y código correcto para un archivo Parquet corrupto."""
    case_dir = make_case_dir("test_03_10_read_corrupt_parquet")
    root = case_dir / "artifact"
    root.mkdir(parents=True, exist_ok=True)

    bad_path = root / "trips.parquet"
    bad_path.write_text("not a parquet file", encoding="utf-8")

    issues = []

    with pytest.raises(ExportError):
        _read_trips_table_from_storage(
            bad_path,
            storage_format="parquet",
            issues=issues,
            destination_path=root,
        )

    assert_issue_present(issues, "READ.PARQUET.LOAD_FAILED")


def test_read_trips_table_from_storage_raises_for_corrupt_feather_file(
    make_case_dir,
    assert_issue_present,
) -> None:
    """Verifica error fatal y código correcto para un archivo Feather corrupto."""
    case_dir = make_case_dir("test_03_11_read_corrupt_feather")
    root = case_dir / "artifact"
    root.mkdir(parents=True, exist_ok=True)

    bad_path = root / "trips.feather"
    bad_path.write_text("not a feather file", encoding="utf-8")

    issues = []

    with pytest.raises(ExportError):
        _read_trips_table_from_storage(
            bad_path,
            storage_format="feather",
            issues=issues,
            destination_path=root,
        )

    assert_issue_present(issues, "READ.FEATHER.LOAD_FAILED")