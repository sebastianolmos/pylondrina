from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import pyarrow as pa
import pyarrow.feather as feather
import pyarrow.parquet as pq

from pylondrina.errors import ExportError
from pylondrina.io.trips import (
    _assert_staging_complete,
    _cleanup_staging_dir,
    _collect_arrow_categorical_fields,
    _commit_staged_trips_artifact,
    _create_trips_staging_dir,
    _prepare_trips_df_for_arrow_write,
    _resolve_trips_artifact_paths,
    _write_trips_table_to_staging,
)
from pylondrina.reports import Issue


def _issue_codes(issues: list[Issue]) -> list[str]:
    """Retorna los códigos de issues emitidos durante el test."""
    return [issue.code for issue in issues]


def _assert_issue_present(issues: list[Issue], code: str) -> None:
    """Verifica que un código de issue esperado esté presente."""
    codes = _issue_codes(issues)
    assert code in codes, f"No se encontró {code}. Codes actuales: {codes}"


def _assert_issue_absent(issues: list[Issue], code: str) -> None:
    """Verifica que un código de issue no esté presente."""
    codes = _issue_codes(issues)
    assert code not in codes, f"Se encontró inesperadamente {code}. Codes actuales: {codes}"


def test_collect_arrow_categorical_fields_uses_schema_and_effective_schema(
    trip_df_minimal,
    trip_schema_minimal,
    trip_schema_effective_minimal,
):
    """Verifica resolución de campos categóricos para persistencia Arrow."""
    categorical_fields = _collect_arrow_categorical_fields(
        trip_df_minimal,
        trip_schema_minimal,
        trip_schema_effective_minimal,
    )

    assert set(categorical_fields) == {"mode", "purpose"}
    assert all(field in trip_df_minimal.columns for field in categorical_fields)
    assert "comment" not in categorical_fields
    assert "trip_weight" not in categorical_fields


def test_prepare_trips_df_for_arrow_write_converts_only_contractual_categories(
    trip_df_minimal,
    trip_schema_minimal,
    trip_schema_effective_minimal,
):
    """Verifica preparación Arrow sin mutar el dataframe fuente."""
    df = trip_df_minimal.copy(deep=True)

    df["mode"] = pd.Categorical(
        df["mode"],
        categories=["bus", "metro", "walk", "car"],
    )
    df_before = df.copy(deep=True)

    prepared = _prepare_trips_df_for_arrow_write(
        df,
        trip_schema_minimal,
        trip_schema_effective_minimal,
    )

    assert prepared is not df
    pd.testing.assert_frame_equal(df, df_before)

    assert isinstance(prepared["mode"].dtype, pd.CategoricalDtype)
    assert isinstance(prepared["purpose"].dtype, pd.CategoricalDtype)

    assert set(prepared["mode"].cat.categories) == set(df["mode"].dropna().unique())
    assert set(prepared["purpose"].cat.categories) == set(df["purpose"].dropna().unique())

    assert not isinstance(prepared["comment"].dtype, pd.CategoricalDtype)
    assert prepared["trip_weight"].dtype == df["trip_weight"].dtype


def test_write_trips_table_to_staging_writes_parquet_file(
    tmp_path: Path,
    trip_df_minimal,
    trip_schema_minimal,
    trip_schema_effective_minimal,
):
    """Verifica escritura tabular Parquet desde helper interno de staging."""
    staging_dir = tmp_path / "staging_parquet"
    staging_dir.mkdir(parents=True)

    data_path = staging_dir / "trips.parquet"
    issues: list[Issue] = []

    _write_trips_table_to_staging(
        trip_df_minimal,
        data_path,
        storage_format="parquet",
        parquet_compression="snappy",
        feather_compression="lz4",
        schema=trip_schema_minimal,
        schema_effective=trip_schema_effective_minimal,
        issues=issues,
        destination_path=staging_dir,
    )

    assert issues == []
    assert data_path.exists()
    assert data_path.is_file()

    loaded = pd.read_parquet(data_path, engine="pyarrow")

    assert len(loaded) == len(trip_df_minimal)
    assert list(loaded.columns) == list(trip_df_minimal.columns)

    pd.testing.assert_series_equal(
        loaded["movement_id"].reset_index(drop=True).astype("string"),
        trip_df_minimal["movement_id"].reset_index(drop=True).astype("string"),
        check_names=False,
    )

    parquet_file = pq.ParquetFile(data_path)
    try:
        schema_names = parquet_file.schema_arrow.names
        checked_categorical_columns = [
            col for col in ["mode", "purpose"]
            if col in schema_names
        ]

        assert checked_categorical_columns

        for col_name in checked_categorical_columns:
            col_idx = schema_names.index(col_name)
            encodings = {
                str(encoding).upper()
                for encoding in parquet_file.metadata.row_group(0).column(col_idx).encodings
            }

            assert any("DICTIONARY" in encoding for encoding in encodings), (
                f"{col_name} no quedó con dictionary encoding observable: {encodings}"
            )
    finally:
        parquet_file.close()


def test_write_trips_table_to_staging_writes_feather_file(
    tmp_path: Path,
    trip_df_minimal,
    trip_schema_minimal,
    trip_schema_effective_minimal,
):
    """Verifica escritura tabular Feather desde helper interno de staging."""
    staging_dir = tmp_path / "staging_feather"
    staging_dir.mkdir(parents=True)

    data_path = staging_dir / "trips.feather"
    issues: list[Issue] = []

    _write_trips_table_to_staging(
        trip_df_minimal,
        data_path,
        storage_format="feather",
        parquet_compression="snappy",
        feather_compression="lz4",
        schema=trip_schema_minimal,
        schema_effective=trip_schema_effective_minimal,
        issues=issues,
        destination_path=staging_dir,
    )

    assert issues == []
    assert data_path.exists()
    assert data_path.is_file()

    loaded = feather.read_feather(data_path)

    assert len(loaded) == len(trip_df_minimal)
    assert list(loaded.columns) == list(trip_df_minimal.columns)

    pd.testing.assert_series_equal(
        loaded["movement_id"].reset_index(drop=True).astype("string"),
        trip_df_minimal["movement_id"].reset_index(drop=True).astype("string"),
        check_names=False,
    )

    table = feather.read_table(data_path)

    assert table.num_rows == len(trip_df_minimal)
    assert table.schema.names == list(trip_df_minimal.columns)

    for col_name in ["mode", "purpose"]:
        arrow_type = table.schema.field(col_name).type
        assert pa.types.is_dictionary(arrow_type), (
            f"{col_name} no quedó como dictionary en Feather: {arrow_type}"
        )


def test_write_trips_table_to_staging_raises_for_unsupported_storage_format(
    tmp_path: Path,
    trip_df_minimal,
    trip_schema_minimal,
    trip_schema_effective_minimal,
):
    """Verifica error de escritura ante backend no soportado en helper tabular."""
    staging_dir = tmp_path / "staging_unsupported"
    staging_dir.mkdir(parents=True)

    issues: list[Issue] = []

    with pytest.raises(ExportError) as excinfo:
        _write_trips_table_to_staging(
            trip_df_minimal,
            staging_dir / "trips.unknown",
            storage_format="unknown",
            parquet_compression="snappy",
            feather_compression="lz4",
            schema=trip_schema_minimal,
            schema_effective=trip_schema_effective_minimal,
            issues=issues,
            destination_path=staging_dir,
        )

    assert excinfo.value.code == "WRT.PARQUET.WRITE_FAILED"
    _assert_issue_present(issues, "WRT.PARQUET.WRITE_FAILED")


def test_create_and_cleanup_trips_staging_dir(tmp_path: Path):
    """Verifica creación y limpieza best-effort del directorio de staging."""
    final_dir = tmp_path / "artifact"
    issues: list[Issue] = []

    staging_dir = _create_trips_staging_dir(final_dir, issues=issues)

    assert issues == []
    assert staging_dir.exists()
    assert staging_dir.is_dir()
    assert staging_dir.parent == tmp_path
    assert staging_dir.name.startswith(f".{final_dir.name}.staging.")

    marker = staging_dir / "marker.txt"
    marker.write_text("staging content", encoding="utf-8")
    assert marker.exists()

    _cleanup_staging_dir(
        staging_dir,
        final_dir,
        files_written=["marker.txt"],
        issues=issues,
    )

    assert not staging_dir.exists()
    _assert_issue_absent(issues, "WRT.IO.CLEANUP_FAILED")


def test_cleanup_staging_dir_is_noop_when_staging_does_not_exist(tmp_path: Path):
    """Verifica que cleanup no emita issues si el staging ya no existe."""
    staging_dir = tmp_path / "missing_staging"
    final_dir = tmp_path / "artifact"
    issues: list[Issue] = []

    _cleanup_staging_dir(
        staging_dir,
        final_dir,
        files_written=["trips.parquet", "trips.metadata.json"],
        issues=issues,
    )

    assert issues == []


def test_assert_staging_complete_accepts_parquet_layout(tmp_path: Path):
    """Verifica staging completo para layout Parquet."""
    staging_dir = tmp_path / "staging_parquet_complete"
    staging_dir.mkdir(parents=True)

    paths = _resolve_trips_artifact_paths(staging_dir)
    (staging_dir / "trips.parquet").write_bytes(b"placeholder")
    paths.sidecar_path.write_text("{}", encoding="utf-8")

    issues: list[Issue] = []

    _assert_staging_complete(
        paths,
        expected_files=["trips.parquet", "trips.metadata.json"],
        issues=issues,
        destination_path=staging_dir,
    )

    assert issues == []


def test_assert_staging_complete_accepts_feather_layout(tmp_path: Path):
    """Verifica staging completo para layout Feather."""
    staging_dir = tmp_path / "staging_feather_complete"
    staging_dir.mkdir(parents=True)

    paths = _resolve_trips_artifact_paths(staging_dir)
    (staging_dir / "trips.feather").write_bytes(b"placeholder")
    paths.sidecar_path.write_text("{}", encoding="utf-8")

    issues: list[Issue] = []

    _assert_staging_complete(
        paths,
        expected_files=["trips.feather", "trips.metadata.json"],
        issues=issues,
        destination_path=staging_dir,
    )

    assert issues == []


def test_assert_staging_complete_raises_when_required_file_is_missing(tmp_path: Path):
    """Verifica fatal por staging incompleto antes del commit."""
    staging_dir = tmp_path / "staging_incomplete"
    staging_dir.mkdir(parents=True)

    paths = _resolve_trips_artifact_paths(staging_dir)
    (staging_dir / "trips.parquet").write_bytes(b"placeholder")

    issues: list[Issue] = []

    with pytest.raises(ExportError) as excinfo:
        _assert_staging_complete(
            paths,
            expected_files=["trips.parquet", "trips.metadata.json"],
            issues=issues,
            destination_path=staging_dir,
        )

    assert excinfo.value.code == "WRT.IO.STAGING_INCOMPLETE"
    _assert_issue_present(issues, "WRT.IO.STAGING_INCOMPLETE")


def test_commit_staged_trips_artifact_moves_staging_to_final_dir(tmp_path: Path):
    """Verifica commit exitoso desde staging hacia destino final inexistente."""
    staging_dir = tmp_path / "staging_commit"
    final_dir = tmp_path / "artifact"

    staging_dir.mkdir(parents=True)
    (staging_dir / "trips.parquet").write_bytes(b"placeholder")
    (staging_dir / "trips.metadata.json").write_text("{}", encoding="utf-8")

    issues: list[Issue] = []

    _commit_staged_trips_artifact(
        staging_dir,
        final_dir,
        mode="error_if_exists",
        files_written=["trips.parquet", "trips.metadata.json"],
        issues=issues,
    )

    assert issues == []
    assert not staging_dir.exists()
    assert final_dir.exists()
    assert (final_dir / "trips.parquet").exists()
    assert (final_dir / "trips.metadata.json").exists()


def test_commit_staged_trips_artifact_overwrites_existing_final_dir(tmp_path: Path):
    """Verifica que `mode='overwrite'` reemplace el destino existente."""
    staging_dir = tmp_path / "staging_overwrite"
    final_dir = tmp_path / "artifact"

    final_dir.mkdir(parents=True)
    old_file = final_dir / "old_residual.txt"
    old_file.write_text("old", encoding="utf-8")

    staging_dir.mkdir(parents=True)
    (staging_dir / "trips.feather").write_bytes(b"placeholder")
    (staging_dir / "trips.metadata.json").write_text("{}", encoding="utf-8")

    issues: list[Issue] = []

    _commit_staged_trips_artifact(
        staging_dir,
        final_dir,
        mode="overwrite",
        files_written=["trips.feather", "trips.metadata.json"],
        issues=issues,
    )

    assert issues == []
    assert not staging_dir.exists()
    assert final_dir.exists()
    assert not old_file.exists()
    assert (final_dir / "trips.feather").exists()
    assert (final_dir / "trips.metadata.json").exists()


def test_commit_staged_trips_artifact_raises_when_destination_exists(tmp_path: Path):
    """Verifica fatal por colisión si `mode='error_if_exists'` y destino existe."""
    staging_dir = tmp_path / "staging_collision"
    final_dir = tmp_path / "artifact"

    final_dir.mkdir(parents=True)
    sentinel = final_dir / "sentinel.txt"
    sentinel.write_text("do not delete", encoding="utf-8")

    staging_dir.mkdir(parents=True)
    (staging_dir / "trips.parquet").write_bytes(b"placeholder")
    (staging_dir / "trips.metadata.json").write_text("{}", encoding="utf-8")

    issues: list[Issue] = []

    with pytest.raises(ExportError) as excinfo:
        _commit_staged_trips_artifact(
            staging_dir,
            final_dir,
            mode="error_if_exists",
            files_written=["trips.parquet", "trips.metadata.json"],
            issues=issues,
        )

    assert excinfo.value.code == "WRT.DEST.ALREADY_EXISTS"
    _assert_issue_present(issues, "WRT.DEST.ALREADY_EXISTS")

    assert staging_dir.exists()
    assert final_dir.exists()
    assert sentinel.exists()
    assert sentinel.read_text(encoding="utf-8") == "do not delete"