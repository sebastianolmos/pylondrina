from __future__ import annotations

import copy
import json
from pathlib import Path

import pandas as pd
import pytest
import pyarrow as pa
import pyarrow.feather as feather
import pyarrow.parquet as pq

from pylondrina.io.trips import WriteTripsOptions, write_trips


CATEGORICAL_CONTRACT_COLUMNS = ["mode", "purpose"]


def _load_sidecar(artifact_dir: Path) -> dict:
    """Carga el sidecar formal de trips desde un artefacto escrito."""
    sidecar_path = artifact_dir / "trips.metadata.json"
    assert sidecar_path.exists(), f"No existe sidecar: {sidecar_path}"
    return json.loads(sidecar_path.read_text(encoding="utf-8"))


def _repeat_trip_data(trips, repetitions: int) -> None:
    """Reemplaza `trips.data` por una versión repetida para probar escritura física."""
    trips.data = pd.concat(
        [trips.data.copy(deep=True)] * repetitions,
        ignore_index=True,
    )


def _categorical_columns_present(df: pd.DataFrame) -> list[str]:
    """Retorna columnas categóricas contractuales presentes en el dataframe."""
    return [col for col in CATEGORICAL_CONTRACT_COLUMNS if col in df.columns]


def _as_naive_arrow_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte categóricos contractuales a object/string para escritura naive."""
    df_naive = df.copy(deep=True)

    for col in _categorical_columns_present(df_naive):
        df_naive[col] = df_naive[col].astype("string").astype(object)

    return df_naive


def test_write_trips_persists_contractual_categories_with_dictionary_encoding_in_parquet(
    tmp_path: Path,
    trip_dataset_validated,
):
    """Verifica que `write_trips` materialice categóricos como dictionary en Parquet."""
    trips = trip_dataset_validated
    _repeat_trip_data(trips, repetitions=3000)

    artifact_dir = tmp_path / "artifact_parquet_categorical"

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

    parquet_path = artifact_dir / "trips.parquet"

    assert report.ok is True
    assert parquet_path.exists()
    assert (artifact_dir / "trips.metadata.json").exists()

    sidecar = _load_sidecar(artifact_dir)
    assert sidecar["storage"]["format"] == "parquet"
    assert sidecar["files"]["data"] == "trips.parquet"

    loaded = pd.read_parquet(parquet_path, engine="pyarrow")

    assert len(loaded) == len(trips.data)
    assert list(loaded.columns) == list(trips.data.columns)

    checked_columns = _categorical_columns_present(trips.data)
    assert checked_columns

    parquet_file = pq.ParquetFile(parquet_path)
    try:
        parquet_columns = parquet_file.schema_arrow.names

        for col_name in checked_columns:
            assert col_name in parquet_columns

            col_idx = parquet_columns.index(col_name)
            encodings = {
                str(encoding).upper()
                for encoding in parquet_file.metadata.row_group(0).column(col_idx).encodings
            }

            assert any("DICTIONARY" in encoding for encoding in encodings), (
                f"{col_name} no quedó con dictionary encoding observable: {encodings}"
            )
    finally:
        parquet_file.close()


def test_write_trips_persists_contractual_categories_as_dictionary_arrays_in_feather(
    tmp_path: Path,
    trip_dataset_validated,
):
    """Verifica que `write_trips` materialice categóricos como dictionary en Feather."""
    trips = trip_dataset_validated
    _repeat_trip_data(trips, repetitions=3000)

    artifact_dir = tmp_path / "artifact_feather_categorical"

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

    feather_path = artifact_dir / "trips.feather"

    assert report.ok is True
    assert feather_path.exists()
    assert (artifact_dir / "trips.metadata.json").exists()

    sidecar = _load_sidecar(artifact_dir)
    assert sidecar["storage"]["format"] == "feather"
    assert sidecar["storage"]["options"]["version"] == 2
    assert sidecar["files"]["data"] == "trips.feather"

    table = feather.read_table(feather_path)

    assert table.num_rows == len(trips.data)
    assert table.schema.names == list(trips.data.columns)

    checked_columns = _categorical_columns_present(trips.data)
    assert checked_columns

    for col_name in checked_columns:
        arrow_type = table.schema.field(col_name).type
        assert pa.types.is_dictionary(arrow_type), (
            f"{col_name} no quedó como dictionary en Feather: {arrow_type}"
        )


def test_write_trips_preserves_logical_values_in_feather_categorical_roundtrip(
    tmp_path: Path,
    trip_dataset_validated,
):
    """Verifica que la representación dictionary Feather preserve valores lógicos."""
    trips = trip_dataset_validated
    data_before = trips.data.copy(deep=True)
    _repeat_trip_data(trips, repetitions=3000)

    expected = pd.concat(
        [data_before] * 3000,
        ignore_index=True,
    )

    artifact_dir = tmp_path / "artifact_feather_logical_values"

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

    loaded = feather.read_feather(artifact_dir / "trips.feather")

    assert len(loaded) == len(expected)
    assert list(loaded.columns) == list(expected.columns)

    for col_name in _categorical_columns_present(expected):
        pd.testing.assert_series_equal(
            loaded[col_name].reset_index(drop=True).astype("string"),
            expected[col_name].reset_index(drop=True).astype("string"),
            check_names=False,
        )


def test_write_trips_feather_optimized_output_is_smaller_than_naive_string_write(
    tmp_path: Path,
    trip_dataset_validated,
):
    """Compara Feather optimizado por `write_trips` contra escritura naive string/object."""
    trips = trip_dataset_validated
    _repeat_trip_data(trips, repetitions=30_000)

    artifact_dir = tmp_path / "artifact_feather_optimized"

    optimized_report = write_trips(
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

    optimized_path = artifact_dir / "trips.feather"

    assert optimized_report.ok is True
    assert optimized_path.exists()

    naive_path = tmp_path / "naive_trips.feather"
    df_naive = _as_naive_arrow_dataframe(trips.data)

    table_naive = pa.Table.from_pandas(df_naive, preserve_index=False)
    feather.write_feather(
        table_naive,
        naive_path,
        compression="lz4",
        version=2,
    )

    assert naive_path.exists()

    optimized_size = optimized_path.stat().st_size
    naive_size = naive_path.stat().st_size

    assert optimized_size < naive_size, (
        "Se esperaba que la ruta optimizada con categóricos dictionary fuera "
        f"menor que la escritura naive. optimized={optimized_size}, naive={naive_size}"
    )


def test_write_trips_feather_sidecar_matches_dictionary_backend_artifact(
    tmp_path: Path,
    trip_dataset_validated,
):
    """Verifica coherencia mínima entre sidecar Feather y artefacto categórico escrito."""
    trips = trip_dataset_validated
    _repeat_trip_data(trips, repetitions=3000)

    artifact_dir = tmp_path / "artifact_feather_sidecar"

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

    feather_path = artifact_dir / "trips.feather"
    sidecar = _load_sidecar(artifact_dir)

    assert report.ok is True
    assert feather_path.exists()

    assert sidecar["storage"]["format"] == "feather"
    assert sidecar["storage"]["options"]["compression"] == "lz4"
    assert sidecar["storage"]["options"]["version"] == 2
    assert sidecar["files"]["data"] == feather_path.name
    assert sidecar["files"]["metadata"] == "trips.metadata.json"

    table = feather.read_table(feather_path)

    for col_name in _categorical_columns_present(trips.data):
        assert pa.types.is_dictionary(table.schema.field(col_name).type)