from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal, assert_series_equal
from pyproj import Transformer

from pylondrina.transforms.spatial import project_xy_to_latlon


SOURCE_CRS = "EPSG:5361"
TARGET_CRS = "EPSG:4326"


def _expected_lon_lat(
    df: pd.DataFrame,
    *,
    x_col: str,
    y_col: str,
    valid_mask: pd.Series | None = None,
) -> tuple[pd.Series, pd.Series]:
    """Calcula lon/lat esperadas con pyproj para comparar contra la función pública."""
    if valid_mask is None:
        valid_mask = df[x_col].notna() & df[y_col].notna()

    lon = pd.Series(np.nan, index=df.index, dtype="float64")
    lat = pd.Series(np.nan, index=df.index, dtype="float64")

    transformer = Transformer.from_crs(SOURCE_CRS, TARGET_CRS, always_xy=True)
    lon_values, lat_values = transformer.transform(
        pd.to_numeric(df.loc[valid_mask, x_col]).to_numpy(),
        pd.to_numeric(df.loc[valid_mask, y_col]).to_numpy(),
    )

    lon.loc[valid_mask] = lon_values
    lat.loc[valid_mask] = lat_values

    return lon, lat


def test_project_xy_to_latlon_converts_eod_origin_and_destination_dropping_input_columns() -> None:
    """Verifica conversión EOD origen/destino y eliminación controlada de columnas X/Y."""
    df_eod = pd.DataFrame(
        {
            "OrigenCoordX": [345000.0, 346200.5],
            "OrigenCoordY": [6291000.0, 6292200.0],
            "DestinoCoordX": [348000.0, 349150.0],
            "DestinoCoordY": [6293000.0, 6294100.0],
        }
    )
    df_before = df_eod.copy(deep=True)

    expected_origin_lon, expected_origin_lat = _expected_lon_lat(
        df_eod,
        x_col="OrigenCoordX",
        y_col="OrigenCoordY",
    )
    expected_destination_lon, expected_destination_lat = _expected_lon_lat(
        df_eod,
        x_col="DestinoCoordX",
        y_col="DestinoCoordY",
    )

    df_out = project_xy_to_latlon(
        df_eod,
        x_col="OrigenCoordX",
        y_col="OrigenCoordY",
        source_crs=SOURCE_CRS,
        lon_col="OrigenCoordLon",
        lat_col="OrigenCoordLat",
        keep_debug_cols=False,
        drop_input_cols=True,
    )

    df_out = project_xy_to_latlon(
        df_out,
        x_col="DestinoCoordX",
        y_col="DestinoCoordY",
        source_crs=SOURCE_CRS,
        lon_col="DestinoCoordLon",
        lat_col="DestinoCoordLat",
        keep_debug_cols=False,
        drop_input_cols=True,
    )

    assert {"OrigenCoordLon", "OrigenCoordLat", "DestinoCoordLon", "DestinoCoordLat"}.issubset(
        df_out.columns
    )
    assert {"OrigenCoordX", "OrigenCoordY", "DestinoCoordX", "DestinoCoordY"}.isdisjoint(
        df_out.columns
    )

    assert_series_equal(
        df_out["OrigenCoordLon"],
        expected_origin_lon.rename("OrigenCoordLon"),
        check_exact=False,
        rtol=1e-12,
    )
    assert_series_equal(
        df_out["OrigenCoordLat"],
        expected_origin_lat.rename("OrigenCoordLat"),
        check_exact=False,
        rtol=1e-12,
    )
    assert_series_equal(
        df_out["DestinoCoordLon"],
        expected_destination_lon.rename("DestinoCoordLon"),
        check_exact=False,
        rtol=1e-12,
    )
    assert_series_equal(
        df_out["DestinoCoordLat"],
        expected_destination_lat.rename("DestinoCoordLat"),
        check_exact=False,
        rtol=1e-12,
    )

    assert_frame_equal(df_eod, df_before)


def test_project_xy_to_latlon_converts_adatrap_string_coordinates_and_keeps_input_columns() -> None:
    """Verifica conversión ADATRAP desde strings numéricos conservando columnas originales."""
    df_adatrap = pd.DataFrame(
        {
            "subida_x": ["349000", "350200"],
            "subida_y": ["6294500", "6295100"],
            "bajada_x": ["351000", "352100"],
            "bajada_y": ["6296000", "6297000"],
        }
    )
    df_before = df_adatrap.copy(deep=True)

    expected_subida_lon, expected_subida_lat = _expected_lon_lat(
        df_adatrap,
        x_col="subida_x",
        y_col="subida_y",
    )
    expected_bajada_lon, expected_bajada_lat = _expected_lon_lat(
        df_adatrap,
        x_col="bajada_x",
        y_col="bajada_y",
    )

    df_out = project_xy_to_latlon(
        df_adatrap,
        x_col="subida_x",
        y_col="subida_y",
        source_crs=SOURCE_CRS,
        lon_col="subida_lon",
        lat_col="subida_lat",
        keep_debug_cols=False,
        drop_input_cols=False,
    )

    df_out = project_xy_to_latlon(
        df_out,
        x_col="bajada_x",
        y_col="bajada_y",
        source_crs=SOURCE_CRS,
        lon_col="bajada_lon",
        lat_col="bajada_lat",
        keep_debug_cols=False,
        drop_input_cols=False,
    )

    assert set(df_before.columns).issubset(set(df_out.columns))
    assert {"subida_lon", "subida_lat", "bajada_lon", "bajada_lat"}.issubset(df_out.columns)

    assert_series_equal(
        df_out["subida_lon"],
        expected_subida_lon.rename("subida_lon"),
        check_exact=False,
        rtol=1e-12,
    )
    assert_series_equal(
        df_out["subida_lat"],
        expected_subida_lat.rename("subida_lat"),
        check_exact=False,
        rtol=1e-12,
    )
    assert_series_equal(
        df_out["bajada_lon"],
        expected_bajada_lon.rename("bajada_lon"),
        check_exact=False,
        rtol=1e-12,
    )
    assert_series_equal(
        df_out["bajada_lat"],
        expected_bajada_lat.rename("bajada_lat"),
        check_exact=False,
        rtol=1e-12,
    )

    assert_frame_equal(df_adatrap, df_before)


def test_project_xy_to_latlon_debug_mode_reports_parse_status_for_problematic_values() -> None:
    """Verifica columnas debug para coma decimal, vacíos, nulos, no numéricos y ceros."""
    df_debug = pd.DataFrame(
        {
            "x": ["349000", "349100,5", "", None, "abc", "0"],
            "y": ["6294500", "6294600,5", "6294700", None, "xyz", "0"],
        }
    )
    df_before = df_debug.copy(deep=True)

    df_out = project_xy_to_latlon(
        df_debug,
        x_col="x",
        y_col="y",
        source_crs=SOURCE_CRS,
        lon_col="lon",
        lat_col="lat",
        decimal_comma=True,
        zero_as_missing=True,
        keep_debug_cols=True,
        drop_input_cols=False,
    )

    expected_debug_cols = {
        "__x_parsed",
        "__y_parsed",
        "__x_status",
        "__y_status",
        "__lon_latlon_status",
    }
    assert expected_debug_cols.issubset(df_out.columns)

    assert df_out["__x_status"].tolist() == [
        "ok_string",
        "ok_string",
        "empty",
        "null",
        "non_numeric",
        "zero_as_missing",
    ]
    assert df_out["__y_status"].tolist() == [
        "ok_string",
        "ok_string",
        "ok_string",
        "null",
        "non_numeric",
        "zero_as_missing",
    ]
    assert df_out["__lon_latlon_status"].tolist() == [
        "transformed",
        "transformed",
        "not_transformed",
        "not_transformed",
        "not_transformed",
        "not_transformed",
    ]

    assert df_out.loc[:1, ["lon", "lat"]].notna().all().all()
    assert df_out.loc[2:, ["lon", "lat"]].isna().all().all()

    assert df_out.loc[1, "__x_parsed"] == pytest.approx(349100.5)
    assert df_out.loc[1, "__y_parsed"] == pytest.approx(6294600.5)

    assert_frame_equal(df_debug, df_before)


def test_project_xy_to_latlon_without_debug_does_not_create_auxiliary_columns() -> None:
    """Verifica que keep_debug_cols=False no agrega columnas auxiliares."""
    df = pd.DataFrame(
        {
            "x": [349000.0],
            "y": [6294500.0],
        }
    )

    df_out = project_xy_to_latlon(
        df,
        x_col="x",
        y_col="y",
        source_crs=SOURCE_CRS,
        lon_col="lon",
        lat_col="lat",
        keep_debug_cols=False,
        drop_input_cols=False,
    )

    debug_cols = [col for col in df_out.columns if col.startswith("__")]
    assert debug_cols == []
    assert {"x", "y", "lon", "lat"}.issubset(df_out.columns)


def test_project_xy_to_latlon_raises_keyerror_when_required_coordinate_columns_are_missing() -> None:
    """Verifica errores explícitos cuando faltan columnas X o Y requeridas."""
    df = pd.DataFrame(
        {
            "x": [349000.0],
            "y": [6294500.0],
        }
    )

    with pytest.raises(KeyError, match="x_col='missing_x'"):
        project_xy_to_latlon(
            df,
            x_col="missing_x",
            y_col="y",
            source_crs=SOURCE_CRS,
            lon_col="lon",
            lat_col="lat",
        )

    with pytest.raises(KeyError, match="y_col='missing_y'"):
        project_xy_to_latlon(
            df,
            x_col="x",
            y_col="missing_y",
            source_crs=SOURCE_CRS,
            lon_col="lon",
            lat_col="lat",
        )