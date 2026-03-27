from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd
from pyproj import Transformer


DEFAULT_TARGET_CRS = "EPSG:4326"


def _normalize_projected_coord_series(
    s: pd.Series,
    *,
    decimal_comma: bool = True,
    zero_as_missing: bool = False,
) -> tuple[pd.Series, pd.Series]:
    """
    Normaliza una serie de coordenadas proyectadas a float.

    Parameters
    ----------
    s : pandas.Series
        Serie de entrada con coordenadas proyectadas.
    decimal_comma : bool, default=True
        Si True, reemplaza comas por punto antes de parsear.
    zero_as_missing : bool, default=False
        Si True, trata valor 0 como faltante.

    Returns
    -------
    values : pandas.Series
        Serie parseada a float.
    status : pandas.Series
        Estado por fila:
        - ok_numeric
        - ok_string
        - empty
        - zero_as_missing
        - non_numeric
        - null
    """
    status = pd.Series(index=s.index, dtype="string")

    # Caso ya numérico
    if pd.api.types.is_numeric_dtype(s):
        values = pd.to_numeric(s, errors="coerce").astype(float)
        status[:] = "ok_numeric"
        status[values.isna()] = "null"

        if zero_as_missing:
            zero_mask = values == 0
            values = values.mask(zero_mask)
            status[zero_mask] = "zero_as_missing"

        return values, status

    # Caso texto / object / string
    s2 = s.astype("string").str.strip()
    status[:] = "ok_string"

    null_mask = s2.isna()
    empty_mask = s2.eq("")

    if decimal_comma:
        s2 = s2.str.replace(",", ".", regex=False)

    values = pd.to_numeric(s2, errors="coerce").astype(float)

    status[null_mask] = "null"
    status[empty_mask] = "empty"
    status[values.isna() & ~null_mask & ~empty_mask] = "non_numeric"

    if zero_as_missing:
        zero_mask = values == 0
        values = values.mask(zero_mask)
        status[zero_mask] = "zero_as_missing"

    return values, status


def project_xy_to_latlon(
    df: pd.DataFrame,
    *,
    x_col: str,
    y_col: str,
    source_crs: str,
    lon_col: str,
    lat_col: str,
    target_crs: str = DEFAULT_TARGET_CRS,
    decimal_comma: bool = True,
    zero_as_missing: bool = False,
    keep_debug_cols: bool = False,
    drop_input_cols: bool = False,
) -> pd.DataFrame:
    """
    Transforma columnas X/Y proyectadas a lon/lat.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame de entrada.
    x_col, y_col : str
        Nombres de columnas X/Y proyectadas.
    source_crs : str
        CRS de origen, por ejemplo "EPSG:5361".
    lon_col, lat_col : str
        Nombres de columnas de salida.
    target_crs : str, default="EPSG:4326"
        CRS de salida.
    decimal_comma : bool, default=True
        Si True, reemplaza comas por punto al parsear strings.
    zero_as_missing : bool, default=False
        Si True, trata 0 como faltante.
    keep_debug_cols : bool, default=False
        Si True, agrega columnas auxiliares con parsed/status.
    drop_input_cols : bool, default=False
        Si True, elimina x_col e y_col después de transformar.

    Returns
    -------
    pandas.DataFrame
        Copia del DataFrame con columnas lon/lat agregadas.
    """
    if x_col not in df.columns:
        raise KeyError(f"No existe la columna x_col={x_col!r} en el DataFrame.")
    if y_col not in df.columns:
        raise KeyError(f"No existe la columna y_col={y_col!r} en el DataFrame.")

    work = df.copy()

    x_vals, x_status = _normalize_projected_coord_series(
        work[x_col],
        decimal_comma=decimal_comma,
        zero_as_missing=zero_as_missing,
    )
    y_vals, y_status = _normalize_projected_coord_series(
        work[y_col],
        decimal_comma=decimal_comma,
        zero_as_missing=zero_as_missing,
    )

    valid_mask = x_vals.notna() & y_vals.notna()

    work[lon_col] = np.nan
    work[lat_col] = np.nan

    if valid_mask.any():
        transformer = Transformer.from_crs(
            source_crs,
            target_crs,
            always_xy=True,
        )
        lon, lat = transformer.transform(
            x_vals.loc[valid_mask].to_numpy(),
            y_vals.loc[valid_mask].to_numpy(),
        )
        work.loc[valid_mask, lon_col] = lon
        work.loc[valid_mask, lat_col] = lat

    if keep_debug_cols:
        work[f"__{x_col}_parsed"] = x_vals
        work[f"__{y_col}_parsed"] = y_vals
        work[f"__{x_col}_status"] = x_status
        work[f"__{y_col}_status"] = y_status
        work[f"__{lon_col}_latlon_status"] = pd.Series(
            np.where(valid_mask, "transformed", "not_transformed"),
            index=work.index,
            dtype="string",
        )

    if drop_input_cols:
        work = work.drop(columns=[x_col, y_col])

    return work