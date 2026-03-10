from __future__ import annotations

from typing import Any, Dict, Mapping, Optional

import pandas as pd


FieldCorrections = Mapping[str, str]
"""
Mapeo de corrección de nombres de columnas.

- key: nombre actual de la columna en df
- value: nombre canónico objetivo (Golondrina o intermedio definido por el usuario)
"""


ValueCorrections = Mapping[str, Mapping[Any, Any]]
"""
Mapeo de corrección de valores categóricos por campo.

Estructura:
- key: nombre de campo canónico (columna en df)
- value: dict que mapea valor_origen -> valor_canónico
"""


def apply_field_corrections(
    df: pd.DataFrame,
    corrections: FieldCorrections,
    *,
    inplace: bool = False,
) -> pd.DataFrame:
    """
    Aplica correcciones de nombres de columnas (renombrado) sobre un DataFrame.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame de entrada.
    corrections : FieldCorrections
        Mapeo de columnas a renombrar.
    inplace : bool, default=False
        Si True, modifica el DataFrame in-place; si False, retorna una copia renombrada.

    Returns
    -------
    pandas.DataFrame
        DataFrame con columnas renombradas según `corrections`.
    """
    raise NotImplementedError


def apply_value_corrections(
    df: pd.DataFrame,
    corrections: ValueCorrections,
    *,
    inplace: bool = False,
) -> pd.DataFrame:
    """
    Aplica correcciones de valores categóricos (recode) por campo.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame de entrada.
    corrections : ValueCorrections
        Diccionario por campo con el mapping valor_origen -> valor_canónico.
    inplace : bool, default=False
        Si True, modifica el DataFrame in-place; si False, retorna una copia corregida.

    Returns
    -------
    pandas.DataFrame
        DataFrame con valores categóricos corregidos en los campos indicados.
    """
    raise NotImplementedError
