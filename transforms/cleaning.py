# pylondrina/transforms/cleaning.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional, Sequence

from ..datasets import TripDataset
from ..reports import OperationReport


@dataclass(frozen=True)
class CleanOptions:
    """
    Opciones para limpiar un TripDataset mediante reglas explícitas de eliminación de filas (solo drops).
    La operación "clean" se usa para remover registros que el usuario considera inválidos/no deseados para análisis posteriores. 

    Attributes
    ----------
    drop_rows_with_nulls_in_required_fields:
        Si True, elimina filas con valores nulos/NaN en campos requeridos (según schema efectivo).
    drop_rows_with_nulls_in_fields:
        Lista de nombres de campos (no necesariamente requeridos) para los cuales, si el valor es
        nulo/NaN en una fila, dicha fila se elimina. Si es None o vacío, no aplica.
    drop_rows_with_invalid_latlon:
        Si True, elimina filas con lat/lon fuera de rango o con lat/lon nulos en campos esperados.
    drop_rows_with_invalid_h3:
        Si True, elimina filas con índices H3 inválidos/no parseables en campos de H3 esperados.
    drop_rows_with_origin_after_destination:
        Si True, elimina filas donde el tiempo de origen sea posterior al tiempo de destino.
    drop_duplicates:
        Si True, elimina filas duplicadas según `duplicates_subset` 
    duplicates_subset:
        Subconjunto de columnas para identificar duplicados. Si None y `drop_duplicates=True`,
        se usa un conjunto de columnas requeridas que el sistema tenga por defecto
    drop_rows_by_categorical_values:
        Mapeo campo categórico -> lista de valores a eliminar. Si una fila tiene en ese campo un
        valor presente en la lista, la fila se elimina. Útil para eliminar 'unknown', NaN ya
        recodificados a 'unknown', u otras categorías no deseadas. Si es vacio o {}, no aplica.
        Ejemplo: {"purpose": ["unknown"], "mode": ["unknown", "other"]}.
    """

    drop_rows_with_nulls_in_required_fields: bool = False
    drop_rows_with_nulls_in_fields: Optional[Sequence[str]] = None

    drop_rows_with_invalid_latlon: bool = False
    drop_rows_with_invalid_h3: bool = False
    drop_rows_with_origin_after_destination: bool = False

    drop_duplicates: bool = False
    duplicates_subset: Optional[Sequence[str]] = None

    drop_rows_by_categorical_values: Optional[Mapping[str, Sequence[Any]]] = None


def clean_trips(
    trips: TripDataset,
    *,
    options: Optional[CleanOptions] = None,
) -> tuple[TripDataset, OperationReport]:
    """
    Limpia un TripDataset eliminando filas según reglas explícitas (solo drops), y registra trazabilidad.

    Parameters
    ----------
    trips:
        Dataset de viajes en formato Golondrina.
    options:
        Reglas de limpieza. Si None, se asume configuración por defecto (sin drops).

    Returns
    -------
    (TripDataset, OperationReport)
        - Un nuevo TripDataset con filas eliminadas (si corresponde).
        - Un OperationReport con issues (warnings/errors si aplica), summary mínimo y parameters efectivos.
    """
    raise NotImplementedError


def build_clean_summary(
    *,
    rows_in: int,
    rows_out: int,
    dropped_total: int,
    dropped_by_rule: Mapping[str, int],
) -> dict:
    """
    Construye un summary pequeño y estable para el reporte/evento de `clean_trips`.

    Parameters
    ----------
    rows_in:
        Número de filas antes de limpiar.
    rows_out:
        Número de filas después de limpiar.
    dropped_total:
        Total de filas eliminadas (rows_in - rows_out).
    dropped_by_rule:
        Conteo por regla aplicada. Las claves deben ser nombres estables (p. ej.,
        "nulls_required", "nulls_fields", "invalid_latlon", "invalid_h3", "origin_after_destination",
        "duplicates", "categorical_values").

    Returns
    -------
    dict
        Diccionario serializable (JSON) con el resumen mínimo de la operación.
    """
    raise NotImplementedError
