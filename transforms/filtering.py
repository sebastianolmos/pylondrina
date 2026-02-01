# pylondrina/transforms/filtering.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Literal, Mapping, Optional, Sequence, Tuple, Union

from ..datasets import TripDataset
from ..reports import OperationReport


# Tipos (contrato v1.1)
BBox = Tuple[float, float, float, float]
"""Bounding box (min_lon, min_lat, max_lon, max_lat)."""

LonLat = Tuple[float, float]
"""Coordenada (lon, lat)."""

Polygon = Sequence[LonLat]
"""Polígono como secuencia de vértices (lon, lat)."""

TimePredicate = Literal["starts_within", "ends_within", "contains", "overlaps"]
"""
Predicado temporal entre el intervalo del viaje y el intervalo [start, end].
El intervalo del viaje se define SIEMPRE como: [origin_time_utc, destination_time_utc]

- "starts_within": origin_time_utc cae dentro de [start, end].
- "ends_within": destination_time_utc cae dentro de [start, end].
- "contains": [origin_time_utc, destination_time_utc] contiene completamente [start, end].
- "overlaps": los intervalos se intersectan.
"""

SpatialPredicate = Literal["origin", "destination", "both", "either"]
"""
Predicado espacial para decidir sobre qué extremo(s) aplicar un filtro espacial.

- "origin": aplica el filtro espacial al punto de origen.
- "destination": aplica el filtro espacial al punto de destino.
- "both": requiere que origen Y destino cumplan el filtro (AND).
- "either": requiere que origen O destino cumpla el filtro (OR).
"""

WhereOp = Literal["eq", "in", "ne", "not_in", "is_null", "not_null", "gt", "gte", "lt", "lte", "between",]
"""
Operadores soportados por `FilterOptions.where` (v1.1).

- eq / in: igualdad y pertenencia (también pueden expresarse implícitamente).
- ne / not_in: desigualdad y no-pertenencia.
- is_null / not_null: filtros por nulidad (aplican a cualquier campo).
- gt / gte / lt / lte / between: comparaciones numéricas (int/float).
"""

WhereValue = Union[Any, Sequence[Any], Mapping[WhereOp, Any]]
"""
Valor permitido para un filtro en `where` (v1.1).

Reglas de interpretación
-----------------------
1) Escalar: {"user_id": 123}  -> equivale a {"user_id": {"eq": 123}}

2) Secuencia (list): {"mode": ["bus", "metro"]} -> equivale a {"mode": {"in": ["bus", "metro"]}}

3) Dict operador->valor:
   {
     "purpose": {"ne": "unknown"},
     "mode": {"not_in": ["car"]},
     "duration_min": {"gte": 5},
     "distance_km": {"between": (0.5, 20.0)},
     "stage_id": {"not_null": True},
   }

Compatibilidad por tipo (v1.1)
------------------------------
- eq/in/ne/not_in: aplicables a cualquier campo; para campos categóricos se recomienda usar valores del dominio.
- is_null/not_null: aplicables a cualquier campo.
- gt/gte/lt/lte/between: destinados a campos numéricos (int/float).
"""

WhereClause = Mapping[str, WhereValue]
"""
Cláusula declarativa de filtros por campos (AND entre campos).

Ejemplos:
where: WhereClause = {
    "user_id": 123,                          # eq implícito: user_id == 123
    "purpose": "work",                       # eq implícito: purpose == "work"
    "user_id": [101, 202, 303],              # in implícito

    "purpose": {"ne": "unknown"},            # purpose != "unknown"
    "mode": {"not_in": ["car", "taxi"]},     # mode NOT IN {"car","taxi"}
    "stage_id": {"not_null": True},          # stage_id is not null

    "distance_km": {"between": (0.5, 20.0)},   # 0.5 <= distance_km <= 20.0
    "duration_min": {"gte": 5},                # duration_min >= 5
    "speed_kmh": {"lt": 120},                  # speed_kmh < 120

    "mode": {"in": ["bus", "metro"]},        # in explícito (redundante pero válido)
    "purpose": {"ne": "unknown"},            # ne explícito
    "fare_type": {"is_null": True},          # is_null explícito
    "distance_km": {"lte": 30.0},            # lte explícito
}
"""



@dataclass(frozen=True)
class TimeFilter:
    """
    Filtro temporal sobre viajes, comparando el intervalo [origin_time_utc, destination_time_utc]
    contra el intervalo [start, end] usando un predicado.

    Attributes
    ----------
    start:
        Inicio del intervalo temporal (inclusive).
    end:
        Fin del intervalo temporal (inclusive).
    predicate:
        Relación entre intervalos (ver TimePredicate).
    """

    start: datetime
    end: datetime
    predicate: TimePredicate = "overlaps"


@dataclass(frozen=True)
class FilterOptions:
    """
    Opciones para filtrar un TripDataset por criterios de atributos (`where`), tiempo y/o espacio.

    Reglas de combinación (v1.1)
    ----------------------------
    - Los criterios presentes se combinan como intersección (AND).
    - Dentro de `where`, cada key también se combina por AND.

    Parámetros / Atributos
    ----------------------
    where : Optional[WhereClause]
        Filtros declarativos por campos. `where` es un diccionario `{campo -> condición}`.
        La condición puede escribirse como:
        - Escalar: equivale a `{"eq": valor}`
        - Secuencia: equivale a `{"in": [..]}`
        - Dict operador->valor: permite `eq, in, ne, not_in, is_null, not_null, gt, gte, lt, lte, between`.
    time : Optional[TimeFilter]
        Filtro temporal sobre el intervalo del viaje definido por [origin_time_utc, destination_time_utc]
    bbox : Optional[BBox]
        Filtro espacial por bounding box (min_lon, min_lat, max_lon, max_lat).
    polygon : Optional[Polygon]
        Filtro espacial por polígono lon/lat (secuencia de vértices).
    h3_cells : Optional[Iterable[str]]
        Filtro espacial por conjunto de celdas H3 permitidas.
    spatial_predicate : SpatialPredicate
        Sobre qué extremo(s) del viaje aplica el filtro espacial: "origin", "destination", "both", "either".
    origin_h3_field : str
        Nombre del campo H3 de origen (para filtros por H3).
    destination_h3_field : str
        Nombre del campo H3 de destino (para filtros por H3).
    keep_metadata : bool
        Si True, agrega un evento de filtrado en `TripDataset.metadata["events"]` del dataset resultante.
    strict : bool
        Si True, configuraciones inválidas pueden escalar; si False, se degradan a issues.
    """

    where: Optional["WhereClause"] = None
    time: Optional["TimeFilter"] = None

    bbox: Optional["BBox"] = None
    polygon: Optional["Polygon"] = None
    h3_cells: Optional[Iterable[str]] = None

    spatial_predicate: "SpatialPredicate" = "origin"
    origin_h3_field: str = "origin_h3"
    destination_h3_field: str = "destination_h3"

    keep_metadata: bool = True
    strict: bool = False

# ---------------------------------------------------------------------
# API pública (v1.1)
# ---------------------------------------------------------------------

def filter_trips(
    trips: TripDataset,
    *,
    options: Optional[FilterOptions] = None,
    max_issues: int = 1000,
) -> Tuple[TripDataset, OperationReport]:
    """
    Filtra un TripDataset combinando criterios por atributos (where), tiempo y/o espacio.

    Parameters
    ----------
    trips:
        Dataset de entrada.
    options:
        Opciones del filtro. Si es None, no aplica filtros y retorna el dataset (con reporte/event si procede).
    max_issues:
        Límite máximo de issues a registrar (para evitar explosión).

    Returns
    -------
    (TripDataset, OperationReport)
        Dataset filtrado + reporte de la operación. Si `options.keep_metadata=True`,
        agrega un evento a `TripDataset.metadata['events']` en el dataset resultante.

    Notes
    -----
    - En v1.1, esta operación NO cambia el estado de validación del dataset; lo preserva.
    - Los filtros inválidos pueden omitirse con issue (strict=False) o escalar (strict=True).
    """
    raise NotImplementedError


def build_filter_summary(
    *,
    n_rows_in: int,
    n_rows_out: int,
    filters_applied: List[str],
) -> Dict[str, Any]:
    """
    Construye el summary mínimo (serializable y estable) para una operación de filtrado.

    Parameters
    ----------
    n_rows_in:
        Número de filas antes del filtrado.
    n_rows_out:
        Número de filas después del filtrado.
    filters_applied:
        Lista de etiquetas de filtros efectivamente aplicados (p. ej. ["where", "time", "bbox"]).

    Returns
    -------
    dict
        Summary mínimo del filtrado, pensado para report/event.
    """
    raise NotImplementedError


# ---------------------------------------------------------------------
# Helpers internos (no-API): factorizar implementación
# ---------------------------------------------------------------------

def _apply_where_filter(
    trips: TripDataset,
    *,
    where: WhereClause,
    report: OperationReport,
    strict: bool,
    max_issues: int,
) -> TripDataset:
    """
    Aplica el filtrado por `where` (AND entre campos) y registra issues en `report`.
    """
    raise NotImplementedError


def _apply_time_filter(
    trips: TripDataset,
    *,
    time: TimeFilter,
    report: OperationReport,
    strict: bool,
    max_issues: int,
) -> TripDataset:
    """
    Aplica el filtrado temporal sobre el intervalo del viaje [origin_time_utc, destination_time_utc].

    """
    raise NotImplementedError


def _apply_spatial_filter(
    trips: TripDataset,
    *,
    bbox: Optional[BBox],
    polygon: Optional[Polygon],
    h3_cells: Optional[Iterable[str]],
    spatial_predicate: SpatialPredicate,
    origin_h3_field: str,
    destination_h3_field: str,
    report: OperationReport,
    strict: bool,
    max_issues: int,
) -> TripDataset:
    """
    Aplica filtrado espacial (bbox/polygon/h3_cells) según `spatial_predicate`.
    """
    raise NotImplementedError
