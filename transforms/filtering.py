# -------------------------
# file: pylondrina/transforms/filtering.py
# -------------------------
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Mapping, Sequence, Literal

from pylondrina.datasets import TripDataset
from pylondrina.types import FieldName, DomainValue

TimePredicate = Literal["starts_within", "ends_within", "contains", "overlaps"]
"""
Criterio para filtrar viajes respecto a un intervalo temporal [start, end).

- "starts_within": el viaje inicia dentro del rango (origin_time ∈ [start, end)).
- "ends_within": el viaje termina dentro del rango (dest_time ∈ [start, end)).
- "contains": el viaje ocurre completamente dentro del rango
  ([origin_time, dest_time] ⊆ [start, end)).
- "overlaps": el viaje se solapa con el rango
  ([origin_time, dest_time] ∩ [start, end) ≠ ∅).

Ejemplo (rango 07:00-08:00):
- 06:50→07:10: ends_within/overlaps = True; starts_within/contains = False.
- 07:50→08:20: starts_within/overlaps = True; ends_within/contains = False.
"""

@dataclass(frozen=True)
class TimeFilter:
    """
    Especificación de filtrado temporal para viajes.

    Parameters
    ----------
    start
        Timestamp de inicio del rango [start, end).
    end
        Timestamp de término del rango [start, end).
    predicate
        Semántica del filtrado temporal.
    origin_field
        Campo temporal de origen (si es None, se resuelve desde el esquema).
    dest_field
        Campo temporal de destino (si es None, se resuelve desde el esquema).
    """
    start: str
    end: str
    predicate: TimePredicate = "starts_within"
    origin_field: FieldName | None = None
    dest_field: FieldName | None = None

def filter_trips(
    trips: TripDataset,
    *,
    where: Mapping[FieldName, Any] | None = None,
    time: TimeFilter | None = None,
    spatial: Mapping[str, Any] | None = None,
    strict: bool = False,
) -> TripDataset:
    """
    Filtra un conjunto de viajes en formato Golondrina en base a condiciones por campos,
    restricciones temporales y/o restricciones espaciales.

    Parameters
    ----------
    trips
        Conjunto de viajes en formato Golondrina.
    where
        Condiciones por campo. La estructura es intencionalmente flexible para soportar
        igualdad, pertenencia, rangos y predicados simples (a definir en la implementación).
        Ejemplos típicos:
        - ``{"mode": "bus"}``
        - ``{"purpose": {"in": ["work", "education"]}}``
        - ``{"income": {"gte": 500000}}``
    time_range
        Rango temporal (inicio, fin) como strings en formato ISO-8601 o el formato adoptado
        por el esquema. Define el intervalo de viajes a conservar.
    spatial
        Especificación espacial. Se admite una de las siguientes variantes:

        **A) H3**
            ``{"h3": {"field": "<campo_h3>", "cells": [<h3>, ...]}}``
            - ``field``: nombre del campo H3 a usar (p.ej. ``"origin_h3"`` o ``"dest_h3"``)
            - ``cells``: lista/colección de celdas H3 permitidas

        **B) Bounding box**
            ``{"bbox": {"fields": ("<lon_field>", "<lat_field>"), "bounds": (minx, miny, maxx, maxy)}}``
            - ``fields``: tupla (lon_field, lat_field) para localizar puntos
            - ``bounds``: (minx, miny, maxx, maxy)

        **C) Polygon**
            ``{"polygon": {"fields": ("<lon_field>", "<lat_field>"), "geometry": <geom>, "predicate": "within"}}``
            - ``geometry``: objeto geométrico (p.ej. shapely), tipo flexible
            - ``predicate``: relación espacial a usar (por defecto ``"within"``)

        Nota: La implementación puede requerir dependencias opcionales para `polygon`.
    strict
        Si es True, inconsistencias (p.ej. campos inexistentes o tipos incompatibles)
        pueden gatillar excepciones. Si es False, se reportan/omiten según política
        definida en la implementación.

    Returns
    -------
    TripDataset
        Nuevo conjunto de viajes filtrado.
    """
    raise NotImplementedError


def filter_by_h3_cells(
    trips: TripDataset,
    *,
    field: FieldName,
    cells: Sequence[str],
    strict: bool = False,
) -> TripDataset:
    """
    Filtra viajes manteniendo aquellos cuyo campo H3 (origen o destino) pertenece a un
    conjunto de celdas H3 permitido.

    Parameters
    ----------
    trips
        Conjunto de viajes en formato Golondrina.
    field
        Campo H3 a utilizar (p.ej. ``"origin_h3"``, ``"dest_h3"``).
    cells
        Colección de celdas H3 permitidas.
    strict
        Política de manejo ante inconsistencias.

    Returns
    -------
    TripDataset
        Dataset filtrado.
    """
    raise NotImplementedError


def filter_by_bbox(
    trips: TripDataset,
    *,
    bounds: tuple[float, float, float, float],
    fields: tuple[FieldName, FieldName] = ("origin_lon", "origin_lat"),
    strict: bool = False,
) -> TripDataset:
    """
    Filtra viajes por un bounding box (minx, miny, maxx, maxy) usando campos de
    longitud/latitud.

    Parameters
    ----------
    trips
        Conjunto de viajes en formato Golondrina.
    bounds
        Bounding box (minx, miny, maxx, maxy).
    fields
        Tupla (lon_field, lat_field) que indica qué campos usar para ubicar el punto
        a evaluar (por defecto se asume el origen).
    strict
        Política de manejo ante inconsistencias.

    Returns
    -------
    TripDataset
        Dataset filtrado.
    """
    raise NotImplementedError


def filter_by_polygon(
    trips: TripDataset,
    *,
    geometry: Any,
    fields: tuple[FieldName, FieldName] = ("origin_lon", "origin_lat"),
    predicate: str = "within",
    strict: bool = False,
) -> TripDataset:
    """
    Filtra viajes por una geometría poligonal.

    Parameters
    ----------
    trips
        Conjunto de viajes en formato Golondrina.
    geometry
        Geometría del polígono (tipo flexible). La implementación puede requerir una
        dependencia opcional (p.ej. shapely).
    fields
        Tupla (lon_field, lat_field) usada para obtener el punto a filtrar.
    predicate
        Relación espacial a aplicar. Valores típicos: ``"within"``, ``"intersects"``.
    strict
        Política de manejo ante inconsistencias.

    Returns
    -------
    TripDataset
        Dataset filtrado.
    """
    raise NotImplementedError


def filter_by_domain_values(
    trips: TripDataset,
    *,
    field: FieldName,
    values: Sequence[DomainValue],
    strict: bool = False,
) -> TripDataset:
    """
    Filtra viajes por valores categóricos permitidos (dominio de valores) de un campo.

    Parameters
    ----------
    trips
        Conjunto de viajes en formato Golondrina.
    field
        Campo categórico a filtrar (p.ej. ``"purpose"``, ``"mode"``).
    values
        Valores permitidos del dominio.
    strict
        Política de manejo ante inconsistencias.

    Returns
    -------
    TripDataset
        Dataset filtrado.
    """
    raise NotImplementedError

def filter_by_time_range(
    trips: TripDataset,
    *,
    time_filter: TimeFilter,
    keep_metadata: bool = True,
) -> TripDataset:
    """
    Aplica un filtro temporal al conjunto de viajes según la especificación dada.

    Parameters
    ----------
    trips
        Conjunto de viajes a filtrar.
    time_filter
        Especificación del filtro temporal (rango + semántica).
    keep_metadata
        Si True, conserva y actualiza metadatos en el dataset resultante.

    Returns
    -------
    TripDataset
        Nuevo dataset filtrado (no modifica el objeto original).
    """
    raise NotImplementedError