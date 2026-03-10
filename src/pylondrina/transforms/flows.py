# -------------------------
# file: pylondrina/transforms/flows.py
# -------------------------
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence, Tuple, Literal

from pylondrina.datasets import FlowDataset, TripDataset
from pylondrina.reports import OperationReport


TimeAggregation = Literal["none", "hour", "day", "week", "month"]
"""
Granularidad temporal para construir flujos.

- "none": no particiona temporalmente (un flujo por OD y segmentación).
- "hour"/"day"/"week"/"month": agrega además por “bin” temporal según `time_basis`.
"""

TimeBasis = Literal["origin", "destination"]
"""
Campo temporal base para ubicar el viaje en el bin cuando `time_aggregation != "none"`.

- "origin": usa `origin_time_utc` para asignar el viaje al bin.
- "destination": usa `destination_time_utc` para asignar el viaje al bin.
"""


@dataclass(frozen=True)
class FlowBuildOptions:
    """
    Opciones para construir flujos OD a partir de un TripDataset.

    Parameters / Attributes
    -----------------------
    h3_resolution:
        Resolución H3 objetivo de agregación para los flujos.

    group_by:
        Campos adicionales por los que se segmenta el flujo (p. ej. `mode`, `purpose`, etc.).
        Si es None, se agregan flujos solo por OD (y por tiempo si aplica).

    time_aggregation:
        Granularidad temporal de agregación. Si es None, no se agrega dimensión temporal.

    time_basis:
        Campo temporal base para el binning: `origin` (inicio) o `destination` (término).

    min_trips_per_flow:
        Umbral mínimo de viajes para conservar un flujo (filtra flujos muy pequeños).

    keep_flow_to_trips:
        Si True, construye `flow_to_trips` (correspondencia flujo→viajes). Si False, omite
        esa tabla para acelerar la construcción.

    require_validated:
        Si True (default), exige que el TripDataset esté validado para construir flujos.
        Si False, permite construir para debugging/exploración (registrando issue).
    """

    h3_resolution: int = 8
    group_by: Optional[Sequence[str]] = None

    time_aggregation: Optional[TimeAggregation] = None
    time_basis: TimeBasis = "origin"

    min_trips_per_flow: int = 1
    keep_flow_to_trips: bool = False

    require_validated: bool = True


def build_flows(
    trips: TripDataset,
    *,
    options: Optional[FlowBuildOptions] = None,
    strict: bool = False,
    max_issues: int = 1000,
) -> Tuple[FlowDataset, OperationReport]:
    """
    Construye flujos OD (FlowDataset) a partir de un TripDataset Golondrina.

    En v1.1:
    - Por defecto requiere trips validados (`options.require_validated=True`).
    - Agrega por OD en resolución H3 objetivo (`options.h3_resolution`), y opcionalmente segmenta
      por campos (`group_by`) y/o por tiempo (`time_aggregation` + `time_basis`).
    - Puede filtrar flujos con pocos viajes (`min_trips_per_flow`).
    - Puede incluir correspondencia flujo→viajes (`keep_flow_to_trips`) de manera opcional.

    Parameters
    ----------
    trips:
        TripDataset de entrada (viajes en formato Golondrina).

    options:
        Opciones de construcción. Si es None, se usa `FlowBuildOptions()`.

    strict:
        Si True, fallas de configuración o precondiciones se elevan a error; si False,
        se registran como issues y se intenta degradar cuando sea posible.

    max_issues:
        Límite máximo de issues acumulados en el reporte.

    Returns
    -------
    (flow_dataset, report):
        flow_dataset: FlowDataset con `flows` y, opcionalmente, `flow_to_trips`.
        report: OperationReport con issues y summary serializable.
    """
    raise NotImplementedError
