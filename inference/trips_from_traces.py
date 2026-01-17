# -------------------------
# file: pylondrina/inference/trips_from_traces.py
# -------------------------
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, Literal

import pandas as pd

from ..datasets import TraceDataset, TripDataset
from ..reports import InferenceReport
from ..schema import TripSchema, TraceSchema


@dataclass
class InferenceOptions:
    """
    Opciones para inferencia de viajes desde trazas (v1).

    En v1, la inferencia se basa exclusivamente en pares de puntos
    consecutivos de un mismo usuario/dispositivo. Cada par define
    un viaje origen-destino potencial.

    Attributes
    ----------
    max_time_delta_s : float, optional
        Umbral máximo de tiempo (en segundos) permitido entre dos puntos
        consecutivos para considerarlos parte del mismo viaje.
        Si es None, no se aplica restricción temporal.
    drop_invalid : bool, default=True
        Si True, los viajes que no cumplan las condiciones mínimas
        (por ejemplo, timestamps inválidos o puntos faltantes)
        se descartan y se reportan como issues.
    """
    max_time_delta_s: Optional[float] = None
    drop_invalid: bool = True


def infer_trips_from_traces(
    traces: TraceDataset,
    trace_schema: TraceSchema,
    trip_schema: TripSchema,
    *,
    options: Optional[InferenceOptions] = None,
    source_name: Optional[str] = None,
    provenance: Optional[Dict[str, Any]] = None,
) -> Tuple[TripDataset, InferenceReport]:
    """
    Infiere viajes origen-destino desde un TraceDataset (v1).

    En v1, la inferencia considera exclusivamente pares de puntos
    consecutivos de un mismo usuario/dispositivo. Cada par de puntos
    define un viaje potencial con:
    - origen: primer punto,
    - destino: segundo punto,
    - tiempos asociados a cada evento.

    Opcionalmente, puede aplicarse una restricción temporal máxima
    entre puntos consecutivos.

    Parameters
    ----------
    traces : TraceDataset
        Dataset de trazas.
    trace_schema : TraceSchema
        Esquema que define los roles de columnas en las trazas
        (user_id, timestamp, coordenadas).
    trip_schema : TripSchema
        Esquema Golondrina de salida para los viajes inferidos.
    options : InferenceOptions, optional
        Opciones de inferencia. Si es None, se usan los defaults.
    source_name : str, optional
        Identificador de la fuente de datos, usado para metadatos
        y trazabilidad.
    provenance : dict[str, Any], optional
        Información adicional de procedencia a registrar en el
        TripDataset resultante.

    Returns
    -------
    trip_dataset : TripDataset
        Dataset de viajes inferidos en formato Golondrina.
    report : InferenceReport
        Reporte de inferencia con issues detectados, resumen agregado
        y parámetros efectivos utilizados.

    Notes
    -----
    - Esta inferencia no intenta reconstruir trayectorias continuas.
    - Métodos más avanzados (detección de paradas, clustering,
      inferencia multi-etapa) se consideran trabajo futuro.
    """
    raise NotImplementedError