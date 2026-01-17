# -------------------------
# file: pylondrina/transforms/traces.py
# -------------------------
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Sequence, Tuple, Literal

import pandas as pd

from ..datasets import TraceDataset
from ..reports import Issue, ConsistencyReport
from ..schema import TraceSchema


@dataclass
class TraceConsistencyOptions:
    """
    Opciones para validación de consistencia de trazas (v1).

    En v1, la validación de consistencia se limita deliberadamente a:
    - consistencia temporal mínima (monotonicidad de timestamps por usuario/dispositivo),
    - validación espacial mínima (rangos plausibles de coordenadas según el CRS).

    Attributes
    ----------
    enforce_monotonic_time_per_user : bool, default=True
        Verifica que, para cada usuario/dispositivo, los timestamps estén en orden
        no decreciente. Este check detecta registros fuera de orden que pueden
        invalidar inferencias o métricas derivadas.
    validate_coord_bounds : bool, default=True
        Verifica rangos plausibles de coordenadas para detectar errores evidentes
        de mapeo de columnas o CRS. Por ejemplo, si `schema.crs` indica EPSG:4326,
        se espera lon en [-180, 180] y lat en [-90, 90].
    """
    enforce_monotonic_time_per_user: bool = True
    validate_coord_bounds: bool = True


def validate_trace_consistency(
    traces: TraceDataset,
    schema: TraceSchema,
    *,
    options: Optional[TraceConsistencyOptions] = None,
) -> ConsistencyReport:
    """
    Valida consistencia mínima de un TraceDataset (v1).

    Esta función diagnostica problemas básicos que comprometen la interpretabilidad
    de las trazas y la confiabilidad de procesos posteriores (por ejemplo,
    inferencia de viajes). No modifica el dataset; solo reporta.

    Checks incluidos (v1)
    ---------------------
    Temporal
        - Monotonicidad temporal por usuario/dispositivo: para cada `user_id`,
          los timestamps deben ser no decrecientes.
    Spatial
        - Validación de rangos plausibles de coordenadas según el CRS indicado
          en `schema.crs` (cuando aplica).

    Parameters
    ----------
    traces : TraceDataset
        Dataset de trazas a validar.
    schema : TraceSchema
        Esquema de trazas que define campos mínimos y contexto. En particular,
        se utiliza para identificar los roles de columnas (user_id, timestamp,
        lon, lat) y el CRS/timezone cuando corresponda.
    options : TraceConsistencyOptions, optional
        Opciones de validación. Si es None, se usan los defaults.

    Returns
    -------
    ConsistencyReport
        Reporte con:
        - `issues`: lista de problemas detectados,
        - `summary`: resumen agregado (conteos y usuarios afectados),
        - `parameters`: parámetros efectivos usados (incluyendo defaults aplicados).

    Notes
    -----
    - Si el dataset no está ordenado por usuario y tiempo, la monotonicidad puede
      verse afectada. En v1, se recomienda documentar como precondición que el
      dataset venga ordenado por (user_id, timestamp), o bien reportar la falta
      de monotonicidad como issue para diagnóstico.
    """
    raise NotImplementedError

def compute_trace_stats(
    traces: TraceDataset,
    schema: TraceSchema,
    *,
    per_user: bool = True,
    include_sampling_intervals: bool = False,
) -> Dict[str, Any]:
    """
    Calcula estadísticas descriptivas básicas de un TraceDataset (v1).

    Parameters
    ----------
    traces : TraceDataset
        Dataset de trazas.
    schema : TraceSchema
        Esquema de trazas que define roles de columnas (user_id, timestamp, etc.) y
        contexto de interpretación temporal (por ejemplo, timezone si aplica).
    per_user : bool, default=True
        Si True, incluye métricas agregadas por usuario (puntos por usuario y sus percentiles).
        Si False, retorna solo métricas globales.
    include_sampling_intervals : bool, default=False
        Si True, calcula estadísticos de los intervalos temporales entre puntos consecutivos
        (por usuario). Requiere que los timestamps sean comparables y que el dataset sea
        interpretable temporalmente.

    Returns
    -------
    dict[str, Any]
        Diccionario serializable con métricas descriptivas.

        Estructura sugerida (no estricta)
        --------------------------------
        - "n_points": int
        - "n_users": int
        - "time_coverage": {"start": Any, "end": Any} (si aplica)
        - "points_per_user": {"p50": float, "p90": float, "p99": float, "min": int, "max": int}
          (si `per_user=True`)
        - "sampling_interval_s": {"p50": float, "p90": float, "p99": float, "min": float, "max": float}
          (si `include_sampling_intervals=True` y es calculable)

    Notes
    -----
    - Esta función no modifica el dataset.
    - Los resultados están pensados para ser incorporados como metadatos (por ejemplo, en
      reportes o archivos companion JSON) y para reportar propiedades del dataset en el informe.
    """
    raise NotImplementedError