# -------------------------
# file: pylondrina/inference/infer_trips.py
# -------------------------
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Tuple

import pandas as pd

from ..datasets import TraceDataset, TripDataset
from ..reports import InferenceReport
from ..schema import TripSchema


@dataclass(frozen=True)
class InferTripsOptions:
    """
    Opciones para inferir viajes (trips) a partir de trazas/trayectorias (traces).

    En v1.1, el método base de inferencia considera pares de puntos consecutivos
    de un mismo usuario/dispositivo, generando un viaje OD por cada par.

    Attributes
    ----------
    max_time_delta_s : float, optional
        Umbral máximo (segundos) permitido entre dos puntos consecutivos para
        considerarlos parte del mismo viaje. Si es None, no se aplica restricción temporal.
    drop_invalid : bool, default=True
        Si True, descarta pares que no puedan formar un viaje válido (por ejemplo, timestamps faltantes o no parseables) y reporta issues.
        Si False, intenta conservarlos como viajes incompletos cuando sea posible, reportando issues de calidad .
    require_validated_traces: bool, default=True
        Si True, requiere que el dataset de trazas esté validado antes de inferir viajes. Si es False, permite inferir directamente (para debugging).
        Busca evidencia de validación en traces.metadata
    """
    max_time_delta_s: Optional[float] = None
    drop_invalid: bool = True
    require_validated_traces: bool = True


def infer_trips_from_traces(
    traces: TraceDataset,
    *,
    options: Optional[InferTripsOptions] = None,
    trip_schema: Optional[TripSchema] = None,
    value_correspondence: Optional[Mapping[str, Mapping[Any, Any]]] = None,
    correspondence_context: Optional[Dict[str, Any]] = None,
    strict: bool = True,
) -> Tuple[TripDataset, InferenceReport]:
    """
    Infiere viajes (OD) desde un TraceDataset.

    La inferencia v1.1 se define como un proceso que toma puntos
    consecutivos por usuario/dispositivo y produce viajes OD en formato Golondrina.

    Además, se permite (opcionalmente) normalizar dominios de valores para campos
    categóricos que se propaguen al viaje (por ejemplo, categorías de POIs usadas
    para origen/destino cuando el preprocesamiento de traces las materializa).

    Parameters
    ----------
    traces : TraceDataset
        Dataset de trazas en formato Golondrina (campos mínimos estandarizados).
    options : InferTripsOptions, optional
        Opciones del algoritmo de inferencia.
    trip_schema : TripSchema, optional
        Esquema Golondrina de salida. Si es None, se usa el esquema por defecto del módulo para trips (v1.1).
    value_correspondence : Mapping[str, Mapping[Any, Any]], optional
        Correspondencias de valores categóricos a aplicar durante la inferencia
        (o al materializar atributos categóricos del viaje), con estructura: {field_name: {raw_value: normalized_value}}.
    correspondence_context : dict, optional
        Contexto auxiliar para la aplicación/registro de correspondencias
        (por ejemplo, nombre de fuente, versión de catálogo, notas del analista).
        Este objeto se registra en metadatos/provenance del TripDataset resultante.
    strict : bool, default=True
        Si True, condiciones que impiden inferir (precondiciones del método,
        ausencia de campos mínimos, etc.) generan excepción.
        Si False, el módulo intenta devolver un TripDataset vacío o parcial
        y reporta issues de nivel ERROR.

    Returns
    -------
    trip_dataset : TripDataset
        Viajes inferidos en formato Golondrina. En v1.1:
        - `trip_dataset.metadata["events"]` inicia desde 0 (dataset nuevo).
        - `trip_dataset.provenance["derived_from"]` referencia el TraceDataset de origen.
        - `trip_dataset.provenance` incluye un resumen breve de eventos previos del origen
          (análogamente a build_flows).
    report : InferenceReport
        Reporte con issues y resumen agregado de la inferencia.

    Raises
    ------
    InferenceError
        Si `strict=True` y no es posible ejecutar la inferencia (por ejemplo,
        faltan campos mínimos o el método/parametrización es incompatible).
    """
    raise NotImplementedError
