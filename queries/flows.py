# pylondrina/queries/flows.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

import pandas as pd

from ..datasets import FlowDataset, TripDataset
from ..reports import OperationReport


def get_trips_from_flows(
    flows: FlowDataset,
    trips: Optional[TripDataset] = None,
    *,
    max_issues: int = 1000,
) -> Tuple[pd.DataFrame, OperationReport]:
    """
    Obtiene (o reconstruye) la correspondencia entre flujos y viajes.

    Esta operación entrega una tabla de correspondencias "qué flujos hay para qué viajes",
    es decir, un DataFrame con pares (flow_id, trip_id).

    La prioridad para obtener la correspondencia es:
    1) flows.flow_to_trips (si existe y es válida)
    2) argumento trips (si fue entregado)
    3) flows.source_trips (si está disponible en memoria)

    Si no existe ninguna fuente para construir la correspondencia, o si faltan campos
    mínimos para reconstruirla, la operación debe fallar con una excepción del módulo.

    Parameters
    ----------
    flows : FlowDataset
        Dataset de flujos Golondrina. Se asume que `flows.flows` incluye la columna
        obligatoria `flow_id`.
    trips : Optional[TripDataset], default=None
        Dataset de viajes a usar para reconstruir la correspondencia si `flows.flow_to_trips`
        no está disponible. Si es None, se intenta usar `flows.source_trips`.
    max_issues : int, default=1000
        Máximo de issues a registrar en el OperationReport.

    Returns
    -------
    linkage : pd.DataFrame
        DataFrame de correspondencias con columnas mínimas:
        - flow_id
        - trip_id
    report : OperationReport
        Reporte de la operación (issues, summary y parameters). No se registran eventos en
        `flows.metadata["events"]` dado que la operación no transforma datasets.

    """
    # v1.1: solo diseño (firmas + docstrings). Implementación posterior.
    raise NotImplementedError