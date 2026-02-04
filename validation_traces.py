# -------------------------
# file: pylondrina/validation_traces.py
# -------------------------
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from .datasets import TraceDataset
from .reports import ConsistencyReport
from .types import IssueLevel
from .errors import ValidationError, SchemaError


@dataclass(frozen=True)
class TraceValidationOptions:
    """
    Opciones de validación para `TraceDataset` (API v1.1).

    La validación de trazas en v1.1 está enfocada en consistencia mínima
    espacio-temporal antes de procesos posteriores conmo inferencia de viajes.

    Attributes
    ----------
    strict : bool, default=False
        Si True, la operación levanta una excepción si se detectan issues
        con severidad "error".
    max_issues : int, default=500
        Límite máximo de issues a registrar.
    sample_rows_per_issue : int, default=5
        Cantidad máxima de filas ejemplo a guardar en `Issue.details["sample_rows"]`
        cuando aplique.

    validate_required_fields : bool, default=True
        Verifica presencia de los campos mínimos definidos por Golondrina para trazas.
    validate_types_and_formats : bool, default=True
        Verifica tipos básicos y parseabilidad (por ejemplo timestamps).
    validate_constraints : bool, default=True
        Verifica restricciones básicas asociadas a los campos (por ejemplo rangos),
        incluye rangos y reglas por campo según el schema (p. ej. lat/lon en EPSG:4326).
    validate_monotonic_time_per_user : bool, default=True
        Verifica que los timestamps sean no decrecientes por `user_id`.
    """
    strict: bool = False
    max_issues: int = 500
    sample_rows_per_issue: int = 5

    validate_required_fields: bool = True
    validate_types_and_formats: bool = True
    validate_constraints: bool = True

    validate_monotonic_time_per_user: bool = True


def validate_traces(
    traces: TraceDataset,
    *,
    options: Optional[TraceValidationOptions] = None,
) -> ConsistencyReport:
    """
    Valida un `TraceDataset` contra su `TraceSchema` y reglas mínimas de consistencia (API v1.1).

    Esta operación:
    - ejecuta un conjunto acotado de checks controlados por `TraceValidationOptions`,
    - produce un `ConsistencyReport` (issues + summary),
    - y registra un evento en `traces.metadata["events"]` con parámetros efectivos y summary.

    Parameters
    ----------
    traces : TraceDataset
        Dataset de trazas en formato Golondrina.
    options : TraceValidationOptions, optional
        Opciones de validación. Si es None, se usan defaults v1.1.

    Returns
    -------
    ConsistencyReport
        Reporte con issues y summary.

    Raises
    ------
    ValidationError
        Si `options.strict=True` y hay issues nivel "error".
    SchemaError
        Si el dataset no tiene schema o el schema es inconsistente.
    """
    raise NotImplementedError
