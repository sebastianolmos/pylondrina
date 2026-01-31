from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from .datasets import TripDataset
from .reports import OperationReport
from .correspondence import FieldCorrections, ValueCorrections


@dataclass(frozen=True)
class FixCorrespondenceOptions:
    """
    Opciones de control para la operación fix_trips_correspondence (API v1.1).

    Attributes
    ----------
    strict : bool, default=False
        Si True, Issues de nivel error pueden gatillar una excepción al finalizar la operación.
    max_issues : int, default=200
        Límite máximo de issues a registrar en el reporte.
    sample_rows_per_issue : int, default=50
        Tamaño máximo de muestra (filas/valores) a incluir en `Issue.details`.
    """
    strict: bool = False
    max_issues: int = 200
    sample_rows_per_issue: int = 50


def fix_trips_correspondence(
    trips: TripDataset,
    *,
    field_corrections: Optional[FieldCorrections] = None,
    value_corrections: Optional[ValueCorrections] = None,
    options: Optional[FixCorrespondenceOptions] = None,
    correspondence_context: Optional[Dict[str, Any]] = None,
) -> Tuple[TripDataset, OperationReport]:
    """
    Corrige correspondencias de un TripDataset Golondrina, soportando:
    (1) correspondencia de campos (renombrado de columnas) y
    (2) correspondencia de valores categóricos (recode por campo).

    Si se entregan ambos tipos de corrección, el orden de aplicación es:
    1) field_corrections  ->  2) value_corrections

    Parameters
    ----------
    trips : TripDataset
        Dataset de viajes en formato Golondrina.
    field_corrections : FieldCorrections, optional
        Correcciones de nombres de columnas. Si es None, no se corrigen campos.
    value_corrections : ValueCorrections, optional
        Correcciones de valores categóricos por campo. Si es None, no se corrigen valores.
    options : FixCorrespondenceOptions, optional
        Opciones de ejecución (strict, límites de issues, muestreo de detalles).
    correspondence_context : dict, optional
        Metadatos adicionales para registrar en el evento de metadata (por ejemplo, versión de catálogo, autor, justificación, etc.).

    Returns
    -------
    fixed : TripDataset
        Nuevo TripDataset con correcciones aplicadas. El dataset resultante queda marcado como `validated=False` en metadata.
    report : OperationReport
        Reporte de la operación (issues + summary). Su `summary` debe ser serializable.

    Notes
    -----
    - La operación registra un evento `fix_trips_correspondence` en `metadata["events"]`.
    """
    raise NotImplementedError
