# -------------------------
# file: pylondrina/datasets.py
# -------------------------
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

import pandas as pd

from .schema import TripSchema, TraceSchema
from .types import FieldCorrespondence, ValueCorrespondence
from .reports import ValidationReport, OperationReport
from .validation import ValidationOptions, validate_trips
from .correspondence import FieldCorrections, ValueCorrections
from .fixing import FixCorrespondenceOptions, fix_trips_correspondence


@dataclass
class TripDataset:
    """
    Conjunto de viajes en formato Golondrina.

    Attributes
    ----------
    data : pandas.DataFrame
        Tabla de viajes normalizada al formato Golondrina (columnas estándar + extensiones).
    schema : TripSchema
        Esquema aplicado al dataset (contrato de validación).
    schema_version : str
        Versión del esquema aplicado (duplicada por conveniencia).
    provenance : dict
        Metadatos de procedencia (fuente, periodo, zona, licencia, etc.).
    field_correspondence : dict
        Correspondencia aplicada durante importación: campo estándar -> nombre en fuente.
    value_correspondence : dict
        Normalización aplicada: campo -> (valor fuente -> valor canónico).
    domains_effective : dict
        Dominios efectivamente usados en el dataset (incluye extensiones controladas).
    metadata : dict
        Metadatos adicionales (p. ej. resumen validación, timestamp importación, etc.).

    Notes
    -----
    - Este objeto es principalmente un contenedor de estado.
    - El estado de validación se representa mediante un flag en metadata: `metadata["flags"]["validated"]` (bool).
    """
    data: pd.DataFrame
    schema: TripSchema
    schema_version: str = "0.1.0"
    provenance: Dict[str, Any] = field(default_factory=dict)
    field_correspondence: Dict[str, str] = field(default_factory=dict)
    value_correspondence: Dict[str, Dict[str, str]] = field(default_factory=dict)
    domains_effective: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def validate(self, *, options: Optional[ValidationOptions] = None) -> ValidationReport:
        """
        Valida este `TripDataset` usando `pylondrina.validation.validate_trips` (API v1.1).

        Parameters
        ----------
        options : ValidationOptions, optional
            Opciones de validación. Si None, se usan defaults v1.1.

        Returns
        -------
        ValidationReport
            Reporte de validación.

        Raises
        ------
        ValidationError
            Si `options.strict=True` y hay issues nivel "error".
        """
        raise NotImplementedError
    
    @property
    def is_validated(self) -> bool:
        """
        Retorna True si el dataset está marcado como validado en metadata.
        """
        flags = self.metadata.get("flags", {})
        return bool(flags.get("validated", False))

    def _set_validated_flag(self, value: bool) -> None:
        """
        Marca el flag de validación en metadata.

        Parameters
        ----------
        value : bool
            True para marcar como validado; False para marcar como no validado.
        """
        self.metadata.setdefault("flags", {})
        self.metadata["flags"]["validated"] = bool(value)
    
    def fix_correspondence(
        self,
        *,
        field_corrections: Optional[FieldCorrections] = None,
        value_corrections: Optional[ValueCorrections] = None,
        options: Optional[FixCorrespondenceOptions] = None,
        correspondence_context: Optional[Dict[str, Any]] = None,
    ) -> Tuple["TripDataset", OperationReport]:
        """
        Wrapper de conveniencia para `fix_trips_correspondence(...)`.
        """
        return fix_trips_correspondence(
            self,
            field_corrections=field_corrections,
            value_corrections=value_corrections,
            options=options,
            correspondence_context=correspondence_context,
        )


@dataclass
class FlowDataset:
    """
    Conjunto de flujos OD construido a partir de un TripDataset.

    Attributes
    ----------
    data : pandas.DataFrame
        Tabla de flujos (origen, destino, conteo, y dimensiones opcionales).
    source_trips : TripDataset, optional
        Referencia al dataset de viajes que dio origen a los flujos (si se conserva).
    aggregation_spec : dict
        Parámetros de agregación usados (resolución H3, intervalos temporales, filtros, umbrales).
    flow_to_trips : dict
        Trazabilidad: identificador de flujo -> identificadores de viajes miembros (si se conserva).
    metadata : dict
        Metadatos adicionales (p. ej. resumen de construcción, exportaciones, etc.).
    """
    data: pd.DataFrame
    source_trips: Optional[TripDataset] = None
    aggregation_spec: Dict[str, Any] = field(default_factory=dict)
    flow_to_trips: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TraceDataset:
    """
    Conjunto de puntos de traza/trayectoria o puntos de estadía (POIs/check-ins).

    Attributes
    ----------
    data : pandas.DataFrame
        Tabla de puntos (id, timestamp, coordenadas y otros campos).
    schema : TraceSchema
        Esquema aplicado para validar estructura mínima.
    provenance : dict
        Metadatos de procedencia (fuente, periodo, resolución temporal, etc.).
    metadata : dict
        Metadatos adicionales (p. ej. resumen validación, parámetros de limpieza, etc.).
    """
    data: pd.DataFrame
    schema: TraceSchema
    provenance: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
