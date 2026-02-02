# -------------------------
# file: pylondrina/datasets.py
# -------------------------
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple, Mapping

import pandas as pd

from .schema import TripSchema, TraceSchema
from .types import FieldCorrespondence, ValueCorrespondence
from .reports import ValidationReport, OperationReport
from .validation import ValidationOptions, validate_trips
from .correspondence import FieldCorrections, ValueCorrections
from .fixing import FixCorrespondenceOptions, fix_trips_correspondence
from transforms.filtering import filter_trips, FilterOptions


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
    def filter(
        self,
        *,
        options: "Optional[FilterOptions]" = None,
        max_issues: int = 1000,
    ) -> "Tuple[TripDataset, OperationReport]":
        """
        Aplica un filtrado al dataset (atajo orientado a API mixta).

        Parameters
        ----------
        options:
            Opciones del filtro. Si es None, no aplica filtros (pero puede registrar evento/report).
        max_issues:
            Límite máximo de issues a registrar.

        Returns
        -------
        (TripDataset, OperationReport)
            Dataset filtrado + reporte de la operación.
        """
        return filter_trips(self, options=options, max_issues=max_issues)


@dataclass
class FlowDataset:
    """
    Dataset de flujos OD construido a partir de un TripDataset en formato Golondrina.

    Attributes
    ----------
    flows:
        DataFrame con flujos agregados (OD). Debe incluir, al menos, las claves OD efectivas.

    flow_to_trips:
        Tabla opcional de correspondencia entre flujos y viajes fuente (útil para auditoría,
        explicación o debugging). Puede omitirse por performance.

    aggregation_spec:
        Especificación serializable (dict) de la agregación efectivamente aplicada: resolución,
        segmentación, base temporal, umbrales, etc. Se usa para reproducibilidad.

    source_trips:
        Referencia opcional al TripDataset de origen (solo en memoria). No se considera parte
        de la persistencia.

    metadata:
        Metadatos serializables del FlowDataset (incluye `events` del pipeline y trazabilidad).

    provenance:
        Provenance opcional del artefacto derivado, en formato serializable (mapping/dict).
    """

    flows: pd.DataFrame
    flow_to_trips: Optional[pd.DataFrame]
    aggregation_spec: dict
    source_trips: Optional["TripDataset"]
    metadata: dict
    provenance: Optional[Mapping[str, Any]] = None


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
