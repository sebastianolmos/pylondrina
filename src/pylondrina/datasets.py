# -------------------------
# file: pylondrina/datasets.py
# -------------------------
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple, Mapping

from pprint import pformat
import pandas as pd

from pylondrina.schema import TripSchema, TraceSchema, TripSchemaEffective
from pylondrina.types import FieldCorrespondence, ValueCorrespondence


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
    metadata: Dict[str, Any] = field(default_factory=dict)
    schema_effective: TripSchemaEffective = field(default_factory=TripSchemaEffective)
    
    @property
    def is_validated(self) -> bool:
        """
        Retorna True si el dataset está marcado como validado en metadata.
        """
        if "is_validated" in self.metadata:
            return bool(self.metadata.get("is_validated", False))
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
        self.metadata["is_validated"] = bool(value)

    def to_display_dict(self) -> Dict[str, Any]:
        """
        Devuelve una versión resumida/estructurada del dataset para impresión.
        """
        return {
            "type": self.__class__.__name__,
            "shape": self.data.shape,
            "columns": list(self.data.columns),
            "schema_version": self.schema_version,
            "is_validated": self.is_validated,
            "field_correspondence": self.field_correspondence,
            "value_correspondence": self.value_correspondence,
            "provenance": self.provenance,
            "metadata": self.metadata,
            "schema": self.schema,
            "schema_effective": self.schema_effective,
        }

    def __str__(self) -> str:
        """
        Salida legible para print(dataset).
        """
        parts = [
            f"{self.__class__.__name__}",
            "-" * len(self.__class__.__name__),
            f"shape: {self.data.shape}",
            f"schema_version: {self.schema_version}",
            f"is_validated: {self.is_validated}",
            f"columns: {list(self.data.columns)}",
        ]

        parts.append("\ndata:")
        data_str = str(self.data)
        parts.append(data_str)

        parts.append("\nschema:")
        parts.append(pformat(self.schema, width=100, sort_dicts=False))

        parts.append("\nschema_effective:")
        parts.append(pformat(self.schema_effective, width=100, sort_dicts=False))

        if self.provenance:
            parts.append("\nprovenance:")
            parts.append(pformat(self.provenance, width=100, sort_dicts=False))

        if self.field_correspondence:
            parts.append("\nfield_correspondence:")
            parts.append(pformat(self.field_correspondence, width=100, sort_dicts=False))

        if self.value_correspondence:
            parts.append("\nvalue_correspondence:")
            parts.append(pformat(self.value_correspondence, width=100, sort_dicts=False))

        if self.metadata:
            parts.append("\nmetadata:")
            parts.append(pformat(self.metadata, width=100, sort_dicts=False))

        return "\n".join(parts)

    def __repr__(self) -> str:
        """
        Representación útil para consola e inspección.
        Más compacta que __str__, pero todavía legible.
        """
        data_preview = self.data.head().to_string(index=True)

        payload = {
            "type": self.__class__.__name__,
            "shape": self.data.shape,
            "columns": list(self.data.columns),
            "schema_version": self.schema_version,
            "is_validated": self.is_validated,
            "data_head": data_preview,
            "provenance": self.provenance,
            "field_correspondence": self.field_correspondence,
            "value_correspondence": self.value_correspondence,
            "metadata": self.metadata,
            "schema": self.schema,
            "schema_effective": self.schema_effective,
        }
        return pformat(payload, width=100, sort_dicts=False)

    def _repr_pretty_(self, p, cycle) -> None:
        """
        Soporte para visualización bonita en IPython/Jupyter.
        """
        if cycle:
            p.text(f"{self.__class__.__name__}(...)")
        else:
            p.text(str(self))

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
