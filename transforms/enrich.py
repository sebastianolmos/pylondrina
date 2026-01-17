from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple, Literal, TypeAlias

import pandas as pd

from ..datasets import TripDataset
from ..schema import TripSchema
from ..types import FieldName
from ..reports import ValidationReport

TripEnrichJoinHow: TypeAlias = Literal["left", "inner"]
"""
Tipo de join soportado por `enrich_trips` en Pylondrina (v1).

Valores
-------
"left"
    Conserva todos los viajes; agrega atributos cuando hay match.
"inner"
    Conserva solo viajes con match en la tabla externa.
"""

@dataclass
class TripEnrichOptions:
    """
    Opciones para enriquecer un TripDataset con información externa (EU-13) (v1).

    En v1, el enriquecimiento se entiende como un join controlado de tipo
    muchos-a-uno (viaje -> entidad externa, por ejemplo persona u hogar).
    Por diseño, el enriquecimiento NO debe aumentar el número de filas de viajes.

    Attributes
    ----------
    how : TripEnrichJoinHow, default="left"
        Tipo de join:
        - "left": conserva todos los viajes; valores faltantes si no hay match.
        - "inner": conserva solo viajes con match en la tabla externa.
    require_unique_enrichment_keys : bool, default=True
        Si True, exige que la tabla externa tenga llaves únicas según las columnas
        usadas en el join. Esta verificación previene duplicaciones accidentales de
        viajes debidas a joins muchos-a-muchos.
    allow_overwrite : bool, default=False
        Si False, no permite sobrescribir columnas ya existentes en el dataset de viajes.
        Si True, permite sobrescritura y debe registrarse en el reporte (columnas afectadas).

    Notes
    -----
    - En v1 la igualdad de llaves es exacta (sin tolerancias temporales/espaciales).
    """
    how: TripEnrichJoinHow = "left"
    require_unique_enrichment_keys: bool = True
    allow_overwrite: bool = False


def enrich_trips(
    trips: TripDataset,
    enrichment: pd.DataFrame,
    *,
    keys: Mapping[FieldName, str],
    add_fields: Optional[Mapping[str, FieldName]] = None,
    schema: Optional[TripSchema] = None,
    options: Optional[TripEnrichOptions] = None,
    provenance: Optional[Dict[str, Any]] = None,
) -> Tuple[TripDataset, ValidationReport]:
    """
    Enriquece un TripDataset adjuntando columnas desde una tabla externa (EU-13) (v1).

    Este escenario cubre el caso en que un investigador quiere agregar atributos
    contextuales a los viajes (por ejemplo, variables de Personas u Hogares de una EOD),
    manteniendo trazabilidad y detectando errores comunes de integración.

    Parameters
    ----------
    trips : TripDataset
        Dataset de viajes en formato Golondrina a enriquecer.
    enrichment : pandas.DataFrame
        Tabla externa con atributos a adjuntar (por ejemplo, tabla Personas u Hogares).
    keys : mapping[FieldName, str]
        Mapeo de llaves para el join:
        - clave: nombre de columna en `trips.data`,
        - valor: nombre de columna en `enrichment`.
        Ejemplo: {"person_id": "PERSON_ID"}.
    add_fields : mapping[str, FieldName], optional
        Selección y renombre de columnas a adjuntar desde `enrichment`.
        - clave: nombre de columna en `enrichment`,
        - valor: nombre de columna destino en `trips.data`.
        Si es None, se adjuntan todas las columnas de `enrichment` excepto las usadas como llave,
        respetando `options.allow_overwrite`.
        Ejemplo: {"SEXO": "gender", "EDAD": "age_years"}.
    schema : TripSchema, optional
        Esquema asociado al dataset resultante. Si es None, se usa `trips.schema`.
        En v1, el enriquecimiento no implica revalidación automática; si el usuario requiere
        conformidad estricta posterior, debe ejecutar `validate_trips(...)`.
    options : TripEnrichOptions, optional
        Opciones del enriquecimiento. Si es None, se usan defaults.
    provenance : dict[str, Any], optional
        Metadatos adicionales del paso de enriquecimiento a registrar en el dataset resultante
        (por ejemplo, fuente de la tabla externa, llaves usadas, supuestos).

    Returns
    -------
    enriched : TripDataset
        Nuevo TripDataset con columnas adicionales. En el caso típico ("left"),
        conserva las mismas filas que `trips`. El schema se mantiene (o se usa el provisto).
    report : ValidationReport
        Reporte con issues y resumen del enriquecimiento. En particular, el `summary`
        debería incluir, al menos:
        - n_rows_before / n_rows_after
        - how
        - keys
        - columnas agregadas
        - n_trips_matched / n_trips_unmatched (para "left")
        - columnas sobrescritas (si aplica)
        - indicador explícito de multiplicación de filas si ocurre
    """
    raise NotImplementedError