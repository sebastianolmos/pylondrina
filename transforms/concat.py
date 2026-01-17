# -------------------------
# file: pylondrina/transforms/concat.py
# -------------------------
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence, Any, Dict, Iterable, List, Optional, Tuple

from ..types import FieldName
from ..datasets import TripDataset
from ..schema import TripSchema
from ..reports import ValidationReport

@dataclass
class TripConcatOptions:
    """
    Opciones para concatenar conjuntos de viajes en formato Golondrina (v1).

    Estas opciones controlan compatibilidad de esquemas/dominios y permiten,
    de forma opcional, detectar y eliminar duplicados generados típicamente por
    operaciones de integración (por ejemplo, joins que multiplican filas).

    Attributes
    ----------
    require_same_schema_version : bool, default=True
        Si True, exige que todos los TripDataset de entrada declaren la misma versión
        de TripSchema. Si False, permite versiones distintas siempre que los esquemas
        sean compatibles (misma semántica en campos obligatorios; diferencias solo en
        campos opcionales y/o extensiones de dominio controladas).
    allow_extended_domains : bool, default=True
        Si True, permite que el dominio categórico resultante sea la unión de los valores
        observados en los datasets de entrada. Si False, valores fuera del dominio base
        generan issues en el reporte.
    deduplicate : bool, default=False
        Si True, intenta identificar y eliminar viajes duplicados durante la concatenación.
        Esta opción está pensada para mitigar duplicaciones accidentales introducidas por
        procesos de ETL/integración (por ejemplo, joins muchos-a-muchos).
    deduplicate_on : Sequence[FieldName] | None, default=None
        Define la clave lógica usada para identificar duplicados, como una secuencia de
        nombres de campos (por ejemplo: ["user_id", "start_time", "origin_cell", "destination_cell"]).

        Reglas de uso:
        - Si `deduplicate=True` y `deduplicate_on is None`, la deduplicación se realiza por
          un identificador estable del viaje (por ejemplo "trip_id") si está disponible.
        - Si `deduplicate=True` y `deduplicate_on` se especifica, la deduplicación se realiza
          por igualdad exacta de esa clave lógica.
        - Si `deduplicate=False`, este parámetro se ignora.

        Advertencia: deduplicar por clave lógica puede producir colisiones (viajes distintos
        que comparten la misma clave). Por ello, se recomienda usarlo solo cuando el usuario
        conoce el origen de la duplicación y aceptar la deduplicación como una política explícita.

    Notes
    -----
    - En v1, la deduplicación por clave lógica se entiende como coincidencia exacta de valores.
      Tolerancias (por ejemplo, ventanas temporales o aproximación espacial) quedan fuera de alcance.
    - La función debe reportar en el reporte cuántos registros fueron removidos y con qué criterio.
    """
    require_same_schema_version: bool = True
    allow_extended_domains: bool = True
    deduplicate: bool = False
    deduplicate_on: Optional[Sequence[FieldName]] = None


def concat_trip_datasets(
    datasets: Iterable[TripDataset],
    schema: TripSchema,
    *,
    options: Optional[TripConcatOptions] = None,
    provenance: Optional[Dict[str, Any]] = None,
) -> Tuple[TripDataset, ValidationReport]:
    """
    Concatena múltiples TripDataset en un único conjunto de viajes Golondrina (v1).

    Esta función permite combinar datasets de viajes que ya están en formato
    Golondrina, por ejemplo para:
    - unir campañas o periodos distintos,
    - agregar viajes provenientes de una nueva fuente ya estandarizada,
    - construir un dataset consolidado para análisis o agregación posterior.

    La concatenación es semántica (no solo tabular): se validan esquemas,
    dominios categóricos y se preserva la trazabilidad de las fuentes.

    Parameters
    ----------
    datasets : Iterable[TripDataset]
        Conjuntos de viajes en formato Golondrina a combinar.
    schema : TripSchema
        Esquema Golondrina que debe cumplir el dataset resultante.
        Se utiliza para validar compatibilidad y normalizar la salida.
    options : TripConcatOptions, optional
        Opciones que controlan compatibilidad de esquemas, dominios
        categóricos y tratamiento de duplicados.
        Si es None, se usan los valores por defecto.
    provenance : dict[str, Any], optional
        Metadatos adicionales de procedencia a adjuntar al dataset resultante,
        por ejemplo información sobre el proceso de combinación, autor,
        fecha o propósito del dataset consolidado.

    Returns
    -------
    trip_dataset : TripDataset
        Dataset de viajes resultante, en formato Golondrina, que contiene
        la concatenación de los viajes de entrada.
    report : ValidationReport
        Reporte que describe:
        - problemas de compatibilidad detectados,
        - valores fuera de dominio,
        - conflictos de esquema,
        - decisiones tomadas durante la combinación (si aplica).

    Notes
    -----
    - Esta función no realiza inferencia ni transformación semántica de viajes;
      solo combina viajes ya estandarizados.
    - Se asume que cada TripDataset de entrada ya fue validado individualmente.
    - La concatenación es un paso común previo a la construcción de flujos
      o análisis comparativos multi-fuente.
    """
    raise NotImplementedError