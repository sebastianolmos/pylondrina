# -------------------------
# file: pylondrina/transforms/flows.py
# -------------------------
from dataclasses import dataclass
from typing import Any, Mapping, Optional, Sequence, Literal

from ..datasets import TripDataset, FlowDataset


TimeAggregation = Literal["hour", "day", "week", "month", "none"]
TimeBasis = Literal["origin", "destination"]
TimePredicate = Literal["starts_within", "ends_within", "overlaps", "contains"]


@dataclass(frozen=True)
class FlowBuildOptions:
    """
    Configuración para construir flujos (FlowDataset) a partir de viajes OD.

    Esta estructura agrupa los parámetros que controlan:
    (i) la agregación espacial (por celdas H3),
    (ii) la agregación/partición temporal opcional, y
    (iii) el umbral mínimo de tamaño de flujo.

    Parameters
    ----------
    h3_resolution : int
        Resolución H3 a la cual se discretizan origen y destino para formar pares OD.
        A mayor valor, mayor detalle espacial (celdas más pequeñas).
    group_by : sequence of str, optional
        Campos estándar del `TripDataset` por los cuales segmentar los flujos además del par OD
        (y además de la partición temporal si aplica). Cada campo agrega una dimensión de
        segmentación, creando flujos más específicos.
        Por ejemplo, segmentar por ``["mode"]`` genera flujos OD separados por modo.
    time_aggregation : {"hour","day","week","month","none"}, default="none"
        Granularidad temporal para particionar el conteo de flujos. Si es ``"none"``,
        no se agrega dimensión temporal al flujo.
    time_basis : {"origin","destination"}, default="origin"
        Campo temporal base para la partición temporal: tiempo de inicio (origen) o de término
        (destino).
    time_predicate : {"starts_within","ends_within","overlaps","contains"}, default="starts_within"
        Regla conceptual utilizada cuando la construcción de flujos se combina con filtros
        temporales en etapas previas (p.ej. `filter_by_time_range`). Se mantiene como metadato
        para trazabilidad y consistencia del pipeline.
    min_trips_per_flow : int, default=1
        Umbral mínimo de viajes para conservar un flujo. Flujos con conteo inferior se descartan.
    keep_flow_to_trips : bool, default=False
        Si es True, el `FlowDataset` mantiene una estructura que permite relacionar cada flujo
        con los viajes que lo componen (puede ser costoso en memoria para datasets grandes).
    strict : bool, default=False
        Si es True, inconsistencias críticas en campos requeridos o tipos esperados deben
        traducirse en errores/excepciones en lugar de solo advertencias (según el diseño del
        validador).

    Notes
    -----
    - `group_by` debe referirse a nombres estándar de campos (no nombres originales de fuente).
    - La semántica exacta de cómo se representa la dimensión temporal en el flujo (p.ej. columna
      `time_bucket`) se define en `build_flows`.
    """
    h3_resolution: int
    group_by: Optional[Sequence[str]] = None

    time_aggregation: TimeAggregation = "none"
    time_basis: TimeBasis = "origin"
    time_predicate: TimePredicate = "starts_within"

    min_trips_per_flow: int = 1

    keep_flow_to_trips: bool = False
    strict: bool = False


def build_flows(
    trips: TripDataset,
    *,
    options: FlowBuildOptions,
    origin_h3_field: str = "origin_h3",
    destination_h3_field: str = "destination_h3",
    origin_time_field: str = "origin_time",
    destination_time_field: str = "destination_time",
    extra_metadata: Optional[Mapping[str, Any]] = None,
) -> FlowDataset:
    """
    Construye un `FlowDataset` agregando viajes OD del `TripDataset`.

    El resultado corresponde a una tabla de flujos donde cada fila representa un agregado
    (p.ej. por par origen-destino discretizado en H3) y su conteo de viajes. Opcionalmente,
    el agregado puede segmentarse por dimensiones adicionales (`options.group_by`) y/o por
    una partición temporal (`options.time_aggregation`).

    Parameters
    ----------
    trips : TripDataset
        Conjunto de viajes en formato Golondrina desde el cual se agregan flujos.
    options : FlowBuildOptions
        Configuración de agregación y umbrales.
    origin_h3_field : str, default="origin_h3"
        Nombre del campo (estándar) que contiene la celda H3 del origen.
    destination_h3_field : str, default="destination_h3"
        Nombre del campo (estándar) que contiene la celda H3 del destino.
    origin_time_field : str, default="origin_time"
        Nombre del campo (estándar) con el timestamp del origen.
        Se usa si `options.time_basis="origin"` y/o si `options.time_aggregation != "none"`.
    destination_time_field : str, default="destination_time"
        Nombre del campo (estándar) con el timestamp del destino.
        Se usa si `options.time_basis="destination"` y/o si `options.time_aggregation != "none"`.
    extra_metadata : mapping, optional
        Metadatos adicionales a incorporar en `FlowDataset.metadata` para trazabilidad
        (p.ej. nombre de la fuente, parámetros externos del experimento, etc.).

    Returns
    -------
    FlowDataset
        Conjunto de flujos OD agregados, con metadatos que describen la especificación de
        agregación (resolución H3, segmentación, dimensión temporal si aplica) y el umbral
        utilizado.

    Raises
    ------
    SchemaValidationError
        Si `options.strict=True` y faltan campos requeridos para la agregación o existen
        incompatibilidades de tipo/forma que impiden construir flujos.
    ValueError
        Si la configuración es incoherente (por ejemplo, `min_trips_per_flow < 1`).

    Notes
    -----
    - Este método asume que el `TripDataset` ya fue estandarizado y validado.
    - Si `options.keep_flow_to_trips=True`, el resultado puede ser costoso para datasets
      masivos; se recomienda usarlo solo para análisis exploratorio o auditorías puntuales.
    """
    raise NotImplementedError

