# pylondrina/transforms/flows_filtering.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ..datasets import FlowDataset
from ..reports import OperationReport
from .filtering import WhereClause, SpatialPredicate


@dataclass(frozen=True)
class FlowFilterOptions:
    """
    Opciones para filtrar un FlowDataset por criterios declarativos (`where`) y/o por celdas H3.

    Parameters / Attributes
    ----------------------
    where : Optional[WhereClause]
        Filtros declarativos por columnas del DataFrame `FlowDataset.flows`.
        La semántica de `where` (operadores soportados e interpretación) es la misma usada en `filter_trips`.
    h3_cells : Optional[Iterable[str]]
        Conjunto de celdas H3 permitidas para aplicar el filtro espacial.
    spatial_predicate : SpatialPredicate
        Define sobre qué extremo(s) se evalúa `h3_cells`: "origin", "destination", "both", "either".
    keep_flow_to_trips : bool
        Si True y `FlowDataset.flow_to_trips` existe, se filtra para mantener consistencia con los flujos resultantes.
    keep_metadata : bool
        Si True, agrega un evento de filtrado en `FlowDataset.metadata["events"]` del dataset resultante.
    strict : bool
        Si True, configuraciones inválidas pueden escalar a excepción; si False, se degradan a issues.
    """

    where: Optional["WhereClause"] = None
    h3_cells: Optional[Iterable[str]] = None

    spatial_predicate: "SpatialPredicate" = "origin"

    keep_flow_to_trips: bool = True
    keep_metadata: bool = True
    strict: bool = False


def filter_flows(
    flows: FlowDataset,
    *,
    options: Optional[FlowFilterOptions] = None,
    max_issues: int = 1000,
) -> Tuple[FlowDataset, OperationReport]:
    """
    Filtra un FlowDataset combinando criterios por atributos (`where`) y/o por celdas H3.

    Parameters
    ----------
    flows:
        Dataset de flujos de entrada.
    options:
        Opciones del filtro. Si es None, no aplica filtros y retorna el dataset (con reporte/event si procede).
    max_issues:
        Límite máximo de issues a registrar (para evitar explosión).

    Returns
    -------
    (FlowDataset, OperationReport)
        Dataset filtrado + reporte de la operación. Si `options.keep_metadata=True`,
        agrega un evento a `FlowDataset.metadata["events"]` en el dataset resultante.
    """
    raise NotImplementedError


def build_flow_filter_summary(
    *,
    n_rows_in: int,
    n_rows_out: int,
    filters_applied: List[str],
) -> Dict[str, Any]:
    """
    Construye el summary mínimo (serializable y estable) para una operación de filtrado de flujos.

    Parameters
    ----------
    n_rows_in:
        Número de filas antes del filtrado (FlowDataset.flows).
    n_rows_out:
        Número de filas después del filtrado (FlowDataset.flows).
    filters_applied:
        Lista de etiquetas de filtros efectivamente aplicados (p. ej. ["where", "h3_cells"]).

    Returns
    -------
    dict
        Summary mínimo del filtrado, pensado para report/event.
    """
    raise NotImplementedError


def _apply_where_filter_to_flows_df(
    flows_df,
    *,
    where: WhereClause,
    report: OperationReport,
    strict: bool,
    max_issues: int,
):
    """
    Aplica el filtrado por `where` (AND entre campos) sobre el DataFrame de flujos y registra issues en `report`.
    """
    raise NotImplementedError


def _apply_h3_filter_to_flows_df(
    flows_df,
    *,
    h3_cells: Iterable[str],
    spatial_predicate: SpatialPredicate,
    origin_h3_field: str,
    destination_h3_field: str,
    report: OperationReport,
    strict: bool,
    max_issues: int,
):
    """
    Aplica filtrado espacial por celdas H3 sobre el DataFrame de flujos según `spatial_predicate`.
    """
    raise NotImplementedError


def _filter_flow_to_trips_if_present(
    flow_to_trips_df,
    *,
    kept_flow_keys_df,
    report: OperationReport,
    strict: bool,
    max_issues: int,
):
    """
    Filtra `flow_to_trips` para mantener consistencia con el subconjunto de flujos conservado.
    """
    raise NotImplementedError
