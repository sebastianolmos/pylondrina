from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from .datasets import TraceDataset
from .reports import ImportReport, Issue
from .schema import TraceSchema
from .types import FieldCorrespondence


TracePreprocess = Callable[
    [pd.DataFrame, Optional[Dict[str, pd.DataFrame]], Optional[Dict[str, Any]]],
    pd.DataFrame,
]


@dataclass(frozen=True)
class ImportTraceOptions:
    """
    Opciones de importación/estandarización para construir un TraceDataset.

    Attributes
    ----------
    keep_extra_fields : bool, default=True
        Si True, conserva columnas no estándar como campos extendidos del dataset.
    selected_fields : sequence of str, optional
        Lista de campos a conservar (además de los obligatorios). Si es None, se conservan
        todos los campos disponibles (sujeto a keep_extra_fields).
    strict : bool, default=False
        Si True, problemas estructurales relevantes detienen el proceso (excepción).
    """
    keep_extra_fields: bool = True
    selected_fields: Optional[Sequence[str]] = None
    strict: bool = False


def import_traces_from_dataframe(
    df: pd.DataFrame,
    schema: TraceSchema,
    *,
    source_name: Optional[str] = None,
    options: Optional[ImportTraceOptions] = None,
    field_correspondence: Optional[FieldCorrespondence] = None,
    preprocess: Optional[TracePreprocess] = None,
    aux_tables: Optional[Dict[str, pd.DataFrame]] = None,
    preprocess_context: Optional[Dict[str, Any]] = None,
    provenance: Optional[Dict[str, Any]] = None,
) -> Tuple[TraceDataset, ImportReport]:
    """
    Importa un DataFrame de trazas desde un formato externo a un TraceDataset Golondrina.

    En v1.1, TraceSchema describe campos estándar y contexto (p. ej. CRS/timezone), mientras que
    la alineación desde columnas externas se realiza mediante:
    - `preprocess` (opcional, para casos fuente-específicos), y/o
    - `field_correspondence` (mapeo estándar -> fuente).

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame fuente con registros de trazas (p. ej., GPS, XDR, check-ins).
    schema : TraceSchema
        Esquema objetivo (define campos estándar y requisitos mínimos de trazas).
    source_name : str, optional
        Nombre de la fuente. Se registra en metadatos.
    options : ImportTraceOptions, optional
        Opciones de importación y política de severidad.
    field_correspondence : mapping, optional
        Correspondencia campo estándar -> columna fuente. Si es None, se asume que el DataFrame ya está en nombres estándar Golondrina.
    preprocess : TracePreprocess, optional
        Función para preprocesamiento fuente-específico. Recibe: (df, aux_tables, preprocess_context) y retorna un DataFrame.
    aux_tables : dict[str, pandas.DataFrame], optional
        Tablas auxiliares usadas por preprocess (p. ej., tabla de POIs para Foursquare).
    preprocess_context : dict, optional
        Parámetros adicionales para preprocess (JSON-serializable recomendado).
    provenance : dict, optional
        Metadatos de procedencia adicionales (periodo, zona, versión, etc.). Debe ser JSON-serializable.

    Returns
    -------
    dataset : TraceDataset
        Conjunto de trazas estandarizado (campos obligatorios en nombres Golondrina).
    report : ImportReport
        Reporte de importación con issues, summary, parameters y trazabilidad.

    Raises
    ------
    ImportError
        Si no es posible construir el dataset (p. ej. faltan campos obligatorios) y la política
        efectiva requiere abortar.
    """
    raise NotImplementedError


def _apply_trace_field_correspondence(
    df: pd.DataFrame,
    schema: TraceSchema,
    *,
    field_correspondence: FieldCorrespondence,
    options: ImportTraceOptions,
) -> Tuple[pd.DataFrame, Dict[str, str], List[Issue]]:
    """
    Aplica un mapeo estándar -> fuente para alinear nombres de campos a Golondrina.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame fuente.
    schema : TraceSchema
        Esquema objetivo.
    field_correspondence : mapping
        Mapeo campo estándar -> columna fuente.
    options : ImportTraceOptions
        Política de selección de campos y severidad.

    Returns
    -------
    df_std : pandas.DataFrame
        DataFrame con columnas estándar (y extras si procede).
    applied : dict
        Mapeo efectivamente aplicado (campo estándar -> campo fuente).
    issues : list[Issue]
        Hallazgos relacionados a mapeo (faltantes, colisiones, etc.).
    """
    raise NotImplementedError


def _build_import_traces_metadata(
    *,
    schema: TraceSchema,
    source_name: Optional[str],
    applied_field_map: Dict[str, str],
) -> Dict[str, Any]:
    """
    Construye metadatos mínimos para un TraceDataset importado.

    Parameters
    ----------
    schema : TraceSchema
        Esquema aplicado.
    source_name : str, optional
        Fuente declarada.
    applied_field_map : dict
        Correspondencia aplicada (campo estándar -> campo fuente).

    Returns
    -------
    dict
        Metadatos listos para almacenarse en `TraceDataset.metadata`.
    """
    raise NotImplementedError
