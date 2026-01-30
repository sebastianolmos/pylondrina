# -------------------------
# file: pylondrina/importing.py
# -------------------------
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

import pandas as pd

from .datasets import TripDataset
from .reports import ImportReport, Issue
from .schema import TripSchema
from .types import FieldCorrespondence, ValueCorrespondence


@dataclass(frozen=True)
class ImportOptions:
    """
    Opciones de importación/estandarización para construir un TripDataset.

    Attributes
    ----------
    keep_extra_fields : bool, default=True
        Si True, conserva columnas que no están en el esquema como campos extendidos del dataset.
    selected_fields : sequence of str, optional
        Lista de campos estándar (Golondrina) que el usuario desea conservar explícitamente además de los obligatorios.
        Si None, se conservan todos los campos del esquema que existan en la fuente.
    strict : bool, default=False
        Si True, inconsistencias relevantes detienen el proceso (excepción) en vez de sólo reportarse.
    strict_domains : bool, default=False
        Si True, valores categóricos fuera del dominio base se consideran error.
        Si False, se permite extensión controlada del dominio a nivel de dataset (si el DomainSpec lo permite).
    """
    keep_extra_fields: bool = True
    selected_fields: Optional[Sequence[str]] = None
    strict: bool = False
    strict_domains: bool = False


def import_trips_from_dataframe(
    df: pd.DataFrame,
    schema: TripSchema,
    *,
    source_name: Optional[str] = None,
    options: Optional[ImportOptions] = None,
    field_correspondence: Optional[FieldCorrespondence] = None,
    value_correspondence: Optional[ValueCorrespondence] = None,
    provenance: Optional[Dict[str, Any]] = None,
    h3_resolution: int = 8,
) -> Tuple[TripDataset, ImportReport]:
    """
    Importa (convierte) un DataFrame de viajes desde un formato externo al formato Golondrina.

    Este proceso realiza la **estandarización** de:
    - nombres de campos (según correspondencias),
    - valores categóricos (según dominios del esquema y/o correspondencias),
    - tipos y formatos básicos (según FieldSpec),
    y genera un TripDataset con trazabilidad (metadatos + reportes).

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame fuente con viajes en el formato original.
    schema : TripSchema
        Esquema del formato Golondrina a aplicar.
    source_name : str, optional
        Nombre de la fuente (p. ej., "EOD", "XDR", "ADATRAP"). Se registra en metadatos.
    options : ImportOptions, optional
        Opciones de importación y política de errores.
    field_correspondence : mapping, optional
        Correspondencia: campo estándar Golondrina -> columna en el DataFrame fuente.
        Si None, se asume que el DataFrame ya usa nombres estándar (o se delega a perfiles de fuente).
    value_correspondence : mapping, optional
        Correspondencia de valores categóricos por campo: campo -> (valor_fuente -> valor_canónico).
    provenance : dict, optional
        Metadatos de procedencia adicionales (periodo, zona, versión del dataset, etc.).
        Debe ser JSON-serializable.
    h3_resolution : int, default=8
        Resolución H3 a utilizar para derivar índices de celdas (origen/destino) cuando sea aplicable.
        Debe estar en el rango permitido por H3 (típicamente 0..15). Esta resolución se registra en
        los metadatos del dataset para reproducibilidad.

    Returns
    -------
    dataset : TripDataset
        Conjunto de viajes en formato Golondrina.
    report : ImportReport
        Reporte de importación con hallazgos y trazabilidad.

    Raises
    ------
    ImportError
        Si faltan campos obligatorios y options.strict=True (o política equivalente).
    """
    raise NotImplementedError


def apply_field_correspondence(
    df: pd.DataFrame,
    schema: TripSchema,
    *,
    field_correspondence: FieldCorrespondence,
    options: ImportOptions,
) -> Tuple[pd.DataFrame, Dict[str, str], List[Issue]]:
    """
    Aplica la correspondencia de campos (renombrado y selección) para alinear con nombres estándar Golondrina.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame fuente.
    schema : TripSchema
        Esquema objetivo.
    field_correspondence : mapping
        Mapeo campo estándar -> columna fuente.
    options : ImportOptions
        Opciones (selección de campos, conservación de extras, política de errores).

    Returns
    -------
    df_std : pandas.DataFrame
        DataFrame con columnas estándar (y extras si procede).
    applied : dict
        Correspondencia efectivamente aplicada (estándar -> fuente).
    issues : list[Issue]
        Hallazgos (p. ej., campo obligatorio no encontrado, campo duplicado, etc.).
    """
    raise NotImplementedError


def standardize_categorical_values(
    df: pd.DataFrame,
    schema: TripSchema,
    *,
    value_correspondence: Optional[ValueCorrespondence],
    options: ImportOptions,
) -> Tuple[pd.DataFrame, Dict[str, Any], Dict[str, Dict[str, str]], List[Issue]]:
    """
    Estandariza valores categóricos a los dominios definidos por el esquema (y registra dominios efectivos).

    El objetivo es que distintos datasets usen el mismo “idioma” de categorías (comparabilidad e interoperabilidad).
    Si aparecen categorías no contempladas, se puede:
    - extender el dominio a nivel de dataset (extensión controlada) o
    - reportar error (strict_domains=True o DomainSpec.extendable=False).

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame ya alineado a nombres estándar.
    schema : TripSchema
        Esquema con dominios base.
    value_correspondence : mapping, optional
        Mapeos explícitos de valores por campo.
    options : ImportOptions
        Control de extensiones y severidad.

    Returns
    -------
    df_norm : pandas.DataFrame
        DataFrame con categorías estandarizadas.
    domains_effective : dict
        Dominios efectivamente utilizados (base + extensiones controladas).
    applied_value_maps : dict
        Mapeos de valores efectivamente aplicados por campo.
    issues : list[Issue]
        Hallazgos (valores fuera de dominio, extensiones aplicadas, etc.).
    """
    raise NotImplementedError


def build_import_metadata(
    *,
    schema: TripSchema,
    source_name: Optional[str],
    applied_field_map: Dict[str, str],
    applied_value_maps: Dict[str, Dict[str, str]],
    domains_effective: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Construye metadatos mínimos de trazabilidad para un TripDataset importado.

    Parameters
    ----------
    schema : TripSchema
        Esquema aplicado.
    source_name : str, optional
        Fuente declarada (p. ej., "EOD", "ADATRAP").
    applied_field_map : dict
        Correspondencia de campos aplicada (campo estándar -> columna/field de origen).
    applied_value_maps : dict
        Correspondencia de valores aplicada por campo (campo -> {valor_origen -> valor_canónico}).
    domains_effective : dict
        Dominios efectivos por campo categórico, incluyendo extensiones controladas si aplica.

    Returns
    -------
    dict
        Metadatos listos para almacenarse en `TripDataset.metadata`.
    """
    raise NotImplementedError
