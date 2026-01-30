# -------------------------
# file: pylondrina/sources/helpers.py
# -------------------------
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import pandas as pd

from ..datasets import TripDataset
from ..importing import ImportOptions, import_trips_from_dataframe
from ..reports import ImportReport
from ..schema import TripSchema
from ..types import FieldCorrespondence, ValueCorrespondence
from .registry import get_profile
from .profile import SourceProfile


def import_trips_from_source(
    profile_name: str,
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
    Importa viajes usando un perfil de fuente registrado (SourceProfile) y la importación genérica.

    Este helper aplica el patrón:
    - cargar perfil de fuente (defaults),
    - preprocesar el DataFrame si el perfil lo define,
    - combinar defaults con overrides del usuario,
    - delegar en `import_trips_from_dataframe(...)`.

    Parameters
    ----------
    profile_name : str
        Nombre del perfil (p. ej., "EOD", "XDR", "ADATRAP", "SCOOTERS").
    df : pandas.DataFrame
        DataFrame fuente.
    schema : TripSchema
        Esquema base Golondrina a aplicar.
    source_name : str, optional
        Nombre de la fuente a registrar en metadatos. Si None, se usa `profile_name`.
    options : ImportOptions, optional
        Opciones de importación (strict, conservar extras, etc.).
    field_correspondence : mapping, optional
        Overrides del usuario para correspondencia de campos (campo estándar -> columna fuente).
        Si se entrega, tiene prioridad por sobre el default del perfil.
    value_correspondence : mapping, optional
        Overrides del usuario para correspondencia de valores categóricos.
    provenance : dict, optional
        Metadatos extra (periodo, zona, versión del dataset original, etc.).
    h3_resolution : int, default=8
        Resolución H3 a utilizar para derivar índices de celdas (origen/destino) cuando sea aplicable.
        Se registra en metadatos para reproducibilidad.

    Returns
    -------
    dataset : TripDataset
        Dataset en formato Golondrina.
    report : ImportReport
        Reporte de importación/estandarización.

    Notes
    -----
    - Este helper NO reemplaza la importación genérica; sólo la configura según un perfil.
    - El perfil puede incluir `preprocess(df)` para unir tablas, decodificar IDs, etc.
    """
    raise NotImplementedError

def import_trips_from_profile(
    profile: SourceProfile,
    df: pd.DataFrame,
    *,
    schema: Optional[TripSchema] = None,
    source_name: Optional[str] = None,
    options: Optional[ImportOptions] = None,
    field_correspondence: Optional[FieldCorrespondence] = None,
    value_correspondence: Optional[ValueCorrespondence] = None,
    provenance: Optional[Dict[str, Any]] = None,
    h3_resolution: int = 8,
) -> Tuple[TripDataset, ImportReport]:
    """
    Importa viajes aplicando directamente un `SourceProfile` (sin usar el sistema de registry).

    Esta función es equivalente a `import_trips_from_source(...)`, pero recibe el perfil como
    objeto. Está pensada para uso “directo” en notebooks/experimentos: puedes construir un
    `SourceProfile` con `preprocess` como closure (capturando tablas auxiliares) y llamar a
    importación sin registrar nada.


    Parameters
    ----------
    profile : SourceProfile
        Perfil/adaptador de fuente que define defaults (correspondencias), preprocess y/o schema_override.
    df : pandas.DataFrame
        DataFrame fuente.
    schema : TripSchema, optional
        Esquema base Golondrina. Se usa si el perfil no define `schema_override`.
    source_name : str, optional
        Nombre de fuente a registrar en metadatos. Si None, se usa `profile.name`.
    options : ImportOptions, optional
        Opciones y políticas de importación (strict, tratamiento de desconocidos, etc.).
    field_correspondence : mapping, optional
        Overrides del usuario para mapeo de campos (campo estándar -> columna fuente).
    value_correspondence : mapping, optional
        Overrides del usuario para mapeo de valores categóricos por campo.
    provenance : dict, optional
        Procedencia externa del dataset (JSON-serializable).
    h3_resolution : int, default=8
        Resolución H3 a utilizar para derivación de índices OD cuando aplique.

    Returns
    -------
    dataset : TripDataset
        Dataset de viajes en formato Golondrina.
    report : ImportReport
        Reporte de importación (issues + summary + trazabilidad).

    Raises
    ------
    ValueError
        Si no se dispone de un `TripSchema` (ni por `schema` ni por `profile.schema_override`)
        o si `h3_resolution` es inválida.
    ImportError
        Si la política de importación determina abortar ante errores estructurales (mapeos/derivaciones
        imposibles para campos obligatorios, etc.).
    """
    raise NotImplementedError