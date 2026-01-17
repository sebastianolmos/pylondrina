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
