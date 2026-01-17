# -------------------------
# file: pylondrina/sources/profile.py
# -------------------------
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

import pandas as pd

from ..types import FieldCorrespondence, ValueCorrespondence
from ..schema import TripSchema


@dataclass(frozen=True)
class SourceProfile:
    """
    Perfil/adaptador para una fuente específica (EOD, XDR, ADATRAP, Scooters, etc.).

    Parameters
    ----------
    name : str
        Nombre corto de la fuente (p. ej., "EOD").
    description : str
        Descripción humana del perfil.
    default_field_correspondence : mapping, optional
        Mapeo recomendado de campos estándar -> columnas típicas de la fuente.
    default_value_correspondence : mapping, optional
        Mapeo recomendado de valores categóricos de la fuente -> valores canónicos.
    preprocess : callable, optional
        Función que recibe el DataFrame fuente y retorna DataFrame ajustado para importación genérica.
        Útil para: unir tablas, decodificar IDs, normalizar etapas, etc.
    schema_override : TripSchema, optional
        Si la fuente requiere una variante de esquema, puede proveerse aquí (evitar si no es necesario).

    Notes
    -----
    - La regla general es mantener el schema base y resolver diferencias con correspondencias y preprocesamiento.
    """
    name: str
    description: str
    default_field_correspondence: Optional[FieldCorrespondence] = None
    default_value_correspondence: Optional[ValueCorrespondence] = None
    preprocess: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None
    schema_override: Optional[TripSchema] = None
