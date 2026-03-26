# -------------------------
# file: pylondrina/sources/helpers.py
# -------------------------
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Optional, Tuple

import pandas as pd

from pylondrina.datasets import TripDataset
from pylondrina.importing import ImportOptions, import_trips_from_dataframe
from pylondrina.reports import ImportReport
from pylondrina.schema import TripSchema
from pylondrina.types import FieldCorrespondence, ValueCorrespondence
from pylondrina.sources.profile import SourceProfile

def _merge_field_correspondence(
    base: Optional[FieldCorrespondence],
    override: Optional[FieldCorrespondence],
) -> Optional[Dict[str, str]]:
    if base is None and override is None:
        return None

    merged: Dict[str, str] = {}
    if base is not None:
        merged.update(dict(base))
    if override is not None:
        merged.update(dict(override))
    return merged


def _merge_value_correspondence(
    base: Optional[ValueCorrespondence],
    override: Optional[ValueCorrespondence],
) -> Optional[Dict[str, Dict[str, str]]]:
    if base is None and override is None:
        return None

    merged: Dict[str, Dict[str, str]] = {}

    if base is not None:
        for field, mapping in base.items():
            merged[field] = dict(mapping)

    if override is not None:
        for field, mapping in override.items():
            if field not in merged:
                merged[field] = dict(mapping)
            else:
                merged[field].update(dict(mapping))

    return merged


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
    effective_schema = profile.schema_override if profile.schema_override is not None else schema
    if effective_schema is None:
        raise ValueError(
            "No se dispone de un TripSchema efectivo: entrega `schema` o define "
            "`profile.schema_override`."
        )

    # Regla simple y no ambigua:
    # - si el usuario entrega `options`, se usan esas;
    # - si no, se usan `profile.default_options`;
    # - si tampoco hay, se deja en None y el import genérico resolverá defaults.
    effective_options = options if options is not None else profile.default_options

    effective_field_correspondence = _merge_field_correspondence(
        profile.default_field_correspondence,
        field_correspondence,
    )
    effective_value_correspondence = _merge_value_correspondence(
        profile.default_value_correspondence,
        value_correspondence,
    )

    effective_source_name = source_name or profile.name

    effective_provenance: Dict[str, Any] = deepcopy(provenance) if provenance is not None else {}
    effective_provenance.setdefault(
        "source_profile",
        {
            "name": profile.name,
            "description": profile.description,
        },
    )

    work = df.copy(deep=True)
    if profile.preprocess is not None:
        work = profile.preprocess(work)
        if not isinstance(work, pd.DataFrame):
            raise TypeError(
                "`profile.preprocess(...)` debe retornar un pandas.DataFrame."
            )

    return import_trips_from_dataframe(
        work,
        effective_schema,
        source_name=effective_source_name,
        options=effective_options,
        field_correspondence=effective_field_correspondence,
        value_correspondence=effective_value_correspondence,
        provenance=effective_provenance,
        h3_resolution=h3_resolution,
    )