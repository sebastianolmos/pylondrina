"""
Built-in SourceProfiles (registro inmediato).

Este módulo existe como conveniencia para notebooks: al importarlo se registran perfiles
mínimos con nombres estables (EOD/ADATRAP; trips/stages). Los perfiles aquí definidos
son intencionalmente "delgados": usan `preprocess=_noop` y describen qué tablas auxiliares
o factories se recomiendan para un uso real.

Diseño (v1.1)
-------------
- Estos builtins son un *esqueleto* para demostrar la UX del registry; en la implementación
  “real” (v1.1) despues se reemplazarán por perfiles creados vía factories
  (closures con tablas auxiliares), o se mantendrán como perfiles “base” sin decodificación.


"""

from __future__ import annotations

import pandas as pd

from .profile import SourceProfile
from .registry import register_profile


def _noop(df: pd.DataFrame) -> pd.DataFrame:
    """Preprocess nulo: retorna el DataFrame sin modificaciones."""
    return df


# ---------------------------------------------------------------------
# EOD_TRIPS (placeholder)
# ---------------------------------------------------------------------
register_profile(
    SourceProfile(
        name="EOD_TRIPS",
        description=(
            "EOD: tabla de viajes (viajes resumidos). "
            "Perfil base (placeholder) sin decodificación. "
            "Para un uso real, se recomienda un preprocess con joins a catálogos "
            "(modo/motivo) y opcionalmente Personas/Hogares (factory con closure)."
        ),
        preprocess=_noop,
        schema_override=None,
        # Opcional en v1.1: defaults de correspondencia para esta fuente.
        # default_field_correspondence=...,
        # default_value_correspondence=...,
    )
)


# ---------------------------------------------------------------------
# EOD_STAGES (placeholder)
# ---------------------------------------------------------------------
register_profile(
    SourceProfile(
        name="EOD_STAGES",
        description=(
            "EOD: tabla de etapas/movimientos (1 fila = 1 etapa). "
            "Perfil base (placeholder) sin construcción de secuencia. "
            "Para un uso real, se recomienda un preprocess que construya `movement_seq` "
            "(por trip_id + orden o timestamp) y decodifique modo/motivo con catálogos."
        ),
        preprocess=_noop,
        schema_override=None,
    )
)


# ---------------------------------------------------------------------
# ADATRAP_TRIPS (placeholder)
# ---------------------------------------------------------------------
register_profile(
    SourceProfile(
        name="ADATRAP_TRIPS",
        description=(
            "ADATRAP: viajes resumidos (1 fila = 1 viaje). "
            "Perfil base (placeholder) sin resolución de coordenadas. "
            "Para un uso real, se recomienda un preprocess que mapee códigos O/D "
            "de paradero/estación a lat/lon usando una tabla auxiliar (stops)."
        ),
        preprocess=_noop,
        schema_override=None,
    )
)


# ---------------------------------------------------------------------
# ADATRAP_STAGES (placeholder)
# ---------------------------------------------------------------------
register_profile(
    SourceProfile(
        name="ADATRAP_STAGES",
        description=(
            "ADATRAP: etapas/movimientos. Frecuente formato wide (hasta 4 etapas por fila). "
            "Perfil base (placeholder) sin wide->long ni secuencia. "
            "Para un uso real, se recomienda un preprocess que: "
            "(i) haga wide->long (1 fila por etapa presente), "
            "(ii) cree `movement_seq`, "
            "(iii) resuelva coordenadas con tabla de paraderos/estaciones."
        ),
        preprocess=_noop,
        schema_override=None,
    )
)