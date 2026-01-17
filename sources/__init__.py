# -------------------------
# file: pylondrina/sources/__init__.py
# -------------------------
"""
Soporte por fuente (adaptadores/perfiles).

La librería expone una importación genérica estable:
    import_trips_from_dataframe(df, schema, ...)

y, opcionalmente, perfiles por fuente que proporcionan:
- correspondencias por defecto (campos/valores),
- decodificación de IDs (lookups),
- preprocesamiento específico (unir tablas, manejar etapas, etc.),
sin alterar la API genérica.
"""

from .profile import SourceProfile
from .registry import register_profile, get_profile, list_profiles
