# -------------------------
# file: pylondrina/sources/registry.py
# -------------------------
from __future__ import annotations

from typing import Dict, List

from .profile import SourceProfile

_REGISTRY: Dict[str, SourceProfile] = {}


def register_profile(profile: SourceProfile) -> None:
    """
    Registra un perfil de fuente.

    Parameters
    ----------
    profile : SourceProfile
        Perfil a registrar.
    """
    _REGISTRY[profile.name] = profile


def get_profile(name: str) -> SourceProfile:
    """
    Obtiene un perfil por nombre.

    Raises
    ------
    KeyError
        Si el perfil no existe.
    """
    return _REGISTRY[name]


def list_profiles() -> List[str]:
    """
    Lista nombres de perfiles registrados.
    """
    return sorted(_REGISTRY.keys())
