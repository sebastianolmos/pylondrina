"""
Pylondrina: Librería Python para trabajar con el formato Golondrina.
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("pylondrina")
except PackageNotFoundError:  # cuando aún no está instalada
    __version__ = "0.0.0"

__all__ = ["__version__"]