# -------------------------
# file: pylondrina/validation.py
# -------------------------
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .reports import Issue, ValidationReport
from .schema import TripSchema


def validate_trips(
    df: pd.DataFrame,
    schema: TripSchema,
    *,
    domains_effective: Optional[Dict[str, Any]] = None,
    strict: bool = False,
) -> ValidationReport:
    """
    Valida un DataFrame de viajes contra el TripSchema.

    La validación se orienta a:
    - presencia de campos obligatorios,
    - tipos y formatos,
    - restricciones declaradas (rangos, formatos, reglas),
    - dominios categóricos (base y/o efectivos),
    - invariantes mínimas del formato (p. ej., “origen y destino no pueden estar ambos ausentes”).

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame en nombres estándar.
    schema : TripSchema
        Esquema del formato Golondrina.
    domains_effective : dict, optional
        Dominios efectivos del dataset. Si se proveen, se valida coherencia con dominios usados.
    strict : bool, default=False
        Si True, la presencia de errores puede gatillar excepciones en niveles superiores.

    Returns
    -------
    ValidationReport
        Reporte con hallazgos y resumen.
    """
    raise NotImplementedError


def check_required_fields(
    df: pd.DataFrame,
    schema: TripSchema,
) -> List[Issue]:
    """
    Revisa que todos los campos obligatorios del esquema estén presentes.

    Returns
    -------
    list[Issue]
        Hallazgos por campos obligatorios ausentes.
    """
    raise NotImplementedError


def check_types_and_formats(
    df: pd.DataFrame,
    schema: TripSchema,
) -> List[Issue]:
    """
    Revisa conformidad de tipos/formatos según FieldSpec (p. ej., datetime parseable).

    Returns
    -------
    list[Issue]
        Hallazgos por incompatibilidades de tipo/formato.
    """
    raise NotImplementedError


def check_constraints(
    df: pd.DataFrame,
    schema: TripSchema,
) -> List[Issue]:
    """
    Revisa restricciones declaradas (rangos, no-negativo, validez H3, etc.).

    Returns
    -------
    list[Issue]
        Hallazgos por violaciones de restricciones.
    """
    raise NotImplementedError


def check_domains(
    df: pd.DataFrame,
    schema: TripSchema,
    *,
    domains_effective: Optional[Dict[str, Any]] = None,
) -> List[Issue]:
    """
    Revisa que valores categóricos estén dentro del dominio base o efectivo (si se proporciona).

    Returns
    -------
    list[Issue]
        Hallazgos por valores fuera de dominio.
    """
    raise NotImplementedError


def build_validation_summary(issues: List[Issue]) -> Dict[str, Any]:
    """
    Construye un resumen estructurado del reporte de validación.

    Returns
    -------
    dict
        Conteos por severidad/código, cobertura de campos, etc.
    """
    raise NotImplementedError
