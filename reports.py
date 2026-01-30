# -------------------------
# file: pylondrina/reports.py
# -------------------------
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .types import FieldName, IssueLevel


@dataclass(frozen=True)
class Issue:
    """
    Hallazgo emitido por el sistema durante importación/validación/inferencia.

    Attributes
    ----------
    level : {"info","warning","error"}
        Severidad del hallazgo.
    code : str
        Código estable para clasificación (p. ej. "MISSING_REQUIRED_FIELD").
    message : str
        Descripción humana del hallazgo.
    field : str, optional
        Campo estándar afectado (si aplica).
    source_field : str, optional
        Campo de la fuente afectado (si aplica).
    row_count : int, optional
        Cantidad de filas afectadas (si aplica).
    details : dict, optional
        Información adicional estructurada (si aplica).
    """
    level: IssueLevel
    code: str
    message: str
    field: Optional[FieldName] = None
    source_field: Optional[str] = None
    row_count: Optional[int] = None
    details: Optional[Dict[str, Any]] = None


@dataclass
class ValidationReport:
    """
    Reporte de validación de un dataset contra un esquema.

    Attributes
    ----------
    ok : bool
        True si no existen errores; False en caso contrario.
    issues : list[Issue]
        Lista completa de hallazgos (errores, advertencias, info).
    summary : dict
        Resumen estructurado (p. ej., conteos por tipo, cobertura de campos, etc.).
    """
    ok: bool
    issues: List[Issue] = field(default_factory=list)
    summary: Dict[str, Any] = field(default_factory=dict)
    parameters: Dict[str, Any] = field(default_factory=dict) 


@dataclass
class ImportReport:
    """
    Reporte de importación/conversión desde una fuente externa al formato Golondrina.

    Attributes
    ----------
    ok : bool
        True si la importación produce un dataset utilizable; False si falla.
    issues : list[Issue]
        Hallazgos durante correspondencia, normalización y validación inicial.
    field_correspondence : dict
        Correspondencia final aplicada: campo estándar -> campo fuente.
    value_correspondence : dict
        Correspondencias aplicadas en campos categóricos: campo -> mapeo valores.
    schema_version : str
        Versión del esquema/formato aplicado.
    metadata : dict
        Metadatos mínimos de trazabilidad (fuente, timestamp, etc.).
    """
    ok: bool
    issues: List[Issue] = field(default_factory=list)

    summary: Dict[str, Any] = field(default_factory=dict)      # NUEVO
    parameters: Dict[str, Any] = field(default_factory=dict) 

    field_correspondence: Dict[str, str] = field(default_factory=dict)
    value_correspondence: Dict[str, Dict[str, str]] = field(default_factory=dict)
    schema_version: str = "0.0.0"
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class InferenceReport:
    """
    Reporte de inferencia de viajes desde trazas (GPS/XDR/POIs).

    Attributes
    ----------
    ok : bool
        True si se generó un conjunto de viajes; False si no.
    issues : list[Issue]
        Hallazgos (p. ej., inconsistencias temporales, puntos inválidos, trayectorias incompletas).
    summary : dict
        Resumen de resultados (número de viajes, cobertura, descartes, etc.).
    metadata : dict
        Metadatos de parámetros de inferencia (umbral de detención, ventanas de tiempo, etc.).
    """
    ok: bool
    issues: List[Issue] = field(default_factory=list)
    summary: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FlowBuildReport:
    """
    Reporte de construcción/agregación de flujos desde viajes.

    Attributes
    ----------
    ok : bool
        True si los flujos fueron construidos; False si falla.
    issues : list[Issue]
        Hallazgos (p. ej., viajes sin geocodificación, celdas inválidas, etc.).
    summary : dict
        Resumen (número de flujos, viajes agregados, viajes descartados, etc.).
    metadata : dict
        Parámetros de agregación (resolución H3, bins temporales, umbrales mínimos, filtros, etc.).
    """
    ok: bool
    issues: List[Issue] = field(default_factory=list)
    summary: Dict[str, Any] = field(default_factory=dict)
    parameters: Dict[str, Any] = field(default_factory=dict) 
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ConsistencyReport:
    """
    Reporte de consistencia para datos de trazas (TraceDataset).

    Este reporte resume los resultados de la validación de consistencia
    espacio-temporal aplicada a un conjunto de trazas antes de procesos
    posteriores como la inferencia de viajes.

    Attributes
    ----------
    issues : list[Issue]
        Lista de problemas detectados durante la validación.
        Cada Issue describe una inconsistencia específica, su severidad
        y el contexto asociado (por ejemplo, usuario afectado o tipo de error).
    summary : dict[str, Any]
        Resumen agregado de los resultados de la validación.
        Contiene métricas globales como:
        - número total de puntos y usuarios,
        - conteos de inconsistencias por tipo,
        - usuarios afectados por cada inconsistencia,
        - rangos temporales cubiertos por los datos.
    parameters : dict[str, Any]
        Parámetros efectivos utilizados durante la validación de consistencia.
        Debe incluir los umbrales y checks aplicados (por ejemplo, límites de
        velocidad, tolerancias temporales, CRS y timezone interpretados).

    Notes
    -----
    - Este reporte no modifica los datos de entrada.
    - La presencia de issues no implica necesariamente que los datos sean
      inutilizables; su interpretación depende del tipo de fuente y del
      proceso posterior (por ejemplo, inferencia de viajes).
    - En v1, este reporte es producido por la función
      `validate_trace_consistency(...)`.
    """
    issues: List["Issue"] = field(default_factory=list)
    summary: Dict[str, Any] = field(default_factory=dict)
    parameters: Dict[str, Any] = field(default_factory=dict)