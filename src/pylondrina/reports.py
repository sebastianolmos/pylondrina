# -------------------------
# file: pylondrina/reports.py
# -------------------------
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from pprint import pformat

from pylondrina.types import FieldName, IssueLevel


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
class OperationReport:
    """
    Reporte genérico para operaciones que procesan o transforman datasets (p. ej., fix/clean/filter/write/build).

    Attributes
    ----------
    ok:
        True si la operación terminó sin issues de nivel ERROR. False si hubo al menos un ERROR.
    issues:
        Lista de Issue emitidos por la operación. Deben ser JSON-serializables en `Issue.details`.
    summary:
        Diccionario serializable (JSON) con el resumen mínimo estable de la operación.
    parameters:
        Diccionario serializable (JSON) con parámetros efectivos de ejecución.
        - Puede ser None cuando la operación no define/usa parameters explícitos (p. ej., validación),
          o cuando los parámetros quedan registrados en otra parte (p. ej., metadata["events"]).
        - Para operaciones de transformación (clean/fix/filter), se recomienda incluirlo para trazabilidad.
    """
    ok: bool
    issues: List["Issue"] = field(default_factory=list)
    summary: Dict[str, Any] = field(default_factory=dict)
    parameters: Optional[Dict[str, Any]] = None


@dataclass
class ValidationReport(OperationReport):
    """
    Reporte de validación de TripDataset.

    Hereda de OperationReport para unificar la forma de reportar (issues + summary).
    """
    pass


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

    def to_display_dict(self) -> Dict[str, Any]:
        """
        Devuelve una versión estructurada del reporte pensada para impresión legible.
        """
        return {
            "type": self.__class__.__name__,
            "ok": self.ok,
            "schema_version": self.schema_version,
            "issues_count": len(self.issues),
            "issues": [
                {
                    "level": issue.level,
                    "code": issue.code,
                    "message": issue.message,
                    "field": issue.field,
                    "source_field": issue.source_field,
                    "row_count": issue.row_count,
                    "details": issue.details,
                }
                for issue in self.issues
            ],
            "summary": self.summary,
            "parameters": self.parameters,
            "field_correspondence": self.field_correspondence,
            "value_correspondence": self.value_correspondence,
            "metadata": self.metadata,
        }

    def __str__(self) -> str:
        """
        Salida legible para print(report).
        """
        parts = [
            f"{self.__class__.__name__}",
            "-" * len(self.__class__.__name__),
            f"ok: {self.ok}",
            f"schema_version: {self.schema_version}",
            f"issues_count: {len(self.issues)}",
        ]

        if self.issues:
            parts.append("\nissues:")
            for i, issue in enumerate(self.issues, start=1):
                parts.append(f"  [{i}] {issue.level.upper()} - {issue.code}")
                parts.append(f"      message: {issue.message}")
                if issue.field is not None:
                    parts.append(f"      field: {issue.field}")
                if issue.source_field is not None:
                    parts.append(f"      source_field: {issue.source_field}")
                if issue.row_count is not None:
                    parts.append(f"      row_count: {issue.row_count}")
                if issue.details is not None:
                    formatted_details = pformat(issue.details, width=88, sort_dicts=False)
                    indented = "\n".join(f"      {line}" for line in formatted_details.splitlines())
                    parts.append("      details:")
                    parts.append(indented)

        if self.summary:
            parts.append("\nsummary:")
            parts.append(pformat(self.summary, width=100, sort_dicts=False))

        if self.parameters:
            parts.append("\nparameters:")
            parts.append(pformat(self.parameters, width=100, sort_dicts=False))

        if self.field_correspondence:
            parts.append("\nfield_correspondence:")
            parts.append(pformat(self.field_correspondence, width=100, sort_dicts=False))

        if self.value_correspondence:
            parts.append("\nvalue_correspondence:")
            parts.append(pformat(self.value_correspondence, width=100, sort_dicts=False))

        if self.metadata:
            parts.append("\nmetadata:")
            parts.append(pformat(self.metadata, width=100, sort_dicts=False))

        return "\n".join(parts)

    def __repr__(self) -> str:
        """
        Representación compacta pero legible para inspección.
        """
        data = self.to_display_dict()
        return pformat(data, width=100, sort_dicts=False)

    def _repr_pretty_(self, p, cycle) -> None:
        """
        Soporte para display bonito en IPython/Jupyter.
        """
        if cycle:
            p.text(f"{self.__class__.__name__}(...)")
        else:
            p.text(str(self))


@dataclass
class InferenceReport:
    """
    Reporte de inferencia de viajes desde trazas discretas.

    Attributes
    ----------
    ok : bool
        True si la operación terminó sin issues de nivel ERROR.
    issues : list[Issue]
        Hallazgos agregados emitidos durante el pipeline de inferencia.
    summary : dict
        Resumen mínimo y estable de la inferencia (puntos, candidatos, descartes y viajes).
    parameters : dict
        Parámetros efectivos de ejecución, serializables y alineados con el evento.
    metadata : dict
        Espacio opcional para contexto adicional del reporte cuando aplique.
    """
    ok: bool
    issues: List[Issue] = field(default_factory=list)
    summary: Dict[str, Any] = field(default_factory=dict)
    parameters: Dict[str, Any] = field(default_factory=dict)
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