# -------------------------
# file: pylondrina/errors.py
# -------------------------
from __future__ import annotations

from typing import Any, Optional, Sequence, TYPE_CHECKING
from pprint import pformat

if TYPE_CHECKING:
    from pylondrina.reports import Issue  # solo para type hints

class PylondrinaError(Exception):
    """
    Excepción base para errores del módulo Pylondrina.

    Atributos extra para trazabilidad:
    - code: código estable (idealmente igual a Issue.code)
    - details: dict JSON-safe con contexto
    - issue: Issue gatillante (si aplica)
    - issues: snapshot de Issues acumuladas hasta el fallo (si aplica)
    """
    def __init__(
        self,
        message: str,
        *,
        code: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
        issue: Optional["Issue"] = None,
        issues: Optional[Sequence["Issue"]] = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.code = code
        self.details = details
        self.issue = issue
        # snapshot inmutable (evita que se modifique la evidencia por accidente)
        self.issues = tuple(issues) if issues is not None else None

    def __str__(self) -> str:
        """
        Representación legible para print(error) o str(error).
        """
        lines = [f"{self.__class__.__name__}: {self.message}"]

        if self.code is not None:
            lines.append(f"code: {self.code}")

        if self.details is not None:
            lines.append("details:")
            lines.append(pformat(self.details, width=88, sort_dicts=False))

        if self.issue is not None:
            lines.append(f"issue: {self.issue!r}")

        if self.issues is not None:
            lines.append(f"issues_count: {len(self.issues)}")
            lines.append("issues:")
            lines.append(pformat(self.issues, width=88))

        return "\n".join(lines)

    def __repr__(self) -> str:
        """
        Representación útil para debugging / notebooks / inspección interactiva.
        """
        return (
            f"{self.__class__.__name__}("
            f"message={self.message!r}, "
            f"code={self.code!r}, "
            f"details={self.details!r}, "
            f"issue={self.issue!r}, "
            f"issues={self.issues!r})"
        )


class SchemaError(PylondrinaError):
    """
    Error asociado a la definición, construcción o uso de un esquema (TripSchema/TraceSchema).

    Ejemplos típicos:
    - El esquema declara un campo obligatorio inexistente.
    - El esquema contiene reglas contradictorias (p. ej., restricciones incompatibles).
    - Se solicita validar un dataset con un esquema no compatible.
    """
    def __init__(
        self,
        message: str,
        *,
        code: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
        issue: Optional["Issue"] = None,
        issues: Optional[Sequence["Issue"]] = None,
    ) -> None:
        super().__init__(message, code=code, details=details, issue=issue, issues=issues)


class ImportError(PylondrinaError):
    """
    Error durante la importación/conversión de una fuente externa hacia el formato Golondrina.

    Ejemplos típicos:
    - No se puede construir el dataset por ausencia de información mínima.
    - La correspondencia de campos no permite resolver campos obligatorios.
    - Tipos de datos de entrada imposibilitan la conversión (p. ej., timestamps ilegibles).

    Incluye errores asociados a correspondencias (campos/valores) durante importación y normalización.
    """
    def __init__(
        self,
        message: str,
        *,
        code: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
        issue: Optional["Issue"] = None,
        issues: Optional[Sequence["Issue"]] = None,
    ) -> None:
        super().__init__(message, code=code, details=details, issue=issue, issues=issues)

class ValidationError(PylondrinaError):
    """
    Error de validación de conformidad respecto de un esquema Golondrina.

    Ejemplos típicos:
    - Falta un campo obligatorio.
    - Valores violan restricciones de tipo o formato.
    - Reglas temporales/espaciales mínimas no se cumplen (cuando la operación lo exige).
    """
    def __init__(
        self,
        message: str,
        *,
        code: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
        issue: Optional["Issue"] = None,
        issues: Optional[Sequence["Issue"]] = None,
    ) -> None:
        super().__init__(message, code=code, details=details, issue=issue, issues=issues)


class InferenceError(PylondrinaError):
    """
    Error al inferir viajes desde datos de trazas/trayectorias.

    Ejemplos típicos:
    - Los datos de entrada no cumplen el esquema mínimo de trazas.
    - Parámetros del algoritmo imposibilitan inferir viajes (p. ej., sin puntos suficientes).
    - El resultado no puede producir un conjunto de viajes conforme al esquema de viajes.
    """
    def __init__(
        self,
        message: str,
        *,
        code: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
        issue: Optional["Issue"] = None,
        issues: Optional[Sequence["Issue"]] = None,
    ) -> None:
        super().__init__(message, code=code, details=details, issue=issue, issues=issues)


class ExportError(PylondrinaError):
    """
    Error durante la exportación/serialización de datasets (p. ej., Parquet, GeoJSON, Flowmap).

    Ejemplos típicos:
    - Falta información necesaria para el formato de salida (p. ej., geometrías).
    - Columnas requeridas por el exportador no están presentes.
    - Fallos de escritura en el destino (p. ej., permisos o ruta inválida).
    """
    def __init__(
        self,
        message: str,
        *,
        code: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
        issue: Optional["Issue"] = None,
        issues: Optional[Sequence["Issue"]] = None,
    ) -> None:
        super().__init__(message, code=code, details=details, issue=issue, issues=issues)
