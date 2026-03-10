# pylondrina/types.py
from __future__ import annotations

from typing import Any, Literal, Mapping, TypeAlias


# -----------------------------------------------------------------------------
# Aliases semánticos (representación textual)
# -----------------------------------------------------------------------------
FieldName: TypeAlias = str
"""Nombre estándar de un campo definido por el formato Golondrina (p. ej., 'origin_time', 'purpose')."""

SourceFieldName: TypeAlias = str
"""Nombre de un campo tal como aparece en una fuente externa (p. ej., 'FECHA_INI', 'MOTIVO')."""

DomainValue: TypeAlias = str
"""Valor categórico dentro del dominio de un campo (p. ej., 'work', 'study', 'health')."""

# -----------------------------------------------------------------------------
# Correspondencias (alineación semántica / estandarización)
# -----------------------------------------------------------------------------
FieldCorrespondence: TypeAlias = Mapping[FieldName, SourceFieldName]
"""
Correspondencia de campos entre el formato Golondrina y una fuente externa.

La clave es el nombre estándar del campo en Golondrina y el valor es el nombre del campo
en la fuente externa.

Ejemplo
-------
{"origin_time": "FECHA_HORA_INI", "purpose": "MOTIVO", "mode": "MODO"}
"""

ValueCorrespondence: TypeAlias = Mapping[FieldName, Mapping[DomainValue, DomainValue]]
"""
Estandarización de valores categóricos (dominios) por campo.

Permite mapear valores originales (de la fuente externa) hacia valores estándar del dominio
definido por Golondrina.

Ejemplo
-------
{
    "purpose": {"Trabajo": "work", "Estudio": "study"},
    "mode": {"Bus": "bus", "Metro": "metro"}
}
"""

# -----------------------------------------------------------------------------
# Tipos auxiliares frecuentes
# -----------------------------------------------------------------------------
IssueLevel: TypeAlias = Literal["info", "warning", "error"]
"""
Nivel de severidad para issues reportados por validación/estandarización.

- 'info'    : registro informativo y trazabilidad.
- 'warning' : se detecta una condición potencialmente problemática; se puede continuar.
- 'error'   : incumplimiento de reglas obligatorias del esquema; puede bloquear la operación en modo estricto.
"""
