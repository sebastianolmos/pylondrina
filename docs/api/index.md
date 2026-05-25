# Referencia API

Esta sección contiene la referencia técnica de la API pública de Pylondrina v1.1.

A diferencia del manual de operaciones, que explica cuándo usar cada operación dentro de un pipeline, esta referencia está orientada a consultar nombres exactos, módulos, firmas, parámetros, tipos de retorno, opciones y clases públicas.

La documentación se genera desde los docstrings del código fuente mediante `mkdocstrings`.

## Organización

| Página | Contenido |
|---|---|
| [Trips](trips.md) | Funciones públicas para importar, validar, corregir, limpiar, filtrar, escribir y leer `TripDataset`. |
| [Flows](flows.md) | Funciones públicas para construir, exportar, persistir, filtrar e inspeccionar `FlowDataset`. |
| [Traces](traces.md) | Funciones públicas para importar traces, validarlas e inferir trips desde puntos discretos. |
| [Datasets y reportes](datasets-and-reports.md) | Clases públicas transversales: datasets, reportes, schemas y excepciones. |

## Qué consultar aquí

Esta referencia sirve cuando el usuario ya conoce qué función, clase u objeto necesita revisar y requiere precisión técnica sobre su interfaz.

En particular, permite consultar:

- rutas de importación;
- firmas de funciones;
- objetos de configuración;
- tipos de retorno;
- reportes asociados;
- excepciones públicas;
- notas técnicas incluidas en los docstrings.

Para entender el orden de uso de las operaciones o el rol de cada una dentro del pipeline, se recomienda consultar primero el manual de operaciones o la guía de usuario.

## Relación con otras secciones

- Para entender el contrato de datos, consultar [Golondrina](../golondrina/overview.md).
- Para aprender flujos de uso completos, consultar la [Guía de usuario](../user-guide/index.md).
- Para revisar el propósito de una operación específica, consultar [Operaciones](../operations/index.md).

## Criterio de inclusión

Esta referencia documenta objetos públicos explícitos. No se generan páginas completas por módulo, para evitar exponer helpers internos o detalles de implementación que no forman parte de la superficie estable de Pylondrina v1.1.

El criterio general es documentar:

- funciones públicas principales de las operaciones OP-01 a OP-16;
- dataclasses públicas de opciones;
- datasets principales;
- reportes públicos;
- schemas públicos;
- excepciones tipadas públicas.

Los helpers internos, utilidades auxiliares y funciones privadas quedan fuera de esta referencia.