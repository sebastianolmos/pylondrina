# Guía de usuario

Esta sección reúne guías prácticas para usar Pylondrina en pipelines completos. A diferencia de `operations/`, donde se documenta una operación a la vez, estas páginas muestran cómo encadenar varias operaciones para resolver tareas frecuentes de trabajo con datos de movilidad urbana.

Pylondrina v1.1 se diseñó como una librería operacional para construir, validar, transformar, derivar, exportar y persistir datasets bajo el contrato Golondrina. Por eso, muchas tareas reales no se resuelven con una sola función, sino mediante secuencias reproducibles de operaciones.

## Guías disponibles

| Guía | Qué explica |
|---|---|
| [Construir flows desde trips](trips-to-flows.md) | Cómo partir desde una fuente tabular de viajes, construir un `TripDataset`, validarlo, preparar el dataset y construir flows OD. |
| [Inferir trips desde traces](traces-to-trips.md) | Cómo partir desde puntos espacio-temporales discretos, construir un `TraceDataset`, validarlo e inferir un `TripDataset`. |
| [Persistencia, exportación y visualización](persistence-and-viewer.md) | Cómo distinguir entre bundles `.golondrina`, layouts exportados y uso del viewer local. |
| [Issues, reportes y trazabilidad](issues-and-reports.md) | Cómo interpretar reportes, issues, summaries, metadata, eventos, sidecars y estado de validación. |

## Relación con otras secciones

- Para entender el contrato de datos, consultar [Golondrina](../golondrina/overview.md).
- Para revisar una operación específica, consultar [Operaciones](../operations/index.md).
- Para detalles de firmas, parámetros y tipos de retorno, consultar [API](../api/index.md).

## Orden sugerido de lectura

Si se está usando Pylondrina por primera vez, se recomienda partir por:

1. [Issues, reportes y trazabilidad](issues-and-reports.md)
2. [Construir flows desde trips](trips-to-flows.md)
3. [Persistencia, exportación y visualización](persistence-and-viewer.md)
4. [Inferir trips desde traces](traces-to-trips.md), si se trabaja con puntos discretos

Este orden permite entender primero cómo leer la evidencia operacional del módulo y luego aplicar esa lógica a pipelines completos.