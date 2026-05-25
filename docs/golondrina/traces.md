# Traces / points

La representación de **traces / points** describe datos de movilidad basados en puntos espacio-temporales discretos. En Golondrina, una trace no se interpreta como una trayectoria GPS densa ni como una reconstrucción continua del recorrido, sino como una secuencia ordenable de observaciones asociadas a un usuario, dispositivo o entidad.

Esta decisión responde al alcance de v1.1: se implementó soporte para trazas discretas, como check-ins, stay-points o registros puntuales de presencia, y no para inferencia avanzada sobre trayectorias densas.

## Conceptos principales

| Concepto | Descripción |
|---|---|
| `point` | Observación espacio-temporal individual. |
| `trace` | Secuencia de puntos asociados a un mismo usuario o entidad. |
| `stay point` | Punto discreto que representa presencia o permanencia en un lugar. |
| `TraceDataset` | Dataset de puntos trazables y operables por Pylondrina. |

Una trace puede permitir derivar trips simples cuando los puntos tienen orden temporal y coordenadas usables. Sin embargo, una trace no equivale directamente a un viaje. La derivación Trace → Trip requiere una operación explícita de inferencia.

## Propósito de la representación

La representación de traces permite:

- estructurar puntos espacio-temporales heterogéneos bajo un núcleo común;
- validar presencia de campos mínimos;
- conservar metadata de temporalidad y procedencia;
- usar trazas discretas como base para inferir trips simples;
- conectar fuentes tipo check-in o stay-point con el pipeline de trips y flows.

## Núcleo canónico de traces

El núcleo mínimo de traces en v1.1 está compuesto por cinco campos.

| Campo | Tipo esperado | Descripción |
|---|---|---|
| `point_id` | string | Identificador único del punto. |
| `user_id` | string | Identificador del usuario, dispositivo o entidad observada. |
| `time_utc` | datetime | Instante asociado al punto, idealmente normalizado a UTC. |
| `latitude` | float | Latitud del punto en EPSG:4326. |
| `longitude` | float | Longitud del punto en EPSG:4326. |

Este núcleo es deliberadamente pequeño. Su objetivo es habilitar importación, validación mínima e inferencia austera de trips desde puntos discretos.

## Entrada mínima compatible

Una fuente de traces no necesita llegar originalmente con los nombres canónicos. Puede usar nombres propios, siempre que sea posible mapearlos hacia el núcleo mediante `field_correspondence`.

Ejemplo conceptual:

| Campo de fuente | Campo Golondrina |
|---|---|
| `id_registro` | `point_id` |
| `id_usuario` | `user_id` |
| `timestamp` | `time_utc` |
| `lat` | `latitude` |
| `lon` | `longitude` |

Si `point_id` no existe, Pylondrina puede generarlo durante la importación. En cambio, `user_id`, `time_utc`, `latitude` y `longitude` deben poder materializarse para que el dataset sea utilizable.

## Temporalidad

La temporalidad de traces se concentra en `time_utc`.

Durante la importación, el sistema puede interpretar tiempos usando una zona horaria declarada cuando la fuente no entrega offset o zona explícita. El resultado debe dejar trazabilidad sobre cómo se resolvió la temporalidad.

La metadata temporal puede registrar información como:

- campo temporal utilizado;
- zona horaria declarada;
- si el tiempo fue normalizado a UTC;
- si la fuente ya contenía tiempo interpretable;
- limitaciones de interpretación temporal.

## Espacialidad

Los puntos de traces usan coordenadas geográficas en EPSG:4326:

| Campo | Descripción |
|---|---|
| `latitude` | Latitud del punto. |
| `longitude` | Longitud del punto. |

A diferencia de trips, el núcleo de traces no exige campos H3 en v1.1. La derivación H3 aparece principalmente al inferir trips desde traces, porque el resultado de OP-16 debe ser compatible con el pipeline de trips y flows.

## Campos extendidos

Las traces pueden conservar campos adicionales como extensiones compatibles. Estos campos no forman parte del núcleo mínimo, pero pueden ser útiles para análisis o inferencia posterior.

Ejemplos:

| Campo extendido | Uso posible |
|---|---|
| `location_ref` | Identificador de lugar, estación, punto de interés o zona. |
| `location_category` | Tipo de lugar observado. |
| `accuracy` | Precisión reportada por la fuente. |
| `source_event_id` | Identificador original de la fuente. |
| `device_type` | Tipo de dispositivo o mecanismo de registro. |

Estos campos pueden propagarse hacia trips inferidos si la operación de inferencia se configura explícitamente para hacerlo.

## Validación de traces

La validación de traces en v1.1 es mínima y controlada. Su objetivo es certificar que el `TraceDataset` contiene el núcleo esperado y que los campos básicos son interpretables.

La validación puede revisar:

- campos requeridos;
- tipos y formatos;
- constraints simples;
- monotonicidad temporal por usuario, cuando corresponde.

No realiza corrección, imputación, clustering ni inferencia. Tampoco transforma el dataframe. Su función es producir evidencia de conformidad mínima y actualizar el estado de validación del dataset.

## Inferencia austera Trace → Trip

La representación de traces se conecta con trips mediante `infer_trips_from_traces`.

En v1.1 se implementaron dos modos de inferencia:

| Modo | Descripción |
|---|---|
| `consecutive_points` | Construye movements entre puntos consecutivos del mismo usuario. |
| `consecutive_clusters` | Agrupa puntos cercanos en espacio-tiempo antes de construir movements entre clusters consecutivos. |

La inferencia no busca reconstruir una trayectoria completa. Su objetivo es derivar trips OD simples desde observaciones discretas, manteniendo una semántica trazable y compatible con el resto del módulo.

## Relación con operaciones de Pylondrina

| Operación | Relación con traces |
|---|---|
| OP-14 `import_traces_from_dataframe` | Construye un `TraceDataset` desde una fuente tabular. |
| OP-15 `validate_traces` | Certifica conformidad mínima del `TraceDataset`. |
| OP-16 `infer_trips_from_traces` | Deriva un `TripDataset` desde puntos discretos. |

Después de OP-16, el resultado ya pertenece a la representación de trips y puede recorrer operaciones como `validate_trips`, `filter_trips`, `build_flows` o `write_trips`.

## Alcance y limitaciones

El soporte de traces en v1.1 tiene un alcance intencionalmente acotado.

Sí cubre:

- puntos espacio-temporales discretos;
- check-ins o stay-points;
- validación mínima;
- inferencia simple hacia trips;
- propagación controlada de campos extra.

No cubre:

- GPS denso;
- map matching;
- detección avanzada de estadías;
- inferencia multimodal;
- reconstrucción robusta de rutas;
- trayectorias continuas de alta frecuencia.

Esta limitación no representa una falla del contrato, sino una decisión explícita de alcance del MVP.

## Resumen

La representación de traces permite incorporar fuentes basadas en observaciones discretas de movilidad y conectarlas con el pipeline general de Pylondrina. Su diseño mantiene un núcleo mínimo estable, evita prometer inferencia avanzada y permite derivar trips simples de manera trazable cuando la fuente contiene secuencias espacio-temporales suficientes.