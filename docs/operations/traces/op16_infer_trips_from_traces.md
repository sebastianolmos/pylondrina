# OP-16 Infer trips from traces

`infer_trips_from_traces` es la operación que deriva un `TripDataset` desde un `TraceDataset` de puntos discretos. Se implementó como un puente Trace → Trip: toma trazas ya estructuradas y, preferentemente, validadas, y materializa viajes OD simples compatibles con el resto del pipeline de trips y flows.

La operación no importa fuentes externas, no reemplaza a `import_trips_from_dataframe`, no reconstruye trayectorias continuas, no resuelve GPS denso, no infiere modo de transporte y no escribe artefactos en disco. Su alcance es una inferencia austera desde puntos discretos hacia viajes OD simples.

## Para qué sirve

Esta operación permite convertir secuencias de puntos observados en viajes OD elementales. El resultado queda expresado como un `TripDataset`, por lo que puede ser usado posteriormente por operaciones de trips y flows, por ejemplo:

```text
import_traces_from_dataframe -> validate_traces -> infer_trips_from_traces -> validate_trips -> build_flows
```

OP-16 es útil cuando una fuente no entrega viajes OD directamente, pero sí contiene puntos espacio-temporales ordenables por usuario. Ejemplos típicos son check-ins, puntos de presencia, trazas discretas o telemetría previamente preparada como puntos.

## Modos de inferencia

La operación soporta dos modos:

| Modo | Descripción |
|---|---|
| `consecutive_points` | Construye viajes entre puntos consecutivos del mismo usuario. |
| `consecutive_clusters` | Agrupa secuencialmente puntos cercanos en espacio-tiempo y luego construye viajes entre clusters consecutivos. |

### `consecutive_points`

Este es el modo base. La operación ordena los puntos por:

```python
user_id
time_utc
point_id
```

Luego forma pares consecutivos dentro de cada usuario. El punto actual se usa como origen y el punto siguiente como destino.

Este modo no busca reconstruir una trayectoria completa. Cada viaje representa una transición OD simple entre dos observaciones consecutivas.

### `consecutive_clusters`

Este modo agrega una etapa previa de colapso secuencial. Los puntos consecutivos de un mismo usuario se agrupan mientras se mantengan dentro de:

- `cluster_radius_m`;
- `cluster_max_time_gap_s`.

Luego se construyen viajes entre clusters consecutivos. El viaje no se forma entre centroides ni puntos sintéticos. Se usan puntos frontera reales:

- origen: último punto del cluster origen;
- destino: primer punto del cluster destino.

En este modo, `cluster_radius_m` y `cluster_max_time_gap_s` son obligatorios y deben ser positivos.

## Qué recibe y qué retorna

La operación recibe:

- `traces`: un `TraceDataset`;
- `trip_schema`: un `TripSchema` para el `TripDataset` resultante;
- `options`: una instancia opcional de `InferTripsOptions`;
- `value_correspondence`: mapping opcional para normalizar valores categóricos del output;
- `provenance`: diccionario opcional con procedencia del proceso.

La operación retorna:

```python
TripDataset, InferenceReport
```

El `TraceDataset` de entrada no se muta. La operación trabaja sobre una copia interna de `traces.data`.

## Precondiciones

El `TraceDataset` de entrada debe contener el núcleo mínimo:

```python
point_id
user_id
time_utc
latitude
longitude
```

Por defecto, la operación exige que:

```python
traces.metadata["is_validated"] == True
```

Este comportamiento puede desactivarse con:

```python
InferTripsOptions(require_validated_traces=False)
```

Cuando se usa ese bypass, la operación deja evidencia explícita en `InferenceReport.parameters`, en el evento `infer_trips` y en los issues.

## Salida mínima

El `TripDataset` resultante materializa al menos:

```python
movement_id
user_id
origin_longitude
origin_latitude
destination_longitude
destination_latitude
origin_time_utc
destination_time_utc
origin_h3_index
destination_h3_index
trip_id
movement_seq
```

Por convención de v1.1, cada viaje inferido se representa como viaje de una sola etapa:

```python
trip_id = movement_id
movement_seq = 0
```

Además, la operación deriva siempre `origin_h3_index` y `destination_h3_index` usando la resolución configurada en `h3_resolution`.

## Opciones principales

`InferTripsOptions` permite configurar:

- `infer_mode`;
- `strict`;
- `strict_domains`;
- `require_validated_traces`;
- `drop_invalid`;
- `h3_resolution`;
- `max_time_delta_s`;
- `min_time_delta_s`;
- `min_distance_m`;
- `cluster_radius_m`;
- `cluster_max_time_gap_s`;
- `propagate_trace_fields`.

Los thresholds disponibles cumplen roles acotados:

| Opción | Efecto |
|---|---|
| `max_time_delta_s` | descarta candidatos con separación temporal demasiado grande |
| `min_time_delta_s` | descarta candidatos con separación temporal demasiado pequeña |
| `min_distance_m` | descarta candidatos con distancia espacial demasiado pequeña |
| `cluster_radius_m` | define proximidad espacial para agrupar puntos en modo clusters |
| `cluster_max_time_gap_s` | define proximidad temporal para agrupar puntos en modo clusters |

No existe `max_distance_m` en v1.1.

## Propagación de campos desde traces

La operación no propaga campos extra automáticamente. La propagación debe pedirse explícitamente mediante `propagate_trace_fields`.

Ejemplo:

```python
InferTripsOptions(
    propagate_trace_fields={
        "category": "both",
        "accuracy": "destination",
        "device_type": "origin",
    }
)
```

La convención de nombres es:

| Modo | Columnas creadas |
|---|---|
| `origin` | `origin_<campo>` |
| `destination` | `destination_<campo>` |
| `both` | `origin_<campo>` y `destination_<campo>` |

En `consecutive_points`, los valores salen de los dos puntos del par. En `consecutive_clusters`, salen de los puntos frontera de los clusters.

## Dominios y value correspondence

`value_correspondence` permite normalizar valores categóricos materializados en el output, por ejemplo campos propagados como `origin_category` o `destination_category`.

La operación procesa campos categóricos materializados del output. Si un campo categórico tiene valores fuera de dominio, puede extender dominios efectivos cuando el campo es extendible. Si `strict_domains=True` y la extensión no está permitida, la operación escala el error después de construir evidencia.

## Qué evidencia deja

OP-16 retorna un `InferenceReport` con:

- `ok`;
- `issues`;
- `summary`;
- `parameters`.

El `summary` contiene:

- `infer_mode`;
- `n_points_in`;
- `n_candidates_in`;
- `n_candidates_dropped`;
- `n_trips_out`;
- `dropped_by_reason`;
- `n_clusters_out`, solo en modo `consecutive_clusters`.

El bloque `parameters` registra:

- `infer_mode`;
- `strict`;
- `strict_domains`;
- `require_validated_traces`;
- `drop_invalid`;
- `h3_resolution`;
- `max_time_delta_s`;
- `min_time_delta_s`;
- `min_distance_m`;
- `cluster_radius_m`;
- `cluster_max_time_gap_s`;
- `propagate_trace_fields`;
- `value_correspondence_used`;
- `validation_bypass_used`.

Además, el `TripDataset` resultante queda con:

- `metadata["is_validated"] = False`;
- `metadata["h3"]["resolution"]`;
- `metadata["temporal"]["tier"] = "tier_1"`;
- evento `infer_trips`;
- `schema_effective`;
- `provenance` derivado desde traces.

El evento `infer_trips` contiene:

- `op`;
- `ts_utc`;
- `parameters`;
- `summary`;
- `issues_summary`.

## Consideraciones importantes

OP-16 no valida formalmente el `TripDataset` resultante. El resultado queda con `metadata["is_validated"] = False`, por lo que debe pasar por `validate_trips` si se requiere conformidad formal antes de continuar.

`drop_invalid` no decide si un candidato inválido entra al dataframe final. Los candidatos estructuralmente inválidos no se materializan. Esta opción afecta principalmente la severidad y evidencia emitida sobre esos descartes.

Si todos los candidatos son descartados por thresholds, la operación puede retornar un `TripDataset` vacío pero con el núcleo mínimo de columnas y evidencia explícita.

`strict` y `strict_domains` escalan errores después de construir dataset derivado, reporte, summary y evento, salvo en errores fatales de input, schema o configuración que impiden ejecutar el pipeline.

## Ejemplo mínimo con `consecutive_points`

```python
from pylondrina.transforms.inference import (
    InferTripsOptions,
    infer_trips_from_traces,
)

trips, report = infer_trips_from_traces(
    traces,
    trip_schema,
    options=InferTripsOptions(
        infer_mode="consecutive_points",
        h3_resolution=8,
        propagate_trace_fields={
            "category": "both",
        },
    ),
    value_correspondence={
        "origin_category": {"education": "study"},
        "destination_category": {"education": "study"},
    },
    provenance={
        "source_family": "checkins",
    },
)

print(report.summary)
print(trips.data.head())
```

## Ejemplo mínimo con `consecutive_clusters`

```python
trips, report = infer_trips_from_traces(
    traces,
    trip_schema,
    options=InferTripsOptions(
        infer_mode="consecutive_clusters",
        h3_resolution=8,
        cluster_radius_m=250,
        cluster_max_time_gap_s=300,
        max_time_delta_s=None,
        min_time_delta_s=None,
        min_distance_m=None,
    ),
)

print(report.summary["n_clusters_out"])
print(report.summary["n_trips_out"])
```

## Operación anterior y siguiente

Dentro de la familia traces, OP-16 conecta puntos discretos con el pipeline de trips.

| Posición | Operación |
|---|---|
| Anterior recomendada | [OP-15 Validate traces](op15_validate_traces.md) |
| Actual | OP-16 Infer trips from traces |
| Siguiente recomendada | [OP-02 Validate trips](../trips/op02_validate_trips.md) |
| Operación posterior relacionada | [OP-08 Build flows](../flows/op08_build_flows.md), después de validar trips |

## Implementación relacionada

Esta operación forma parte de la API pública de Pylondrina v1.1. Su implementación principal puede revisarse en el código fuente del proyecto.

| Recurso | Enlace |
|---|---|
| Archivo fuente | [`src/pylondrina/transforms/inference.py`](https://github.com/sebastianolmos/pylondrina/blob/main/src/pylondrina/transforms/inference.py) |
| Catálogo de issues | [`src/pylondrina/issues/catalogo_infer_trips.py`](https://github.com/sebastianolmos/pylondrina/blob/main/src/pylondrina/issues/catalogo_infer_trips.py) |
| Referencia API | [Ver referencia técnica](../../api/traces.md) |