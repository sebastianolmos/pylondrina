# Inferir trips desde traces

## Propósito de la guía

Esta guía muestra cómo construir viajes OD simples a partir de puntos espacio-temporales discretos. El flujo cubre la entrada de puntos como `TraceDataset`, su validación mínima y la inferencia posterior de un `TripDataset` compatible con el pipeline de trips y flows.

El bloque de traces de Pylondrina v1.1 está diseñado para fuentes discretas, como check-ins, puntos de presencia, registros de telefonía o telemetría ya preparada como observaciones puntuales. No está diseñado para GPS denso, map matching ni reconstrucción avanzada de trayectorias continuas.

## Flujo general

```text
fuente tabular de puntos discretos
  -> import_traces_from_dataframe
  -> validate_traces
  -> infer_trips_from_traces
  -> validate_trips
  -> filter_trips / write_trips / build_flows
```

En términos prácticos:

* `import_traces_from_dataframe` construye un `TraceDataset` mínimo, pero no lo certifica.
* `validate_traces` certifica conformidad mínima del dataset de puntos.
* `infer_trips_from_traces` deriva un `TripDataset` nuevo.
* El `TripDataset` inferido debe tratarse como cualquier otro dataset de trips: puede validarse, filtrarse, persistirse o usarse para construir flows.

Inferir trips desde traces no equivale a importar trips directamente. OP-16 no lee una fuente externa de viajes, sino que deriva viajes OD simples a partir de puntos ya estructurados.

## 1. Preparar puntos discretos

El punto de partida debe ser una tabla de observaciones espacio-temporales discretas. No se requiere que la fuente ya use nombres canónicos, pero sí debe contener información suficiente para materializar el núcleo mínimo de traces:

```text
point_id
user_id
time_utc
latitude
longitude
```

`point_id` puede generarse automáticamente durante el import si no existe en la fuente. Los otros cuatro campos deben poder alcanzarse desde la tabla original o mediante `field_correspondence`.

Un ejemplo de tabla fuente podría tener nombres propios:

```text
device_id
timestamp
lat
lon
category
```

En ese caso, la correspondencia permite declarar cómo se alcanza el contrato de traces:

```python
field_correspondence = {
    "user_id": "device_id",
    "time_utc": "timestamp",
    "latitude": "lat",
    "longitude": "lon",
}
```

Los campos adicionales, como `category`, `position_speed` o identificadores externos, pueden conservarse como extensiones si son útiles para análisis o propagación posterior hacia trips.

## 2. Importar traces

La importación de traces construye un `TraceDataset` canónico desde un `DataFrame` de puntos. Esta etapa alinea nombres de campos, interpreta temporalidad, conserva campos extra según la política configurada y registra metadata operacional.

```python
from pylondrina.importing_traces import (
    ImportTraceOptions,
    import_traces_from_dataframe,
)

traces, import_report = import_traces_from_dataframe(
    raw_points,
    trace_schema,
    source_name="checkins",
    options=ImportTraceOptions(
        source_timezone="UTC",
        keep_extra_fields=True,
    ),
    field_correspondence={
        "user_id": "device_id",
        "time_utc": "timestamp",
        "latitude": "lat",
        "longitude": "lon",
    },
    provenance={
        "source_family": "checkins",
    },
)
```

Después del import, el dataset queda construido pero no validado:

```python
traces.metadata["is_validated"]
# False
```

Esto es intencional. Importar significa que la fuente fue llevada a una representación operable; no significa que ya fue certificada formalmente.

La metadata temporal permite revisar cómo se interpretó `time_utc`, por ejemplo si se usó una timezone explícita, si los timestamps ya venían con zona horaria o si quedaron normalizados a UTC.

```python
traces.metadata["temporal"]
```

## 3. Validar traces

La validación de traces certifica conformidad mínima sobre el `TraceDataset`. Esta operación revisa campos requeridos, tipos, constraints simples y monotonicidad temporal por usuario.

```python
from pylondrina.validation_traces import (
    TraceValidationOptions,
    validate_traces,
)

trace_report = validate_traces(
    traces,
    options=TraceValidationOptions(
        strict=False,
        validate_required_fields=True,
        validate_types_and_formats=True,
        validate_constraints=True,
        validate_monotonic_time_per_user=True,
    ),
)
```

En OP-15, la señal principal del resultado se consulta en:

```python
trace_report.summary["ok"]
```

Si no hay errores, la operación actualiza:

```python
traces.metadata["is_validated"] = True
```

La monotonicidad temporal se evalúa sobre el orden observado de `TraceDataset.data`. Si se detectan retrocesos temporales por usuario, se reportan como warnings. Esos warnings no invalidan necesariamente el dataset.

Después de validar, se puede revisar la evidencia operacional:

```python
trace_report.summary
trace_report.issues
traces.metadata["events"][-1]
```

## 4. Inferir trips

La inferencia deriva un `TripDataset` desde el `TraceDataset` validado. OP-16 produce viajes OD simples, no trayectorias completas.

```python
from pylondrina.transforms.inference import (
    InferTripsOptions,
    infer_trips_from_traces,
)

trips, infer_report = infer_trips_from_traces(
    traces,
    trip_schema,
    options=InferTripsOptions(
        infer_mode="consecutive_points",
        h3_resolution=10,
    ),
    provenance={
        "method": "trace_to_trip",
    },
)
```

El resultado es un nuevo `TripDataset` con campos como:

```text
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

Por convención de v1.1, cada viaje inferido se representa como un viaje de una sola etapa:

```text
trip_id = movement_id
movement_seq = 0
```

El dataset resultante queda no validado:

```python
trips.metadata["is_validated"]
# False
```

Por eso, si se continuará hacia flows o persistencia formal, se recomienda validarlo con `validate_trips`.

### Modo `consecutive_points`

`consecutive_points` es el modo base. Ordena los puntos por usuario y tiempo, y construye candidatos OD entre puntos consecutivos del mismo usuario.

Este modo es adecuado cuando la fuente contiene observaciones discretas separadas, como check-ins o puntos de presencia.

```python
options = InferTripsOptions(
    infer_mode="consecutive_points",
    h3_resolution=10,
    max_time_delta_s=86400,
    min_time_delta_s=300,
    min_distance_m=100,
    propagate_trace_fields={
        "category": "both",
    },
)
```

Con `propagate_trace_fields`, atributos del punto pueden copiarse hacia los extremos del viaje:

```text
origin_category
destination_category
```

Esto permite conservar contexto analítico, por ejemplo la categoría del lugar de origen y destino.

### Modo `consecutive_clusters`

`consecutive_clusters` agrupa puntos cercanos en espacio-tiempo antes de construir viajes. Su objetivo es absorber repeticiones locales o bursts de puntos que no deberían convertirse directamente en viajes sucesivos.

```python
options = InferTripsOptions(
    infer_mode="consecutive_clusters",
    h3_resolution=10,
    cluster_radius_m=250,
    cluster_max_time_gap_s=300,
)
```

En este modo, los viajes se construyen entre clusters consecutivos. El origen se toma desde el último punto real del cluster origen y el destino desde el primer punto real del cluster destino. No se usan centroides ni puntos sintéticos.

Este modo es útil para fuentes con muchos puntos cercanos por usuario, por ejemplo registros de telefonía o telemetría discreta.

## 5. Usar el TripDataset resultante

Después de OP-16, el resultado entra al pipeline normal de trips. Se debe tratar como un `TripDataset` derivado.

Un flujo recomendado es:

```python
from pylondrina.validation import ValidationOptions, validate_trips
from pylondrina.transforms.flows import FlowBuildOptions, build_flows

trip_report = validate_trips(
    trips,
    options=ValidationOptions(
        validate_temporal_consistency=True,
        validate_domains="off",
    ),
)

flows, flow_report = build_flows(
    trips,
    options=FlowBuildOptions(
        h3_resolution=8,
        min_trips_per_flow=2,
        require_validated=True,
    ),
)
```

También pueden aplicarse operaciones de trips según el objetivo:

| Necesidad                                     | Operación        |
| --------------------------------------------- | ---------------- |
| Certificar conformidad del resultado inferido | `validate_trips` |
| Restringir una ventana temporal o zona        | `filter_trips`   |
| Persistir el dataset derivado                 | `write_trips`    |
| Construir flujos OD                           | `build_flows`    |
| Exportar flows para visualización             | `export_flows`   |

La separación es importante: OP-16 deriva trips, pero no certifica que el `TripDataset` resultante ya esté validado para todos los usos posteriores.

## Consideraciones y límites

El bloque Trace → Trip de v1.1 tiene un alcance deliberadamente austero.

No se implementa:

* procesamiento de GPS denso;
* map matching;
* inferencia multimodal;
* reconstrucción de ruta;
* detección avanzada de estadías;
* imputación de trayectorias;
* estimación causal de movilidad.

La inferencia trabaja sobre puntos discretos ya estructurados. Por eso, la calidad de los viajes inferidos depende de la granularidad, cobertura temporal y significado de los puntos de entrada.

También conviene considerar:

* `require_validated_traces=True` es el comportamiento recomendado antes de inferir.
* Si se desactiva esa precondición, la operación deja evidencia de bypass.
* Los thresholds temporales y espaciales controlan qué candidatos OD se descartan.
* Si todos los candidatos son descartados, puede producirse un `TripDataset` vacío con evidencia en el reporte.
* Los campos extra no se propagan automáticamente; deben solicitarse mediante `propagate_trace_fields`.
* El resultado debe validarse como trips si se usará en operaciones que requieren conformidad formal.

## Patrón mínimo de código

El siguiente ejemplo muestra un pipeline compacto desde puntos discretos hasta flows.

```python
from pylondrina.importing_traces import (
    ImportTraceOptions,
    import_traces_from_dataframe,
)
from pylondrina.validation_traces import (
    TraceValidationOptions,
    validate_traces,
)
from pylondrina.transforms.inference import (
    InferTripsOptions,
    infer_trips_from_traces,
)
from pylondrina.validation import (
    ValidationOptions,
    validate_trips,
)
from pylondrina.transforms.flows import (
    FlowBuildOptions,
    build_flows,
)

# 1. Importar puntos discretos como TraceDataset.
traces, import_report = import_traces_from_dataframe(
    raw_points,
    trace_schema,
    source_name="checkins",
    options=ImportTraceOptions(
        source_timezone="UTC",
        keep_extra_fields=True,
    ),
    field_correspondence={
        "user_id": "device_id",
        "time_utc": "timestamp",
        "latitude": "lat",
        "longitude": "lon",
    },
    provenance={
        "source_family": "checkins",
    },
)

# 2. Validar conformidad mínima de traces.
trace_report = validate_traces(
    traces,
    options=TraceValidationOptions(
        strict=False,
        validate_required_fields=True,
        validate_types_and_formats=True,
        validate_constraints=True,
        validate_monotonic_time_per_user=True,
    ),
)

# 3. Inferir trips desde puntos consecutivos.
trips, infer_report = infer_trips_from_traces(
    traces,
    trip_schema,
    options=InferTripsOptions(
        infer_mode="consecutive_points",
        h3_resolution=10,
        max_time_delta_s=86400,
        min_time_delta_s=300,
        min_distance_m=100,
        propagate_trace_fields={
            "category": "both",
        },
    ),
    value_correspondence={
        "origin_category": {
            "Office": "work",
            "Home (private)": "home",
        },
        "destination_category": {
            "Office": "work",
            "Home (private)": "home",
        },
    },
    provenance={
        "method": "consecutive_points",
    },
)

# 4. Validar el TripDataset inferido.
trip_report = validate_trips(
    trips,
    options=ValidationOptions(
        validate_temporal_consistency=True,
        validate_domains="full",
    ),
)

# 5. Construir flows desde los trips inferidos.
flows, flow_report = build_flows(
    trips,
    options=FlowBuildOptions(
        h3_resolution=8,
        min_trips_per_flow=2,
        keep_flow_to_trips=True,
        require_validated=True,
    ),
)
```

Para el modo con clusters, la etapa de inferencia puede cambiarse por:

```python
trips, infer_report = infer_trips_from_traces(
    traces,
    trip_schema,
    options=InferTripsOptions(
        infer_mode="consecutive_clusters",
        h3_resolution=10,
        cluster_radius_m=250,
        cluster_max_time_gap_s=300,
        max_time_delta_s=43200,
        min_time_delta_s=300,
        min_distance_m=300,
    ),
)
```

En ambos casos, los reportes permiten revisar qué ocurrió:

```python
print(import_report.summary)
print(trace_report.summary)
print(infer_report.summary)
print(trip_report.summary)
print(flow_report.summary)
```

## Enlaces relacionados

* [OP-14 Import traces](../operations/traces/op14_import_traces.md)
* [OP-15 Validate traces](../operations/traces/op15_validate_traces.md)
* [OP-16 Infer trips from traces](../operations/traces/op16_infer_trips_from_traces.md)
* [OP-02 Validate trips](../operations/trips/op02_validate_trips.md)
* [OP-08 Build flows](../operations/flows/op08_build_flows.md)
* [Traces en Golondrina](../golondrina/traces.md)
* [Trips en Golondrina](../golondrina/trips.md)
