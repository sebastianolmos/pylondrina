# OP-08 Build flows

`build_flows` es la operación que construye un `FlowDataset` a partir de un `TripDataset`. Se implementó para agregar movements origen-destino bajo reglas explícitas de resolución H3, segmentación categórica y, opcionalmente, agregación temporal.

La operación concentra la semántica interna de construcción de flujos. No exporta archivos, no escribe bundles, no genera sidecars y no transforma el resultado a formatos externos de visualización. Esa frontera queda reservada para operaciones posteriores, como [OP-09 Export flows](op09_export_flows.md) o persistencia formal de flows.

## Para qué sirve

Esta operación permite transformar viajes o movements individuales en flujos OD agregados. En términos prácticos, toma filas de `TripDataset.data` con origen y destino H3 y produce una tabla de flujos con magnitud agregada.

La tabla interna `FlowDataset.flows` queda construida con columnas mínimas:

- `flow_id`;
- `origin_h3_index`;
- `destination_h3_index`;
- `flow_count`;
- `flow_value`.

Cuando corresponde, también puede incluir:

- columnas de segmentación definidas en `group_by`;
- `window_start_utc`;
- `window_end_utc`.

`flow_count` representa el número de movements agregados. `flow_value` representa la magnitud analítica del flujo: si existe `trip_weight`, se calcula como suma ponderada; si no existe, cae al conteo de movements.

## Cuándo usarla

Esta operación se usa después de construir y validar un `TripDataset`, cuando se necesita producir una representación OD agregada para análisis, inspección, exportación o persistencia.

Un flujo típico es:

```text
import_trips_from_dataframe -> validate_trips -> clean_trips/filter_trips -> validate_trips -> build_flows
```

También puede usarse sobre trips inferidos desde traces, siempre que el resultado de inferencia sea un `TripDataset` compatible y cuente con H3 OD utilizables.

Por defecto, la operación exige que el dataset de entrada esté validado:

```python
trips.metadata["is_validated"] == True
```

Esa precondición puede desactivarse con `require_validated=False`, pero no es la política recomendada para un flujo analítico formal.

## Qué recibe y qué retorna

La operación recibe:

* `trips`: un `TripDataset`;
* `options`: una instancia opcional de `FlowBuildOptions`.

`FlowBuildOptions` permite configurar:

* `h3_resolution`: resolución H3 objetivo de agregación;
* `group_by`: campos adicionales para segmentar flujos;
* `time_aggregation`: granularidad temporal;
* `time_basis`: campo temporal usado para ubicar cada movement en una ventana;
* `min_trips_per_flow`: umbral mínimo de movements para conservar un flujo;
* `keep_flow_to_trips`: construcción opcional de tabla auxiliar;
* `require_validated`: exigencia de validación previa;
* `strict`;
* `max_issues`.

La operación retorna:

```python
FlowDataset, FlowBuildReport
```

El `FlowDataset` retornado es un objeto nuevo y derivado. El `TripDataset` de entrada no se muta.

## Qué evidencia deja

OP-08 retorna un `FlowBuildReport` con:

* `ok`;
* `issues`;
* `summary`;
* `parameters`;
* `metadata`.

El `summary` se mantiene pequeño y estable. Sus claves principales son:

* `n_trips_in`;
* `n_trips_eligible`;
* `n_trips_dropped`;
* `n_flows_out`;
* `n_flow_to_trips_rows`.

Si se alcanza el límite de issues, puede incluir un bloque `limits`.

El bloque `parameters` registra las opciones efectivas usadas en la agregación:

* `h3_resolution`;
* `group_by`;
* `time_aggregation`;
* `time_basis`;
* `min_trips_per_flow`;
* `keep_flow_to_trips`;
* `require_validated`;
* `strict`;
* `max_issues`.

Además, el `FlowDataset` de salida registra un evento `build_flows` en `metadata["events"]`. El evento contiene:

* `op`;
* `ts_utc`;
* `parameters`;
* `summary`;
* `issues_summary`.

## Qué contiene el FlowDataset resultante

El `FlowDataset` construido contiene:

* `flows`: tabla interna de flujos;
* `flow_to_trips`: tabla auxiliar opcional;
* `aggregation_spec`: especificación efectiva de agregación;
* `metadata`: metadata propia del dataset de flujos;
* `provenance`: resumen de derivación desde el dataset de trips;
* `source_trips`: referencia viva al `TripDataset` usado como origen.

`aggregation_spec` registra tanto la configuración declarada como el estado efectivo de la agregación, incluyendo `effective_flow_keys`.

La metadata del `FlowDataset` se mantiene deliberadamente pequeña. Incluye un nuevo `dataset_id` con prefijo `flows_`, `artifact_id = None`, `is_validated = False`, `events` y la resolución H3 objetivo usada.

## Consideraciones importantes

La operación consume los campos canónicos:

```python
origin_h3_index
destination_h3_index
```

No usa `origin_h3` ni `destination_h3` como contrato interno.

La operación distingue entre un movement válido como registro Golondrina y un movement buildable como flujo. Para construir un flujo, el movement debe tener ambos H3 OD utilizables. Si faltan H3 en origen o destino, el registro se descarta de la agregación y se deja evidencia agregada. Si no queda ningún movement buildable, la operación falla.

La resolución H3 de entrada se infiere desde los valores observados en `origin_h3_index` y `destination_h3_index`. Si la resolución objetivo es igual, se usan las celdas tal cual. Si la resolución objetivo es más gruesa, se realiza roll-up con `cell_to_parent`. Si se solicita una resolución más fina que la disponible, la operación aborta.

La agregación temporal solo se admite cuando `time_aggregation != "none"` y el dataset tiene temporalidad Tier 1. En ese caso, la operación materializa ventanas mediante:

* `window_start_utc`;
* `window_end_utc`.

El campo temporal usado se controla con `time_basis`:

* `origin`: usa `origin_time_utc`;
* `destination`: usa `destination_time_utc`.

`flow_to_trips` es opcional. Cuando `keep_flow_to_trips=True`, se construye una tabla mínima con:

```python
flow_id
movement_id
```

Esta tabla permite inspeccionar posteriormente qué movements sostienen cada flujo, por ejemplo mediante [OP-13 Get trips from flows](op13_get_trips_from_flows.md). Como puede aumentar el tamaño del resultado, no forma parte obligatoria del camino mínimo.

## Ejemplo mínimo

El siguiente ejemplo construye flujos OD segmentados por género, usando resolución H3 8 y exigiendo al menos tres movements por flujo.

```python
from pylondrina.transforms.flows import FlowBuildOptions, build_flows

flows, report = build_flows(
    trips_work,
    options=FlowBuildOptions(
        h3_resolution=8,
        group_by=["user_gender"],
        time_aggregation="none",
        min_trips_per_flow=3,
        keep_flow_to_trips=False,
        require_validated=True,
    ),
)

print(report.summary)
print(flows.flows.head())
```

Si se necesita conservar la relación `flow_id -> movement_id`, puede activarse `keep_flow_to_trips`:

```python
flows, report = build_flows(
    trips,
    options=FlowBuildOptions(
        h3_resolution=8,
        group_by=["mode"],
        time_aggregation="none",
        min_trips_per_flow=1,
        keep_flow_to_trips=True,
        require_validated=True,
    ),
)

print(flows.flow_to_trips.head())
```

## Operación anterior y siguiente

Dentro de la familia Trip → Flow, OP-08 es la operación que construye el objeto interno de flujos. Las operaciones posteriores consumen ese `FlowDataset`.

| Posición                 | Operación                                                                                                                                        |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| Anterior recomendada     | [OP-02 Validate trips](../trips/op02_validate_trips.md), después de limpiar o filtrar si corresponde                                             |
| Actual                   | OP-08 Build flows                                                                                                                                |
| Siguiente recomendada    | [OP-09 Export flows](op09_export_flows.md)                                                                                                       |
| Alternativas posteriores | [OP-10 Write flows](op10_write_flows.md), [OP-12 Filter flows](op12_filter_flows.md), [OP-13 Get trips from flows](op13_get_trips_from_flows.md) |

## Implementación relacionada

Esta operación forma parte de la API pública de Pylondrina v1.1. Su implementación principal puede revisarse en el código fuente del proyecto.

| Recurso            | Enlace                                                                                                                                                |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| Archivo fuente     | [`src/pylondrina/transforms/flows.py`](https://github.com/sebastianolmos/pylondrina/blob/main/src/pylondrina/transforms/flows.py)                     |
| Catálogo de issues | [`src/pylondrina/issues/catalog_build_flows.py`](https://github.com/sebastianolmos/pylondrina/blob/main/src/pylondrina/issues/catalog_build_flows.py) |
| Referencia API     | [Ver referencia técnica](../../api/flows.md)                                                                                                          |