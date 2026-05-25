# OP-13 Get trips from flows

`get_trips_from_flows` es una operación de consulta para inspeccionar qué movements sustentan un conjunto de flows ya construido. Se implementó como una query pura sobre `FlowDataset`: no crea un dataset nuevo, no registra eventos, no modifica metadata, no escribe en disco y no muta los objetos de entrada.

La operación retorna una tabla de correspondencia `flow_id` - `movement_id` y un `OperationReport`. Su objetivo principal es habilitar drill-down desde flujos agregados hacia los viajes o movements que explican esos flujos.

## Para qué sirve

Esta operación permite responder preguntas como:

- qué movements componen un flujo dominante;
- qué viajes sustentan un subconjunto de flows filtrados;
- si un flow agregado puede explicarse desde `flow_to_trips` o desde el `TripDataset` original;
- qué cobertura tiene la correspondencia reconstruida.

La unidad contractual mínima de salida es:

```python
flow_id
movement_id
```

Cuando la correspondencia se reconstruye desde un `TripDataset` que contiene `trip_id`, la tabla puede incluir también:

```python
trip_id
```

`trip_id` es un enriquecimiento opcional, no parte del mínimo contractual.

## Cuándo usarla

Esta operación se usa después de construir o filtrar flows, cuando se necesita explicar qué registros de trips están detrás de los flujos agregados.

Un flujo típico es:

```text
build_flows -> filter_flows -> get_trips_from_flows
```

También puede usarse directamente después de [OP-08 Build flows](op08_build_flows.md), especialmente si el `FlowDataset` fue construido con:

```python
keep_flow_to_trips=True
```

Si se requiere consultar solo una parte de los flujos, el camino recomendado es filtrar primero el `FlowDataset` con [OP-12 Filter flows](op12_filter_flows.md) y luego ejecutar OP-13 sobre el resultado.

## Qué recibe y qué retorna

La operación recibe:

- `flows`: un `FlowDataset`;
- `trips`: un `TripDataset` opcional para reconstrucción;
- `max_issues`: límite máximo de issues retenidos en el reporte.

La firma pública es:

```python
get_trips_from_flows(
    flows,
    trips=None,
    *,
    max_issues=1000,
)
```

La operación retorna:

```python
pd.DataFrame, OperationReport
```

El dataframe retornado contiene la correspondencia flujo-movement. El reporte resume la fuente usada, la cobertura obtenida, los issues detectados y el tamaño de la salida.

## Prioridad de fuentes

OP-13 usa una prioridad explícita de fuentes:

1. `flows.flow_to_trips`;
2. `trips` entregado como argumento;
3. `flows.source_trips`.

La primera ruta consume una tabla auxiliar ya materializada. Las otras dos rutas reconstruyen la correspondencia desde trips, reproduciendo las llaves efectivas de agregación usadas al construir los flows.

Si `flows.flow_to_trips` existe y es usable, se usa como fuente preferente. Para ser usable debe ser un `DataFrame` con al menos:

```python
flow_id
movement_id
```

Si `flow_to_trips` existe pero no es usable, la operación puede degradar hacia `trips` o hacia `flows.source_trips`, dejando evidencia en el reporte.

Si no existe ninguna fuente usable, la operación aborta.

## Reconstrucción desde trips

Cuando no hay `flow_to_trips` usable, la operación intenta reconstruir la correspondencia desde un `TripDataset`. Esta reconstrucción no es heurística: intenta reproducir exactamente las llaves de agregación del `FlowDataset`.

Para ello usa `flows.aggregation_spec`, incluyendo:

- llaves espaciales H3;
- `group_by`;
- ventanas temporales si existen;
- `effective_flow_keys`, cuando está disponible;
- resolución H3 objetivo.

La reconstrucción puede usar roll-up H3 cuando el flow fue construido a una resolución más gruesa que la disponible en trips. No intenta reconstruir celdas más finas que las existentes.

Si el flow tiene agregación temporal, se reconstruyen las ventanas a partir de:

- `origin_time_utc`, si `time_basis="origin"`;
- `destination_time_utc`, si `time_basis="destination"`.

Las agregaciones temporales soportadas por la reconstrucción son:

- `none`;
- `hour`;
- `day`;
- `week`.

La reconstrucción exige que las llaves efectivas sean no ambiguas. Si varias filas de `flows.flows` comparten la misma llave de agregación, la operación aborta porque no puede asignar trips a flows de manera exacta.

## Qué evidencia deja

OP-13 retorna un `OperationReport` con:

- `ok`;
- `issues`;
- `summary`;
- `parameters`.

El bloque `parameters` contiene:

- `max_issues`;
- `used_source`;
- `reconstruction_attempted`;
- `n_flows_input`;
- `n_trips_input`.

`used_source` usa uno de estos valores:

```python
"flow_to_trips"
"trips_argument"
"flows.source_trips"
```

El `summary` contiene:

- `n_rows_out`;
- `n_unique_flows_out`;
- `n_unique_movements_out`;
- `n_unmatched_flows`;
- `n_unmatched_movements`;
- `limits`, solo si hubo truncamiento de issues.

La operación puede retornar una tabla y un reporte con `ok=False` si hubo issues recuperables de nivel error, pero la salida tabular sigue siendo retornable.

## Consideraciones importantes

OP-13 no registra eventos. Esta decisión es parte del contrato de la operación: al ser una consulta pura, no altera `flows.metadata`, `flows.provenance`, `flows.aggregation_spec`, `flows.flows`, `flows.flow_to_trips` ni `trips.data`.

`flows.source_trips` solo funciona como fallback cuando el `FlowDataset` sigue vivo en memoria con esa referencia. Si el dataset fue leído desde persistencia formal mediante [OP-11 Read flows](op11_read_flows.md), normalmente `source_trips` será `None`. En ese caso, se debe entregar explícitamente `trips` o haber conservado `flow_to_trips`.

Si `flow_to_trips` contiene pares exactos repetidos, la operación los deduplica y deja evidencia. Si la reconstrucción desde trips encuentra cobertura parcial, también lo informa mediante issues.

Un resultado vacío puede ser retornable cuando la operación fue interpretable, pero no encontró correspondencias entre flows y trips.

## Ejemplo mínimo

El siguiente ejemplo recupera la correspondencia usando `flow_to_trips` si está disponible, o reconstruyéndola desde `trips` si es necesario.

```python
from pylondrina.queries.flows import get_trips_from_flows

links, report = get_trips_from_flows(
    flows,
    trips=trips,
    max_issues=100,
)

print(report.summary)
print(links.head())
```

Una salida típica contiene:

```python
flow_id
movement_id
trip_id
```

si la operación reconstruyó desde un `TripDataset` con `trip_id`.

Para inspeccionar un flujo específico:

```python
flow_id = links["flow_id"].value_counts().index[0]

movement_ids = links.loc[
    links["flow_id"] == flow_id,
    "movement_id",
]

trips_for_flow = trips.data.loc[
    trips.data["movement_id"].isin(movement_ids)
]
```

## Operación anterior y siguiente

Dentro de la familia Trip → Flow, OP-13 funciona como consulta de inspección sobre un `FlowDataset` ya construido o filtrado.

| Posición | Operación |
|---|---|
| Anterior recomendada | [OP-08 Build flows](op08_build_flows.md) con `keep_flow_to_trips=True` |
| Alternativa anterior | [OP-12 Filter flows](op12_filter_flows.md), si se quiere consultar solo un subconjunto |
| Actual | OP-13 Get trips from flows |
| Operación relacionada | [OP-11 Read flows](op11_read_flows.md), considerando que `source_trips` no se reconstruye desde persistencia |

## Implementación relacionada

Esta operación forma parte de la API pública de Pylondrina v1.1. Su implementación principal puede revisarse en el código fuente del proyecto.

| Recurso | Enlace |
|---|---|
| Archivo fuente | [`src/pylondrina/queries/flows.py`](https://github.com/sebastianolmos/pylondrina/blob/main/src/pylondrina/queries/flows.py) |
| Catálogo de issues | [`src/pylondrina/issues/catalog_trips_from_flows.py`](https://github.com/sebastianolmos/pylondrina/blob/main/src/pylondrina/issues/catalog_trips_from_flows.py) |
| Referencia API | [Ver referencia técnica](../../api/flows.md) |