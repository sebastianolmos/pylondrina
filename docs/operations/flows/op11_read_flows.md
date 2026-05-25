# OP-11 Read flows

`read_flows` es la operación de lectura formal para artefactos persistidos de flows. Se implementó para reconstruir un `FlowDataset` desde un bundle `.golondrina` escrito previamente por [OP-10 Write flows](op10_write_flows.md).

La operación no importa una fuente externa, no exporta a visualización, no valida formalmente el dataset y no reconstruye el pipeline completo que produjo los flows. Su responsabilidad es leer el artefacto formal, reconstruir el estado persistido y devolver un `FlowDataset` utilizable dentro de Pylondrina.

## Para qué sirve

Esta operación permite retomar un pipeline a partir de un artefacto formal de flows. Se usa cuando un `FlowDataset` fue persistido con `write_flows` y se necesita cargarlo nuevamente en memoria.

El artefacto esperado contiene:

- `flows.metadata.json`, sidecar obligatorio;
- `flows.feather` o `flows.parquet`, según el backend declarado en el sidecar;
- opcionalmente, `flow_to_trips.feather` o `flow_to_trips.parquet`.

El usuario no debe indicar manualmente el backend. La operación lo resuelve desde:

```python
sidecar["storage"]["format"]
```

y valida la coherencia con:

```python
sidecar["files"]["data"]
sidecar["files"]["flow_to_trips"]
```

cuando corresponde.

## Cuándo usarla

Esta operación se usa al inicio de una etapa posterior, cuando se quiere recuperar un `FlowDataset` persistido formalmente.

Un flujo típico es:

```text
build_flows -> write_flows -> read_flows -> filter_flows/get_trips_from_flows
```

Si el objetivo es leer archivos externos de visualización, OP-11 no es la operación adecuada. Para eso se debe trabajar con el layout exportado por [OP-09 Export flows](op09_export_flows.md). OP-11 está pensada para persistencia formal interna de Pylondrina.

## Qué recibe y qué retorna

La operación recibe:

- `path`: ruta al directorio del artefacto;
- `options`: una instancia opcional de `ReadFlowsOptions`.

`ReadFlowsOptions` permite configurar:

- `strict`: política frente a inconsistencias recuperables del sidecar o layout;
- `keep_metadata`: incorporación o no del evento `read_flows`;
- `read_flow_to_trips`: intento de carga de la tabla auxiliar `flow_to_trips`.

La operación retorna:

```python
FlowDataset, OperationReport
```

El `FlowDataset` reconstruido contiene:

- `flows`;
- `flow_to_trips`, si se solicitó y pudo cargarse;
- `aggregation_spec`;
- `metadata`;
- `provenance`;
- `source_trips = None`.

`source_trips` no se reconstruye porque es una referencia viva en memoria, no parte del snapshot persistido.

## Resolución del path

La operación intenta primero leer el path exacto entregado por el usuario. Si ese path no existe y no termina en `.golondrina`, intenta automáticamente con el sufijo canónico.

Por ejemplo, si se llama:

```python
read_flows("outputs/flows_work_gender")
```

y existe:

```text
outputs/flows_work_gender.golondrina/
```

la operación usa ese directorio como artefacto efectivo.

## Qué artefacto lee

El layout formal mínimo es:

```text
flows_work_gender.golondrina/
├── flows.feather
└── flows.metadata.json
```

o bien:

```text
flows_work_gender.golondrina/
├── flows.parquet
└── flows.metadata.json
```

Si el bundle incluye `flow_to_trips`, el layout puede ser:

```text
flows_work_gender.golondrina/
├── flows.feather
├── flow_to_trips.feather
└── flows.metadata.json
```

o su variante Parquet:

```text
flows_work_gender.golondrina/
├── flows.parquet
├── flow_to_trips.parquet
└── flows.metadata.json
```

El sidecar `flows.metadata.json` es obligatorio. Sin ese archivo, no existe lectura formal de flows.

## Qué evidencia deja

OP-11 retorna un `OperationReport` con:

- `ok`;
- `issues`;
- `summary`;
- `parameters`.

El `summary` contiene:

- `n_flows`;
- `n_columns`;
- `flow_to_trips_loaded`;
- `n_flow_to_trips`;
- `files_read`;
- `dataset_id`;
- `artifact_id`.

El bloque `parameters` registra:

- `path`;
- `strict`;
- `keep_metadata`;
- `read_flow_to_trips`.

Cuando `keep_metadata=True`, el dataset reconstruido agrega un evento `read_flows` en `metadata["events"]`. El evento contiene:

- `op`;
- `ts_utc`;
- `parameters`;
- `summary`;
- `issues_summary`.

Cuando `keep_metadata=False`, se conserva la metadata cargada desde el sidecar, pero no se agrega un nuevo evento de lectura.

## Consideraciones importantes

Leer no equivale a validar. Por diseño, `read_flows` fuerza siempre:

```python
metadata["is_validated"] = False
```

Esto ocurre incluso si el `FlowDataset` persistido estaba marcado como validado. La lectura reconstruye un objeto en memoria, pero no ejecuta una certificación formal.

La operación reconstruye solo lo persistido. No intenta recuperar referencias vivas como `source_trips`. Por eso, en el resultado:

```python
loaded_flows.source_trips is None
```

Si `read_flow_to_trips=True` y el auxiliar existe, se carga y queda disponible como `loaded_flows.flow_to_trips`. Si se solicitó pero falta, con `strict=False` la operación continúa, deja `flow_to_trips=None` y registra un warning. Con `strict=True`, la ausencia del auxiliar solicitado puede escalar a error fatal.

Si el sidecar declara `storage.format="feather"`, el archivo principal esperado es `flows.feather`. Si declara `storage.format="parquet"`, el archivo esperado es `flows.parquet`. Una inconsistencia entre backend declarado y nombre físico del archivo se trata como layout inválido.

Con `strict=False`, algunas piezas degradadas del sidecar pueden recuperarse de forma controlada, por ejemplo `dataset_id`, `artifact_id`, `aggregation_spec`, `provenance` o `metadata`. Con `strict=True`, esas inconsistencias pueden abortar la lectura.

## Ejemplo mínimo

El siguiente ejemplo reconstruye un `FlowDataset` desde un bundle formal, cargando también `flow_to_trips` si fue persistido.

```python
from pylondrina.io.flows import ReadFlowsOptions, read_flows

flows, report = read_flows(
    "outputs/flows_work_gender",
    options=ReadFlowsOptions(
        strict=False,
        keep_metadata=True,
        read_flow_to_trips=True,
    ),
)

print(report.summary)
print(flows.metadata["is_validated"])
print(flows.flow_to_trips is not None)
```

Una salida típica incluye `files_read` con `flows.feather`, `flows.metadata.json` y, si corresponde, `flow_to_trips.feather`.

Si no se desea agregar un evento `read_flows` a la metadata reconstruida:

```python
flows, report = read_flows(
    "outputs/flows_work_gender",
    options=ReadFlowsOptions(
        strict=False,
        keep_metadata=False,
        read_flow_to_trips=False,
    ),
)
```

## Operación anterior y siguiente

Dentro de la familia Trip → Flow, OP-11 reconstruye un `FlowDataset` previamente persistido por OP-10.

| Posición | Operación |
|---|---|
| Anterior recomendada | [OP-10 Write flows](op10_write_flows.md) |
| Actual | OP-11 Read flows |
| Siguiente recomendada | [OP-12 Filter flows](op12_filter_flows.md) o [OP-13 Get trips from flows](op13_get_trips_from_flows.md) |
| Operación relacionada | [OP-09 Export flows](op09_export_flows.md), si se necesita layout externo para visualización |

## Implementación relacionada

Esta operación forma parte de la API pública de Pylondrina v1.1. Su implementación principal puede revisarse en el código fuente del proyecto.

| Recurso | Enlace |
|---|---|
| Archivo fuente | [`src/pylondrina/io/flows.py`](https://github.com/sebastianolmos/pylondrina/blob/main/src/pylondrina/io/flows.py) |
| Catálogo de issues | [`src/pylondrina/issues/catalog_read_flows.py`](https://github.com/sebastianolmos/pylondrina/blob/main/src/pylondrina/issues/catalog_read_flows.py) |
| Referencia API | [Ver referencia técnica](../../api/flows.md) |