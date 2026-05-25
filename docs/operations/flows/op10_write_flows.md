# OP-10 Write flows

`write_flows` es la operación de persistencia formal interna para datasets de flows. Se implementó para materializar un `FlowDataset` como bundle `.golondrina`, con una tabla principal de flows, un sidecar obligatorio y, opcionalmente, una tabla auxiliar `flow_to_trips`.

La operación no exporta a layouts externos de visualización, no reconstruye flows desde trips, no valida formalmente el dataset y no recalcula la agregación. Su responsabilidad es congelar el estado persistible de un `FlowDataset` para que pueda ser reconstruido posteriormente mediante [OP-11 Read flows](op11_read_flows.md).

## Para qué sirve

Esta operación permite persistir un `FlowDataset` como artefacto formal interno de Pylondrina. El artefacto resultante contiene:

- una tabla principal de flows;
- un sidecar `flows.metadata.json`;
- opcionalmente, una tabla auxiliar `flow_to_trips`.

El backend tabular puede ser:

- `flows.feather`, si `storage_format="feather"`;
- `flows.parquet`, si `storage_format="parquet"`.

Cuando se persiste `flow_to_trips`, el auxiliar usa el mismo backend:

- `flow_to_trips.feather`;
- `flow_to_trips.parquet`.

## Cuándo usarla

Esta operación se usa después de construir flows con [OP-08 Build flows](op08_build_flows.md), cuando se necesita conservar internamente el `FlowDataset` para reconstrucción posterior dentro de Pylondrina.

Un flujo típico es:

```text
build_flows -> write_flows -> read_flows
```

Si el objetivo es generar archivos externos para visualización, corresponde usar [OP-09 Export flows](op09_export_flows.md). Si el objetivo es persistir el objeto interno de Pylondrina, corresponde usar OP-10.

## Qué recibe y qué retorna

La operación recibe:

- `flows`: un `FlowDataset`;
- `path`: directorio destino del artefacto;
- `options`: una instancia opcional de `WriteFlowsOptions`.

`WriteFlowsOptions` permite configurar:

- `mode`: política frente a un destino existente;
- `storage_format`: backend físico, `feather` o `parquet`;
- `parquet_compression`: compresión para Parquet;
- `feather_compression`: compresión para Feather;
- `normalize_artifact_dir`: normalización automática del sufijo `.golondrina`;
- `write_flow_to_trips`: persistencia opcional de la tabla auxiliar.

La operación retorna:

```python
OperationReport
```

A diferencia de `build_flows`, `write_flows` no retorna un nuevo `FlowDataset`. El dataset recibido se mantiene como objeto vivo en memoria. La operación no muta `flows.flows`, pero sí alinea `flows.metadata` después de una escritura exitosa, agregando `artifact_id` y evento `write_flows`.

## Qué artefacto genera

Con `normalize_artifact_dir=True`, si el path no termina en `.golondrina`, la operación normaliza el directorio destino.

Por ejemplo:

```python
write_flows(flows, "outputs/flows_work_gender")
```

puede materializar:

```text
outputs/flows_work_gender.golondrina/
├── flows.feather
├── flow_to_trips.feather
└── flows.metadata.json
```

o bien:

```text
outputs/flows_work_gender.golondrina/
├── flows.parquet
├── flow_to_trips.parquet
└── flows.metadata.json
```

Si `write_flow_to_trips=False`, o si el `FlowDataset` no contiene `flow_to_trips`, el bundle se escribe sin tabla auxiliar.

El sidecar `flows.metadata.json` incluye, entre otros bloques:

- `dataset_type`;
- `format`;
- `layout_version`;
- `storage`;
- `files`;
- `dataset_id`;
- `artifact_id`;
- `aggregation_spec`;
- `provenance`;
- `metadata`;
- `tables`.

`source_trips` no se persiste en el sidecar, porque es una referencia viva de memoria, no parte del snapshot serializable.

## Qué evidencia deja

OP-10 retorna un `OperationReport` con:

- `ok`;
- `issues`;
- `summary`;
- `parameters`.

El `summary` contiene:

- `n_flows`;
- `n_flow_to_trips`;
- `files_written`;
- `dataset_id`;
- `artifact_id`;
- `path`.

El bloque `parameters` registra:

- `path`;
- `mode`;
- `storage_format`;
- `parquet_compression`;
- `feather_compression`;
- `normalize_artifact_dir`;
- `write_flow_to_trips`.

Además, si la escritura termina correctamente, el sistema agrega un evento `write_flows` en `flows.metadata["events"]`. El evento contiene:

- `op`;
- `ts_utc`;
- `parameters`;
- `summary`;
- `issues_summary`.

Ese mismo evento queda persistido dentro de `flows.metadata.json`, de modo que el estado vivo del objeto y el snapshot en disco quedan alineados.

## Consideraciones importantes

`write_flows` no exige que `flows.metadata["is_validated"]` sea `True`. A diferencia de `write_trips`, la persistencia de flows no incorpora una opción `require_validated`.

`dataset_id` representa la identidad lógica del `FlowDataset`. Si existe y es válido, se preserva. Si falta, se crea. Si existe pero no es interpretable, se regenera con evidencia.

`artifact_id` representa la materialización concreta escrita en disco. Se genera uno nuevo en cada escritura exitosa.

`mode="error_if_exists"` aborta si el bundle destino ya existe. `mode="overwrite"` reemplaza el bundle existente y deja evidencia.

La operación escribe mediante staging antes del commit final. Esto evita exponer bundles incompletos como si fueran artefactos válidos.

Feather es el backend por defecto actual para flows. Parquet se mantiene soportado explícitamente. En ambos casos, el sidecar declara qué backend fue usado y qué archivos forman parte del bundle.

Si `write_flow_to_trips=True` pero el `FlowDataset` no contiene una tabla auxiliar usable, el bundle se escribe sin auxiliar y se registra un warning. Si el auxiliar existe, se persiste con el mismo backend que la tabla principal.

## Ejemplo mínimo

El siguiente ejemplo persiste un `FlowDataset` usando Feather y guardando también `flow_to_trips` cuando está disponible.

```python
from pylondrina.io.flows import WriteFlowsOptions, write_flows

report = write_flows(
    flows,
    "outputs/flows_work_gender",
    options=WriteFlowsOptions(
        mode="overwrite",
        storage_format="feather",
        feather_compression="lz4",
        normalize_artifact_dir=True,
        write_flow_to_trips=True,
    ),
)

print(report.summary)
print(flows.metadata["artifact_id"])
```

El resultado esperado es un bundle `.golondrina` con `flows.feather`, `flows.metadata.json` y, si corresponde, `flow_to_trips.feather`.

También puede escribirse en Parquet:

```python
report = write_flows(
    flows,
    "outputs/flows_work_gender_parquet",
    options=WriteFlowsOptions(
        mode="overwrite",
        storage_format="parquet",
        parquet_compression="snappy",
        normalize_artifact_dir=True,
        write_flow_to_trips=False,
    ),
)
```

## Operación anterior y siguiente

Dentro de la familia Trip → Flow, OP-10 materializa un `FlowDataset` como persistencia formal interna.

| Posición | Operación |
|---|---|
| Anterior recomendada | [OP-08 Build flows](op08_build_flows.md) |
| Actual | OP-10 Write flows |
| Siguiente recomendada | [OP-11 Read flows](op11_read_flows.md) |
| Operación relacionada | [OP-09 Export flows](op09_export_flows.md), si se necesita layout externo para visualización |

## Implementación relacionada

Esta operación forma parte de la API pública de Pylondrina v1.1. Su implementación principal puede revisarse en el código fuente del proyecto.

| Recurso | Enlace |
|---|---|
| Archivo fuente | [`src/pylondrina/io/flows.py`](https://github.com/sebastianolmos/pylondrina/blob/main/src/pylondrina/io/flows.py) |
| Catálogo de issues | [`src/pylondrina/issues/catalog_write_flows.py`](https://github.com/sebastianolmos/pylondrina/blob/main/src/pylondrina/issues/catalog_write_flows.py) |
| Referencia API | [Ver referencia técnica](../../api/flows.md) |