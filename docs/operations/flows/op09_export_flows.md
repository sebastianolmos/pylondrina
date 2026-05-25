# OP-09 Export flows

`export_flows` es la operación que materializa un `FlowDataset` en un layout externo orientado a visualización. Se implementó para transformar el contrato interno de flows de Pylondrina hacia archivos interoperables con el formato `flowmap_blue`.

La operación no reconstruye flujos, no recalcula agregaciones, no valida formalmente el dataset, no escribe bundles internos `.golondrina` y no reemplaza la persistencia formal de flows. Su responsabilidad es exportar un `FlowDataset` ya construido hacia un conjunto de archivos externos listos para visualización o intercambio.

## Para qué sirve

Esta operación permite llevar un `FlowDataset` desde el contrato interno de Pylondrina hacia un layout externo tipo flowmap. En v1.1, el único formato soportado es:

```python
"flowmap_blue"
```

El resultado materializado contiene:

* `flows.csv`;
* `locations.csv`;
* `metadata.json`.

`flows.csv` representa los flujos OD en el layout externo. `locations.csv` representa los nodos H3 usados como origen o destino. `metadata.json` documenta el artefacto exportado, la configuración usada y una referencia trazable al `FlowDataset` de origen.

## Cuándo usarla

Esta operación se usa después de [OP-08 Build flows](op08_build_flows.md), cuando se necesita producir artefactos externos para visualización, inspección o interoperabilidad.

Un flujo típico es:

```text
validate_trips -> build_flows -> export_flows
```

Si el objetivo es persistir internamente un `FlowDataset` para reconstruirlo después dentro de Pylondrina, se debe usar [OP-10 Write flows](op10_write_flows.md). Si el objetivo es producir archivos para herramientas de visualización tipo flowmap, se usa OP-09.

## Qué recibe y qué retorna

La operación recibe:

* `flows`: un `FlowDataset`;
* `output_root`: directorio raíz donde se creará la carpeta de exportación;
* `options`: una instancia opcional de `ExportFlowsOptions`.

`ExportFlowsOptions` permite configurar:

* `format`: formato externo, actualmente `flowmap_blue`;
* `mode`: política frente a una carpeta de exportación existente;
* `folder_name`: nombre de la carpeta a crear dentro de `output_root`;
* `extra_flow_fields`: columnas adicionales de `FlowDataset.flows` que se desean preservar en `flows.csv`.

La operación retorna:

```python
FlowExportResult, OperationReport
```

`FlowExportResult` contiene el directorio final de exportación y las rutas de los artefactos escritos. `OperationReport` resume la ejecución, parámetros efectivos, issues y conteos principales del export.

## Qué artefactos genera

La estructura esperada es:

```text
output_root/
└── folder_name/
    ├── flows.csv
    ├── locations.csv
    └── metadata.json
```

`flows.csv` se construye desde `FlowDataset.flows` con el siguiente mapping fijo:

| Campo externo | Campo interno          |
| ------------- | ---------------------- |
| `origin`      | `origin_h3_index`      |
| `dest`        | `destination_h3_index` |
| `count`       | `flow_value`           |

En v1.1, `count` se construye desde `flow_value`. `flow_count` no reemplaza a `count` en el layout externo, aunque puede exportarse como campo extra si se solicita explícitamente.

`locations.csv` se construye a partir de todos los H3 únicos usados como origen o destino. Sus columnas son:

* `id`;
* `name`;
* `lat`;
* `lon`.

`lat` y `lon` corresponden al centroide de la celda H3.

## Qué evidencia deja

OP-09 retorna un `OperationReport` con:

* `ok`;
* `issues`;
* `summary`;
* `parameters`.

El `summary` contiene:

* `n_flows`;
* `n_locations`;
* `files_written`.

El bloque `parameters` registra:

* `output_root`;
* `export_dir`;
* `format`;
* `mode`;
* `folder_name`;
* `extra_flow_fields`.

Además, si la exportación se completa, la operación agrega un evento `export_flows` en `flows.metadata["events"]`. El evento contiene:

* `op`;
* `ts_utc`;
* `parameters`;
* `summary`;
* `issues_summary`.

El sidecar `metadata.json` conserva una referencia al `FlowDataset` usado como origen mediante `flow_dataset_ref`, incluyendo `dataset_id`, `aggregation_spec`, `provenance` y metadata del dataset de flows. También registra la configuración de exportación, el summary y `count_source`.

## Consideraciones importantes

La operación exige que `FlowDataset.flows` tenga al menos:

```python
origin_h3_index
destination_h3_index
flow_value
```

Además, `origin_h3_index` y `destination_h3_index` no deben ser nulos, y `flow_value` debe ser numérico y no nulo. Si estas condiciones no se cumplen, la exportación aborta.

`extra_flow_fields` no exporta todas las columnas adicionales por defecto. Solo se preservan las columnas solicitadas explícitamente. Esas columnas deben existir en `FlowDataset.flows`, no pueden usar nombres reservados del layout externo y deben ser serializables a CSV. Los nombres reservados son:

```python
origin
dest
count
```

Los campos extra más habituales son columnas de segmentación o ventanas temporales, por ejemplo:

```python
["user_gender", "mode", "purpose", "window_start_utc", "window_end_utc"]
```

`flow_to_trips` no se exporta como archivo separado en v1.1. Si se necesita inspeccionar la relación entre flujos y movements, se debe conservar ese auxiliar en el `FlowDataset` y usar operaciones internas como [OP-13 Get trips from flows](op13_get_trips_from_flows.md).

Si `folder_name=None`, el sistema genera un nombre efectivo para la carpeta de exportación. Si `folder_name` contiene caracteres no operables, se sanea. Si el directorio final ya existe, `mode="error_if_exists"` aborta, mientras que `mode="overwrite"` reemplaza el destino y deja evidencia.

## Ejemplo mínimo

El siguiente ejemplo exporta un `FlowDataset` al layout `flowmap_blue`, reemplazando una carpeta previa si existe.

```python
from pylondrina.export.flows import ExportFlowsOptions, export_flows

result, report = export_flows(
    flows,
    output_root="outputs/flow_exports",
    options=ExportFlowsOptions(
        format="flowmap_blue",
        mode="overwrite",
        folder_name="baseline_flows",
        extra_flow_fields=None,
    ),
)

print(report.summary)
print(result.artifacts)
```

El resultado esperado es una carpeta:

```text
outputs/flow_exports/baseline_flows/
├── flows.csv
├── locations.csv
└── metadata.json
```

Un ejemplo con columnas extra puede conservar segmentación o ventanas temporales:

```python
result, report = export_flows(
    flows_by_gender,
    output_root="outputs/flow_exports",
    options=ExportFlowsOptions(
        format="flowmap_blue",
        mode="overwrite",
        folder_name="flows_by_gender",
        extra_flow_fields=["user_gender", "window_start_utc"],
    ),
)
```

## Operación anterior y siguiente

Dentro de la familia Trip → Flow, OP-09 consume un `FlowDataset` ya construido y produce un layout externo de visualización.

| Posición              | Operación                                                                                               |
| --------------------- | ------------------------------------------------------------------------------------------------------- |
| Anterior recomendada  | [OP-08 Build flows](op08_build_flows.md)                                                                |
| Actual                | OP-09 Export flows                                                                                      |
| Alternativa posterior | [OP-10 Write flows](op10_write_flows.md), si se necesita persistencia interna                           |
| Operación relacionada | [OP-13 Get trips from flows](op13_get_trips_from_flows.md), si se necesita inspeccionar `flow_to_trips` |

## Implementación relacionada

Esta operación forma parte de la API pública de Pylondrina v1.1. Su implementación principal puede revisarse en el código fuente del proyecto.

| Recurso            | Enlace                                                                                                                                                  |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Archivo fuente     | [`src/pylondrina/export/flows.py`](https://github.com/sebastianolmos/pylondrina/blob/main/src/pylondrina/export/flows.py)                               |
| Catálogo de issues | [`src/pylondrina/issues/catalog_export_flows.py`](https://github.com/sebastianolmos/pylondrina/blob/main/src/pylondrina/issues/catalog_export_flows.py) |
| Referencia API     | [Ver referencia técnica](../../api/flows.md)                                                                                                            |