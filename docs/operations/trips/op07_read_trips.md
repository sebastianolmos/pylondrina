# OP-07 Read trips

`read_trips` es la operación de lectura formal para artefactos persistidos de trips. Se implementó para reconstruir un `TripDataset` desde un bundle `.golondrina` escrito previamente por [OP-06 Write trips](op06_write_trips.md).

La operación no importa una fuente externa, no valida semánticamente los datos y no certifica conformidad formal. Su responsabilidad es reconstruir desde disco el dataset, su schema, su schema efectivo, su provenance, sus mappings y su metadata, usando el sidecar `trips.metadata.json` como fuente de verdad del artefacto.

## Para qué sirve

Esta operación permite retomar un pipeline a partir de un artefacto formal de trips. Se usa cuando un dataset ya fue persistido como bundle `.golondrina` y se necesita reconstruirlo en memoria como `TripDataset`.

El artefacto esperado contiene:

- `trips.metadata.json`, sidecar obligatorio;
- `trips.parquet` o `trips.feather`, según el backend declarado en el sidecar.

El usuario no debe indicar manualmente el backend. La operación lo resuelve desde:

```python
sidecar["storage"]["format"]
```

y verifica su coherencia con:

```python
sidecar["files"]["data"]
```

## Cuándo usarla

Esta operación se usa al inicio de una sesión o etapa posterior, cuando se quiere continuar trabajando desde un artefacto persistido por Pylondrina.

Un flujo típico es:

```text
write_trips -> read_trips -> validate_trips -> filter_trips/build_flows
```

Después de leer, se recomienda ejecutar [OP-02 Validate trips](op02_validate_trips.md) si se necesita restablecer una certificación formal de conformidad antes de seguir con operaciones que exigen datasets validados.

## Qué recibe y qué retorna

La operación recibe:

* `path`: ruta al directorio del artefacto;
* `options`: una instancia opcional de `ReadTripsOptions`.

`ReadTripsOptions` permite configurar:

* `schema`: schema explícito opcional;
* `strict`: política frente a inconsistencias recuperables;
* `keep_metadata`: incorporación o no del evento `read_trips`.

La operación retorna:

```python 
TripDataset, OperationReport
```

El `TripDataset` reconstruido contiene:

* `data`;
* `schema`;
* `schema_effective`;
* `provenance`;
* `field_correspondence`;
* `value_correspondence`;
* `metadata`.

El `OperationReport` resume la lectura, el backend detectado, la fuente del schema, la identidad reconstruida y los issues detectados.

## Resolución del path

La operación intenta primero leer el path exacto entregado por el usuario. Si ese path no existe y no termina en `.golondrina`, intenta automáticamente con el sufijo canónico.

Por ejemplo, si se llama:

```python
read_trips("outputs/eod_trips")
```

y existe:

```text
outputs/eod_trips.golondrina/
```

la operación resuelve ese directorio como artefacto efectivo.

## Qué artefacto lee

El layout formal mínimo es:

```text
eod_trips.golondrina/
├── trips.metadata.json
└── trips.parquet
```

o bien:

```text
eod_trips.golondrina/
├── trips.metadata.json
└── trips.feather
```

El sidecar `trips.metadata.json` es obligatorio. Un archivo tabular aislado no es suficiente para lectura formal.

Si falta `trips.metadata.json`, la operación aborta. Si solo existe un `metadata.json` legacy, la operación también aborta porque no lo interpreta como sidecar formal de trips.

## Qué evidencia deja

OP-07 retorna un `OperationReport` con:

* `ok`;
* `issues`;
* `summary`;
* `parameters`.

El `summary` incluye:

* `n_rows`;
* `n_columns`;
* `path`;
* `storage_format`;
* `schema_source`;
* `schema_mismatch`;
* `dataset_id`;
* `dataset_id_status`;
* `artifact_id`;
* `artifact_id_status`.

El bloque `parameters` registra el path efectivo, la política `strict`, la política `keep_metadata` y la fuente efectiva del schema.

Cuando `keep_metadata=True`, la operación agrega un evento `read_trips` en `metadata["events"]` del dataset reconstruido. El evento contiene:

* `op`;
* `ts_utc`;
* `parameters`;
* `summary`;
* `issues_summary`.

Cuando `keep_metadata=False`, se conserva la metadata cargada desde el sidecar, pero no se agrega un nuevo evento de lectura.

## Consideraciones importantes

Leer no equivale a validar. Por diseño, `read_trips` fuerza siempre:

```python
metadata["is_validated"] = False
```

Esto ocurre incluso si el artefacto persistido había sido escrito desde un dataset validado. La razón es que la lectura reconstruye el objeto en memoria, pero no ejecuta una certificación formal de conformidad. Si se necesita recuperar ese estado, debe ejecutarse `validate_trips`.

La fuente del schema se resuelve con esta precedencia:

1. `options.schema`, si se entrega;
2. snapshot `schema` del sidecar.

Si se entrega `options.schema` y no coincide con el schema persistido, la operación registra `schema_mismatch=True`. Con `strict=False`, puede continuar con advertencia. Con `strict=True`, puede abortar.

`schema_effective` se reconstruye desde el sidecar. Si falta o no es recuperable, con `strict=False` la operación puede degradar a un `TripSchemaEffective` vacío y dejar evidencia. Con `strict=True`, esa situación aborta.

`artifact_id` faltante o inválido no se reinventa como si correspondiera al artefacto original. Bajo `strict=False`, puede quedar como `None` y registrarse advertencia.

## Ejemplo mínimo

El siguiente ejemplo reconstruye un `TripDataset` desde un bundle `.golondrina` usando el schema persistido en metadata.

```python
from pylondrina.io.trips import ReadTripsOptions, read_trips

trips, report = read_trips(
    "outputs/eod_trips",
    options=ReadTripsOptions(
        schema=None,
        strict=False,
        keep_metadata=True,
    ),
)

print(report.summary)
print(trips.metadata["is_validated"])
```

Una salida típica indica `schema_source="metadata"` y `storage_format="parquet"` o `storage_format="feather"`, según el backend declarado en el sidecar.

Después de leer, si se requiere certificación formal:

```python
from pylondrina.validation import validate_trips

validation_report = validate_trips(trips)
```

## Operación anterior y siguiente

Dentro de la familia de operaciones sobre trips, OP-07 reconstruye un dataset persistido por OP-06.

| Posición              | Operación                                      |
| --------------------- | ---------------------------------------------- |
| Anterior recomendada  | [OP-06 Write trips](op06_write_trips.md)       |
| Actual                | OP-07 Read trips                               |
| Siguiente recomendada | [OP-02 Validate trips](op02_validate_trips.md) |

## Implementación relacionada

Esta operación forma parte de la API pública de Pylondrina v1.1. Su implementación principal puede revisarse en el código fuente del proyecto.

| Recurso            | Enlace                                                                                                                                              |
| ------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| Archivo fuente     | [`src/pylondrina/io/trips.py`](https://github.com/sebastianolmos/pylondrina/blob/main/src/pylondrina/io/trips.py)                                   |
| Catálogo de issues | [`src/pylondrina/issues/catalog_read_trips.py`](https://github.com/sebastianolmos/pylondrina/blob/main/src/pylondrina/issues/catalog_read_trips.py) |
| Referencia API     | [Ver referencia técnica](../../api/trips.md)                                                                                                        |
