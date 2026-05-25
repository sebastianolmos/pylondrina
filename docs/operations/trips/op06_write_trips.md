# OP-06 Write trips

`write_trips` es la operación de persistencia formal para datasets de trips. Se implementó para materializar un `TripDataset` en un artefacto reproducible de Pylondrina, compuesto por un directorio bundle, un archivo tabular y un sidecar JSON obligatorio.

La operación no transforma los datos, no limpia filas, no filtra registros y no certifica conformidad formal. Su responsabilidad es escribir el estado actual del dataset en disco, dejando evidencia suficiente para que luego pueda ser reconstruido mediante [OP-07 Read trips](op07_read_trips.md).

## Para qué sirve

Esta operación permite persistir un `TripDataset` como bundle `.golondrina`. El artefacto resultante contiene:

- un archivo tabular con los datos de trips;
- un sidecar `trips.metadata.json` con schema, schema efectivo, metadata, provenance, identidad y configuración de almacenamiento.

El backend tabular puede ser:

- `trips.parquet`, si `storage_format="parquet"`;
- `trips.feather`, si `storage_format="feather"`.

El sidecar declara explícitamente qué backend fue usado, qué archivo tabular forma parte del artefacto y qué identidad tiene la materialización escrita.

## Cuándo usarla

Esta operación se usa cuando un `TripDataset` ya preparado debe quedar persistido como artefacto formal del módulo. En un flujo típico, aparece después de importar, validar y, si corresponde, corregir, limpiar o filtrar trips.

```text
import_trips_from_dataframe -> validate_trips -> clean_trips/filter_trips -> validate_trips -> write_trips
```

Por defecto, la operación exige que el dataset esté marcado como validado:

```python
metadata["is_validated"] == True
```

Esta precondición puede desactivarse con `require_validated=False`, pero el artefacto persistido conservará explícitamente que el dataset no estaba validado.

## Qué recibe y qué retorna

La operación recibe:

* `trips`: un `TripDataset` a persistir;
* `path`: directorio destino del artefacto;
* `options`: una instancia opcional de `WriteTripsOptions`.

`WriteTripsOptions` permite configurar:

* `mode`: política frente a un destino existente;
* `require_validated`: exigencia de validación previa;
* `storage_format`: backend físico, `parquet` o `feather`;
* `parquet_compression`: compresión para Parquet;
* `feather_compression`: compresión para Feather;
* `normalize_artifact_dir`: normalización automática del sufijo `.golondrina`.

La operación retorna:

```python
OperationReport
```

A diferencia de otras operaciones sobre trips, `write_trips` no retorna un nuevo `TripDataset`. El dataset recibido se mantiene como objeto vivo en memoria, pero su metadata se alinea con la escritura exitosa: se conserva o crea `dataset_id`, se genera un nuevo `artifact_id` y se agrega un evento `write_trips`.

## Qué artefacto genera

Con `normalize_artifact_dir=True`, si el path no termina en `.golondrina`, la operación normaliza el directorio destino.

Por ejemplo:

```python
write_trips(trips, "outputs/eod_trips")
```

puede materializar:

```text
outputs/eod_trips.golondrina/
├── trips.feather
└── trips.metadata.json
```

o bien:

```text
outputs/eod_trips.golondrina/
├── trips.parquet
└── trips.metadata.json
```

según el backend seleccionado.

El sidecar `trips.metadata.json` incluye, entre otros bloques:

* `dataset_type`;
* `format`;
* `layout_version`;
* `storage`;
* `files`;
* `dataset_id`;
* `artifact_id`;
* `schema`;
* `schema_effective`;
* `provenance`;
* `metadata`.

## Qué evidencia deja

OP-06 retorna un `OperationReport` con:

* `ok`;
* `issues`;
* `summary`;
* `parameters`.

El `summary` se mantiene pequeño y estable. Sus claves principales son:

* `n_rows`;
* `files_written`;
* `path`;
* `dataset_id`;
* `artifact_id`;
* `dataset_id_status`;
* `storage_format`.

El bloque `parameters` registra el request efectivo de escritura, incluyendo path resuelto, modo, backend, compresión y política de validación.

Además, si la escritura termina correctamente, el sistema agrega un evento `write_trips` en `trips.metadata["events"]`. Este evento contiene:

* `op`;
* `ts_utc`;
* `parameters`;
* `summary`;
* `issues_summary`.

El evento queda también persistido dentro del sidecar, de modo que el artefacto en disco conserva la misma evidencia que el dataset vivo tras la escritura.

## Consideraciones importantes

`write_trips` no muta `trips.data`. Su efecto sobre el objeto en memoria se limita a alinear `trips.metadata` con la materialización escrita, agregando o actualizando identidad y evento de escritura.

`dataset_id` representa la identidad lógica del dataset. Si ya existe y es válido, se preserva. Si falta o es inválido, la operación genera uno nuevo y deja evidencia en el reporte.

`artifact_id` representa la identidad de una materialización concreta. Se genera uno nuevo en cada escritura exitosa, incluso cuando se sobrescribe un artefacto anterior.

Si `mode="error_if_exists"` y el destino ya existe, la operación aborta sin sobrescribir el artefacto. Si `mode="overwrite"`, reemplaza el destino por una nueva materialización.

La escritura se realiza mediante staging antes del commit final. Esto evita exponer artefactos parciales como si fueran bundles válidos.

El sidecar es obligatorio para lectura formal. Un archivo `trips.parquet` o `trips.feather` aislado no equivale por sí solo a un artefacto Golondrina reconstruible por `read_trips`.

## Ejemplo mínimo

El siguiente ejemplo persiste un `TripDataset` validado usando Feather y normalización automática del directorio `.golondrina`.

```python
from pylondrina.io.trips import WriteTripsOptions, write_trips

report = write_trips(
    trips,
    "outputs/eod_trips",
    options=WriteTripsOptions(
        mode="overwrite",
        require_validated=True,
        storage_format="feather",
        feather_compression="lz4",
        normalize_artifact_dir=True,
    ),
)

print(report.summary)
print(trips.metadata["artifact_id"])
```

El resultado esperado es un directorio `outputs/eod_trips.golondrina/` con `trips.feather` y `trips.metadata.json`.

## Operación anterior y siguiente

Dentro de la familia de operaciones sobre trips, OP-06 materializa un dataset ya preparado como artefacto formal de persistencia.

| Posición              | Operación                                                                                   |
| --------------------- | ------------------------------------------------------------------------------------------- |
| Anterior recomendada  | [OP-02 Validate trips](op02_validate_trips.md), después de limpiar o filtrar si corresponde |
| Actual                | OP-06 Write trips                                                                           |
| Siguiente recomendada | [OP-07 Read trips](op07_read_trips.md)                                                      |

## Implementación relacionada

Esta operación forma parte de la API pública de Pylondrina v1.1. Su implementación principal puede revisarse en el código fuente del proyecto.

| Recurso            | Enlace                                                                                                                                                |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| Archivo fuente     | [`src/pylondrina/io/trips.py`](https://github.com/sebastianolmos/pylondrina/blob/main/src/pylondrina/io/trips.py)                                     |
| Catálogo de issues | [`src/pylondrina/issues/catalog_write_trips.py`](https://github.com/sebastianolmos/pylondrina/blob/main/src/pylondrina/issues/catalog_write_trips.py) |
| Referencia API     | [Ver referencia técnica](../../api/trips.md)                                                                                                          |
