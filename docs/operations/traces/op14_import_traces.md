# OP-14 Import traces

`import_traces_from_dataframe` es la operación de entrada para construir un `TraceDataset` canónico desde una tabla de puntos espacio-temporales discretos. Se implementó para alinear columnas, materializar el núcleo mínimo de traces, normalizar temporalidad básica, conservar campos extra según una política explícita y dejar evidencia reproducible del proceso de importación.

La operación no valida formalmente el dataset, no infiere viajes, no reconstruye trayectorias continuas, no procesa GPS denso y no escribe artefactos en disco. Su responsabilidad es transformar un `DataFrame` ya pointificado, o al menos mapeable a puntos, en un `TraceDataset` utilizable por las operaciones posteriores del bloque de traces.

## Para qué sirve

Esta operación permite incorporar fuentes de puntos discretos al contrato Golondrina usado por Pylondrina. Sirve para datasets como registros de telefonía agregados a puntos, check-ins, puntos de presencia o telemetría discreta ya preparada como observaciones espacio-temporales.

El núcleo canónico post-import es:

```python
point_id
user_id
time_utc
latitude
longitude
```

Si `point_id` no existe después del mapeo efectivo, la operación lo genera automáticamente. En cambio, `user_id`, `time_utc`, `latitude` y `longitude` deben poder materializarse desde la fuente o mediante `field_correspondence`.

## Cuándo usarla

Esta operación se usa al inicio del bloque de traces, cuando existe una tabla de puntos y se necesita llevarla a un `TraceDataset` operable.

Un flujo típico es:

```text
import_traces_from_dataframe -> validate_traces -> infer_trips_from_traces
```

OP-14 no reemplaza a [OP-15 Validate traces](op15_validate_traces.md). Después de importar, el dataset queda siempre con:

```python
metadata["is_validated"] = False
```

Por lo tanto, si se requiere certificación formal antes de inferir viajes o continuar el pipeline, debe ejecutarse `validate_traces`.

## Qué recibe y qué retorna

La operación recibe:

- `df`: un `pandas.DataFrame` con puntos discretos;
- `schema`: un `TraceSchema`;
- `source_name`: nombre opcional de la fuente;
- `options`: una instancia opcional de `ImportTraceOptions`;
- `field_correspondence`: mapping opcional desde campos canónicos hacia columnas fuente;
- `provenance`: diccionario opcional con procedencia externa.

`ImportTraceOptions` permite configurar:

- `keep_extra_fields`;
- `selected_fields`;
- `strict`;
- `source_timezone`.

La operación retorna:

```python
TraceDataset, ImportReport
```

El `TraceDataset` resultante contiene:

- `data`;
- `schema`;
- `provenance`;
- `metadata`.

A diferencia de trips, el `TraceDataset` de v1.1 no expone atributos vivos como `field_correspondence`, `value_correspondence`, `schema_effective` o `domains_effective`. Esos elementos no forman parte del alcance austero de traces.

## Correspondencia de campos

`field_correspondence` permite mapear columnas fuente hacia nombres canónicos. El mapping se expresa como:

```python
{
    "user_id": "subscriber_id",
    "time_utc": "event_timestamp",
    "latitude": "lat",
    "longitude": "lon",
}
```

La operación aplica solo las correspondencias que son alcanzables desde el `DataFrame`. Las correspondencias efectivamente aplicadas quedan registradas en:

- `ImportReport.field_correspondence`;
- `metadata["field_correspondence_applied"]`, cuando existe al menos una correspondencia aplicada.

No se aplican correspondencias de valores categóricos en OP-14. Por diseño, `ImportReport.value_correspondence` queda como `{}`.

## Política de campos extra

Por defecto, `keep_extra_fields=True`, por lo que la operación conserva columnas adicionales que no pertenecen al núcleo canónico. Esto permite mantener atributos propios de la fuente, por ejemplo proveedor, tipo de punto, peso muestral, nombre de lugar o identificadores auxiliares.

Si `keep_extra_fields=False`, se conservan solo los campos alcanzables que pertenecen al núcleo o al `TraceSchema`.

Si `selected_fields=None`, se aplica la política general de `keep_extra_fields`.

Si `selected_fields=[]`, se conserva solo el núcleo canónico de traces.

Si `selected_fields` contiene nombres explícitos, la operación conserva el núcleo canónico más esos campos seleccionados, siempre que existan después del mapeo efectivo.

## Temporalidad y zona horaria

OP-14 consolida la columna `time_utc` como campo temporal canónico. La operación interpreta timestamps con esta precedencia:

1. timezone explícita en los datos;
2. `options.source_timezone`;
3. `schema.timezone`;
4. timezone no resuelta.

Cuando los datos no traen zona horaria explícita, `source_timezone` permite interpretar timestamps naive. Por ejemplo:

```python
ImportTraceOptions(source_timezone="America/Santiago")
```

La metadata temporal registra:

- `time_field`;
- `timezone_resolution`;
- `source_timezone_used`;
- `schema_timezone`;
- `normalized_to_utc`.

Si la timezone no puede resolverse, la operación puede continuar con advertencia cuando los timestamps son parseables, pero deja evidencia de que la temporalidad no fue normalizada a UTC.

## Qué evidencia deja

OP-14 retorna un `ImportReport` con:

- `ok`;
- `issues`;
- `summary`;
- `parameters`;
- `field_correspondence`;
- `value_correspondence`;
- `schema_version`;
- `metadata`.

El `summary` contiene:

- `rows_in`;
- `rows_out`;
- `n_fields_mapped`;
- `point_id_generated`.

El bloque `parameters` registra, entre otros elementos:

- `source_name`;
- `strict`;
- `keep_extra_fields`;
- `selected_fields`;
- `source_timezone`;
- `schema_version`;
- `crs`;
- `timezone`;
- `has_field_correspondence`.

Además, el dataset resultante registra un evento `import_traces` en `metadata["events"]`. El evento contiene:

- `op`;
- `ts_utc`;
- `parameters`;
- `summary`;
- `issues_summary`.

## Consideraciones importantes

OP-14 trabaja sobre una copia del dataframe de entrada. La operación no muta el `DataFrame` original.

`point_id` se genera solo cuando falta después del mapeo efectivo. Los identificadores generados siguen una convención técnica simple y quedan informados mediante `point_id_generated=True`.

El núcleo mínimo obligatorio para importación efectiva es:

```python
user_id
time_utc
latitude
longitude
```

Si alguno de esos campos no puede materializarse, la operación aborta porque no puede construir un `TraceDataset` mínimo.

El tipo `categorical` no está soportado en `TraceSchema` para OP-14 v1.1. Los dtypes permitidos son acotados y buscan mantener traces como una representación simple de puntos discretos.

La operación permite conservar campos extra, pero no interpreta dominios categóricos ricos. Si se requiere validar conformidad mínima del resultado, debe usarse [OP-15 Validate traces](op15_validate_traces.md).

Si `provenance` no es un mapping serializable, se omite con warning y la importación puede continuar.

## Ejemplo mínimo

El siguiente ejemplo importa una tabla de puntos usando correspondencias explícitas de campos.

```python
from pylondrina.importing_traces import (
    ImportTraceOptions,
    import_traces_from_dataframe,
)

traces, report = import_traces_from_dataframe(
    raw_points,
    trace_schema,
    source_name="telefonia_rm",
    options=ImportTraceOptions(
        source_timezone="America/Santiago",
        keep_extra_fields=True,
    ),
    field_correspondence={
        "point_id": "id_punto",
        "user_id": "id_usuario",
        "time_utc": "timestamp",
        "latitude": "lat",
        "longitude": "lon",
    },
    provenance={
        "source_family": "telefonia",
        "region": "RM",
    },
)

print(report.summary)
print(traces.metadata["is_validated"])
print(traces.data.head())
```

Si la fuente no trae `point_id`, se puede omitir esa correspondencia. La operación generará identificadores y dejará evidencia en el reporte.

```python
traces, report = import_traces_from_dataframe(
    raw_points,
    trace_schema,
    options=ImportTraceOptions(source_timezone="UTC"),
    field_correspondence={
        "user_id": "device_id",
        "time_utc": "event_time",
        "latitude": "lat",
        "longitude": "lon",
    },
)

print(report.summary["point_id_generated"])
```

## Operación anterior y siguiente

Dentro de la familia traces, OP-14 es la puerta de entrada al bloque de puntos discretos.

| Posición | Operación |
|---|---|
| Actual | OP-14 Import traces |
| Siguiente recomendada | [OP-15 Validate traces](op15_validate_traces.md) |
| Operación posterior relacionada | [OP-16 Infer trips from traces](op16_infer_trips_from_traces.md), después de validar cuando corresponda |

## Implementación relacionada

Esta operación forma parte de la API pública de Pylondrina v1.1. Su implementación principal puede revisarse en el código fuente del proyecto.

| Recurso | Enlace |
|---|---|
| Archivo fuente | [`src/pylondrina/importing_traces.py`](https://github.com/sebastianolmos/pylondrina/blob/main/src/pylondrina/importing_traces.py) |
| Catálogo de issues | [`src/pylondrina/issues/catalog_import_traces.py`](https://github.com/sebastianolmos/pylondrina/blob/main/src/pylondrina/issues/catalog_import_traces.py) |
| Referencia API | [Ver referencia técnica](../../api/traces.md) |