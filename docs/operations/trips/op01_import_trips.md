# OP-01 Import trips

`import_trips_from_dataframe` es la operación de entrada para construir un `TripDataset` a partir de una tabla externa de viajes o desplazamientos OD. Su propósito es llevar una fuente tabular heterogénea hacia una representación operable bajo el contrato Golondrina, aplicando correspondencias de campos, correspondencias de valores categóricos, coerciones mínimas de tipo, detección temporal, derivación H3 cuando corresponde y registro de trazabilidad.

La operación no certifica conformidad completa. Su salida es un dataset construible y trazable, pero queda explícitamente marcado como no validado. La certificación formal se realiza después mediante [OP-02 Validate trips](op02_validate_trips.md).

## Para qué sirve

Esta operación permite incorporar una fuente externa de viajes al pipeline de Pylondrina. Se utiliza cuando los datos existen como `pandas.DataFrame`, pero sus nombres de columnas, categorías o nivel de completitud todavía no coinciden completamente con el contrato Golondrina.

Durante la importación, el sistema puede:

- mapear columnas de la fuente hacia nombres canónicos;
- mapear valores categóricos hacia dominios estándar;
- conservar campos adicionales como extensiones compatibles;
- generar `movement_id` cuando falta;
- derivar `trip_id` y `movement_seq` en fuentes de una sola etapa;
- interpretar la temporalidad disponible como Tier 1, Tier 2 o Tier 3;
- normalizar datetimes cuando corresponde;
- derivar `origin_h3_index` y `destination_h3_index` desde coordenadas OD;
- registrar metadata, reporte e historial de ejecución.

## Cuándo usarla

Esta operación se usa al inicio del flujo de trabajo sobre trips, después de preparar mínimamente la fuente en memoria y antes de validar formalmente el dataset.

Un flujo típico es:

```text
tabla fuente -> import_trips_from_dataframe -> validate_trips -> clean/filter/fix -> validate_trips -> build_flows
```

Conviene usarla cuando la fuente contiene información suficiente para construir viajes o movements OD, aunque todavía use nombres o valores propios de la fuente. Por ejemplo, una encuesta puede usar columnas como `id_persona`, `motivo` o `modo`, mientras que Golondrina espera campos como `user_id`, `purpose` o `mode`.

## Qué recibe y qué retorna

La operación recibe principalmente:

* `df`: tabla fuente en forma de `pandas.DataFrame`;
* `schema`: `TripSchema` que define el contrato Golondrina esperado;
* `options`: configuración de importación mediante `ImportOptions`;
* `field_correspondence`: mapping desde campo canónico Golondrina hacia columna fuente;
* `value_correspondence`: mapping de valores categóricos por campo;
* `provenance`: información de procedencia definida por el usuario;
* `h3_resolution`: resolución H3 usada para derivar celdas OD cuando existan coordenadas suficientes.

Retorna una tupla:

```python
TripDataset, ImportReport
```

El `TripDataset` contiene la tabla importada en `data`, el `schema`, el `schema_effective`, la `provenance`, las correspondencias aplicadas y la metadata de trazabilidad. El `ImportReport` resume el resultado de la importación, incluyendo issues, parámetros efectivos, correspondencias aplicadas y un resumen pequeño con filas de entrada/salida y cantidad de mappings aplicados.

## Qué evidencia deja

OP-01 deja evidencia en dos niveles.

Primero, retorna un `ImportReport` con:

* `ok`;
* `issues`;
* `summary`;
* `parameters`;
* `field_correspondence`;
* `value_correspondence`;
* `schema_version`;
* `metadata`.

El `summary` del reporte se mantiene compacto y estable. Sus claves principales son:

* `rows_in`;
* `rows_out`;
* `n_fields_mapped`;
* `n_domain_mappings_applied`.

Segundo, registra trazabilidad dentro de `TripDataset.metadata`. Entre los bloques más relevantes se encuentran:

* `dataset_id`;
* `is_validated`;
* `schema`;
* `schema_effective`;
* `mappings`;
* `domains_effective`;
* `domains_extended`;
* `extra_fields_kept`;
* `temporal`;
* `h3`, cuando aplica;
* `events`.

El evento registrado usa `op = "import_trips"` e incluye parámetros efectivos, resumen operativo e `issues_summary`. Este evento es más expresivo que el `summary` del reporte, porque puede registrar columnas agregadas o eliminadas, tier temporal detectado, dominios extendidos y otros detalles del recorrido de importación.

## Consideraciones importantes

La importación no reemplaza la validación. Todo `TripDataset` construido por esta operación queda con:

```python
metadata["is_validated"] == False
```

Esto es intencional: importar significa construir una representación operable; validar significa certificar conformidad formal.

`field_correspondence` se define desde el campo canónico hacia la columna fuente. Por ejemplo:

```python
{
    "user_id": "id_persona",
    "purpose": "motivo"
}
```

`value_correspondence` se define por campo, desde valores de la fuente hacia valores canónicos. Por ejemplo:

```python
{
    "purpose": {
        "Trabajo": "work",
        "Estudio": "education"
    }
}
```

Si `single_stage=True`, la operación interpreta que cada fila representa un viaje de una sola etapa. En ese caso puede completar `trip_id` desde `movement_id` y fijar `movement_seq = 0` cuando esos campos no vienen desde la fuente.

La opción `source_timezone` se usa para interpretar datetimes sin zona horaria explícita cuando el dataset alcanza temporalidad Tier 1. Si la fuente solo contiene horas locales tipo `HH:MM`, la operación registra temporalidad Tier 2. Si no hay temporalidad OD explícita, registra Tier 3.

La derivación H3 requiere coordenadas OD utilizables y una resolución válida. Cuando no es posible materializar campos H3 requeridos, la operación puede abortar porque el dataset no alcanza el núcleo necesario para operaciones posteriores como construcción de flujos.

Aunque el `TripSchema` permite extensión controlada del contrato, OP-01 no debe entenderse como un importador genérico para contratos arbitrarios. Varias operaciones posteriores dependen de nombres canónicos como `movement_id`, coordenadas OD, tiempos OD, H3, `trip_id`, `movement_seq` y, cuando corresponde, `trip_weight`.

## Ejemplo mínimo

El siguiente ejemplo muestra una importación con correspondencias de campos, correspondencias de valores, zona horaria de origen y derivación H3. Se asume que `trip_schema` ya fue definido como un `TripSchema` válido.

```python
from pylondrina.importing import ImportOptions, import_trips_from_dataframe

options = ImportOptions(
    keep_extra_fields=True,
    strict=False,
    strict_domains=False,
    single_stage=True,
    source_timezone="America/Santiago",
)

field_correspondence = {
    "user_id": "id_persona",
    "origin_longitude": "lon_origen",
    "origin_latitude": "lat_origen",
    "destination_longitude": "lon_destino",
    "destination_latitude": "lat_destino",
    "origin_time_utc": "hora_inicio",
    "destination_time_utc": "hora_fin",
    "purpose": "motivo",
    "mode": "modo",
}

value_correspondence = {
    "purpose": {
        "Trabajo": "work",
        "Estudio": "education",
    },
    "mode": {
        "Auto": "car",
        "Bus": "bus",
    },
}

trips, report = import_trips_from_dataframe(
    df_source,
    schema=trip_schema,
    source_name="EOD",
    options=options,
    field_correspondence=field_correspondence,
    value_correspondence=value_correspondence,
    provenance={"source": {"name": "EOD Santiago"}},
    h3_resolution=10,
)

print(report.summary)
print(trips.metadata["is_validated"])
```

Una salida esperada del reporte incluye la cantidad de filas importadas, los campos mapeados y los valores categóricos transformados. Después de esta operación, el paso recomendado es ejecutar `validate_trips`.

## Operación anterior y siguiente

Dentro de la familia de operaciones sobre trips, OP-01 es la primera operación del pipeline.

| Posición  | Operación                                      |
| --------- | ---------------------------------------------- |
| Anterior  | No aplica dentro del pipeline de trips         |
| Actual    | OP-01 Import trips                             |
| Siguiente | [OP-02 Validate trips](op02_validate_trips.md) |

## Implementación relacionada

Esta operación forma parte de la API pública de Pylondrina v1.1. Su implementación principal puede revisarse en el código fuente del proyecto.

| Recurso            | Enlace                                                                                                                                                  |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Archivo fuente     | [`src/pylondrina/importing.py`](https://github.com/sebastianolmos/pylondrina/blob/main/src/pylondrina/importing.py)                                     |
| Catálogo de issues | [`src/pylondrina/issues/catalog_import_trips.py`](https://github.com/sebastianolmos/pylondrina/blob/main/src/pylondrina/issues/catalog_import_trips.py) |
| Referencia API     | [Ver referencia técnica](../../api/trips.md)                                                                                                            |

