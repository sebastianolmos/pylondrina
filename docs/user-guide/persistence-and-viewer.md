# Persistencia, exportación y visualización

## Propósito de la guía

Esta guía explica cómo usar las salidas persistibles y visuales de Pylondrina dentro de un pipeline. La idea central es distinguir tres acciones que suelen confundirse:

* **persistir** un dataset interno para reconstruirlo después;
* **exportar** flows hacia un layout externo de visualización;
* **visualizar** flows en el viewer local del proyecto.

Estas acciones se relacionan, pero no cumplen la misma función. Persistir no es exportar, exportar no es validar, y visualizar no forma parte del core operacional de Pylondrina.

## Tres salidas distintas

| Salida                          | Para qué sirve                                                        | Artefactos típicos                                                              | Operaciones o componente                                 |
| ------------------------------- | --------------------------------------------------------------------- | ------------------------------------------------------------------------------- | -------------------------------------------------------- |
| Bundle `.golondrina`            | Reconstrucción formal interna dentro de Pylondrina                    | `trips.metadata.json`, `flows.metadata.json`, archivo tabular Parquet o Feather | `write_trips`, `read_trips`, `write_flows`, `read_flows` |
| Layout externo de visualización | Interoperabilidad o visualización de flows fuera del contrato interno | `flows.csv`, `locations.csv`, `metadata.json`                                   | `export_flows`                                           |
| Viewer local                    | Inspección visual auxiliar de flows                                   | Usa datasets registrados en `viewer_registry.json`                              | Viewer web del repositorio                               |

La diferencia práctica es:

```text
write/read -> guardar y reconstruir datasets internos
export     -> generar archivos para visualización externa
viewer     -> inspeccionar flows visualmente
```

## Persistencia formal de trips

La persistencia formal de trips se realiza con `write_trips` y `read_trips`.

`write_trips` materializa un `TripDataset` como bundle `.golondrina`. El artefacto incluye un archivo tabular y un sidecar obligatorio:

```text
trips_artifact.golondrina/
├── trips.parquet
└── trips.metadata.json
```

o bien:

```text
trips_artifact.golondrina/
├── trips.feather
└── trips.metadata.json
```

El archivo tabular guarda los datos. El sidecar `trips.metadata.json` conserva información necesaria para reconstrucción formal, incluyendo schema, schema efectivo, metadata, provenance, identidad lógica y configuración del backend físico.

`write_trips` no transforma el dataset, no limpia filas y no certifica conformidad. Por defecto, exige que el dataset esté validado antes de escribir:

```python
trips.metadata["is_validated"] == True
```

`read_trips` reconstruye un `TripDataset` desde el bundle. La lectura usa el sidecar como fuente de verdad del artefacto.

Leer no equivale a validar. Después de `read_trips`, el dataset reconstruido queda marcado como no validado:

```python
trips_read.metadata["is_validated"] == False
```

Si se necesita continuar el pipeline con una precondición de conformidad, debe ejecutarse `validate_trips` después de leer.

## Persistencia formal de flows

La persistencia formal de flows se realiza con `write_flows` y `read_flows`.

`write_flows` materializa un `FlowDataset` como bundle `.golondrina`. El artefacto incluye un sidecar obligatorio y un archivo tabular principal:

```text
flows_artifact.golondrina/
├── flows.feather
└── flows.metadata.json
```

o bien:

```text
flows_artifact.golondrina/
├── flows.parquet
└── flows.metadata.json
```

Si el `FlowDataset` conserva la tabla auxiliar `flow_to_trips` y la opción de escritura lo permite, el bundle puede incluir también:

```text
flow_to_trips.feather
```

o:

```text
flow_to_trips.parquet
```

El sidecar `flows.metadata.json` conserva bloques como:

* identidad del dataset y del artefacto;
* backend físico usado;
* archivos incluidos;
* `aggregation_spec`;
* metadata;
* provenance;
* información de tablas persistidas.

`read_flows` reconstruye un `FlowDataset` desde ese bundle. La operación no reconstruye el pipeline original ni recupera referencias vivas como `source_trips`. Solo reconstruye lo que fue persistido formalmente.

Igual que en trips, leer flows no equivale a validar ni a recomputar la agregación.

## Exportar flows para visualización

`export_flows` cumple una función distinta a `write_flows`.

`write_flows` persiste un `FlowDataset` interno para reconstrucción formal. En cambio, `export_flows` transforma un `FlowDataset` ya construido hacia un layout externo orientado a visualización.

En v1.1, el formato externo principal es:

```python
"flowmap_blue"
```

La exportación genera una carpeta con archivos como:

```text
flow_exports/
└── baseline_flows/
    ├── flows.csv
    ├── locations.csv
    └── metadata.json
```

La correspondencia principal del layout externo es:

| Campo externo | Campo interno          |
| ------------- | ---------------------- |
| `origin`      | `origin_h3_index`      |
| `dest`        | `destination_h3_index` |
| `count`       | `flow_value`           |

Esto significa que la magnitud visual del flujo usa `flow_value`, no necesariamente `flow_count`. Si el `FlowDataset` fue construido desde trips con `trip_weight`, `flow_value` representa la suma ponderada.

El sidecar `metadata.json` de exportación documenta el layout generado, pero no reemplaza el sidecar formal `flows.metadata.json`. Sirve para interpretar el artefacto externo, no para reconstruir formalmente un `FlowDataset` con `read_flows`.

## Usar el viewer como inspección auxiliar

El viewer local es un componente auxiliar del repositorio para inspeccionar flows visualmente. No reemplaza el core de Pylondrina ni las operaciones de construcción, exportación o persistencia.

El viewer puede trabajar con datasets registrados en:

```text
/data/flows/viewer_registry.json
```

y está pensado para cargar datasets disponibles bajo la estructura de datos del repositorio. El selector del viewer no explora libremente el disco desde el navegador; consume el registry generado previamente.

Según el código actual del viewer, los formatos reconocidos en el selector incluyen:

| Formato en registry  | Uso                                              |
| -------------------- | ------------------------------------------------ |
| `flowmap_layout`     | Layout externo con `flows.csv` y `locations.csv` |
| `golondrina_parquet` | Flows Golondrina en Parquet                      |
| `golondrina_feather` | Flows Golondrina en Feather                      |

Para el layout exportado, el viewer consume `flows.csv` y `locations.csv`. Para flows Golondrina, espera las columnas internas mínimas:

```text
flow_id
origin_h3_index
destination_h3_index
flow_count
flow_value
```

El viewer convierte esos flows internos a la estructura requerida por la capa visual, usando `origin_h3_index` y `destination_h3_index` como nodos, y `flow_value` como magnitud.

Los detalles de ejecución, controles, selector, registry y formatos soportados se documentan en [Uso del viewer](../viewer/usage.md).

## Patrón mínimo de código

El siguiente ejemplo muestra un flujo compacto donde se persisten trips, se reconstruyen, se construyen flows, se persisten flows y además se exportan para visualización.

```python
from pylondrina.validation import validate_trips
from pylondrina.io.trips import (
    WriteTripsOptions,
    ReadTripsOptions,
    write_trips,
    read_trips,
)
from pylondrina.transforms.flows import (
    FlowBuildOptions,
    build_flows,
)
from pylondrina.io.flows import (
    WriteFlowsOptions,
    ReadFlowsOptions,
    write_flows,
    read_flows,
)
from pylondrina.export.flows import (
    ExportFlowsOptions,
    export_flows,
)

# 1. Persistir un TripDataset ya preparado y validado.
write_trips_report = write_trips(
    trips,
    "artifacts/trips_work",
    options=WriteTripsOptions(
        mode="overwrite",
        require_validated=True,
        storage_format="feather",
        feather_compression="lz4",
        normalize_artifact_dir=True,
    ),
)

# 2. Reconstruir formalmente el TripDataset desde el bundle.
trips_read, read_trips_report = read_trips(
    "artifacts/trips_work",
    options=ReadTripsOptions(
        schema=None,
        strict=False,
        keep_metadata=True,
    ),
)

# 3. Revalidar si se necesita continuar con operaciones que exigen conformidad.
validation_report = validate_trips(trips_read)

# 4. Construir flows desde trips validados.
flows, build_report = build_flows(
    trips_read,
    options=FlowBuildOptions(
        h3_resolution=8,
        group_by=None,
        min_trips_per_flow=1,
        keep_flow_to_trips=True,
        require_validated=True,
    ),
)

# 5. Persistir formalmente el FlowDataset.
write_flows_report = write_flows(
    flows,
    "artifacts/flows_work",
    options=WriteFlowsOptions(
        mode="overwrite",
        storage_format="feather",
        feather_compression="lz4",
        normalize_artifact_dir=True,
        write_flow_to_trips=True,
    ),
)

# 6. Reconstruir flows desde persistencia formal si se requiere.
flows_read, read_flows_report = read_flows(
    "artifacts/flows_work",
    options=ReadFlowsOptions(
        strict=False,
        keep_metadata=True,
        read_flow_to_trips=True,
    ),
)

# 7. Exportar flows a layout externo de visualización.
export_result, export_report = export_flows(
    flows_read,
    output_root="outputs/flow_exports",
    options=ExportFlowsOptions(
        format="flowmap_blue",
        mode="overwrite",
        folder_name="flows_work",
        extra_flow_fields=None,
    ),
)
```

Después de ejecutar, conviene revisar los summaries:

```python
print(write_trips_report.summary)
print(read_trips_report.summary)
print(validation_report.summary)
print(build_report.summary)
print(write_flows_report.summary)
print(read_flows_report.summary)
print(export_report.summary)
```

## Buenas prácticas

### No editar sidecars manualmente

Los sidecars son parte del contrato observable del artefacto. Editarlos manualmente puede romper la coherencia entre metadata, archivos tabulares, backend físico e identidad del artefacto.

Si se necesita cambiar el dataset, se recomienda reconstruirlo mediante operaciones de Pylondrina y volver a escribirlo.

### No confundir lectura con validación

`read_trips` y `read_flows` reconstruyen objetos desde disco. No ejecutan una validación formal de conformidad.

En trips, después de leer, se recomienda revalidar si el dataset seguirá en un pipeline que exige `metadata["is_validated"] == True`.

### Conservar bundles completos

Un archivo `trips.parquet`, `trips.feather`, `flows.parquet` o `flows.feather` aislado no equivale a un bundle formal completo.

Para reconstrucción interna se debe conservar el directorio `.golondrina` completo, incluyendo su sidecar:

```text
*.golondrina/
├── archivo tabular
└── *.metadata.json
```

### Exportar cuando el objetivo sea visualización

Si el objetivo es producir archivos para una herramienta externa o para inspección visual, se debe usar `export_flows`.

La exportación produce `flows.csv`, `locations.csv` y `metadata.json`, pero no reemplaza la persistencia formal de `FlowDataset`.

### Persistir cuando el objetivo sea reconstrucción interna

Si el objetivo es retomar el trabajo en otra sesión, guardar un resultado intermedio o conservar un snapshot reproducible del pipeline, se debe usar `write_trips` o `write_flows`.

### Distinguir sidecars internos y sidecars de exportación

| Sidecar               | Se genera en   | Propósito                                  |
| --------------------- | -------------- | ------------------------------------------ |
| `trips.metadata.json` | `write_trips`  | Reconstrucción formal de `TripDataset`     |
| `flows.metadata.json` | `write_flows`  | Reconstrucción formal de `FlowDataset`     |
| `metadata.json`       | `export_flows` | Documentación del layout externo exportado |

### Usar el viewer como apoyo de inspección

El viewer permite revisar visualmente flows y comparar resultados exportados o persistidos. Sin embargo, no debe tratarse como fuente de verdad del pipeline. La construcción, validación, persistencia y exportación siguen ocurriendo en Pylondrina.

## Enlaces relacionados

* [Bundles `.golondrina`](../golondrina/bundles.md)
* [OP-06 Write trips](../operations/trips/op06_write_trips.md)
* [OP-07 Read trips](../operations/trips/op07_read_trips.md)
* [OP-09 Export flows](../operations/flows/op09_export_flows.md)
* [OP-10 Write flows](../operations/flows/op10_write_flows.md)
* [OP-11 Read flows](../operations/flows/op11_read_flows.md)
* [Uso del viewer](../viewer/usage.md)
