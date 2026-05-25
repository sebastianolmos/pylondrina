# Quickstart

Esta página presenta un recorrido mínimo de uso de **Pylondrina** sobre un conjunto pequeño de viajes origen-destino. El objetivo no es cubrir todas las opciones del módulo, sino mostrar el flujo base: construir un `TripDataset`, validarlo, derivar flows, exportarlos y persistir artefactos.

## Flujo general

El flujo mínimo sobre datos de viajes puede resumirse así:

```text
tabla fuente -> import_trips -> validate_trips -> build_flows -> export/write
```

En este recorrido:

* `import_trips_from_dataframe` construye un `TripDataset` operable bajo Golondrina.
* `validate_trips` certifica formalmente el dataset construido.
* `build_flows` deriva un `FlowDataset` agregado.
* `export_flows` materializa una salida orientada a visualización.
* `write_trips` y `write_flows` persisten artefactos formales `.golondrina`.

## Imports básicos

```python
from pathlib import Path

import pandas as pd

from pylondrina.schema import TripSchema, FieldSpec, DomainSpec
from pylondrina.importing import import_trips_from_dataframe, ImportOptions
from pylondrina.validation import validate_trips, ValidationOptions
from pylondrina.transforms.flows import build_flows, FlowBuildOptions
from pylondrina.export.flows import export_flows, ExportFlowsOptions
from pylondrina.io.trips import write_trips, read_trips, WriteTripsOptions
from pylondrina.io.flows import write_flows, read_flows, WriteFlowsOptions
```

## 1. Crear una tabla fuente mínima

En un caso real, esta tabla podría provenir de una encuesta OD, registros transaccionales, datos sintéticos o una fuente previamente preparada. Para este ejemplo, se define directamente un `DataFrame` pequeño.

```python
source_df = pd.DataFrame(
    {
        "id_usuario": ["u1", "u2", "u3", "u4"],
        "lon_origen": [-70.66, -70.64, -70.61, -70.70],
        "lat_origen": [-33.45, -33.44, -33.48, -33.42],
        "lon_destino": [-70.58, -70.60, -70.67, -70.62],
        "lat_destino": [-33.40, -33.43, -33.46, -33.41],
        "inicio": [
            "2026-01-01T08:00:00",
            "2026-01-01T08:20:00",
            "2026-01-01T09:00:00",
            "2026-01-01T09:30:00",
        ],
        "termino": [
            "2026-01-01T08:35:00",
            "2026-01-01T08:55:00",
            "2026-01-01T09:40:00",
            "2026-01-01T10:05:00",
        ],
        "proposito": ["Trabajo", "Trabajo", "Estudio", "Trabajo"],
        "modo": ["Auto", "Bus", "Metro", "Bus"],
        "peso": [1.0, 1.0, 1.0, 1.0],
    }
)
```

Esta tabla todavía no está en Golondrina. Usa nombres propios de una fuente hipotética, como `lon_origen`, `lat_origen`, `inicio` o `proposito`.

## 2. Definir un schema Golondrina mínimo

El schema indica qué campos serán interpretados como parte del contrato de trips. En este ejemplo se incluyen campos canónicos necesarios para importar, validar y construir flows.

```python
trip_schema = TripSchema(
    version="quickstart-v1",
    fields={
        "movement_id": FieldSpec(
            name="movement_id",
            dtype="string",
            required=True,
            constraints={"nullable": False},
        ),
        "trip_id": FieldSpec(
            name="trip_id",
            dtype="string",
            required=True,
            constraints={"nullable": False},
        ),
        "movement_seq": FieldSpec(
            name="movement_seq",
            dtype="int",
            required=True,
            constraints={"nullable": False},
        ),
        "user_id": FieldSpec(
            name="user_id",
            dtype="string",
            required=True,
            constraints={"nullable": False},
        ),
        "origin_longitude": FieldSpec(
            name="origin_longitude",
            dtype="float",
            required=True,
            constraints={"nullable": False, "range": [-180, 180]},
        ),
        "origin_latitude": FieldSpec(
            name="origin_latitude",
            dtype="float",
            required=True,
            constraints={"nullable": False, "range": [-90, 90]},
        ),
        "destination_longitude": FieldSpec(
            name="destination_longitude",
            dtype="float",
            required=True,
            constraints={"nullable": False, "range": [-180, 180]},
        ),
        "destination_latitude": FieldSpec(
            name="destination_latitude",
            dtype="float",
            required=True,
            constraints={"nullable": False, "range": [-90, 90]},
        ),
        "origin_time_utc": FieldSpec(
            name="origin_time_utc",
            dtype="datetime",
            required=True,
            constraints={"nullable": False},
        ),
        "destination_time_utc": FieldSpec(
            name="destination_time_utc",
            dtype="datetime",
            required=True,
            constraints={"nullable": False},
        ),
        "origin_h3_index": FieldSpec(
            name="origin_h3_index",
            dtype="string",
            required=True,
            constraints={"nullable": False},
        ),
        "destination_h3_index": FieldSpec(
            name="destination_h3_index",
            dtype="string",
            required=True,
            constraints={"nullable": False},
        ),
        "trip_weight": FieldSpec(
            name="trip_weight",
            dtype="float",
            required=False,
            constraints={"nullable": True},
        ),
        "purpose": FieldSpec(
            name="purpose",
            dtype="categorical",
            required=False,
            constraints={"nullable": True},
            domain=DomainSpec(values=["work", "study"], extendable=True),
        ),
        "mode": FieldSpec(
            name="mode",
            dtype="categorical",
            required=False,
            constraints={"nullable": True},
            domain=DomainSpec(values=["car", "bus", "metro"], extendable=True),
        ),
    },
    required=[
        "movement_id",
        "trip_id",
        "movement_seq",
        "user_id",
        "origin_longitude",
        "origin_latitude",
        "destination_longitude",
        "destination_latitude",
        "origin_time_utc",
        "destination_time_utc",
        "origin_h3_index",
        "destination_h3_index",
    ],
)
```

En este schema, `origin_h3_index` y `destination_h3_index` se declaran como parte del resultado esperado. En el import, Pylondrina puede derivarlos desde coordenadas OD cuando existe información espacial suficiente.

## 3. Definir correspondencias de campos y valores

Las correspondencias de campos indican cómo se traducen las columnas de la fuente hacia nombres canónicos Golondrina.

```python
field_correspondence = {
    "user_id": "id_usuario",
    "origin_longitude": "lon_origen",
    "origin_latitude": "lat_origen",
    "destination_longitude": "lon_destino",
    "destination_latitude": "lat_destino",
    "origin_time_utc": "inicio",
    "destination_time_utc": "termino",
    "purpose": "proposito",
    "mode": "modo",
    "trip_weight": "peso",
}
```

Las correspondencias de valores normalizan categorías propias de la fuente hacia valores canónicos.

```python
value_correspondence = {
    "purpose": {
        "Trabajo": "work",
        "Estudio": "study",
    },
    "mode": {
        "Auto": "car",
        "Bus": "bus",
        "Metro": "metro",
    },
}
```

## 4. Importar trips

```python
trips, import_report = import_trips_from_dataframe(
    source_df,
    trip_schema,
    source_name="quickstart_synthetic",
    options=ImportOptions(
        keep_extra_fields=True,
        single_stage=True,
        source_timezone="America/Santiago",
    ),
    field_correspondence=field_correspondence,
    value_correspondence=value_correspondence,
    provenance={
        "description": "Dataset sintético mínimo usado en quickstart",
        "city": "Santiago",
    },
    h3_resolution=8,
)
```

Después del import, el dataset queda en una representación operable por Pylondrina, pero todavía no debe interpretarse como formalmente validado.

```python
print(type(trips))
print(trips.data.head())
print(import_report.summary)
print(trips.metadata["is_validated"])
```

Resultado esperado a nivel conceptual:

```text
TripDataset
is_validated = False
```

La distinción es importante: importar construye el dataset; validar certifica conformidad.

## 5. Validar trips

```python
validation_report = validate_trips(
    trips,
    options=ValidationOptions(
        validate_domains="full",
        validate_temporal_consistency=True,
    ),
)
```

Luego se puede inspeccionar el resumen:

```python
print(validation_report.ok)
print(validation_report.summary)
print(trips.metadata["is_validated"])
```

Si el dataset cumple el contrato definido por el schema, `metadata["is_validated"]` queda en `True`.

## 6. Construir flows

Una vez validado el `TripDataset`, se puede construir un `FlowDataset`.

```python
flows, flow_report = build_flows(
    trips,
    options=FlowBuildOptions(
        h3_resolution=8,
        group_by=["purpose", "mode"],
        keep_flow_to_trips=True,
        require_validated=True,
    ),
)
```

Inspección básica:

```python
print(type(flows))
print(flows.flows.head())
print(flow_report.summary)
```

El resultado interno usa el contrato canónico de flows, con campos como:

```text
flow_id
origin_h3_index
destination_h3_index
flow_count
flow_value
```

Si existe `trip_weight`, `flow_value` representa la suma ponderada del flujo. Si no existe, cae al conteo de viajes.

## 7. Exportar flows para visualización

Los flows pueden exportarse a un layout externo orientado a visualización.

```python
export_result, export_report = export_flows(
    flows,
    output_root=Path("data/flows/quickstart"),
    options=ExportFlowsOptions(
        mode="overwrite",
        folder_name="quickstart_flowmap",
    ),
)
```

Inspección:

```python
print(export_result.export_dir)
print(export_result.artifacts)
print(export_report.summary)
```

Esta exportación produce artefactos orientados a visualización. No debe confundirse con la persistencia formal interna de `FlowDataset`.

## 8. Persistir trips y flows como bundles `.golondrina`

Además de exportar, Pylondrina puede persistir datasets internos como artefactos formales `.golondrina`.

### Persistir trips

```python
trips_write_report = write_trips(
    trips,
    Path("data/quickstart/trips_example"),
    options=WriteTripsOptions(
        mode="overwrite",
        storage_format="feather",
    ),
)
```

Leer nuevamente:

```python
trips_reloaded, trips_read_report = read_trips(
    Path("data/quickstart/trips_example.golondrina")
)

print(trips_reloaded.data.head())
print(trips_read_report.summary)
```

### Persistir flows

```python
flows_write_report = write_flows(
    flows,
    Path("data/quickstart/flows_example"),
    options=WriteFlowsOptions(
        mode="overwrite",
        storage_format="feather",
    ),
)
```

Leer nuevamente:

```python
flows_reloaded, flows_read_report = read_flows(
    Path("data/quickstart/flows_example.golondrina")
)

print(flows_reloaded.flows.head())
print(flows_read_report.summary)
```

La persistencia formal usa un bundle `.golondrina` con sidecar JSON. Ese bundle permite reconstruir datasets dentro de Pylondrina, pero su existencia por sí sola no debe interpretarse como certificación semántica completa del contenido.

## 9. Actualizar el visualizador

Si la exportación generó artefactos dentro de `data/flows/`, el registro del visualizador puede actualizarse con:

```bash
python scripts/generate_viewer_registry.py
```

Luego se puede levantar el visualizador local:

```bash
python -m http.server 8000
```

Y abrir:

```text
http://localhost:8000/viewer/
```

## Qué se mostró en este recorrido

Este quickstart recorrió el camino mínimo desde una tabla fuente hasta artefactos derivados:

1. Se definió una fuente tabular pequeña.
2. Se definió un schema Golondrina de trips.
3. Se aplicaron correspondencias de campos y valores.
4. Se construyó un `TripDataset`.
5. Se validó formalmente el dataset.
6. Se construyó un `FlowDataset`.
7. Se exportaron flows para visualización.
8. Se persistieron trips y flows como bundles `.golondrina`.

Este flujo resume la lógica base del módulo: transformar datos heterogéneos en una representación común, operar sobre esa representación y dejar evidencia reproducible del proceso mediante reportes, metadata, eventos y artefactos persistibles.
