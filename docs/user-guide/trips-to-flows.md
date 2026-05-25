# Construir flows desde trips

## Propósito de la guía

Esta guía muestra cómo construir flujos origen-destino a partir de una fuente tabular de viajes. El objetivo es recorrer el flujo completo de uso: importar trips, validar el dataset, aplicar correcciones o recortes cuando corresponda, construir un `FlowDataset` y exportar los flows para visualización o análisis externo.

La guía no reemplaza los manuales individuales de operaciones. Cada operación tiene su propia página en `docs/operations/`. Aquí se muestra cómo encadenarlas en un pipeline práctico.

## Flujo general

```text
fuente tabular de viajes
  -> import_trips_from_dataframe
  -> validate_trips
  -> fix_trips_correspondence, clean_trips o filter_trips si corresponde
  -> validate_trips si el dataset fue corregido o preparado para certificación
  -> build_flows
  -> export_flows
```

En términos prácticos:

* `import_trips_from_dataframe` construye un `TripDataset`, pero no certifica conformidad.
* `validate_trips` revisa si el dataset cumple el contrato esperado.
* `fix_trips_correspondence` corrige problemas semánticos de mappings o valores.
* `clean_trips` elimina registros problemáticos.
* `filter_trips` restringe el universo analítico.
* `build_flows` agrega los trips en flows OD.
* `export_flows` prepara artefactos externos de visualización.

## 1. Importar trips

La importación es la puerta de entrada desde una tabla externa hacia un `TripDataset`. En esta etapa se alinean columnas, se aplican correspondencias de campos, se pueden normalizar valores categóricos y se materializan campos canónicos necesarios para operar con Pylondrina.

Un caso típico parte desde una tabla con nombres propios de la fuente, por ejemplo columnas de una encuesta o de un sistema transaccional. En vez de renombrar todo manualmente antes del pipeline, se declara una correspondencia:

```python
field_correspondence = {
    "user_id": "persona_id",
    "origin_time_utc": "hora_inicio",
    "destination_time_utc": "hora_fin",
    "origin_latitude": "lat_origen",
    "origin_longitude": "lon_origen",
    "destination_latitude": "lat_destino",
    "destination_longitude": "lon_destino",
    "trip_weight": "factor_expansion",
    "purpose": "proposito",
    "mode": "modo",
}
```

Si la fuente usa categorías propias, también puede declararse una correspondencia de valores:

```python
value_correspondence = {
    "purpose": {
        "Trabajo": "work",
        "Estudio": "education",
    },
    "mode": {
        "Auto": "car",
        "Bus": "bus",
        "Metro": "metro",
    },
}
```

La importación produce un `TripDataset` operable y un `ImportReport`. El dataset queda con:

```python
trips.metadata["is_validated"] == False
```

Esto es intencional. Importar construye una representación utilizable, pero no certifica que el dataset ya cumple completamente el contrato.

## 2. Validar el dataset importado

Después del import, se recomienda ejecutar validación formal. Esta etapa permite responder si el `TripDataset` cumple el contrato esperado para continuar el análisis.

```python
validation_report = validate_trips(
    trips,
    options=ValidationOptions(
        validate_domains="full",
        validate_temporal_consistency=True,
    ),
)
```

La validación no corrige datos ni elimina filas. Su rol es producir evidencia:

```python
validation_report.ok
validation_report.summary
validation_report.issues
trips.metadata["is_validated"]
```

Un resultado `ok=False` no significa necesariamente que el pipeline terminó. Significa que el dataset importado todavía requiere revisión. En una fuente real, es común que la primera validación detecte problemas como:

* coordenadas OD incompletas;
* valores fuera de dominio;
* problemas de tipo o formato;
* inconsistencias temporales;
* campos requeridos ausentes.

La decisión posterior depende del tipo de problema.

## 3. Corregir, limpiar o filtrar si corresponde

Después de validar, el usuario debe distinguir tres acciones distintas.

### Corregir correspondencias semánticas

Se usa `fix_trips_correspondence` cuando el problema está en mappings, nombres semánticos o valores categóricos. Por ejemplo, si una categoría de propósito quedó mal normalizada o si se requiere ajustar dominios efectivos sin repetir todo el import.

```python
trips_fixed, fix_report = fix_trips_correspondence(
    trips,
    value_corrections={
        "purpose": {
            "Otra actividad": "other",
        }
    },
)
```

Si la corrección cambia la semántica del dataset, corresponde volver a validar.

```python
validation_report = validate_trips(trips_fixed)
```

### Limpiar registros problemáticos

Se usa `clean_trips` cuando existen registros que no son utilizables para el análisis y deben eliminarse bajo reglas explícitas. Por ejemplo, filas sin coordenadas necesarias, H3 inválidos, tiempos incoherentes o campos analíticos críticos nulos.

```python
trips_clean, clean_report = clean_trips(
    trips,
    options=CleanOptions(
        drop_rows_with_nulls_in_required_fields=True,
        drop_rows_with_invalid_latlon=True,
        drop_rows_with_invalid_h3=True,
    ),
)
```

`clean_trips` es una operación drop-only. No corrige valores ni reinterpreta el contrato; solo elimina filas según reglas declaradas. En rutas retornables, preserva el estado de validación previo. Aun así, en pipelines analíticos suele ser recomendable revalidar después de una limpieza importante.

### Filtrar el universo analítico

Se usa `filter_trips` cuando el dataset ya está preparado y se necesita restringir el universo de estudio. Por ejemplo, viajes de trabajo, día laboral, peso positivo y ciertos grupos de análisis.

```python
trips_work, filter_report = filter_trips(
    trips_clean,
    options=FilterOptions(
        where={
            "purpose": "work",
            "day_type": "weekday",
            "trip_weight": {"gt": 0},
            "user_gender": ["Hombre", "Mujer"],
        },
    ),
)
```

A diferencia de `clean_trips`, el filtrado no necesariamente elimina datos “malos”. Muchas veces solo define el recorte metodológico del análisis.

## 4. Construir flows

Una vez que se tiene un `TripDataset` validado y recortado al universo de interés, se pueden construir flows OD.

```python
flows, flow_report = build_flows(
    trips_work,
    options=FlowBuildOptions(
        h3_resolution=8,
        group_by=["user_gender"],
        min_trips_per_flow=3,
        keep_flow_to_trips=True,
        require_validated=True,
    ),
)
```

`build_flows` agrega trips por par origen-destino H3. El resultado es un `FlowDataset` con una tabla interna de flows y metadata de agregación.

La tabla de flows contiene, como mínimo:

```text
flow_id
origin_h3_index
destination_h3_index
flow_count
flow_value
```

La diferencia entre `flow_count` y `flow_value` es importante:

* `flow_count` representa el número de movements agregados.
* `flow_value` usa `trip_weight` si existe; si no existe, cae al conteo de registros.

Esto permite trabajar tanto con conteos crudos como con magnitudes ponderadas, por ejemplo cuando se usan factores de expansión de una encuesta.

`group_by` permite construir flows segmentados. Por ejemplo, si se usa:

```python
group_by=["user_gender"]
```

los flows se agregan separadamente por género. Esto permite comparar estructuras OD entre grupos sin reconstruir el pipeline desde cero.

Si `keep_flow_to_trips=True`, el `FlowDataset` conserva una tabla auxiliar que relaciona cada `flow_id` con los `movement_id` que lo sustentan. Esto es útil para inspección posterior con `get_trips_from_flows`.

## 5. Exportar flows

`export_flows` prepara un layout externo para visualización. En v1.1, el formato principal es `flowmap_blue`.

```python
export_result, export_report = export_flows(
    flows,
    output_root="outputs/flowmap_work_gender",
    options=ExportFlowsOptions(
        format="flowmap_blue",
        mode="overwrite",
    ),
)
```

La exportación no reconstruye ni reinterpreta los flows. Solo toma el `FlowDataset` ya construido y lo materializa en archivos externos, por ejemplo:

```text
flows.csv
locations.csv
metadata.json
```

La diferencia entre construir y exportar es central:

```text
build_flows  -> crea el FlowDataset interno
export_flows -> crea un layout externo de visualización
```

Por eso, si se detecta un problema de agregación, debe revisarse `build_flows`. Si el problema está en archivos de salida o layout, debe revisarse `export_flows`.

## 6. Persistir resultados, si se necesita reproducibilidad

Exportar flows no equivale a persistir formalmente un dataset interno. La exportación genera artefactos para consumo externo, mientras que la persistencia formal permite reconstruir objetos de Pylondrina.

Si se necesita conservar el dataset de trips o flows para reusarlo después, se pueden usar operaciones de escritura:

```python
write_trips(trips_work, "artifacts/trips_work.golondrina")
write_flows(flows, "artifacts/flows_work_gender.golondrina")
```

La lectura posterior reconstruye datasets internos, pero no certifica automáticamente conformidad. Si se requiere estado validado, debe ejecutarse validación después de leer.

La guía detallada de persistencia y visualización se desarrolla en la página correspondiente de `user-guide`.

## Patrón mínimo de código

El siguiente ejemplo muestra un pipeline compacto desde una tabla fuente hasta flows exportados.

```python
from pylondrina.importing import ImportOptions, import_trips_from_dataframe
from pylondrina.validation import ValidationOptions, validate_trips
from pylondrina.transforms.cleaning import CleanOptions, clean_trips
from pylondrina.transforms.filtering import FilterOptions, filter_trips
from pylondrina.transforms.flows import FlowBuildOptions, build_flows
from pylondrina.export.flows import ExportFlowsOptions, export_flows

# 1. Importar fuente tabular como TripDataset.
trips, import_report = import_trips_from_dataframe(
    source_df,
    trip_schema,
    source_name="eod_work_trips",
    options=ImportOptions(
        single_stage=True,
        source_timezone="America/Santiago",
        keep_extra_fields=True,
    ),
    field_correspondence={
        "user_id": "persona_id",
        "origin_time_utc": "inicio",
        "destination_time_utc": "fin",
        "origin_latitude": "lat_origen",
        "origin_longitude": "lon_origen",
        "destination_latitude": "lat_destino",
        "destination_longitude": "lon_destino",
        "trip_weight": "peso_laboral",
        "purpose": "proposito",
        "mode": "modo",
        "day_type": "tipo_dia",
        "user_gender": "sexo",
    },
    value_correspondence={
        "purpose": {"Trabajo": "work"},
        "day_type": {"Laboral": "weekday"},
    },
    h3_resolution=10,
)

# 2. Validar el dataset importado.
validation_report = validate_trips(
    trips,
    options=ValidationOptions(
        validate_domains="full",
        validate_temporal_consistency=True,
    ),
)

# 3. Limpiar si la validación muestra problemas de completitud relevantes.
trips_clean, clean_report = clean_trips(
    trips,
    options=CleanOptions(
        drop_rows_with_nulls_in_required_fields=True,
        drop_rows_with_invalid_latlon=True,
        drop_rows_with_invalid_h3=True,
    ),
)

# 4. Revalidar después de la limpieza.
validation_clean_report = validate_trips(
    trips_clean,
    options=ValidationOptions(
        validate_domains="full",
        validate_temporal_consistency=True,
    ),
)

# 5. Definir el universo analítico.
trips_work, filter_report = filter_trips(
    trips_clean,
    options=FilterOptions(
        where={
            "purpose": "work",
            "day_type": "weekday",
            "trip_weight": {"gt": 0},
            "user_gender": ["Hombre", "Mujer"],
        },
    ),
)

# 6. Construir flows OD segmentados.
flows, flow_report = build_flows(
    trips_work,
    options=FlowBuildOptions(
        h3_resolution=8,
        group_by=["user_gender"],
        min_trips_per_flow=3,
        keep_flow_to_trips=True,
        require_validated=True,
    ),
)

# 7. Exportar a layout externo de visualización.
export_result, export_report = export_flows(
    flows,
    output_root="outputs/flows_work_gender",
    options=ExportFlowsOptions(
        format="flowmap_blue",
        mode="overwrite",
    ),
)
```

En un pipeline real, cada paso debe revisarse mediante su reporte:

```python
print(import_report.summary)
print(validation_report.summary)
print(clean_report.summary)
print(filter_report.summary)
print(flow_report.summary)
print(export_report.summary)
```

Si una validación falla, no conviene saltar directamente a `build_flows`. Primero debe revisarse si el problema requiere corrección semántica, limpieza o un ajuste del universo analítico.

## Decisiones prácticas

### Cuándo corregir

Se corrige con `fix_trips_correspondence` cuando el problema está en cómo se interpretaron campos o valores. Por ejemplo, una categoría fuente quedó mapeada a un valor incorrecto o un dominio efectivo necesita actualizarse.

### Cuándo limpiar

Se limpia con `clean_trips` cuando existen registros problemáticos que no deben entrar al análisis, por ejemplo coordenadas inválidas, H3 no utilizables, nulos críticos o inconsistencias temporales.

### Cuándo filtrar

Se filtra con `filter_trips` cuando se quiere definir el universo analítico. Por ejemplo: viajes de trabajo, una ventana horaria, una zona espacial o un grupo de usuarios.

### Cuándo validar

Se valida después de importar y después de cambios que puedan afectar la conformidad. Las operaciones de importación y lectura no deben interpretarse como certificación automática.

### Cuándo construir flows

Se construyen flows cuando el `TripDataset` ya representa el universo de análisis y contiene H3 OD utilizables. Si se usará `require_validated=True`, el dataset debe estar validado antes de llamar `build_flows`.

### Cuándo exportar

Se exporta cuando el objetivo es usar los flows en una herramienta externa o en el visualizador. Exportar no reemplaza la persistencia formal.

## Enlaces relacionados

* [OP-01 Import trips](../operations/trips/op01_import_trips.md)
* [OP-02 Validate trips](../operations/trips/op02_validate_trips.md)
* [OP-03 Fix trips correspondence](../operations/trips/op03_fix_trips_correspondence.md)
* [OP-04 Clean trips](../operations/trips/op04_clean_trips.md)
* [OP-05 Filter trips](../operations/trips/op05_filter_trips.md)
* [OP-08 Build flows](../operations/flows/op08_build_flows.md)
* [OP-09 Export flows](../operations/flows/op09_export_flows.md)
* [Trips en Golondrina](../golondrina/trips.md)
* [Flows en Golondrina](../golondrina/flows.md)
