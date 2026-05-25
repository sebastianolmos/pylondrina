# OP-04 Clean trips

`clean_trips` es la operación de limpieza drop-only para datasets de trips. Se implementó para eliminar filas problemáticas o no deseadas según reglas explícitas, sin corregir valores, sin recodificar categorías, sin validar formalmente y sin modificar el contrato semántico del dataset.

La operación permite preparar un `TripDataset` para análisis posteriores mediante una poda controlada y reproducible. Su salida es un nuevo `TripDataset`, acompañado por un `OperationReport` y un evento `clean_trips` en la metadata del resultado.

## Para qué sirve

Esta operación permite retirar registros que no son utilizables bajo ciertos criterios de limpieza. En v1.1, las reglas soportadas cubren:

- nulos en campos requeridos del schema;
- nulos en campos indicados explícitamente;
- coordenadas OD inválidas;
- índices H3 inválidos o faltantes;
- viajes con `origin_time_utc` posterior a `destination_time_utc`;
- duplicados;
- valores categóricos no deseados.

A diferencia de `validate_trips`, esta operación no certifica conformidad. A diferencia de `fix_trips_correspondence`, no corrige mappings ni recodifica dominios. Su función es eliminar filas bajo reglas observables.

## Cuándo usarla

Esta operación se usa después de importar y, normalmente, después de una primera validación que revela problemas de completitud o calidad. También puede usarse antes de filtrar, construir flows o exportar resultados, cuando se necesita retirar registros que no deben formar parte del universo analítico.

Un flujo típico es:

```text
import_trips_from_dataframe -> validate_trips -> clean_trips -> validate_trips -> filter_trips -> build_flows
```

En un caso de estudio con datos EOD, por ejemplo, se usó para retirar filas con faltantes en campos relevantes, coordenadas no utilizables y campos analíticos incompletos antes de construir el baseline de análisis.

## Qué recibe y qué retorna

La operación recibe:

* `trips`: un `TripDataset` ya construido;
* `options`: una instancia opcional de `CleanOptions`.

Retorna:

```python
TripDataset, OperationReport
```

El `TripDataset` retornado es un nuevo objeto. El input no se muta. El resultado conserva el mismo `schema`, `schema_version`, `provenance`, `field_correspondence`, `value_correspondence`, `schema_effective`, `dataset_id` y `metadata["domains_effective"]`. No crea `artifact_id` ni escribe en disco.

El único cambio tabular esperado es el subconjunto de filas conservadas en `data`.

## Qué evidencia deja

OP-04 retorna un `OperationReport` con:

* `ok`;
* `issues`;
* `summary`;
* `parameters`.

El `summary` se mantiene pequeño y estable. Sus claves principales son:

* `rows_in`;
* `rows_out`;
* `dropped_total`;
* `dropped_by_rule`.

El bloque `dropped_by_rule` distingue las reglas aplicadas:

```python
{
    "nulls_required": ...,
    "nulls_fields": ...,
    "invalid_latlon": ...,
    "invalid_h3": ...,
    "origin_after_destination": ...,
    "duplicates": ...,
    "categorical_values": ...,
}
```

Estos conteos son incrementales: cada regla se evalúa sobre el estado vigente después de las reglas anteriores, no como conteos independientes sobre el dataframe original.

La operación también registra un evento `clean_trips` en `metadata["events"]` del dataset resultante. El evento incluye:

* `op`;
* `ts_utc`;
* `parameters`;
* `summary`;
* `issues_summary`.

El bloque `parameters` conserva la configuración efectiva, incluyendo `duplicates_subset_effective` cuando corresponde.

## Consideraciones importantes

`clean_trips` preserva `metadata["is_validated"]` en toda ruta retornable. Esto es una decisión de diseño de las operaciones drop-only: eliminar filas no cambia el significado de las filas sobrevivientes ni reinterpreta el contrato del dataset.

La regla `drop_rows_with_invalid_latlon` acepta OD parcial, pero rechaza extremos rotos. Es decir, puede conservar una fila con solo origen completo o solo destino completo, pero descarta casos donde un extremo tiene latitud sin longitud, longitud sin latitud, coordenadas fuera de rango o ambos extremos ausentes.

La regla `drop_rows_with_invalid_h3` exige que existan y sean válidos ambos índices H3: `origin_h3_index` y `destination_h3_index`.

La regla `drop_rows_with_origin_after_destination` solo se evalúa cuando el dataset tiene temporalidad Tier 1 y dispone de `origin_time_utc` y `destination_time_utc`. Si el dataset está en Tier 2 o Tier 3, la regla se omite con evidencia.

Si `drop_duplicates=True` y `duplicates_subset=None`, la operación usa como subset efectivo la intersección entre `trips.schema.required` y las columnas presentes en `trips.data`. Si se entrega un subset explícito con columnas inexistentes, la operación aborta por configuración inválida.

En `drop_rows_by_categorical_values`, el valor `None` dentro de la lista de valores prohibidos se interpreta como instrucción para eliminar también nulos/NaN de ese campo.

## Ejemplo mínimo

El siguiente ejemplo elimina filas con nulos en campos requeridos, coordenadas inválidas, H3 inválidos, duplicados y valores categóricos no deseados.

```python
from pylondrina.transforms.cleaning import CleanOptions, clean_trips

options = CleanOptions(
    drop_rows_with_nulls_in_required_fields=True,
    drop_rows_with_nulls_in_fields=["purpose", "trip_weight"],
    drop_rows_with_invalid_latlon=True,
    drop_rows_with_invalid_h3=True,
    drop_rows_with_origin_after_destination=True,
    drop_duplicates=True,
    duplicates_subset=[
        "user_id",
        "origin_time_utc",
        "origin_h3_index",
        "destination_h3_index",
    ],
    drop_rows_by_categorical_values={
        "mode": ["unknown"],
        "purpose": ["no_destination", None],
    },
)

cleaned_trips, report = clean_trips(
    trips,
    options=options,
)

print(report.summary)
print(cleaned_trips.metadata["is_validated"])
```

Después de limpiar, es recomendable ejecutar nuevamente `validate_trips` cuando el objetivo sea certificar formalmente el dataset limpio antes de construir flows o persistirlo.

## Operación anterior y siguiente

Dentro de la familia de operaciones sobre trips, OP-04 se ubica como una transformación reproducible posterior a importación, validación inicial o corrección semántica.

| Posición              | Operación                                                                                                           |
| --------------------- | ------------------------------------------------------------------------------------------------------------------- |
| Anterior recomendada  | [OP-02 Validate trips](op02_validate_trips.md) u [OP-03 Fix trips correspondence](op03_fix_trips_correspondence.md) |
| Actual                | OP-04 Clean trips                                                                                                   |
| Siguiente recomendada | [OP-02 Validate trips](op02_validate_trips.md) o [OP-05 Filter trips](op05_filter_trips.md)                         |

## Implementación relacionada

Esta operación forma parte de la API pública de Pylondrina v1.1. Su implementación principal puede revisarse en el código fuente del proyecto.

| Recurso            | Enlace                                                                                                                                                |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| Archivo fuente     | [`src/pylondrina/transforms/cleaning.py`](https://github.com/sebastianolmos/pylondrina/blob/main/src/pylondrina/transforms/cleaning.py)               |
| Catálogo de issues | [`src/pylondrina/issues/catalog_clean_trips.py`](https://github.com/sebastianolmos/pylondrina/blob/main/src/pylondrina/issues/catalog_clean_trips.py) |
| Referencia API     | [Ver referencia técnica](../../api/trips.md)                                                                                                          |
