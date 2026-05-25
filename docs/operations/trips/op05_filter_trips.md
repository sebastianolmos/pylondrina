# OP-05 Filter trips

`filter_trips` es la operación de filtrado declarativo para datasets de trips. Se implementó como una operación drop-only: permite obtener un subconjunto de filas desde un `TripDataset`, sin corregir valores, sin validar formalmente, sin escribir en disco y sin mutar el dataset de entrada.

La operación permite aplicar filtros por atributos, tiempo y espacio, combinándolos de manera reproducible. Su salida es un nuevo `TripDataset` acompañado por un `OperationReport` que registra qué filtros se solicitaron, cuáles se aplicaron, cuáles se omitieron y cuántas filas fueron descartadas.

## Para qué sirve

Esta operación permite seleccionar viajes o movements según criterios analíticos explícitos. En v1.1, soporta tres familias de filtros:

- filtros por atributos mediante `where`;
- filtro temporal mediante `TimeFilter`;
- filtros espaciales mediante `bbox`, `polygon` o `h3_cells`.

Los filtros presentes se combinan por AND global. Esto significa que una fila se conserva solo si cumple todos los criterios aplicados.

OP-05 se usa para construir subconjuntos reproducibles, por ejemplo:

- viajes de trabajo en día laboral;
- viajes de un grupo sociodemográfico;
- viajes que empiezan dentro de una ventana temporal;
- viajes cuyo origen cae dentro de un bounding box;
- viajes asociados a un conjunto de celdas H3.

## Cuándo usarla

Esta operación se usa después de importar, validar, corregir o limpiar un `TripDataset`, cuando se necesita restringir el universo de análisis.

Un flujo típico es:

```text
import_trips_from_dataframe -> validate_trips -> clean_trips -> filter_trips -> build_flows
```

También puede usarse después de inferir trips desde traces, siempre que el resultado sea un `TripDataset` con los campos necesarios para los filtros solicitados.

A diferencia de `clean_trips`, que elimina filas problemáticas según reglas de calidad, `filter_trips` selecciona filas por criterios analíticos. Por ejemplo, limpiar puede retirar registros con coordenadas inválidas, mientras que filtrar puede seleccionar solo viajes de propósito `work`, con `trip_weight > 0` y dentro de una ventana horaria.

## Qué recibe y qué retorna

La operación recibe:

* `trips`: un `TripDataset`;
* `options`: una instancia de `FilterOptions`;
* `max_issues`: límite máximo de issues retenidos en el reporte;
* `sample_rows_per_issue`: tamaño máximo de muestras incluidas en `Issue.details`.

`FilterOptions` concentra la semántica del filtrado:

* `where`: filtros declarativos por columnas;
* `time`: filtro temporal absoluto;
* `bbox`: bounding box en coordenadas `(min_lon, min_lat, max_lon, max_lat)`;
* `polygon`: polígono en coordenadas `(lon, lat)`;
* `h3_cells`: conjunto de celdas H3 permitidas;
* `spatial_predicate`: extremo espacial evaluado;
* `origin_h3_field` y `destination_h3_field`: nombres de campos H3;
* `keep_metadata`: política de metadata en la salida;
* `strict`: política de escalamiento de errores recuperables.

La operación retorna:

```python
TripDataset, OperationReport
```

El `TripDataset` retornado es un nuevo objeto. El input no se muta. En rutas retornables, el resultado preserva el estado previo de `metadata["is_validated"]`.

## Qué evidencia deja

OP-05 retorna un `OperationReport` con:

* `ok`;
* `issues`;
* `summary`;
* `parameters`.

El `summary` contiene:

* `rows_in`;
* `rows_out`;
* `dropped_total`;
* `dropped_by_filter`;
* `filters_requested`;
* `filters_applied`;
* `filters_omitted`;
* `limits`, solo si hubo truncamiento de issues.

El bloque `dropped_by_filter` usa conteos incrementales reales. Es decir, cada filtro se evalúa sobre las filas que sobrevivieron a los filtros anteriores, no como conteos independientes sobre el dataframe original.

El bloque `parameters` registra el request efectivo normalizado y serializable. Por ejemplo, `time` queda como `{start, end, predicate}`, `bbox` queda como lista, `h3_cells` queda normalizado y deduplicado, y `where` queda convertido a una forma JSON-safe.

Cuando `keep_metadata=True`, el dataset resultante registra un evento `filter_trips` en `metadata["events"]` con:

* `op`;
* `ts_utc`;
* `parameters`;
* `summary`;
* `issues_summary`.

Cuando `keep_metadata=False`, la salida no queda sin metadata. En cambio, conserva una metadata mínima operativa con `dataset_id`, `is_validated`, `temporal`, `h3`, `schema` y `domains_effective` cuando esos bloques existen.

## Sintaxis de `where`

`where` permite expresar filtros por columnas de `TripDataset.data`.

Formas recomendadas:

```python
# eq implícito
{"mode": "bus"}

# in implícito
{"mode": ["bus", "metro"]}

# operadores explícitos
{"trip_weight": {"gt": 0}}
```

Operadores soportados:

| Tipo de operador       | Operadores                          |
| ---------------------- | ----------------------------------- |
| Igualdad / pertenencia | `eq`, `in`, `ne`, `not_in`          |
| Nulidad                | `is_null`, `not_null`               |
| Comparación            | `gt`, `gte`, `lt`, `lte`, `between` |

La compatibilidad depende del tipo lógico del campo. Por ejemplo, campos `string` o `categorical` soportan igualdad, pertenencia y nulidad; campos numéricos y temporales soportan además comparaciones; campos booleanos soportan igualdad, desigualdad y nulidad.

Si una cláusula de `where` apunta a un campo inexistente, usa un operador incompatible o tiene forma inválida, la cláusula puede omitirse con un issue recuperable. Si `strict=True`, esos errores recuperables pueden escalar a `FilterError`.

## Filtro temporal

El filtro temporal se define mediante `TimeFilter`:

```python
TimeFilter(
    start="2026-01-01T07:00:00Z",
    end="2026-01-01T09:00:00Z",
    predicate="overlaps",
)
```

La operación interpreta cada viaje como el intervalo:

```text
[origin_time_utc, destination_time_utc)
```

y el rango solicitado como:

```text
[start, end)
```

Predicados temporales disponibles:

| Predicado       | Significado                                         |
| --------------- | --------------------------------------------------- |
| `starts_within` | el origen del viaje cae dentro del rango            |
| `ends_within`   | el destino del viaje cae dentro del rango           |
| `contains`      | el viaje contiene completamente el rango solicitado |
| `overlaps`      | el viaje y el rango solicitado se intersectan       |

El filtro temporal requiere temporalidad Tier 1. Si el dataset solo tiene Tier 2 o Tier 3, la regla se omite con evidencia y puede escalar bajo `strict=True`.

## Filtros espaciales

Los filtros espaciales pueden definirse mediante:

* `bbox`;
* `polygon`;
* `h3_cells`.

El parámetro `spatial_predicate` define sobre qué extremo se evalúa el filtro:

| Valor         | Semántica                          |
| ------------- | ---------------------------------- |
| `origin`      | evalúa solo el origen              |
| `destination` | evalúa solo el destino             |
| `both`        | exige que origen y destino cumplan |
| `either`      | exige que origen o destino cumplan |

`bbox` y `polygon` usan coordenadas OD. `h3_cells` usa los campos `origin_h3_field` y `destination_h3_field`, cuyos valores por defecto son:

```python
origin_h3_field = "origin_h3_index"
destination_h3_field = "destination_h3_index"
```

## Consideraciones importantes

La operación preserva `metadata["is_validated"]` en rutas retornables. Esto ocurre porque OP-05 solo selecciona filas; no cambia la semántica de las filas sobrevivientes ni reinterpreta el contrato del dataset.

`strict` no gobierna abortos fatales de configuración. Problemas como un `bbox` inválido, timestamps ilegibles, `start >= end`, `options` inválido o `max_issues <= 0` abortan porque el request no es interpretable. En cambio, problemas por eje, como campo inexistente en `where` o filtro temporal no evaluable, pueden registrarse como issues recuperables y omitirse con `strict=False`.

Si no se define ningún filtro, la operación retorna un nuevo dataset sin cambios efectivos y registra evidencia informativa. Si los filtros se aplican pero no descartan filas, también se deja evidencia. Si el resultado queda vacío, el dataset vacío es retornable y se registra un warning.

## Ejemplo mínimo

El siguiente ejemplo selecciona viajes de trabajo o estudio, realizados en bus o metro, dentro de una ventana temporal y con origen dentro de un bounding box.

```python
from pylondrina.transforms.filtering import (
    FilterOptions,
    TimeFilter,
    filter_trips,
)

options = FilterOptions(
    where={
        "mode": ["bus", "metro"],
        "purpose": ["work", "study"],
        "trip_weight": {"gt": 0},
    },
    time=TimeFilter(
        start="2026-01-01T07:00:00Z",
        end="2026-01-01T09:00:00Z",
        predicate="overlaps",
    ),
    bbox=(-70.70, -33.50, -70.60, -33.40),
    spatial_predicate="origin",
)

filtered_trips, report = filter_trips(
    trips,
    options=options,
    max_issues=100,
    sample_rows_per_issue=10,
)

print(report.summary)
print(filtered_trips.metadata["is_validated"])
```

## Operación anterior y siguiente

Dentro de la familia de operaciones sobre trips, OP-05 se ubica como una transformación reproducible orientada a selección analítica.

| Posición              | Operación                                         |
| --------------------- | ------------------------------------------------- |
| Anterior recomendada  | [OP-04 Clean trips](op04_clean_trips.md)          |
| Actual                | OP-05 Filter trips                                |
| Siguiente recomendada | [OP-08 Build flows](../flows/op08_build_flows.md) |
| Alternativa posterior | [OP-06 Write trips](op06_write_trips.md)          |

## Implementación relacionada

Esta operación forma parte de la API pública de Pylondrina v1.1. Su implementación principal puede revisarse en el código fuente del proyecto.

| Recurso            | Enlace                                                                                                                                                    |
| ------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Archivo fuente     | [`src/pylondrina/transforms/filtering.py`](https://github.com/sebastianolmos/pylondrina/blob/main/src/pylondrina/transforms/filtering.py)                 |
| Catálogo de issues | [`src/pylondrina/issues/catalogo_filter_trips.py`](https://github.com/sebastianolmos/pylondrina/blob/main/src/pylondrina/issues/catalogo_filter_trips.py) |
| Referencia API     | [Ver referencia técnica](../../api/trips.md)                                                                                                              |
