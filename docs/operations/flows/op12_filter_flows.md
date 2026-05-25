# OP-12 Filter flows

`filter_flows` es la operación de filtrado declarativo para datasets de flows. Se implementó como una operación drop-only sobre `FlowDataset`: permite obtener un subconjunto de flujos ya construidos, sin reconstruirlos desde trips, sin corregir valores, sin validar formalmente, sin escribir en disco y sin mutar el input.

La operación permite aplicar filtros por atributos mediante `where` y filtros espaciales mediante `h3_cells`. Su salida es un nuevo `FlowDataset`, acompañado por un `OperationReport` que registra qué filtros se solicitaron, cuáles se aplicaron, cuáles se omitieron y cómo se trató el auxiliar `flow_to_trips`.

## Para qué sirve

Esta operación permite seleccionar subconjuntos de un `FlowDataset` ya construido. En v1.1, soporta dos ejes de filtrado:

- `where`, sobre columnas de `FlowDataset.flows`;
- `h3_cells`, sobre los extremos H3 del flujo.

Los filtros presentes se combinan por AND global. Esto significa que un flujo se conserva solo si cumple todos los criterios aplicados.

OP-12 se usa, por ejemplo, para separar flows por género, modo, propósito, magnitud, ventana temporal o pertenencia espacial a un conjunto de celdas H3.

## Cuándo usarla

Esta operación se usa después de construir flows con [OP-08 Build flows](op08_build_flows.md) o después de reconstruirlos mediante [OP-11 Read flows](op11_read_flows.md), cuando se necesita trabajar con un subconjunto del `FlowDataset`.

Un flujo típico es:

```text
build_flows -> filter_flows -> export_flows
```

o bien:

```text
read_flows -> filter_flows -> get_trips_from_flows
```

A diferencia de `build_flows`, OP-12 no vuelve a agregar trips. A diferencia de `export_flows`, no produce archivos externos. Su responsabilidad es reducir un `FlowDataset` ya existente sin alterar la semántica agregada de sus flujos.

## Qué recibe y qué retorna

La operación recibe:

- `flows`: un `FlowDataset`;
- `options`: una instancia opcional de `FlowFilterOptions`;
- `max_issues`: límite máximo de issues retenidos en el reporte.

`FlowFilterOptions` permite configurar:

- `where`: filtros declarativos por columnas;
- `h3_cells`: conjunto de celdas H3 permitidas;
- `spatial_predicate`: extremo espacial evaluado;
- `keep_flow_to_trips`: sincronización del auxiliar;
- `keep_metadata`: política de metadata en la salida;
- `strict`: política de escalamiento de errores recuperables.

La operación retorna:

```python
FlowDataset, OperationReport
```

El `FlowDataset` retornado es un nuevo objeto. El input no se muta. En rutas retornables, se preserva `metadata["is_validated"]`.

## Contrato interno esperado

OP-12 opera sobre el contrato interno canónico de `FlowDataset.flows`, no sobre layouts externos de exportación.

La tabla debe contener al menos:

```python
flow_id
origin_h3_index
destination_h3_index
flow_count
flow_value
```

También puede contener:

- columnas de segmentación provenientes de `group_by`;
- `window_start_utc`;
- `window_end_utc`;
- otros campos analíticos agregados al `FlowDataset`.

La operación no usa `origin_h3`, `destination_h3` ni `count` como nombres internos. Esos nombres pertenecen a layouts externos, no al contrato interno de flows.

## Sintaxis de `where`

`where` permite expresar filtros sobre columnas de `FlowDataset.flows`.

Formas recomendadas:

```python
# eq implícito
{"user_gender": "Mujer"}

# in implícito
{"mode": ["bus", "metro"]}

# operadores explícitos
{"flow_value": {"gte": 10}}
```

Operadores soportados:

| Tipo de operador | Operadores |
|---|---|
| Igualdad / pertenencia | `eq`, `in`, `ne`, `not_in` |
| Nulidad | `is_null`, `not_null` |
| Comparación | `gt`, `gte`, `lt`, `lte`, `between` |

La compatibilidad depende del tipo lógico del campo. Los campos canónicos tienen tipos efectivos conocidos:

| Campo | Tipo lógico |
|---|---|
| `flow_id` | string |
| `origin_h3_index` | string |
| `destination_h3_index` | string |
| `flow_count` | int |
| `flow_value` | float |
| `window_start_utc` | datetime |
| `window_end_utc` | datetime |

Los campos adicionales usan el dtype observado en pandas como fallback.

## Filtro espacial por H3

El filtro espacial se define mediante `h3_cells` y `spatial_predicate`.

```python
FlowFilterOptions(
    h3_cells=["8828308281fffff", "8828308283fffff"],
    spatial_predicate="origin",
)
```

`spatial_predicate` define sobre qué extremo del flujo se evalúa el conjunto H3:

| Valor | Semántica |
|---|---|
| `origin` | evalúa solo `origin_h3_index` |
| `destination` | evalúa solo `destination_h3_index` |
| `both` | exige que origen y destino estén en `h3_cells` |
| `either` | exige que origen o destino estén en `h3_cells` |

Las celdas se normalizan y deduplican antes de evaluar. Si el contenedor no es interpretable o queda vacío después de normalización, la operación aborta. Si algunas celdas no son válidas pero otras sí, se registra evidencia y se continúa con las celdas válidas.

## Qué evidencia deja

OP-12 retorna un `OperationReport` con:

- `ok`;
- `issues`;
- `summary`;
- `parameters`.

El `summary` contiene:

- `rows_in`;
- `rows_out`;
- `dropped_total`;
- `dropped_by_filter`;
- `filters_requested`;
- `filters_applied`;
- `filters_omitted`;
- `flow_to_trips_status`;
- `limits`, solo si hubo truncamiento de issues.

`dropped_by_filter` usa conteos incrementales reales. Es decir, cada filtro se evalúa sobre el estado vigente después de los filtros anteriores, no como conteos independientes sobre el dataframe original.

El bloque `parameters` registra la configuración efectiva y serializable:

- `where`;
- `h3_cells`;
- `spatial_predicate`;
- `keep_flow_to_trips`;
- `keep_metadata`;
- `strict`;
- `max_issues`.

Cuando `keep_metadata=True`, el dataset resultante registra un evento `filter_flows` en `metadata["events"]`. El evento incluye:

- `op`;
- `ts_utc`;
- `parameters`;
- `summary`;
- `issues_summary`.

Cuando `keep_metadata=False`, se preserva la metadata operativa excepto `events`, y no se agrega un evento nuevo.

## Política de `flow_to_trips`

Si `keep_flow_to_trips=True`, la operación intenta mantener sincronizado el auxiliar `flow_to_trips` con los `flow_id` retenidos.

Los estados posibles se reflejan en `summary["flow_to_trips_status"]`:

| Estado | Significado |
|---|---|
| `synced` | el auxiliar existe, es usable y fue filtrado por los `flow_id` retenidos |
| `missing` | el auxiliar fue solicitado, pero no existe |
| `discarded_invalid` | el auxiliar existe, pero no tiene estructura usable |
| `not_requested` | el usuario pidió no conservar auxiliar |

Nunca se conserva un `flow_to_trips` desalineado respecto de `flows`. Si no puede sincronizarse de forma segura, se descarta con evidencia.

## Consideraciones importantes

`filter_flows` preserva `metadata["is_validated"]` en rutas retornables. Esto ocurre porque la operación solo selecciona flujos existentes; no cambia la semántica de los flujos sobrevivientes.

`strict` no gobierna errores fatales de configuración. Problemas como input inválido, ausencia de columnas canónicas mínimas, `max_issues <= 0`, `where` no interpretable como mapping, `spatial_predicate` inválido o `h3_cells` vacío tras normalización abortan directamente.

En cambio, problemas por eje pueden degradar con `strict=False`, por ejemplo un campo inexistente dentro de `where`, un operador incompatible, un valor con forma inválida, algunas celdas H3 inválidas o un `flow_to_trips` no usable. Con `strict=True`, esos errores recuperables escalan a `FilterError`.

Si no se define ningún filtro, la operación retorna un nuevo dataset derivado sin cambios tabulares y deja evidencia informativa. Si los filtros se aplican pero no descartan filas, también queda evidencia. Si el resultado queda vacío, el dataset vacío es retornable y se registra un warning.

La operación reconstruye `provenance` como dataset derivado desde el `FlowDataset` de entrada, incluyendo una referencia resumida al dataset origen y un resumen compacto de eventos previos recientes.

## Ejemplo mínimo

El siguiente ejemplo filtra flows por género y conserva sincronizado `flow_to_trips` si está disponible.

```python
from pylondrina.transforms.flows_filtering import (
    FlowFilterOptions,
    filter_flows,
)

flows_mujer, report = filter_flows(
    flows,
    options=FlowFilterOptions(
        where={"user_gender": "Mujer"},
        keep_flow_to_trips=True,
        keep_metadata=True,
        strict=False,
    ),
)

print(report.summary)
print(flows_mujer.flows.head())
```

También puede combinarse filtrado atributivo y espacial:

```python
flows_subset, report = filter_flows(
    flows,
    options=FlowFilterOptions(
        where={
            "mode": ["bus", "metro"],
            "flow_value": {"gte": 10},
        },
        h3_cells=["8828308281fffff", "8828308283fffff"],
        spatial_predicate="origin",
        keep_flow_to_trips=True,
    ),
    max_issues=100,
)
```

## Operación anterior y siguiente

Dentro de la familia Trip → Flow, OP-12 selecciona subconjuntos de un `FlowDataset` ya construido o leído desde persistencia formal.

| Posición | Operación |
|---|---|
| Anterior recomendada | [OP-08 Build flows](op08_build_flows.md) u [OP-11 Read flows](op11_read_flows.md) |
| Actual | OP-12 Filter flows |
| Siguiente recomendada | [OP-09 Export flows](op09_export_flows.md), [OP-10 Write flows](op10_write_flows.md) u [OP-13 Get trips from flows](op13_get_trips_from_flows.md) |

## Implementación relacionada

Esta operación forma parte de la API pública de Pylondrina v1.1. Su implementación principal puede revisarse en el código fuente del proyecto.

| Recurso | Enlace |
|---|---|
| Archivo fuente | [`src/pylondrina/transforms/flows_filtering.py`](https://github.com/sebastianolmos/pylondrina/blob/main/src/pylondrina/transforms/flows_filtering.py) |
| Catálogo de issues | [`src/pylondrina/issues/catalog_filter_flows.py`](https://github.com/sebastianolmos/pylondrina/blob/main/src/pylondrina/issues/catalog_filter_flows.py) |
| Referencia API | [Ver referencia técnica](../../api/flows.md) |