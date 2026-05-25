# OP-15 Validate traces

`validate_traces` es la operación de validación formal mínima para datasets de trazas. Se implementó para certificar si un `TraceDataset` ya construido cumple el núcleo canónico y las reglas simples declaradas en su `TraceSchema`.

La operación no crea un dataset nuevo, no corrige valores, no reordena puntos, no infiere viajes, no escribe artefactos y no modifica `TraceDataset.data`. Sus efectos se concentran en el reporte retornado y en `traces.metadata`, donde actualiza `metadata["is_validated"]` y registra un evento `validate_traces`.

## Para qué sirve

Esta operación permite verificar si un `TraceDataset` importado puede considerarse conforme al contrato mínimo de traces usado por Pylondrina v1.1.

El núcleo canónico validado es:

```python
point_id
user_id
time_utc
latitude
longitude
```

Además del núcleo, la operación puede validar campos adicionales declarados en el `TraceSchema`, siempre que usen dtypes y constraints soportadas por el bloque de traces.

## Cuándo usarla

Esta operación se usa después de [OP-14 Import traces](op14_import_traces.md), cuando se necesita certificar que el dataset de puntos discretos quedó estructurado correctamente antes de usarlo en operaciones posteriores.

Un flujo típico es:

```text
import_traces_from_dataframe -> validate_traces -> infer_trips_from_traces
```

OP-15 es especialmente relevante antes de [OP-16 Infer trips from traces](op16_infer_trips_from_traces.md), porque la inferencia espera trabajar con trazas estructuradas y, por defecto, validadas.

## Qué recibe y qué retorna

La operación recibe:

- `traces`: un `TraceDataset`;
- `options`: una instancia opcional de `TraceValidationOptions`.

`TraceValidationOptions` permite configurar:

- `strict`;
- `sample_rows_per_issue`;
- `validate_required_fields`;
- `validate_types_and_formats`;
- `validate_constraints`;
- `validate_monotonic_time_per_user`.

La operación retorna:

```python
ConsistencyReport
```

En OP-15, el resultado semántico de validación se consulta en:

```python
report.summary["ok"]
```

No debe asumirse que `ConsistencyReport` expone un atributo top-level `ok`. El reporte contiene principalmente:

- `issues`;
- `summary`.

Los parámetros efectivos de la validación quedan registrados en el evento `validate_traces` dentro de `traces.metadata["events"]`.

## Qué valida

OP-15 valida cuatro bloques principales:

1. campos requeridos;
2. tipos y formatos;
3. constraints simples declaradas en el schema;
4. monotonicidad temporal por usuario.

Los campos requeridos efectivos se construyen como:

```text
núcleo canónico de traces + schema.required
```

Esto significa que `point_id`, `user_id`, `time_utc`, `latitude` y `longitude` son parte del mínimo esperado aunque el schema no los repita explícitamente.

## Dtypes y constraints soportadas

El bloque de traces v1.1 usa un contrato deliberadamente acotado. Los dtypes soportados son:

```python
"string"
"int"
"float"
"datetime"
"bool"
```

El dtype `categorical` no está soportado para traces v1.1. Si aparece en un `TraceSchema`, la operación aborta como error de schema antes de construir reporte o evento.

Las constraints reconocidas son:

```python
"nullable"
"range"
"datetime"
"pattern"
"length"
"unique"
```

No todas las constraints aplican a todos los dtypes. La operación valida esa compatibilidad durante el preflight del schema. Si una constraint es desconocida o no está permitida para el dtype declarado, la operación aborta. Si la constraint es conocida y compatible, pero tiene un payload inválido, se omite con warning y se continúa.

## Monotonicidad temporal por usuario

La validación de monotonicidad revisa que `time_utc` no retroceda dentro de cada `user_id`.

La regla se evalúa sobre el orden observado de `TraceDataset.data`. La operación no reordena el dataframe antes de validar.

Un retroceso temporal por usuario se reporta como warning, no como error. Por lo tanto, si el único problema detectado es monotonicidad no creciente, el reporte puede quedar con:

```python
report.summary["ok"] == True
```

y el dataset puede quedar con:

```python
traces.metadata["is_validated"] == True
```

aunque exista evidencia de advertencia en `report.issues`.

## Qué evidencia deja

OP-15 retorna un `ConsistencyReport` con:

- `issues`;
- `summary`.

El `summary` contiene:

- `ok`;
- `n_rows`;
- `n_issues`;
- `n_errors`;
- `n_warnings`;
- `n_info`;
- `counts_by_level`;
- `counts_by_code`;
- `checked_fields`;
- `checks_executed`;
- `schema_version`.

Además, la operación actualiza `traces.metadata`:

- asegura `metadata["events"]`;
- agrega un evento `validate_traces`;
- actualiza `metadata["is_validated"]`.

El evento `validate_traces` contiene:

- `op`;
- `ts_utc`;
- `parameters`;
- `summary`;
- `issues_summary`.

`parameters` corresponde a las opciones efectivas de validación. `issues_summary` mantiene una estructura compacta con conteos por severidad y códigos principales.

## Política de estado validado

La señal oficial de validación queda en:

```python
traces.metadata["is_validated"]
```

La regla es:

- `True` si no hay issues de nivel error;
- `False` si existe al menos un issue de nivel error.

Los warnings no invalidan por sí solos el dataset. Por eso una advertencia de monotonicidad temporal puede quedar registrada sin impedir que `metadata["is_validated"]` sea `True`.

## Consideraciones importantes

OP-15 no muta `TraceDataset.data`. La tabla de puntos observada antes de validar se mantiene igual después de la operación.

La operación sí modifica `traces.metadata`, porque ese es el lugar donde registra el estado de validación y el evento operacional.

`strict` no gobierna errores fatales de schema o configuración. Problemas como `TraceSchema` ausente, dtype `categorical`, dtype desconocido, constraint desconocida, constraint no permitida para un dtype o flags inválidas abortan antes de construir reporte o evento.

En cambio, los errores de datos sí producen evidencia normal. Con `strict=False`, la operación retorna un `ConsistencyReport` con `summary["ok"] = False`. Con `strict=True`, primero construye el reporte, actualiza metadata, registra el evento y luego escala a `ValidationError` si existe al menos un issue de nivel error.

## Ejemplo mínimo

El siguiente ejemplo valida un `TraceDataset` importado desde una fuente de puntos discretos.

```python
from pylondrina.validation_traces import (
    TraceValidationOptions,
    validate_traces,
)

report = validate_traces(
    traces,
    options=TraceValidationOptions(
        strict=False,
        sample_rows_per_issue=5,
        validate_required_fields=True,
        validate_types_and_formats=True,
        validate_constraints=True,
        validate_monotonic_time_per_user=True,
    ),
)

print(report.summary["ok"])
print(report.summary["n_rows"])
print(report.summary["counts_by_code"])
print(traces.metadata["is_validated"])
```

Una salida válida puede incluir warnings recuperables, por ejemplo:

```python
{
    "VAL.TEMPORAL.NON_MONOTONIC_TIME": 1
}
```

Si ese warning no viene acompañado de errores, el dataset puede quedar validado.

## Operación anterior y siguiente

Dentro de la familia traces, OP-15 certifica formalmente el resultado construido por OP-14.

| Posición | Operación |
|---|---|
| Anterior recomendada | [OP-14 Import traces](op14_import_traces.md) |
| Actual | OP-15 Validate traces |
| Siguiente recomendada | [OP-16 Infer trips from traces](op16_infer_trips_from_traces.md) |

## Implementación relacionada

Esta operación forma parte de la API pública de Pylondrina v1.1. Su implementación principal puede revisarse en el código fuente del proyecto.

| Recurso | Enlace |
|---|---|
| Archivo fuente | [`src/pylondrina/validation_traces.py`](https://github.com/sebastianolmos/pylondrina/blob/main/src/pylondrina/validation_traces.py) |
| Catálogo de issues | [`src/pylondrina/issues/catalog_validate_traces.py`](https://github.com/sebastianolmos/pylondrina/blob/main/src/pylondrina/issues/catalog_validate_traces.py) |
| Referencia API | [Ver referencia técnica](../../api/traces.md) |