# OP-02 Validate trips

`validate_trips` certifica la conformidad formal de un `TripDataset` ya construido bajo el contrato Golondrina. Su rol no es corregir datos, imputar valores ni transformar el dataframe, sino ejecutar checks configurables, construir un `ValidationReport`, registrar evidencia en `metadata["events"]` y actualizar `metadata["is_validated"]` según el resultado.

Esta operación consolida una frontera importante del pipeline: **Import construye; Validate certifica**. Por eso, un dataset importado por OP-01 puede estar en una forma operable, pero todavía no debe considerarse formalmente validado hasta ejecutar OP-02.

## Para qué sirve

Se implementó `validate_trips` para responder una pregunta concreta:

> ¿Este `TripDataset` cumple el contrato esperado para trips en Golondrina?

La operación revisa el dataset usando `trips.schema` como contrato base, `trips.schema_effective` como contexto complementario y `trips.metadata` como espacio de trazabilidad. En v1.1, los checks soportados incluyen:

- columnas requeridas;
- nullabilidad efectiva;
- tipos y formatos básicos;
- constraints simples declarativas;
- dominios categóricos en modo `off`, `full` o `sample`;
- consistencia temporal mínima para datasets Tier 1;
- duplicados opcionales mediante un subset explícito.

## Cuándo usarla

Esta operación se usa después de construir un `TripDataset`, típicamente después de OP-01 `import_trips_from_dataframe`.

También se recomienda volver a ejecutarla después de operaciones que cambian el significado o la conformidad del dataset. Por ejemplo, un flujo frecuente es:

```text
OP-01 import_trips
        ↓
OP-02 validate_trips
        ↓
OP-04 clean_trips / OP-05 filter_trips
        ↓
OP-02 validate_trips
```

Este patrón permite distinguir entre una validación inicial, que puede revelar problemas de completitud o consistencia, y una validación final sobre el dataset ya preparado para análisis.

## Qué recibe y qué retorna

La operación recibe un `TripDataset` y opciones de validación.

```python
from pylondrina.validation import ValidationOptions, validate_trips

report = validate_trips(
    trips,
    options=ValidationOptions(
        validate_domains="full",
        validate_temporal_consistency=True,
    ),
)
```

La salida es un `ValidationReport` con:

- `ok`: indica si el reporte no contiene errores;
- `issues`: lista de hallazgos agregados;
- `summary`: resumen pequeño y estable de la validación.

La operación no retorna un nuevo `TripDataset`. Trabaja sobre el objeto recibido y no reemplaza `trips.data`.

## Qué evidencia deja

Cuando la validación se ejecuta normalmente, el sistema registra un evento `validate_trips` en `trips.metadata["events"]`.

Ese evento incluye:

- `op`;
- `ts_utc`;
- `parameters`;
- `summary`;
- `issues_summary`.

Además, la operación actualiza:

```python
trips.metadata["is_validated"]
```

La regla general es:

- `True` si el reporte queda sin errores;
- `False` si existen errores de validación.

El reporte mantiene un `summary` compacto con conteos por severidad, conteos por código, campos revisados, checks ejecutados y bloques opcionales para dominios, temporalidad, duplicados o límites cuando esos checks fueron evaluados.

## Consideraciones importantes

### Validate no corrige datos

`validate_trips` no modifica `trips.data`. Si detecta errores, no intenta corregirlos automáticamente. La corrección o reducción del dataset debe hacerse con operaciones posteriores, como `fix_trips_correspondence`, `clean_trips` o `filter_trips`, según el tipo de problema.

### `strict=True` registra evidencia antes de lanzar

Cuando `strict=True`, si la validación detecta errores, la operación primero construye el reporte, registra el evento y actualiza `metadata["is_validated"]`. Solo después lanza `ValidationError`.

Esto permite conservar evidencia de la validación fallida.

### Los errores fatales de configuración ocurren antes del evento

Algunos problemas de configuración impiden ejecutar la operación. Por ejemplo, si `validate_duplicates=True` pero no se entrega un `duplicates_subset` usable, la operación aborta antes del pipeline normal y no registra evento nuevo.

### La regla OD parcial aplica solo a coordenadas

Con `allow_partial_od_spatial=True`, una fila puede tener solo origen completo o solo destino completo. Se considera inválida cuando faltan ambos extremos espaciales. Esta excepción aplica a las coordenadas OD canónicas, no a los índices H3.

### Dominios efectivos

La validación de dominios usa dominios efectivos cuando están disponibles. Esto permite validar datasets importados donde OP-01 extendió o resolvió dominios a partir de la fuente.

## Ejemplo mínimo

```python
from pylondrina.validation import ValidationOptions, validate_trips

report = validate_trips(
    trips,
    options=ValidationOptions(
        validate_required_fields=True,
        validate_types_and_formats=True,
        validate_constraints=True,
        validate_domains="full",
        validate_temporal_consistency=True,
        validate_duplicates=False,
        allow_partial_od_spatial=True,
    ),
)

print(report.ok)
print(report.summary)
print(trips.metadata["is_validated"])
```

Si el dataset cumple los checks habilitados, `report.ok` será `True` y `metadata["is_validated"]` quedará en `True`.

Si existen errores, el reporte permite inspeccionar los `issues` sin que la operación corrija el dataset automáticamente.

## Operación anterior y siguiente

En el flujo de trips, OP-02 normalmente se ubica después de OP-01:

```text
OP-01 Import trips → OP-02 Validate trips
```

Después de validar, el usuario puede:

- corregir correspondencias con OP-03;
- limpiar filas problemáticas con OP-04;
- filtrar subconjuntos analíticos con OP-05;
- persistir el dataset con OP-06;
- construir flows con OP-08.

Cuando una operación posterior cambia el dataset, puede ser necesario volver a ejecutar OP-02 para certificar el nuevo estado.

## Implementación relacionada

Esta operación forma parte de la API pública de Pylondrina v1.1. Su implementación principal puede revisarse en el código fuente del proyecto.

| Recurso | Enlace |
|---|---|
| Archivo fuente | [`src/pylondrina/validation.py`](https://github.com/sebastianolmos/pylondrina/blob/main/src/pylondrina/validation.py) |
| Catálogo de issues | [`src/pylondrina/issues/catalog_validate_trips.py`](https://github.com/sebastianolmos/pylondrina/blob/main/src/pylondrina/issues/catalog_validate_trips.py) |
| Referencia API | [Ver referencia técnica](../../api/trips.md) |