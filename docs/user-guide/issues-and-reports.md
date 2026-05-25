# Issues, reportes y trazabilidad

## Propósito de la guía

Esta guía explica cómo interpretar la evidencia operacional que deja Pylondrina al ejecutar sus operaciones públicas. La pregunta práctica es:

> Cuando se ejecuta una operación de Pylondrina, ¿cómo se entiende qué pasó?

En Pylondrina, una operación no se interpreta solo por la tabla resultante. El contrato observable puede incluir también un reporte, issues, metadata, eventos y, cuando hay persistencia o exportación, sidecars. Esta evidencia permite auditar un pipeline, distinguir entre construcción y validación, identificar pérdidas de filas, revisar configuraciones efectivas y decidir el siguiente paso del flujo de trabajo.

Esta lógica es transversal a las operaciones sobre trips, traces y flows.

## Qué produce una operación

Según la operación, la ejecución puede producir una o varias de estas piezas:

| Pieza                     | Rol                                                                                                                                           |
| ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| Dataset o tabla de salida | Objeto principal producido por la operación, por ejemplo `TripDataset`, `TraceDataset`, `FlowDataset` o una tabla de correspondencia.         |
| Reporte                   | Resumen estructurado de la ejecución. Permite ver si la operación terminó correctamente, qué parámetros efectivos usó y qué hallazgos emitió. |
| Issues                    | Hallazgos agregados de diagnóstico. Pueden indicar información, advertencias o errores.                                                       |
| Metadata                  | Estado persistible del dataset, incluyendo identidad, validación, trazabilidad y eventos.                                                     |
| Evento                    | Registro append-only dentro de `metadata["events"]` que documenta una operación ejecutada.                                                    |
| Sidecar                   | Archivo JSON que materializa metadata y trazabilidad en artefactos persistidos o exportados.                                                  |

Por ejemplo, una operación de importación produce un dataset y un `ImportReport`. Una operación de validación actualiza `metadata["is_validated"]` y agrega un evento de validación. Una operación de persistencia escribe archivos tabulares y un sidecar. Una operación de exportación produce un layout externo y un `metadata.json` de exportación.

## Cómo leer un reporte

Los reportes de Pylondrina se mantienen pequeños y estables. Su objetivo no es reemplazar al dataset ni contener todo el detalle del pipeline, sino entregar una síntesis operacional legible.

En general, conviene revisar cuatro partes.

### `ok`

`ok` indica si la operación terminó sin issues de nivel `error`.

```python
report.ok
```

En la mayoría de los reportes, `ok=True` significa que la operación pudo completarse sin errores operacionales. `ok=False` significa que la operación retornó un resultado, pero detectó al menos un problema de nivel error.

En OP-15 `validate_traces`, el reporte retornado es un `ConsistencyReport`; en ese caso, la señal semántica principal está en:

```python
report.summary["ok"]
```

### `summary`

`summary` entrega el resumen mínimo y estable de la operación.

Ejemplos típicos:

```python
report.summary
```

Puede incluir, según la operación:

* filas de entrada y salida;
* filas descartadas;
* número de flows construidos;
* número de issues;
* archivos escritos o leídos;
* fuente usada para una reconstrucción;
* candidatos descartados en inferencia.

La idea es que `summary` responda rápidamente qué ocurrió, sin obligar a inspeccionar todos los detalles internos.

### `issues`

`issues` contiene hallazgos agregados.

```python
report.issues
```

Los issues no deben interpretarse necesariamente como “un issue por fila”. Muchas operaciones agregan problemas por regla, campo, filtro, eje o causa. Por ejemplo, una validación puede emitir un issue que representa miles de filas afectadas por una misma regla.

### `parameters`

`parameters` registra la configuración efectiva usada por la operación.

```python
report.parameters
```

Esto es especialmente útil para reproducibilidad. Permite saber con qué filtros, umbrales, modo de persistencia, resolución H3, backend o política `strict` se ejecutó una operación.

En algunas validaciones, los parámetros efectivos pueden quedar principalmente en el evento registrado en `metadata["events"]`.

## Cómo leer un issue

Un issue representa un hallazgo estructurado. No es solo un mensaje de texto. Normalmente contiene:

| Campo          | Significado                                                     |
| -------------- | --------------------------------------------------------------- |
| `code`         | Código estable del hallazgo. Sirve para clasificar el problema. |
| `level`        | Severidad: `info`, `warning` o `error`.                         |
| `message`      | Explicación legible para el usuario.                            |
| `field`        | Campo afectado, cuando aplica.                                  |
| `source_field` | Campo fuente afectado, cuando aplica.                           |
| `row_count`    | Cantidad agregada de filas afectadas, cuando aplica.            |
| `details`      | Información adicional estructurada.                             |

Ejemplo de inspección:

```python
for issue in report.issues:
    print(issue.level, issue.code, issue.field, issue.row_count)
```

No se recomienda memorizar todos los códigos. Es más útil leerlos como señales diagnósticas. Algunos ejemplos representativos son:

| Código                                    | Interpretación práctica                                                                                          |
| ----------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `VAL.CORE.OD_SPATIAL_BOTH_MISSING`        | Hay filas sin origen ni destino espacial completo. Puede requerirse limpieza antes de construir flows.           |
| `IMP.CORE.POINT_ID_GENERATED`             | El import de traces generó `point_id` porque la fuente no lo entregaba.                                          |
| `READ.METADATA.VALIDATED_FORCED_FALSE`    | Un dataset leído desde persistencia formal quedó marcado como no validado, porque leer no certifica conformidad. |
| `FLT_FLOW.WHERE.FIELD_MISSING`            | Un filtro pidió una columna inexistente. Con `strict=False`, ese eje puede omitirse con evidencia.               |
| `INF.PRECONDITION.VALIDATION_BYPASS_USED` | Se permitió inferir trips desde traces no validadas usando bypass explícito.                                     |

Un issue de nivel `warning` no siempre bloquea el flujo. Un issue de nivel `error` sí indica que algo no cumplió el contrato esperado, aunque algunas operaciones pueden retornar resultado con `ok=False` para que el usuario decida cómo continuar.

## Metadata y eventos

La metadata del dataset es parte del contrato observable. No debe tratarse como un detalle accesorio. En particular, Pylondrina usa:

```python
dataset.metadata
```

para guardar identidad, estado de validación, trazabilidad, información temporal, configuración efectiva y eventos.

La lista de eventos vive en:

```python
dataset.metadata["events"]
```

Cada evento registra una operación ejecutada sobre el dataset o sobre el dataset derivado. La forma mínima esperada es:

```python
{
    "op": "...",
    "ts_utc": "...",
    "parameters": {...},
    "summary": {...},
    "issues_summary": {...}
}
```

Los campos principales son:

| Campo            | Rol                                                            |
| ---------------- | -------------------------------------------------------------- |
| `op`             | Nombre de la operación ejecutada.                              |
| `ts_utc`         | Timestamp de ejecución en UTC.                                 |
| `parameters`     | Parámetros efectivos usados.                                   |
| `summary`        | Resumen estable de la ejecución.                               |
| `issues_summary` | Conteo compacto de issues por severidad y códigos principales. |

Para revisar la última operación registrada:

```python
last_event = dataset.metadata["events"][-1]

print(last_event["op"])
print(last_event["summary"])
print(last_event["issues_summary"])
```

Los eventos permiten reconstruir el historial operacional sin depender solo del notebook que produjo el resultado.

## Estado de validación

La señal oficial del estado de validación de un dataset es:

```python
dataset.metadata["is_validated"]
```

Esta señal es importante porque varias operaciones usan el estado validado como precondición o como información de trazabilidad.

La política general en Pylondrina v1.1 es:

| Situación                      | Efecto sobre `metadata["is_validated"]`                                             |
| ------------------------------ | ----------------------------------------------------------------------------------- |
| Importar trips o traces        | Deja el dataset como no validado. Import construye, pero no certifica.              |
| Validar trips o traces         | Puede marcar el dataset como validado si no hay errores.                            |
| Corregir correspondencias      | Si cambia la semántica, puede invalidar el dataset para exigir revalidación.        |
| Limpiar o filtrar              | Preserva el estado validado en rutas retornables, porque son operaciones drop-only. |
| Construir flows                | Produce un dataset derivado no validado.                                            |
| Leer desde persistencia formal | No certifica conformidad automáticamente; normalmente fuerza estado no validado.    |
| Inferir trips desde traces     | Produce un `TripDataset` derivado no validado.                                      |

Por eso, un patrón frecuente es:

```text
import -> validate -> clean/filter/fix -> validate -> build/export/write
```

Después de operaciones que construyen o derivan un nuevo dataset, conviene decidir explícitamente si corresponde ejecutar una validación formal antes de continuar.

## Strict, warnings y abortos

Los issues usan tres niveles de severidad:

| Nivel     | Sentido práctico                                                                         |
| --------- | ---------------------------------------------------------------------------------------- |
| `info`    | Información operacional. No indica problema.                                             |
| `warning` | Situación recuperable o limitación que conviene revisar.                                 |
| `error`   | Problema que impide certificar conformidad o que afecta una parte relevante del request. |

Muchas operaciones tienen una opción `strict`.

Con `strict=False`, ciertos problemas recuperables pueden quedar como issues y la operación puede retornar un resultado. Por ejemplo, un filtro puede omitir un eje inválido, o una operación puede continuar con un auxiliar ausente dejando evidencia.

Con `strict=True`, esos errores recuperables pueden escalar a excepción después de construir evidencia suficiente.

Esto no significa que `strict` gobierne todos los errores. Los abortos fatales de configuración o precondición pueden ocurrir siempre, incluso con `strict=False`. Por ejemplo:

* input que no es el tipo de dataset esperado;
* ausencia de columnas canónicas mínimas;
* schema inválido;
* path inexistente o sidecar obligatorio ausente;
* opción no interpretable;
* timezone inválida;
* resolución H3 no usable.

Una forma práctica de leerlo es:

> `strict` controla parte de la degradación operacional recuperable, pero no convierte configuraciones inválidas en ejecuciones válidas.

## Sidecars

Los sidecars materializan trazabilidad fuera de memoria. Son archivos JSON que acompañan artefactos persistidos o exportados.

### Bundles `.golondrina`

En persistencia formal interna, Pylondrina usa bundles `.golondrina` con sidecar obligatorio.

Ejemplos:

```text
trips_artifact.golondrina/
├── trips.feather
└── trips.metadata.json
```

```text
flows_artifact.golondrina/
├── flows.feather
├── flow_to_trips.feather
└── flows.metadata.json
```

Estos sidecars permiten reconstruir formalmente datasets mediante operaciones de lectura como `read_trips` o `read_flows`. Contienen identidad, backend físico, archivos, schema o `aggregation_spec`, metadata y provenance.

Un bundle `.golondrina` no debe confundirse con una certificación automática de conformidad. Es un artefacto formal de persistencia. La conformidad semántica depende del contenido y, cuando corresponda, de una validación posterior.

### Exportaciones externas

Las exportaciones a layouts externos, por ejemplo para flowmaps, también pueden generar sidecars como `metadata.json`.

Ese sidecar documenta el artefacto exportado: qué se escribió, desde qué dataset, con qué formato y con qué configuración. Sin embargo, no reemplaza la persistencia formal interna ni permite reconstruir necesariamente un `FlowDataset` completo como lo hace `read_flows`.

La diferencia práctica es:

| Caso                                          | Propósito                                   |
| --------------------------------------------- | ------------------------------------------- |
| `trips.metadata.json` / `flows.metadata.json` | Reconstrucción formal interna del dataset.  |
| `metadata.json` de exportación                | Interpretación del layout externo generado. |

## Patrón práctico de diagnóstico

Cuando una operación termina, se recomienda revisar la evidencia en este orden:

1. Mirar el estado general:

   ```python
   report.ok
   ```

   En OP-15, usar:

   ```python
   report.summary["ok"]
   ```

2. Revisar el resumen:

   ```python
   report.summary
   ```

3. Inspeccionar los issues principales:

   ```python
   [(i.level, i.code, i.field, i.row_count) for i in report.issues]
   ```

4. Revisar el último evento del dataset, si la operación registra eventos:

   ```python
   dataset.metadata["events"][-1]
   ```

5. Revisar el estado de validación:

   ```python
   dataset.metadata["is_validated"]
   ```

6. Decidir el siguiente paso:

   | Diagnóstico                              | Acción típica                                                                     |
   | ---------------------------------------- | --------------------------------------------------------------------------------- |
   | Import exitoso pero `is_validated=False` | Ejecutar validación.                                                              |
   | Validación con errores de datos          | Revisar si corresponde `fix`, `clean` o ajustar el schema.                        |
   | Faltan campos o columnas canónicas       | Volver al import o revisar correspondencias.                                      |
   | Filtros no aplicados                     | Revisar `parameters`, `issues` y nombres de columnas.                             |
   | Resultado vacío                          | Confirmar si el recorte era esperado o si los filtros son demasiado restrictivos. |
   | Dataset leído desde persistencia         | Revalidar si se requiere conformidad formal.                                      |
   | Dataset inferido desde traces            | Validar trips antes de usarlo como entrada crítica para flows.                    |

## Ejemplo breve

Un patrón mínimo de inspección puede ser:

```python
print("ok:", report.ok)
print("summary:", report.summary)

for issue in report.issues[:10]:
    print(issue.level, issue.code, issue.field, issue.row_count)

print("is_validated:", dataset.metadata.get("is_validated"))

if dataset.metadata.get("events"):
    print("last op:", dataset.metadata["events"][-1]["op"])
    print("last issues:", dataset.metadata["events"][-1]["issues_summary"])
```

La idea no es revisar todo manualmente en cada ejecución, sino contar con una rutina corta para entender si el resultado es confiable, si necesita revalidación o si requiere una operación correctiva.

## Enlaces relacionados

* [Operaciones de Pylondrina](../operations/index.md)
* [OP-02 Validate trips](../operations/trips/op02_validate_trips.md)
* [OP-03 Fix trips correspondence](../operations/trips/op03_fix_trips_correspondence.md)
* [OP-04 Clean trips](../operations/trips/op04_clean_trips.md)
* [OP-05 Filter trips](../operations/trips/op05_filter_trips.md)
* [Bundles `.golondrina`](../golondrina/bundles.md)
* [Guía Trips to flows](../user-guide/trips-to-flows.md)
* [Guía Persistence and viewer](../user-guide/persistence-and-viewer.md)
