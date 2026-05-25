# Flows

La representación de **flows** describe flujos origen-destino agregados. A diferencia de trips y traces, un flow no es una observación primaria de movilidad, sino un producto analítico derivado desde un conjunto de movements o trips mediante una regla explícita de agregación.

En Pylondrina, los flows se materializan como `FlowDataset`.

## Qué es un flow

Un flow representa una relación agregada entre un origen y un destino. En v1.1, esa relación se construye principalmente sobre celdas H3 de origen y destino.

Un flow puede responder preguntas como:

- cuántos viajes se observaron entre dos zonas;
- cuál es la masa ponderada entre dos celdas H3;
- qué flujos aparecen para un grupo específico;
- qué corredores OD dominan un subconjunto del dataset;
- qué trips sustentan un flujo agregado.

## Flow como producto derivado

Un flow depende de:

- un dataset de trips de entrada;
- una unidad espacial de agregación;
- una resolución H3;
- una regla de conteo o ponderación;
- posibles segmentaciones categóricas;
- posibles ventanas temporales.

Por ello, un flow siempre debe interpretarse junto con su especificación de agregación y su provenance. No basta con mirar la tabla final.

## Núcleo canónico de flows

El contrato interno de flows en Pylondrina v1.1 usa estos campos mínimos:

| Campo | Descripción |
|---|---|
| `flow_id` | Identificador único del flujo agregado. |
| `origin_h3_index` | Celda H3 de origen. |
| `destination_h3_index` | Celda H3 de destino. |
| `flow_count` | Número de movements agregados. |
| `flow_value` | Magnitud analítica del flujo. |

Cuando existe agregación temporal, se agregan:

| Campo | Descripción |
|---|---|
| `window_start_utc` | Inicio de la ventana temporal del flujo. |
| `window_end_utc` | Término de la ventana temporal del flujo. |

Cuando existe segmentación, se agregan columnas homónimas a los campos usados en `group_by`.

## `flow_count` y `flow_value`

Golondrina distingue dos magnitudes:

| Campo | Semántica |
|---|---|
| `flow_count` | Conteo crudo de movements que componen el flujo. |
| `flow_value` | Magnitud analítica del flujo. Si existe `trip_weight`, corresponde a suma ponderada; si no, cae al conteo. |

Esta distinción permite que un mismo flow conserve tanto el número de registros agregados como la magnitud analítica usada para interpretar demanda, expansión o intensidad.

## Agregación espacial

Los flows se construyen sobre pares OD de celdas H3:

```text
origin_h3_index -> destination_h3_index
```

La resolución H3 de salida queda definida por la operación de construcción. Si los trips de entrada tienen una resolución más fina, la construcción puede hacer roll-up hacia una resolución más gruesa. En cambio, no se debe interpretar una resolución más fina como recuperable automáticamente si la información original no la contiene.

La resolución efectiva debe quedar registrada en la metadata y en la especificación de agregación del `FlowDataset`.

## Agregación temporal

Los flows pueden construirse sin dimensión temporal o con ventanas temporales explícitas.

En v1.1, la agregación temporal se apoya en temporalidad absoluta de trips. Por ello, requiere que los datos de entrada tengan tiempos comparables, normalmente `origin_time_utc` y `destination_time_utc`.

Las ventanas temporales pueden representarse con:

- `window_start_utc`;
- `window_end_utc`.

La dimensión temporal no debe entenderse como una etiqueta informal, sino como una ventana persistible dentro del contrato del flow.

## Segmentación mediante `group_by`

La construcción de flows puede segmentar por campos adicionales. Por ejemplo:

- `mode`;
- `purpose`;
- `user_gender`;
- `day_type`;
- cualquier campo compatible presente en el `TripDataset`.

Cuando se usa `group_by`, cada combinación de valores forma parte de la llave efectiva del flow. Esto permite comparar flujos por grupo sin perder trazabilidad sobre la regla de agregación usada.

## `flow_to_trips`

Pylondrina puede mantener una tabla auxiliar `flow_to_trips`, que relaciona flows con los movements que los sustentan.

Su esquema mínimo es:

| Campo | Descripción |
|---|---|
| `flow_id` | Identificador del flow. |
| `movement_id` | Identificador del movement que contribuye al flow. |

Esta tabla es opcional, porque puede ser costosa en datasets grandes. Cuando existe, permite inspeccionar o explicar un flujo agregado mediante la operación flow → trips.

## FlowDataset

Un `FlowDataset` no contiene solo una tabla de flows. También puede incluir:

| Componente | Rol |
|---|---|
| `flows` | Tabla principal de flujos agregados. |
| `flow_to_trips` | Tabla auxiliar opcional de correspondencia. |
| `aggregation_spec` | Especificación de cómo se construyó el flow. |
| `provenance` | Relación con el dataset origen. |
| `metadata` | Estado, identidad y trazabilidad operativa. |
| `source_trips` | Referencia viva opcional en memoria. |

Esta estructura permite mantener separada la tabla agregada, la especificación analítica y la evidencia operacional.

## Diferencia entre FlowDataset y layout de visualización

Un `FlowDataset` es la representación interna de Pylondrina. Un layout de visualización es una salida externa derivada desde ese dataset.

Por ejemplo, `export_flows` puede producir archivos orientados a visualización tipo flowmap. Esa salida puede usar nombres, estructura o archivos distintos a los del contrato interno.

| Objeto | Rol |
|---|---|
| `FlowDataset` | Representación interna operable por Pylondrina. |
| Layout flowmap | Artefacto externo orientado a visualización. |
| Bundle `.golondrina` de flows | Persistencia formal interna reconstruible por `read_flows`. |

No debe confundirse exportación con persistencia formal. La exportación prepara artefactos para visualización; la persistencia formal permite reconstruir un `FlowDataset`.

## Relación con operaciones de Pylondrina

| Operación | Relación con flows |
|---|---|
| OP-08 `build_flows` | Construye un `FlowDataset` desde trips. |
| OP-09 `export_flows` | Exporta flows a un layout externo de visualización. |
| OP-10 `write_flows` | Persiste formalmente un `FlowDataset`. |
| OP-11 `read_flows` | Reconstruye un `FlowDataset` desde un bundle `.golondrina`. |
| OP-12 `filter_flows` | Selecciona subconjuntos de flows. |
| OP-13 `get_trips_from_flows` | Recupera correspondencia flow → trips. |

## Estado de validación

Los flows son objetos derivados. En v1.1, la construcción de flows no debe interpretarse como una validación formal del dataset resultante. El `FlowDataset` conserva metadata y provenance que permiten entender cómo fue construido, pero no reemplaza la validación de trips de entrada ni una certificación semántica independiente.

## Ejemplo conceptual

Un conjunto de trips puede agregarse así:

```text
TripDataset validado
        |
        | build_flows
        v
FlowDataset
        |
        | export_flows
        v
Layout externo para visualización
```

También puede persistirse formalmente:

```text
FlowDataset
        |
        | write_flows
        v
Bundle .golondrina
        |
        | read_flows
        v
FlowDataset reconstruido
```

## Resumen

Los flows representan el puente entre datos OD individuales y productos analíticos agregados. Su contrato interno distingue conteo, magnitud analítica, espacialidad H3, segmentación y temporalidad. Además, permite conectar resultados agregados con los trips que los sustentan cuando se conserva `flow_to_trips`.