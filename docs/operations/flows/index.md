# Operaciones Trip → Flow

Este bloque agrupa las operaciones públicas que trabajan con `FlowDataset`. Su objetivo es cubrir el paso desde viajes individuales hacia flujos OD agregados, y luego permitir exportarlos, persistirlos, filtrarlos o inspeccionarlos.

En términos de pipeline, este bloque puede leerse así:

```text
build flows → exportar / persistir / filtrar / consultar
```

## Rol del bloque

Las operaciones Trip → Flow conectan el contrato de trips con productos analíticos agregados. Primero se construye un `FlowDataset` desde un `TripDataset`; después ese dataset puede materializarse para visualización, persistirse como artefacto interno, filtrarse o inspeccionarse mediante la relación flow → trips.

Este bloque mantiene una frontera importante:

- OP-08 concentra la semántica de agregación.
- OP-09 exporta a un layout externo de visualización.
- OP-10 y OP-11 resuelven persistencia formal interna.
- OP-12 filtra flows ya construidos.
- OP-13 permite inspeccionar qué trips sustentan los flows.

## Operaciones incluidas

| Operación | Función principal | Rol en el pipeline |
|---|---|---|
| [OP-08 Build flows](op08_build_flows.md) | `build_flows` | Construye un `FlowDataset` desde un `TripDataset`. |
| [OP-09 Export flows](op09_export_flows.md) | `export_flows` | Exporta flows a un layout externo orientado a visualización. |
| [OP-10 Write flows](op10_write_flows.md) | `write_flows` | Persiste formalmente un `FlowDataset` como bundle `.golondrina`. |
| [OP-11 Read flows](op11_read_flows.md) | `read_flows` | Reconstruye un `FlowDataset` desde un bundle formal. |
| [OP-12 Filter flows](op12_filter_flows.md) | `filter_flows` | Selecciona subconjuntos de flows por atributos o celdas H3. |
| [OP-13 Get trips from flows](op13_get_trips_from_flows.md) | `get_trips_from_flows` | Recupera la correspondencia flow → trips cuando existe información suficiente. |

## Orden recomendado de lectura

Para entender el flujo completo, se recomienda leer las operaciones en este orden:

1. [OP-08 Build flows](op08_build_flows.md)
2. [OP-09 Export flows](op09_export_flows.md)
3. [OP-10 Write flows](op10_write_flows.md)
4. [OP-11 Read flows](op11_read_flows.md)
5. [OP-12 Filter flows](op12_filter_flows.md)
6. [OP-13 Get trips from flows](op13_get_trips_from_flows.md)

No todos los usos requieren las seis operaciones. Por ejemplo, un flujo exploratorio puede construir y exportar flows sin persistirlos. En cambio, un pipeline reproducible puede construir, escribir, leer y luego filtrar flows desde un bundle formal.

## Build, export y persistencia no son lo mismo

Este bloque separa tres responsabilidades distintas:

| Acción | Operación | Resultado |
|---|---|---|
| Construir flows | OP-08 | `FlowDataset` interno. |
| Exportar a visualización | OP-09 | Layout externo, como `flows.csv`, `locations.csv` y `metadata.json`. |
| Persistir internamente | OP-10 / OP-11 | Bundle `.golondrina` reconstruible por Pylondrina. |

Esta distinción evita confundir un layout de visualización con la persistencia formal del módulo.

## `flow_to_trips`

Al construir flows, Pylondrina puede conservar una tabla auxiliar `flow_to_trips`. Esta tabla permite explicar qué movements sustentan cada flujo agregado.

Cuando esa información existe, OP-13 puede recuperar la correspondencia directamente. Si no existe, la operación puede intentar reconstruirla desde un `TripDataset` compatible o desde `flows.source_trips`, cuando estén disponibles.

## Relación con trips

El bloque Trip → Flow depende de `TripDataset` como entrada analítica principal. Un flujo se interpreta correctamente solo si se conoce cómo fue construido: resolución H3, columnas de segmentación, regla temporal, uso de pesos y configuración efectiva de agregación.

```text
TripDataset
    ↓
OP-08 Build flows
    ↓
FlowDataset
```

## Referencia API

Los detalles técnicos de firmas, parámetros, opciones y tipos de retorno se documentan en la [referencia API de flows](../../api/flows.md).