# Operaciones de Pylondrina

Esta sección reúne el manual de uso de las operaciones públicas de **Pylondrina v1.1**. Las operaciones no se presentan como funciones aisladas, sino como un catálogo operacional organizado por familias de trabajo sobre datasets de movilidad urbana bajo el contrato Golondrina.

Cada operación documenta qué hace, cuándo usarla, qué recibe, qué retorna, qué evidencia deja y qué consideraciones debe tener el usuario al integrarla en un pipeline reproducible.

## Organización general

El catálogo operacional se organiza en tres familias:

| Familia | Operaciones | Dataset principal | Rol general |
|---|---:|---|---|
| [Trips](trips/index.md) | OP-01 a OP-07 | `TripDataset` | Entrada, validación, corrección, limpieza, filtrado y persistencia formal de viajes. |
| [Trip → Flow](flows/index.md) | OP-08 a OP-13 | `FlowDataset` | Construcción, exportación, persistencia, filtrado e inspección de flujos OD. |
| [Traces](traces/index.md) | OP-14 a OP-16 | `TraceDataset` y `TripDataset` derivado | Importación de puntos, validación mínima e inferencia austera de viajes. |

## Convenciones del catálogo

El diseño operacional de Pylondrina mantiene separadas responsabilidades que suelen confundirse en pipelines de datos:

- **Importar** construye un dataset utilizable, pero no certifica conformidad.
- **Validar** certifica conformidad, pero no corrige datos.
- **Corregir** ajusta correspondencias semánticas, pero no reemplaza la validación.
- **Limpiar** y **filtrar** eliminan o seleccionan filas, pero no reinterpretan el contrato del dataset.
- **Construir flows** deriva un objeto analítico agregado desde trips.
- **Exportar** transforma un contrato interno hacia un layout externo.
- **Persistir** guarda y reconstruye datasets internos mediante bundles formales.
- **Inferir** deriva trips desde traces, pero no equivale a importar una fuente de viajes.

Esta separación permite que cada paso deje evidencia propia y que el usuario pueda reconstruir qué ocurrió en cada etapa.

## Evidencia operacional

Las operaciones no se entienden solo por su efecto tabular. Según el caso, una operación puede producir o actualizar:

- un dataset de salida;
- un reporte estructurado;
- issues agregados;
- metadata persistible;
- eventos en `metadata["events"]`;
- sidecars en artefactos persistidos o exportados.

El reporte se mantiene pequeño y estable. La evidencia más detallada se concentra en `Issue.details`, metadata, eventos o sidecars, según corresponda.

## Recorridos principales

Pylondrina v1.1 permite al menos tres recorridos de uso frecuentes.

### Viajes directos

```text
OP-01 Import trips
        ↓
OP-02 Validate trips
        ↓
OP-03 / OP-04 / OP-05
        ↓
OP-06 / OP-07
```

Este recorrido se usa cuando la fuente ya representa viajes o movements OD, aunque requiera estandarización, validación, corrección o preparación posterior.

### Viajes a flujos

```text
TripDataset
        ↓
OP-08 Build flows
        ↓
OP-09 Export flows
        ↓
OP-10 / OP-11 / OP-12 / OP-13
```

Este recorrido se usa cuando el objetivo es construir flujos OD agregados, exportarlos a visualización, persistirlos o inspeccionar qué viajes sustentan un flujo.

### Trazas a viajes y flujos

```text
OP-14 Import traces
        ↓
OP-15 Validate traces
        ↓
OP-16 Infer trips from traces
        ↓
OP-02 Validate trips
        ↓
OP-08 Build flows
```

Este recorrido se usa cuando la fuente contiene puntos espacio-temporales discretos y se desea derivar viajes OD simples compatibles con el resto del pipeline.

## Índices por familia

- [Operaciones sobre trips](trips/index.md)
- [Operaciones Trip → Flow](flows/index.md)
- [Operaciones sobre traces](traces/index.md)

Para detalles técnicos de firmas, parámetros y tipos de retorno, se debe consultar la [referencia API](../api/index.md).