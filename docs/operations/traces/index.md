# Operaciones sobre traces

Este bloque agrupa las operaciones públicas que trabajan con `TraceDataset` y con la inferencia austera de trips desde puntos espacio-temporales discretos.

En términos de pipeline, este bloque puede leerse así:

```text
import traces → validate traces → infer trips
```

## Rol del bloque

Las operaciones sobre traces permiten incorporar fuentes basadas en puntos discretos, como check-ins, stay-points o registros puntuales de presencia. El objetivo no es reconstruir trayectorias densas ni resolver movilidad avanzada, sino estructurar puntos mínimos, validar su conformidad básica y derivar viajes OD simples compatibles con el resto de Pylondrina.

Este bloque actúa como una puerta de entrada alternativa hacia el pipeline de trips:

```text
TraceDataset
    ↓
TripDataset derivado
    ↓
validación, filtrado, persistencia o construcción de flows
```

## Alcance de v1.1

El soporte de traces en v1.1 es deliberadamente acotado.

Sí cubre:

- importación de puntos discretos;
- materialización del núcleo mínimo de traces;
- validación mínima de campos, tipos, constraints y monotonicidad temporal;
- inferencia austera de trips entre puntos o clusters consecutivos;
- derivación de un `TripDataset` compatible con el pipeline de trips y flows.

No cubre:

- GPS denso;
- map matching;
- reconstrucción continua de trayectorias;
- inferencia multimodal;
- detección avanzada de estadías;
- reglas de negocio específicas por fuente.

## Operaciones incluidas

| Operación | Función principal | Rol en el pipeline |
|---|---|---|
| [OP-14 Import traces](op14_import_traces.md) | `import_traces_from_dataframe` | Construye un `TraceDataset` desde una tabla de puntos. |
| [OP-15 Validate traces](op15_validate_traces.md) | `validate_traces` | Certifica conformidad mínima del dataset de traces. |
| [OP-16 Infer trips from traces](op16_infer_trips_from_traces.md) | `infer_trips_from_traces` | Deriva un `TripDataset` desde traces discretas. |

## Orden recomendado de lectura

Para entender el bloque completo, se recomienda leer las operaciones en este orden:

1. [OP-14 Import traces](op14_import_traces.md)
2. [OP-15 Validate traces](op15_validate_traces.md)
3. [OP-16 Infer trips from traces](op16_infer_trips_from_traces.md)

Después de OP-16, el resultado ya pertenece al bloque de trips. Por ello, puede pasar a operaciones como:

- [OP-02 Validate trips](../trips/op02_validate_trips.md);
- [OP-05 Filter trips](../trips/op05_filter_trips.md);
- [OP-06 Write trips](../trips/op06_write_trips.md);
- [OP-08 Build flows](../flows/op08_build_flows.md).

## Núcleo mínimo de traces

El bloque de traces trabaja sobre un núcleo canónico pequeño:

| Campo | Rol |
|---|---|
| `point_id` | Identificador del punto. |
| `user_id` | Identificador del usuario, dispositivo o entidad observada. |
| `time_utc` | Instante del punto. |
| `latitude` | Latitud en EPSG:4326. |
| `longitude` | Longitud en EPSG:4326. |

OP-14 intenta construir este núcleo desde una fuente tabular. OP-15 verifica su conformidad mínima. OP-16 usa ese núcleo para derivar movements OD simples.

## Relación con trips y flows

OP-16 es el puente entre traces y trips. Su salida es un `TripDataset`, no un `TraceDataset` enriquecido. Por eso, una vez inferidos los trips, el dataset resultante puede recorrer el pipeline general de trips y flows.

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

## Referencia API

Los detalles técnicos de firmas, parámetros, opciones y tipos de retorno se documentan en la [referencia API de traces](../../api/traces.md).