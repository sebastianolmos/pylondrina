# Operaciones sobre trips

Este bloque agrupa las operaciones públicas que trabajan sobre `TripDataset`. Su objetivo es cubrir el ciclo base de trabajo sobre viajes o movements OD: importar, validar, corregir, limpiar, filtrar y persistir formalmente datasets de trips bajo el contrato Golondrina.

En términos de pipeline, este bloque puede leerse así:

```text
importar → validar → corregir / limpiar / filtrar → persistir
```

## Rol del bloque

Las operaciones sobre trips constituyen el centro operacional de Pylondrina v1.1. A partir de ellas se construyen datasets canónicos, se certifica conformidad, se preparan subconjuntos analíticos y se habilita la construcción posterior de flows.

Este bloque mantiene una frontera importante:

- OP-01 construye un dataset utilizable, pero no lo valida.
- OP-02 certifica conformidad, pero no corrige.
- OP-03 corrige correspondencias semánticas, pero no reemplaza validación.
- OP-04 y OP-05 eliminan o seleccionan filas, pero no reinterpretan el contrato.
- OP-06 y OP-07 persisten y reconstruyen datasets, pero no certifican conformidad.

## Operaciones incluidas

| Operación | Función principal | Rol en el pipeline |
|---|---|---|
| [OP-01 Import trips](op01_import_trips.md) | `import_trips_from_dataframe` | Construye un `TripDataset` desde una tabla externa. |
| [OP-02 Validate trips](op02_validate_trips.md) | `validate_trips` | Certifica conformidad formal del dataset. |
| [OP-03 Fix trips correspondence](op03_fix_trips_correspondence.md) | `fix_trips_correspondence` | Corrige mappings de campos o valores categóricos post-import. |
| [OP-04 Clean trips](op04_clean_trips.md) | `clean_trips` | Elimina filas problemáticas mediante reglas explícitas. |
| [OP-05 Filter trips](op05_filter_trips.md) | `filter_trips` | Selecciona subconjuntos por criterios atributivos, temporales o espaciales. |
| [OP-06 Write trips](op06_write_trips.md) | `write_trips` | Persiste formalmente un `TripDataset` como bundle `.golondrina`. |
| [OP-07 Read trips](op07_read_trips.md) | `read_trips` | Reconstruye un `TripDataset` desde un bundle formal. |

## Orden recomendado de lectura

Para entender el flujo completo, se recomienda leer las operaciones en este orden:

1. [OP-01 Import trips](op01_import_trips.md)
2. [OP-02 Validate trips](op02_validate_trips.md)
3. [OP-03 Fix trips correspondence](op03_fix_trips_correspondence.md)
4. [OP-04 Clean trips](op04_clean_trips.md)
5. [OP-05 Filter trips](op05_filter_trips.md)
6. [OP-06 Write trips](op06_write_trips.md)
7. [OP-07 Read trips](op07_read_trips.md)

No todos los pipelines usan todas las operaciones. Por ejemplo, un dataset ya corregido puede pasar directamente de importación a validación y persistencia. En cambio, una fuente real con problemas de completitud puede requerir limpieza, filtrado y revalidación antes de construir flows.

## Estados y trazabilidad

Las operaciones de este bloque usan `metadata["is_validated"]` como señal oficial de validación:

| Tipo de operación | Política general |
|---|---|
| Importación | Deja el dataset como no validado. |
| Validación | Puede marcar el dataset como validado si no hay errores. |
| Corrección semántica | Invalida si hubo cambio real; preserva el estado si fue NOOP. |
| Limpieza y filtrado | Preservan el estado validado en rutas retornables. |
| Lectura formal | Reconstruye desde disco, pero no certifica conformidad. |

Además, cada operación deja evidencia mediante reportes, issues y eventos cuando corresponde.

## Relación con flows

`TripDataset` es la entrada natural de [OP-08 Build flows](../flows/op08_build_flows.md). Por eso, antes de construir flows se recomienda contar con un dataset de trips importado, preparado y validado según las necesidades del análisis.

```text
TripDataset
    ↓
OP-08 Build flows
    ↓
FlowDataset
```

## Referencia API

Los detalles técnicos de firmas, parámetros, opciones y tipos de retorno se documentan en la [referencia API de trips](../../api/trips.md).