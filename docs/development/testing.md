# Tests

La suite de pruebas de Pylondrina v1.1 verifica el comportamiento del módulo a nivel de operaciones públicas, helpers internos relevantes, contratos observables y recorridos de integración.

La organización física de los tests sigue principalmente la estructura del catálogo operacional: cada operación tiene su propia carpeta de pruebas. Sin embargo, esta página presenta la suite por **tipo de prueba**, no como inventario archivo por archivo. Esto permite entender el rol de los tests sin convertir la documentación en una lista difícil de mantener.

## Propósito de la suite

La suite cumple cuatro propósitos principales:

1. verificar que las operaciones públicas respeten su contrato de entrada y salida;
2. proteger decisiones de diseño implementadas, como `Import != Validate`, persistencia formal con sidecars y separación entre build/export/write/read;
3. comprobar que los reportes, issues, metadata y eventos se produzcan de forma consistente;
4. asegurar que los pipelines principales de trips, flows y traces funcionen de extremo a extremo.

Las pruebas no buscan demostrar que Pylondrina resuelve todo problema de movilidad urbana. Su propósito es verificar el comportamiento implementado para el alcance v1.1.

## Cómo ejecutar los tests

Desde la raíz del repositorio:

```bash
python -m pytest tests -q
```

Para ejecutar una carpeta específica:

```bash
python -m pytest tests/op08_build_flows -q
```

Para ejecutar un archivo específico:

```bash
python -m pytest tests/op02_validate_trips/test_integration_validate_trips.py -q
```

Para ver más detalle de ejecución:

```bash
python -m pytest tests -v
```

!!! note "Uso de `python -m pytest`"
    Se recomienda ejecutar pytest como módulo de Python para reducir diferencias entre entornos y asegurar que se use el intérprete activo del ambiente de trabajo.

## Organización general

La suite se organiza en carpetas por operación:

```text
tests/
  core/
  op01_import_trips/
  op02_validate_trips/
  op03_fix_trips_correspondence/
  op04_clean_trips/
  op05_filter_trips/
  op06_write_trips/
  op07_read_trips/
  op08_build_flows/
  op09_export_flows/
  op10_write_flows/
  op11_read_flows/
  op12_filter_flows/
  op13_get_trips_from_flows/
  op14_import_traces/
  op15_validate_traces/
  op16_infer_trips/
```

Esta organización facilita mantener cada bloque cerca de la operación que verifica. Aun así, conceptualmente la suite se entiende por niveles:

- smoke tests;
- helper-level tests;
- integration tests;
- public contract / failure tests;
- regression tests.

## Tipos de tests

### Smoke tests

Los smoke tests verifican que una operación pública pueda ejecutarse en un escenario mínimo esperado.

Normalmente revisan que:

- la función pública sea importable;
- la llamada básica retorne el tipo de objeto esperado;
- el reporte tenga una forma mínima válida;
- no se rompa el camino feliz más simple.

Estos tests son útiles como primera alerta cuando un cambio rompe la superficie pública de una operación.

Ejemplo de nombres frecuentes:

```text
test_op01_public_smoke.py
test_op08_public_smoke.py
test_op16_public_smoke.py
```

### Helper-level tests

Los helper-level tests verifican piezas internas relevantes para la semántica de una operación. Aunque los helpers no formen parte de la API pública, muchas decisiones importantes del módulo viven en estos bloques: normalización de parámetros, construcción de máscaras, serialización JSON-safe, detección de columnas, resolución de sidecars, staging, recuperación desde metadata o reconstrucción de llaves.

Estos tests ayudan a aislar errores y evitar que toda la verificación dependa solamente de pruebas end-to-end.

Ejemplo de focos cubiertos:

- correspondencias de campos y valores;
- coerción temporal;
- coordenadas y derivación H3;
- construcción de reportes y metadata;
- validación de constraints;
- construcción de máscaras de limpieza o filtrado;
- lectura de sidecars;
- sincronización de `flow_to_trips`;
- inferencia desde traces.

### Integration tests

Los integration tests verifican operaciones completas en escenarios más ricos. Su objetivo es observar el comportamiento público de una operación o bloque operacional considerando datos de entrada más realistas, metadata, reportes, eventos y efectos laterales esperados.

Estos tests son especialmente importantes para operaciones que producen artefactos o datasets derivados, por ejemplo:

- `import_trips_from_dataframe`;
- `validate_trips`;
- `write_trips` / `read_trips`;
- `build_flows`;
- `export_flows`;
- `write_flows` / `read_flows`;
- `filter_flows`;
- `get_trips_from_flows`;
- `infer_trips_from_traces`.

En estas pruebas no solo importa que exista una tabla de salida. También se verifica que el contrato observable quede consistente.

### Public contract y failure tests

Algunas operaciones incluyen pruebas orientadas explícitamente al contrato público y a fallas esperadas.

Estas pruebas verifican, por ejemplo:

- precondiciones de entrada;
- errores por configuración inválida;
- comportamiento con `strict=True` o `strict=False`;
- casos donde la operación debe abortar;
- casos donde la operación debe degradar con issue recuperable;
- estructura mínima de los reportes públicos.

Este tipo de prueba es relevante porque Pylondrina distingue entre errores fatales, warnings, errores recuperables y evidencia agregada en reportes.

### Regression tests

Los regression tests protegen comportamientos que fueron corregidos o estabilizados durante la implementación.

Su función no es cubrir toda una operación, sino evitar que reaparezcan errores conocidos, especialmente en bordes del contrato público o en interacciones entre metadata, reportes y datos.

## Qué se verifica en la suite

La suite verifica, entre otros aspectos:

- construcción de `TripDataset`, `TraceDataset` y `FlowDataset`;
- separación entre importación y validación;
- actualización de `metadata["is_validated"]`;
- reportes públicos y summaries;
- emisión de issues con niveles y códigos esperados;
- metadata y eventos operacionales;
- limpieza y filtrado drop-only;
- persistencia formal con bundles `.golondrina`;
- lectura formal desde sidecars;
- soporte de Parquet y Feather en persistencia;
- construcción de flows OD;
- exportación de flows a layout externo;
- filtrado de flows;
- recuperación `flow_to_trips`;
- importación y validación de traces;
- inferencia Trace → Trip;
- utilidades core de apoyo, como proyección de coordenadas.

## Qué garantías entregan

La suite entrega garantías prácticas sobre el comportamiento implementado en v1.1:

- las operaciones públicas principales pueden ejecutarse con entradas válidas;
- los errores de configuración más relevantes se detectan;
- las operaciones retornan datasets, reportes y tablas con estructura esperada;
- la trazabilidad mediante metadata, eventos y sidecars se mantiene estable;
- las operaciones drop-only no reinterpretan el contrato del dataset;
- las operaciones de persistencia reconstruyen artefactos formales bajo reglas explícitas;
- los pipelines principales de trips, flows y traces están cubiertos por pruebas de integración.

En conjunto, la suite funciona como una red de protección para mantener estable el MVP implementado.

## Qué no garantizan

La suite no debe interpretarse como una certificación absoluta del sistema.

En particular, no garantiza:

- cobertura exhaustiva de todas las combinaciones posibles de parámetros;
- validación semántica de cualquier fuente externa arbitraria;
- ausencia total de errores en datasets reales no vistos;
- rendimiento óptimo para todos los tamaños de datos;
- precisión de inferencia sobre trayectorias densas;
- compatibilidad con funcionalidades fuera del alcance v1.1.

Los tests verifican el contrato implementado, no convierten a Pylondrina en una plataforma ETL generalista ni en un sistema avanzado de inferencia de movilidad.

## Relación con escenarios de uso

Los escenarios de uso del diseño no son unidades de testeo. Los escenarios describen metas de actores y recorridos esperados dentro del sistema; los tests, en cambio, verifican comportamientos concretos del código.

La relación correcta es:

```text
escenarios de uso → justifican operaciones y pipelines
tests → verifican comportamiento implementado
```

Por eso, la suite está organizada por operación y tipo de prueba, no por escenario de uso.

## Buenas prácticas al agregar tests

Al extender la suite, se recomienda:

- ubicar los tests en la carpeta de la operación correspondiente;
- usar nombres de archivo que indiquen el nivel de prueba;
- mantener separados smoke, helper-level e integration tests;
- evitar asserts frágiles sobre detalles irrelevantes;
- preferir asserts sobre contrato observable: tipos, columnas, summaries, issues, eventos y metadata;
- usar fixtures en `conftest.py` cuando varios tests comparten datasets o artefactos;
- verificar explícitamente casos de error cuando la operación tenga política `strict`;
- no depender de orden accidental de columnas o filas salvo que sea parte del contrato probado.

## Ejecución recomendada antes de cambios

Antes de integrar cambios al core, se recomienda ejecutar:

```bash
python -m pytest tests -q
```

Si el cambio afecta una operación específica, primero puede ejecutarse la carpeta correspondiente:

```bash
python -m pytest tests/op05_filter_trips -q
```

Luego se recomienda ejecutar la suite completa para detectar efectos colaterales entre operaciones.

## Relación con otras secciones

- Para revisar el propósito de cada operación, consultar [Operaciones](../operations/index.md).
- Para consultar firmas y objetos públicos, revisar [Referencia API](../api/index.md).
- Para entender reportes, issues y trazabilidad, consultar [Issues, reportes y trazabilidad](../user-guide/issues-and-reports.md).