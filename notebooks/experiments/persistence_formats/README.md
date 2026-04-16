# Experimentos de persistencia: Parquet vs Feather

Esta carpeta contiene el código experimental usado para comparar **Parquet** y **Feather v2** como backends de persistencia para datasets Golondrina/Pylondrina.

El experimento se diseñó para el patrón real de uso del módulo: persistencia orientada principalmente a operaciones de **escritura y lectura** (`write_*` / `read_*`), considerando como artefacto persistido la combinación **datos + sidecar**.

## Objetivo

Evaluar ambos formatos en términos de:

- **correctitud del roundtrip**;
- **tiempo total** de escritura + lectura;
- **tamaño total** del artefacto persistido;
- **viabilidad operativa**, usando RAM como restricción.

## Estructura del experimento

Se evaluaron:

- **9 configuraciones** de dataset,
- **2 backends** (`parquet`, `feather`),
- **1 warm-up** por configuración/backend,
- **5 runs medidas** por configuración/backend.

En total se ejecutaron **108 runs**.

## Implementación

Esta carpeta contiene:

- `generate_case.py`  
  Generación reproducible de datasets experimentales.

- `run_one.py`  
  Ejecución de una run individual, con medición de tiempo, tamaño, RAM y verificación de fidelidad.

- `run_matrix.py`  
  Orquestación automática de toda la matriz experimental.

## Análisis de resultados

El procesamiento, análisis, interpretación y conclusión final del experimento se encuentran en:

`notebooks/experiments/persistence_formats/analyze_persistence_experiment.ipynb`

En ese cuaderno se calculan:

- medianas e IQR,
- comparación Feather vs Parquet por configuración,
- score por configuración,
- score global,
- chequeo de restricción de RAM,
- e interpretación final de resultados.

## Resultado general

El experimento mostró que:

- **Feather fue consistentemente más rápido** que Parquet en todas las configuraciones evaluadas;
- **Parquet fue mejor en almacenamiento** en algunos casos pesados;
- la restricción de RAM se cumplió para Feather.

Bajo la regla de decisión definida para el experimento, **Feather resultó ganador**.

## Decisión adoptada

A partir de estos resultados:

- **Feather queda como backend por defecto** del módulo;
- **Parquet se mantiene como backend soportado**.

Parquet sigue siendo útil en escenarios donde interesa priorizar:

- mayor compacidad en algunos casos pesados,
- mejor compatibilidad con herramientas externas,
- o facilidad de inspección/visualización con software de terceros.
