
# Estructura del repositorio

Esta página describe la organización general del repositorio de Pylondrina. Su objetivo es servir como mapa de orientación para ubicar el código fuente, la documentación, los datos, los notebooks, los scripts auxiliares, las pruebas y el visualizador.

No corresponde a un inventario exhaustivo de archivos. Para detalles de uso, API u operaciones específicas, se deben consultar las secciones especializadas de la documentación.

## Vista general

El repositorio separa los componentes principales del proyecto según su rol dentro del flujo de trabajo.

```text
pylondrina/
  src/pylondrina/        # Código fuente de la librería
  docs/                  # Documentación MkDocs
  tests/                 # Suite de pruebas pytest
  notebooks/             # Demos, experimentos y caso de estudio
  data/                  # Datos, artefactos y salidas de apoyo
  scripts/               # Scripts auxiliares
  experiments/           # Experimentos reproducibles
  viewer_src/            # Código fuente del visualizador
  viewer/                # Build estática del visualizador
```

La separación permite distinguir entre el core instalable de Pylondrina, los artefactos usados para reproducibilidad, los notebooks de demostración y el componente web auxiliar de inspección de flows.

## Código fuente

El código fuente principal se encuentra en:

```text
src/pylondrina/
```

Esta carpeta contiene la librería Python importable. Allí se implementan las operaciones públicas de Pylondrina v1.1 y las estructuras transversales usadas por el módulo.

Entre sus bloques principales se encuentran:

* importación, validación y corrección de trips;
* limpieza y filtrado de trips;
* construcción, exportación, filtrado y persistencia de flows;
* importación, validación e inferencia desde traces;
* datasets, schemas, reportes, errores e issues;
* perfiles y helpers para fuentes específicas.

El detalle técnico de funciones, clases y objetos públicos se documenta en la [Referencia API](../api/index.md). El propósito y uso de cada operación se documenta en [Operaciones](../operations/index.md).

## Documentación

La documentación del proyecto se encuentra en:

```text
docs/
```

Esta carpeta contiene las páginas MkDocs del proyecto. Su función es explicar el contrato Golondrina, el uso de Pylondrina, las operaciones disponibles, la referencia API, el visualizador y aspectos de desarrollo.

La documentación se organiza por capas:

* **Golondrina**: contrato de datos, representaciones y bundles.
* **Operaciones**: explicación operación por operación.
* **Guía de usuario**: pipelines completos usando varias operaciones.
* **API**: referencia técnica generada desde docstrings.
* **Viewer**: uso del visualizador y registro de datasets.
* **Desarrollo**: prácticas de pruebas y trabajo técnico sobre el repositorio.

Para preparar un entorno local, revisar [Instalación](../getting-started/installation.md).

## Tests

La suite de pruebas se encuentra en:

```text
tests/
```

Los tests están organizados principalmente por operación, con carpetas para OP-01 a OP-16 y pruebas adicionales para componentes core.

Su función es verificar el comportamiento implementado en Pylondrina v1.1, incluyendo:

* operaciones públicas;
* reportes e issues;
* metadata y eventos;
* persistencia formal;
* lectura y escritura de artefactos;
* construcción y filtrado de flows;
* inferencia desde traces;
* recorridos de integración.

La documentación general de la suite se encuentra en [Tests](../development/testing.md).

## Datos y artefactos

La carpeta de datos se encuentra en:

```text
data/
```

Esta carpeta agrupa datos de apoyo, entradas para demos, salidas derivadas y artefactos usados por el visualizador.

Puede contener, según el entorno local o la versión del repositorio:

* datos públicos o versionados;
* datos no publicados por tamaño, licencia o procedencia interna;
* outputs de demos;
* artefactos de caso de estudio;
* bundles `.golondrina`;
* layouts exportados para visualización;
* `viewer_registry.json`, usado por el selector del viewer.

La carpeta `data/flows/` cumple un rol especial porque conecta resultados de Pylondrina con el visualizador. Allí pueden convivir bundles formales de flows, layouts externos tipo Flowmap y el registro de datasets usado por la interfaz web.

## Notebooks

Los notebooks se encuentran en:

```text
notebooks/
```

Se usan como material de demostración, exploración, reproducción de experimentos y desarrollo del caso de estudio.

Dentro del proyecto, los notebooks cumplen principalmente tres roles:

1. mostrar pipelines reales con datasets heterogéneos;
2. documentar recorridos end-to-end de uso de Pylondrina;
3. sostener análisis reproducibles, como el caso de estudio EOD y experimentos de persistencia.

Los notebooks no reemplazan la API pública ni la documentación de operaciones. Funcionan como evidencia práctica y como ejemplos completos de uso.

## Scripts y experimentos

Los scripts auxiliares se encuentran en:

```text
scripts/
```

Esta carpeta contiene utilidades que apoyan tareas específicas del repositorio, por ejemplo:

* generación del registro de datasets del viewer;
* generación de datos sintéticos;
* perfiles de fuentes;
* adaptaciones de apoyo para demos o experimentos.

No todos los scripts deben interpretarse como interfaz pública estable. Algunos existen para reproducibilidad, otros para desarrollo o preparación de datos.

Los experimentos técnicos se ubican en:

```text
experiments/
```

En particular, el experimento de formatos de persistencia se organiza bajo:

```text
experiments/persistence_formats/
```

Ese bloque contiene scripts para generar casos experimentales, ejecutar una corrida individual y ejecutar una matriz completa de runs.

## Visualizador

El visualizador web se divide en dos carpetas:

```text
viewer_src/
viewer/
```

`viewer_src/` contiene el código fuente del visualizador. Se usa para desarrollo, modificación y generación de la build estática.

`viewer/` contiene la build estática generada. Esta es la versión que se sirve localmente para inspeccionar flows desde el navegador.

El visualizador es un componente auxiliar de inspección. No reemplaza las operaciones del core, no valida datasets y no reconstruye pipelines. Su función es permitir la exploración visual de flows previamente construidos, exportados o persistidos.

Para más detalles, revisar [Uso del visualizador](../viewer/usage.md).

## Recomendación de recorrido

Para entender el repositorio desde cero, se recomienda seguir este orden:

1. preparar el entorno local con [Instalación](../getting-started/installation.md);
2. revisar el contrato y las representaciones en la sección Golondrina;
3. entender las operaciones disponibles en [Operaciones](../operations/index.md);
4. consultar firmas y objetos públicos en la [Referencia API](../api/index.md);
5. ejecutar o revisar notebooks de demostración;
6. revisar la suite de [Tests](../development/testing.md);
7. usar el [Visualizador](../viewer/usage.md) cuando existan flows disponibles.

## Enlaces relacionados

* [Instalación](../getting-started/installation.md)
* [Operaciones](../operations/index.md)
* [Referencia API](../api/index.md)
* [Tests](../development/testing.md)
* [Uso del visualizador](../viewer/usage.md)