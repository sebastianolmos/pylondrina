# Datos y notebooks

Esta página describe el rol de las carpetas `data/` y `notebooks/` dentro del repositorio de Pylondrina. Su propósito es orientar al usuario sobre qué insumos, salidas y ejemplos existen, y cómo se relacionan con las demos, el caso de estudio, el visualizador y la reproducibilidad del proyecto.

No corresponde a un inventario exhaustivo de archivos. La disponibilidad exacta de algunos datasets puede depender del entorno local, permisos de uso, tamaño de los archivos o restricciones de publicación.

## Rol de `data/`

La carpeta `data/` agrupa insumos de trabajo, datos de apoyo, artefactos derivados y salidas generadas por pipelines de Pylondrina.

En términos generales, puede contener:

- datos fuente usados en demos o casos de estudio;
- archivos auxiliares para interpretar fuentes;
- salidas de notebooks;
- artefactos persistidos `.golondrina`;
- layouts exportados para visualización;
- datasets sintéticos;
- resultados de experimentos;
- flows disponibles para el visualizador.

La carpeta `data/` no debe interpretarse como un catálogo público completo de todos los datos usados durante el desarrollo. Algunas fuentes fueron usadas localmente, pero no se publican en GitHub por restricciones de tamaño, licencia, permisos o procedencia interna.

## Datasets públicos o versionados

### `data/EOD_STGO/`

La carpeta `data/EOD_STGO/` contiene datos de la Encuesta Origen-Destino de Santiago usados como fuente real en demos y en el caso de estudio.

Estos datos permiten trabajar con tablas relacionales como viajes, hogares, personas y etapas. En el proyecto se usan para demostrar que Pylondrina puede operar sobre una fuente real con identificadores relacionales, campos heterogéneos, categorías propias de la fuente, coordenadas, temporalidad y factores de expansión.

Esta fuente cumple un rol importante porque sostiene tanto demos generales como el caso de estudio sobre conectividad OD por género en viajes de trabajo.

### `data/ADATRAP/`

La carpeta `data/ADATRAP/` contiene archivos de apoyo versionados para trabajar con la fuente ADATRAP, como catálogos, dominios o configuraciones auxiliares.

Los datos completos usados localmente para demos de ADATRAP no necesariamente forman parte del repositorio público. Esta decisión evita publicar archivos cuya difusión puede estar restringida o cuya disponibilidad depende del contexto institucional del proyecto.

En la documentación y notebooks, ADATRAP se usa como una fuente real para demostrar pipelines sobre viajes resumidos y etapas de transporte público.

### `data/Foursquare/`

La carpeta `data/Foursquare/` contiene archivos auxiliares para interpretar datos Foursquare, como catálogos o agrupaciones de categorías.

Los datos originales de Foursquare no se almacenan completos en el repositorio. Para reproducir notebooks que dependan de esa fuente, los datos deben obtenerse desde la [fuente externa del dataset Foursquare](https://sites.google.com/site/yangdingqi/home/foursquare-dataset), respetando sus condiciones de uso y citación.

En el proyecto, Foursquare se usa como ejemplo de fuente de puntos discretos o check-ins, útil para demostrar el flujo de traces hacia trips.

### `data/synthetic/`

La carpeta `data/synthetic/` contiene o puede contener datasets sintéticos generados para pruebas, ejemplos o validación técnica controlada.

Estos datos no buscan representar una fuente real de movilidad urbana. Su función es facilitar escenarios reproducibles donde se conocen de antemano las condiciones de entrada, los campos disponibles y los casos de borde que se desea ejercitar.

## Datasets no versionados públicamente

Algunas fuentes usadas durante el desarrollo o en demos pueden estar disponibles solo en entornos locales autorizados. Esto incluye, según el caso:

```text
data/awto/
data/scooters/
data/telefonia/
```

Estas carpetas no deben asumirse como parte del repositorio público completo. Su ausencia en GitHub puede deberse a restricciones de publicación, tamaño, confidencialidad, permisos de uso o procedencia interna de los datos.

La documentación puede mencionar estas fuentes como parte de la validación práctica del módulo, pero sin presentar sus datos como descargables desde el repositorio público.

## Outputs y artefactos derivados

Además de datos fuente, `data/` contiene salidas producidas por notebooks, demos, experimentos o pipelines de Pylondrina.

### `data/demo_outputs/`

Esta carpeta reúne salidas generadas por notebooks de demostración. Su función es dejar evidencia material de pipelines ejecutados sobre distintas fuentes y operaciones del módulo.

Puede contener resultados intermedios, artefactos persistidos, exports y otros productos derivados de las demos.

### `data/case_study_outputs/`

Esta carpeta contiene salidas asociadas al caso de estudio EOD. Su rol es separar los resultados analíticos del caso respecto de los datos fuente.

Estas salidas pueden incluir tablas, métricas, artefactos de flows, visualizaciones o resultados derivados del pipeline aplicado al universo de viajes de trabajo.

### `data/experiments/persistence_formats/`

Esta carpeta contiene resultados o artefactos asociados al experimento de formatos de persistencia. El experimento compara backends como Parquet y Feather en términos de escritura, lectura, tamaño, fidelidad y costo práctico de uso.

La lógica de ejecución del experimento se documenta mediante scripts en `experiments/persistence_formats/`, mientras que esta carpeta concentra salidas o artefactos generados.

### `data/flows/`

La carpeta `data/flows/` funciona como punto de encuentro entre Pylondrina y el visualizador.

Puede contener:

* bundles `.golondrina` de flows;
* artefactos en Parquet o Feather;
* layouts exportados tipo Flowmap;
* outputs de demos;
* outputs del caso de estudio;
* `viewer_registry.json`.

El visualizador usa `data/flows/viewer_registry.json` para saber qué datasets puede mostrar en el selector. Por eso, cuando se agregan o modifican flows en esta carpeta, se debe regenerar el registro correspondiente.

Para más detalles, consultar [Registro de datasets del viewer](../viewer/data-registry.md).

## Rol de `notebooks/`

La carpeta `notebooks/` contiene material ejecutable de demostración, exploración y análisis aplicado.

Los notebooks cumplen tres roles principales:

1. mostrar pipelines completos de uso de Pylondrina;
2. conectar operaciones individuales en recorridos reproducibles;
3. documentar casos reales o semi-reales de trabajo con distintas fuentes de movilidad.

Los notebooks no reemplazan la API pública ni las páginas de operaciones. Su valor está en mostrar cómo se encadenan varias operaciones en un flujo concreto.

## Demos

La carpeta `notebooks/demo/` reúne notebooks de demostración sobre distintas fuentes y representaciones.

Entre las demos principales se encuentran:

```text
adatrap_trips.ipynb
adatrap_stages.ipynb
eod_trips.ipynb
eod_stages.ipynb
foursquare_traces.ipynb
telefonia_traces.ipynb
awto_traces.ipynb
scooters_trips.ipynb
```

Estas demos permiten observar el uso de Pylondrina en escenarios diversos:

* trips o viajes resumidos;
* etapas;
* traces discretas;
* inferencia Trace → Trip;
* construcción de flows;
* exportación;
* persistencia;
* inspección visual.

No todos los datos requeridos por estas demos están necesariamente versionados en GitHub. Cuando una fuente no esté disponible públicamente, el notebook debe leerse como evidencia del flujo ejecutado y no como garantía de reproducción completa desde cero.

## Caso de estudio EOD

El notebook principal del caso de estudio se encuentra en:

```text
notebooks/case_studies/eod_gender_work_trips.ipynb
```

Este notebook implementa un análisis descriptivo-comparativo sobre conectividad OD por género en viajes de trabajo, usando datos reales de la EOD de Santiago.

El caso cumple dos roles:

* funciona como análisis aplicado de movilidad urbana;
* demuestra que Golondrina y Pylondrina pueden sostener un pipeline reproducible sobre una fuente real, relacional y heterogénea.

El flujo del caso incluye preparación de tablas, importación a Golondrina, validación, limpieza, filtrado, construcción de flows, métricas descriptivas y visualización de resultados.

## Buenas prácticas

Al trabajar con `data/` y `notebooks/`, se recomienda:

* no asumir que todos los datasets usados localmente están disponibles en GitHub;
* separar datos fuente, outputs derivados y artefactos de visualización;
* mantener los resultados de demos en `data/demo_outputs/`;
* mantener los resultados del caso de estudio en `data/case_study_outputs/`;
* usar `data/flows/` para artefactos que deban ser visibles desde el viewer;
* regenerar `viewer_registry.json` después de agregar o modificar flows disponibles para visualización;
* no publicar datasets con restricciones de licencia, permisos o procedencia interna;
* documentar en cada notebook las rutas de datos esperadas y los supuestos mínimos de entrada.

## Enlaces relacionados

* [Construir flows desde trips](../user-guide/trips-to-flows.md)
* [Inferir trips desde traces](../user-guide/traces-to-trips.md)
* [Uso del visualizador](../viewer/usage.md)
* [Registro de datasets del viewer](../viewer/data-registry.md)
* [Tests](../development/testing.md)
