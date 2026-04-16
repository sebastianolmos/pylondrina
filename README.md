# Pylondrina

Repositorio del proyecto **Golondrina / Pylondrina** para la estandarización, validación, transformación, persistencia y visualización de datos de movilidad urbana.

## Qué es este proyecto

**Golondrina** es una propuesta de formato unificado para representar datos de movilidad urbana, con foco en viajes origen-destino (OD), trazas y flujos agregados, usando campos estándar, dominios categóricos y metadatos de trazabilidad.

**Pylondrina** es la librería Python del proyecto. Su objetivo es reducir la fricción técnica al trabajar con fuentes heterogéneas como encuestas OD, trazas, check-ins u otras tablas de movilidad que vienen con nombres de columnas, categorías y estructuras distintas.

En el estado actual del repositorio, el proyecto incluye:

- el **módulo core** instalable en `src/pylondrina/`,
- **datasets reales y sintéticos** de apoyo en `data/`,
- **notebooks** de pruebas y ejemplos de uso,
- **scripts auxiliares** para generación de datos y perfiles de fuente,
- y un **visualizador web** para explorar datasets de flujos.

## Estado actual del repositorio

El repositorio corresponde a una versión de trabajo de **Pylondrina v1.1**. El foco principal está en un pipeline reproducible para:

1. importar datos al formato Golondrina,
2. validar y transformar datasets,
3. construir flujos OD,
4. exportarlos o persistirlos,
5. visualizarlos en un viewer web incluido en el repositorio,
6. Además, se incorporó un bloque experimental para evaluar **Parquet** y **Feather** como backends de persistencia. A partir de ese experimento, **Feather** quedó adoptado como backend por defecto para persistencia, mientras que **Parquet** se mantiene como opción soportada, ya que en algunos escenarios pesados puede producir artefactos más compactos y además tiene mejor compatibilidad con herramientas externas.




## Estructura del repositorio

### `src/pylondrina/`
Contiene el **código fuente principal** de la librería instalable.

Los módulos más relevantes son:

- `importing.py`: importación de viajes desde `DataFrame` a `TripDataset`.
- `validation.py`: validación formal de viajes.
- `fixing.py`: corrección de correspondencias de campos y valores.
- `transforms/cleaning.py`: limpieza de viajes.
- `transforms/filtering.py`: filtrado de viajes.
- `transforms/flows.py`: construcción de flujos a partir de viajes.
- `transforms/flows_filtering.py`: filtrado de flujos.
- `export/flows.py`: exportación de flujos a layout externo orientado a visualización.
- `io/trips.py`: escritura y lectura formal de datasets de viajes.
- `io/flows.py`: escritura y lectura formal de datasets de flujos.
- `queries/flows.py`: consulta de correspondencia entre flujos y viajes.
- `importing_traces.py`, `validation_traces.py`, `transforms/inference.py`: soporte inicial para trazas e inferencia básica de viajes.
- `schema.py`, `datasets.py`, `reports.py`, `errors.py`, `types.py`: contrato base del módulo.
- `sources/`: perfiles y helpers para adaptar fuentes específicas.
- `issues/`: catálogos de issues y utilidades del sistema de reportes.

### `data/`
Contiene datos de apoyo para desarrollo y demostración.

Incluye:

- `EOD_STGO/`: archivos asociados a la Encuesta Origen-Destino.
- `ADATRAP/`: insumos y configuraciones asociadas a esa fuente.
- `Foursquare/`: insumos y catálogos para esa fuente.
- `synthetic/`: datasets sintéticos y salidas de demo.
- `flows/`: datasets de flujos listos para persistencia, exportación y visualización.

Dentro de `data/flows/` conviven, según el caso:

- datasets de flujos en formato interno Golondrina,
- layouts exportados para visualización tipo Flowmap,
- y el archivo `viewer_registry.json`, usado por el visualizador web para poblar el selector de datasets disponibles.

### `notebooks/`
Contiene notebooks Jupyter para pruebas y ejemplos.

Se divide principalmente en:

- `notebooks/testing/`: notebooks de pruebas por operación, incluyendo tests helper-level e integration tests para varias operaciones implementadas.
- `notebooks/usage_examples/`: ejemplos de uso y demos de pipeline para importación, validación, construcción de flujos e I/O.

### `scripts/`
Contiene utilidades auxiliares del repositorio.

Las carpetas más relevantes son:

- `synthetic_data/`: generadores de datasets sintéticos y scripts de demo.
- `source_profiles/`: factories y configuraciones de perfiles para fuentes como EOD, ADATRAP y Foursquare.
- `generate_viewer_registry.py`: script que inspecciona `data/flows/` y actualiza el archivo `viewer_registry.json` requerido por el visualizador web.

### `viewer_src/`
Contiene el **código fuente del visualizador web** de flujos.

Esta carpeta está pensada para desarrollo o modificación del viewer. Incluye su propio `README.md` con instrucciones específicas de build, ejecución y mantenimiento.

### `viewer/`
Contiene la **build estática** del visualizador web generada desde `viewer_src/`.

Para uso normal del visualizador no es necesario modificar esta carpeta manualmente.

### `experiments/`

Contiene código y documentación asociada a experimentos técnicos del proyecto.

Actualmente incluye `experiments/persistence_formats/`, donde se implementó y ejecutó el experimento comparativo entre **Parquet** y **Feather** como formatos de persistencia. En esa carpeta se encuentran el generador de casos experimentales, la ejecución de runs individuales y el orquestador de la matriz completa del experimento.

Para más detalles sobre la metodología, archivos involucrados y conclusión adoptada, ver:

- `experiments/persistence_formats/README.md`

El análisis completo de resultados e interpretación final se encuentra en:

- `notebooks/experiments/persistence_formats/analyze_persistence_experiment.ipynb`

## Instalación rápida del módulo

Desde la raíz del repositorio:

```bash
python -m pip install -U pip
python -m pip install -e .
```

Esto instala `pylondrina` como librería editable, que es la modalidad utilizada para el desarrollo del proyecto.

## Dependencias

El proyecto usa Python y pandas como base, pero varias operaciones y scripts del repositorio utilizan además librerías como `numpy`, `h3`, `pyproj`, `pyarrow` y, en algunos casos, `geopandas`.

Por esa razón, para ejecutar todo el repositorio de punta a punta conviene trabajar dentro de un entorno virtual o conda preparado para desarrollo, especialmente si se van a usar notebooks, exportación, persistencia Parquet, perfiles de fuente o el visualizador.

## Capacidades principales del core

A nivel funcional, el módulo actualmente provee soporte para:

- **importación de viajes** al formato Golondrina,
- **validación** de datasets de viajes,
- **corrección de correspondencias** de campos y valores,
- **limpieza y filtrado** de viajes,
- **construcción de flujos OD** a partir de viajes,
- **exportación de flujos** a layout externo para visualización,
- **persistencia formal de trips y flows** mediante artefactos con sidecar, con **Feather** como backend por defecto y **Parquet** como alternativa soportada,
- **consulta de correspondencia flujo-viajes**,
- y **soporte inicial para trazas** e inferencia básica de viajes.

## Ejemplo mínimo de imports

```python
from pylondrina.importing import import_trips_from_dataframe, ImportOptions
from pylondrina.validation import validate_trips, ValidationOptions
from pylondrina.transforms.flows import build_flows, FlowBuildOptions
from pylondrina.export.flows import export_flows, ExportFlowsOptions
from pylondrina.io.trips import write_trips, read_trips
from pylondrina.io.flows import write_flows, read_flows
```

## Datasets y salidas dentro del repo

El repositorio no contiene solo código. También se usa como espacio de trabajo para:

- mantener fuentes reales de referencia,
- guardar datasets sintéticos de prueba,
- persistir artefactos de trips y flows,
- conservar salidas de demos reproducibles,
- y dejar datasets listos para ser visualizados en el viewer.

En particular, `data/flows/` funciona como punto de encuentro entre el módulo core y el visualizador web.

## Visualizador web de flujos

El repositorio incluye un visualizador web para explorar datasets de flujos de manera interactiva.

El flujo de uso es el siguiente:

1. generar o actualizar datasets dentro de `data/flows/`,
2. regenerar el archivo `viewer_registry.json`,
3. levantar la build estática del viewer desde la raíz del repo.

### Actualizar el registro de datasets del viewer

Desde la raíz del repositorio:

```bash
python scripts/generate_viewer_registry.py
```

Opcionalmente:

```bash
python scripts/generate_viewer_registry.py --max-depth 6
```

### Ejecutar la build estática del viewer

Desde la raíz del repositorio:

```bash
python -m http.server 8000
```

Luego abrir en el navegador:

```text
http://localhost:8000/viewer/
```

### Desarrollo del viewer

Para modificar el visualizador, trabajar en `viewer_src/`.

Comandos habituales:

```bash
cd viewer_src
yarn
yarn dev
```

Para generar una nueva build:

```bash
cd viewer_src
yarn build
```

La documentación específica del visualizador se encuentra en `viewer_src/README.md`.

## Recomendación para revisar el repositorio

Para una revisión rápida del proyecto, el recorrido sugerido es:

1. leer este `README.md`,
2. revisar `src/pylondrina/` para ver el núcleo de la librería,
3. mirar `notebooks/usage_examples/` para ejemplos de uso,
4. revisar `notebooks/testing/` para ver pruebas por operación,
5. inspeccionar `data/flows/` y luego ejecutar el visualizador web.

## Observación final

Este repositorio combina implementación, experimentación y soporte de demostración. Por ello, conviven código fuente, datasets de trabajo, notebooks, scripts auxiliares y una interfaz visual. La organización actual busca que el proyecto sea reproducible y navegable, manteniendo en un mismo lugar tanto el núcleo del módulo como los insumos necesarios para entender su uso y estado de avance.
