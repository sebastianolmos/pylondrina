# Scripts del repositorio

Esta página describe los scripts relevantes del repositorio Pylondrina. Su objetivo es orientar sobre qué scripts son necesarios para uso, cuáles apoyan reproducibilidad y cuáles corresponden a utilidades internas o temporales de desarrollo.

Los scripts documentados aquí no reemplazan la API pública de Pylondrina. La API estable del módulo se encuentra en `src/pylondrina/` y se documenta en la [Referencia API](../api/index.md).

## Criterio de clasificación

Los scripts se organizan en tres grupos:

| Categoría | Rol |
|---|---|
| Scripts necesarios para el usuario | Herramientas que deben ejecutarse para que un componente del proyecto funcione correctamente. |
| Scripts de apoyo o reproducibilidad | Utilidades para generar datos, preparar fuentes o reproducir experimentos. |
| Scripts internos o temporales | Archivos usados durante desarrollo, diagnóstico o pruebas puntuales. No forman parte de una interfaz estable. |

Esta clasificación evita presentar todos los scripts como si fueran parte de la superficie pública del módulo.

## Script requerido para el viewer

### `scripts/generate_viewer_registry.py`

Este script genera el registro de datasets usado por el visualizador web.

El viewer no explora libremente el sistema de archivos desde el navegador. En su lugar, lee:

```text
data/flows/viewer_registry.json
```

Ese archivo indica qué datasets están disponibles, en qué carpetas se encuentran y qué formato debe usar el viewer para cargarlos.

### Cuándo ejecutarlo

Debe ejecutarse cada vez que cambie el contenido disponible para visualización bajo `data/flows/`.

Casos típicos:

* se agregan nuevos flows exportados;
* se escriben nuevos bundles `.golondrina` de flows;
* se eliminan resultados antiguos;
* se renombran carpetas;
* se actualizan archivos `flows.csv`, `locations.csv`, `flows.parquet` o `flows.feather`;
* se agregan outputs nuevos de demos o del caso de estudio.

### Comando típico

Desde la raíz del repositorio:

```bash
python scripts/generate_viewer_registry.py
```

El resultado se escribe por defecto en:

```text
data/flows/viewer_registry.json
```

### Argumentos principales

El script acepta argumentos para ajustar la raíz del repositorio, la carpeta escaneada, la salida y la profundidad de búsqueda.

```bash
python scripts/generate_viewer_registry.py \
  --repo-root . \
  --data-root data/flows \
  --output data/flows/viewer_registry.json \
  --max-depth 10
```

También se puede activar salida de diagnóstico:

```bash
python scripts/generate_viewer_registry.py --verbose
```

| Argumento     | Uso                                                    |
| ------------- | ------------------------------------------------------ |
| `--repo-root` | Define la raíz del repositorio.                        |
| `--data-root` | Define la carpeta que se escanea para buscar datasets. |
| `--output`    | Define dónde se escribe `viewer_registry.json`.        |
| `--max-depth` | Controla la profundidad máxima del escaneo recursivo.  |
| `--verbose`   | Imprime mensajes de diagnóstico durante la detección.  |

### Entradas esperadas

El script escanea datasets bajo:

```text
data/flows/
```

Puede detectar tres familias de entrada:

| Formato detectado    | Archivos esperados                                        |
| -------------------- | --------------------------------------------------------- |
| `flowmap_layout`     | `flows.csv` y `locations.csv`                             |
| `golondrina_parquet` | `flows.parquet` y, opcionalmente, `flow_to_trips.parquet` |
| `golondrina_feather` | `flows.feather` y, opcionalmente, `flow_to_trips.feather` |

Para `flowmap_layout`, el archivo de flows debe tener al menos:

```text
origin
dest
count
```

y el archivo de locations debe tener al menos:

```text
id
lat
lon
```

Para flows Golondrina, la tabla principal debe tener al menos:

```text
flow_id
origin_h3_index
destination_h3_index
flow_count
flow_value
```

La tabla auxiliar `flow_to_trips`, cuando existe, se reconoce por:

```text
flow_id
movement_id
```

### Salidas generadas

La salida principal es:

```text
data/flows/viewer_registry.json
```

El archivo generado contiene metadata del registro y un árbol de carpetas/datasets. Los nodos de dataset incluyen información como:

* `id`;
* `label`;
* `format`;
* `dataset_path`;
* `files`.

El viewer usa esos campos para construir el selector de datasets y decidir qué loader aplicar.

### Errores comunes

#### Falta `pyarrow`

El script usa `pyarrow` para inspeccionar archivos Parquet y Feather. Si la dependencia no está instalada, el script no puede detectar datasets Golondrina en esos formatos.

Instalación con `pip`:

```bash
pip install pyarrow
```

o con `conda`:

```bash
conda install pyarrow
```

#### `data/flows/` no existe

Si la carpeta de entrada no existe o no es un directorio, el script termina con error. En ese caso, se debe revisar la ruta usada en `--data-root`.

#### El dataset no aparece en el selector

Las causas más frecuentes son:

* no se regeneró `viewer_registry.json`;
* el dataset no está bajo `data/flows/`;
* la carpeta tiene CSV ambiguos para `flowmap_layout`;
* faltan columnas mínimas;
* el archivo Parquet o Feather no contiene la firma esperada de flows Golondrina.

Para diagnóstico, se recomienda ejecutar:

```bash
python scripts/generate_viewer_registry.py --verbose
```

Para más detalles, consultar [Registro de datasets del viewer](../viewer/data-registry.md).

## Scripts de apoyo y reproducibilidad

### `scripts/synthetic_data/base_generator.py`

Este script contiene utilidades para generar DataFrames sintéticos de trips/movements.

Su función principal es producir datos controlados para pruebas, demos y validación de operaciones como importación, validación, limpieza, filtrado y construcción de flows.

La utilidad central es la generación de una tabla sintética de trips con parámetros configurables, por ejemplo:

* cantidad de filas;
* semilla de reproducibilidad;
* estructura temporal;
* formato de coordenadas;
* presencia o ausencia de H3;
* duplicados controlados;
* campos extra;
* nulos;
* ruido;
* corrupción de tipos;
* correspondencias de campos.

Este script es útil para construir escenarios repetibles y degradaciones controladas. Sin embargo, no forma parte del core público de Pylondrina. Su rol es apoyar desarrollo, pruebas y demostraciones, no definir una API estable para usuarios finales.

### `scripts/source_profiles/`

Esta carpeta contiene perfiles o factories para facilitar el trabajo con fuentes específicas.

En el repositorio se organiza en subcarpetas asociadas a fuentes como:

```text
scripts/source_profiles/
  factories_adatrap/
  factories_eod/
  factories_foursquare/
```

Estos scripts ayudan a preparar configuraciones, correspondencias o estructuras reutilizables para fuentes trabajadas durante el proyecto.

Su rol es práctico:

* reducir repetición en notebooks;
* centralizar configuraciones de fuentes;
* facilitar adaptación de EOD, ADATRAP y Foursquare;
* apoyar demos reproducibles.

Estos perfiles no reemplazan la lógica del core. Las operaciones públicas siguen estando en `src/pylondrina/`; los perfiles funcionan como apoyo para usar esas operaciones con fuentes concretas.

### `experiments/persistence_formats/`

Esta carpeta contiene scripts asociados al experimento de formatos de persistencia.

El experimento compara backends de persistencia, especialmente Parquet y Feather, usando datasets generados bajo configuraciones controladas.

Los scripts principales son:

| Script             | Rol                                                   |
| ------------------ | ----------------------------------------------------- |
| `generate_case.py` | Genera un caso o dataset experimental.                |
| `run_one.py`       | Ejecuta una corrida individual del experimento.       |
| `run_matrix.py`    | Ejecuta una matriz de configuraciones experimentales. |

Estos scripts permiten reproducir la generación de casos y mediciones del experimento. La interpretación de resultados, discusión y conclusiones no viven en esta página, sino en notebooks, resultados derivados o documentación de evaluación.

## Buenas prácticas

Al trabajar con scripts del repositorio, se recomienda:

* ejecutar los scripts desde la raíz del repositorio, salvo que indiquen otra cosa;
* revisar rutas relativas antes de correr scripts que leen o escriben archivos;
* no editar manualmente outputs generados, como `viewer_registry.json`, salvo para diagnóstico puntual;
* regenerar el registro del viewer después de modificar `data/flows/`;
* no tratar scripts auxiliares como reemplazo de operaciones públicas;
* mantener scripts de apoyo separados de la lógica del core;
* documentar solo scripts que aporten a uso, reproducibilidad o mantenimiento;
* evitar que scripts temporales aparezcan como parte de una interfaz estable.

## Enlaces relacionados

* [Registro de datasets del viewer](../viewer/data-registry.md)
* [Uso del visualizador](../viewer/usage.md)
* [Datos y notebooks](../repository/data-and-notebooks.md)
* [Tests](../development/testing.md)
