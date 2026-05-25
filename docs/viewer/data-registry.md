# Registro de datasets del viewer

El visualizador de Pylondrina no explora libremente el sistema de archivos desde el navegador. En su lugar, carga un archivo de registro llamado `viewer_registry.json`, que enumera los datasets de flows disponibles para inspección visual.

Este registro permite que el selector del viewer muestre carpetas, datasets y formatos soportados de manera explícita y reproducible.

## Para qué sirve

El registro conecta los artefactos disponibles en `data/flows/` con el selector del visualizador.

En términos simples:

```text
data/flows/
    ↓
scripts/generate_viewer_registry.py
    ↓
data/flows/viewer_registry.json
    ↓
viewer
```

Sin este archivo, el viewer no puede saber qué datasets debe mostrar en la pantalla de selección.

## Cuándo regenerarlo

El registro debe regenerarse cada vez que cambien los datasets disponibles bajo `data/flows/`.

Casos típicos:

- se exportan nuevos flows con `export_flows`;
- se escriben nuevos bundles de flows con `write_flows`;
- se elimina una carpeta de resultados;
- se cambia el nombre de un artefacto;
- se agregan datasets de demostración;
- se actualizan archivos `flows.csv`, `locations.csv`, `flows.parquet` o `flows.feather`.

Desde la raíz del repositorio:

```bash
python scripts/generate_viewer_registry.py
```

El resultado se escribe por defecto en:

```text
data/flows/viewer_registry.json
```

## Requisitos del script

El script usa `pyarrow` para inspeccionar archivos Parquet y Feather sin depender del viewer. Si la dependencia no está instalada, el script termina con error y muestra un mensaje de instalación.

Instalación con `pip`:

```bash
pip install pyarrow
```

o con `conda`:

```bash
conda install pyarrow
```

## Comando básico

La forma recomendada de uso es ejecutar el script desde la raíz del repositorio:

```bash
python scripts/generate_viewer_registry.py
```

Con los valores por defecto, el script usa:

| Parámetro | Valor por defecto |
|---|---|
| Raíz del repositorio | carpeta padre de `scripts/` |
| Carpeta escaneada | `data/flows/` |
| Archivo de salida | `data/flows/viewer_registry.json` |
| Profundidad máxima | `10` |

## Opciones disponibles

El script permite ajustar rutas y profundidad de escaneo:

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

`--verbose` es útil cuando un directorio no aparece en el selector y se necesita revisar si fue omitido por ambigüedad o porque no cumplía la firma mínima esperada.

## Formatos detectados

El registry puede registrar tres formatos de dataset.

| Formato en el registry | Qué representa |
|---|---|
| `flowmap_layout` | Layout externo con `flows.csv` y `locations.csv`. |
| `golondrina_parquet` | Bundle o carpeta de flows Golondrina con `flows.parquet`. |
| `golondrina_feather` | Bundle o carpeta de flows Golondrina con `flows.feather`. |

Estos nombres son los que usa internamente el viewer para decidir qué loader aplicar.

## Detección de Flowmap layout

Un directorio se reconoce como `flowmap_layout` cuando contiene exactamente un CSV interpretable como archivo de flows y exactamente un CSV interpretable como archivo de locations.

El archivo de flows debe contener al menos:

```text
origin
dest
count
```

El archivo de locations debe contener al menos:

```text
id
lat
lon
```

Si una carpeta contiene candidatos ambiguos, por ejemplo más de un CSV que podría ser flows o más de un CSV que podría ser locations, el script omite esa carpeta en vez de adivinar.

Un ejemplo de layout válido es:

```text
data/flows/demo/baseline/
  flows.csv
  locations.csv
  metadata.json
```

El `metadata.json` es opcional para la detección, pero se registra si existe.

## Detección de flows Golondrina

Un archivo Parquet o Feather se reconoce como tabla principal de flows Golondrina si contiene al menos:

```text
flow_id
origin_h3_index
destination_h3_index
flow_count
flow_value
```

Si el directorio contiene un sidecar, también se registra:

```text
flows.metadata.json
```

Si existe una tabla auxiliar compatible, también puede registrarse:

```text
flow_to_trips.parquet
```

o:

```text
flow_to_trips.feather
```

La tabla `flow_to_trips` se reconoce por la firma mínima:

```text
flow_id
movement_id
```

Un ejemplo de bundle válido es:

```text
data/flows/case_study/work_all_day_mujer.golondrina/
  flows.feather
  flows.metadata.json
```

y, si corresponde:

```text
data/flows/case_study/work_all_day_mujer.golondrina/
  flows.feather
  flow_to_trips.feather
  flows.metadata.json
```

## Estructura del registry

El archivo generado tiene una estructura jerárquica. En el nivel superior incluye metadata del registro:

```json
{
  "version": "1.0",
  "generated_at_utc": "2026-04-25T23:40:40Z",
  "root_label": "flows",
  "root_path": "/data/flows",
  "max_scan_depth": 10,
  "root": {}
}
```

El bloque `root` contiene un árbol de carpetas y datasets. Los nodos de carpeta tienen esta forma:

```json
{
  "type": "directory",
  "name": "case_study_gender_work",
  "path": "/data/flows/case_study_gender_work",
  "children": []
}
```

Los nodos de dataset tienen esta forma:

```json
{
  "type": "dataset",
  "id": "feather__case_study_gender_work__work_all_day_mujer.golondrina__flows",
  "label": "work_all_day_mujer.golondrina",
  "format": "golondrina_feather",
  "dataset_path": "/data/flows/case_study_gender_work/work_all_day_mujer.golondrina",
  "files": {
    "flows": "flows.feather",
    "metadata": "flows.metadata.json"
  }
}
```

El viewer usa:

- `type` para distinguir carpetas y datasets;
- `label` o `name` para mostrar nombres en el selector;
- `format` para decidir qué loader usar;
- `dataset_path` y `files` para construir las rutas de carga.

## Relación con el viewer

Al iniciar, el viewer solicita:

```text
/data/flows/viewer_registry.json
```

Luego muestra `registry.root` como árbol navegable en el selector.

Cuando el usuario selecciona un dataset:

- si `format="flowmap_layout"`, se cargan `flows.csv` y `locations.csv`;
- si `format="golondrina_parquet"`, se carga el archivo `flows.parquet`;
- si `format="golondrina_feather"`, se carga el archivo `flows.feather`.

En datasets Golondrina, el viewer construye las locations desde los índices H3 y usa `flow_value` como magnitud visual principal.

## Buenas prácticas

Se recomienda:

- ejecutar el script después de generar nuevos artefactos en `data/flows/`;
- no editar manualmente `viewer_registry.json` salvo para diagnóstico puntual;
- conservar juntos los archivos de cada dataset;
- evitar carpetas con múltiples candidatos ambiguos a `flows.csv` o `locations.csv`;
- mantener nombres estables para carpetas y bundles usados en demos;
- revisar `--verbose` si un dataset esperado no aparece en el selector.

## Problemas frecuentes

### El selector no muestra un dataset nuevo

Probablemente falta regenerar el registry:

```bash
python scripts/generate_viewer_registry.py
```

También puede ocurrir que el dataset no cumpla la firma mínima de columnas esperada.

### El viewer no carga y muestra error sobre `viewer_registry.json`

El archivo puede no existir o el servidor puede no estar levantado desde la raíz del repositorio.

La forma recomendada para probar la build estática es:

```bash
python -m http.server 8000
```

desde la raíz del repo, y luego abrir:

```text
http://localhost:8000/viewer/
```

### Un directorio con CSV no aparece

Para `flowmap_layout`, el script requiere una detección no ambigua:

- exactamente un CSV con columnas `origin`, `dest`, `count`;
- exactamente un CSV con columnas `id`, `lat`, `lon`.

Si hay más de un candidato para alguno de esos roles, la carpeta se omite.

### Un bundle Golondrina no aparece

El archivo principal debe ser Parquet o Feather y contener al menos:

```text
flow_id
origin_h3_index
destination_h3_index
flow_count
flow_value
```

Además, si se usa Parquet o Feather, el entorno que ejecuta el script debe tener `pyarrow` instalado.

## Relación con otras páginas

- Para aprender a usar el visualizador, revisar [Uso del visualizador](usage.md).
- Para entender la diferencia entre persistir, exportar y visualizar, revisar [Persistencia, exportación y visualización](../user-guide/persistence-and-viewer.md).
- Para revisar operaciones de construcción, exportación y persistencia de flows, consultar [Operaciones Trip → Flow](../operations/flows/index.md).
- Para detalles del script, revisar la ruta fuente `scripts/generate_viewer_registry.py`.