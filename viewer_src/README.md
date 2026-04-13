# Visualizador web de flujos

## Propósito

Esta carpeta contiene el código fuente del visualizador web del proyecto, usado para explorar flujos OD de manera nativa dentro del repositorio.

El visualizador se desarrolla en esta carpeta (`viewer_src/`) y su build estática final se genera por separado en `viewer/`.

Este componente debe entenderse como una herramienta complementaria del repositorio para inspección y demostración de resultados. El foco principal de Pylondrina v1.1 sigue siendo la construcción, validación, exportación y persistencia reproducible de datasets Golondrina, mientras que la visualización se incorpora aquí como soporte práctico del proyecto. :contentReference[oaicite:2]{index=2} :contentReference[oaicite:3]{index=3}

## Origen

Este visualizador se construyó como una adaptación del ejemplo público:

- Repositorio base: [flowmap.gl-purejs-example](https://github.com/ilyabo/flowmap.gl-purejs-example)

A partir de esa base, se extendió el viewer para integrarlo al repositorio del proyecto, agregar selección de datasets, soporte de formatos del proyecto y una interfaz de uso más coherente con Pylondrina.

## Capacidades actuales

El visualizador actualmente permite:

1. visualizar flujos OD como flowmap interactivo;
2. seleccionar datasets desde una vista jerárquica tipo explorador;
3. cargar datasets en formato **Flowmap layout**:
   - `flows.csv`
   - `locations.csv`
4. cargar datasets de flujos en formato **Golondrina**:
   - `flows.parquet`
5. convertir en el navegador datasets Golondrina a la estructura requerida por Flowmap;
6. advertir explícitamente cuando un dataset contiene **flujos segmentados** no soportados por el viewer actual;
7. volver desde la vista del mapa al selector de datasets para cargar otro artefacto.

## Formatos soportados

### 1. Flowmap layout

Dataset externo orientado a visualización, compuesto por:

- `flows.csv`
- `locations.csv`

Este es el layout utilizado por herramientas tipo Flowmap Blue / Flowmap City, y corresponde al formato de salida de `export_flows`. :contentReference[oaicite:4]{index=4} :contentReference[oaicite:5]{index=5}

### 2. Golondrina flows

Dataset persistido en formato interno de flujos Golondrina, compuesto principalmente por:

- `flows.parquet`

El viewer espera al menos las columnas mínimas de flujos Golondrina:

- `flow_id`
- `origin_h3_index`
- `destination_h3_index`
- `flow_count`
- `flow_value`

La conversión visual utiliza `origin_h3_index` y `destination_h3_index` como nodos, y `flow_value` como magnitud del flujo, manteniendo consistencia con el contrato del bloque `build_flows` / `export_flows`. :contentReference[oaicite:6]{index=6}

## Limitaciones actuales

- El viewer está pensado para **flujos no segmentados**.
- Si un dataset trae columnas extra que indican segmentación, el visualizador muestra una advertencia antes de continuar.
- El soporte actual de Golondrina está enfocado en `flows.parquet`; no interpreta todavía el sidecar como fuente primaria de configuración ni soporta filtros analíticos avanzados por segmentación.
- El selector utiliza un `viewer_registry.json` previamente generado; no explora el árbol de directorios de manera libre desde el navegador.

## Estructura del proyecto

- `app/main.js`: bootstrap del viewer.
- `app/config.js`: constantes, textos y configuración visual inicial.
- `app/state.js`: estado compartido mínimo del viewer.
- `app/data/`: carga de datasets, lectura de registry y transformación de formatos.
- `app/map/`: inicialización del mapa, capa Flowmap y actualización de render.
- `app/ui/`: selector, paneles, tooltips, overlays, warning y controles.
- `app/utils/`: helpers puros reutilizables.
- `app/styles/`: estilos estructurales y visuales del viewer.

## Registro de datasets (`viewer_registry.json`)

El selector de datasets no explora directorios directamente desde el navegador. En su lugar, consume un archivo de registro jerárquico:

- `data/flows/viewer_registry.json`

Este archivo enumera los datasets válidos disponibles bajo `data/flows/`, organizados como árbol de carpetas y datasets seleccionables.

## Antes de ejecutar el visualizador

Cada vez que cambien los datasets disponibles en `data/flows/`, se debe regenerar el archivo `viewer_registry.json`.

### Cómo generar el registro

Desde la raíz del repo:

```bash
python scripts/generate_viewer_registry.py
```

Opcionalmente, con otra profundidad máxima de escaneo:

```bash
python scripts/generate_viewer_registry.py --max-depth 6
```

## Cómo ejecutar el visualizador

### Modo desarrollo

Ejecutar desde `pylondrina/viewer_src/`:

```
yarn
yarn dev
```

### Generar build

Ejecutar desde `pylondrina/viewer_src/`:

```
yarn build
```

La build se genera en:

```
pylondrina/viewer/
```

## Cómo probar la build estática

La build debe servirse desde la **raíz del repositorio**, ya que el viewer sigue cargando datasets desde `/data/flows/...`.

Desde `pylondrina/`:

```
python -m http.server 8000
```

Luego visitar:

```
http://localhost:8000/viewer/
```

## Dependencias relevantes del viewer

El visualizador usa, entre otras, estas piezas principales:

- `flowmap.gl`
- `deck.gl`
- `maplibre-gl`
- `lil-gui`
- `parquet-wasm`
- `apache-arrow`
- `h3-js`

## Futuras extensiones sugeridas

- selector y filtros de segmentación;
- soporte directo para Feather;
- lectura más rica de metadata Golondrina;
- mejoras de UX para inspección de datasets grandes;
- controles visuales adicionales y ayuda contextual más detallada.