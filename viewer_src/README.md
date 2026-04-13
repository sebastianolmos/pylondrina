# Visualizador web de flujos - área de trabajo

## Propósito

Esta carpeta contiene el código fuente del visualizador web usado para explorar flujos OD de manera nativa dentro del repositorio del proyecto.

Su propósito es servir como área de desarrollo del visualizador. El entregable estático final se generará por separado en la carpeta `viewer/`.

## Origen

Este visualizador parte como una adaptación basada en el ejemplo público:

- Repositorio base: [flowmap.gl-purejs-example](https://github.com/ilyabo/flowmap.gl-purejs-example)


## Objetivo final esperado

Se busca obtener un visualizador web estático que:

1. permita visualizar flujos OD como flowmap;
2. incorpore un menú básico para elegir datasets;
3. soporte primero datasets en layout Flowmap (`locations.csv` + `flows.csv`);
4. y posteriormente permita cargar datos de flujos en formato Golondrina de manera directa.

## Estado actual

- [x] Copiar/adaptar el ejemplo base en esta carpeta
- [x] Instalar dependencias y verificar ejecución en modo desarrollo
- [x] Verificar generación de build estática
- [x] Probar visualización de un dataset hardcodeado
- [x] Reemplazar dataset remoto por dataset local
- [x] Agregar menú básico de selección de dataset
- [ ] Evaluar soporte directo para formato Golondrina

## Estructura propuesta

```text
viewer_src/
  index.html
  package.json
  vite.config.js
  app/
    main.js
    config.js
    state.js
    data/
    map/
    ui/
    utils/
    styles/
```

## Responsabilidades por carpeta

- `app/main.js`: bootstrap del viewer.
- `app/config.js`: constantes, textos y configuración visual inicial.
- `app/state.js`: estado compartido mínimo del viewer.
- `app/data/`: carga de datasets y detección de segmentación.
- `app/map/`: inicialización del mapa, capa Flowmap y actualización de render.
- `app/ui/`: paneles, tooltips, warning overlay y controles lil-gui.
- `app/utils/`: helpers puros reutilizables.
- `app/styles/`: estilos estructurales y visuales del viewer.

## Futuras extensiones sugeridas

- Lectura nativa de `flows.golondrina`: `app/data/loadGolondrinaArtifact.js`
- Selector/filtros de segmentación: `app/ui/segmentationPanel.js`

## Notas

- Esta carpeta contiene el proyecto fuente del visualizador.
- La carpeta `viewer/` contendrá la build final generada.
- Los datasets que requieran ser visualizados debe estar dentro del directorio `data/flows`


## Antes de ejecutar el visualizador

Para poder utilizar el selector de datasets de flujos, se debe generar un registro de los datasets que hay en `data/flows/`, por lo que se debe asegurar de tener este registro actualizado antes de usar el visualizador.

### Como generar el registro
Desde la raíz del repo:

```
python scripts/generate_viewer_registry.py
```

Con otra profundidad:

```
python scripts/generate_viewer_registry.py --max-depth 6
```

## Como ejecutar el visualizador

### En modo dev
Ejecutar desde `pylondrina/viewer_src/`
```
yarn
yarn dev
```

### Generar build
Ejecutar desde `pylondrina/viewer_src/`
```
yarn build
```

Luego para visualizar la build, ejecutar desde la raiz del proyecto, es decir, desde `pylondrina/` ejecutar :
```
python -m http.server 8000
```

Finalmente visitar:
```
http://localhost:8000/viewer/
```