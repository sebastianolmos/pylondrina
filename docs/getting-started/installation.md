# Instalación

Esta página describe cómo preparar un entorno local para instalar y usar **Pylondrina** desde el repositorio del proyecto. La instalación recomendada para el desarrollo y reproducción de ejemplos es una instalación editable dentro de un environment de Conda.

## Requisitos

Se requiere contar con:

- Python 3.10 o superior.
- Conda o un gestor equivalente de entornos virtuales.
- Git, si se clonará el repositorio desde GitHub.
- Acceso local al repositorio `pylondrina`.
- Dependencias Python necesarias para el core y para las operaciones que se quieran ejecutar.

El paquete se instala como una librería Python importable. Por ello, los scripts, notebooks y pruebas deben ejecutarse desde el mismo environment donde se instaló Pylondrina.

## Crear o activar un environment

Si ya existe un environment para el proyecto, basta con activarlo:

```bash
conda activate nombre_del_entorno
```

Por ejemplo:

```bash
conda activate pylondrina
```

Si se desea crear uno nuevo:

```bash
conda create -n pylondrina python=3.11
conda activate pylondrina
```

## Instalar Pylondrina en modo editable

Desde la raíz del repositorio:

```bash
python -m pip install -U pip
python -m pip install -e .
```

La instalación editable permite modificar el código del repositorio y usar esos cambios inmediatamente desde scripts, notebooks o pruebas, sin reinstalar el paquete cada vez.

Esta modalidad es la recomendada para el desarrollo de Pylondrina y para reproducir los ejemplos del repositorio.

## Instalar dependencias operacionales

El paquete base instala las dependencias declaradas en el proyecto. Sin embargo, algunas operaciones del repositorio usan librerías adicionales para indexación espacial, persistencia, experimentos, notebooks o visualización.

Para ejecutar el flujo principal de trips, flows y persistencia se recomienda instalar, al menos:

```bash
python -m pip install numpy pandas h3 pyarrow pyproj
```

Para trabajar con notebooks o análisis exploratorio:

```bash
python -m pip install jupyter ipykernel matplotlib
```

Si se utilizarán notebooks o scripts que dependan de herramientas geoespaciales adicionales, puede ser necesario instalar dependencias como `geopandas` o librerías asociadas al entorno geográfico usado.

## Verificar la instalación

Después de instalar el paquete, se puede verificar que Python resuelve correctamente los imports:

```bash
python -c "import pylondrina; print(pylondrina.__version__)"
```

También se puede verificar la importación de operaciones principales:

```bash
python -c "from pylondrina.importing import import_trips_from_dataframe; print('import ok')"
python -c "from pylondrina.validation import validate_trips; print('validation ok')"
python -c "from pylondrina.transforms.flows import build_flows; print('flows ok')"
```

Si estos comandos fallan con `ModuleNotFoundError`, normalmente significa que el environment activo no es el mismo donde se instaló Pylondrina, o que falta alguna dependencia operacional.

## Uso en notebooks

Cuando se trabaja con Jupyter o VSCode, el notebook debe usar el kernel asociado al mismo environment donde se instaló Pylondrina.

La regla práctica es:

1. Activar el environment.
2. Instalar Pylondrina con `python -m pip install -e .`.
3. Abrir el notebook usando ese mismo kernel.
4. Importar desde `pylondrina` sin modificar `sys.path`.

No se recomienda agregar rutas manualmente con `sys.path.append(...)`, porque eso vuelve menos reproducible el entorno de ejecución.

## Instalar herramientas de documentación

La documentación del proyecto se construye con MkDocs y Material for MkDocs. Para trabajar localmente con la documentación:

```bash
python -m pip install mkdocs-material "mkdocstrings[python]"
```

Luego, desde la raíz del repositorio:

```bash
mkdocs serve
```

Esto levanta una vista local de la documentación en:

```text
http://127.0.0.1:8000/
```

Para construir la documentación estática:

```bash
mkdocs build
```

El resultado se genera en la carpeta `site/`.

## Visualizador web

El repositorio incluye una build estática del visualizador web en `viewer/`. Para usarla localmente, se puede levantar un servidor HTTP simple desde la raíz del repositorio:

```bash
python -m http.server 8000
```

Luego se accede desde el navegador a:

```text
http://localhost:8000/viewer/
```

Si se agregan o actualizan datasets de flows en `data/flows/`, se debe regenerar el registro del visualizador:

```bash
python scripts/generate_viewer_registry.py
```

## Problemas frecuentes

### `ModuleNotFoundError: No module named 'pylondrina'`

El paquete no está instalado en el environment activo. Se debe activar el environment correcto y ejecutar:

```bash
python -m pip install -e .
```

### `ModuleNotFoundError: No module named 'h3'`

Falta una dependencia usada por operaciones espaciales o por la derivación de índices H3. Se puede instalar con:

```bash
python -m pip install h3
```

### Errores al escribir Parquet o Feather

La persistencia tabular requiere soporte de Arrow/Parquet. Se recomienda instalar:

```bash
python -m pip install pyarrow
```

### El notebook no encuentra el paquete, pero la terminal sí

El notebook probablemente está usando otro kernel. Se debe seleccionar el kernel asociado al environment donde se instaló Pylondrina.

### Se ejecutó un archivo interno del paquete directamente

No se recomienda ejecutar archivos internos como `src/pylondrina/importing.py` directamente. El uso esperado es importar el paquete desde scripts, notebooks o tests externos al paquete.
