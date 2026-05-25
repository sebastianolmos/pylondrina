# Pylondrina

[![Documentación](https://img.shields.io/badge/docs-GitHub%20Pages-009688)](https://sebastianolmos.github.io/pylondrina/)
[![Python](https://img.shields.io/badge/python-3.12%2B-blue)](#instalación-rápida)
[![Estado](https://img.shields.io/badge/estado-MVP%20v1.1-555555)](#alcance)

![Visualizador de flujos de Pylondrina](images/pylondrina_viewer.gif)

Repositorio del proyecto **Golondrina / Pylondrina**, desarrollado como trabajo de título para apoyar la integración, validación, transformación, persistencia y visualización de datos heterogéneos de movilidad urbana.

## Documentación

La documentación completa está disponible en:

- Sitio web de documentación: <https://sebastianolmos.github.io/pylondrina/>
- Guía rápida: <https://sebastianolmos.github.io/pylondrina/getting-started/quickstart/>
- Referencia API: <https://sebastianolmos.github.io/pylondrina/api/>
- Operaciones: <https://sebastianolmos.github.io/pylondrina/operations/>
- Visualizador: <https://sebastianolmos.github.io/pylondrina/viewer/usage/>

## Qué es este proyecto

**Golondrina** es un contrato de datos unificado para representar información de movilidad urbana orientada al análisis origen-destino. Define representaciones, campos canónicos, reglas mínimas, dominios y metadata para trabajar con:

- viajes o desplazamientos OD (`trips` / `movements`);
- puntos espacio-temporales discretos (`traces`);
- flujos agregados (`flows`).

**Pylondrina** es la librería Python importable que opera sobre ese contrato. Su objetivo es reducir la fricción técnica al trabajar con fuentes heterogéneas que usan nombres de columnas, categorías, estructuras y niveles de granularidad distintos.

## Capacidades principales

Pylondrina v1.1 implementa un pipeline operacional para:

- importar trips desde tablas heterogéneas;
- validar conformidad formal mediante reportes estructurados;
- corregir correspondencias de campos y valores;
- limpiar y filtrar datasets de viajes;
- construir flows OD desde trips;
- exportar flows a layouts externos de visualización;
- persistir trips y flows en bundles `.golondrina`;
- leer artefactos persistidos mediante sidecars;
- filtrar flows y consultar correspondencias `flow -> trips`;
- importar y validar traces discretas;
- inferir trips simples desde traces;
- inspeccionar flows en un visualizador web local.

## Alcance

La versión actual corresponde a **Pylondrina v1.1**, un MVP implementado y acotado para el trabajo de título.

El proyecto no busca ser una plataforma ETL generalista, una suite completa de visualización ni un sistema avanzado de inferencia sobre trayectorias GPS densas. El foco está en ofrecer un contrato común, operaciones reproducibles y evidencia de ejecución mediante reportes, issues, metadata, eventos y sidecars.

## Instalación rápida

Desde la raíz del repositorio:

```bash
python -m pip install -e .
```

Para construir la documentación localmente:

```bash
python -m pip install -r requirements-docs.txt
mkdocs serve
```

## Uso mínimo

```python
from pylondrina.importing import import_trips_from_dataframe, ImportOptions
from pylondrina.validation import validate_trips
from pylondrina.transforms.flows import build_flows, FlowBuildOptions
from pylondrina.export.flows import export_flows, ExportFlowsOptions
```

Para ejemplos completos de uso, revisar la [guía rápida](https://sebastianolmos.github.io/pylondrina/getting-started/quickstart/) y los notebooks del repositorio.

## Estructura del repositorio

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

Más detalles en la página de [estructura del repositorio](https://sebastianolmos.github.io/pylondrina/repository/structure/).

## Visualizador

El repositorio incluye un visualizador web auxiliar para inspeccionar flows OD generados o persistidos por Pylondrina.

Para usarlo localmente:

```bash
python scripts/generate_viewer_registry.py
python -m http.server 8000
```

Luego abrir:

```text
http://localhost:8000/viewer/
```

La documentación específica está disponible en [Uso del visualizador](https://sebastianolmos.github.io/pylondrina/viewer/usage/).

## Demos y caso de estudio

El repositorio incluye notebooks de demostración sobre distintas fuentes de movilidad, además de un caso de estudio aplicado con datos de la Encuesta Origen-Destino de Santiago.

Estos notebooks muestran recorridos end-to-end de importación, validación, limpieza, construcción de flows, exportación, persistencia y visualización.

## Desarrollo y tests

La suite de pruebas se ejecuta con:

```bash
python -m pytest tests -q
```

La documentación de testing se encuentra en:

<https://sebastianolmos.github.io/pylondrina/development/testing/>

## Estado del proyecto

Este repositorio corresponde al estado final del MVP v1.1 desarrollado para la memoria de título. La documentación publicada en GitHub Pages es la referencia principal para entender el contrato Golondrina, las operaciones de Pylondrina, la API pública, el visualizador y la organización del repositorio.