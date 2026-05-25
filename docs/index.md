# Pylondrina

Esta documentación presenta **Golondrina** y **Pylondrina**, dos componentes complementarios para trabajar con datos heterogéneos de movilidad urbana de forma estructurada, trazable y reproducible.

**Golondrina** es un contrato de datos unificado para representar viajes, trazas discretas y flujos origen-destino. No corresponde a un formato físico de archivo, sino a una forma común de organizar campos, reglas, dominios y metadatos para que distintas fuentes puedan interoperar.

**Pylondrina** es una librería Python importable que opera sobre ese contrato. En su versión v1.1, implementa un catálogo de operaciones para importar, validar, corregir, limpiar, filtrar, construir flows, exportar, persistir e inferir trips desde traces discretas.

## Qué permite hacer

Pylondrina v1.1 permite construir pipelines reproducibles sobre datos de movilidad urbana. Entre sus capacidades principales se encuentran:

- importar fuentes tabulares heterogéneas hacia `TripDataset` o `TraceDataset`;
- validar conformidad formal mediante reportes estructurados;
- limpiar y filtrar datasets sin perder trazabilidad;
- construir `FlowDataset` desde trips;
- exportar flows a layouts externos de visualización;
- persistir trips y flows en bundles `.golondrina`;
- reconstruir datasets persistidos mediante sidecars;
- inferir trips simples desde puntos espacio-temporales discretos;
- inspeccionar flows mediante un visualizador web auxiliar.

El foco de v1.1 es un MVP implementado y acotado. No se plantea como plataforma ETL generalista, suite completa de visualización ni sistema avanzado de inferencia sobre trayectorias GPS densas.

## Recorrido recomendado

Si se está revisando el proyecto por primera vez, se recomienda seguir este orden:

1. [Instalación](getting-started/installation.md), para preparar el entorno.
2. [Quickstart](getting-started/quickstart.md), para ejecutar un flujo mínimo.
3. [Visión general de Golondrina](golondrina/overview.md), para entender el contrato de datos.
4. [Guía de usuario](user-guide/index.md), para revisar pipelines completos.
5. [Operaciones](operations/index.md), para consultar una operación específica.
6. [Referencia API](api/index.md), para ver firmas, opciones y objetos públicos.
7. [Uso del visualizador](viewer/usage.md), para inspeccionar flows disponibles.
8. [Estructura del repositorio](repository/structure.md), para ubicar datos, notebooks, scripts y código fuente.

## Secciones de la documentación

| Sección | Qué contiene |
|---|---|
| [Primeros pasos](getting-started/installation.md) | Instalación y uso inicial de Pylondrina. |
| [Golondrina](golondrina/overview.md) | Contrato de datos, representaciones principales y bundles `.golondrina`. |
| [Operaciones](operations/index.md) | Manual de las operaciones públicas OP-01 a OP-16. |
| [Guía de usuario](user-guide/index.md) | Pipelines completos que combinan varias operaciones. |
| [Visualizador](viewer/usage.md) | Uso del viewer local y registro de datasets disponibles. |
| [API](api/index.md) | Referencia técnica generada desde docstrings con `mkdocstrings`. |
| [Repositorio](repository/structure.md) | Organización del repositorio, datos, notebooks y scripts. |
| [Desarrollo](development/testing.md) | Descripción general de la suite de tests. |

## Pipeline general

El flujo principal de Pylondrina puede leerse como una cadena de operaciones reproducibles:

```text
fuente externa
    ↓
importación
    ↓
validación
    ↓
corrección / limpieza / filtrado
    ↓
construcción de flows
    ↓
exportación / persistencia / visualización
```

Para fuentes basadas en puntos discretos, el flujo agrega una etapa previa de traces:

```text
puntos espacio-temporales
    ↓
import traces
    ↓
validate traces
    ↓
infer trips from traces
    ↓
validate trips / build flows
```

Cada etapa deja evidencia operacional mediante reportes, issues, metadata, eventos o sidecars, según corresponda.

## Componentes principales

| Componente            | Rol                                                                 |
| --------------------- | ------------------------------------------------------------------- |
| `TripDataset`         | Representa viajes o movements OD operables bajo Golondrina.         |
| `TraceDataset`        | Representa puntos espacio-temporales discretos.                     |
| `FlowDataset`         | Representa flujos OD agregados derivados desde trips.               |
| Reportes e issues     | Registran evidencia estructurada de ejecución.                      |
| Metadata y eventos    | Conservan trazabilidad dentro de los datasets.                      |
| Bundles `.golondrina` | Materializan persistencia formal de trips y flows.                  |
| Viewer                | Permite inspeccionar visualmente flows ya construidos o exportados. |

## Diferencias clave

La documentación mantiene algunas separaciones importantes del diseño:

* importar no equivale a validar;
* validar no corrige datos;
* corregir correspondencias no reemplaza una nueva validación;
* limpiar y filtrar son operaciones drop-only;
* construir flows no equivale a exportarlos;
* persistir no es lo mismo que exportar a visualización;
* leer un bundle `.golondrina` no certifica conformidad automáticamente;
* el viewer es un componente auxiliar de inspección, no parte del core operacional.

Estas fronteras permiten que los pipelines sean más claros, auditables y reproducibles.

## Siguiente paso

Para comenzar a usar el módulo, revisar [Instalación](getting-started/installation.md) y luego ejecutar el recorrido de [Quickstart](getting-started/quickstart.md).


