# `src/pylondrina/`

Código fuente principal de la librería **Pylondrina**.

## Propósito

Este directorio contiene el núcleo del módulo Python del proyecto Golondrina. Aquí se implementan las estructuras de datos, contratos, reportes y operaciones principales para trabajar con datasets de movilidad urbana en un formato unificado y trazable.

## Componentes principales

### Contrato base

- `schema.py`: definición de esquemas, campos y dominios.
- `datasets.py`: contenedores principales (`TripDataset`, `FlowDataset`, `TraceDataset`).
- `reports.py`: reportes estructurados e issues.
- `errors.py`: jerarquía de excepciones del módulo.
- `types.py`: aliases y tipos comunes.

### Operaciones sobre viajes/Trips

- `importing.py`: importación de viajes desde `DataFrame`.
- `validation.py`: validación formal de trips.
- `fixing.py`: corrección de correspondencias.
- `transforms/cleaning.py`: limpieza de trips.
- `transforms/filtering.py`: filtrado de trips.
- `io/trips.py`: escritura y lectura formal de datasets de viajes.

### Operaciones Trip -> Flow

- `transforms/flows.py`: construcción de flujos.

### Operaciones sobre flujos/Flows

- `transforms/flows_filtering.py`: filtrado de flujos.
- `export/flows.py`: exportación de flujos a layout externo.
- `io/flows.py`: persistencia formal de flows.
- `queries/flows.py`: consulta de correspondencia flujo-viajes.

### Trazas e inferencia (sin implementación)

- `importing_traces.py`: importación de trazas.
- `validation_traces.py`: validación básica de trazas.
- `transforms/inference.py`: inferencia básica de viajes a partir de trazas.

### Adaptación de fuentes y soporte interno

- `sources/`: perfiles y helpers para fuentes específicas.
- `issues/`: catálogos de codes e infraestructura del sistema de issues.

## Forma de uso esperada

La librería está organizada por módulos funcionales. En el estado actual del proyecto, el uso típico consiste en importar explícitamente desde el submódulo correspondiente, por ejemplo:

```python
from pylondrina.importing import import_trips_from_dataframe
from pylondrina.validation import validate_trips
from pylondrina.transforms.flows import build_flows
from pylondrina.io.flows import write_flows, read_flows
```