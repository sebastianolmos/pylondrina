# Bundles `.golondrina`

Un bundle `.golondrina` es el artefacto formal de persistencia usado por Pylondrina para materializar datasets internos en disco y reconstruirlos posteriormente.

No debe confundirse con el contrato Golondrina. El contrato define la semántica de los datos; el bundle `.golondrina` materializa un estado de un dataset mediante archivos tabulares y sidecar de metadata.

!!! warning "Distinción importante"
    Que un directorio tenga sufijo `.golondrina` no significa, por sí solo, que el contenido esté semánticamente validado. La conformidad depende del contenido, del schema persistido y de las operaciones de validación aplicadas.

## Qué problema resuelve

Sin una persistencia formal, un dataset puede quedar atrapado en memoria, en un notebook o en archivos tabulares difíciles de interpretar después. El bundle `.golondrina` busca resolver esto mediante un contenedor reproducible que conserva:

- tabla de datos;
- metadata operacional;
- schema o especificación de agregación;
- provenance;
- identidad lógica del dataset;
- identidad del artefacto persistido;
- backend físico usado;
- archivos que componen el artefacto.

## Contrato vs bundle

| Nivel | Descripción |
|---|---|
| Contrato Golondrina | Define campos, reglas y semántica de una representación. |
| Dataset Pylondrina | Objeto vivo en memoria que implementa una representación operable. |
| Bundle `.golondrina` | Materialización persistida de un dataset en disco. |

El bundle no reemplaza al contrato. Solo congela un estado persistible de un dataset para que pueda reconstruirse después.

## Estructura general

Un bundle `.golondrina` corresponde a un directorio. Su contenido depende del tipo de dataset y del backend físico usado.

La idea general es:

```text
dataset.golondrina/
  archivo_tabular.parquet o archivo_tabular.feather
  metadata.json específico del tipo de dataset
```

En v1.1, los bundles formales están implementados para:

- `TripDataset`;
- `FlowDataset`.

## Bundle de trips

La persistencia formal de trips usa un layout de este tipo:

```text
trips_example.golondrina/
  trips.feather
  trips.metadata.json
```

o bien:

```text
trips_example.golondrina/
  trips.parquet
  trips.metadata.json
```

El archivo tabular contiene los datos de trips. El sidecar `trips.metadata.json` contiene la información necesaria para interpretar y reconstruir formalmente el `TripDataset`.

## Bundle de flows

La persistencia formal de flows usa un layout de este tipo:

```text
flows_example.golondrina/
  flows.feather
  flows.metadata.json
```

o bien:

```text
flows_example.golondrina/
  flows.parquet
  flows.metadata.json
```

Cuando existe `flow_to_trips`, el bundle puede incluir además:

```text
flow_to_trips.feather
```

o:

```text
flow_to_trips.parquet
```

El archivo principal contiene la tabla de flows. El auxiliar, si existe, conserva la correspondencia entre flows y movements.

## Sidecar JSON

El sidecar es el archivo que permite interpretar formalmente el artefacto. Sin sidecar, la lectura no debe tratarse como lectura formal de Pylondrina.

Según el tipo de dataset, el sidecar puede incluir:

| Bloque | Rol |
|---|---|
| `dataset_type` | Tipo de dataset persistido. |
| `format` | Identificación del formato lógico del artefacto. |
| `layout_version` | Versión del layout persistido. |
| `storage` | Backend físico usado, como Feather o Parquet. |
| `dataset_id` | Identidad lógica del dataset. |
| `artifact_id` | Identidad de la materialización concreta. |
| `files` | Archivos que componen el bundle. |
| `schema` | Schema persistido, en trips. |
| `schema_effective` | Schema efectivo persistido, en trips cuando corresponde. |
| `aggregation_spec` | Especificación de agregación, en flows. |
| `provenance` | Procedencia o relación de derivación. |
| `metadata` | Metadata operacional persistible. |

## `dataset_id` y `artifact_id`

Pylondrina distingue dos identidades:

| Identidad | Significado |
|---|---|
| `dataset_id` | Identidad lógica del dataset. |
| `artifact_id` | Identidad de una escritura o materialización concreta. |

Un mismo dataset lógico puede escribirse más de una vez y generar distintos artefactos. Por eso, `dataset_id` y `artifact_id` no cumplen el mismo rol.

## Backends físicos

En v1.1, los bundles de trips y flows pueden usar dos backends físicos:

| Backend | Uso |
|---|---|
| Feather | Backend eficiente y preferido para la persistencia actual del módulo. |
| Parquet | Backend soportado, útil por compacidad e interoperabilidad con herramientas externas. |

El backend usado queda registrado en el sidecar. La lectura formal no se basa en adivinar el archivo, sino en reconstruir el dataset desde la metadata persistida.

## Lectura formal

Las operaciones `read_trips` y `read_flows` reconstruyen datasets desde bundles `.golondrina`.

Esta lectura es formal porque:

- exige sidecar obligatorio;
- valida estructura mínima del artefacto;
- reconstruye metadata y provenance;
- interpreta el backend desde el sidecar;
- reconstruye el objeto de Pylondrina correspondiente.

Sin embargo, leer un bundle no equivale a validar semánticamente su contenido. La validación debe ejecutarse mediante las operaciones de validación cuando corresponda.

## Diferencia con exportación

La persistencia formal y la exportación tienen propósitos distintos.

| Operación | Propósito |
|---|---|
| `write_trips` / `read_trips` | Persistir y reconstruir `TripDataset`. |
| `write_flows` / `read_flows` | Persistir y reconstruir `FlowDataset`. |
| `export_flows` | Generar un layout externo orientado a visualización. |

La exportación puede producir archivos útiles para un visualizador, pero no debe confundirse con un bundle interno reconstruible mediante `read_flows`.

## Relación con trazabilidad

Los bundles `.golondrina` materializan parte de la trazabilidad operacional del sistema. Permiten que la evidencia no quede solo en memoria o en notebooks.

Un bundle puede conservar:

- schema o especificación efectiva;
- metadata;
- eventos previos;
- provenance;
- backend usado;
- archivos generados;
- identidad lógica y de artefacto.

Esto permite reconstruir un dataset con más contexto que si solo se guardara una tabla aislada.

## Buenas prácticas de uso

Se recomienda:

- usar `write_trips` y `write_flows` para persistencia interna;
- no editar manualmente los sidecars salvo que se entienda completamente su estructura;
- no asumir que un bundle leído está validado;
- conservar juntos archivo tabular y sidecar;
- evitar mezclar manualmente archivos de distintos bundles;
- usar `export_flows` solo cuando el objetivo sea visualización externa.

## Resumen

El bundle `.golondrina` es la forma de persistencia formal de Pylondrina. Su función es materializar datasets internos de forma reproducible, usando archivo tabular y sidecar JSON. No reemplaza al contrato Golondrina ni certifica por sí solo la conformidad semántica, pero permite reconstruir datasets con identidad, metadata, provenance y evidencia operacional.