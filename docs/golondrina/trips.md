# Trips / movements

La representación de **trips / movements** describe datos de viajes origen-destino a nivel de fila. En Golondrina, la unidad tabular básica no se define simplemente como “viaje”, sino como **movement**, entendido como un desplazamiento OD elemental e indivisible dentro del dataset.

Esta distinción permite representar de forma consistente fuentes que entregan viajes resumidos, viajes de una sola etapa o viajes compuestos por múltiples etapas.

## Movement y trip conceptual

En Golondrina se distinguen dos niveles:

| Concepto | Descripción |
|---|---|
| `movement` | Unidad mínima de representación tabular. Cada fila describe un desplazamiento OD elemental. |
| `trip` | Viaje conceptual, que puede estar compuesto por uno o más movements. |

Un viaje simple puede representarse como un único movement. En cambio, un viaje multietapa puede representarse como varios movements asociados a un mismo `trip_id` y ordenados mediante `movement_seq`.

Esta separación evita forzar una única interpretación sobre fuentes distintas. Algunas fuentes registran etapas, otras viajes resumidos y otras movimientos ya agregados. El contrato Golondrina permite tratar esos casos bajo una estructura común.

## Propósito de la representación

La representación de trips permite:

- estandarizar viajes OD provenientes de fuentes heterogéneas;
- conservar identificadores mínimos de usuario, viaje y fila;
- representar origen y destino con coordenadas e índices H3;
- manejar temporalidad absoluta, parcial o ausente;
- incorporar atributos analíticos como propósito, modo, peso o segmento;
- construir flows agregados desde viajes;
- persistir datasets reproducibles mediante bundles `.golondrina`.

## Núcleo canónico de trips

El núcleo canónico corresponde al conjunto de nombres y reglas que Pylondrina v1.1 asume para habilitar el pipeline principal. Aunque algunos campos pueden ser derivados durante la importación, el dataset resultante debe converger hacia este contrato para operar de forma completa.

| Grupo | Campos principales | Rol |
|---|---|---|
| Identificación | `movement_id`, `user_id` | Identifican la fila OD y la entidad observada. |
| Agrupación de viaje | `trip_id`, `movement_seq` | Permiten representar viajes simples o multietapa. |
| Espacialidad OD | `origin_longitude`, `origin_latitude`, `destination_longitude`, `destination_latitude` | Representan origen y destino en coordenadas geográficas. |
| Indexación espacial | `origin_h3_index`, `destination_h3_index` | Permiten agregación espacial y construcción de flows. |
| Temporalidad | `origin_time_utc`, `destination_time_utc` | Representan inicio y término del movement cuando existe temporalidad absoluta. |
| Temporalidad parcial | `origin_time_local_hhmm`, `destination_time_local_hhmm` | Permiten análisis intradía cuando no existe fecha completa. |
| Magnitud analítica | `trip_weight` | Permite construir flows ponderados cuando existe factor de expansión o peso. |

## Campos obligatorios y campos derivados

En la práctica, el usuario no siempre necesita entregar todos los campos canónicos ya materializados en la fuente original. La importación puede generar o derivar algunos de ellos.

Por ejemplo:

- `movement_id` puede generarse si la fuente no lo entrega.
- `trip_id` y `movement_seq` pueden derivarse en escenarios de una sola etapa.
- `origin_h3_index` y `destination_h3_index` pueden derivarse desde coordenadas OD.
- La temporalidad UTC puede normalizarse cuando la fuente entrega fecha, hora y zona horaria interpretable.

Por ello, se distingue entre:

| Nivel | Descripción |
|---|---|
| Entrada mínima compatible | Campos suficientes para que el import construya un `TripDataset` operable. |
| Contrato canónico resultante | Campos y metadata esperados después de importar y estandarizar. |
| Conformidad validada | Estado alcanzado después de ejecutar validación formal. |

## Entrada mínima compatible

Una entrada mínima compatible para el flujo base de trips debe contener, al menos, información que permita identificar usuario, origen, destino y temporalidad cuando corresponda.

Un conjunto típico de entrada mínima puede incluir:

| Campo fuente esperado | Campo canónico al que converge |
|---|---|
| identificador de usuario | `user_id` |
| longitud de origen | `origin_longitude` |
| latitud de origen | `origin_latitude` |
| longitud de destino | `destination_longitude` |
| latitud de destino | `destination_latitude` |
| tiempo de origen | `origin_time_utc` o representación temporal equivalente |
| tiempo de destino | `destination_time_utc` o representación temporal equivalente |

La fuente puede usar nombres propios. La correspondencia hacia nombres Golondrina se resuelve mediante `field_correspondence` durante la importación.

## Temporalidad en trips

Golondrina reconoce que no todas las fuentes entregan el mismo nivel de información temporal. Por eso, la temporalidad de trips se organiza en tiers.

| Tier | Señal temporal | Capacidades habilitadas | Limitaciones |
|---|---|---|---|
| Tier 1 | `origin_time_utc` y `destination_time_utc` con fecha y hora completas | Filtrado temporal absoluto, validación temporal, agregación temporal de flows | Requiere instantes comparables |
| Tier 2 | `origin_time_local_hhmm` y `destination_time_local_hhmm` | Análisis intradía o por franjas horarias | No habilita comparación temporal absoluta |
| Tier 3 | Sin temporalidad OD usable | Operaciones espaciales y categóricas básicas | No habilita operaciones temporales |

La zona horaria no se modela como una columna por fila. Se registra como parte del contexto de importación y de la metadata del dataset. Si los tiempos vienen sin zona horaria explícita, su interpretación depende de la zona declarada al importar.

## Espacialidad e H3

La representación espacial de trips usa coordenadas geográficas en EPSG:4326 y, cuando es posible, índices H3 derivados desde esas coordenadas.

Los campos espaciales principales son:

| Campo | Descripción |
|---|---|
| `origin_longitude` | Longitud del origen. |
| `origin_latitude` | Latitud del origen. |
| `destination_longitude` | Longitud del destino. |
| `destination_latitude` | Latitud del destino. |
| `origin_h3_index` | Celda H3 del origen. |
| `destination_h3_index` | Celda H3 del destino. |

Los índices H3 cumplen un rol central porque permiten construir flows agregados y comparar viajes bajo una grilla espacial común.

## Campos categóricos y dominios

Golondrina permite definir dominios para campos categóricos. En trips, los campos categóricos más frecuentes son:

| Campo | Uso típico |
|---|---|
| `purpose` | Propósito del viaje o movement. |
| `mode` | Modo de transporte. |
| `day_type` | Tipo de día, cuando la fuente lo provee. |
| `user_gender` | Segmentación sociodemográfica, cuando corresponde al análisis. |

Las fuentes reales suelen usar codificaciones propias. Por ejemplo, una encuesta puede codificar propósito, modo o género con etiquetas, números o categorías específicas. Pylondrina permite mapear esos valores hacia dominios efectivos mediante `value_correspondence`.

## Campos extendidos

Un `TripDataset` puede conservar campos adicionales como extensiones compatibles. Estos campos no forman parte necesariamente del núcleo canónico, pero pueden ser útiles para análisis posteriores.

Ejemplos de extensiones compatibles:

- comuna de origen;
- comuna de destino;
- código de encuesta;
- identificador original de la fuente;
- variables sociodemográficas;
- subcategorías modales;
- factores de expansión alternativos;
- etiquetas internas de calidad.

La regla general es que una extensión compatible puede coexistir con Golondrina siempre que no contradiga los campos canónicos necesarios para operar el pipeline.

## Relación con operaciones de Pylondrina

La representación de trips es usada por varias operaciones públicas.

| Operación | Relación con trips |
|---|---|
| OP-01 `import_trips_from_dataframe` | Construye un `TripDataset` desde una fuente tabular. |
| OP-02 `validate_trips` | Certifica conformidad formal del dataset. |
| OP-03 `fix_trips_correspondence` | Corrige correspondencias semánticas post-import. |
| OP-04 `clean_trips` | Elimina filas problemáticas bajo reglas explícitas. |
| OP-05 `filter_trips` | Selecciona subconjuntos por criterios atributivos, temporales o espaciales. |
| OP-06 `write_trips` | Persiste un `TripDataset` como bundle `.golondrina`. |
| OP-07 `read_trips` | Reconstruye un `TripDataset` desde persistencia formal. |
| OP-08 `build_flows` | Construye flows agregados desde trips. |

## Estado de validación

El estado de validación se registra en `metadata["is_validated"]`.

La convención general es:

- después de importar, el dataset queda no validado;
- después de validar correctamente, puede quedar validado;
- operaciones drop-only como limpieza o filtrado preservan el estado cuando no reinterpretan el contrato;
- operaciones que reconstruyen o leen desde disco no deben entenderse como certificación automática de conformidad.

Esta política permite distinguir entre un dataset construido, un dataset certificado y un artefacto leído o derivado.

## Resumen

La representación de trips / movements es el eje principal del pipeline Golondrina/Pylondrina. Su diseño permite trabajar con viajes OD de distintas fuentes sin asumir que todas representan el fenómeno de la misma manera. La distinción entre movement y trip conceptual permite soportar viajes simples, viajes resumidos y viajes multietapa dentro de un mismo contrato.