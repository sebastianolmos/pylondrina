# Visión general de Golondrina

**Golondrina** es el contrato de datos definido para representar información de movilidad urbana de forma común, trazable y reutilizable. Su propósito es reducir la fricción técnica que aparece cuando distintas fuentes describen viajes, puntos espacio-temporales o flujos agregados con estructuras, nombres de campos y dominios categóricos incompatibles.

En esta versión, Golondrina se entiende como un **contrato lógico de datos**, no como un formato físico de archivo. Por lo tanto, no equivale a CSV, Parquet, Feather ni JSON. Esos formatos pueden usarse para almacenar o intercambiar datos, pero el contrato Golondrina describe qué campos, reglas y relaciones debe respetar una representación para ser operable dentro del sistema.

!!! abstract "Idea central"
    Golondrina define el estado común al que deben converger los datos de movilidad para que puedan ser importados, validados, transformados, agregados, exportados y persistidos de forma reproducible mediante Pylondrina.

## Qué problema aborda

En movilidad urbana es frecuente trabajar con fuentes heterogéneas. Una encuesta origen-destino, una fuente transaccional de transporte público, registros tipo check-in o trazas discretas pueden representar fenómenos relacionados, pero no necesariamente comparten:

- nombres de columnas;
- tipos de datos;
- unidades espaciales;
- codificaciones categóricas;
- granularidad temporal;
- estructura de identificadores;
- criterios explícitos de trazabilidad.

Sin un contrato común, cada análisis tiende a depender de adaptaciones locales difíciles de reutilizar. Golondrina responde a ese problema definiendo un núcleo canónico de representación y una forma explícita de conservar extensiones compatibles cuando la fuente contiene información adicional.

## Qué es y qué no es Golondrina

| Aspecto | Interpretación correcta |
|---|---|
| Contrato de datos | Define campos, reglas mínimas, dominios y convenciones por representación. |
| Formato físico | No es un formato físico como CSV, Parquet o Feather. |
| Esquema extensible | Permite campos adicionales mientras se conserve el núcleo canónico necesario. |
| Pipeline de software | No ejecuta operaciones por sí mismo; las operaciones las implementa Pylondrina. |
| Bundle `.golondrina` | No es el contrato en sí, sino un artefacto formal de persistencia usado por Pylondrina. |

Esta distinción es importante porque evita confundir tres niveles distintos:

1. el **contrato semántico** de los datos;
2. las **extensiones compatibles** que preservan información propia de una fuente;
3. el **artefacto persistido** que materializa un dataset en disco.

## Representaciones principales

Golondrina organiza los datos de movilidad en tres representaciones principales.

| Representación | Unidad principal | Rol dentro del sistema |
|---|---|---|
| Trips / movements | Desplazamiento OD elemental | Representa registros origen-destino a nivel de fila. |
| Traces / points | Punto espacio-temporal discreto | Representa observaciones secuenciales de usuarios, dispositivos o entidades. |
| Flows | Agregación OD | Representa flujos derivados desde trips mediante una regla explícita de agregación. |

Estas representaciones no son equivalentes entre sí. Un trip o movement describe una unidad OD; una trace describe una secuencia de puntos observados; un flow es un producto analítico agregado derivado desde trips.

## Relación entre Golondrina y Pylondrina

**Pylondrina** es la librería Python que opera sobre el contrato Golondrina. Su rol es llevar fuentes heterogéneas hacia representaciones operables, validar conformidad, transformar datasets, construir flows, exportar artefactos, persistir datasets y derivar trips desde traces discretas.

En v1.1, la librería se organizó en tres familias de operaciones:

- operaciones sobre trips;
- operaciones Trip → Flow;
- operaciones sobre traces e inferencia Trace → Trip.

El contrato Golondrina define qué estructura debe tener cada representación. Pylondrina implementa las operaciones que permiten construir, revisar, transformar y materializar esas representaciones.

## Niveles de adopción

Golondrina no exige que toda fuente externa llegue ya perfectamente alineada al contrato. El sistema distingue tres niveles prácticos de adopción.

### Nivel A. Adopción canónica completa

La fuente ya cumple, o puede mapearse directamente, al núcleo canónico de Golondrina. Este es el caso más favorable, porque habilita validación más completa, mayor interoperabilidad y menor trabajo de adaptación.

### Nivel B. Extensión compatible

La fuente respeta el núcleo canónico, pero agrega campos propios o adapta partes del schema para conservar información específica. Este nivel permite flexibilidad sin romper la compatibilidad con las operaciones principales de Pylondrina.

### Nivel C. Entrada mínima compatible

La fuente todavía no está completamente en Golondrina, pero contiene información suficiente para que la importación pueda construir una representación operable. En este caso, el import puede mapear campos, coercionar tipos, normalizar temporalidad, derivar H3 y generar identificadores mínimos cuando corresponde.

!!! note "Importar no equivale a validar"
    Una entrada mínima compatible puede transformarse en un dataset operable, pero eso no significa que ya esté formalmente certificado. La validación es una operación separada.

## Convenciones transversales

El contrato Golondrina usa algunas convenciones comunes a las tres representaciones:

- **Campo:** propiedad semántica identificada por un nombre canónico, tipo y reglas asociadas.
- **Dominio:** conjunto de valores esperados para un campo categórico.
- **Esquema:** especificación de campos, tipos, restricciones y dominios esperados.
- **Dataset:** conjunto de datos acompañado por schema, metadata, provenance y estado operativo.
- **Provenance:** información que permite explicar origen, contexto y derivación de un dataset.
- **Metadata:** espacio persistible para registrar estado, trazabilidad y decisiones operacionales.
- **Extensión compatible:** campo adicional que conserva información de la fuente sin romper el núcleo canónico.

Estas convenciones permiten que un dataset no sea interpretado solo como una tabla, sino como un objeto de trabajo trazable y reproducible.

## Alcance de v1.1

La versión v1.1 corresponde al MVP implementado del proyecto. El sistema cubre:

- importación y estructuración de trips;
- validación formal de trips;
- corrección semántica post-import;
- limpieza y filtrado reproducible;
- construcción de flows OD;
- exportación de flows a layouts de visualización;
- persistencia formal de trips y flows;
- consulta flow → trips;
- importación y validación mínima de traces;
- inferencia austera de trips desde traces discretas.

Quedan fuera del alcance de esta versión:

- una plataforma ETL generalista;
- inferencia avanzada sobre trayectorias GPS densas;
- reconstrucción multimodal compleja;
- una suite completa de visualización interactiva integrada al core;
- reglas de negocio específicas por fuente como parte del núcleo de la librería.

## Organización de esta sección

Esta sección se organiza en cuatro páginas complementarias:

- [Trips / movements](trips.md): contrato para desplazamientos origen-destino a nivel de fila.
- [Traces / points](traces.md): contrato para puntos espacio-temporales discretos.
- [Flows](flows.md): contrato para agregaciones OD derivadas.
- [Bundles .golondrina](bundles.md): artefactos formales de persistencia usados por Pylondrina.