# API de datasets y reportes

Esta página reúne clases públicas transversales usadas por las operaciones de Pylondrina.

Los datasets representan el estado operativo del pipeline. Los reportes e issues concentran evidencia de ejecución. Los schemas describen contratos de interpretación, y las excepciones tipadas permiten distinguir errores por familia operacional.

Para una explicación conceptual del contrato de datos, consultar [Golondrina](../golondrina/overview.md). Para una explicación práctica de reportes, issues, metadata y eventos, consultar [Issues, reportes y trazabilidad](../user-guide/issues-and-reports.md).

## Datasets

Los datasets son los objetos principales sobre los que opera Pylondrina. Cada uno agrupa una tabla o conjunto de tablas, junto con metadata, provenance y otros bloques necesarios para sostener trazabilidad operacional.

::: pylondrina.datasets.TripDataset
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.datasets.FlowDataset
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.datasets.TraceDataset
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

## Issues y reportes

Los reportes registran evidencia estructurada sobre la ejecución de operaciones públicas. Según la operación, pueden describir importación, validación, transformación, construcción de flows, inferencia o consistencia.

::: pylondrina.reports.Issue
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.reports.ImportReport
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.reports.ValidationReport
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.reports.OperationReport
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.reports.FlowBuildReport
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.reports.InferenceReport
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.reports.ConsistencyReport
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

## Schemas

Los schemas describen la estructura esperada de una representación Golondrina. Se usan para declarar campos, tipos, dominios, reglas mínimas y contratos efectivos durante importación, validación e inferencia.

::: pylondrina.schema.DomainSpec
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.schema.FieldSpec
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.schema.TripSchema
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.schema.TripSchemaEffective
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.schema.TraceSchema
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

## Excepciones

Las excepciones públicas permiten distinguir errores por familia operacional. Esta separación facilita manejar fallas de importación, validación, filtrado, inferencia, exportación o configuración de schema sin depender solo del mensaje textual del error.

::: pylondrina.errors.PylondrinaError
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.errors.SchemaError
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.errors.ImportError
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.errors.ValidationError
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.errors.FixError
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.errors.FilterError
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.errors.InferenceError
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.errors.ExportError
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true