# API de traces

Esta página reúne la referencia técnica de las funciones públicas que operan sobre `TraceDataset` y sobre la inferencia de trips desde traces discretas.

Las operaciones de traces cubren el flujo Trace → Trip de Pylondrina: importación de puntos espacio-temporales discretos, validación mínima del dataset de traces e inferencia austera de trips compatibles con el resto del pipeline. Para una explicación de uso por operación, consultar el bloque [Operaciones sobre traces](../operations/traces/index.md).

## OP-14 Import traces

Referencia técnica de la operación de entrada para construir un `TraceDataset` desde una tabla de puntos discretos.

::: pylondrina.importing_traces.import_traces_from_dataframe
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.importing_traces.ImportTraceOptions
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

Manual de uso: [OP-14 Import traces](../operations/traces/op14_import_traces.md).

## OP-15 Validate traces

Referencia técnica de la operación de validación formal mínima sobre `TraceDataset`.

::: pylondrina.validation_traces.validate_traces
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.validation_traces.TraceValidationOptions
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

Manual de uso: [OP-15 Validate traces](../operations/traces/op15_validate_traces.md).

## OP-16 Infer trips from traces

Referencia técnica de la operación que deriva un `TripDataset` desde traces discretas.

::: pylondrina.transforms.inference.infer_trips_from_traces
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.transforms.inference.InferTripsOptions
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

Manual de uso: [OP-16 Infer trips from traces](../operations/traces/op16_infer_trips_from_traces.md).