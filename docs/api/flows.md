# API de flows

Esta página reúne la referencia técnica de las funciones públicas que operan sobre `FlowDataset`.

Las operaciones de flows cubren el bloque Trip → Flow de Pylondrina: construcción de flujos OD desde trips, exportación a layouts externos, persistencia formal, lectura, filtrado e inspección de la relación entre flujos y viajes. Para una explicación de uso por operación, consultar el bloque [Operaciones Trip → Flow](../operations/flows/index.md).

## OP-08 Build flows

Referencia técnica de la operación que construye un `FlowDataset` desde un `TripDataset`.

::: pylondrina.transforms.flows.build_flows
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.transforms.flows.FlowBuildOptions
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

Manual de uso: [OP-08 Build flows](../operations/flows/op08_build_flows.md).

## OP-09 Export flows

Referencia técnica de la operación que exporta flows a un layout externo orientado a visualización.

::: pylondrina.export.flows.export_flows
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.export.flows.ExportFlowsOptions
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.export.flows.FlowExportResult
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

Manual de uso: [OP-09 Export flows](../operations/flows/op09_export_flows.md).

## OP-10 Write flows

Referencia técnica de la operación de persistencia formal de `FlowDataset`.

::: pylondrina.io.flows.write_flows
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.io.flows.WriteFlowsOptions
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

Manual de uso: [OP-10 Write flows](../operations/flows/op10_write_flows.md).

## OP-11 Read flows

Referencia técnica de la operación de lectura formal de bundles de flows.

::: pylondrina.io.flows.read_flows
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.io.flows.ReadFlowsOptions
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

Manual de uso: [OP-11 Read flows](../operations/flows/op11_read_flows.md).

## OP-12 Filter flows

Referencia técnica de la operación de filtrado declarativo sobre `FlowDataset`.

::: pylondrina.transforms.flows_filtering.filter_flows
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.transforms.flows_filtering.FlowFilterOptions
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

Manual de uso: [OP-12 Filter flows](../operations/flows/op12_filter_flows.md).

## OP-13 Get trips from flows

Referencia técnica de la operación que recupera la correspondencia entre flows y trips cuando existe información suficiente.

::: pylondrina.queries.flows.get_trips_from_flows
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

Manual de uso: [OP-13 Get trips from flows](../operations/flows/op13_get_trips_from_flows.md).