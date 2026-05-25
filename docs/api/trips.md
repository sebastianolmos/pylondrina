# API de trips

Esta página reúne la referencia técnica de las funciones públicas que operan sobre `TripDataset`.

Las operaciones de trips cubren el ciclo base de trabajo sobre viajes o movements OD: importación, validación, corrección semántica, limpieza, filtrado y persistencia formal. Para una explicación de uso por operación, consultar el bloque [Operaciones sobre trips](../operations/trips/index.md).

## OP-01 Import trips

Referencia técnica de la operación de entrada para construir un `TripDataset` desde una tabla fuente.

::: pylondrina.importing.import_trips_from_dataframe
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.importing.ImportOptions
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

Manual de uso: [OP-01 Import trips](../operations/trips/op01_import_trips.md).

## OP-02 Validate trips

Referencia técnica de la operación de validación formal de trips.

::: pylondrina.validation.validate_trips
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.validation.ValidationOptions
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

Manual de uso: [OP-02 Validate trips](../operations/trips/op02_validate_trips.md).

## OP-03 Fix trips correspondence

Referencia técnica de la operación de corrección semántica post-import.

::: pylondrina.fixing.fix_trips_correspondence
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.fixing.FixCorrespondenceOptions
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

Manual de uso: [OP-03 Fix trips correspondence](../operations/trips/op03_fix_trips_correspondence.md).

## OP-04 Clean trips

Referencia técnica de la operación de limpieza drop-only sobre trips.

::: pylondrina.transforms.cleaning.clean_trips
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.transforms.cleaning.CleanOptions
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

Manual de uso: [OP-04 Clean trips](../operations/trips/op04_clean_trips.md).

## OP-05 Filter trips

Referencia técnica de la operación de filtrado declarativo sobre trips.

::: pylondrina.transforms.filtering.filter_trips
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.transforms.filtering.FilterOptions
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.transforms.filtering.TimeFilter
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

Manual de uso: [OP-05 Filter trips](../operations/trips/op05_filter_trips.md).

## OP-06 Write trips

Referencia técnica de la operación de persistencia formal de `TripDataset`.

::: pylondrina.io.trips.write_trips
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.io.trips.WriteTripsOptions
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

Manual de uso: [OP-06 Write trips](../operations/trips/op06_write_trips.md).

## OP-07 Read trips

Referencia técnica de la operación de lectura formal de bundles de trips.

::: pylondrina.io.trips.read_trips
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

::: pylondrina.io.trips.ReadTripsOptions
    options:
      show_source: false
      show_root_heading: true
      show_signature_annotations: true

Manual de uso: [OP-07 Read trips](../operations/trips/op07_read_trips.md).