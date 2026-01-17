# ============================================================
# Pylondrina — API v1 
# ============================================================

# -------------------------
# file: pylondrina/__init__.py
# -------------------------
"""
Pylondrina: Librería Python para trabajar con el formato Golondrina.

El estilo de programacion de esta API se basa en:
- Objetos (datasets y esquemas) como contenedores de estado y contexto.
- Funciones de módulo para importación, validación, inferencia, agregación y visualización.
"""
from .schema import FieldSpec, DomainSpec, TripSchema, TraceSchema
from .datasets import TripDataset, FlowDataset, TraceDataset
from .reports import Issue, ImportReport, ValidationReport, InferenceReport, FlowBuildReport, ConsistencyReport
from .errors import PylondrinaError, SchemaError, ValidationError, ImportError, InferenceError

from .importing import (
    import_trips_from_dataframe,
    apply_field_correspondence,
    standardize_categorical_values,
    build_import_metadata,
)
from .validation import validate_trips

from .sources.helpers import import_trips_from_source

from .transforms.flows import FlowBuildOptions, build_flows
from .transforms.concat import TripConcatOptions, concat_trip_datasets
from .transforms.enrich import TripEnrichOptions, enrich_trips
from .export.flowmap_blue import export_to_flowmap_blue

__all__ = [
    # schemas / specs
    "FieldSpec", "DomainSpec", "TripSchema", "TraceSchema",
    # datasets
    "TripDataset", "FlowDataset", "TraceDataset",
    # reports
    "Issue", "ImportReport", "ValidationReport", "InferenceReport", "FlowBuildReport", "ConsistencyReport",
    # errors
    "PylondrinaError", "SchemaError", "ValidationError", "ImportError", "InferenceError",
    # importing / validation
    "import_trips_from_dataframe", "apply_field_correspondence", "standardize_categorical_values", "build_import_metadata",
    "validate_trips",
    "import_trips_from_source",
    # transforms / export (atajos)
    "FlowBuildOptions", "build_flows",
    "TripConcatOptions", "concat_trip_datasets",
    "export_to_flowmap_blue",
    "TripEnrichOptions",
    "enrich_trips",
]


