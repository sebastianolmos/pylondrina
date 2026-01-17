"""
Transformaciones sobre datasets Golondrina.

Incluye:
- filtros espaciales/temporales sobre TripDataset,
- construcción de flujos (FlowDataset),
- concatenación de TripDataset,
- diagnósticos mínimos sobre TraceDataset (v1).
"""

from .filtering import (
    filter_trips,
    filter_by_h3_cells,
    filter_by_bbox,
    filter_by_polygon,
    filter_by_domain_values,
    filter_by_time_range,
)

from .flows import (
    FlowBuildOptions,
    build_flows,
    TimeAggregation,
    TimeBasis,
    TimePredicate,
)

from .concat import (
    TripConcatOptions,
    concat_trip_datasets,
)

from .traces import (
    TraceConsistencyOptions,
    validate_trace_consistency,
    compute_trace_stats,
)

from .enrich import (
    TripEnrichJoinHow, 
    TripEnrichOptions, 
    enrich_trips
)

__all__ = [
    # filtering
    "filter_trips",
    "filter_by_h3_cells",
    "filter_by_bbox",
    "filter_by_polygon",
    "filter_by_domain_values",
    "filter_by_time_range",
    # flows
    "FlowBuildOptions",
    "build_flows",
    "TimeAggregation",
    "TimeBasis",
    "TimePredicate",
    # concat
    "TripConcatOptions",
    "concat_trip_datasets",
    # traces
    "TraceConsistencyOptions",
    "validate_trace_consistency",
    "compute_trace_stats",
    # enrich
    "TripEnrichJoinHow",
    "TripEnrichOptions",
    "enrich_trips",
]
