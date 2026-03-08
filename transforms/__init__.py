"""
Transformaciones sobre datasets Golondrina.

Incluye:
- filtros espaciales/temporales sobre TripDataset,
- construcción de flujos (FlowDataset),
- concatenación de TripDataset,
- diagnósticos mínimos sobre TraceDataset (v1).
"""

from .filtering import (
    FilterOptions,
    TimeFilter,
    TimePredicate,
    SpatialPredicate,
    filter_trips,
)

from .flows import (
    FlowBuildOptions,
    build_flows,
    TimeAggregation,
    TimeBasis,
)

from .concat import (
    TripConcatOptions,
    concat_trip_datasets,
)


from .enrich import (
    TripEnrichJoinHow, 
    TripEnrichOptions, 
    enrich_trips
)

__all__ = [
    # filtering
    "FilterOptions",
    "TimeFilter",
    "TimePredicate",
    "SpatialPredicate",
    "filter_trips",
    # flows
    "FlowBuildOptions",
    "build_flows",
    "TimeAggregation",
    "TimeBasis",
    # concat
    "TripConcatOptions",
    "concat_trip_datasets",
    # enrich
    "TripEnrichJoinHow",
    "TripEnrichOptions",
    "enrich_trips",
]
