"""
Inferencia de viajes (TripDataset) a partir de trazas (TraceDataset).
"""

from .trips_from_traces import (
    InferenceOptions,
    infer_trips_from_traces,
)

__all__ = [
    "InferenceOptions",
    "infer_trips_from_traces",
]
