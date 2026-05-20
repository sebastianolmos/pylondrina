from __future__ import annotations

import copy
from collections.abc import Callable, Mapping
from typing import Any

import pandas as pd
import pytest

from pylondrina.datasets import FlowDataset


# ---------------------------------------------------------------------------
# Constantes mínimas reutilizables
# ---------------------------------------------------------------------------

SOURCE_TRIPS_SENTINEL = "SENTINEL_SOURCE_TRIPS_ONLY_IN_MEMORY"


def _minimal_aggregation_spec() -> dict[str, Any]:
    """Construye la aggregation_spec mínima reutilizada en fixtures de OP-10."""
    return {
        "h3_resolution": 8,
        "group_by": ["mode"],
        "time_aggregation": "hour",
        "time_basis": "origin",
        "min_trips_per_flow": 1,
    }


def _minimal_helper_provenance() -> dict[str, Any]:
    """Construye provenance mínima equivalente a la usada en helper-level tests."""
    return {
        "derived_from": [
            {
                "type": "trips",
                "dataset_id": "trip_dset_001",
            }
        ],
        "prior_events_summary": {
            "build_flows": 1,
        },
    }


def _minimal_smoke_provenance() -> dict[str, Any]:
    """Construye provenance mínima equivalente a la usada en smoke tests."""
    return {
        "derived_from": [
            {
                "source_type": "trips",
                "dataset_id": "trip-dset-origin-001",
            }
        ],
        "prior_events_summary": {
            "n_events": 2,
        },
    }


def _minimal_build_flows_event(*, n_flows: int) -> dict[str, Any]:
    """Construye un evento previo simple para probar append de write_flows."""
    return {
        "op": "build_flows",
        "ts_utc": "2026-04-06T00:00:00Z",
        "parameters": {
            "h3_resolution": 8,
        },
        "summary": {
            "n_flows": n_flows,
        },
        "issues_summary": {
            "counts": {
                "info": 0,
                "warning": 0,
                "error": 0,
            },
            "top_codes": [],
        },
    }


# ---------------------------------------------------------------------------
# DataFrames mínimos
# ---------------------------------------------------------------------------

@pytest.fixture
def flows_df_minimal() -> pd.DataFrame:
    """Entrega una tabla mínima de flows reutilizable en tests de OP-10."""
    return pd.DataFrame(
        {
            "flow_id": ["f1", "f2", "f3"],
            "origin_h3_index": [
                "881111111111111",
                "882222222222222",
                "883333333333333",
            ],
            "destination_h3_index": [
                "884444444444444",
                "885555555555555",
                "886666666666666",
            ],
            "flow_count": [2, 1, 3],
            "flow_value": [2.0, 1.0, 4.5],
            "mode": ["bus", "metro", "bus"],
            "window_start_utc": [
                "2026-01-01T08:00:00Z",
                "2026-01-01T09:00:00Z",
                "2026-01-01T10:00:00Z",
            ],
            "window_end_utc": [
                "2026-01-01T08:59:59Z",
                "2026-01-01T09:59:59Z",
                "2026-01-01T10:59:59Z",
            ],
        }
    )


@pytest.fixture
def flow_to_trips_df_minimal() -> pd.DataFrame:
    """Entrega una tabla mínima flow-to-trips reutilizable en tests de OP-10."""
    return pd.DataFrame(
        {
            "flow_id": ["f1", "f1", "f2"],
            "movement_id": ["m1", "m2", "m3"],
        }
    )


# ---------------------------------------------------------------------------
# Factory de FlowDataset mínimo
# ---------------------------------------------------------------------------

@pytest.fixture
def make_flowdataset_minimal(
    flows_df_minimal: pd.DataFrame,
    flow_to_trips_df_minimal: pd.DataFrame,
) -> Callable[..., FlowDataset]:
    """
    Entrega un factory flexible para construir FlowDataset mínimos de OP-10.

    Permite variar identidad, validación, presencia del auxiliar, metadata,
    provenance, eventos previos y aggregation_spec sin duplicar código
    en cada archivo de tests.
    """

    def _make_flowdataset_minimal(
        *,
        validated: bool = False,
        include_dataset_id: bool = True,
        dataset_id: str = "dset_existing",
        include_artifact_id: bool = False,
        artifact_id: str = "art_existing",
        include_aux: bool = True,
        aggregation_spec: Mapping[str, Any] | None = None,
        metadata_extra: Mapping[str, Any] | None = None,
        provenance: Mapping[str, Any] | None = None,
        events: list[dict[str, Any]] | None = None,
        source_trips: Any = SOURCE_TRIPS_SENTINEL,
    ) -> FlowDataset:
        metadata: dict[str, Any] = {
            "events": (
                copy.deepcopy(events)
                if events is not None
                else [
                    _minimal_build_flows_event(
                        n_flows=len(flows_df_minimal),
                    )
                ]
            ),
            "is_validated": validated,
        }

        if include_dataset_id:
            metadata["dataset_id"] = dataset_id

        if include_artifact_id:
            metadata["artifact_id"] = artifact_id

        if metadata_extra:
            metadata.update(copy.deepcopy(dict(metadata_extra)))

        aggregation_spec_eff = (
            copy.deepcopy(dict(aggregation_spec))
            if aggregation_spec is not None
            else _minimal_aggregation_spec()
        )

        provenance_eff = (
            copy.deepcopy(dict(provenance))
            if provenance is not None
            else _minimal_helper_provenance()
        )

        return FlowDataset(
            flows=flows_df_minimal.copy(deep=True),
            flow_to_trips=(
                flow_to_trips_df_minimal.copy(deep=True)
                if include_aux
                else None
            ),
            aggregation_spec=aggregation_spec_eff,
            source_trips=copy.deepcopy(source_trips),
            metadata=metadata,
            provenance=provenance_eff,
        )

    return _make_flowdataset_minimal


# ---------------------------------------------------------------------------
# Factory de payload sidecar mínimo
# ---------------------------------------------------------------------------

@pytest.fixture
def make_sidecar_payload(
    flows_df_minimal: pd.DataFrame,
    flow_to_trips_df_minimal: pd.DataFrame,
) -> Callable[..., dict[str, Any]]:
    """
    Entrega un factory de sidecar mínimo para tests helper-level de persistencia.

    El payload conserva la estructura formal usada en los notebooks de OP-10
    y permite alternar entre backend Feather y Parquet, con o sin auxiliar.
    """

    def _make_sidecar_payload(
        *,
        storage_format: str = "feather",
        include_flow_to_trips: bool = True,
    ) -> dict[str, Any]:
        if storage_format == "parquet":
            data_name = "flows.parquet"
            aux_name = "flow_to_trips.parquet"
            storage_options = {
                "compression": "snappy",
            }
        elif storage_format == "feather":
            data_name = "flows.feather"
            aux_name = "flow_to_trips.feather"
            storage_options = {
                "compression": "lz4",
                "version": 2,
            }
        else:
            raise ValueError(
                f"storage_format inesperado: {storage_format!r}"
            )

        return {
            "dataset_type": "flows",
            "format": "golondrina",
            "layout_version": "1.1",
            "storage": {
                "format": storage_format,
                "options": storage_options,
            },
            "dataset_id": "dset_sidecar",
            "artifact_id": "art_sidecar",
            "files": {
                "data": data_name,
                "metadata": "flows.metadata.json",
                "flow_to_trips": (
                    aux_name
                    if include_flow_to_trips
                    else None
                ),
            },
            "aggregation_spec": _minimal_aggregation_spec(),
            "provenance": {
                "derived_from": [
                    {
                        "type": "trips",
                        "dataset_id": "trip_dset_001",
                    }
                ],
            },
            "metadata": {
                "dataset_id": "dset_sidecar",
                "artifact_id": "art_sidecar",
                "is_validated": False,
                "events": [],
            },
            "tables": {
                "flows": {
                    "n_rows": len(flows_df_minimal),
                    "n_cols": len(flows_df_minimal.columns),
                    "columns": list(flows_df_minimal.columns),
                },
                "flow_to_trips": (
                    {
                        "n_rows": len(flow_to_trips_df_minimal),
                        "n_cols": len(flow_to_trips_df_minimal.columns),
                        "columns": list(flow_to_trips_df_minimal.columns),
                    }
                    if include_flow_to_trips
                    else None
                ),
            },
        }

    return _make_sidecar_payload


# ---------------------------------------------------------------------------
# FlowDataset listos para smoke tests simples
# ---------------------------------------------------------------------------

@pytest.fixture
def flowdataset_minimal(
    make_flowdataset_minimal: Callable[..., FlowDataset],
) -> FlowDataset:
    """
    Entrega un FlowDataset mínimo sin auxiliar, útil para smoke tests públicos.

    Se aproxima al dataset usado en el notebook de smoke tests:
    validado, sin flow_to_trips y con metadata sin eventos previos.
    """
    return make_flowdataset_minimal(
        validated=True,
        include_dataset_id=True,
        dataset_id="flow-dset-smoke-001",
        include_artifact_id=False,
        include_aux=False,
        events=[],
        provenance=_minimal_smoke_provenance(),
        source_trips={"debug_only": True},
    )


@pytest.fixture
def flowdataset_with_aux(
    make_flowdataset_minimal: Callable[..., FlowDataset],
) -> FlowDataset:
    """
    Entrega un FlowDataset mínimo con flow_to_trips, útil para smoke tests públicos.

    Se aproxima al dataset usado en el notebook de smoke tests cuando se prueba
    la persistencia opcional del auxiliar.
    """
    return make_flowdataset_minimal(
        validated=True,
        include_dataset_id=True,
        dataset_id="flow-dset-smoke-001",
        include_artifact_id=False,
        include_aux=True,
        events=[],
        provenance=_minimal_smoke_provenance(),
        source_trips={"debug_only": True},
    )