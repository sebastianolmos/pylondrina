from __future__ import annotations

from copy import deepcopy
from typing import Any, Callable, Sequence

import h3
import pandas as pd
import pytest

from pylondrina.datasets import FlowDataset, TripDataset
from pylondrina.reports import Issue
from pylondrina.schema import TripSchema


# -----------------------------------------------------------------------------
# Helpers H3 compartidos
# -----------------------------------------------------------------------------


def _h3_from_latlon(lat: float, lon: float, res: int = 8) -> str:
    """Construye una celda H3 compatible con las APIs v3 y v4 de h3-py."""
    if hasattr(h3, "latlng_to_cell"):
        return h3.latlng_to_cell(lat, lon, res)
    return h3.geo_to_h3(lat, lon, res)


def _h3_to_parent(cell: str, res: int) -> str:
    """Resuelve el parent H3 compatible con las APIs v3 y v4 de h3-py."""
    if hasattr(h3, "cell_to_parent"):
        return h3.cell_to_parent(cell, res)
    return h3.h3_to_parent(cell, res)


# -----------------------------------------------------------------------------
# Helpers de issues, request context y snapshots
# -----------------------------------------------------------------------------


def _assert_issue_codes(
    issues: Sequence[Issue],
    expected_codes: Sequence[str],
) -> None:
    """Verifica que los códigos de issue emitidos coincidan exactamente con lo esperado."""
    observed = [issue.code for issue in issues]
    expected = list(expected_codes)

    assert observed == expected, (
        f"Esperados={expected}, observados={observed}"
    )


def _make_request_ctx(
    *,
    max_issues: int = 1000,
    n_flows_input: int = 0,
    n_trips_input: int | None = None,
    used_source: str | None = None,
    reconstruction_attempted: bool = False,
) -> dict[str, Any]:
    """Construye el request context mínimo usado por helpers internos de OP-13."""
    return {
        "max_issues": max_issues,
        "n_flows_input": n_flows_input,
        "n_trips_input": n_trips_input,
        "used_source": used_source,
        "reconstruction_attempted": reconstruction_attempted,
    }


def _snapshot_flowdataset_state(flows: FlowDataset) -> dict[str, Any]:
    """Captura el estado observable de un FlowDataset antes de ejecutar OP-13."""
    return {
        "flows": flows.flows.copy(deep=True),
        "flow_to_trips": (
            None
            if flows.flow_to_trips is None
            else flows.flow_to_trips.copy(deep=True)
        ),
        "aggregation_spec": deepcopy(flows.aggregation_spec),
        "metadata": deepcopy(flows.metadata),
        "provenance": deepcopy(flows.provenance),
        "source_trips": flows.source_trips,
    }


def _snapshot_tripdataset_state(trips: TripDataset) -> dict[str, Any]:
    """Captura el estado observable de un TripDataset antes de usarlo para reconstrucción."""
    return {
        "data": trips.data.copy(deep=True),
        "schema": trips.schema,
        "schema_effective": getattr(trips, "schema_effective", None),
        "schema_version": trips.schema_version,
        "metadata": deepcopy(trips.metadata),
        "provenance": deepcopy(getattr(trips, "provenance", None)),
    }


# -----------------------------------------------------------------------------
# Factories pequeñas derivadas del helper notebook
# -----------------------------------------------------------------------------


def _make_op13_test_schema() -> TripSchema:
    """Construye el TripSchema mínimo usado por las fixtures pequeñas de OP-13."""
    return TripSchema(
        version="0.1.0",
        fields={},
        required=[],
    )


def _make_op13_test_tripdataset(res: int = 8) -> TripDataset:
    """Construye el TripDataset pequeño usado en helper-level y smoke tests."""
    points = {
        "A": (-33.4500, -70.6600),
        "B": (-33.4400, -70.6400),
        "C": (-33.4600, -70.6200),
        "D": (-33.4700, -70.6100),
        "E": (-33.4300, -70.6000),
        "F": (-33.4200, -70.5800),
    }

    cells = {
        key: _h3_from_latlon(lat, lon, res)
        for key, (lat, lon) in points.items()
    }

    data = pd.DataFrame(
        [
            {
                "movement_id": "m0",
                "trip_id": "t0",
                "origin_h3_index": cells["A"],
                "destination_h3_index": cells["B"],
                "mode": "bus",
                "purpose": "work",
                "origin_time_utc": "2026-01-01T08:05:00Z",
                "destination_time_utc": "2026-01-01T08:25:00Z",
            },
            {
                "movement_id": "m1",
                "trip_id": "t1",
                "origin_h3_index": cells["A"],
                "destination_h3_index": cells["B"],
                "mode": "bus",
                "purpose": "work",
                "origin_time_utc": "2026-01-01T08:15:00Z",
                "destination_time_utc": "2026-01-01T08:35:00Z",
            },
            {
                "movement_id": "m2",
                "trip_id": "t2",
                "origin_h3_index": cells["A"],
                "destination_h3_index": cells["C"],
                "mode": "metro",
                "purpose": "study",
                "origin_time_utc": "2026-01-01T09:10:00Z",
                "destination_time_utc": "2026-01-01T09:35:00Z",
            },
            {
                "movement_id": "m3",
                "trip_id": "t3",
                "origin_h3_index": cells["D"],
                "destination_h3_index": cells["E"],
                "mode": "bus",
                "purpose": "work",
                "origin_time_utc": "2026-01-01T08:20:00Z",
                "destination_time_utc": "2026-01-01T08:50:00Z",
            },
        ]
    )

    return TripDataset(
        data=data,
        schema=_make_op13_test_schema(),
        metadata={
            "dataset_id": "trips_op13_test",
            "is_validated": True,
            "temporal": {"tier": "tier_1"},
            "events": [],
        },
    )


def _make_op13_test_flowdataset(
    *,
    include_flow_to_trips: bool = True,
    duplicate_direct_pairs: bool = False,
    include_extra_unmatched_flow: bool = True,
    source_trips: TripDataset | None = None,
    flow_resolution: int = 8,
) -> FlowDataset:
    """Construye el FlowDataset pequeño usado en helpers y smoke tests de OP-13."""
    trips_for_build = (
        source_trips
        if source_trips is not None
        else _make_op13_test_tripdataset(res=8)
    )
    trips_df = trips_for_build.data.copy(deep=True)

    def maybe_roll(cell: str) -> str:
        return cell if flow_resolution == 8 else _h3_to_parent(cell, flow_resolution)

    flows_rows = [
        {
            "flow_id": "f_ab_bus_work_h08",
            "origin_h3_index": maybe_roll(trips_df.loc[0, "origin_h3_index"]),
            "destination_h3_index": maybe_roll(
                trips_df.loc[0, "destination_h3_index"]
            ),
            "window_start_utc": pd.Timestamp("2026-01-01 08:00:00"),
            "window_end_utc": pd.Timestamp("2026-01-01 09:00:00"),
            "mode": "bus",
            "purpose": "work",
            "flow_count": 2,
            "flow_value": 2.0,
        },
        {
            "flow_id": "f_ac_metro_study_h09",
            "origin_h3_index": maybe_roll(trips_df.loc[2, "origin_h3_index"]),
            "destination_h3_index": maybe_roll(
                trips_df.loc[2, "destination_h3_index"]
            ),
            "window_start_utc": pd.Timestamp("2026-01-01 09:00:00"),
            "window_end_utc": pd.Timestamp("2026-01-01 10:00:00"),
            "mode": "metro",
            "purpose": "study",
            "flow_count": 1,
            "flow_value": 1.0,
        },
    ]

    if include_extra_unmatched_flow:
        extra_origin = _h3_from_latlon(-33.4100, -70.5700, 8)
        extra_destination = _h3_from_latlon(-33.4000, -70.5500, 8)

        flows_rows.append(
            {
                "flow_id": "f_extra_walk_leisure_h10",
                "origin_h3_index": maybe_roll(extra_origin),
                "destination_h3_index": maybe_roll(extra_destination),
                "window_start_utc": pd.Timestamp("2026-01-01 10:00:00"),
                "window_end_utc": pd.Timestamp("2026-01-01 11:00:00"),
                "mode": "walk",
                "purpose": "leisure",
                "flow_count": 5,
                "flow_value": 5.0,
            }
        )

    flows_df = pd.DataFrame(flows_rows)

    flow_to_trips_df = None
    if include_flow_to_trips:
        flow_to_trips_df = pd.DataFrame(
            [
                {
                    "flow_id": "f_ab_bus_work_h08",
                    "movement_id": "m0",
                },
                {
                    "flow_id": "f_ab_bus_work_h08",
                    "movement_id": "m1",
                },
                {
                    "flow_id": "f_ac_metro_study_h09",
                    "movement_id": "m2",
                },
            ]
        )

        if duplicate_direct_pairs:
            flow_to_trips_df = pd.concat(
                [flow_to_trips_df, flow_to_trips_df.iloc[[0]]],
                ignore_index=True,
            )

    aggregation_spec = {
        "h3_resolution": flow_resolution,
        "group_by": ["mode", "purpose"],
        "time_aggregation": "hour",
        "time_basis": "origin",
        "effective_flow_keys": [
            "origin_h3_index",
            "destination_h3_index",
            "window_start_utc",
            "window_end_utc",
            "mode",
            "purpose",
        ],
    }

    return FlowDataset(
        flows=flows_df,
        flow_to_trips=flow_to_trips_df,
        aggregation_spec=aggregation_spec,
        source_trips=source_trips,
        metadata={
            "dataset_id": "flows_op13_test",
            "events": [],
        },
        provenance={
            "derived_from": [
                {
                    "type": "trips",
                    "dataset_id": trips_for_build.metadata.get("dataset_id"),
                }
            ]
        },
    )


# -----------------------------------------------------------------------------
# Factories ricas derivadas del integration notebook
# -----------------------------------------------------------------------------


def _build_integration_cells() -> tuple[dict[str, str], dict[str, str]]:
    """Construye las celdas H3 resolución 8 y sus parents resolución 7 para integración."""
    points = {
        "A": (-33.4500, -70.6600),
        "B": (-33.4400, -70.6400),
        "C": (-33.4600, -70.6200),
        "D": (-33.4700, -70.6100),
        "E": (-33.4300, -70.6000),
        "F": (-33.4200, -70.5800),
        "G": (-33.4100, -70.5600),
        "H": (-33.4050, -70.5450),
        "Z1": (-33.3900, -70.5200),
        "Z2": (-33.3850, -70.5050),
    }

    cells8 = {
        key: _h3_from_latlon(lat, lon, 8)
        for key, (lat, lon) in points.items()
    }
    cells7 = {
        key: _h3_to_parent(cell, 7)
        for key, cell in cells8.items()
    }

    return cells8, cells7


def _make_canonical_tripdataset() -> TripDataset:
    """Construye el TripDataset canónico rico usado en los integration tests de OP-13."""
    cells8, _ = _build_integration_cells()

    data = pd.DataFrame(
        [
            {
                "movement_id": "m0",
                "trip_id": "t0",
                "movement_seq": 0,
                "user_id": "u0",
                "origin_h3_index": cells8["A"],
                "destination_h3_index": cells8["B"],
                "origin_time_utc": "2026-01-01T08:05:00Z",
                "destination_time_utc": "2026-01-01T08:25:00Z",
                "mode": "bus",
                "purpose": "work",
                "day_type": "weekday",
                "time_period": "AM",
                "user_gender": "female",
                "trip_weight": 1.0,
                "origin_municipality": "Santiago",
                "destination_municipality": "Providencia",
            },
            {
                "movement_id": "m1",
                "trip_id": "t1",
                "movement_seq": 0,
                "user_id": "u1",
                "origin_h3_index": cells8["A"],
                "destination_h3_index": cells8["B"],
                "origin_time_utc": "2026-01-01T08:15:00Z",
                "destination_time_utc": "2026-01-01T08:40:00Z",
                "mode": "bus",
                "purpose": "work",
                "day_type": "weekday",
                "time_period": "AM",
                "user_gender": "male",
                "trip_weight": 1.2,
                "origin_municipality": "Santiago",
                "destination_municipality": "Providencia",
            },
            {
                "movement_id": "m2",
                "trip_id": "t2",
                "movement_seq": 0,
                "user_id": "u2",
                "origin_h3_index": cells8["A"],
                "destination_h3_index": cells8["C"],
                "origin_time_utc": "2026-01-01T09:10:00Z",
                "destination_time_utc": "2026-01-01T09:35:00Z",
                "mode": "metro",
                "purpose": "study",
                "day_type": "weekday",
                "time_period": "AM",
                "user_gender": "female",
                "trip_weight": 0.8,
                "origin_municipality": "Santiago",
                "destination_municipality": "Ñuñoa",
            },
            {
                "movement_id": "m3",
                "trip_id": "t3",
                "movement_seq": 0,
                "user_id": "u3",
                "origin_h3_index": cells8["D"],
                "destination_h3_index": cells8["E"],
                "origin_time_utc": "2026-01-01T10:05:00Z",
                "destination_time_utc": "2026-01-01T10:25:00Z",
                "mode": "walk",
                "purpose": "leisure",
                "day_type": "weekday",
                "time_period": "AM",
                "user_gender": "male",
                "trip_weight": 1.0,
                "origin_municipality": "Las Condes",
                "destination_municipality": "Las Condes",
            },
            {
                "movement_id": "m4",
                "trip_id": "t4",
                "movement_seq": 0,
                "user_id": "u4",
                "origin_h3_index": cells8["F"],
                "destination_h3_index": cells8["G"],
                "origin_time_utc": "2026-01-01T18:10:00Z",
                "destination_time_utc": "2026-01-01T18:40:00Z",
                "mode": "bike",
                "purpose": "leisure",
                "day_type": "weekday",
                "time_period": "PM",
                "user_gender": "female",
                "trip_weight": 1.0,
                "origin_municipality": "Providencia",
                "destination_municipality": "Ñuñoa",
            },
            {
                "movement_id": "m5",
                "trip_id": "t5",
                "movement_seq": 0,
                "user_id": "u5",
                "origin_h3_index": cells8["F"],
                "destination_h3_index": cells8["G"],
                "origin_time_utc": "2026-01-01T18:20:00Z",
                "destination_time_utc": "2026-01-01T18:45:00Z",
                "mode": "bike",
                "purpose": "leisure",
                "day_type": "weekday",
                "time_period": "PM",
                "user_gender": "male",
                "trip_weight": 1.5,
                "origin_municipality": "Providencia",
                "destination_municipality": "Ñuñoa",
            },
            {
                "movement_id": "m6",
                "trip_id": "t6",
                "movement_seq": 0,
                "user_id": "u6",
                "origin_h3_index": cells8["H"],
                "destination_h3_index": cells8["A"],
                "origin_time_utc": "2026-01-01T08:50:00Z",
                "destination_time_utc": "2026-01-01T09:15:00Z",
                "mode": "bus",
                "purpose": "work",
                "day_type": "weekday",
                "time_period": "AM",
                "user_gender": "female",
                "trip_weight": 1.0,
                "origin_municipality": "Santiago",
                "destination_municipality": "Santiago",
            },
            {
                "movement_id": "m7",
                "trip_id": "t7",
                "movement_seq": 0,
                "user_id": "u7",
                "origin_h3_index": cells8["B"],
                "destination_h3_index": cells8["A"],
                "origin_time_utc": "2026-01-01T07:55:00Z",
                "destination_time_utc": "2026-01-01T08:10:00Z",
                "mode": "bus",
                "purpose": "work",
                "day_type": "weekday",
                "time_period": "AM",
                "user_gender": "male",
                "trip_weight": 1.0,
                "origin_municipality": "Providencia",
                "destination_municipality": "Santiago",
            },
        ]
    )

    return TripDataset(
        data=data,
        schema=TripSchema(
            version="0.1.0",
            fields={},
            required=[],
        ),
        metadata={
            "dataset_id": "tripdataset_canonical_small",
            "is_validated": True,
            "temporal": {"tier": "tier_1"},
            "events": [
                {
                    "op": "import_trips",
                    "ts_utc": "2026-04-08T00:00:00Z",
                    "parameters": {"profile": "synthetic_integration"},
                    "summary": {"rows_out": 8},
                }
            ],
        },
        provenance={
            "source": "synthetic",
            "kind": "integration_test",
        },
    )


def _base_integration_aggregation_spec() -> dict[str, Any]:
    """Construye la aggregation_spec horaria compartida por fixtures ricas de OP-13."""
    return {
        "h3_resolution": 8,
        "group_by": ["mode", "purpose"],
        "time_aggregation": "hour",
        "time_basis": "origin",
        "effective_flow_keys": [
            "origin_h3_index",
            "destination_h3_index",
            "window_start_utc",
            "window_end_utc",
            "mode",
            "purpose",
        ],
    }


def _base_integration_provenance() -> dict[str, Any]:
    """Construye el provenance común de flows ricos derivados del fixture canónico."""
    return {
        "derived_from": [
            {
                "type": "trips",
                "dataset_id": "tripdataset_canonical_small",
            }
        ]
    }


def _make_flowdataset_with_trip_links() -> FlowDataset:
    """Construye el FlowDataset rico con `flow_to_trips` materializado."""
    cells8, _ = _build_integration_cells()

    flows_df = pd.DataFrame(
        [
            {
                "flow_id": "f_ab_bus_work_h08",
                "origin_h3_index": cells8["A"],
                "destination_h3_index": cells8["B"],
                "window_start_utc": pd.Timestamp("2026-01-01 08:00:00"),
                "window_end_utc": pd.Timestamp("2026-01-01 09:00:00"),
                "mode": "bus",
                "purpose": "work",
                "flow_count": 2,
                "flow_value": 2.2,
            },
            {
                "flow_id": "f_ac_metro_study_h09",
                "origin_h3_index": cells8["A"],
                "destination_h3_index": cells8["C"],
                "window_start_utc": pd.Timestamp("2026-01-01 09:00:00"),
                "window_end_utc": pd.Timestamp("2026-01-01 10:00:00"),
                "mode": "metro",
                "purpose": "study",
                "flow_count": 1,
                "flow_value": 0.8,
            },
            {
                "flow_id": "f_fg_bike_leisure_h18",
                "origin_h3_index": cells8["F"],
                "destination_h3_index": cells8["G"],
                "window_start_utc": pd.Timestamp("2026-01-01 18:00:00"),
                "window_end_utc": pd.Timestamp("2026-01-01 19:00:00"),
                "mode": "bike",
                "purpose": "leisure",
                "flow_count": 2,
                "flow_value": 2.5,
            },
        ]
    )

    flow_to_trips_df = pd.DataFrame(
        [
            {
                "flow_id": "f_ab_bus_work_h08",
                "movement_id": "m0",
            },
            {
                "flow_id": "f_ab_bus_work_h08",
                "movement_id": "m1",
            },
            {
                "flow_id": "f_ac_metro_study_h09",
                "movement_id": "m2",
            },
            {
                "flow_id": "f_fg_bike_leisure_h18",
                "movement_id": "m4",
            },
            {
                "flow_id": "f_fg_bike_leisure_h18",
                "movement_id": "m5",
            },
        ]
    )

    return FlowDataset(
        flows=flows_df,
        flow_to_trips=flow_to_trips_df,
        aggregation_spec=_base_integration_aggregation_spec(),
        source_trips=None,
        metadata={
            "dataset_id": "flowdataset_with_trip_links",
            "is_validated": False,
            "events": [
                {
                    "op": "build_flows",
                    "ts_utc": "2026-04-08T00:10:00Z",
                    "parameters": {"keep_flow_to_trips": True},
                    "summary": {"n_flows_out": 3},
                }
            ],
        },
        provenance=_base_integration_provenance(),
    )


def _make_flowdataset_small() -> FlowDataset:
    """Construye el FlowDataset rico sin auxiliar, incluyendo un flow sin correspondencia."""
    cells8, _ = _build_integration_cells()

    flows_df = pd.DataFrame(
        [
            {
                "flow_id": "f_ab_bus_work_h08",
                "origin_h3_index": cells8["A"],
                "destination_h3_index": cells8["B"],
                "window_start_utc": pd.Timestamp("2026-01-01 08:00:00"),
                "window_end_utc": pd.Timestamp("2026-01-01 09:00:00"),
                "mode": "bus",
                "purpose": "work",
                "flow_count": 2,
                "flow_value": 2.2,
            },
            {
                "flow_id": "f_ac_metro_study_h09",
                "origin_h3_index": cells8["A"],
                "destination_h3_index": cells8["C"],
                "window_start_utc": pd.Timestamp("2026-01-01 09:00:00"),
                "window_end_utc": pd.Timestamp("2026-01-01 10:00:00"),
                "mode": "metro",
                "purpose": "study",
                "flow_count": 1,
                "flow_value": 0.8,
            },
            {
                "flow_id": "f_fg_bike_leisure_h18",
                "origin_h3_index": cells8["F"],
                "destination_h3_index": cells8["G"],
                "window_start_utc": pd.Timestamp("2026-01-01 18:00:00"),
                "window_end_utc": pd.Timestamp("2026-01-01 19:00:00"),
                "mode": "bike",
                "purpose": "leisure",
                "flow_count": 2,
                "flow_value": 2.5,
            },
            {
                "flow_id": "f_unmatched_walk_other_h11",
                "origin_h3_index": cells8["Z1"],
                "destination_h3_index": cells8["Z2"],
                "window_start_utc": pd.Timestamp("2026-01-01 11:00:00"),
                "window_end_utc": pd.Timestamp("2026-01-01 12:00:00"),
                "mode": "walk",
                "purpose": "other",
                "flow_count": 7,
                "flow_value": 7.0,
            },
        ]
    )

    return FlowDataset(
        flows=flows_df,
        flow_to_trips=None,
        aggregation_spec=_base_integration_aggregation_spec(),
        source_trips=None,
        metadata={
            "dataset_id": "flowdataset_small",
            "is_validated": False,
            "events": [
                {
                    "op": "build_flows",
                    "ts_utc": "2026-04-08T00:20:00Z",
                    "parameters": {"keep_flow_to_trips": False},
                    "summary": {"n_flows_out": 4},
                }
            ],
        },
        provenance=_base_integration_provenance(),
    )


def _make_flowdataset_source_trips_only() -> FlowDataset:
    """Construye un FlowDataset rico que usa `source_trips` como único fallback vivo."""
    source_trips = _make_canonical_tripdataset()
    base_flows = _make_flowdataset_small()

    return FlowDataset(
        flows=base_flows.flows.copy(deep=True),
        flow_to_trips=None,
        aggregation_spec=deepcopy(base_flows.aggregation_spec),
        source_trips=source_trips,
        metadata=deepcopy(base_flows.metadata),
        provenance=deepcopy(base_flows.provenance),
    )


def _make_flowdataset_rollup_temporal() -> FlowDataset:
    """Construye un FlowDataset con roll-up H3 a resolución 7 y agregación temporal diaria."""
    _, cells7 = _build_integration_cells()

    flows_df = pd.DataFrame(
        [
            {
                "flow_id": "f_parent_ab_bus_work_d1",
                "origin_h3_index": cells7["A"],
                "destination_h3_index": cells7["B"],
                "window_start_utc": pd.Timestamp("2026-01-01 00:00:00"),
                "window_end_utc": pd.Timestamp("2026-01-02 00:00:00"),
                "mode": "bus",
                "purpose": "work",
                "flow_count": 2,
                "flow_value": 2.2,
            },
            {
                "flow_id": "f_parent_fg_bike_leisure_d1",
                "origin_h3_index": cells7["F"],
                "destination_h3_index": cells7["G"],
                "window_start_utc": pd.Timestamp("2026-01-01 00:00:00"),
                "window_end_utc": pd.Timestamp("2026-01-02 00:00:00"),
                "mode": "bike",
                "purpose": "leisure",
                "flow_count": 2,
                "flow_value": 2.5,
            },
        ]
    )

    return FlowDataset(
        flows=flows_df,
        flow_to_trips=None,
        aggregation_spec={
            "h3_resolution": 7,
            "group_by": ["mode", "purpose"],
            "time_aggregation": "day",
            "time_basis": "origin",
            "effective_flow_keys": [
                "origin_h3_index",
                "destination_h3_index",
                "window_start_utc",
                "window_end_utc",
                "mode",
                "purpose",
            ],
        },
        source_trips=None,
        metadata={
            "dataset_id": "flowdataset_rollup_temporal",
            "is_validated": False,
            "events": [
                {
                    "op": "build_flows",
                    "ts_utc": "2026-04-08T00:30:00Z",
                    "parameters": {
                        "h3_resolution": 7,
                        "time_aggregation": "day",
                    },
                    "summary": {"n_flows_out": 2},
                }
            ],
        },
        provenance=_base_integration_provenance(),
    )


# -----------------------------------------------------------------------------
# Fixtures expuestas a los archivos de tests de OP-13
# -----------------------------------------------------------------------------


@pytest.fixture
def h3_from_latlon() -> Callable[[float, float, int], str]:
    """Entrega el helper H3 para construir celdas desde latitud, longitud y resolución."""
    return _h3_from_latlon


@pytest.fixture
def h3_to_parent() -> Callable[[str, int], str]:
    """Entrega el helper H3 para resolver parents a una resolución más gruesa."""
    return _h3_to_parent


@pytest.fixture
def assert_issue_codes() -> Callable[[Sequence[Issue], Sequence[str]], None]:
    """Entrega el helper que valida secuencias exactas de códigos de issue."""
    return _assert_issue_codes


@pytest.fixture
def op13_request_ctx_factory() -> Callable[..., dict[str, Any]]:
    """Entrega la factory de request context usada por helpers internos de OP-13."""
    return _make_request_ctx


@pytest.fixture
def op13_small_tripdataset_factory() -> Callable[..., TripDataset]:
    """Entrega la factory del TripDataset pequeño para helper-level y smoke."""
    return _make_op13_test_tripdataset


@pytest.fixture
def op13_small_flowdataset_factory() -> Callable[..., FlowDataset]:
    """Entrega la factory del FlowDataset pequeño para helper-level y smoke."""
    return _make_op13_test_flowdataset


@pytest.fixture
def snapshot_flowdataset_state() -> Callable[[FlowDataset], dict[str, Any]]:
    """Entrega el helper que captura el estado observable de un FlowDataset."""
    return _snapshot_flowdataset_state


@pytest.fixture
def snapshot_tripdataset_state() -> Callable[[TripDataset], dict[str, Any]]:
    """Entrega el helper que captura el estado observable de un TripDataset."""
    return _snapshot_tripdataset_state


@pytest.fixture
def canonical_tripdataset_factory() -> Callable[[], TripDataset]:
    """Entrega la factory del TripDataset canónico rico para integración."""
    return _make_canonical_tripdataset


@pytest.fixture
def flowdataset_with_trip_links_factory() -> Callable[[], FlowDataset]:
    """Entrega la factory del FlowDataset rico con `flow_to_trips` directo."""
    return _make_flowdataset_with_trip_links


@pytest.fixture
def flowdataset_small_factory() -> Callable[[], FlowDataset]:
    """Entrega la factory del FlowDataset rico sin auxiliar y con cobertura parcial."""
    return _make_flowdataset_small


@pytest.fixture
def flowdataset_source_trips_only_factory() -> Callable[[], FlowDataset]:
    """Entrega la factory del FlowDataset que depende de `flows.source_trips`."""
    return _make_flowdataset_source_trips_only


@pytest.fixture
def flowdataset_rollup_temporal_factory() -> Callable[[], FlowDataset]:
    """Entrega la factory del FlowDataset con roll-up H3 y agregación diaria."""
    return _make_flowdataset_rollup_temporal