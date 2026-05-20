from __future__ import annotations

from copy import deepcopy
from typing import Any, Callable, Sequence

import h3
import pandas as pd
import pytest

from pylondrina.datasets import FlowDataset
from pylondrina.reports import Issue


# -----------------------------------------------------------------------------
# Helpers de construcción y verificación compartidos por OP-12
# -----------------------------------------------------------------------------


def _h3_from_latlon(lat: float, lon: float, res: int = 8) -> str:
    """Construye una celda H3 compatible con las APIs v3 y v4 de h3-py."""
    if hasattr(h3, "latlng_to_cell"):
        return h3.latlng_to_cell(lat, lon, res)
    return h3.geo_to_h3(lat, lon, res)


def _assert_issue_codes(
    issues: Sequence[Issue],
    expected_present: Sequence[str] = (),
    expected_absent: Sequence[str] = (),
) -> None:
    """Verifica presencia y ausencia de códigos de issue en una secuencia dada."""
    codes = [issue.code for issue in issues]

    for code in expected_present:
        assert code in codes, f"Falta issue {code}. Codes emitidos: {codes}"

    for code in expected_absent:
        assert code not in codes, f"No debía aparecer {code}. Codes emitidos: {codes}"


def _make_flowdataset_small() -> tuple[FlowDataset, dict[str, str]]:
    """Construye el FlowDataset pequeño usado en helpers y smoke tests de OP-12."""
    origin_a = _h3_from_latlon(-33.4500, -70.6500, 8)
    origin_b = _h3_from_latlon(-33.4400, -70.6400, 8)
    origin_c = _h3_from_latlon(-33.4600, -70.6600, 8)

    dest_a = _h3_from_latlon(-33.4300, -70.6300, 8)
    dest_b = _h3_from_latlon(-33.4700, -70.6200, 8)

    flows_df = pd.DataFrame(
        {
            "flow_id": ["f1", "f2", "f3", "f4"],
            "origin_h3_index": [origin_a, origin_a, origin_b, origin_c],
            "destination_h3_index": [dest_a, dest_b, dest_b, origin_a],
            "flow_count": [10, 5, 2, 1],
            "flow_value": [15.0, 5.0, 1.0, 3.5],
            "mode": ["bus", "metro", "walk", "bus"],
            "gender": ["F", "M", "F", None],
            "window_start_utc": pd.to_datetime(
                [
                    "2026-01-01T10:00:00Z",
                    "2026-01-01T12:00:00Z",
                    "2026-01-02T08:00:00Z",
                    "2026-01-03T09:00:00Z",
                ],
                utc=True,
            ),
            "window_end_utc": pd.to_datetime(
                [
                    "2026-01-01T10:30:00Z",
                    "2026-01-01T12:45:00Z",
                    "2026-01-02T08:15:00Z",
                    "2026-01-03T09:20:00Z",
                ],
                utc=True,
            ),
        }
    )

    flow_to_trips = pd.DataFrame(
        {
            "flow_id": ["f1", "f1", "f2", "f4"],
            "movement_id": ["m1", "m2", "m3", "m4"],
        }
    )

    metadata = {
        "dataset_id": "flows_demo",
        "artifact_id": "artifact_demo",
        "is_validated": False,
        "events": [
            {"op": "build_flows", "ts_utc": "2026-04-07T10:00:00Z"},
            {"op": "write_flows", "ts_utc": "2026-04-07T10:10:00Z"},
        ],
        "h3": {"resolution": 8},
    }

    aggregation_spec = {
        "h3_resolution": 8,
        "group_by": ["mode", "gender"],
    }

    provenance = {
        "source": "synthetic",
        "note": "fixture helper-level",
    }

    dataset = FlowDataset(
        flows=flows_df,
        flow_to_trips=flow_to_trips,
        aggregation_spec=aggregation_spec,
        source_trips=None,
        metadata=metadata,
        provenance=provenance,
    )

    cells = {
        "origin_a": origin_a,
        "origin_b": origin_b,
        "origin_c": origin_c,
        "dest_a": dest_a,
        "dest_b": dest_b,
    }

    return dataset, cells


def _make_request_ctx(
    flows: FlowDataset,
    *,
    strict: bool = False,
    where_provided: bool = True,
    h3_cells_provided: bool = False,
    spatial_predicate: str = "origin",
    keep_flow_to_trips: bool = True,
    keep_metadata: bool = True,
    max_issues: int = 1000,
) -> dict[str, Any]:
    """Construye el request context usado por helpers internos de OP-12."""
    metadata = flows.metadata

    return {
        "dataset_id": metadata.get("dataset_id"),
        "artifact_id": metadata.get("artifact_id"),
        "strict": strict,
        "where_provided": where_provided,
        "h3_cells_provided": h3_cells_provided,
        "spatial_predicate": spatial_predicate,
        "keep_flow_to_trips": keep_flow_to_trips,
        "keep_metadata": keep_metadata,
        "max_issues": max_issues,
    }


def _get_last_event(flows: FlowDataset) -> dict[str, Any]:
    """Retorna el último evento registrado en `metadata['events']`."""
    events = flows.metadata.get("events", [])
    assert isinstance(events, list) and len(events) > 0, (
        "No hay eventos en metadata['events']"
    )
    return events[-1]


def _snapshot_flowdataset_state(flows: FlowDataset) -> dict[str, Any]:
    """Captura el estado relevante de un FlowDataset antes de ejecutar una operación."""
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
    }


def _assert_flowdataset_input_intact(
    flows: FlowDataset,
    snapshot: dict[str, Any],
) -> None:
    """Verifica que un FlowDataset de entrada permanezca sin mutaciones observables."""
    pd.testing.assert_frame_equal(flows.flows, snapshot["flows"])

    if snapshot["flow_to_trips"] is None:
        assert flows.flow_to_trips is None
    else:
        pd.testing.assert_frame_equal(
            flows.flow_to_trips,
            snapshot["flow_to_trips"],
        )

    assert flows.aggregation_spec == snapshot["aggregation_spec"]
    assert flows.metadata == snapshot["metadata"]
    assert flows.provenance == snapshot["provenance"]


# -----------------------------------------------------------------------------
# Fixtures ricos de FlowDataset para integration tests
# -----------------------------------------------------------------------------


def _build_base_flows_df() -> tuple[pd.DataFrame, dict[str, str]]:
    """Construye la tabla base rica y su mapa de celdas H3 para integración."""
    origin_a = _h3_from_latlon(-33.4500, -70.6500, 8)
    origin_b = _h3_from_latlon(-33.4400, -70.6400, 8)
    origin_c = _h3_from_latlon(-33.4600, -70.6600, 8)
    origin_d = _h3_from_latlon(-33.4700, -70.6700, 8)

    dest_a = _h3_from_latlon(-33.4300, -70.6300, 8)
    dest_b = _h3_from_latlon(-33.4200, -70.6200, 8)
    dest_c = _h3_from_latlon(-33.4100, -70.6100, 8)

    flows_df = pd.DataFrame(
        {
            "flow_id": [
                "f01",
                "f02",
                "f03",
                "f04",
                "f05",
                "f06",
                "f07",
                "f08",
                "f09",
                "f10",
                "f11",
                "f12",
            ],
            "origin_h3_index": [
                origin_a,
                origin_a,
                origin_a,
                origin_b,
                origin_b,
                origin_b,
                origin_c,
                origin_c,
                origin_d,
                origin_d,
                origin_a,
                origin_c,
            ],
            "destination_h3_index": [
                dest_a,
                dest_b,
                dest_c,
                dest_a,
                dest_b,
                origin_a,
                dest_b,
                dest_c,
                dest_a,
                dest_b,
                origin_a,
                origin_a,
            ],
            "flow_count": [12, 7, 3, 9, 2, 1, 15, 5, 4, 6, 8, 11],
            "flow_value": [
                18.0,
                7.0,
                4.0,
                20.0,
                2.0,
                5.0,
                30.0,
                8.0,
                4.0,
                12.0,
                8.0,
                50.0,
            ],
            "mode": [
                "bus",
                "metro",
                "walk",
                "bus",
                "bike",
                "bus",
                "car",
                "bus",
                "metro",
                "bus",
                "scooter",
                "bus",
            ],
            "purpose": [
                "work",
                "work",
                "education",
                "work",
                "leisure",
                "health",
                "work",
                "leisure",
                "education",
                "work",
                "shopping",
                "work",
            ],
            "gender": [
                "F",
                "M",
                "F",
                "M",
                "F",
                None,
                "M",
                "F",
                "F",
                "F",
                "M",
                "F",
            ],
            "day_type": [
                "weekday",
                "weekday",
                "weekday",
                "weekday",
                "weekend",
                "weekday",
                "weekday",
                "weekend",
                "weekday",
                "weekday",
                "weekend",
                "weekday",
            ],
            "time_period": [
                "am_peak",
                "am_peak",
                "am_peak",
                "am_peak",
                "midday",
                "midday",
                "pm_peak",
                "pm_peak",
                "am_peak",
                "am_peak",
                "midday",
                "pm_peak",
            ],
            "corridor": [
                "north",
                "north",
                "north",
                "west",
                "west",
                "west",
                "south",
                "south",
                "east",
                "east",
                "north",
                "south",
            ],
            "window_start_utc": pd.to_datetime(
                [
                    "2026-01-01T10:00:00Z",
                    "2026-01-01T10:30:00Z",
                    "2026-01-01T11:00:00Z",
                    "2026-01-01T12:00:00Z",
                    "2026-01-02T15:00:00Z",
                    "2026-01-02T16:00:00Z",
                    "2026-01-03T22:00:00Z",
                    "2026-01-03T22:30:00Z",
                    "2026-01-04T09:00:00Z",
                    "2026-01-04T09:15:00Z",
                    "2026-01-04T17:00:00Z",
                    "2026-01-05T23:00:00Z",
                ],
                utc=True,
            ),
            "window_end_utc": pd.to_datetime(
                [
                    "2026-01-01T10:40:00Z",
                    "2026-01-01T11:10:00Z",
                    "2026-01-01T11:20:00Z",
                    "2026-01-01T12:45:00Z",
                    "2026-01-02T15:20:00Z",
                    "2026-01-02T16:10:00Z",
                    "2026-01-03T22:30:00Z",
                    "2026-01-03T22:50:00Z",
                    "2026-01-04T09:20:00Z",
                    "2026-01-04T09:55:00Z",
                    "2026-01-04T17:25:00Z",
                    "2026-01-05T23:40:00Z",
                ],
                utc=True,
            ),
        }
    )

    cells = {
        "origin_a": origin_a,
        "origin_b": origin_b,
        "origin_c": origin_c,
        "origin_d": origin_d,
        "dest_a": dest_a,
        "dest_b": dest_b,
        "dest_c": dest_c,
    }

    return flows_df, cells


def _build_flow_to_trips_df() -> pd.DataFrame:
    """Construye el auxiliar rico `flow_to_trips` de las pruebas de integración."""
    rows: list[dict[str, str]] = []

    n_links_by_flow = {
        "f01": 3,
        "f02": 2,
        "f03": 1,
        "f04": 3,
        "f05": 1,
        "f06": 1,
        "f07": 4,
        "f08": 2,
        "f09": 1,
        "f10": 2,
        "f11": 2,
        "f12": 3,
    }

    for flow_id, n_links in n_links_by_flow.items():
        for i in range(1, n_links + 1):
            rows.append(
                {
                    "flow_id": flow_id,
                    "movement_id": f"m_{flow_id}_{i:02d}",
                }
            )

    return pd.DataFrame(rows)


def _base_metadata(is_validated: bool = False) -> dict[str, Any]:
    """Construye metadata base rica para fixtures de integración."""
    return {
        "dataset_id": "flows_rich_demo",
        "artifact_id": "artifact_rich_demo",
        "is_validated": bool(is_validated),
        "events": [
            {"op": "build_flows", "ts_utc": "2026-04-07T10:00:00Z"},
            {"op": "write_flows", "ts_utc": "2026-04-07T10:10:00Z"},
        ],
        "h3": {"resolution": 8},
        "custom_tag": "integration_fixture",
    }


def _base_aggregation_spec() -> dict[str, Any]:
    """Construye la aggregation_spec base de los FlowDataset ricos."""
    return {
        "h3_resolution": 8,
        "group_by": [
            "mode",
            "purpose",
            "gender",
            "day_type",
            "time_period",
        ],
    }


def _base_provenance() -> dict[str, Any]:
    """Construye el provenance base de los FlowDataset ricos."""
    return {
        "source": "synthetic_manual_fixture",
        "note": "integration tests OP-12",
    }


def _make_flowdataset_segmented_rich() -> tuple[FlowDataset, dict[str, str]]:
    """Construye el FlowDataset rico segmentado sin auxiliar `flow_to_trips`."""
    flows_df, cells = _build_base_flows_df()

    dataset = FlowDataset(
        flows=flows_df,
        flow_to_trips=None,
        aggregation_spec=_base_aggregation_spec(),
        source_trips=None,
        metadata=_base_metadata(is_validated=False),
        provenance=_base_provenance(),
    )

    return dataset, cells


def _make_flowdataset_with_trip_links_rich() -> tuple[FlowDataset, dict[str, str]]:
    """Construye el FlowDataset rico con auxiliar `flow_to_trips` utilizable."""
    flows_df, cells = _build_base_flows_df()
    flow_to_trips_df = _build_flow_to_trips_df()

    dataset = FlowDataset(
        flows=flows_df,
        flow_to_trips=flow_to_trips_df,
        aggregation_spec=_base_aggregation_spec(),
        source_trips=None,
        metadata=_base_metadata(is_validated=False),
        provenance=_base_provenance(),
    )

    return dataset, cells


def _make_flowdataset_validated_rich() -> tuple[FlowDataset, dict[str, str]]:
    """Construye el FlowDataset rico validado usado en pruebas de metadata y eventos."""
    flows_df, cells = _build_base_flows_df()
    flow_to_trips_df = _build_flow_to_trips_df()

    dataset = FlowDataset(
        flows=flows_df,
        flow_to_trips=flow_to_trips_df,
        aggregation_spec=_base_aggregation_spec(),
        source_trips=None,
        metadata=_base_metadata(is_validated=True),
        provenance=_base_provenance(),
    )

    return dataset, cells


# -----------------------------------------------------------------------------
# Fixtures expuestas a los archivos de tests de OP-12
# -----------------------------------------------------------------------------


@pytest.fixture
def small_flowdataset_factory() -> Callable[[], tuple[FlowDataset, dict[str, str]]]:
    """Entrega la factory del FlowDataset pequeño usado en helper-level y smoke."""
    return _make_flowdataset_small


@pytest.fixture
def request_ctx_factory() -> Callable[..., dict[str, Any]]:
    """Entrega la factory de request context usada por helpers internos de OP-12."""
    return _make_request_ctx


@pytest.fixture
def assert_issue_codes() -> Callable[..., None]:
    """Entrega el helper para verificar presencia y ausencia de códigos de issue."""
    return _assert_issue_codes


@pytest.fixture
def get_last_event() -> Callable[[FlowDataset], dict[str, Any]]:
    """Entrega el helper para recuperar el último evento de un FlowDataset."""
    return _get_last_event


@pytest.fixture
def snapshot_flowdataset_state() -> Callable[[FlowDataset], dict[str, Any]]:
    """Entrega el helper que captura el estado observable del input antes de filtrar."""
    return _snapshot_flowdataset_state


@pytest.fixture
def assert_flowdataset_input_intact() -> Callable[
    [FlowDataset, dict[str, Any]],
    None,
]:
    """Entrega el helper que verifica no mutación del FlowDataset de entrada."""
    return _assert_flowdataset_input_intact


@pytest.fixture
def rich_segmented_flowdataset_factory() -> Callable[
    [],
    tuple[FlowDataset, dict[str, str]],
]:
    """Entrega la factory del FlowDataset rico segmentado sin auxiliar."""
    return _make_flowdataset_segmented_rich


@pytest.fixture
def rich_flowdataset_with_links_factory() -> Callable[
    [],
    tuple[FlowDataset, dict[str, str]],
]:
    """Entrega la factory del FlowDataset rico con `flow_to_trips`."""
    return _make_flowdataset_with_trip_links_rich


@pytest.fixture
def rich_validated_flowdataset_factory() -> Callable[
    [],
    tuple[FlowDataset, dict[str, str]],
]:
    """Entrega la factory del FlowDataset rico con `is_validated=True`."""
    return _make_flowdataset_validated_rich