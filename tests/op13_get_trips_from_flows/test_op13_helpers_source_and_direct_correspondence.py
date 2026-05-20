from __future__ import annotations

import pandas as pd
import pytest

from pylondrina.errors import PylondrinaError
from pylondrina.queries.flows import (
    _extract_correspondence_from_flow_to_trips,
    _resolve_correspondence_source,
)


# -----------------------------------------------------------------------------
# Bloque 2.1 a 2.4 - Resolución de fuente efectiva
# -----------------------------------------------------------------------------


def test_resolve_correspondence_source_uses_flow_to_trips_when_direct_auxiliary_is_usable(
    op13_small_flowdataset_factory,
    op13_request_ctx_factory,
):
    """Verifica prioridad de `flow_to_trips` cuando el auxiliar directo es usable."""
    flows = op13_small_flowdataset_factory(
        include_flow_to_trips=True,
        duplicate_direct_pairs=False,
        include_extra_unmatched_flow=False,
    )

    issues = []
    request_ctx = op13_request_ctx_factory(
        n_flows_input=len(flows.flows),
    )

    used_source, source_obj, reconstruction_attempted, n_trips_input = (
        _resolve_correspondence_source(
            flows,
            trips=None,
            issues=issues,
            request_ctx=request_ctx,
        )
    )

    assert used_source == "flow_to_trips"
    assert source_obj is flows.flow_to_trips
    assert reconstruction_attempted is False
    assert n_trips_input is None
    assert issues == []


def test_resolve_correspondence_source_falls_back_to_trips_argument_when_direct_auxiliary_is_unusable(
    op13_small_flowdataset_factory,
    op13_small_tripdataset_factory,
    op13_request_ctx_factory,
    assert_issue_codes,
):
    """Verifica fallback a `trips_argument` y warning si `flow_to_trips` existe pero no es usable."""
    trips = op13_small_tripdataset_factory()

    flows = op13_small_flowdataset_factory(
        include_flow_to_trips=True,
        include_extra_unmatched_flow=False,
    )
    flows.flow_to_trips = flows.flow_to_trips.loc[:, ["flow_id"]].copy()

    issues = []
    request_ctx = op13_request_ctx_factory(
        n_flows_input=len(flows.flows),
    )

    used_source, source_obj, reconstruction_attempted, n_trips_input = (
        _resolve_correspondence_source(
            flows,
            trips=trips,
            issues=issues,
            request_ctx=request_ctx,
        )
    )

    assert used_source == "trips_argument"
    assert source_obj is trips
    assert reconstruction_attempted is True
    assert n_trips_input == len(trips.data)

    assert_issue_codes(
        issues,
        ["GET_TRIPS_FROM_FLOWS.SOURCE.PREFERRED_SOURCE_UNUSABLE"],
    )


def test_resolve_correspondence_source_uses_flows_source_trips_as_third_fallback(
    op13_small_flowdataset_factory,
    op13_small_tripdataset_factory,
    op13_request_ctx_factory,
):
    """Verifica uso de `flows.source_trips` cuando no hay auxiliar usable ni trips explícito."""
    source_trips = op13_small_tripdataset_factory()

    flows = op13_small_flowdataset_factory(
        include_flow_to_trips=False,
        include_extra_unmatched_flow=False,
        source_trips=source_trips,
    )

    issues = []
    request_ctx = op13_request_ctx_factory(
        n_flows_input=len(flows.flows),
    )

    used_source, source_obj, reconstruction_attempted, n_trips_input = (
        _resolve_correspondence_source(
            flows,
            trips=None,
            issues=issues,
            request_ctx=request_ctx,
        )
    )

    assert used_source == "flows.source_trips"
    assert source_obj is source_trips
    assert reconstruction_attempted is True
    assert n_trips_input == len(source_trips.data)
    assert issues == []


def test_resolve_correspondence_source_raises_when_no_usable_source_exists(
    op13_small_flowdataset_factory,
    op13_request_ctx_factory,
    assert_issue_codes,
):
    """Verifica error fatal cuando no existe ninguna fuente usable de correspondencia."""
    flows = op13_small_flowdataset_factory(
        include_flow_to_trips=False,
        include_extra_unmatched_flow=False,
        source_trips=None,
    )

    issues = []
    request_ctx = op13_request_ctx_factory(
        n_flows_input=len(flows.flows),
    )

    with pytest.raises(PylondrinaError) as excinfo:
        _resolve_correspondence_source(
            flows,
            trips=None,
            issues=issues,
            request_ctx=request_ctx,
        )

    error = excinfo.value

    assert error.issue is not None
    assert error.issue.code == "GET_TRIPS_FROM_FLOWS.SOURCE.NO_USABLE_SOURCE"

    assert_issue_codes(
        issues,
        ["GET_TRIPS_FROM_FLOWS.SOURCE.NO_USABLE_SOURCE"],
    )


# -----------------------------------------------------------------------------
# Bloque 2.5 - Consumo directo desde flow_to_trips
# -----------------------------------------------------------------------------


def test_extract_correspondence_from_flow_to_trips_keeps_minimal_contract_and_deduplicates_exact_pairs(
    op13_small_flowdataset_factory,
    op13_request_ctx_factory,
    assert_issue_codes,
):
    """Verifica contrato mínimo, deduplicación exacta y warning agregado del auxiliar directo."""
    flows = op13_small_flowdataset_factory(
        include_flow_to_trips=True,
        duplicate_direct_pairs=True,
        include_extra_unmatched_flow=False,
    )

    issues = []
    request_ctx = op13_request_ctx_factory(
        n_flows_input=len(flows.flows),
        used_source="flow_to_trips",
        reconstruction_attempted=False,
    )

    provisional = _extract_correspondence_from_flow_to_trips(
        flows.flow_to_trips,
        issues=issues,
        request_ctx=request_ctx,
    )

    expected = (
        flows.flow_to_trips.loc[:, ["flow_id", "movement_id"]]
        .drop_duplicates(subset=["flow_id", "movement_id"], keep="first")
        .reset_index(drop=True)
    )

    assert provisional.columns.tolist() == ["flow_id", "movement_id"]

    pd.testing.assert_frame_equal(
        provisional,
        expected,
        check_dtype=False,
        check_categorical=False,
    )

    assert_issue_codes(
        issues,
        ["GET_TRIPS_FROM_FLOWS.SOURCE.DUPLICATE_PAIRS_NORMALIZED"],
    )