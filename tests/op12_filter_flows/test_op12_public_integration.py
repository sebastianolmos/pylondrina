from __future__ import annotations

from copy import deepcopy

import pandas as pd
import pytest

from pylondrina.datasets import FlowDataset
from pylondrina.errors import FilterError
from pylondrina.transforms.flows_filtering import (
    FlowFilterOptions,
    filter_flows,
)


# -----------------------------------------------------------------------------
# Test 1 - happy path principal: where + h3_cells
# -----------------------------------------------------------------------------


def test_filter_flows_integration_happy_path_combines_where_h3_and_syncs_auxiliary(
    rich_flowdataset_with_links_factory,
    get_last_event,
    snapshot_flowdataset_state,
    assert_flowdataset_input_intact,
):
    """Verifica el camino feliz público con filtros combinados, auxiliar sincronizado y trazabilidad completa."""
    flows, cells = rich_flowdataset_with_links_factory()
    snapshot = snapshot_flowdataset_state(flows)

    options = FlowFilterOptions(
        where={
            "mode": ["bus", "metro"],
            "purpose": "work",
            "flow_value": {"gte": 10},
            "gender": "F",
        },
        h3_cells=[cells["origin_a"], cells["origin_d"]],
        spatial_predicate="origin",
        keep_flow_to_trips=True,
        keep_metadata=True,
        strict=False,
    )

    filtered, report = filter_flows(
        flows,
        options=options,
        max_issues=50,
    )

    where_mask = (
        flows.flows["mode"].isin(["bus", "metro"])
        & flows.flows["purpose"].eq("work")
        & flows.flows["flow_value"].ge(10)
        & flows.flows["gender"].eq("F")
    )
    after_where = flows.flows.loc[where_mask].copy(deep=True)

    h3_mask = after_where["origin_h3_index"].isin(
        [cells["origin_a"], cells["origin_d"]]
    )
    expected_flows = after_where.loc[h3_mask].copy(deep=True)

    expected_flow_ids = set(expected_flows["flow_id"].tolist())
    expected_aux = flows.flow_to_trips.loc[
        flows.flow_to_trips["flow_id"].isin(expected_flow_ids)
    ].copy(deep=True)

    assert filtered is not flows

    pd.testing.assert_frame_equal(
        filtered.flows.reset_index(drop=True),
        expected_flows.reset_index(drop=True),
        check_dtype=False,
        check_categorical=False,
    )

    assert report.ok is True
    assert report.summary["rows_in"] == len(flows.flows)
    assert report.summary["rows_out"] == len(expected_flows)
    assert report.summary["dropped_total"] == (
        len(flows.flows) - len(expected_flows)
    )
    assert report.summary["dropped_by_filter"] == {
        "where": len(flows.flows) - len(after_where),
        "h3_cells": len(after_where) - len(expected_flows),
    }
    assert report.summary["filters_requested"] == ["where", "h3_cells"]
    assert report.summary["filters_applied"] == ["where", "h3_cells"]
    assert report.summary["filters_omitted"] == []
    assert report.summary["flow_to_trips_status"] == "synced"

    assert report.parameters["spatial_predicate"] == "origin"
    assert report.parameters["keep_flow_to_trips"] is True
    assert report.parameters["keep_metadata"] is True
    assert report.parameters["strict"] is False
    assert report.parameters["max_issues"] == 50

    assert filtered.flow_to_trips is not None
    pd.testing.assert_frame_equal(
        filtered.flow_to_trips.reset_index(drop=True),
        expected_aux.reset_index(drop=True),
        check_dtype=False,
        check_categorical=False,
    )

    assert filtered.metadata["is_validated"] is False

    last_event = get_last_event(filtered)
    assert last_event["op"] == "filter_flows"
    assert last_event["parameters"] == report.parameters
    assert last_event["summary"] == report.summary
    assert "issues_summary" in last_event
    assert "context" not in last_event

    assert filtered.aggregation_spec == flows.aggregation_spec

    assert filtered.provenance is not None
    assert (
        filtered.provenance["derived_from"][0]["dataset_id"]
        == flows.metadata["dataset_id"]
    )
    assert (
        filtered.provenance["derived_from"][0]["artifact_id"]
        == flows.metadata["artifact_id"]
    )
    assert "prior_events_summary" in filtered.provenance

    assert_flowdataset_input_intact(flows, snapshot)


# -----------------------------------------------------------------------------
# Test 2 - spatial_predicate="both"
# -----------------------------------------------------------------------------


def test_filter_flows_integration_spatial_both_predicate_filters_segmented_fixture(
    rich_segmented_flowdataset_factory,
    get_last_event,
    snapshot_flowdataset_state,
    assert_flowdataset_input_intact,
    assert_issue_codes,
):
    """Verifica la semántica espacial `both` sobre flows segmentados sin auxiliar disponible."""
    flows, cells = rich_segmented_flowdataset_factory()
    snapshot = snapshot_flowdataset_state(flows)

    requested_cells = {
        cells["origin_a"],
        cells["dest_a"],
        cells["dest_b"],
    }

    options = FlowFilterOptions(
        h3_cells=list(requested_cells),
        spatial_predicate="both",
        keep_flow_to_trips=True,
        keep_metadata=True,
        strict=False,
    )

    filtered, report = filter_flows(
        flows,
        options=options,
    )

    expected_mask = (
        flows.flows["origin_h3_index"].isin(requested_cells)
        & flows.flows["destination_h3_index"].isin(requested_cells)
    )
    expected_flows = flows.flows.loc[expected_mask].copy(deep=True)

    pd.testing.assert_frame_equal(
        filtered.flows.reset_index(drop=True),
        expected_flows.reset_index(drop=True),
        check_dtype=False,
        check_categorical=False,
    )

    assert report.summary["rows_in"] == len(flows.flows)
    assert report.summary["rows_out"] == len(expected_flows)
    assert report.summary["filters_requested"] == ["h3_cells"]
    assert report.summary["filters_applied"] == ["h3_cells"]
    assert report.summary["filters_omitted"] == []
    assert report.summary["flow_to_trips_status"] == "missing"

    assert_issue_codes(
        report.issues,
        expected_present=[
            "FLT_FLOW.H3.APPLIED",
            "FLT_FLOW.AUX.FLOW_TO_TRIPS_REQUESTED_BUT_MISSING",
        ],
    )

    last_event = get_last_event(filtered)
    assert last_event["summary"] == report.summary
    assert last_event["parameters"] == report.parameters

    assert_flowdataset_input_intact(flows, snapshot)


# -----------------------------------------------------------------------------
# Test 3 - fatal por falta de columna canónica
# -----------------------------------------------------------------------------


def test_filter_flows_integration_raises_when_required_canonical_column_is_missing(
    rich_segmented_flowdataset_factory,
    snapshot_flowdataset_state,
    assert_flowdataset_input_intact,
):
    """Verifica abort fatal por ausencia de una columna canónica mínima del contrato de flows."""
    flows, _ = rich_segmented_flowdataset_factory()
    snapshot = snapshot_flowdataset_state(flows)

    flows_bad = FlowDataset(
        flows=flows.flows.drop(columns=["flow_value"]).copy(deep=True),
        flow_to_trips=flows.flow_to_trips,
        aggregation_spec=deepcopy(flows.aggregation_spec),
        source_trips=flows.source_trips,
        metadata=deepcopy(flows.metadata),
        provenance=deepcopy(flows.provenance),
    )

    with pytest.raises(FilterError) as excinfo:
        filter_flows(
            flows_bad,
            options=FlowFilterOptions(
                where={"mode": "bus"},
            ),
        )

    assert excinfo.value.code == "FLT_FLOW.CONTRACT.MISSING_CANONICAL_COLUMNS"

    assert_flowdataset_input_intact(flows, snapshot)


# -----------------------------------------------------------------------------
# Test 4 - degradación strict=False
# -----------------------------------------------------------------------------


def test_filter_flows_integration_non_strict_omits_invalid_where_and_applies_h3(
    rich_segmented_flowdataset_factory,
    get_last_event,
    snapshot_flowdataset_state,
    assert_flowdataset_input_intact,
    assert_issue_codes,
):
    """Verifica degradación controlada: `where` inválido se omite y el eje H3 sí se aplica."""
    flows, cells = rich_segmented_flowdataset_factory()
    snapshot = snapshot_flowdataset_state(flows)

    options = FlowFilterOptions(
        where={"campo_inexistente": "x"},
        h3_cells=[cells["origin_b"]],
        spatial_predicate="origin",
        keep_flow_to_trips=True,
        keep_metadata=True,
        strict=False,
    )

    filtered, report = filter_flows(
        flows,
        options=options,
    )

    expected_flows = flows.flows.loc[
        flows.flows["origin_h3_index"].eq(cells["origin_b"])
    ].copy(deep=True)

    assert report.ok is False

    pd.testing.assert_frame_equal(
        filtered.flows.reset_index(drop=True),
        expected_flows.reset_index(drop=True),
        check_dtype=False,
        check_categorical=False,
    )

    assert report.summary["filters_requested"] == ["where", "h3_cells"]
    assert report.summary["filters_applied"] == ["h3_cells"]
    assert report.summary["filters_omitted"] == ["where"]
    assert report.summary["flow_to_trips_status"] == "missing"

    assert_issue_codes(
        report.issues,
        expected_present=[
            "FLT_FLOW.WHERE.FIELD_MISSING",
            "FLT_FLOW.H3.APPLIED",
            "FLT_FLOW.AUX.FLOW_TO_TRIPS_REQUESTED_BUT_MISSING",
        ],
    )

    last_event = get_last_event(filtered)
    assert last_event["summary"] == report.summary
    assert last_event["parameters"] == report.parameters

    assert_flowdataset_input_intact(flows, snapshot)


# -----------------------------------------------------------------------------
# Test 5 - metadata, event y summary sobre fixture validada
# -----------------------------------------------------------------------------


def test_filter_flows_integration_preserves_validated_state_and_builds_consistent_event(
    rich_validated_flowdataset_factory,
    get_last_event,
    snapshot_flowdataset_state,
    assert_flowdataset_input_intact,
):
    """Verifica preservación de `is_validated`, evento append-only y provenance derivada."""
    flows, _ = rich_validated_flowdataset_factory()
    snapshot = snapshot_flowdataset_state(flows)
    n_events_before = len(flows.metadata["events"])

    options = FlowFilterOptions(
        where={
            "mode": "bus",
            "flow_count": {"gte": 8},
        },
        keep_flow_to_trips=True,
        keep_metadata=True,
        strict=False,
    )

    filtered, report = filter_flows(
        flows,
        options=options,
    )

    expected_flows = flows.flows.loc[
        flows.flows["mode"].eq("bus")
        & flows.flows["flow_count"].ge(8)
    ].copy(deep=True)

    pd.testing.assert_frame_equal(
        filtered.flows.reset_index(drop=True),
        expected_flows.reset_index(drop=True),
        check_dtype=False,
        check_categorical=False,
    )

    assert filtered.metadata["is_validated"] is True

    assert len(filtered.metadata["events"]) == n_events_before + 1

    last_event = get_last_event(filtered)
    assert last_event["op"] == "filter_flows"
    assert last_event["summary"] == report.summary
    assert last_event["parameters"] == report.parameters
    assert "issues_summary" in last_event

    assert filtered.aggregation_spec == flows.aggregation_spec

    assert filtered.provenance is not None
    assert (
        filtered.provenance["derived_from"][0]["dataset_id"]
        == flows.metadata["dataset_id"]
    )
    assert (
        filtered.provenance["derived_from"][0]["artifact_id"]
        == flows.metadata["artifact_id"]
    )
    assert "prior_events_summary" in filtered.provenance
    assert isinstance(filtered.provenance["prior_events_summary"], list)

    assert_flowdataset_input_intact(flows, snapshot)


# -----------------------------------------------------------------------------
# Test 6 - keep_metadata=False
# -----------------------------------------------------------------------------


def test_filter_flows_integration_keep_metadata_false_preserves_operational_metadata_without_events(
    rich_validated_flowdataset_factory,
    snapshot_flowdataset_state,
    assert_flowdataset_input_intact,
):
    """Verifica política `keep_metadata=False`: sin eventos, pero con metadata operativa preservada."""
    flows, _ = rich_validated_flowdataset_factory()
    snapshot = snapshot_flowdataset_state(flows)

    options = FlowFilterOptions(
        where={
            "mode": ["bus", "scooter"],
            "day_type": "weekend",
        },
        keep_flow_to_trips=True,
        keep_metadata=False,
        strict=False,
    )

    filtered, report = filter_flows(
        flows,
        options=options,
    )

    expected_flows = flows.flows.loc[
        flows.flows["mode"].isin(["bus", "scooter"])
        & flows.flows["day_type"].eq("weekend")
    ].copy(deep=True)

    pd.testing.assert_frame_equal(
        filtered.flows.reset_index(drop=True),
        expected_flows.reset_index(drop=True),
        check_dtype=False,
        check_categorical=False,
    )

    assert "events" not in filtered.metadata

    assert filtered.metadata["dataset_id"] == flows.metadata["dataset_id"]
    assert filtered.metadata["artifact_id"] == flows.metadata["artifact_id"]
    assert filtered.metadata["is_validated"] is True
    assert filtered.metadata["h3"] == flows.metadata["h3"]
    assert filtered.metadata["custom_tag"] == flows.metadata["custom_tag"]

    assert report.parameters is not None
    assert report.summary["rows_in"] == len(flows.flows)
    assert report.summary["rows_out"] == len(expected_flows)

    assert_flowdataset_input_intact(flows, snapshot)


# -----------------------------------------------------------------------------
# Test 7 - flow_to_trips inválido se descarta
# -----------------------------------------------------------------------------


def test_filter_flows_integration_discards_invalid_flow_to_trips_without_breaking_main_filtering(
    rich_flowdataset_with_links_factory,
    get_last_event,
    snapshot_flowdataset_state,
    assert_flowdataset_input_intact,
    assert_issue_codes,
):
    """Verifica descarte explícito de `flow_to_trips` inválido sin romper el filtrado principal."""
    flows, _ = rich_flowdataset_with_links_factory()
    snapshot = snapshot_flowdataset_state(flows)

    flows_bad_aux = FlowDataset(
        flows=flows.flows.copy(deep=True),
        flow_to_trips=flows.flow_to_trips.drop(
            columns=["movement_id"]
        ).copy(deep=True),
        aggregation_spec=deepcopy(flows.aggregation_spec),
        source_trips=flows.source_trips,
        metadata=deepcopy(flows.metadata),
        provenance=deepcopy(flows.provenance),
    )

    options = FlowFilterOptions(
        where={"mode": "bus"},
        keep_flow_to_trips=True,
        keep_metadata=True,
        strict=False,
    )

    filtered, report = filter_flows(
        flows_bad_aux,
        options=options,
    )

    expected_flows = flows.flows.loc[
        flows.flows["mode"].eq("bus")
    ].copy(deep=True)

    pd.testing.assert_frame_equal(
        filtered.flows.reset_index(drop=True),
        expected_flows.reset_index(drop=True),
        check_dtype=False,
        check_categorical=False,
    )

    assert filtered.flow_to_trips is None
    assert report.summary["flow_to_trips_status"] == "discarded_invalid"
    assert report.ok is False

    assert_issue_codes(
        report.issues,
        expected_present=[
            "FLT_FLOW.WHERE.APPLIED",
            "FLT_FLOW.AUX.FLOW_TO_TRIPS_INVALID",
        ],
    )

    last_event = get_last_event(filtered)
    assert last_event["summary"] == report.summary
    assert last_event["parameters"] == report.parameters

    assert_flowdataset_input_intact(flows, snapshot)


# -----------------------------------------------------------------------------
# Test 8 - strict=True escala a FilterError
# -----------------------------------------------------------------------------


def test_filter_flows_integration_strict_true_escalates_recoverable_axis_error(
    rich_segmented_flowdataset_factory,
    snapshot_flowdataset_state,
    assert_flowdataset_input_intact,
):
    """Verifica que `strict=True` escale errores recuperables a `FilterError` con evidencia asociada."""
    flows, cells = rich_segmented_flowdataset_factory()
    snapshot = snapshot_flowdataset_state(flows)

    with pytest.raises(FilterError) as excinfo:
        filter_flows(
            flows,
            options=FlowFilterOptions(
                where={"campo_inexistente": "x"},
                h3_cells=[cells["origin_a"]],
                spatial_predicate="origin",
                keep_flow_to_trips=True,
                keep_metadata=True,
                strict=True,
            ),
        )

    error = excinfo.value

    assert error.issue is not None
    assert error.issue.code == "FLT_FLOW.WHERE.FIELD_MISSING"
    assert error.issues is not None
    assert any(
        issue.code == "FLT_FLOW.WHERE.FIELD_MISSING"
        for issue in error.issues
    )

    assert_flowdataset_input_intact(flows, snapshot)


# -----------------------------------------------------------------------------
# Test 9 - resultado vacío retornable
# -----------------------------------------------------------------------------


def test_filter_flows_integration_returns_empty_dataset_with_warning_when_filters_remove_all_rows(
    rich_flowdataset_with_links_factory,
    get_last_event,
    snapshot_flowdataset_state,
    assert_flowdataset_input_intact,
    assert_issue_codes,
):
    """Verifica resultado vacío retornable, warning explícito y contrato de salida consistente."""
    flows, _ = rich_flowdataset_with_links_factory()
    snapshot = snapshot_flowdataset_state(flows)

    options = FlowFilterOptions(
        where={
            "mode": "bus",
            "flow_count": {"lt": 0},
        },
        keep_flow_to_trips=True,
        keep_metadata=True,
        strict=False,
    )

    filtered, report = filter_flows(
        flows,
        options=options,
    )

    expected_flows = flows.flows.loc[
        flows.flows["mode"].eq("bus")
        & flows.flows["flow_count"].lt(0)
    ].copy(deep=True)

    assert filtered.flows.empty is True
    pd.testing.assert_frame_equal(
        filtered.flows.reset_index(drop=True),
        expected_flows.reset_index(drop=True),
        check_dtype=False,
        check_categorical=False,
    )

    assert report.ok is True
    assert report.summary["rows_in"] == len(flows.flows)
    assert report.summary["rows_out"] == len(expected_flows)
    assert report.summary["dropped_total"] == (
        len(flows.flows) - len(expected_flows)
    )
    assert report.summary["flow_to_trips_status"] == "synced"

    assert_issue_codes(
        report.issues,
        expected_present=[
            "FLT_FLOW.WHERE.APPLIED",
            "FLT_FLOW.AUX.FLOW_TO_TRIPS_SYNCED",
            "FLT_FLOW.RESULT.EMPTY_DATASET",
        ],
    )

    last_event = get_last_event(filtered)
    assert last_event["summary"] == report.summary
    assert last_event["parameters"] == report.parameters

    assert_flowdataset_input_intact(flows, snapshot)


# -----------------------------------------------------------------------------
# Test 10 - truncamiento de issues y bloque limits
# -----------------------------------------------------------------------------


def test_filter_flows_integration_truncates_issues_and_keeps_limits_consistent_with_event(
    rich_segmented_flowdataset_factory,
    get_last_event,
    snapshot_flowdataset_state,
    assert_flowdataset_input_intact,
    assert_issue_codes,
):
    """Verifica truncamiento de issues, bloque `limits` y consistencia entre report y evento."""
    flows, _ = rich_segmented_flowdataset_factory()
    snapshot = snapshot_flowdataset_state(flows)

    options = FlowFilterOptions(
        where={
            "does_not_exist": "x",
            "mode": {"gt": "bus"},
            "window_start_utc": {"gte": "bad_ts"},
            "gender": {"is_null": False},
        },
        keep_flow_to_trips=True,
        keep_metadata=True,
        strict=False,
    )

    filtered, report = filter_flows(
        flows,
        options=options,
        max_issues=3,
    )

    assert "limits" in report.summary
    assert report.summary["limits"]["max_issues"] == 3
    assert report.summary["limits"]["issues_truncated"] is True
    assert report.summary["limits"]["n_issues_emitted"] <= 3
    assert (
        report.summary["limits"]["n_issues_detected_total"]
        >= report.summary["limits"]["n_issues_emitted"]
    )

    assert_issue_codes(
        report.issues,
        expected_present=[
            "FLT_FLOW.REPORT.ISSUES_TRUNCATED",
        ],
    )

    last_event = get_last_event(filtered)
    assert last_event["summary"] == report.summary
    assert last_event["parameters"] == report.parameters
    assert "issues_summary" in last_event

    assert_flowdataset_input_intact(flows, snapshot)


# -----------------------------------------------------------------------------
# Test 11 - options=None
# -----------------------------------------------------------------------------


def test_filter_flows_integration_options_none_returns_derived_dataset_without_filtering(
    rich_flowdataset_with_links_factory,
    get_last_event,
    snapshot_flowdataset_state,
    assert_flowdataset_input_intact,
    assert_issue_codes,
):
    """Verifica que `options=None` retorne un dataset derivado sin cambios tabulares y con evidencia explícita."""
    flows, _ = rich_flowdataset_with_links_factory()
    snapshot = snapshot_flowdataset_state(flows)

    filtered, report = filter_flows(
        flows,
        options=None,
        max_issues=20,
    )

    assert filtered is not flows

    pd.testing.assert_frame_equal(
        filtered.flows.reset_index(drop=True),
        flows.flows.reset_index(drop=True),
        check_dtype=False,
        check_categorical=False,
    )
    pd.testing.assert_frame_equal(
        filtered.flow_to_trips.reset_index(drop=True),
        flows.flow_to_trips.reset_index(drop=True),
        check_dtype=False,
        check_categorical=False,
    )

    assert report.ok is True
    assert report.summary["rows_in"] == len(flows.flows)
    assert report.summary["rows_out"] == len(flows.flows)
    assert report.summary["dropped_total"] == 0
    assert report.summary["filters_requested"] == []
    assert report.summary["filters_applied"] == []
    assert report.summary["filters_omitted"] == []
    assert report.summary["flow_to_trips_status"] == "synced"

    assert_issue_codes(
        report.issues,
        expected_present=[
            "FLT_FLOW.NO_CHANGES.NO_FILTERS_DEFINED",
            "FLT_FLOW.AUX.FLOW_TO_TRIPS_SYNCED",
        ],
    )

    last_event = get_last_event(filtered)
    assert last_event["summary"] == report.summary
    assert last_event["parameters"] == report.parameters

    assert filtered.metadata["is_validated"] is False

    assert_flowdataset_input_intact(flows, snapshot)