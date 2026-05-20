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
# Bloque 7. Smoke tests públicos de filter_flows
# -----------------------------------------------------------------------------


def test_filter_flows_smoke_happy_path_combines_where_h3_and_syncs_auxiliary(
    small_flowdataset_factory,
    get_last_event,
    snapshot_flowdataset_state,
    assert_flowdataset_input_intact,
):
    """Verifica el camino feliz público con `where`, H3, reporte, evento y auxiliar sincronizado."""
    flows, cells = small_flowdataset_factory()
    snapshot = snapshot_flowdataset_state(flows)
    validated_before = flows.metadata["is_validated"]

    options = FlowFilterOptions(
        where={
            "mode": ["bus", "metro"],
            "flow_count": {"gte": 5},
        },
        h3_cells=[cells["origin_a"]],
        spatial_predicate="origin",
        keep_flow_to_trips=True,
        keep_metadata=True,
        strict=False,
    )

    filtered, report = filter_flows(
        flows,
        options=options,
        max_issues=20,
    )

    expected_mask = (
        flows.flows["mode"].isin(["bus", "metro"])
        & flows.flows["flow_count"].ge(5)
        & flows.flows["origin_h3_index"].eq(cells["origin_a"])
    )
    expected_flows = flows.flows.loc[expected_mask].copy(deep=True)

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
    assert report.summary["filters_requested"] == ["where", "h3_cells"]
    assert report.summary["filters_applied"] == ["where", "h3_cells"]
    assert report.summary["filters_omitted"] == []
    assert report.summary["flow_to_trips_status"] == "synced"

    assert report.parameters["max_issues"] == 20
    assert report.parameters["spatial_predicate"] == "origin"
    assert report.parameters["keep_flow_to_trips"] is True
    assert report.parameters["keep_metadata"] is True
    assert report.parameters["strict"] is False

    assert filtered.metadata["is_validated"] == validated_before

    event = get_last_event(filtered)
    assert event["op"] == "filter_flows"
    assert event["summary"] == report.summary
    assert event["parameters"] == report.parameters
    assert "issues_summary" in event

    assert filtered.flow_to_trips is not None
    pd.testing.assert_frame_equal(
        filtered.flow_to_trips.reset_index(drop=True),
        expected_aux.reset_index(drop=True),
        check_dtype=False,
        check_categorical=False,
    )

    assert_flowdataset_input_intact(flows, snapshot)


def test_filter_flows_smoke_keep_metadata_false_filters_without_event_history(
    small_flowdataset_factory,
    snapshot_flowdataset_state,
    assert_flowdataset_input_intact,
):
    """Verifica filtrado público sin historial de eventos ni append de evento nuevo."""
    flows, _ = small_flowdataset_factory()
    snapshot = snapshot_flowdataset_state(flows)
    validated_before = flows.metadata["is_validated"]

    options = FlowFilterOptions(
        where={"mode": "bus"},
        keep_metadata=False,
    )

    filtered, report = filter_flows(
        flows,
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

    assert report.ok is True
    assert report.summary["rows_in"] == len(flows.flows)
    assert report.summary["rows_out"] == len(expected_flows)
    assert report.summary["filters_requested"] == ["where"]
    assert report.summary["filters_applied"] == ["where"]
    assert report.summary["filters_omitted"] == []

    assert "events" not in filtered.metadata

    assert filtered.metadata["dataset_id"] == flows.metadata["dataset_id"]
    assert filtered.metadata["artifact_id"] == flows.metadata["artifact_id"]
    assert filtered.metadata["is_validated"] == validated_before
    assert filtered.metadata["h3"] == flows.metadata["h3"]

    assert_flowdataset_input_intact(flows, snapshot)


def test_filter_flows_smoke_non_strict_degrades_invalid_where_and_still_applies_h3(
    small_flowdataset_factory,
    get_last_event,
    snapshot_flowdataset_state,
    assert_flowdataset_input_intact,
    assert_issue_codes,
):
    """Verifica degradación recuperable: `where` inválido se omite y H3 sí se aplica."""
    flows, cells = small_flowdataset_factory()
    snapshot = snapshot_flowdataset_state(flows)
    validated_before = flows.metadata["is_validated"]

    options = FlowFilterOptions(
        where={"campo_inexistente": "x"},
        h3_cells=[cells["origin_a"]],
        spatial_predicate="origin",
        strict=False,
    )

    filtered, report = filter_flows(
        flows,
        options=options,
    )

    expected_flows = flows.flows.loc[
        flows.flows["origin_h3_index"].eq(cells["origin_a"])
    ].copy(deep=True)

    assert_issue_codes(
        report.issues,
        expected_present=[
            "FLT_FLOW.WHERE.FIELD_MISSING",
            "FLT_FLOW.H3.APPLIED",
        ],
    )

    assert report.ok is False
    assert report.summary["filters_requested"] == ["where", "h3_cells"]
    assert report.summary["filters_applied"] == ["h3_cells"]
    assert report.summary["filters_omitted"] == ["where"]

    pd.testing.assert_frame_equal(
        filtered.flows.reset_index(drop=True),
        expected_flows.reset_index(drop=True),
        check_dtype=False,
        check_categorical=False,
    )

    assert filtered.metadata["is_validated"] == validated_before

    event = get_last_event(filtered)
    assert event["op"] == "filter_flows"
    assert event["summary"] == report.summary
    assert event["parameters"] == report.parameters
    assert "issues_summary" in event

    assert_flowdataset_input_intact(flows, snapshot)


def test_filter_flows_smoke_strict_true_escalates_recoverable_axis_error(
    small_flowdataset_factory,
    snapshot_flowdataset_state,
    assert_flowdataset_input_intact,
    assert_issue_codes,
):
    """Verifica que `strict=True` eleve a `FilterError` un error recuperable por eje."""
    flows, cells = small_flowdataset_factory()
    snapshot = snapshot_flowdataset_state(flows)

    with pytest.raises(FilterError) as excinfo:
        filter_flows(
            flows,
            options=FlowFilterOptions(
                where={"campo_inexistente": "x"},
                h3_cells=[cells["origin_a"]],
                spatial_predicate="origin",
                strict=True,
            ),
        )

    error = excinfo.value

    assert error.issue is not None
    assert error.issue.code == "FLT_FLOW.WHERE.FIELD_MISSING"
    assert error.issues is not None

    assert_issue_codes(
        error.issues,
        expected_present=[
            "FLT_FLOW.WHERE.FIELD_MISSING",
        ],
    )

    assert_flowdataset_input_intact(flows, snapshot)


def test_filter_flows_smoke_returns_empty_dataset_with_warning_when_filters_remove_all_rows(
    small_flowdataset_factory,
    get_last_event,
    snapshot_flowdataset_state,
    assert_flowdataset_input_intact,
    assert_issue_codes,
):
    """Verifica que un resultado vacío sea retornable y quede evidenciado con warning."""
    flows, _ = small_flowdataset_factory()
    snapshot = snapshot_flowdataset_state(flows)
    validated_before = flows.metadata["is_validated"]

    options = FlowFilterOptions(
        where={
            "mode": "bus",
            "flow_count": {"lt": 0},
        },
    )

    filtered, report = filter_flows(
        flows,
        options=options,
    )

    expected_flows = flows.flows.loc[
        flows.flows["mode"].eq("bus")
        & flows.flows["flow_count"].lt(0)
    ].copy(deep=True)

    assert report.ok is True
    assert_issue_codes(
        report.issues,
        expected_present=[
            "FLT_FLOW.RESULT.EMPTY_DATASET",
        ],
    )

    assert report.summary["rows_in"] == len(flows.flows)
    assert report.summary["rows_out"] == len(expected_flows)
    assert report.summary["dropped_total"] == (
        len(flows.flows) - len(expected_flows)
    )

    assert filtered.flows.empty is True
    pd.testing.assert_frame_equal(
        filtered.flows.reset_index(drop=True),
        expected_flows.reset_index(drop=True),
        check_dtype=False,
        check_categorical=False,
    )

    event = get_last_event(filtered)
    assert event["op"] == "filter_flows"
    assert event["summary"] == report.summary

    assert filtered.metadata["is_validated"] == validated_before

    assert_flowdataset_input_intact(flows, snapshot)


def test_filter_flows_smoke_truncates_issues_and_reflects_limits_in_summary_and_event(
    small_flowdataset_factory,
    get_last_event,
    assert_issue_codes,
):
    """Verifica truncamiento de issues y propagación consistente de `limits` al summary y evento."""
    flows, _ = small_flowdataset_factory()

    options = FlowFilterOptions(
        where={
            "does_not_exist": "x",
            "mode": {"gt": "bus"},
            "window_start_utc": {"gte": "bad_ts"},
            "gender": {"is_null": False},
        },
        strict=False,
    )

    filtered, report = filter_flows(
        flows,
        options=options,
        max_issues=2,
    )

    assert_issue_codes(
        report.issues,
        expected_present=[
            "FLT_FLOW.REPORT.ISSUES_TRUNCATED",
        ],
    )

    assert report.summary["limits"]["issues_truncated"] is True
    assert report.summary["limits"]["max_issues"] == 2
    assert report.summary["limits"]["n_issues_emitted"] <= 2
    assert (
        report.summary["limits"]["n_issues_detected_total"]
        >= report.summary["limits"]["n_issues_emitted"]
    )

    event = get_last_event(filtered)
    assert event["summary"] == report.summary
    assert event["parameters"] == report.parameters
    assert "issues_summary" in event


def test_filter_flows_smoke_reports_missing_auxiliary_without_breaking_main_filtering(
    small_flowdataset_factory,
    get_last_event,
    assert_issue_codes,
):
    """Verifica degradación correcta si `flow_to_trips` se solicita pero no existe."""
    flows, cells = small_flowdataset_factory()

    flows_no_aux = FlowDataset(
        flows=flows.flows.copy(deep=True),
        flow_to_trips=None,
        aggregation_spec=deepcopy(flows.aggregation_spec),
        source_trips=flows.source_trips,
        metadata=deepcopy(flows.metadata),
        provenance=deepcopy(flows.provenance),
    )

    options = FlowFilterOptions(
        h3_cells=[cells["origin_a"]],
        spatial_predicate="origin",
        keep_flow_to_trips=True,
    )

    filtered, report = filter_flows(
        flows_no_aux,
        options=options,
    )

    expected_flows = flows_no_aux.flows.loc[
        flows_no_aux.flows["origin_h3_index"].eq(cells["origin_a"])
    ].copy(deep=True)

    assert_issue_codes(
        report.issues,
        expected_present=[
            "FLT_FLOW.AUX.FLOW_TO_TRIPS_REQUESTED_BUT_MISSING",
        ],
    )

    assert report.summary["flow_to_trips_status"] == "missing"
    assert filtered.flow_to_trips is None

    pd.testing.assert_frame_equal(
        filtered.flows.reset_index(drop=True),
        expected_flows.reset_index(drop=True),
        check_dtype=False,
        check_categorical=False,
    )

    event = get_last_event(filtered)
    assert event["summary"] == report.summary