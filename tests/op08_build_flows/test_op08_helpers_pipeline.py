from __future__ import annotations

import pandas as pd
import pytest

from pylondrina.datasets import FlowDataset
from pylondrina.errors import ValidationError
from pylondrina.reports import FlowBuildReport
from pylondrina.transforms.flows import (
    FlowBuildOptions,
    _aggregate_flows,
    _build_flow_dataset,
    _build_flow_report_and_event,
    _build_flow_to_trips,
    _prepare_buildable_movements,
    _resolve_and_precheck_build_request,
)


def test_resolve_and_precheck_build_request_normalizes_effective_options(
    make_tripdataset_for_flows,
) -> None:
    """Verifica precheck feliz y serialización de parámetros efectivos de build_flows."""
    trips = make_tripdataset_for_flows(
        validated=True,
        tier="tier_1",
    )

    options_eff, parameters = _resolve_and_precheck_build_request(
        trips,
        FlowBuildOptions(
            h3_resolution=8,
            group_by=["mode"],
            time_aggregation="hour",
            time_basis="origin",
            min_trips_per_flow=1,
            keep_flow_to_trips=True,
            require_validated=True,
            strict=False,
            max_issues=20,
        ),
    )

    assert isinstance(options_eff, FlowBuildOptions)

    assert options_eff.h3_resolution == 8
    assert options_eff.group_by == ["mode"]
    assert options_eff.time_aggregation == "hour"
    assert options_eff.time_basis == "origin"
    assert options_eff.min_trips_per_flow == 1
    assert options_eff.keep_flow_to_trips is True
    assert options_eff.require_validated is True
    assert options_eff.strict is False
    assert options_eff.max_issues == 20

    assert parameters == {
        "h3_resolution": options_eff.h3_resolution,
        "group_by": list(options_eff.group_by),
        "time_aggregation": options_eff.time_aggregation,
        "time_basis": options_eff.time_basis,
        "min_trips_per_flow": options_eff.min_trips_per_flow,
        "keep_flow_to_trips": options_eff.keep_flow_to_trips,
        "require_validated": options_eff.require_validated,
        "strict": options_eff.strict,
        "max_issues": options_eff.max_issues,
    }


def test_resolve_and_precheck_build_request_rejects_unvalidated_trips(
    make_tripdataset_for_flows,
) -> None:
    """Verifica error fatal cuando require_validated=True y trips no está validado."""
    trips = make_tripdataset_for_flows(
        validated=False,
        tier="tier_1",
    )

    with pytest.raises(ValidationError) as excinfo:
        _resolve_and_precheck_build_request(
            trips,
            FlowBuildOptions(require_validated=True),
        )

    assert excinfo.value.code == "FLOW.VALIDATION.REQUIRED_NOT_VALIDATED"


def test_prepare_buildable_movements_adds_temporal_windows_and_effective_flow_keys(
    make_tripdataset_for_flows,
) -> None:
    """Verifica preparación feliz del working set buildable con agregación temporal."""
    trips = make_tripdataset_for_flows(
        validated=True,
        tier="tier_1",
    )

    options = FlowBuildOptions(
        h3_resolution=8,
        group_by=["mode"],
        time_aggregation="hour",
        time_basis="origin",
        min_trips_per_flow=1,
        keep_flow_to_trips=False,
        require_validated=True,
    )

    prepared_df, issues, prep_info = _prepare_buildable_movements(
        trips,
        options,
    )

    assert issues == []
    assert len(prepared_df) == len(trips.data)

    assert "window_start_utc" in prepared_df.columns
    assert "window_end_utc" in prepared_df.columns
    assert (prepared_df["window_end_utc"] > prepared_df["window_start_utc"]).all()

    assert prep_info["effective_flow_keys"] == [
        "origin_h3_index",
        "destination_h3_index",
        "window_start_utc",
        "window_end_utc",
        "mode",
    ]

    assert prep_info["n_trips_in"] == len(trips.data)
    assert prep_info["n_trips_eligible"] == len(prepared_df)
    assert prep_info["n_trips_dropped"] == 0
    assert prep_info["h3_resolution_target"] == options.h3_resolution


def test_prepare_buildable_movements_drops_rows_missing_complete_od_h3(
    make_tripdataset_for_flows,
    assert_issue_present,
) -> None:
    """Verifica descarte agregado de movements sin H3 OD completo."""
    trips = make_tripdataset_for_flows(
        validated=True,
        tier="tier_1",
    )
    trips.data.loc[1, "destination_h3_index"] = None

    prepared_df, issues, prep_info = _prepare_buildable_movements(
        trips,
        FlowBuildOptions(
            h3_resolution=8,
            group_by=None,
            time_aggregation="none",
        ),
    )

    assert_issue_present(
        issues,
        "FLOW.OUTPUT.MOVEMENTS_DROPPED_MISSING_OD_H3",
    )

    assert len(prepared_df) == len(trips.data) - 1
    assert prepared_df["destination_h3_index"].notna().all()

    assert prep_info["n_trips_in"] == len(trips.data)
    assert prep_info["n_trips_eligible"] == len(prepared_df)
    assert prep_info["n_trips_dropped"] == len(trips.data) - len(prepared_df)


def test_aggregate_flows_uses_trip_weight_and_threshold_to_build_canonical_output() -> None:
    """Verifica agregación ponderada, umbral mínimo y salida canónica de flows."""
    prepared_df = pd.DataFrame(
        {
            "movement_id": ["m0", "m1", "m2"],
            "origin_h3_index": [
                "8828308281fffff",
                "8828308281fffff",
                "882830828dfffff",
            ],
            "destination_h3_index": [
                "8828308285fffff",
                "8828308285fffff",
                "8828308287fffff",
            ],
            "mode": ["bus", "bus", "metro"],
            "trip_weight": [1.0, 2.0, 5.0],
        }
    )

    effective_flow_keys = [
        "origin_h3_index",
        "destination_h3_index",
        "mode",
    ]

    flows_df = _aggregate_flows(
        prepared_df,
        effective_flow_keys,
        FlowBuildOptions(min_trips_per_flow=2),
    )

    assert list(flows_df.columns) == [
        "flow_id",
        "origin_h3_index",
        "destination_h3_index",
        "mode",
        "flow_count",
        "flow_value",
    ]

    assert len(flows_df) == 1

    retained_group = (
        prepared_df.groupby(effective_flow_keys, dropna=False, observed=True)
        .agg(
            expected_flow_count=("movement_id", "size"),
            expected_flow_value=("trip_weight", "sum"),
        )
        .reset_index()
        .loc[lambda df: df["expected_flow_count"] >= 2]
        .reset_index(drop=True)
    )

    assert len(retained_group) == len(flows_df)
    assert flows_df.iloc[0]["flow_count"] == retained_group.iloc[0]["expected_flow_count"]
    assert float(flows_df.iloc[0]["flow_value"]) == float(
        retained_group.iloc[0]["expected_flow_value"]
    )
    assert flows_df.iloc[0]["flow_id"].startswith("flow_")


def test_build_flow_to_trips_returns_minimal_backlink_table_when_enabled() -> None:
    """Verifica construcción de backlinks mínimos flow_id + movement_id."""
    prepared_df = pd.DataFrame(
        {
            "movement_id": ["m0", "m1", "m2"],
            "origin_h3_index": [
                "8828308281fffff",
                "8828308281fffff",
                "882830828dfffff",
            ],
            "destination_h3_index": [
                "8828308285fffff",
                "8828308285fffff",
                "8828308287fffff",
            ],
            "mode": ["bus", "bus", "metro"],
        }
    )

    flows_df = pd.DataFrame(
        {
            "flow_id": ["flow_0000000", "flow_0000001"],
            "origin_h3_index": [
                "8828308281fffff",
                "882830828dfffff",
            ],
            "destination_h3_index": [
                "8828308285fffff",
                "8828308287fffff",
            ],
            "mode": ["bus", "metro"],
            "flow_count": [2, 1],
            "flow_value": [2.0, 1.0],
        }
    )

    flow_to_trips = _build_flow_to_trips(
        prepared_df,
        flows_df,
        FlowBuildOptions(keep_flow_to_trips=True),
    )

    assert flow_to_trips is not None
    assert list(flow_to_trips.columns) == ["flow_id", "movement_id"]
    assert len(flow_to_trips) == len(prepared_df)

    assert sorted(flow_to_trips["movement_id"].tolist()) == sorted(
        prepared_df["movement_id"].tolist()
    )
    assert set(flow_to_trips["flow_id"]).issubset(set(flows_df["flow_id"]))


def test_build_flow_dataset_creates_derived_flowdataset_with_metadata_and_provenance(
    make_tripdataset_for_flows,
    assert_json_safe,
) -> None:
    """Verifica armado del FlowDataset derivado con metadata, provenance y aggregation_spec."""
    trips = make_tripdataset_for_flows(
        validated=True,
        tier="tier_1",
    )

    flows_df = pd.DataFrame(
        {
            "flow_id": ["flow_0000000"],
            "origin_h3_index": ["8828308281fffff"],
            "destination_h3_index": ["8828308285fffff"],
            "flow_count": [2],
            "flow_value": [3.0],
            "mode": ["bus"],
        }
    )

    flow_to_trips = pd.DataFrame(
        {
            "flow_id": ["flow_0000000", "flow_0000000"],
            "movement_id": ["m0", "m1"],
        }
    )

    prep_info = {
        "effective_flow_keys": [
            "origin_h3_index",
            "destination_h3_index",
            "mode",
        ],
        "n_trips_in": len(trips.data),
        "n_trips_eligible": len(trips.data),
        "n_trips_dropped": 0,
        "h3_resolution_input": 8,
    }

    options = FlowBuildOptions(
        h3_resolution=8,
        group_by=["mode"],
        keep_flow_to_trips=True,
    )

    flow_dataset = _build_flow_dataset(
        trips,
        flows_df,
        flow_to_trips,
        options,
        prep_info,
    )

    assert isinstance(flow_dataset, FlowDataset)

    assert flow_dataset.flows.equals(flows_df)
    assert flow_dataset.flow_to_trips is flow_to_trips

    assert flow_dataset.metadata["is_validated"] is False
    assert flow_dataset.metadata["artifact_id"] is None
    assert flow_dataset.metadata["h3"]["resolution"] == options.h3_resolution

    assert flow_dataset.provenance["derived_from"][0]["source_type"] == "trips"
    assert flow_dataset.provenance["derived_from"][0]["dataset_id"] == trips.metadata["dataset_id"]
    assert flow_dataset.provenance["derived_from"][0]["n_rows"] == len(trips.data)

    assert flow_dataset.source_trips is trips

    assert flow_dataset.aggregation_spec["h3_resolution"] == options.h3_resolution
    assert flow_dataset.aggregation_spec["group_by"] == ["mode"]
    assert flow_dataset.aggregation_spec["keep_flow_to_trips"] is True
    assert flow_dataset.aggregation_spec["effective_flow_keys"] == prep_info["effective_flow_keys"]

    assert_json_safe(flow_dataset.aggregation_spec, "aggregation_spec")
    assert_json_safe(flow_dataset.provenance, "provenance")


def test_build_flow_report_and_event_reflect_summary_parameters_and_traceability(
    assert_json_safe,
) -> None:
    """Verifica construcción coherente de FlowBuildReport y evento build_flows."""
    issues = []

    prep_info = {
        "n_trips_in": 4,
        "n_trips_eligible": 4,
        "n_trips_dropped": 0,
    }

    flows_df = pd.DataFrame(
        {
            "flow_id": ["flow_0000000"],
            "origin_h3_index": ["8828308281fffff"],
            "destination_h3_index": ["8828308285fffff"],
            "flow_count": [2],
            "flow_value": [3.0],
        }
    )

    flow_to_trips = pd.DataFrame(
        {
            "flow_id": ["flow_0000000", "flow_0000000"],
            "movement_id": ["m0", "m1"],
        }
    )

    options = FlowBuildOptions(
        h3_resolution=8,
        group_by=None,
        time_aggregation="none",
        time_basis="origin",
        min_trips_per_flow=1,
        keep_flow_to_trips=True,
        require_validated=True,
        strict=False,
        max_issues=1000,
    )

    report, event = _build_flow_report_and_event(
        issues,
        options,
        prep_info,
        flows_df,
        flow_to_trips,
    )

    assert isinstance(report, FlowBuildReport)
    assert report.ok is True

    assert report.summary["n_trips_in"] == prep_info["n_trips_in"]
    assert report.summary["n_trips_eligible"] == prep_info["n_trips_eligible"]
    assert report.summary["n_trips_dropped"] == prep_info["n_trips_dropped"]
    assert report.summary["n_flows_out"] == len(flows_df)
    assert report.summary["n_flow_to_trips_rows"] == len(flow_to_trips)

    assert report.parameters == {
        "h3_resolution": options.h3_resolution,
        "group_by": None,
        "time_aggregation": options.time_aggregation,
        "time_basis": options.time_basis,
        "min_trips_per_flow": options.min_trips_per_flow,
        "keep_flow_to_trips": options.keep_flow_to_trips,
        "require_validated": options.require_validated,
        "strict": options.strict,
        "max_issues": options.max_issues,
    }

    assert event["op"] == "build_flows"
    assert event["parameters"] == report.parameters
    assert event["summary"] == report.summary
    assert "ts_utc" in event
    assert "issues_summary" in event

    assert_json_safe(event, "build_flow_event")