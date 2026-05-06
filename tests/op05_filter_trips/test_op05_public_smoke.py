from __future__ import annotations

from copy import deepcopy
from typing import Any

import h3
import pandas as pd
import pytest

from pylondrina.datasets import TripDataset
from pylondrina.errors import FilterError
from pylondrina.reports import OperationReport
from pylondrina.schema import DomainSpec, FieldSpec, TripSchema, TripSchemaEffective
from pylondrina.transforms.filtering import FilterOptions, TimeFilter, filter_trips


def make_valid_h3(lat: float = -33.45, lon: float = -70.66, res: int = 8) -> str:
    """Construye una celda H3 válida para smoke tests de OP-05."""
    if hasattr(h3, "latlng_to_cell"):
        return h3.latlng_to_cell(lat, lon, res)
    return h3.geo_to_h3(lat, lon, res)


def make_filter_field(
    name: str,
    dtype: str,
    *,
    required: bool = False,
    domain: DomainSpec | None = None,
) -> FieldSpec:
    """Construye un FieldSpec mínimo para fixtures smoke de OP-05."""
    return FieldSpec(
        name=name,
        dtype=dtype,
        required=required,
        domain=domain,
    )


@pytest.fixture()
def base_filter_schema() -> TripSchema:
    """Schema mínimo para smoke tests públicos de filter_trips."""
    fields = {
        "movement_id": make_filter_field("movement_id", "string", required=True),
        "user_id": make_filter_field("user_id", "string", required=True),
        "mode": make_filter_field(
            "mode",
            "categorical",
            domain=DomainSpec(values=["bus", "metro", "car", "walk"], extendable=True),
        ),
        "purpose": make_filter_field(
            "purpose",
            "categorical",
            domain=DomainSpec(values=["work", "study", "leisure"], extendable=True),
        ),
        "distance_km": make_filter_field("distance_km", "float"),
        "is_peak": make_filter_field("is_peak", "bool"),
        "origin_time_utc": make_filter_field("origin_time_utc", "datetime"),
        "destination_time_utc": make_filter_field("destination_time_utc", "datetime"),
        "origin_longitude": make_filter_field("origin_longitude", "float"),
        "origin_latitude": make_filter_field("origin_latitude", "float"),
        "destination_longitude": make_filter_field("destination_longitude", "float"),
        "destination_latitude": make_filter_field("destination_latitude", "float"),
        "origin_h3_index": make_filter_field("origin_h3_index", "string"),
        "destination_h3_index": make_filter_field("destination_h3_index", "string"),
    }

    return TripSchema(
        version="test-1.0",
        fields=fields,
        required=["movement_id", "user_id"],
    )


def make_filter_test_dataframe() -> pd.DataFrame:
    """Construye el dataframe pequeño usado por los smoke tests públicos de OP-05."""
    df = pd.DataFrame(
        [
            {
                "movement_id": "m0",
                "user_id": "u0",
                "mode": "bus",
                "purpose": "work",
                "distance_km": 5.0,
                "is_peak": True,
                "origin_time_utc": "2026-01-01T07:10:00Z",
                "destination_time_utc": "2026-01-01T07:40:00Z",
                "origin_longitude": -70.66,
                "origin_latitude": -33.45,
                "destination_longitude": -70.65,
                "destination_latitude": -33.44,
            },
            {
                "movement_id": "m1",
                "user_id": "u1",
                "mode": "metro",
                "purpose": "study",
                "distance_km": 12.5,
                "is_peak": True,
                "origin_time_utc": "2026-01-01T08:00:00Z",
                "destination_time_utc": "2026-01-01T08:35:00Z",
                "origin_longitude": -70.64,
                "origin_latitude": -33.46,
                "destination_longitude": -70.63,
                "destination_latitude": -33.45,
            },
            {
                "movement_id": "m2",
                "user_id": "u2",
                "mode": "car",
                "purpose": "work",
                "distance_km": 1.2,
                "is_peak": False,
                "origin_time_utc": "2026-01-01T09:30:00Z",
                "destination_time_utc": "2026-01-01T10:00:00Z",
                "origin_longitude": -70.80,
                "origin_latitude": -33.60,
                "destination_longitude": -70.79,
                "destination_latitude": -33.59,
            },
            {
                "movement_id": "m3",
                "user_id": "u3",
                "mode": "walk",
                "purpose": None,
                "distance_km": 0.4,
                "is_peak": False,
                "origin_time_utc": "2026-01-01T10:00:00Z",
                "destination_time_utc": "2026-01-01T10:20:00Z",
                "origin_longitude": -70.66,
                "origin_latitude": -33.45,
                "destination_longitude": -70.81,
                "destination_latitude": -33.61,
            },
            {
                "movement_id": "m4",
                "user_id": "u4",
                "mode": "bus",
                "purpose": "leisure",
                "distance_km": 25.0,
                "is_peak": True,
                "origin_time_utc": "2026-01-01T11:00:00Z",
                "destination_time_utc": "2026-01-01T11:30:00Z",
                "origin_longitude": -70.90,
                "origin_latitude": -33.70,
                "destination_longitude": -70.91,
                "destination_latitude": -33.71,
            },
        ]
    )

    df["origin_time_utc"] = pd.to_datetime(df["origin_time_utc"], utc=True)
    df["destination_time_utc"] = pd.to_datetime(df["destination_time_utc"], utc=True)

    df["origin_h3_index"] = [
        make_valid_h3(lat, lon, 8)
        for lat, lon in zip(df["origin_latitude"], df["origin_longitude"])
    ]
    df["destination_h3_index"] = [
        make_valid_h3(lat, lon, 8)
        for lat, lon in zip(df["destination_latitude"], df["destination_longitude"])
    ]
    return df


@pytest.fixture()
def make_filter_tripdataset(base_filter_schema: TripSchema):
    """Factory mínima de TripDataset para smoke tests públicos de filter_trips."""

    def _make(*, validated: bool = True, temporal_tier: str = "tier_1") -> TripDataset:
        df = make_filter_test_dataframe()

        metadata = {
            "dataset_id": "ds_filter_small",
            "is_validated": validated,
            "temporal": {
                "tier": temporal_tier,
                "fields_present": ["origin_time_utc", "destination_time_utc"]
                if temporal_tier == "tier_1"
                else ["origin_time_local_hhmm", "destination_time_local_hhmm"],
            },
            "h3": {
                "resolution": 8,
                "derived_fields": ["origin_h3_index", "destination_h3_index"],
            },
            "schema": {"schema_version": base_filter_schema.version},
            "domains_effective": {
                "mode": {"values": ["bus", "metro", "car", "walk"]},
                "purpose": {"values": ["work", "study", "leisure"]},
            },
            "events": [
                {
                    "op": "import_trips",
                    "ts_utc": "2026-04-03T12:00:00Z",
                    "parameters": {},
                    "summary": {"rows_in": len(df), "rows_out": len(df)},
                    "issues_summary": {
                        "counts": {"info": 0, "warning": 0, "error": 0},
                        "top_codes": [],
                    },
                }
            ],
        }

        schema_effective = TripSchemaEffective(
            dtype_effective={
                "movement_id": "string",
                "user_id": "string",
                "mode": "categorical",
                "purpose": "categorical",
                "distance_km": "float",
                "is_peak": "bool",
                "origin_time_utc": "datetime",
                "destination_time_utc": "datetime",
                "origin_longitude": "float",
                "origin_latitude": "float",
                "destination_longitude": "float",
                "destination_latitude": "float",
                "origin_h3_index": "string",
                "destination_h3_index": "string",
            },
            domains_effective=deepcopy(metadata["domains_effective"]),
            temporal={"tier": temporal_tier},
            fields_effective=list(df.columns),
        )

        return TripDataset(
            data=df,
            schema=base_filter_schema,
            schema_version=base_filter_schema.version,
            provenance={"source": {"name": "synthetic_filter_smoke_tests"}},
            field_correspondence={},
            value_correspondence={},
            metadata=metadata,
            schema_effective=schema_effective,
        )

    return _make


@pytest.fixture()
def make_filter_tripdataset_tier2(make_filter_tripdataset):
    """Factory de TripDataset con metadata temporal Tier 2 no evaluable por filtro absoluto."""

    def _make() -> TripDataset:
        return make_filter_tripdataset(temporal_tier="tier_2")

    return _make


def _issue_codes(report: OperationReport) -> list[str]:
    """Retorna códigos de issues del reporte en el orden emitido."""
    return [issue.code for issue in report.issues]


def _last_event(trips: TripDataset) -> dict[str, Any]:
    """Retorna el último evento registrado en metadata."""
    events = trips.metadata.get("events", [])
    assert isinstance(events, list)
    assert events
    return events[-1]


def _assert_event_matches_report(filtered: TripDataset, report: OperationReport) -> None:
    """Verifica alineación mínima entre evento filter_trips y OperationReport."""
    event = _last_event(filtered)
    assert event["op"] == "filter_trips"
    assert event["summary"] == report.summary
    assert event["parameters"] == report.parameters
    assert "issues_summary" in event


def test_filter_trips_smoke_happy_path_where_time_bbox(make_filter_tripdataset) -> None:
    """Verifica el camino feliz público con filtros where, time y bbox."""
    trips = make_filter_tripdataset(validated=True)
    data_before = trips.data.copy(deep=True)
    events_before = deepcopy(trips.metadata["events"])
    validated_before = trips.metadata["is_validated"]

    options = FilterOptions(
        where={"mode": ["bus", "metro"]},
        time=TimeFilter(
            start="2026-01-01T07:00:00Z",
            end="2026-01-01T09:00:00Z",
            predicate="overlaps",
        ),
        bbox=(-70.70, -33.50, -70.60, -33.40),
    )

    filtered, report = filter_trips(
        trips,
        options=options,
        max_issues=20,
        sample_rows_per_issue=3,
    )

    expected = data_before.loc[data_before["movement_id"].isin(["m0", "m1"])]

    assert isinstance(filtered, TripDataset)
    assert isinstance(report, OperationReport)
    assert report.ok is True
    assert filtered is not trips
    pd.testing.assert_frame_equal(filtered.data, expected)
    pd.testing.assert_frame_equal(trips.data, data_before)
    assert trips.metadata["events"] == events_before
    assert trips.metadata["is_validated"] is validated_before

    assert report.summary["rows_in"] == len(data_before)
    assert report.summary["rows_out"] == len(expected)
    assert report.summary["dropped_total"] == len(data_before) - len(expected)
    assert report.summary["filters_requested"] == ["where", "time", "bbox"]
    assert report.summary["filters_applied"] == ["where", "time", "bbox"]
    assert report.summary["filters_omitted"] == []

    assert report.parameters["max_issues"] == 20
    assert report.parameters["sample_rows_per_issue"] == 3
    assert report.parameters["origin_h3_field"] == "origin_h3_index"
    assert report.parameters["destination_h3_field"] == "destination_h3_index"

    assert filtered.metadata["is_validated"] is validated_before
    assert len(filtered.metadata["events"]) == len(events_before) + 1
    _assert_event_matches_report(filtered, report)


def test_filter_trips_smoke_keep_metadata_false_returns_minimal_metadata(
    make_filter_tripdataset,
) -> None:
    """Verifica que keep_metadata=False produzca metadata mínima y no agregue evento."""
    trips = make_filter_tripdataset(validated=True)
    data_before = trips.data.copy(deep=True)
    events_before = deepcopy(trips.metadata["events"])

    filtered, report = filter_trips(
        trips,
        options=FilterOptions(
            where={"mode": "bus"},
            keep_metadata=False,
        ),
    )

    expected = data_before.loc[data_before["mode"].eq("bus")]

    assert isinstance(filtered, TripDataset)
    assert isinstance(report, OperationReport)
    assert report.ok is True
    pd.testing.assert_frame_equal(filtered.data, expected)
    pd.testing.assert_frame_equal(trips.data, data_before)
    assert trips.metadata["events"] == events_before

    assert report.summary["rows_in"] == len(data_before)
    assert report.summary["rows_out"] == len(expected)
    assert report.summary["dropped_total"] == len(data_before) - len(expected)

    assert "events" not in filtered.metadata
    assert set(filtered.metadata) == {
        "dataset_id",
        "is_validated",
        "temporal",
        "h3",
        "schema",
        "domains_effective",
    }
    assert filtered.metadata["dataset_id"] == trips.metadata["dataset_id"]
    assert filtered.metadata["is_validated"] is trips.metadata["is_validated"]


def test_filter_trips_smoke_non_fatal_degradation_applies_remaining_filters(
    make_filter_tripdataset_tier2,
) -> None:
    """Verifica degradación no fatal con strict=False y aplicación del filtro restante."""
    trips = make_filter_tripdataset_tier2()
    data_before = trips.data.copy(deep=True)

    options = FilterOptions(
        time=TimeFilter(
            start="2026-01-01T07:00:00Z",
            end="2026-01-01T09:00:00Z",
            predicate="overlaps",
        ),
        bbox=(-70.70, -33.50, -70.60, -33.40),
        strict=False,
    )

    filtered, report = filter_trips(trips, options=options)

    expected = data_before.loc[data_before["movement_id"].isin(["m0", "m1", "m3"])]
    codes = _issue_codes(report)

    assert isinstance(filtered, TripDataset)
    assert isinstance(report, OperationReport)
    assert report.ok is False
    assert "FLT.TIME.UNSUPPORTED_TIER" in codes
    assert "FLT.INFO.BBOX_APPLIED" in codes
    pd.testing.assert_frame_equal(filtered.data, expected)
    pd.testing.assert_frame_equal(trips.data, data_before)

    assert report.summary["filters_requested"] == ["time", "bbox"]
    assert report.summary["filters_applied"] == ["bbox"]
    assert report.summary["filters_omitted"] == ["time"]
    assert filtered.metadata["is_validated"] is trips.metadata["is_validated"]
    _assert_event_matches_report(filtered, report)


def test_filter_trips_smoke_strict_raises_filter_error_without_mutating_input(
    make_filter_tripdataset_tier2,
) -> None:
    """Verifica que strict=True escale un error recuperable a FilterError."""
    trips = make_filter_tripdataset_tier2()
    data_before = trips.data.copy(deep=True)
    events_before = deepcopy(trips.metadata["events"])
    validated_before = trips.metadata["is_validated"]

    with pytest.raises(FilterError) as exc_info:
        filter_trips(
            trips,
            options=FilterOptions(
                time=TimeFilter(
                    start="2026-01-01T07:00:00Z",
                    end="2026-01-01T09:00:00Z",
                    predicate="overlaps",
                ),
                strict=True,
            ),
        )

    err = exc_info.value
    assert err.issue is not None
    assert err.issue.code == "FLT.TIME.UNSUPPORTED_TIER"
    assert err.issues is not None

    pd.testing.assert_frame_equal(trips.data, data_before)
    assert trips.metadata["events"] == events_before
    assert trips.metadata["is_validated"] is validated_before


def test_filter_trips_smoke_invalid_bbox_aborts_without_side_effects(
    make_filter_tripdataset,
) -> None:
    """Verifica que un bbox inválido aborte sin mutar data ni metadata del input."""
    trips = make_filter_tripdataset()
    data_before = trips.data.copy(deep=True)
    events_before = deepcopy(trips.metadata["events"])
    validated_before = trips.metadata["is_validated"]

    with pytest.raises(ValueError) as exc_info:
        filter_trips(
            trips,
            options=FilterOptions(
                bbox=(-70.70, -33.50, -70.80, -33.40),
            ),
        )

    assert getattr(exc_info.value, "code", None) == "FLT.SPATIAL.INVALID_BBOX"
    pd.testing.assert_frame_equal(trips.data, data_before)
    assert trips.metadata["events"] == events_before
    assert trips.metadata["is_validated"] is validated_before


def test_filter_trips_smoke_empty_result_is_returnable_with_warning(
    make_filter_tripdataset,
) -> None:
    """Verifica que un resultado vacío sea retornable y quede reportado con warning."""
    trips = make_filter_tripdataset(validated=True)
    data_before = trips.data.copy(deep=True)

    options = FilterOptions(
        where={"mode": "bus"},
        time=TimeFilter(
            start="2026-01-01T09:00:00Z",
            end="2026-01-01T09:15:00Z",
            predicate="overlaps",
        ),
    )

    filtered, report = filter_trips(trips, options=options)

    assert isinstance(filtered, TripDataset)
    assert isinstance(report, OperationReport)
    assert report.ok is True
    assert "FLT.OUTPUT.EMPTY_RESULT" in _issue_codes(report)
    pd.testing.assert_frame_equal(trips.data, data_before)

    assert filtered.data.empty
    assert report.summary["rows_in"] == len(data_before)
    assert report.summary["rows_out"] == 0
    assert report.summary["dropped_total"] == len(data_before)
    assert filtered.metadata["is_validated"] is True
    _assert_event_matches_report(filtered, report)


def test_filter_trips_smoke_truncation_keeps_limits_and_event_consistency(
    make_filter_tripdataset_tier2,
) -> None:
    """Verifica truncamiento de issues, bloque limits y consistencia evento/reporte."""
    trips = make_filter_tripdataset_tier2()
    data_before = trips.data.copy(deep=True)
    max_issues = 2

    options = FilterOptions(
        where={
            "does_not_exist": "x",
            "mode": {"gt": "bus"},
            "purpose": {"in": {"work"}},
        },
        time=TimeFilter(
            start="2026-01-01T07:00:00Z",
            end="2026-01-01T09:00:00Z",
            predicate="overlaps",
        ),
        strict=False,
    )

    filtered, report = filter_trips(
        trips,
        options=options,
        max_issues=max_issues,
    )

    assert isinstance(filtered, TripDataset)
    assert isinstance(report, OperationReport)
    assert "FLT.LIMIT.ISSUES_TRUNCATED" in _issue_codes(report)
    assert report.summary["limits"]["issues_truncated"] is True
    assert report.summary["limits"]["max_issues"] == max_issues
    assert report.summary["limits"]["n_issues_emitted"] <= max_issues
    assert (
        report.summary["limits"]["n_issues_detected_total"]
        >= report.summary["limits"]["n_issues_emitted"]
    )

    pd.testing.assert_frame_equal(trips.data, data_before)
    _assert_event_matches_report(filtered, report)