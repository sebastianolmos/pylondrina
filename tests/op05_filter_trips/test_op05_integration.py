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


def h3_from_latlon(lat: float, lon: float, res: int = 8) -> str:
    """Construye una celda H3 válida para fixtures de integración de OP-05."""
    if hasattr(h3, "latlng_to_cell"):
        return h3.latlng_to_cell(lat, lon, res)
    return h3.geo_to_h3(lat, lon, res)


def get_last_event(trips: TripDataset) -> dict[str, Any]:
    """Retorna el último evento registrado en metadata."""
    events = trips.metadata.get("events", [])
    assert isinstance(events, list)
    assert events
    return events[-1]


def issue_codes(report: OperationReport) -> list[str]:
    """Retorna códigos de issues del reporte en orden de emisión."""
    return [issue.code for issue in report.issues]


def assert_codes_present(report: OperationReport, expected_codes: list[str]) -> None:
    """Verifica que los códigos esperados estén presentes en el reporte."""
    observed = issue_codes(report)
    for code in expected_codes:
        assert code in observed, f"No apareció {code!r}. Observados={observed}"


def assert_input_not_mutated(
    original: TripDataset,
    data_before: pd.DataFrame,
    events_before: list[dict[str, Any]],
    validated_before: bool,
) -> None:
    """Verifica que la operación no haya mutado data, eventos ni estado validado del input."""
    pd.testing.assert_frame_equal(original.data, data_before)
    assert original.metadata["events"] == events_before
    assert original.metadata["is_validated"] is validated_before


def assert_event_matches_report(filtered: TripDataset, report: OperationReport) -> dict[str, Any]:
    """Verifica consistencia mínima entre último evento filter_trips y OperationReport."""
    event = get_last_event(filtered)
    assert event["op"] == "filter_trips"
    assert event["summary"] == report.summary
    assert event["parameters"] == report.parameters
    assert "issues_summary" in event
    return event


def make_filter_integration_schema() -> TripSchema:
    """Construye el schema rico usado por los tests de integración de OP-05."""
    fields = {
        "movement_id": FieldSpec(name="movement_id", dtype="string", required=True),
        "user_id": FieldSpec(name="user_id", dtype="string", required=True),
        "trip_id": FieldSpec(name="trip_id", dtype="string", required=True),
        "movement_seq": FieldSpec(name="movement_seq", dtype="int", required=True),
        "origin_longitude": FieldSpec(name="origin_longitude", dtype="float", required=True),
        "origin_latitude": FieldSpec(name="origin_latitude", dtype="float", required=True),
        "destination_longitude": FieldSpec(name="destination_longitude", dtype="float", required=True),
        "destination_latitude": FieldSpec(name="destination_latitude", dtype="float", required=True),
        "origin_h3_index": FieldSpec(name="origin_h3_index", dtype="string", required=True),
        "destination_h3_index": FieldSpec(name="destination_h3_index", dtype="string", required=True),
        "origin_time_utc": FieldSpec(name="origin_time_utc", dtype="datetime", required=True),
        "destination_time_utc": FieldSpec(name="destination_time_utc", dtype="datetime", required=True),
        "origin_time_local_hhmm": FieldSpec(name="origin_time_local_hhmm", dtype="string"),
        "destination_time_local_hhmm": FieldSpec(name="destination_time_local_hhmm", dtype="string"),
        "origin_municipality": FieldSpec(name="origin_municipality", dtype="string"),
        "destination_municipality": FieldSpec(name="destination_municipality", dtype="string"),
        "mode": FieldSpec(
            name="mode",
            dtype="categorical",
            domain=DomainSpec(
                values=["walk", "bicycle", "car", "bus", "metro", "other", "scooter"],
                extendable=True,
            ),
        ),
        "purpose": FieldSpec(
            name="purpose",
            dtype="categorical",
            domain=DomainSpec(
                values=["home", "work", "study", "shopping", "leisure", "other"],
                extendable=True,
            ),
        ),
        "day_type": FieldSpec(
            name="day_type",
            dtype="categorical",
            domain=DomainSpec(values=["weekday", "weekend", "holiday"], extendable=True),
        ),
        "time_period": FieldSpec(
            name="time_period",
            dtype="categorical",
            domain=DomainSpec(values=["night", "morning", "midday", "afternoon", "evening"], extendable=True),
        ),
        "user_gender": FieldSpec(
            name="user_gender",
            dtype="categorical",
            domain=DomainSpec(values=["female", "male", "other", "unknown"], extendable=True),
        ),
        "income_quintile": FieldSpec(
            name="income_quintile",
            dtype="categorical",
            domain=DomainSpec(values=["1", "2", "3", "4", "5", "unknown"], extendable=True),
        ),
        "trip_weight": FieldSpec(name="trip_weight", dtype="float"),
        "distance_km": FieldSpec(name="distance_km", dtype="float"),
        "is_peak": FieldSpec(name="is_peak", dtype="bool"),
        "mode_sequence": FieldSpec(name="mode_sequence", dtype="string"),
    }

    return TripSchema(
        version="test-filter-1.0",
        fields=fields,
        required=[
            "movement_id",
            "user_id",
            "trip_id",
            "movement_seq",
            "origin_longitude",
            "origin_latitude",
            "destination_longitude",
            "destination_latitude",
            "origin_h3_index",
            "destination_h3_index",
            "origin_time_utc",
            "destination_time_utc",
        ],
    )


def make_filter_integration_dataframe() -> pd.DataFrame:
    """Construye el dataframe rico usado por los tests de integración de OP-05."""
    rows = [
        dict(
            movement_id="m0", user_id="u0", trip_id="t0", movement_seq=0,
            mode="bus", purpose="work", day_type="weekday", time_period="morning",
            user_gender="female", income_quintile="3", trip_weight=1.2, distance_km=5.0,
            is_peak=True, mode_sequence="bus",
            origin_municipality="Santiago", destination_municipality="Providencia",
            origin_longitude=-70.66, origin_latitude=-33.45,
            destination_longitude=-70.65, destination_latitude=-33.44,
            origin_time_utc="2026-01-01T07:10:00Z",
            destination_time_utc="2026-01-01T07:40:00Z",
        ),
        dict(
            movement_id="m1", user_id="u1", trip_id="t1", movement_seq=0,
            mode="metro", purpose="study", day_type="weekday", time_period="morning",
            user_gender="male", income_quintile="2", trip_weight=0.8, distance_km=12.5,
            is_peak=True, mode_sequence="metro",
            origin_municipality="Ñuñoa", destination_municipality="Santiago",
            origin_longitude=-70.64, origin_latitude=-33.46,
            destination_longitude=-70.63, destination_latitude=-33.45,
            origin_time_utc="2026-01-01T08:00:00Z",
            destination_time_utc="2026-01-01T08:35:00Z",
        ),
        dict(
            movement_id="m2", user_id="u2", trip_id="t2", movement_seq=0,
            mode="car", purpose="work", day_type="weekday", time_period="morning",
            user_gender="female", income_quintile="5", trip_weight=1.5, distance_km=1.2,
            is_peak=False, mode_sequence="car",
            origin_municipality="Puente Alto", destination_municipality="Puente Alto",
            origin_longitude=-70.80, origin_latitude=-33.60,
            destination_longitude=-70.79, destination_latitude=-33.59,
            origin_time_utc="2026-01-01T09:30:00Z",
            destination_time_utc="2026-01-01T10:00:00Z",
        ),
        dict(
            movement_id="m3", user_id="u3", trip_id="t3", movement_seq=0,
            mode="walk", purpose="leisure", day_type="weekday", time_period="morning",
            user_gender="male", income_quintile="1", trip_weight=0.7, distance_km=0.4,
            is_peak=False, mode_sequence="walk",
            origin_municipality="Santiago", destination_municipality="Quilicura",
            origin_longitude=-70.66, origin_latitude=-33.45,
            destination_longitude=-70.81, destination_latitude=-33.61,
            origin_time_utc="2026-01-01T10:00:00Z",
            destination_time_utc="2026-01-01T10:20:00Z",
        ),
        dict(
            movement_id="m4", user_id="u4", trip_id="t4", movement_seq=0,
            mode="bus", purpose="leisure", day_type="weekend", time_period="midday",
            user_gender="female", income_quintile="4", trip_weight=1.1, distance_km=25.0,
            is_peak=True, mode_sequence="bus",
            origin_municipality="Maipú", destination_municipality="Maipú",
            origin_longitude=-70.90, origin_latitude=-33.70,
            destination_longitude=-70.91, destination_latitude=-33.71,
            origin_time_utc="2026-01-01T11:00:00Z",
            destination_time_utc="2026-01-01T11:30:00Z",
        ),
        dict(
            movement_id="m5", user_id="u5", trip_id="t5", movement_seq=0,
            mode="metro", purpose="work", day_type="weekday", time_period="morning",
            user_gender="other", income_quintile="2", trip_weight=1.0, distance_km=8.2,
            is_peak=True, mode_sequence="metro",
            origin_municipality="Providencia", destination_municipality="Santiago",
            origin_longitude=-70.62, origin_latitude=-33.43,
            destination_longitude=-70.63, destination_latitude=-33.44,
            origin_time_utc="2026-01-01T07:50:00Z",
            destination_time_utc="2026-01-01T08:10:00Z",
        ),
        dict(
            movement_id="m6", user_id="u6", trip_id="t6", movement_seq=0,
            mode="bicycle", purpose="work", day_type="weekday", time_period="morning",
            user_gender="male", income_quintile="1", trip_weight=0.9, distance_km=3.0,
            is_peak=True, mode_sequence="bicycle",
            origin_municipality="Providencia", destination_municipality="Ñuñoa",
            origin_longitude=-70.61, origin_latitude=-33.42,
            destination_longitude=-70.62, destination_latitude=-33.43,
            origin_time_utc="2026-01-01T08:20:00Z",
            destination_time_utc="2026-01-01T08:50:00Z",
        ),
        dict(
            movement_id="m7", user_id="u7", trip_id="t7", movement_seq=0,
            mode="bus", purpose="shopping", day_type="weekday", time_period="morning",
            user_gender="female", income_quintile="2", trip_weight=0.6, distance_km=6.0,
            is_peak=False, mode_sequence="bus",
            origin_municipality="Santiago", destination_municipality="Santiago",
            origin_longitude=-70.67, origin_latitude=-33.46,
            destination_longitude=-70.66, destination_latitude=-33.45,
            origin_time_utc="2026-01-01T06:30:00Z",
            destination_time_utc="2026-01-01T06:50:00Z",
        ),
        dict(
            movement_id="m8", user_id="u8", trip_id="t8", movement_seq=0,
            mode="car", purpose="work", day_type="weekday", time_period="morning",
            user_gender="female", income_quintile="3", trip_weight=1.3, distance_km=14.0,
            is_peak=True, mode_sequence="car",
            origin_municipality="Pudahuel", destination_municipality="Providencia",
            origin_longitude=-70.82, origin_latitude=-33.61,
            destination_longitude=-70.64, destination_latitude=-33.45,
            origin_time_utc="2026-01-01T08:10:00Z",
            destination_time_utc="2026-01-01T08:40:00Z",
        ),
        dict(
            movement_id="m9", user_id="u9", trip_id="t9", movement_seq=0,
            mode="metro", purpose="work", day_type="weekday", time_period="morning",
            user_gender="male", income_quintile="4", trip_weight=1.4, distance_km=10.5,
            is_peak=True, mode_sequence="metro",
            origin_municipality="Ñuñoa", destination_municipality="Providencia",
            origin_longitude=-70.63, origin_latitude=-33.44,
            destination_longitude=-70.64, destination_latitude=-33.45,
            origin_time_utc="2026-01-01T08:45:00Z",
            destination_time_utc="2026-01-01T09:15:00Z",
        ),
        dict(
            movement_id="m10", user_id="u10", trip_id="t10", movement_seq=0,
            mode="bus", purpose="work", day_type="weekday", time_period="morning",
            user_gender="female", income_quintile="3", trip_weight=1.0, distance_km=7.7,
            is_peak=True, mode_sequence="bus",
            origin_municipality="Santiago", destination_municipality="Providencia",
            origin_longitude=-70.65, origin_latitude=-33.44,
            destination_longitude=-70.64, destination_latitude=-33.43,
            origin_time_utc="2026-01-01T08:59:00Z",
            destination_time_utc="2026-01-01T09:05:00Z",
        ),
        dict(
            movement_id="m11", user_id="u11", trip_id="t11", movement_seq=0,
            mode="walk", purpose="study", day_type="holiday", time_period="afternoon",
            user_gender="unknown", income_quintile="1", trip_weight=0.5, distance_km=1.0,
            is_peak=False, mode_sequence="walk",
            origin_municipality="Providencia", destination_municipality="Ñuñoa",
            origin_longitude=-70.68, origin_latitude=-33.47,
            destination_longitude=-70.67, destination_latitude=-33.46,
            origin_time_utc="2026-01-01T14:00:00Z",
            destination_time_utc="2026-01-01T14:30:00Z",
        ),
    ]

    df = pd.DataFrame(rows)
    df["origin_time_utc"] = pd.to_datetime(df["origin_time_utc"], utc=True)
    df["destination_time_utc"] = pd.to_datetime(df["destination_time_utc"], utc=True)

    df["origin_time_local_hhmm"] = df["origin_time_utc"].dt.strftime("%H:%M")
    df["destination_time_local_hhmm"] = df["destination_time_utc"].dt.strftime("%H:%M")

    df["origin_h3_index"] = [
        h3_from_latlon(lat, lon, 8)
        for lat, lon in zip(df["origin_latitude"], df["origin_longitude"])
    ]
    df["destination_h3_index"] = [
        h3_from_latlon(lat, lon, 8)
        for lat, lon in zip(df["destination_latitude"], df["destination_longitude"])
    ]
    return df


def _build_schema_effective(df: pd.DataFrame) -> TripSchemaEffective:
    """Construye schema_effective consistente con el dataframe de integración."""
    dtype_effective = {
        "movement_id": "string",
        "user_id": "string",
        "trip_id": "string",
        "movement_seq": "int",
        "origin_longitude": "float",
        "origin_latitude": "float",
        "destination_longitude": "float",
        "destination_latitude": "float",
        "origin_h3_index": "string",
        "destination_h3_index": "string",
        "origin_time_utc": "datetime",
        "destination_time_utc": "datetime",
        "origin_time_local_hhmm": "string",
        "destination_time_local_hhmm": "string",
        "origin_municipality": "string",
        "destination_municipality": "string",
        "mode": "categorical",
        "purpose": "categorical",
        "day_type": "categorical",
        "time_period": "categorical",
        "user_gender": "categorical",
        "income_quintile": "categorical",
        "trip_weight": "float",
        "distance_km": "float",
        "is_peak": "bool",
        "mode_sequence": "string",
    }
    return TripSchemaEffective(
        dtype_effective={k: v for k, v in dtype_effective.items() if k in df.columns},
        domains_effective={
            "mode": {"values": ["walk", "bicycle", "car", "bus", "metro", "other", "scooter"]},
            "purpose": {"values": ["home", "work", "study", "shopping", "leisure", "other"]},
            "day_type": {"values": ["weekday", "weekend", "holiday"]},
            "time_period": {"values": ["night", "morning", "midday", "afternoon", "evening"]},
            "user_gender": {"values": ["female", "male", "other", "unknown"]},
            "income_quintile": {"values": ["1", "2", "3", "4", "5", "unknown"]},
        },
        temporal={"tier": "tier_1"},
        fields_effective=list(df.columns),
    )


def make_tripdataset_with_time_space_fields(
    *,
    validated: bool = False,
    temporal_tier: str = "tier_1",
) -> TripDataset:
    """Construye un TripDataset rico con campos temporales, espaciales y categóricos."""
    df = make_filter_integration_dataframe()
    schema = make_filter_integration_schema()

    metadata = {
        "dataset_id": "ds_filter_integration_rich",
        "is_validated": bool(validated),
        "temporal": {
            "tier": temporal_tier,
            "fields_present": (
                ["origin_time_utc", "destination_time_utc"]
                if temporal_tier == "tier_1"
                else ["origin_time_local_hhmm", "destination_time_local_hhmm"]
            ),
        },
        "h3": {
            "resolution": 8,
            "derived_fields": ["origin_h3_index", "destination_h3_index"],
        },
        "schema": {"schema_version": schema.version},
        "domains_effective": {
            "mode": {"values": ["walk", "bicycle", "car", "bus", "metro", "other", "scooter"]},
            "purpose": {"values": ["home", "work", "study", "shopping", "leisure", "other"]},
        },
        "events": [
            {
                "op": "import_trips",
                "ts_utc": "2026-04-03T12:00:00Z",
                "parameters": {"source_name": "synthetic_filter_integration"},
                "summary": {"rows_in": len(df), "rows_out": len(df)},
                "issues_summary": {"counts": {"info": 0, "warning": 0, "error": 0}, "top_codes": []},
            }
        ],
    }

    if validated:
        metadata["events"].append(
            {
                "op": "validate_trips",
                "ts_utc": "2026-04-03T12:10:00Z",
                "parameters": {"strict": False},
                "summary": {"ok": True, "n_rows": len(df), "n_errors": 0},
                "issues_summary": {"counts": {"info": 0, "warning": 0, "error": 0}, "top_codes": []},
            }
        )

    schema_effective = _build_schema_effective(df)
    schema_effective.temporal = {"tier": temporal_tier}

    return TripDataset(
        data=df,
        schema=schema,
        schema_version=schema.version,
        provenance={"source": {"name": "synthetic_filter_integration"}},
        field_correspondence={},
        value_correspondence={},
        metadata=metadata,
        schema_effective=schema_effective,
    )


@pytest.fixture()
def tripdataset_with_time_space_fields() -> TripDataset:
    """Fixture rica base con temporalidad Tier 1 y estado no validado."""
    return make_tripdataset_with_time_space_fields(validated=False, temporal_tier="tier_1")


@pytest.fixture()
def tripdataset_canonical_small() -> TripDataset:
    """Fixture pequeña no validada derivada de las primeras filas del dataset rico."""
    trips = make_tripdataset_with_time_space_fields(validated=False, temporal_tier="tier_1")
    trips.data = trips.data.iloc[:8].reset_index(drop=True)
    trips.metadata["dataset_id"] = "ds_filter_integration_small"
    trips.metadata["events"][0]["summary"] = {
        "rows_in": len(trips.data),
        "rows_out": len(trips.data),
    }
    trips.schema_effective.fields_effective = list(trips.data.columns)
    return trips


@pytest.fixture()
def tripdataset_validated_small() -> TripDataset:
    """Fixture pequeña validada derivada de las primeras filas del dataset rico."""
    trips = make_tripdataset_with_time_space_fields(validated=True, temporal_tier="tier_1")
    trips.data = trips.data.iloc[:8].reset_index(drop=True)
    trips.metadata["dataset_id"] = "ds_filter_integration_small"
    trips.metadata["events"][0]["summary"] = {
        "rows_in": len(trips.data),
        "rows_out": len(trips.data),
    }
    trips.metadata["events"][1]["summary"]["n_rows"] = len(trips.data)
    trips.schema_effective.fields_effective = list(trips.data.columns)
    return trips


def _bbox_origin_mask(df: pd.DataFrame, bbox: tuple[float, float, float, float]) -> pd.Series:
    """Construye máscara bbox sobre origen."""
    return df["origin_longitude"].between(bbox[0], bbox[2]) & df["origin_latitude"].between(bbox[1], bbox[3])


def _bbox_destination_mask(df: pd.DataFrame, bbox: tuple[float, float, float, float]) -> pd.Series:
    """Construye máscara bbox sobre destino."""
    return df["destination_longitude"].between(bbox[0], bbox[2]) & df["destination_latitude"].between(bbox[1], bbox[3])


def test_filter_trips_integration_combines_where_time_and_bbox() -> None:
    """Verifica el caso principal con where, time, bbox, reporte, evento y no mutación."""
    trips = make_tripdataset_with_time_space_fields(validated=True, temporal_tier="tier_1")
    data_before = trips.data.copy(deep=True)
    events_before = deepcopy(trips.metadata["events"])
    validated_before = trips.metadata["is_validated"]

    bbox = (-70.70, -33.50, -70.60, -33.40)
    options = FilterOptions(
        where={
            "mode": ["bus", "metro"],
            "purpose": ["work", "study"],
        },
        time=TimeFilter(
            start="2026-01-01T07:00:00Z",
            end="2026-01-01T09:00:00Z",
            predicate="overlaps",
        ),
        bbox=bbox,
    )

    filtered, report = filter_trips(
        trips,
        options=options,
        max_issues=50,
        sample_rows_per_issue=5,
    )

    where_mask = data_before["mode"].isin(options.where["mode"]) & data_before["purpose"].isin(options.where["purpose"])
    time_mask = data_before["origin_time_utc"].lt(pd.Timestamp(options.time.end)) & data_before[
        "destination_time_utc"
    ].gt(pd.Timestamp(options.time.start))
    bbox_mask = _bbox_origin_mask(data_before, bbox)
    expected = data_before.loc[where_mask & time_mask & bbox_mask]

    assert isinstance(filtered, TripDataset)
    assert isinstance(report, OperationReport)
    assert report.ok is True
    pd.testing.assert_frame_equal(filtered.data, expected)
    assert_input_not_mutated(trips, data_before, events_before, validated_before)

    assert report.summary["rows_in"] == len(data_before)
    assert report.summary["rows_out"] == len(expected)
    assert report.summary["dropped_total"] == len(data_before) - len(expected)
    assert report.summary["filters_requested"] == ["where", "time", "bbox"]
    assert report.summary["filters_applied"] == ["where", "time", "bbox"]
    assert report.summary["filters_omitted"] == []

    assert report.parameters["max_issues"] == 50
    assert report.parameters["sample_rows_per_issue"] == 5
    assert report.parameters["origin_h3_field"] == "origin_h3_index"
    assert report.parameters["destination_h3_field"] == "destination_h3_index"

    assert_codes_present(
        report,
        [
            "FLT.INFO.WHERE_APPLIED",
            "FLT.INFO.TIME_APPLIED",
            "FLT.INFO.BBOX_APPLIED",
        ],
    )

    assert filtered.metadata["is_validated"] is validated_before
    assert len(filtered.metadata["events"]) == len(events_before) + 1
    assert_event_matches_report(filtered, report)


def test_filter_trips_integration_bbox_with_both_spatial_predicate() -> None:
    """Verifica que spatial_predicate='both' retenga solo viajes con ambos extremos dentro del bbox."""
    trips = make_tripdataset_with_time_space_fields(validated=False, temporal_tier="tier_1")
    data_before = trips.data.copy(deep=True)
    bbox = (-70.70, -33.50, -70.60, -33.40)

    filtered, report = filter_trips(
        trips,
        options=FilterOptions(
            bbox=bbox,
            spatial_predicate="both",
        ),
    )

    expected = data_before.loc[_bbox_origin_mask(data_before, bbox) & _bbox_destination_mask(data_before, bbox)]

    assert report.ok is True
    pd.testing.assert_frame_equal(filtered.data, expected)
    assert report.summary["rows_in"] == len(data_before)
    assert report.summary["rows_out"] == len(expected)
    assert report.summary["dropped_total"] == len(data_before) - len(expected)
    assert report.summary["filters_requested"] == ["bbox"]
    assert report.summary["filters_applied"] == ["bbox"]
    assert report.summary["filters_omitted"] == []
    assert_codes_present(report, ["FLT.INFO.BBOX_APPLIED"])


def test_filter_trips_integration_invalid_bbox_aborts_without_side_effects() -> None:
    """Verifica que un bbox inválido aborte temprano sin mutar data, eventos ni validación."""
    trips = make_tripdataset_with_time_space_fields(validated=True, temporal_tier="tier_1")
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
    assert_input_not_mutated(trips, data_before, events_before, validated_before)


def test_filter_trips_integration_strict_false_omits_tier2_time_and_applies_bbox() -> None:
    """Verifica degradación con strict=False: omite time en Tier 2 y aplica bbox."""
    trips = make_tripdataset_with_time_space_fields(validated=False, temporal_tier="tier_2")
    data_before = trips.data.copy(deep=True)
    events_before = deepcopy(trips.metadata["events"])
    validated_before = trips.metadata["is_validated"]
    bbox = (-70.70, -33.50, -70.60, -33.40)

    options = FilterOptions(
        time=TimeFilter(
            start="2026-01-01T07:00:00Z",
            end="2026-01-01T09:00:00Z",
            predicate="overlaps",
        ),
        bbox=bbox,
        strict=False,
    )

    filtered, report = filter_trips(trips, options=options)
    expected = data_before.loc[_bbox_origin_mask(data_before, bbox)]

    assert report.ok is False
    pd.testing.assert_frame_equal(filtered.data, expected)
    assert report.summary["filters_requested"] == ["time", "bbox"]
    assert report.summary["filters_applied"] == ["bbox"]
    assert report.summary["filters_omitted"] == ["time"]
    assert_codes_present(
        report,
        [
            "FLT.TIME.UNSUPPORTED_TIER",
            "FLT.INFO.BBOX_APPLIED",
        ],
    )
    assert filtered.metadata["is_validated"] is validated_before
    assert_event_matches_report(filtered, report)
    assert_input_not_mutated(trips, data_before, events_before, validated_before)


def test_filter_trips_integration_preserves_validated_state_and_appends_event(
    tripdataset_validated_small: TripDataset,
) -> None:
    """Verifica preservación de is_validated, append-only de eventos y consistencia evento/reporte."""
    trips = tripdataset_validated_small
    data_before = trips.data.copy(deep=True)
    events_before = deepcopy(trips.metadata["events"])
    validated_before = trips.metadata["is_validated"]

    filtered, report = filter_trips(
        trips,
        options=FilterOptions(where={"mode": "bus"}),
    )

    expected = data_before.loc[data_before["mode"].eq("bus")]

    assert report.ok is True
    pd.testing.assert_frame_equal(filtered.data, expected)
    assert filtered.metadata["is_validated"] is validated_before
    assert len(filtered.metadata["events"]) == len(events_before) + 1
    assert filtered.metadata["events"][:-1] == events_before
    assert_event_matches_report(filtered, report)
    assert_input_not_mutated(trips, data_before, events_before, validated_before)


def test_filter_trips_integration_keep_metadata_false_returns_minimal_metadata(
    tripdataset_validated_small: TripDataset,
) -> None:
    """Verifica la política de metadata mínima cuando keep_metadata=False."""
    trips = tripdataset_validated_small
    data_before = trips.data.copy(deep=True)
    events_before = deepcopy(trips.metadata["events"])
    validated_before = trips.metadata["is_validated"]

    options = FilterOptions(
        where={"mode": ["bus", "metro"]},
        keep_metadata=False,
    )

    filtered, report = filter_trips(trips, options=options)
    expected = data_before.loc[data_before["mode"].isin(options.where["mode"])]

    assert report.ok is True
    pd.testing.assert_frame_equal(filtered.data, expected)
    assert set(filtered.metadata.keys()) == {"dataset_id", "is_validated", "temporal", "h3", "schema", "domains_effective"}
    assert filtered.metadata["dataset_id"] == trips.metadata["dataset_id"]
    assert filtered.metadata["is_validated"] is validated_before
    assert "events" not in filtered.metadata
    assert_input_not_mutated(trips, data_before, events_before, validated_before)


def test_filter_trips_integration_strict_true_raises_filter_error_without_mutating_input() -> None:
    """Verifica que strict=True escale un error recuperable a FilterError sin mutar el input."""
    trips = make_tripdataset_with_time_space_fields(validated=False, temporal_tier="tier_2")
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
    assert_input_not_mutated(trips, data_before, events_before, validated_before)


def test_filter_trips_integration_empty_result_is_returnable_with_warning(
    tripdataset_canonical_small: TripDataset,
) -> None:
    """Verifica que un resultado vacío sea retornable y emita warning no fatal."""
    trips = tripdataset_canonical_small
    data_before = trips.data.copy(deep=True)
    events_before = deepcopy(trips.metadata["events"])
    validated_before = trips.metadata["is_validated"]

    filtered, report = filter_trips(
        trips,
        options=FilterOptions(where={"mode": "scooter"}),
    )

    assert report.ok is True
    assert filtered.data.empty is True
    assert report.summary["rows_in"] == len(data_before)
    assert report.summary["rows_out"] == 0
    assert report.summary["dropped_total"] == len(data_before)
    assert_codes_present(report, ["FLT.OUTPUT.EMPTY_RESULT"])
    assert filtered.metadata["is_validated"] is validated_before
    assert_event_matches_report(filtered, report)
    assert_input_not_mutated(trips, data_before, events_before, validated_before)


def test_filter_trips_integration_truncates_issues_and_keeps_report_event_consistency() -> None:
    """Verifica truncamiento de issues, bloque limits y consistencia entre evento y reporte."""
    trips = make_tripdataset_with_time_space_fields(validated=False, temporal_tier="tier_2")
    data_before = trips.data.copy(deep=True)
    events_before = deepcopy(trips.metadata["events"])
    validated_before = trips.metadata["is_validated"]
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

    assert "FLT.LIMIT.ISSUES_TRUNCATED" in issue_codes(report)
    assert "limits" in report.summary
    assert report.summary["limits"]["issues_truncated"] is True
    assert report.summary["limits"]["max_issues"] == max_issues
    assert report.summary["limits"]["n_issues_emitted"] <= max_issues
    assert report.summary["limits"]["n_issues_detected_total"] >= report.summary["limits"]["n_issues_emitted"]

    assert_event_matches_report(filtered, report)
    assert_input_not_mutated(trips, data_before, events_before, validated_before)