from __future__ import annotations

import json
from typing import Any

import h3
import numpy as np
import pandas as pd
import pytest

from pylondrina.datasets import TripDataset
from pylondrina.reports import Issue
from pylondrina.schema import DomainSpec, FieldSpec, TripSchema, TripSchemaEffective
from pylondrina.transforms.filtering import (
    FilterOptions,
    TimeFilter,
    _json_safe_scalar,
    _normalize_bbox_or_abort,
    _normalize_filter_request,
    _normalize_h3_cells_or_abort,
    _normalize_iso_timestamp_or_abort,
    _normalize_polygon_or_abort,
    _to_json_serializable_or_none,
)


def make_valid_h3(lat: float = -33.45, lon: float = -70.66, res: int = 8) -> str:
    """Construye una celda H3 válida para fixtures mínimas de OP-05."""
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
    """Construye un FieldSpec mínimo para fixtures de normalización de OP-05."""
    return FieldSpec(
        name=name,
        dtype=dtype,
        required=required,
        domain=domain,
    )


@pytest.fixture()
def base_filter_schema() -> TripSchema:
    """Schema mínimo para probar normalización del request de OP-05."""
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


@pytest.fixture()
def make_filter_tripdataset(base_filter_schema: TripSchema):
    """Factory mínima de TripDataset para probar normalización de request."""

    def _make(data: pd.DataFrame | None = None) -> TripDataset:
        df = (
            data.copy(deep=True)
            if data is not None
            else pd.DataFrame(
                {
                    "movement_id": ["m0", "m1", "m2", "m3", "m4"],
                    "user_id": ["u0", "u1", "u2", "u3", "u4"],
                    "mode": ["bus", "metro", "car", "walk", "bus"],
                    "purpose": ["work", "study", "work", None, "leisure"],
                    "distance_km": [5.0, 12.5, 1.2, 0.4, 25.0],
                    "is_peak": [True, True, False, False, True],
                    "origin_time_utc": [
                        "2026-01-01T07:10:00Z",
                        "2026-01-01T08:00:00Z",
                        "2026-01-01T09:30:00Z",
                        "2026-01-01T08:10:00Z",
                        "2026-01-01T11:00:00Z",
                    ],
                    "destination_time_utc": [
                        "2026-01-01T07:40:00Z",
                        "2026-01-01T08:35:00Z",
                        "2026-01-01T10:00:00Z",
                        "2026-01-01T08:30:00Z",
                        "2026-01-01T11:30:00Z",
                    ],
                    "origin_longitude": [-70.66, -70.64, -70.80, -70.66, -70.62],
                    "origin_latitude": [-33.45, -33.46, -33.60, -33.45, -33.43],
                    "destination_longitude": [-70.65, -70.63, -70.79, -70.90, -70.61],
                    "destination_latitude": [-33.44, -33.45, -33.59, -33.70, -33.42],
                }
            )
        )

        df["origin_h3_index"] = [
            make_valid_h3(lat, lon, 8)
            for lat, lon in zip(df["origin_latitude"], df["origin_longitude"])
        ]
        df["destination_h3_index"] = [
            make_valid_h3(lat, lon, 8)
            for lat, lon in zip(df["destination_latitude"], df["destination_longitude"])
        ]

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
            domains_effective={
                "mode": {"values": ["bus", "metro", "car", "walk"]},
                "purpose": {"values": ["work", "study", "leisure"]},
            },
            temporal={
                "tier": "tier_1",
                "fields_present": ["origin_time_utc", "destination_time_utc"],
            },
            fields_effective=list(df.columns),
        )

        return TripDataset(
            data=df,
            schema=base_filter_schema,
            schema_version=base_filter_schema.version,
            provenance={"source": {"name": "synthetic_filter_request_tests"}},
            field_correspondence={},
            value_correspondence={},
            metadata={
                "dataset_id": "ds_filter_request_small",
                "is_validated": True,
                "events": [],
                "domains_effective": schema_effective.domains_effective,
                "temporal": schema_effective.temporal,
            },
            schema_effective=schema_effective,
        )

    return _make


def _issue_codes(issues: list[Issue]) -> list[str]:
    """Retorna códigos de issues en el orden emitido."""
    return [issue.code for issue in issues]


def _assert_json_safe(obj: Any, label: str = "object") -> None:
    """Verifica que un objeto pueda serializarse como JSON."""
    try:
        json.dumps(obj)
    except Exception as exc:
        raise AssertionError(f"{label} no es JSON-safe: {exc}") from exc


def test_json_serialization_helpers_normalize_scalars_and_nested_payloads() -> None:
    """Verifica serialización JSON-safe de escalares y estructuras anidadas."""
    ts = pd.Timestamp("2026-01-01T07:00:00Z")

    assert _json_safe_scalar(None) is None
    assert _json_safe_scalar(np.nan) is None
    assert _json_safe_scalar(ts).startswith("2026-01-01T07:00:00")
    assert _json_safe_scalar(True) is True

    payload = {
        "a": 1,
        "b": [ts, np.nan, {"x": (1, 2), "y": None}],
    }

    serialized = _to_json_serializable_or_none(payload)

    assert serialized["a"] == payload["a"]
    assert serialized["b"][0].startswith("2026-01-01T07:00:00")
    assert serialized["b"][1] is None
    assert serialized["b"][2]["x"] == [1, 2]
    assert serialized["b"][2]["y"] is None
    _assert_json_safe(serialized, "serialized_payload")


def test_normalize_iso_timestamp_converts_to_utc_z_and_rejects_unparseable_values() -> None:
    """Verifica normalización temporal a UTC con Z y error fatal para timestamp inválido."""
    issues: list[Issue] = []
    value = "2026-01-01T08:00:00-03:00"

    normalized = _normalize_iso_timestamp_or_abort(
        value,
        issues,
        code="FLT.TIME.INVALID_RANGE",
        predicate="overlaps",
        value_role="start",
    )

    assert normalized == "2026-01-01T11:00:00Z"
    assert issues == []

    invalid_issues: list[Issue] = []
    with pytest.raises(ValueError):
        _normalize_iso_timestamp_or_abort(
            12345,
            invalid_issues,
            code="FLT.TIME.INVALID_RANGE",
            predicate="overlaps",
            value_role="end",
        )

    assert _issue_codes(invalid_issues) == ["FLT.TIME.INVALID_RANGE"]


def test_normalize_bbox_returns_canonical_tuple_and_rejects_inverted_bbox() -> None:
    """Verifica bbox canónico y error fatal cuando los límites vienen invertidos."""
    issues: list[Issue] = []
    raw_bbox = [-70.7, -33.5, -70.6, -33.4]

    bbox = _normalize_bbox_or_abort(raw_bbox, issues)

    assert bbox == tuple(raw_bbox)
    assert issues == []

    invalid_issues: list[Issue] = []
    with pytest.raises(ValueError):
        _normalize_bbox_or_abort([-70.7, -33.5, -70.8, -33.4], invalid_issues)

    assert _issue_codes(invalid_issues) == ["FLT.SPATIAL.INVALID_BBOX"]


def test_normalize_polygon_returns_vertices_and_rejects_too_short_polygon() -> None:
    """Verifica polígono mínimo válido y error fatal para geometría con menos de tres vértices."""
    issues: list[Issue] = []
    raw_polygon = [
        (-70.7, -33.5),
        (-70.6, -33.5),
        (-70.6, -33.4),
        (-70.7, -33.4),
    ]

    polygon = _normalize_polygon_or_abort(raw_polygon, issues)

    assert polygon == raw_polygon
    assert issues == []

    invalid_issues: list[Issue] = []
    with pytest.raises(ValueError):
        _normalize_polygon_or_abort(
            [(-70.7, -33.5), (-70.6, -33.5)],
            invalid_issues,
        )

    assert _issue_codes(invalid_issues) == ["FLT.SPATIAL.INVALID_POLYGON"]


def test_normalize_h3_cells_deduplicates_preserving_order_and_rejects_invalid_cells(
    make_filter_tripdataset,
) -> None:
    """Verifica deduplicación H3 preservando orden y error fatal para celdas inválidas."""
    trips = make_filter_tripdataset()
    valid_h3_a = trips.data.loc[0, "origin_h3_index"]
    valid_h3_b = trips.data.loc[1, "origin_h3_index"]

    issues: list[Issue] = []
    cells = _normalize_h3_cells_or_abort(
        [valid_h3_a, valid_h3_a, valid_h3_b],
        issues,
    )

    assert cells == [valid_h3_a, valid_h3_b]
    assert issues == []

    invalid_issues: list[Issue] = []
    with pytest.raises(ValueError):
        _normalize_h3_cells_or_abort(
            [valid_h3_a, "NOT_A_VALID_H3"],
            invalid_issues,
        )

    assert _issue_codes(invalid_issues) == ["FLT.SPATIAL.INVALID_H3_CELLS"]


def test_normalize_filter_request_builds_effective_options_parameters_and_requested_filters(
    make_filter_tripdataset,
) -> None:
    """Verifica normalización canónica del request efectivo y parameters JSON-safe."""
    trips = make_filter_tripdataset()
    issues: list[Issue] = []

    h3_allow = [
        trips.data.loc[0, "origin_h3_index"],
        trips.data.loc[1, "origin_h3_index"],
    ]

    options = FilterOptions(
        where={
            "mode": ["bus", "metro"],
            "distance_km": {"gte": 1.0, "lt": 15.0},
        },
        time=TimeFilter(
            start="2026-01-01T04:00:00-03:00",
            end="2026-01-01T06:00:00-03:00",
            predicate="overlaps",
        ),
        bbox=(-70.7, -33.5, -70.6, -33.4),
        h3_cells=h3_allow,
    )

    options_eff, parameters, filters_requested = _normalize_filter_request(
        trips,
        options=options,
        max_issues=10,
        sample_rows_per_issue=3,
        issues=issues,
    )

    assert issues == []
    assert isinstance(options_eff, FilterOptions)
    assert filters_requested == ["where", "time", "bbox", "h3_cells"]

    assert options_eff.where == options.where
    assert options_eff.time == TimeFilter(
        start="2026-01-01T07:00:00Z",
        end="2026-01-01T09:00:00Z",
        predicate="overlaps",
    )
    assert options_eff.bbox == tuple(options.bbox)
    assert options_eff.h3_cells == h3_allow

    assert parameters["where"] == options.where
    assert parameters["time"]["start"] == "2026-01-01T07:00:00Z"
    assert parameters["time"]["end"] == "2026-01-01T09:00:00Z"
    assert parameters["time"]["predicate"] == "overlaps"
    assert parameters["bbox"] == list(options.bbox)
    assert parameters["h3_cells"] == h3_allow
    assert parameters["spatial_predicate"] == "origin"
    assert parameters["origin_h3_field"] == "origin_h3_index"
    assert parameters["destination_h3_field"] == "destination_h3_index"
    assert parameters["keep_metadata"] is True
    assert parameters["strict"] is False
    assert parameters["max_issues"] == 10
    assert parameters["sample_rows_per_issue"] == 3
    _assert_json_safe(parameters, "filter_request_parameters")


def test_normalize_filter_request_aborts_on_invalid_bbox_configuration(
    make_filter_tripdataset,
) -> None:
    """Verifica abort temprano del request cuando la geometría bbox es inválida."""
    trips = make_filter_tripdataset()
    issues: list[Issue] = []

    bad_options = FilterOptions(bbox=(-70.7, -33.5, -70.8, -33.4))

    with pytest.raises(ValueError):
        _normalize_filter_request(
            trips,
            options=bad_options,
            max_issues=10,
            sample_rows_per_issue=3,
            issues=issues,
        )

    assert _issue_codes(issues) == ["FLT.SPATIAL.INVALID_BBOX"]


def test_normalize_filter_request_aborts_on_invalid_time_range(
    make_filter_tripdataset,
) -> None:
    """Verifica abort temprano del request cuando el rango temporal queda vacío o invertido."""
    trips = make_filter_tripdataset()
    issues: list[Issue] = []

    bad_options = FilterOptions(
        time=TimeFilter(
            start="2026-01-01T09:00:00Z",
            end="2026-01-01T07:00:00Z",
            predicate="overlaps",
        )
    )

    with pytest.raises(ValueError):
        _normalize_filter_request(
            trips,
            options=bad_options,
            max_issues=10,
            sample_rows_per_issue=3,
            issues=issues,
        )

    assert "FLT.TIME.INVALID_RANGE" in _issue_codes(issues)


def test_normalize_filter_request_aborts_on_invalid_polygon_and_invalid_h3_cells(
    make_filter_tripdataset,
) -> None:
    """Verifica abort temprano para polygon ilegible y whitelist H3 inválida."""
    trips = make_filter_tripdataset()

    polygon_issues: list[Issue] = []
    with pytest.raises(ValueError):
        _normalize_filter_request(
            trips,
            options=FilterOptions(
                polygon=[(-70.7, -33.5), (-70.6, -33.5)],
            ),
            max_issues=10,
            sample_rows_per_issue=3,
            issues=polygon_issues,
        )

    assert _issue_codes(polygon_issues) == ["FLT.SPATIAL.INVALID_POLYGON"]

    h3_issues: list[Issue] = []
    with pytest.raises(ValueError):
        _normalize_filter_request(
            trips,
            options=FilterOptions(
                h3_cells=[trips.data.loc[0, "origin_h3_index"], "NOT_A_VALID_H3"],
            ),
            max_issues=10,
            sample_rows_per_issue=3,
            issues=h3_issues,
        )

    assert _issue_codes(h3_issues) == ["FLT.SPATIAL.INVALID_H3_CELLS"]