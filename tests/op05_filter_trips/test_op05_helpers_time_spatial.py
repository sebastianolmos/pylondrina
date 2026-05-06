from __future__ import annotations

from copy import deepcopy
from typing import Any

import h3
import pandas as pd
import pytest

from pylondrina.datasets import TripDataset
from pylondrina.reports import Issue
from pylondrina.schema import DomainSpec, FieldSpec, TripSchema, TripSchemaEffective
from pylondrina.transforms.filtering import (
    TimeFilter,
    _build_spatial_mask,
    _build_time_mask,
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
    """Construye un FieldSpec mínimo para fixtures temporales/espaciales de OP-05."""
    return FieldSpec(
        name=name,
        dtype=dtype,
        required=required,
        domain=domain,
    )


@pytest.fixture()
def base_filter_schema() -> TripSchema:
    """Schema mínimo para probar máscaras temporales y espaciales de OP-05."""
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
    """Construye el dataframe pequeño usado por los tests helper-level de OP-05."""
    rows = [
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

    df = pd.DataFrame(rows)
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
    """Factory mínima de TripDataset para probar máscaras temporales y espaciales."""

    def _make(data: pd.DataFrame | None = None, *, temporal_tier: str = "tier_1") -> TripDataset:
        df = data.copy(deep=True) if data is not None else make_filter_test_dataframe()

        metadata = {
            "dataset_id": "ds_filter_time_spatial_small",
            "is_validated": True,
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
            "domains_effective": {
                "mode": {"values": ["bus", "metro", "car", "walk"]},
                "purpose": {"values": ["work", "study", "leisure"]},
            },
            "events": [],
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
            provenance={"source": {"name": "synthetic_filter_time_spatial_tests"}},
            field_correspondence={},
            value_correspondence={},
            metadata=metadata,
            schema_effective=schema_effective,
        )

    return _make


@pytest.fixture()
def make_filter_tripdataset_tier2(make_filter_tripdataset):
    """Factory de TripDataset con metadata temporal Tier 2 no evaluable para filtro absoluto."""

    def _make() -> TripDataset:
        return make_filter_tripdataset(temporal_tier="tier_2")

    return _make


@pytest.fixture()
def make_filter_tripdataset_missing_destination_coords(make_filter_tripdataset):
    """Factory de TripDataset sin coordenadas de destino para probar degradación espacial."""

    def _make() -> TripDataset:
        trips = make_filter_tripdataset()
        trips.data = trips.data.drop(columns=["destination_longitude", "destination_latitude"])
        trips.schema_effective.fields_effective = list(trips.data.columns)
        return trips

    return _make


def _issue_codes(issues: list[Issue]) -> list[str]:
    """Retorna códigos de issues en el orden emitido."""
    return [issue.code for issue in issues]


def _kept_ids(mask: pd.Series, trips: TripDataset) -> list[str]:
    """Retorna movement_id retenidos por una máscara booleana."""
    return trips.data.loc[mask, "movement_id"].tolist()


def test_build_time_mask_applies_overlaps_on_tier1_dataset(make_filter_tripdataset) -> None:
    """Verifica filtro temporal evaluable en Tier 1 usando predicado overlaps."""
    trips = make_filter_tripdataset()
    issues: list[Issue] = []

    time_filter = TimeFilter(
        start="2026-01-01T07:00:00Z",
        end="2026-01-01T09:00:00Z",
        predicate="overlaps",
    )

    mask, applied, omitted = _build_time_mask(
        trips,
        time=time_filter,
        sample_rows_per_issue=3,
        issues=issues,
    )

    expected_mask = (
        trips.data["origin_time_utc"].lt(pd.Timestamp(time_filter.end))
        & trips.data["destination_time_utc"].gt(pd.Timestamp(time_filter.start))
    )

    assert applied is True
    assert omitted is False
    pd.testing.assert_series_equal(mask, expected_mask)
    assert _kept_ids(mask, trips) == _kept_ids(expected_mask, trips)
    assert "FLT.INFO.TIME_APPLIED" in _issue_codes(issues)


def test_build_time_mask_omits_time_filter_on_tier2_dataset(
    make_filter_tripdataset_tier2,
) -> None:
    """Verifica omisión recuperable del eje temporal cuando el dataset está en Tier 2."""
    trips = make_filter_tripdataset_tier2()
    issues: list[Issue] = []

    time_filter = TimeFilter(
        start="2026-01-01T07:00:00Z",
        end="2026-01-01T09:00:00Z",
        predicate="overlaps",
    )

    mask, applied, omitted = _build_time_mask(
        trips,
        time=time_filter,
        sample_rows_per_issue=3,
        issues=issues,
    )

    assert mask is None
    assert applied is False
    assert omitted is True
    assert _issue_codes(issues) == ["FLT.TIME.UNSUPPORTED_TIER"]


def test_build_spatial_mask_applies_bbox_polygon_and_h3_with_origin_predicate(
    make_filter_tripdataset,
) -> None:
    """Verifica subfiltros espaciales simultáneos sobre origen: bbox, polygon y h3_cells."""
    trips = make_filter_tripdataset()
    issues: list[Issue] = []

    bbox = (-70.70, -33.50, -70.60, -33.40)
    polygon = [
        (-70.70, -33.50),
        (-70.60, -33.50),
        (-70.60, -33.40),
        (-70.70, -33.40),
    ]
    h3_cells = [
        trips.data.loc[0, "origin_h3_index"],
        trips.data.loc[1, "origin_h3_index"],
        trips.data.loc[3, "origin_h3_index"],
    ]

    masks, applied, omitted = _build_spatial_mask(
        trips,
        bbox=bbox,
        polygon=polygon,
        h3_cells=h3_cells,
        spatial_predicate="origin",
        origin_h3_field="origin_h3_index",
        destination_h3_field="destination_h3_index",
        sample_rows_per_issue=3,
        issues=issues,
    )

    expected_bbox_mask = (
        trips.data["origin_longitude"].between(bbox[0], bbox[2])
        & trips.data["origin_latitude"].between(bbox[1], bbox[3])
    )
    expected_h3_mask = trips.data["origin_h3_index"].isin(set(h3_cells))

    assert applied == ["bbox", "polygon", "h3_cells"]
    assert omitted == []

    assert _kept_ids(masks["bbox"], trips) == _kept_ids(expected_bbox_mask, trips)
    assert _kept_ids(masks["polygon"], trips) == _kept_ids(expected_bbox_mask, trips)
    assert _kept_ids(masks["h3_cells"], trips) == _kept_ids(expected_h3_mask, trips)

    codes = _issue_codes(issues)
    assert "FLT.INFO.BBOX_APPLIED" in codes
    assert "FLT.INFO.POLYGON_APPLIED" in codes
    assert "FLT.INFO.H3_APPLIED" in codes


def test_build_spatial_mask_omits_bbox_and_h3_when_required_columns_are_missing(
    make_filter_tripdataset_missing_destination_coords,
) -> None:
    """Verifica degradación espacial por columnas lat/lon y H3 requeridas ausentes."""
    trips = make_filter_tripdataset_missing_destination_coords()
    issues: list[Issue] = []

    bbox = (-70.70, -33.50, -70.60, -33.40)
    h3_cells = [trips.data.loc[0, "origin_h3_index"]]

    masks, applied, omitted = _build_spatial_mask(
        trips,
        bbox=bbox,
        polygon=None,
        h3_cells=h3_cells,
        spatial_predicate="both",
        origin_h3_field="origin_h3_missing",
        destination_h3_field="destination_h3_missing",
        sample_rows_per_issue=3,
        issues=issues,
    )

    assert masks == {"bbox": None, "polygon": None, "h3_cells": None}
    assert applied == []
    assert omitted == ["bbox", "h3_cells"]

    assert sorted(_issue_codes(issues)) == sorted(
        [
            "FLT.SPATIAL.MISSING_REQUIRED_COLUMNS",
            "FLT.SPATIAL.H3_FIELD_MISSING",
        ]
    )