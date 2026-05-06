from __future__ import annotations

from copy import deepcopy
from typing import Any

import pandas as pd
import pytest

from pylondrina.datasets import TripDataset
from pylondrina.reports import Issue
from pylondrina.schema import DomainSpec, FieldSpec, TripSchema, TripSchemaEffective
from pylondrina.transforms.filtering import (
    _build_issues_summary,
    _combine_filter_masks,
    _materialize_filtered_tripdataset,
    _truncate_issues_with_limit,
    build_filter_summary,
)


def make_filter_field(
    name: str,
    dtype: str,
    *,
    required: bool = False,
    domain: DomainSpec | None = None,
) -> FieldSpec:
    """Construye un FieldSpec mínimo para fixtures de cierre de pipeline de OP-05."""
    return FieldSpec(
        name=name,
        dtype=dtype,
        required=required,
        domain=domain,
    )


@pytest.fixture()
def base_filter_schema() -> TripSchema:
    """Schema mínimo para probar helpers de pipeline y summary de OP-05."""
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
                "origin_h3_index": "h3_origin_m0",
                "destination_h3_index": "h3_destination_m0",
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
                "origin_h3_index": "h3_origin_m1",
                "destination_h3_index": "h3_destination_m1",
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
                "origin_h3_index": "h3_origin_m2",
                "destination_h3_index": "h3_destination_m2",
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
                "origin_h3_index": "h3_origin_m3",
                "destination_h3_index": "h3_destination_m3",
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
                "origin_h3_index": "h3_origin_m4",
                "destination_h3_index": "h3_destination_m4",
            },
        ]
    )

    df["origin_time_utc"] = pd.to_datetime(df["origin_time_utc"], utc=True)
    df["destination_time_utc"] = pd.to_datetime(df["destination_time_utc"], utc=True)
    return df


@pytest.fixture()
def make_filter_tripdataset(base_filter_schema: TripSchema):
    """Factory mínima de TripDataset para helpers de pipeline y materialización."""

    def _make(*, validated: bool = True) -> TripDataset:
        df = make_filter_test_dataframe()
        metadata = {
            "dataset_id": "ds_filter_small",
            "is_validated": validated,
            "temporal": {
                "tier": "tier_1",
                "fields_present": ["origin_time_utc", "destination_time_utc"],
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
            temporal={"tier": "tier_1"},
            fields_effective=list(df.columns),
        )

        return TripDataset(
            data=df,
            schema=base_filter_schema,
            schema_version=base_filter_schema.version,
            provenance={"source": {"name": "synthetic_filter_pipeline_tests"}},
            field_correspondence={},
            value_correspondence={},
            metadata=metadata,
            schema_effective=schema_effective,
        )

    return _make


def _issue_codes(issues: list[Issue]) -> list[str]:
    """Retorna los códigos de issues en el orden emitido."""
    return [issue.code for issue in issues]


def _kept_ids(mask: pd.Series, trips: TripDataset) -> list[str]:
    """Retorna movement_id retenidos por una máscara booleana."""
    return trips.data.loc[mask, "movement_id"].tolist()


def test_truncate_issues_with_limit_adds_truncation_issue_and_builds_summary() -> None:
    """Verifica truncamiento contractual de issues y resumen compacto por severidad/código."""
    issues_all = [
        Issue(level="info", code="FLT.INFO.WHERE_APPLIED", message="where aplicado"),
        Issue(level="warning", code="FLT.OUTPUT.EMPTY_RESULT", message="resultado vacío"),
        Issue(level="error", code="FLT.TIME.UNSUPPORTED_TIER", message="tier no soportado"),
        Issue(level="error", code="FLT.TIME.UNSUPPORTED_TIER", message="tier no soportado"),
    ]
    max_issues = 3

    retained, limits = _truncate_issues_with_limit(issues_all, max_issues=max_issues)

    assert limits is not None
    assert len(retained) <= max_issues
    assert retained[-1].code == "FLT.LIMIT.ISSUES_TRUNCATED"
    assert limits["max_issues"] == max_issues
    assert limits["issues_truncated"] is True
    assert limits["n_issues_emitted"] == len(retained)
    assert limits["n_issues_detected_total"] == len(issues_all)

    issues_summary = _build_issues_summary(retained)

    assert set(issues_summary.keys()) == {"counts", "top_codes"}
    assert set(issues_summary["counts"].keys()) == {"info", "warning", "error"}
    assert sum(issues_summary["counts"].values()) == len(retained)
    assert any(
        item["code"] == "FLT.LIMIT.ISSUES_TRUNCATED"
        for item in issues_summary["top_codes"]
    )


def test_combine_filter_masks_without_filters_keeps_all_rows_and_emits_no_filters_issue(
    make_filter_tripdataset,
) -> None:
    """Verifica que ausencia total de filtros preserve filas y emita evidencia informativa."""
    trips = make_filter_tripdataset()
    issues: list[Issue] = []

    mask_survival, dropped_by_filter, rows_in, rows_out, dropped_total = _combine_filter_masks(
        trips,
        mask_items=[],
        filters_requested=[],
        filters_applied=[],
        filters_omitted=[],
        issues=issues,
    )

    expected_mask = pd.Series(True, index=trips.data.index, dtype=bool)

    assert rows_in == len(trips.data)
    assert rows_out == len(trips.data)
    assert dropped_total == 0
    pd.testing.assert_series_equal(mask_survival, expected_mask)

    assert set(dropped_by_filter.keys()) == {"where", "time", "bbox", "polygon", "h3_cells"}
    assert all(count == 0 for count in dropped_by_filter.values())
    assert _issue_codes(issues) == ["FLT.INFO.NO_FILTERS_DEFINED"]


def test_combine_filter_masks_counts_incremental_drops_and_warns_on_empty_result(
    make_filter_tripdataset,
) -> None:
    """Verifica conteo incremental de drops y warning cuando el resultado queda vacío."""
    trips = make_filter_tripdataset()
    issues: list[Issue] = []

    where_mask = trips.data["mode"].isin(["bus", "metro", "car"])
    time_mask = trips.data["purpose"].eq("work")
    h3_mask = pd.Series(False, index=trips.data.index, dtype=bool)

    mask_survival, dropped_by_filter, rows_in, rows_out, dropped_total = _combine_filter_masks(
        trips,
        mask_items=[
            ("where", where_mask),
            ("time", time_mask),
            ("h3_cells", h3_mask),
        ],
        filters_requested=["where", "time", "h3_cells"],
        filters_applied=["where", "time", "h3_cells"],
        filters_omitted=[],
        issues=issues,
    )

    assert rows_in == len(trips.data)
    assert rows_out == 0
    assert dropped_total == len(trips.data)
    assert not mask_survival.any()

    expected_after_where = trips.data.loc[where_mask]
    expected_time_drops = int((~time_mask.loc[expected_after_where.index]).sum())
    expected_after_time = expected_after_where.loc[time_mask.loc[expected_after_where.index]]

    assert dropped_by_filter["where"] == int((~where_mask).sum())
    assert dropped_by_filter["time"] == expected_time_drops
    assert dropped_by_filter["h3_cells"] == len(expected_after_time)
    assert dropped_by_filter["bbox"] == 0
    assert dropped_by_filter["polygon"] == 0

    assert _issue_codes(issues) == ["FLT.OUTPUT.EMPTY_RESULT"]


def test_materialize_filtered_tripdataset_with_metadata_preserves_contract_and_does_not_mutate_input(
    make_filter_tripdataset,
) -> None:
    """Verifica materialización con metadata completa, nuevo dataset y no mutación del input."""
    trips = make_filter_tripdataset(validated=True)
    data_before = trips.data.copy(deep=True)
    metadata_before = deepcopy(trips.metadata)

    mask_survival = trips.data["mode"].isin(["bus", "metro"])

    filtered = _materialize_filtered_tripdataset(
        trips,
        mask_survival=mask_survival,
        keep_metadata=True,
    )

    expected = data_before.loc[mask_survival].copy(deep=True)

    assert filtered is not trips
    pd.testing.assert_frame_equal(filtered.data, expected)
    pd.testing.assert_frame_equal(trips.data, data_before)
    assert trips.metadata == metadata_before

    assert filtered.metadata["dataset_id"] == metadata_before["dataset_id"]
    assert filtered.metadata["is_validated"] is metadata_before["is_validated"]
    assert filtered.metadata["events"] == metadata_before["events"]
    assert filtered.metadata["events"] is not trips.metadata["events"]

    assert filtered.schema is trips.schema
    assert filtered.schema_version == trips.schema_version
    assert filtered.provenance == trips.provenance
    assert filtered.field_correspondence == trips.field_correspondence
    assert filtered.value_correspondence == trips.value_correspondence
    assert filtered.schema_effective == trips.schema_effective
    assert filtered.schema_effective is not trips.schema_effective


def test_materialize_filtered_tripdataset_without_metadata_keeps_minimal_metadata_only(
    make_filter_tripdataset,
) -> None:
    """Verifica metadata mínima cuando keep_metadata=False y preservación de is_validated."""
    trips = make_filter_tripdataset(validated=True)
    trips.metadata["artifact_id"] = "artifact_should_not_survive_here"

    data_before = trips.data.copy(deep=True)
    metadata_before = deepcopy(trips.metadata)
    mask_survival = trips.data["mode"].isin(["bus", "metro"])

    filtered = _materialize_filtered_tripdataset(
        trips,
        mask_survival=mask_survival,
        keep_metadata=False,
    )

    expected = data_before.loc[mask_survival].copy(deep=True)
    pd.testing.assert_frame_equal(filtered.data, expected)
    pd.testing.assert_frame_equal(trips.data, data_before)
    assert trips.metadata == metadata_before

    expected_keys = {"dataset_id", "is_validated", "temporal", "h3", "schema", "domains_effective"}
    assert set(filtered.metadata.keys()) == expected_keys
    assert "events" not in filtered.metadata
    assert "artifact_id" not in filtered.metadata
    assert filtered.metadata["dataset_id"] == metadata_before["dataset_id"]
    assert filtered.metadata["is_validated"] is metadata_before["is_validated"]
    assert filtered.metadata["temporal"] == metadata_before["temporal"]
    assert filtered.metadata["h3"] == metadata_before["h3"]
    assert filtered.metadata["domains_effective"] == metadata_before["domains_effective"]


def test_build_filter_summary_uses_stable_shape_zero_fills_and_optional_limits() -> None:
    """Verifica shape estable del summary, dropped_by_filter completo y bloque limits opcional."""
    rows_in = 5
    rows_out = 2
    dropped_by_filter = {"where": 2, "time": 1}
    filters_requested = ["where", "time", "bbox"]
    filters_applied = ["where", "time"]
    filters_omitted = ["bbox"]
    limits = {
        "max_issues": 3,
        "issues_truncated": True,
        "n_issues_emitted": 3,
        "n_issues_detected_total": 7,
    }

    summary = build_filter_summary(
        rows_in=rows_in,
        rows_out=rows_out,
        dropped_by_filter=dropped_by_filter,
        filters_requested=filters_requested,
        filters_applied=filters_applied,
        filters_omitted=filters_omitted,
        limits=limits,
    )

    assert set(summary.keys()) == {
        "rows_in",
        "rows_out",
        "dropped_total",
        "dropped_by_filter",
        "filters_requested",
        "filters_applied",
        "filters_omitted",
        "limits",
    }

    assert summary["rows_in"] == rows_in
    assert summary["rows_out"] == rows_out
    assert summary["dropped_total"] == rows_in - rows_out
    assert summary["filters_requested"] == filters_requested
    assert summary["filters_applied"] == filters_applied
    assert summary["filters_omitted"] == filters_omitted

    assert set(summary["dropped_by_filter"].keys()) == {
        "where",
        "time",
        "bbox",
        "polygon",
        "h3_cells",
    }
    assert summary["dropped_by_filter"]["where"] == dropped_by_filter["where"]
    assert summary["dropped_by_filter"]["time"] == dropped_by_filter["time"]

    omitted_filter_keys = set(summary["dropped_by_filter"]) - set(dropped_by_filter)
    assert all(summary["dropped_by_filter"][key] == 0 for key in omitted_filter_keys)

    assert summary["limits"] == limits