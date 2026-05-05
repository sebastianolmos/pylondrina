from __future__ import annotations

from copy import deepcopy
from typing import Any

import h3
import pandas as pd
import pytest

from pylondrina.datasets import TripDataset
from pylondrina.reports import OperationReport
from pylondrina.schema import DomainSpec, FieldSpec, TripSchema, TripSchemaEffective
from pylondrina.transforms.cleaning import CleanOptions, clean_trips


def make_valid_h3(lat: float = -33.45, lon: float = -70.66, res: int = 8) -> str:
    """Construye una celda H3 válida para smoke tests de OP-04."""
    return h3.latlng_to_cell(lat, lon, res)


def make_clean_field(
    name: str,
    dtype: str,
    *,
    required: bool = False,
    constraints: dict | None = None,
    domain: DomainSpec | None = None,
) -> FieldSpec:
    """Construye un FieldSpec mínimo equivalente al usado en el notebook helper-level."""
    return FieldSpec(
        name=name,
        dtype=dtype,
        required=required,
        constraints=constraints,
        domain=domain,
    )


@pytest.fixture()
def base_clean_schema() -> TripSchema:
    """Schema mínimo compartido por los smoke tests públicos de OP-04."""
    fields = {
        "movement_id": make_clean_field("movement_id", "string", required=True),
        "user_id": make_clean_field("user_id", "string", required=True),
        "origin_latitude": make_clean_field("origin_latitude", "float"),
        "origin_longitude": make_clean_field("origin_longitude", "float"),
        "destination_latitude": make_clean_field("destination_latitude", "float"),
        "destination_longitude": make_clean_field("destination_longitude", "float"),
        "origin_h3_index": make_clean_field("origin_h3_index", "string"),
        "destination_h3_index": make_clean_field("destination_h3_index", "string"),
        "origin_time_utc": make_clean_field("origin_time_utc", "datetime"),
        "destination_time_utc": make_clean_field("destination_time_utc", "datetime"),
        "origin_time_local_hhmm": make_clean_field("origin_time_local_hhmm", "string"),
        "destination_time_local_hhmm": make_clean_field("destination_time_local_hhmm", "string"),
        "mode": make_clean_field(
            "mode",
            "categorical",
            domain=DomainSpec(values=["walk", "bus", "metro", "unknown"], extendable=False),
        ),
        "purpose": make_clean_field(
            "purpose",
            "categorical",
            domain=DomainSpec(values=["work", "study", "unknown"], extendable=False),
        ),
    }
    return TripSchema(
        version="1.1",
        fields=fields,
        required=[name for name, field in fields.items() if field.required],
        semantic_rules=None,
    )


@pytest.fixture()
def base_clean_schema_effective(base_clean_schema: TripSchema) -> TripSchemaEffective:
    """Schema efectivo mínimo con dominios y temporalidad Tier 1."""
    return TripSchemaEffective(
        domains_effective={
            "mode": ["walk", "bus", "metro", "unknown"],
            "purpose": ["work", "study", "unknown"],
        },
        temporal={"tier": "tier_1"},
        fields_effective=list(base_clean_schema.fields.keys()),
    )


@pytest.fixture()
def make_tripdataset_for_clean(
    base_clean_schema: TripSchema,
    base_clean_schema_effective: TripSchemaEffective,
):
    """Factory mínima de TripDataset para smoke tests públicos de clean_trips."""

    def _make(
        df: pd.DataFrame,
        *,
        schema: TripSchema | None = None,
        schema_effective: TripSchemaEffective | None = None,
        is_validated: bool = True,
        temporal_tier: str = "tier_1",
        metadata: dict[str, Any] | None = None,
    ) -> TripDataset:
        schema_eff = schema or base_clean_schema
        schema_effective_eff = schema_effective or base_clean_schema_effective

        categorical_fields = [
            name
            for name, field in schema_eff.fields.items()
            if getattr(field, "domain", None) is not None
        ]
        domains_effective = {
            field_name: list(schema_eff.fields[field_name].domain.values)
            for field_name in categorical_fields
            if field_name in df.columns
        }

        base_metadata: dict[str, Any] = {
            "dataset_id": "ds_clean_smoke",
            "events": [],
            "is_validated": is_validated,
            "domains_effective": domains_effective,
            "temporal": {
                "tier": temporal_tier,
                "fields_present": list(df.columns),
            },
        }
        if metadata is not None:
            base_metadata.update(metadata)

        return TripDataset(
            data=df.copy(deep=True),
            schema=schema_eff,
            schema_version=schema_eff.version,
            provenance={"source": {"name": "synthetic_clean_smoke"}},
            field_correspondence={},
            value_correspondence={},
            metadata=base_metadata,
            schema_effective=schema_effective_eff,
        )

    return _make


def _issue_codes(report: OperationReport) -> list[str]:
    """Retorna los códigos de issues de un OperationReport."""
    return [issue.code for issue in report.issues]


def _assert_clean_event_alignment(cleaned: TripDataset, report: OperationReport) -> dict[str, Any]:
    """Verifica alineación mínima entre reporte y último evento clean_trips."""
    assert cleaned.metadata["events"]

    event = cleaned.metadata["events"][-1]
    assert event["op"] == "clean_trips"
    assert event["summary"] == report.summary
    assert event["parameters"] == report.parameters
    assert "issues_summary" in event

    return event


def test_clean_trips_smoke_happy_path_drops_duplicates_and_categorical_values(
    make_tripdataset_for_clean,
) -> None:
    """Verifica ejecución pública mínima con drops efectivos por duplicados y categoría."""
    valid_h3_a = make_valid_h3(-33.45, -70.66, 8)
    valid_h3_b = make_valid_h3(-33.46, -70.67, 8)
    valid_h3_c = make_valid_h3(-33.47, -70.68, 8)
    valid_h3_d = make_valid_h3(-33.48, -70.69, 8)

    df_source = pd.DataFrame(
        {
            "movement_id": ["m1", "m2", "m3", "m4"],
            "user_id": ["u1", "u1", "u3", "u4"],
            "origin_latitude": [-33.45, -33.45, -33.47, -33.48],
            "origin_longitude": [-70.66, -70.66, -70.68, -70.69],
            "destination_latitude": [-33.46, -33.46, -33.48, -33.49],
            "destination_longitude": [-70.67, -70.67, -70.69, -70.70],
            "origin_h3_index": [valid_h3_a, valid_h3_a, valid_h3_c, valid_h3_d],
            "destination_h3_index": [valid_h3_b, valid_h3_b, valid_h3_d, valid_h3_a],
            "origin_time_utc": [
                "2026-04-01T08:00:00Z",
                "2026-04-01T08:00:00Z",
                "2026-04-01T09:00:00Z",
                "2026-04-01T10:00:00Z",
            ],
            "destination_time_utc": [
                "2026-04-01T08:20:00Z",
                "2026-04-01T08:20:00Z",
                "2026-04-01T09:25:00Z",
                "2026-04-01T10:30:00Z",
            ],
            "mode": ["walk", "walk", "unknown", "bus"],
            "purpose": ["work", "work", "study", "study"],
        }
    )
    duplicates_subset = [
        "user_id",
        "origin_time_utc",
        "origin_h3_index",
        "destination_h3_index",
    ]

    trips = make_tripdataset_for_clean(df_source, is_validated=True)
    data_before = trips.data.copy(deep=True)
    events_before = deepcopy(trips.metadata["events"])

    cleaned, report = clean_trips(
        trips,
        options=CleanOptions(
            drop_duplicates=True,
            duplicates_subset=duplicates_subset,
            drop_rows_by_categorical_values={"mode": ["unknown"]},
        ),
    )

    assert isinstance(cleaned, TripDataset)
    assert isinstance(report, OperationReport)
    assert report.ok is True
    pd.testing.assert_frame_equal(trips.data, data_before)
    assert trips.metadata["events"] == events_before

    expected_after_duplicates = data_before.loc[
        ~data_before.duplicated(subset=duplicates_subset, keep="first")
    ]
    expected = expected_after_duplicates.loc[expected_after_duplicates["mode"] != "unknown"]
    pd.testing.assert_frame_equal(cleaned.data, expected)

    assert report.summary["rows_in"] == len(data_before)
    assert report.summary["rows_out"] == len(expected)
    assert report.summary["dropped_total"] == len(data_before) - len(expected)
    assert report.summary["dropped_by_rule"]["duplicates"] == int(
        data_before.duplicated(subset=duplicates_subset, keep="first").sum()
    )
    assert report.summary["dropped_by_rule"]["categorical_values"] == int(
        (expected_after_duplicates["mode"] == "unknown").sum()
    )

    assert report.parameters["drop_duplicates"] is True
    assert report.parameters["duplicates_subset"] == duplicates_subset
    assert report.parameters["duplicates_subset_effective"] == duplicates_subset
    assert cleaned.metadata["is_validated"] is True
    _assert_clean_event_alignment(cleaned, report)


def test_clean_trips_smoke_rule_active_without_effect_preserves_validated_flag(
    make_tripdataset_for_clean,
) -> None:
    """Verifica regla activa sin drops efectivos, issue informativo y evento mínimo."""
    valid_h3_a = make_valid_h3(-33.45, -70.66, 8)
    valid_h3_b = make_valid_h3(-33.46, -70.67, 8)
    valid_h3_c = make_valid_h3(-33.47, -70.68, 8)
    valid_h3_d = make_valid_h3(-33.48, -70.69, 8)

    df_source = pd.DataFrame(
        {
            "movement_id": ["m1", "m2"],
            "user_id": ["u1", "u2"],
            "origin_h3_index": [valid_h3_a, valid_h3_c],
            "destination_h3_index": [valid_h3_b, valid_h3_d],
        }
    )
    trips = make_tripdataset_for_clean(df_source, is_validated=False)
    data_before = trips.data.copy(deep=True)

    cleaned, report = clean_trips(
        trips,
        options=CleanOptions(drop_rows_with_invalid_h3=True),
    )

    assert report.ok is True
    pd.testing.assert_frame_equal(trips.data, data_before)
    pd.testing.assert_frame_equal(cleaned.data, data_before)

    assert report.summary["rows_in"] == len(data_before)
    assert report.summary["rows_out"] == len(data_before)
    assert report.summary["dropped_total"] == 0
    assert report.summary["dropped_by_rule"]["invalid_h3"] == 0
    assert "CLN.NO_CHANGES.NO_ROWS_DROPPED" in _issue_codes(report)
    assert cleaned.metadata["is_validated"] is False
    _assert_clean_event_alignment(cleaned, report)


def test_clean_trips_smoke_temporal_rule_not_evaluable_in_tier2_returns_warning(
    make_tripdataset_for_clean,
) -> None:
    """Verifica degradación recuperable cuando la regla temporal se pide en Tier 2."""
    df_source = pd.DataFrame(
        {
            "movement_id": ["m1", "m2"],
            "user_id": ["u1", "u2"],
            "origin_time_local_hhmm": ["08:30", "09:00"],
            "destination_time_local_hhmm": ["08:10", "09:20"],
            "mode": ["walk", "bus"],
        }
    )
    trips = make_tripdataset_for_clean(
        df_source,
        is_validated=True,
        temporal_tier="tier_2",
    )
    data_before = trips.data.copy(deep=True)

    cleaned, report = clean_trips(
        trips,
        options=CleanOptions(drop_rows_with_origin_after_destination=True),
    )

    assert report.ok is True
    pd.testing.assert_frame_equal(trips.data, data_before)
    pd.testing.assert_frame_equal(cleaned.data, data_before)

    assert "CLN.RULE.TEMPORAL_RULE_NOT_EVALUABLE" in _issue_codes(report)
    assert "CLN.NO_CHANGES.NO_RULES_ACTIVE" in _issue_codes(report)
    assert report.summary["rows_in"] == len(data_before)
    assert report.summary["rows_out"] == len(data_before)
    assert report.summary["dropped_total"] == 0
    assert report.summary["dropped_by_rule"]["origin_after_destination"] == 0
    assert cleaned.metadata["is_validated"] is True
    _assert_clean_event_alignment(cleaned, report)


def test_clean_trips_smoke_duplicates_default_subset_is_recorded_in_report_and_event(
    make_tripdataset_for_clean,
) -> None:
    """Verifica default de duplicates_subset desde schema.required y su trazabilidad."""
    schema_dup_default = TripSchema(
        version="1.1",
        fields={
            "user_id": make_clean_field("user_id", "string", required=True),
            "origin_time_utc": make_clean_field("origin_time_utc", "datetime", required=True),
            "mode": make_clean_field(
                "mode",
                "categorical",
                domain=DomainSpec(values=["walk", "bus", "unknown"], extendable=False),
            ),
        },
        required=["user_id", "origin_time_utc"],
        semantic_rules=None,
    )
    df_source = pd.DataFrame(
        {
            "user_id": ["u1", "u1", "u2"],
            "origin_time_utc": [
                "2026-04-01T08:00:00Z",
                "2026-04-01T08:00:00Z",
                "2026-04-01T09:00:00Z",
            ],
            "mode": ["walk", "walk", "bus"],
        }
    )

    trips = make_tripdataset_for_clean(
        df_source,
        schema=schema_dup_default,
        is_validated=True,
    )
    data_before = trips.data.copy(deep=True)
    expected_subset = [
        field for field in schema_dup_default.required if field in data_before.columns
    ]
    expected = data_before.loc[
        ~data_before.duplicated(subset=expected_subset, keep="first")
    ]

    cleaned, report = clean_trips(
        trips,
        options=CleanOptions(drop_duplicates=True, duplicates_subset=None),
    )

    assert report.ok is True
    pd.testing.assert_frame_equal(trips.data, data_before)
    pd.testing.assert_frame_equal(cleaned.data, expected)

    assert report.parameters["duplicates_subset"] is None
    assert report.parameters["duplicates_subset_effective"] == expected_subset
    assert report.summary["rows_in"] == len(data_before)
    assert report.summary["rows_out"] == len(expected)
    assert report.summary["dropped_total"] == len(data_before) - len(expected)
    assert report.summary["dropped_by_rule"]["duplicates"] == int(
        data_before.duplicated(subset=expected_subset, keep="first").sum()
    )

    event = _assert_clean_event_alignment(cleaned, report)
    assert event["parameters"]["duplicates_subset"] is None
    assert event["parameters"]["duplicates_subset_effective"] == expected_subset


def test_clean_trips_smoke_invalid_duplicates_subset_aborts_without_side_effects(
    make_tripdataset_for_clean,
) -> None:
    """Verifica error fatal por subset imposible, sin evento ni mutación del input."""
    df_source = pd.DataFrame(
        {
            "movement_id": ["m1", "m2"],
            "user_id": ["u1", "u2"],
            "mode": ["walk", "bus"],
        }
    )
    trips = make_tripdataset_for_clean(df_source, is_validated=True)
    data_before = trips.data.copy(deep=True)
    events_before = deepcopy(trips.metadata["events"])
    validated_before = trips.metadata["is_validated"]

    with pytest.raises(ValueError) as exc_info:
        clean_trips(
            trips,
            options=CleanOptions(
                drop_duplicates=True,
                duplicates_subset=["missing_field"],
            ),
        )

    assert getattr(exc_info.value, "code", None) == "CLN.CONFIG.INVALID_DUPLICATES_SUBSET"
    pd.testing.assert_frame_equal(trips.data, data_before)
    assert trips.metadata["events"] == events_before
    assert trips.metadata["is_validated"] == validated_before