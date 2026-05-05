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


def make_valid_h3(lat: float, lon: float, res: int = 8) -> str:
    """Construye una celda H3 válida para fixtures de integración de OP-04."""
    return h3.latlng_to_cell(lat, lon, res)


def make_clean_field(
    name: str,
    dtype: str,
    *,
    required: bool = False,
    domain: DomainSpec | None = None,
    constraints: dict | None = None,
) -> FieldSpec:
    """Construye un FieldSpec mínimo equivalente al usado en el notebook de integración."""
    return FieldSpec(
        name=name,
        dtype=dtype,
        required=required,
        domain=domain,
        constraints=constraints,
    )


@pytest.fixture()
def base_clean_schema() -> TripSchema:
    """Schema base usado por las fixtures de integración de OP-04."""
    fields = {
        "movement_id": make_clean_field("movement_id", "string", required=True),
        "user_id": make_clean_field("user_id", "string", required=True),
        "origin_longitude": make_clean_field("origin_longitude", "float"),
        "origin_latitude": make_clean_field("origin_latitude", "float"),
        "destination_longitude": make_clean_field("destination_longitude", "float"),
        "destination_latitude": make_clean_field("destination_latitude", "float"),
        "origin_h3_index": make_clean_field("origin_h3_index", "string"),
        "destination_h3_index": make_clean_field("destination_h3_index", "string"),
        "origin_time_utc": make_clean_field("origin_time_utc", "datetime"),
        "destination_time_utc": make_clean_field("destination_time_utc", "datetime"),
        "trip_id": make_clean_field("trip_id", "string"),
        "movement_seq": make_clean_field("movement_seq", "int"),
        "mode": make_clean_field(
            "mode",
            "categorical",
            domain=DomainSpec(
                values=["walk", "bus", "metro", "car", "unknown"],
                extendable=False,
            ),
        ),
        "purpose": make_clean_field(
            "purpose",
            "categorical",
            domain=DomainSpec(
                values=["home", "work", "study", "other", "unknown"],
                extendable=False,
            ),
        ),
        "day_type": make_clean_field(
            "day_type",
            "categorical",
            domain=DomainSpec(
                values=["weekday", "weekend", "holiday"],
                extendable=False,
            ),
        ),
        "user_gender": make_clean_field(
            "user_gender",
            "categorical",
            domain=DomainSpec(
                values=["female", "male", "other", "unknown"],
                extendable=False,
            ),
        ),
        "user_age_group": make_clean_field(
            "user_age_group",
            "categorical",
            domain=DomainSpec(
                values=["15-24", "25-34", "35-44", "45-54", "unknown"],
                extendable=False,
            ),
        ),
        "origin_municipality": make_clean_field("origin_municipality", "string"),
        "destination_municipality": make_clean_field("destination_municipality", "string"),
        "trip_weight": make_clean_field("trip_weight", "float"),
        "origin_time_local_hhmm": make_clean_field("origin_time_local_hhmm", "string"),
        "destination_time_local_hhmm": make_clean_field("destination_time_local_hhmm", "string"),
    }

    return TripSchema(
        version="1.1",
        fields=fields,
        required=["movement_id", "user_id"],
        semantic_rules=None,
    )


@pytest.fixture()
def base_clean_schema_effective(base_clean_schema: TripSchema) -> TripSchemaEffective:
    """Schema efectivo base con dominios y temporalidad Tier 1."""
    return TripSchemaEffective(
        domains_effective={
            "mode": ["walk", "bus", "metro", "car", "unknown"],
            "purpose": ["home", "work", "study", "other", "unknown"],
            "day_type": ["weekday", "weekend", "holiday"],
            "user_gender": ["female", "male", "other", "unknown"],
            "user_age_group": ["15-24", "25-34", "35-44", "45-54", "unknown"],
        },
        temporal={
            "tier": "tier_1",
            "fields_present": ["origin_time_utc", "destination_time_utc"],
        },
        fields_effective=list(base_clean_schema.fields.keys()),
        dtype_effective={},
        overrides={},
    )


def _canonical_rows() -> list[dict[str, Any]]:
    """Construye filas canónicas pequeñas usadas por varias fixtures."""
    pts = [
        (-33.45, -70.66, -33.46, -70.67),
        (-33.46, -70.67, -33.47, -70.68),
        (-33.47, -70.68, -33.48, -70.69),
        (-33.48, -70.69, -33.49, -70.70),
        (-33.49, -70.70, -33.50, -70.71),
        (-33.50, -70.71, -33.51, -70.72),
    ]
    modes = ["walk", "bus", "metro", "car", "bus", "walk"]
    purposes = ["work", "study", "home", "work", "study", "home"]

    rows: list[dict[str, Any]] = []
    for i, (olat, olon, dlat, dlon) in enumerate(pts, start=1):
        hour = 7 + i
        rows.append(
            {
                "movement_id": f"m{i}",
                "user_id": f"u{i}",
                "origin_longitude": olon,
                "origin_latitude": olat,
                "destination_longitude": dlon,
                "destination_latitude": dlat,
                "origin_h3_index": make_valid_h3(olat, olon, 8),
                "destination_h3_index": make_valid_h3(dlat, dlon, 8),
                "origin_time_utc": f"2026-04-01T{hour:02d}:00:00Z",
                "destination_time_utc": f"2026-04-01T{hour:02d}:20:00Z",
                "trip_id": f"t{i}",
                "movement_seq": 0,
                "mode": modes[i - 1],
                "purpose": purposes[i - 1],
                "day_type": "weekday",
                "user_gender": "female" if i % 2 == 0 else "male",
                "user_age_group": "25-34" if i <= 3 else "35-44",
                "origin_municipality": "Santiago",
                "destination_municipality": "Providencia",
                "trip_weight": 1.0 + i / 10,
                "origin_time_local_hhmm": f"{hour:02d}:00",
                "destination_time_local_hhmm": f"{hour:02d}:20",
            }
        )
    return rows


@pytest.fixture()
def tripdataset_canonical_small(
    base_clean_schema: TripSchema,
    base_clean_schema_effective: TripSchemaEffective,
) -> TripDataset:
    """TripDataset canónico pequeño, no validado y sin suciedad intencional."""
    df = pd.DataFrame(_canonical_rows())

    return TripDataset(
        data=df.copy(),
        schema=base_clean_schema,
        schema_version=base_clean_schema.version,
        provenance={"source": {"name": "canonical_fixture"}},
        field_correspondence={},
        value_correspondence={},
        metadata={
            "dataset_id": "tripdataset_canonical_small",
            "events": [],
            "is_validated": False,
            "domains_effective": deepcopy(base_clean_schema_effective.domains_effective),
            "temporal": {
                "tier": "tier_1",
                "fields_present": ["origin_time_utc", "destination_time_utc"],
            },
        },
        schema_effective=deepcopy(base_clean_schema_effective),
    )


@pytest.fixture()
def tripdataset_validated_small(
    base_clean_schema: TripSchema,
    base_clean_schema_effective: TripSchemaEffective,
) -> TripDataset:
    """TripDataset canónico pequeño con validación previa registrada."""
    df = pd.DataFrame(_canonical_rows())

    return TripDataset(
        data=df.copy(),
        schema=base_clean_schema,
        schema_version=base_clean_schema.version,
        provenance={"source": {"name": "validated_fixture"}},
        field_correspondence={"user_id": "id_persona", "mode": "modo"},
        value_correspondence={"mode": {"BUS": "bus", "METRO": "metro"}},
        metadata={
            "dataset_id": "tripdataset_validated_small",
            "events": [
                {
                    "op": "validate_trips",
                    "ts_utc": "2026-04-01T12:00:00Z",
                    "parameters": {"strict": False},
                    "summary": {"ok": True, "n_rows": len(df)},
                    "issues_summary": {
                        "counts": {"info": 0, "warning": 0, "error": 0},
                        "top_codes": [],
                    },
                }
            ],
            "is_validated": True,
            "domains_effective": deepcopy(base_clean_schema_effective.domains_effective),
            "temporal": {
                "tier": "tier_1",
                "fields_present": ["origin_time_utc", "destination_time_utc"],
            },
        },
        schema_effective=deepcopy(base_clean_schema_effective),
    )


@pytest.fixture()
def tripdataset_dirty_small(
    base_clean_schema: TripSchema,
    base_clean_schema_effective: TripSchemaEffective,
) -> TripDataset:
    """TripDataset sintético con una fila problemática por regla principal de limpieza."""
    rows = [
        {
            "movement_id": "m1",
            "user_id": "u1",
            "origin_longitude": -70.66,
            "origin_latitude": -33.45,
            "destination_longitude": -70.67,
            "destination_latitude": -33.46,
            "origin_h3_index": make_valid_h3(-33.45, -70.66, 8),
            "destination_h3_index": make_valid_h3(-33.46, -70.67, 8),
            "origin_time_utc": "2026-04-01T08:00:00Z",
            "destination_time_utc": "2026-04-01T08:20:00Z",
            "trip_id": "t1",
            "movement_seq": 0,
            "mode": "walk",
            "purpose": "work",
            "day_type": "weekday",
            "user_gender": "male",
            "user_age_group": "25-34",
            "origin_municipality": "Santiago",
            "destination_municipality": "Providencia",
            "trip_weight": 1.0,
            "origin_time_local_hhmm": "08:00",
            "destination_time_local_hhmm": "08:20",
        },
        {
            "movement_id": "m2",
            "user_id": "u1",
            "origin_longitude": -70.66,
            "origin_latitude": -33.45,
            "destination_longitude": -70.67,
            "destination_latitude": -33.46,
            "origin_h3_index": make_valid_h3(-33.45, -70.66, 8),
            "destination_h3_index": make_valid_h3(-33.46, -70.67, 8),
            "origin_time_utc": "2026-04-01T08:00:00Z",
            "destination_time_utc": "2026-04-01T08:20:00Z",
            "trip_id": "t2",
            "movement_seq": 0,
            "mode": "walk",
            "purpose": "work",
            "day_type": "weekday",
            "user_gender": "female",
            "user_age_group": "25-34",
            "origin_municipality": "Santiago",
            "destination_municipality": "Providencia",
            "trip_weight": 1.2,
            "origin_time_local_hhmm": "08:00",
            "destination_time_local_hhmm": "08:20",
        },
        {
            "movement_id": "m3",
            "user_id": "u2",
            "origin_longitude": -70.68,
            "origin_latitude": -33.47,
            "destination_longitude": -70.69,
            "destination_latitude": -33.48,
            "origin_h3_index": make_valid_h3(-33.47, -70.68, 8),
            "destination_h3_index": make_valid_h3(-33.48, -70.69, 8),
            "origin_time_utc": "2026-04-01T09:00:00Z",
            "destination_time_utc": "2026-04-01T09:25:00Z",
            "trip_id": "t3",
            "movement_seq": 0,
            "mode": "unknown",
            "purpose": "study",
            "day_type": "weekday",
            "user_gender": "female",
            "user_age_group": "15-24",
            "origin_municipality": "Ñuñoa",
            "destination_municipality": "Providencia",
            "trip_weight": 0.9,
            "origin_time_local_hhmm": "09:00",
            "destination_time_local_hhmm": "09:25",
        },
        {
            "movement_id": "m4",
            "user_id": "u3",
            "origin_longitude": -70.70,
            "origin_latitude": 95.0,
            "destination_longitude": -70.71,
            "destination_latitude": -33.50,
            "origin_h3_index": make_valid_h3(-33.49, -70.70, 8),
            "destination_h3_index": make_valid_h3(-33.50, -70.71, 8),
            "origin_time_utc": "2026-04-01T10:00:00Z",
            "destination_time_utc": "2026-04-01T10:20:00Z",
            "trip_id": "t4",
            "movement_seq": 0,
            "mode": "bus",
            "purpose": "work",
            "day_type": "weekday",
            "user_gender": "male",
            "user_age_group": "35-44",
            "origin_municipality": "Las Condes",
            "destination_municipality": "Providencia",
            "trip_weight": 1.1,
            "origin_time_local_hhmm": "10:00",
            "destination_time_local_hhmm": "10:20",
        },
        {
            "movement_id": "m5",
            "user_id": "u4",
            "origin_longitude": -70.72,
            "origin_latitude": -33.51,
            "destination_longitude": -70.73,
            "destination_latitude": -33.52,
            "origin_h3_index": "not_a_real_h3",
            "destination_h3_index": make_valid_h3(-33.52, -70.73, 8),
            "origin_time_utc": "2026-04-01T11:00:00Z",
            "destination_time_utc": "2026-04-01T11:20:00Z",
            "trip_id": "t5",
            "movement_seq": 0,
            "mode": "metro",
            "purpose": "study",
            "day_type": "weekday",
            "user_gender": "female",
            "user_age_group": "25-34",
            "origin_municipality": "Santiago",
            "destination_municipality": "Providencia",
            "trip_weight": 1.3,
            "origin_time_local_hhmm": "11:00",
            "destination_time_local_hhmm": "11:20",
        },
        {
            "movement_id": "m6",
            "user_id": "u5",
            "origin_longitude": -70.74,
            "origin_latitude": -33.53,
            "destination_longitude": -70.75,
            "destination_latitude": -33.54,
            "origin_h3_index": make_valid_h3(-33.53, -70.74, 8),
            "destination_h3_index": make_valid_h3(-33.54, -70.75, 8),
            "origin_time_utc": "2026-04-01T12:30:00Z",
            "destination_time_utc": "2026-04-01T12:00:00Z",
            "trip_id": "t6",
            "movement_seq": 0,
            "mode": "car",
            "purpose": "work",
            "day_type": "weekday",
            "user_gender": "male",
            "user_age_group": "45-54",
            "origin_municipality": "Maipú",
            "destination_municipality": "Santiago",
            "trip_weight": 1.0,
            "origin_time_local_hhmm": "12:30",
            "destination_time_local_hhmm": "12:00",
        },
        {
            "movement_id": "m7",
            "user_id": None,
            "origin_longitude": -70.76,
            "origin_latitude": -33.55,
            "destination_longitude": -70.77,
            "destination_latitude": -33.56,
            "origin_h3_index": make_valid_h3(-33.55, -70.76, 8),
            "destination_h3_index": make_valid_h3(-33.56, -70.77, 8),
            "origin_time_utc": "2026-04-01T13:00:00Z",
            "destination_time_utc": "2026-04-01T13:20:00Z",
            "trip_id": "t7",
            "movement_seq": 0,
            "mode": "walk",
            "purpose": "home",
            "day_type": "weekday",
            "user_gender": "female",
            "user_age_group": "35-44",
            "origin_municipality": "Santiago",
            "destination_municipality": "Recoleta",
            "trip_weight": 0.8,
            "origin_time_local_hhmm": "13:00",
            "destination_time_local_hhmm": "13:20",
        },
        {
            "movement_id": "m8",
            "user_id": "u7",
            "origin_longitude": -70.78,
            "origin_latitude": -33.57,
            "destination_longitude": -70.79,
            "destination_latitude": -33.58,
            "origin_h3_index": make_valid_h3(-33.57, -70.78, 8),
            "destination_h3_index": make_valid_h3(-33.58, -70.79, 8),
            "origin_time_utc": "2026-04-01T14:00:00Z",
            "destination_time_utc": "2026-04-01T14:25:00Z",
            "trip_id": "t8",
            "movement_seq": 0,
            "mode": "bus",
            "purpose": None,
            "day_type": "weekday",
            "user_gender": "male",
            "user_age_group": "25-34",
            "origin_municipality": "Puente Alto",
            "destination_municipality": "Santiago",
            "trip_weight": 1.4,
            "origin_time_local_hhmm": "14:00",
            "destination_time_local_hhmm": "14:25",
        },
        {
            "movement_id": "m9",
            "user_id": "u8",
            "origin_longitude": -70.80,
            "origin_latitude": -33.59,
            "destination_longitude": -70.81,
            "destination_latitude": -33.60,
            "origin_h3_index": make_valid_h3(-33.59, -70.80, 8),
            "destination_h3_index": make_valid_h3(-33.60, -70.81, 8),
            "origin_time_utc": "2026-04-01T15:00:00Z",
            "destination_time_utc": "2026-04-01T15:20:00Z",
            "trip_id": "t9",
            "movement_seq": 0,
            "mode": "metro",
            "purpose": "study",
            "day_type": "weekday",
            "user_gender": "female",
            "user_age_group": "15-24",
            "origin_municipality": "Santiago",
            "destination_municipality": "Providencia",
            "trip_weight": 1.0,
            "origin_time_local_hhmm": "15:00",
            "destination_time_local_hhmm": "15:20",
        },
        {
            "movement_id": "m10",
            "user_id": "u9",
            "origin_longitude": None,
            "origin_latitude": -33.61,
            "destination_longitude": -70.83,
            "destination_latitude": -33.62,
            "origin_h3_index": make_valid_h3(-33.61, -70.82, 8),
            "destination_h3_index": make_valid_h3(-33.62, -70.83, 8),
            "origin_time_utc": "2026-04-01T16:00:00Z",
            "destination_time_utc": "2026-04-01T16:20:00Z",
            "trip_id": "t10",
            "movement_seq": 0,
            "mode": "metro",
            "purpose": "work",
            "day_type": "weekday",
            "user_gender": "male",
            "user_age_group": "35-44",
            "origin_municipality": "Santiago",
            "destination_municipality": "Providencia",
            "trip_weight": 1.1,
            "origin_time_local_hhmm": "16:00",
            "destination_time_local_hhmm": "16:20",
        },
    ]
    df = pd.DataFrame(rows)

    return TripDataset(
        data=df.copy(),
        schema=base_clean_schema,
        schema_version=base_clean_schema.version,
        provenance={"source": {"name": "dirty_fixture"}},
        field_correspondence={"user_id": "id_persona", "mode": "modo"},
        value_correspondence={"mode": {"BUS": "bus", "METRO": "metro"}},
        metadata={
            "dataset_id": "tripdataset_dirty_small",
            "events": [
                {
                    "op": "validate_trips",
                    "ts_utc": "2026-04-01T12:00:00Z",
                    "parameters": {"strict": False},
                    "summary": {"ok": True, "n_rows": len(df)},
                    "issues_summary": {
                        "counts": {"info": 0, "warning": 0, "error": 0},
                        "top_codes": [],
                    },
                }
            ],
            "is_validated": True,
            "domains_effective": deepcopy(base_clean_schema_effective.domains_effective),
            "temporal": {
                "tier": "tier_1",
                "fields_present": ["origin_time_utc", "destination_time_utc"],
            },
        },
        schema_effective=deepcopy(base_clean_schema_effective),
    )


def _issue_codes(report: OperationReport) -> list[str]:
    """Retorna los códigos de issues de un OperationReport."""
    return [issue.code for issue in report.issues]


def _assert_clean_event_alignment(
    cleaned: TripDataset,
    report: OperationReport,
) -> dict[str, Any]:
    """Verifica la alineación mínima entre reporte y último evento clean_trips."""
    assert cleaned.metadata["events"]

    event = cleaned.metadata["events"][-1]
    assert event["op"] == "clean_trips"
    assert event["summary"] == report.summary
    assert event["parameters"] == report.parameters
    assert set(event["issues_summary"].keys()) == {"counts", "top_codes"}

    return event


def _assert_base_contract_preserved(
    cleaned: TripDataset,
    original: TripDataset,
    *,
    metadata_before: dict[str, Any] | None = None,
) -> None:
    """Verifica preservaciones contractuales transversales de OP-04."""
    base_metadata = metadata_before or original.metadata

    assert cleaned.schema is original.schema
    assert cleaned.schema_version == original.schema_version
    assert cleaned.provenance == original.provenance
    assert cleaned.field_correspondence == original.field_correspondence
    assert cleaned.value_correspondence == original.value_correspondence
    assert cleaned.metadata["dataset_id"] == base_metadata["dataset_id"]
    assert cleaned.metadata["domains_effective"] == base_metadata["domains_effective"]
    assert cleaned.schema_effective == original.schema_effective


def test_clean_trips_integration_applies_multiple_rules_incrementally(
    tripdataset_dirty_small: TripDataset,
) -> None:
    """Verifica caso principal con múltiples reglas, drops incrementales, evento y no mutación."""
    trips = tripdataset_dirty_small
    data_before = trips.data.copy(deep=True)
    metadata_before = deepcopy(trips.metadata)
    events_before = deepcopy(trips.metadata["events"])

    duplicates_subset = [
        "user_id",
        "origin_time_utc",
        "origin_h3_index",
        "destination_h3_index",
    ]

    cleaned, report = clean_trips(
        trips,
        options=CleanOptions(
            drop_rows_with_nulls_in_required_fields=True,
            drop_rows_with_nulls_in_fields=["purpose"],
            drop_rows_with_invalid_latlon=True,
            drop_rows_with_invalid_h3=True,
            drop_rows_with_origin_after_destination=True,
            drop_duplicates=True,
            duplicates_subset=duplicates_subset,
            drop_rows_by_categorical_values={"mode": ["unknown"]},
        ),
    )

    expected = data_before.loc[data_before["movement_id"].isin(["m1", "m9"])]

    assert isinstance(cleaned, TripDataset)
    assert isinstance(report, OperationReport)
    assert report.ok is True

    pd.testing.assert_frame_equal(trips.data, data_before)
    assert trips.metadata == metadata_before
    pd.testing.assert_frame_equal(cleaned.data, expected)

    assert report.summary["rows_in"] == len(data_before)
    assert report.summary["rows_out"] == len(expected)
    assert report.summary["dropped_total"] == len(data_before) - len(expected)
    assert report.summary["dropped_by_rule"] == {
        "nulls_required": 1,
        "nulls_fields": 1,
        "invalid_latlon": 2,
        "invalid_h3": 1,
        "origin_after_destination": 1,
        "duplicates": 1,
        "categorical_values": 1,
    }

    assert report.parameters["duplicates_subset"] == duplicates_subset
    assert report.parameters["duplicates_subset_effective"] == duplicates_subset
    assert cleaned.metadata["is_validated"] is True
    assert cleaned.metadata["events"][:-1] == events_before

    _assert_base_contract_preserved(cleaned, trips, metadata_before=metadata_before)
    _assert_clean_event_alignment(cleaned, report)


def test_clean_trips_integration_invalid_explicit_duplicates_subset_aborts_without_side_effects(
    tripdataset_validated_small: TripDataset,
) -> None:
    """Verifica abort fatal por duplicates_subset inexistente, sin mutar input ni eventos."""
    trips = tripdataset_validated_small
    data_before = trips.data.copy(deep=True)
    metadata_before = deepcopy(trips.metadata)

    with pytest.raises(ValueError) as exc_info:
        clean_trips(
            trips,
            options=CleanOptions(
                drop_duplicates=True,
                duplicates_subset=["campo_inexistente"],
            ),
        )

    assert getattr(exc_info.value, "code", None) == "CLN.CONFIG.INVALID_DUPLICATES_SUBSET"
    pd.testing.assert_frame_equal(trips.data, data_before)
    assert trips.metadata == metadata_before


def test_clean_trips_integration_temporal_rule_not_evaluable_in_tier2_warns(
    tripdataset_canonical_small: TripDataset,
) -> None:
    """Verifica degradación recuperable cuando la regla temporal se solicita en Tier 2."""
    trips = tripdataset_canonical_small
    data_before = trips.data.copy(deep=True)

    trips.metadata["temporal"] = {
        "tier": "tier_2",
        "fields_present": ["origin_time_local_hhmm", "destination_time_local_hhmm"],
    }
    metadata_before = deepcopy(trips.metadata)

    cleaned, report = clean_trips(
        trips,
        options=CleanOptions(drop_rows_with_origin_after_destination=True),
    )

    codes = _issue_codes(report)

    assert report.ok is True
    assert "CLN.RULE.TEMPORAL_RULE_NOT_EVALUABLE" in codes
    assert "CLN.NO_CHANGES.NO_RULES_ACTIVE" in codes

    pd.testing.assert_frame_equal(cleaned.data, data_before)
    assert report.summary["rows_in"] == len(data_before)
    assert report.summary["rows_out"] == len(data_before)
    assert report.summary["dropped_total"] == 0
    assert report.summary["dropped_by_rule"]["origin_after_destination"] == 0
    assert cleaned.metadata["is_validated"] is False

    _assert_base_contract_preserved(cleaned, trips, metadata_before=metadata_before)
    _assert_clean_event_alignment(cleaned, report)


def test_clean_trips_integration_preserves_contract_and_appends_event(
    tripdataset_validated_small: TripDataset,
) -> None:
    """Verifica preservación contractual y append-only de eventos en una limpieza categórica."""
    trips = tripdataset_validated_small
    data_before = trips.data.copy(deep=True)
    metadata_before = deepcopy(trips.metadata)

    expected = data_before.loc[data_before["mode"] != "metro"]

    cleaned, report = clean_trips(
        trips,
        options=CleanOptions(drop_rows_by_categorical_values={"mode": ["metro"]}),
    )

    assert report.ok is True
    pd.testing.assert_frame_equal(trips.data, data_before)
    assert trips.metadata == metadata_before
    pd.testing.assert_frame_equal(cleaned.data, expected)

    assert report.summary["rows_in"] == len(data_before)
    assert report.summary["rows_out"] == len(expected)
    assert report.summary["dropped_total"] == len(data_before) - len(expected)
    assert report.summary["dropped_by_rule"]["categorical_values"] == int(
        (data_before["mode"] == "metro").sum()
    )

    assert cleaned is not trips
    assert cleaned.metadata["is_validated"] is True
    assert "artifact_id" not in cleaned.metadata
    assert len(cleaned.metadata["events"]) == len(metadata_before["events"]) + 1
    assert cleaned.metadata["events"][:-1] == metadata_before["events"]

    _assert_base_contract_preserved(cleaned, trips, metadata_before=metadata_before)
    _assert_clean_event_alignment(cleaned, report)


def test_clean_trips_integration_uses_default_duplicates_subset(
    base_clean_schema: TripSchema,
    base_clean_schema_effective: TripSchemaEffective,
) -> None:
    """Verifica que duplicates_subset default use schema.required intersectado con columnas."""
    schema_for_default_dups = TripSchema(
        version="1.1",
        fields=deepcopy(base_clean_schema.fields),
        required=[
            "user_id",
            "origin_time_utc",
            "origin_h3_index",
            "destination_h3_index",
        ],
        semantic_rules=None,
    )

    rows = _canonical_rows()
    rows[1]["user_id"] = rows[0]["user_id"]
    rows[1]["origin_time_utc"] = rows[0]["origin_time_utc"]
    rows[1]["origin_h3_index"] = rows[0]["origin_h3_index"]
    rows[1]["destination_h3_index"] = rows[0]["destination_h3_index"]

    data = pd.DataFrame(rows)
    trips = TripDataset(
        data=data.copy(),
        schema=schema_for_default_dups,
        schema_version=schema_for_default_dups.version,
        provenance={"source": {"name": "default_dups_fixture"}},
        field_correspondence={},
        value_correspondence={},
        metadata={
            "dataset_id": "tripdataset_default_dups_small",
            "events": [],
            "is_validated": True,
            "domains_effective": deepcopy(base_clean_schema_effective.domains_effective),
            "temporal": {
                "tier": "tier_1",
                "fields_present": ["origin_time_utc", "destination_time_utc"],
            },
        },
        schema_effective=deepcopy(base_clean_schema_effective),
    )

    expected_subset = [
        field for field in schema_for_default_dups.required if field in trips.data.columns
    ]
    expected = trips.data.loc[
        ~trips.data.duplicated(subset=expected_subset, keep="first")
    ]

    cleaned, report = clean_trips(
        trips,
        options=CleanOptions(
            drop_duplicates=True,
            duplicates_subset=None,
        ),
    )

    assert report.ok is True
    pd.testing.assert_frame_equal(cleaned.data, expected)

    assert report.parameters["duplicates_subset"] is None
    assert report.parameters["duplicates_subset_effective"] == expected_subset
    assert report.summary["rows_in"] == len(trips.data)
    assert report.summary["rows_out"] == len(expected)
    assert report.summary["dropped_total"] == len(trips.data) - len(expected)
    assert report.summary["dropped_by_rule"]["duplicates"] == int(
        trips.data.duplicated(subset=expected_subset, keep="first").sum()
    )
    assert cleaned.metadata["is_validated"] is True

    event = _assert_clean_event_alignment(cleaned, report)
    assert event["parameters"]["duplicates_subset_effective"] == expected_subset


def test_clean_trips_integration_active_rule_without_drops_keeps_dataset(
    tripdataset_canonical_small: TripDataset,
) -> None:
    """Verifica regla activa sin drops efectivos y evidencia informativa."""
    trips = tripdataset_canonical_small
    data_before = trips.data.copy(deep=True)

    cleaned, report = clean_trips(
        trips,
        options=CleanOptions(drop_rows_with_invalid_h3=True),
    )

    assert report.ok is True
    assert "CLN.NO_CHANGES.NO_ROWS_DROPPED" in _issue_codes(report)
    pd.testing.assert_frame_equal(cleaned.data, data_before)

    assert report.summary["rows_in"] == len(data_before)
    assert report.summary["rows_out"] == len(data_before)
    assert report.summary["dropped_total"] == 0
    assert report.summary["dropped_by_rule"]["invalid_h3"] == 0
    assert cleaned.metadata["is_validated"] is False

    _assert_clean_event_alignment(cleaned, report)


def test_clean_trips_integration_empty_result_is_returnable_with_warning(
    tripdataset_canonical_small: TripDataset,
) -> None:
    """Verifica que un resultado vacío sea retornable y emita warning no fatal."""
    trips = tripdataset_canonical_small
    data_before = trips.data.copy(deep=True)
    drop_modes = sorted(data_before["mode"].dropna().unique().tolist())

    cleaned, report = clean_trips(
        trips,
        options=CleanOptions(drop_rows_by_categorical_values={"mode": drop_modes}),
    )

    assert report.ok is True
    assert "CLN.RESULT.EMPTY_DATASET" in _issue_codes(report)
    assert cleaned.data.empty

    assert report.summary["rows_in"] == len(data_before)
    assert report.summary["rows_out"] == 0
    assert report.summary["dropped_total"] == len(data_before)
    assert report.summary["dropped_by_rule"]["categorical_values"] == len(data_before)
    assert cleaned.metadata["is_validated"] is False

    _assert_clean_event_alignment(cleaned, report)


def test_clean_trips_integration_accepts_partial_od_and_rejects_broken_endpoint(
    base_clean_schema: TripSchema,
    base_clean_schema_effective: TripSchemaEffective,
) -> None:
    """Verifica OD parcial válido y descarte de endpoint espacial roto."""
    rows = [
        {
            "movement_id": "m_ok",
            "user_id": "u_ok",
            "origin_longitude": -70.66,
            "origin_latitude": -33.45,
            "destination_longitude": -70.67,
            "destination_latitude": -33.46,
            "trip_id": "t_ok",
            "movement_seq": 0,
            "mode": "walk",
            "purpose": "work",
        },
        {
            "movement_id": "m_partial_valid",
            "user_id": "u_partial",
            "origin_longitude": -70.68,
            "origin_latitude": -33.47,
            "destination_longitude": None,
            "destination_latitude": None,
            "trip_id": "t_partial",
            "movement_seq": 0,
            "mode": "bus",
            "purpose": "study",
        },
        {
            "movement_id": "m_broken",
            "user_id": "u_broken",
            "origin_longitude": None,
            "origin_latitude": -33.49,
            "destination_longitude": -70.71,
            "destination_latitude": -33.50,
            "trip_id": "t_broken",
            "movement_seq": 0,
            "mode": "metro",
            "purpose": "work",
        },
    ]

    trips = TripDataset(
        data=pd.DataFrame(rows),
        schema=base_clean_schema,
        schema_version=base_clean_schema.version,
        provenance={"source": {"name": "od_partial_fixture"}},
        field_correspondence={},
        value_correspondence={},
        metadata={
            "dataset_id": "tripdataset_od_partial_small",
            "events": [],
            "is_validated": True,
            "domains_effective": deepcopy(base_clean_schema_effective.domains_effective),
            "temporal": {"tier": "tier_3", "fields_present": []},
        },
        schema_effective=deepcopy(base_clean_schema_effective),
    )

    cleaned, report = clean_trips(
        trips,
        options=CleanOptions(drop_rows_with_invalid_latlon=True),
    )

    expected = trips.data.loc[
        trips.data["movement_id"].isin(["m_ok", "m_partial_valid"])
    ]

    assert report.ok is True
    pd.testing.assert_frame_equal(cleaned.data, expected)

    assert report.summary["rows_in"] == len(trips.data)
    assert report.summary["rows_out"] == len(expected)
    assert report.summary["dropped_total"] == len(trips.data) - len(expected)
    assert report.summary["dropped_by_rule"]["invalid_latlon"] == 1
    assert cleaned.metadata["is_validated"] is True

    _assert_clean_event_alignment(cleaned, report)


def test_clean_trips_integration_non_categorical_field_entry_is_omitted(
    tripdataset_canonical_small: TripDataset,
) -> None:
    """Verifica que un drop categórico sobre campo no categórico se omita con warning."""
    trips = tripdataset_canonical_small
    data_before = trips.data.copy(deep=True)

    cleaned, report = clean_trips(
        trips,
        options=CleanOptions(drop_rows_by_categorical_values={"trip_weight": [1.1, 1.2]}),
    )

    codes = _issue_codes(report)

    assert report.ok is True
    assert "CLN.RULE.FIELD_NOT_CATEGORICAL" in codes
    assert "CLN.NO_CHANGES.NO_RULES_ACTIVE" in codes
    pd.testing.assert_frame_equal(cleaned.data, data_before)

    assert report.summary["rows_in"] == len(data_before)
    assert report.summary["rows_out"] == len(data_before)
    assert report.summary["dropped_total"] == 0
    assert report.summary["dropped_by_rule"]["categorical_values"] == 0
    assert cleaned.metadata["is_validated"] is False

    _assert_clean_event_alignment(cleaned, report)