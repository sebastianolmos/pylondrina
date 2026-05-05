from __future__ import annotations

import json
from typing import Any

import pandas as pd
import pytest

from pylondrina.datasets import TripDataset
from pylondrina.schema import DomainSpec, FieldSpec, TripSchema, TripSchemaEffective
from pylondrina.transforms.cleaning import (
    _extract_temporal_tier,
    _extract_validated_flag,
    _normalize_categorical_drop_map_or_none,
    _normalize_string_list_or_none,
    resolve_duplicates_subset_effective,
)


def assert_json_safe(obj: Any, label: str = "object") -> None:
    """Verifica que un objeto pueda serializarse a JSON."""
    try:
        json.dumps(obj, default=str)
    except Exception as exc:
        raise AssertionError(f"{label} no es JSON-safe: {exc}") from exc


def make_field(
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
    """Schema mínimo usado para helpers de configuración de OP-04."""
    fields = [
        make_field("movement_id", "string", required=True),
        make_field("user_id", "string", required=True),
        make_field(
            "mode",
            "categorical",
            required=False,
            domain=DomainSpec(values=["walk", "bus", "metro", "unknown"]),
        ),
        make_field(
            "purpose",
            "categorical",
            required=False,
            domain=DomainSpec(values=["work", "study", "unknown"]),
        ),
        make_field("origin_time_utc", "datetime", required=False),
        make_field("destination_time_utc", "datetime", required=False),
        make_field("origin_h3_index", "string", required=False),
        make_field("destination_h3_index", "string", required=False),
    ]

    return TripSchema(
        version="1.1",
        fields={field.name: field for field in fields},
        required=[field.name for field in fields if field.required],
        semantic_rules=None,
    )


@pytest.fixture()
def make_tripdataset_for_clean(base_clean_schema: TripSchema):
    """Factory mínima de TripDataset para probar helpers de configuración."""

    def _make(
        data: pd.DataFrame,
        *,
        schema: TripSchema | None = None,
        metadata: dict[str, Any] | None = None,
        schema_effective: TripSchemaEffective | None = None,
    ) -> TripDataset:
        schema_eff = schema or base_clean_schema

        return TripDataset(
            data=data.copy(deep=True),
            schema=schema_eff,
            schema_version=schema_eff.version,
            provenance={"source": {"name": "synthetic_helper_test"}},
            field_correspondence={},
            value_correspondence={},
            metadata=metadata or {},
            schema_effective=schema_effective or TripSchemaEffective(),
        )

    return _make


def test_resolve_duplicates_subset_effective_handles_inactive_explicit_default_and_empty_default(
    make_tripdataset_for_clean,
) -> None:
    """Verifica subset efectivo para regla inactiva, subset explícito, default y default vacío."""
    df_with_required = pd.DataFrame(
        {
            "movement_id": ["m1", "m2"],
            "user_id": ["u1", "u2"],
            "other": [1, 2],
        }
    )
    trips_with_required = make_tripdataset_for_clean(df_with_required)

    inactive_subset = resolve_duplicates_subset_effective(
        trips_with_required,
        drop_duplicates=False,
        duplicates_subset=None,
    )
    assert inactive_subset is None

    explicit_request = ["user_id", "movement_id", "user_id", "movement_id"]
    explicit_subset = resolve_duplicates_subset_effective(
        trips_with_required,
        drop_duplicates=True,
        duplicates_subset=explicit_request,
    )
    assert explicit_subset == list(dict.fromkeys(explicit_request))

    default_subset = resolve_duplicates_subset_effective(
        trips_with_required,
        drop_duplicates=True,
        duplicates_subset=None,
    )
    expected_default = [
        field_name
        for field_name in trips_with_required.schema.required
        if field_name in trips_with_required.data.columns
    ]
    assert default_subset == expected_default

    df_without_required = pd.DataFrame({"x": [1], "y": [2]})
    trips_without_required = make_tripdataset_for_clean(df_without_required)

    empty_default_subset = resolve_duplicates_subset_effective(
        trips_without_required,
        drop_duplicates=True,
        duplicates_subset=None,
    )
    assert empty_default_subset == []


def test_normalize_string_list_or_none_returns_none_or_deduplicated_string_list() -> None:
    """Verifica normalización de listas textuales, deduplicación y coerción simple a string."""
    assert _normalize_string_list_or_none(None) is None
    assert _normalize_string_list_or_none([]) is None

    requested = ["mode", "purpose", "mode"]
    normalized = _normalize_string_list_or_none(requested)
    assert normalized == list(dict.fromkeys(requested))

    mixed_requested = ["a", 1, "a"]
    mixed_normalized = _normalize_string_list_or_none(mixed_requested)
    assert mixed_normalized == ["a", "1"]


def test_normalize_categorical_drop_map_or_none_deduplicates_and_preserves_null_sentinel() -> None:
    """Verifica normalización JSON-safe del mapping categórico y preservación de None."""
    assert _normalize_categorical_drop_map_or_none(None) is None
    assert _normalize_categorical_drop_map_or_none({}) is None

    requested_drop_map = {
        "mode": ["unknown", "unknown", None, None, "other"],
        "purpose": [None, "unknown", "unknown"],
    }

    normalized = _normalize_categorical_drop_map_or_none(requested_drop_map)

    assert normalized == {
        "mode": ["unknown", None, "other"],
        "purpose": [None, "unknown"],
    }
    assert_json_safe(normalized, "normalized_drop_map")


def test_extract_temporal_tier_prioritizes_metadata_and_infers_tier_from_columns() -> None:
    """Verifica prioridad de metadata temporal e inferencia conservadora Tier 1, 2 y 3."""
    df_tier_1 = pd.DataFrame(
        {
            "origin_time_utc": ["2026-01-01T08:00:00Z"],
            "destination_time_utc": ["2026-01-01T09:00:00Z"],
        }
    )
    df_tier_2 = pd.DataFrame(
        {
            "origin_time_local_hhmm": ["08:00"],
            "destination_time_local_hhmm": ["09:00"],
        }
    )
    df_tier_3 = pd.DataFrame(
        {
            "movement_id": ["m1"],
        }
    )

    assert _extract_temporal_tier({"tier": "tier_2"}, df_tier_1) == "tier_2"
    assert _extract_temporal_tier({}, df_tier_1) == "tier_1"
    assert _extract_temporal_tier({}, df_tier_2) == "tier_2"
    assert _extract_temporal_tier({}, df_tier_3) == "tier_3"


def test_extract_validated_flag_reads_current_flag_and_legacy_fallback() -> None:
    """Verifica lectura de is_validated, fallback legacy y metadata ausente o inválida."""
    assert _extract_validated_flag({"is_validated": True}) is True
    assert _extract_validated_flag({"is_validated": False}) is False

    assert _extract_validated_flag({"flags": {"validated": True}}) is True
    assert _extract_validated_flag({"flags": {"validated": False}}) is False

    assert _extract_validated_flag({}) is False
    assert _extract_validated_flag(None) is False