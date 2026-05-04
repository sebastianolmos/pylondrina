from __future__ import annotations

import copy
import json
from typing import Any

import pandas as pd
import pytest

from pylondrina.fixing import _rebuild_domains_effective_for_fields
from pylondrina.schema import DomainSpec, FieldSpec, TripSchema


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
def base_fix_schema() -> TripSchema:
    """Schema mínimo usado para reconstruir dominios efectivos en OP-03."""
    fields = [
        make_field("movement_id", "string", required=True),
        make_field("user_id", "string", required=True),
        make_field(
            "mode",
            "categorical",
            domain=DomainSpec(
                values=["walk", "bus", "metro", "car", "unknown"],
                extendable=True,
            ),
        ),
        make_field(
            "purpose",
            "categorical",
            domain=DomainSpec(
                values=["work", "study", "shopping", "unknown"],
                extendable=True,
            ),
        ),
        make_field("trip_weight", "float"),
    ]
    return TripSchema(
        version="1.1",
        fields={field.name: field for field in fields},
        required=[field.name for field in fields if field.required],
        semantic_rules=None,
    )


def _assert_json_safe(value: Any) -> None:
    """Falla si el valor no puede serializarse como JSON."""
    json.dumps(value)


def test_rebuild_domains_effective_reconstructs_from_observed_post_fix_values(
    base_fix_schema: TripSchema,
) -> None:
    """Verifica que los dominios tocados se reconstruyan desde los valores observados finales."""
    df_after_values = pd.DataFrame(
        {
            "mode": ["bus", "walk", "bike", "unknown", None],
            "purpose": ["work", "health", "unknown", "work", None],
        }
    )
    metadata_domains_before = {
        "mode": {
            "values": ["BUS", "walk", "unknown"],
            "extended": False,
            "unknown_value": "unknown",
            "strict_applied": False,
        },
        "purpose": {
            "values": ["work", "study", "unknown"],
            "extended": False,
            "unknown_value": "unknown",
            "strict_applied": True,
        },
    }
    schema_effective_domains_before = {
        "mode": {"values": ["BUS", "walk", "unknown"], "extended": False},
        "purpose": {"values": ["work", "study", "unknown"], "extended": False},
    }
    metadata_domains_before_snapshot = copy.deepcopy(metadata_domains_before)
    schema_effective_domains_before_snapshot = copy.deepcopy(schema_effective_domains_before)

    metadata_domains_updated, schema_domains_updated, updated_fields = _rebuild_domains_effective_for_fields(
        data_after_values=df_after_values,
        touched_fields=["mode", "purpose"],
        schema=base_fix_schema,
        metadata_domains_effective=metadata_domains_before,
        schema_effective_domains=schema_effective_domains_before,
    )

    assert updated_fields == ["mode", "purpose"]

    for field in updated_fields:
        observed_non_null = sorted(
            set(df_after_values[field].dropna().tolist()),
            key=lambda value: str(value),
        )
        base_values = set(base_fix_schema.fields[field].domain.values)
        expected_added_values = sorted(
            [value for value in observed_non_null if value not in base_values],
            key=lambda value: str(value),
        )

        assert metadata_domains_updated[field]["values"] == observed_non_null
        assert metadata_domains_updated[field]["extended"] is bool(expected_added_values)
        assert metadata_domains_updated[field].get("added_values", []) == expected_added_values
        assert metadata_domains_updated[field]["unknown_value"] == metadata_domains_before_snapshot[field]["unknown_value"]
        assert metadata_domains_updated[field]["strict_applied"] is metadata_domains_before_snapshot[field]["strict_applied"]
        assert schema_domains_updated[field] == metadata_domains_updated[field]

    assert metadata_domains_before == metadata_domains_before_snapshot
    assert schema_effective_domains_before == schema_effective_domains_before_snapshot
    _assert_json_safe(metadata_domains_updated)
    _assert_json_safe(schema_domains_updated)


def test_rebuild_domains_effective_updates_only_touched_existing_fields(
    base_fix_schema: TripSchema,
) -> None:
    """Verifica que solo se actualicen campos tocados existentes y se preserven los demás dominios."""
    df_after_values = pd.DataFrame(
        {
            "mode": ["bus", "walk", None],
        }
    )
    metadata_domains_before = {
        "mode": {
            "values": ["walk", "bus"],
            "extended": False,
        },
        "user_gender": {
            "values": ["female", "male", "unknown"],
            "extended": False,
        },
    }
    schema_effective_domains_before = {
        "mode": {
            "values": ["walk", "bus"],
            "extended": False,
        },
        "user_gender": {
            "values": ["female", "male", "unknown"],
            "extended": False,
        },
    }
    metadata_domains_before_snapshot = copy.deepcopy(metadata_domains_before)
    schema_effective_domains_before_snapshot = copy.deepcopy(schema_effective_domains_before)

    metadata_domains_updated, schema_domains_updated, updated_fields = _rebuild_domains_effective_for_fields(
        data_after_values=df_after_values,
        touched_fields=["mode", "missing_field"],
        schema=base_fix_schema,
        metadata_domains_effective=metadata_domains_before,
        schema_effective_domains=schema_effective_domains_before,
    )

    assert updated_fields == ["mode"]
    assert "missing_field" not in metadata_domains_updated
    assert "missing_field" not in schema_domains_updated

    expected_mode_values = sorted(
        set(df_after_values["mode"].dropna().tolist()),
        key=lambda value: str(value),
    )
    assert metadata_domains_updated["mode"]["values"] == expected_mode_values
    assert schema_domains_updated["mode"] == metadata_domains_updated["mode"]

    assert metadata_domains_updated["user_gender"] == metadata_domains_before_snapshot["user_gender"]
    assert schema_domains_updated["user_gender"] == schema_effective_domains_before_snapshot["user_gender"]
    assert metadata_domains_before == metadata_domains_before_snapshot
    assert schema_effective_domains_before == schema_effective_domains_before_snapshot
    _assert_json_safe(metadata_domains_updated)
    _assert_json_safe(schema_domains_updated)