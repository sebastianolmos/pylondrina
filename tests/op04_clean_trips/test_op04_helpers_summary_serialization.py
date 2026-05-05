from __future__ import annotations

import json
from typing import Any

import h3
import numpy as np
import pandas as pd

from pylondrina.transforms.cleaning import (
    _is_valid_h3_value,
    _json_safe_scalar,
    _to_json_serializable_or_none,
    build_clean_summary,
)


def assert_json_safe(obj: Any, label: str = "object") -> None:
    """Verifica que un objeto pueda serializarse a JSON sin perder estabilidad básica."""
    try:
        json.dumps(obj, default=str)
    except Exception as exc:
        raise AssertionError(f"{label} no es JSON-safe: {exc}") from exc


def make_valid_h3(lat: float = -33.45, lon: float = -70.66, res: int = 8) -> str:
    """Construye una celda H3 válida para tests de helpers de OP-04."""
    return h3.latlng_to_cell(lat, lon, res)


def test_build_clean_summary_uses_canonical_keys_and_zero_fills_missing_rules() -> None:
    """Verifica el summary canónico, dropped_total y relleno en cero de reglas omitidas."""
    rows_in = 10
    rows_out = 7
    dropped_by_rule = {
        "duplicates": 2,
        "invalid_h3": 1,
    }

    summary = build_clean_summary(
        rows_in=rows_in,
        rows_out=rows_out,
        dropped_by_rule=dropped_by_rule,
    )

    expected_rule_keys = {
        "nulls_required",
        "nulls_fields",
        "invalid_latlon",
        "invalid_h3",
        "origin_after_destination",
        "duplicates",
        "categorical_values",
    }

    assert set(summary.keys()) == {
        "rows_in",
        "rows_out",
        "dropped_total",
        "dropped_by_rule",
    }
    assert summary["rows_in"] == rows_in
    assert summary["rows_out"] == rows_out
    assert summary["dropped_total"] == rows_in - rows_out

    assert set(summary["dropped_by_rule"].keys()) == expected_rule_keys
    assert summary["dropped_by_rule"]["duplicates"] == dropped_by_rule["duplicates"]
    assert summary["dropped_by_rule"]["invalid_h3"] == dropped_by_rule["invalid_h3"]

    omitted_rules = expected_rule_keys - set(dropped_by_rule)
    assert all(summary["dropped_by_rule"][rule] == 0 for rule in omitted_rules)

    assert_json_safe(summary, "clean_summary")


def test_is_valid_h3_value_accepts_valid_cell_and_rejects_null_empty_or_invalid_values() -> None:
    """Verifica validación tolerante de H3 válido, nulos, vacío e inválido textual."""
    valid_h3 = make_valid_h3(-33.45, -70.66, 8)

    assert _is_valid_h3_value(valid_h3) is True
    assert _is_valid_h3_value(None) is False
    assert _is_valid_h3_value("") is False
    assert _is_valid_h3_value("not_a_real_h3") is False


def test_json_safe_scalar_normalizes_problematic_scalars_and_preserves_simple_values() -> None:
    """Verifica normalización de escalares simples, NaN y timestamps."""
    ts = pd.Timestamp("2026-01-01T08:00:00Z")

    assert _json_safe_scalar(None) is None
    assert _json_safe_scalar("x") == "x"
    assert _json_safe_scalar(5) == 5
    assert _json_safe_scalar(True) is True
    assert _json_safe_scalar(np.nan) is None

    ts_serialized = _json_safe_scalar(ts)
    assert isinstance(ts_serialized, str)
    assert ts_serialized.startswith("2026-01-01T08:00:00")
    assert_json_safe(ts_serialized, "timestamp_scalar")


def test_to_json_serializable_or_none_normalizes_nested_objects() -> None:
    """Verifica que estructuras anidadas con Timestamp y NaN queden JSON-safe."""
    ts = pd.Timestamp("2026-01-01T08:00:00Z")
    obj = {
        "a": 1,
        "b": [ts, np.nan, {"x": "y"}],
        "c": None,
    }

    serializable = _to_json_serializable_or_none(obj)

    assert serializable["a"] == obj["a"]
    assert isinstance(serializable["b"][0], str)
    assert serializable["b"][0].startswith("2026-01-01T08:00:00")
    assert serializable["b"][1] is None
    assert serializable["b"][2] == obj["b"][2]
    assert serializable["c"] is None

    assert_json_safe(serializable, "serializable_nested_obj")