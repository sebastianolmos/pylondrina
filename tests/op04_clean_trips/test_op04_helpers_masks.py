from __future__ import annotations

import h3
import pandas as pd

from pylondrina.transforms.cleaning import (
    mask_categorical_values,
    mask_duplicates,
    mask_invalid_h3,
    mask_invalid_latlon,
    mask_nulls_in_fields,
    mask_origin_after_destination,
)


def make_valid_h3(lat: float = -33.45, lon: float = -70.66, res: int = 8) -> str:
    """Construye una celda H3 válida para fixtures mínimas de máscaras."""
    return h3.latlng_to_cell(lat, lon, res)


def test_mask_nulls_in_fields_marks_rows_with_any_target_null() -> None:
    """Verifica nulos en cualquiera de los campos objetivo y máscara vacía sin targets."""
    df = pd.DataFrame(
        {
            "a": [1, None, 3, 4],
            "b": ["x", "y", None, "z"],
            "c": [10, 20, 30, 40],
        }
    )

    mask_ab = mask_nulls_in_fields(df, ["a", "b"])
    expected_ab = df[["a", "b"]].isna().any(axis=1)
    pd.testing.assert_series_equal(mask_ab, expected_ab)

    mask_empty = mask_nulls_in_fields(df, [])
    expected_empty = pd.Series(False, index=df.index, dtype=bool)
    pd.testing.assert_series_equal(mask_empty, expected_empty)


def test_mask_invalid_latlon_accepts_partial_od_but_rejects_broken_or_invalid_endpoints() -> None:
    """Verifica la semántica espacial: OD parcial válido, endpoint roto e inválidos se marcan."""
    df = pd.DataFrame(
        {
            "origin_latitude": [-33.45, -33.45, -33.45, -95.00, None],
            "origin_longitude": [-70.66, -70.66, None, -70.66, None],
            "destination_latitude": [-33.46, None, -33.46, -33.46, None],
            "destination_longitude": [-70.67, None, -70.67, -70.67, None],
        },
        index=[
            "valid_full",
            "od_partial_valid",
            "origin_broken",
            "origin_out_of_range",
            "both_absent",
        ],
    )

    mask = mask_invalid_latlon(df)

    assert bool(mask.loc["valid_full"]) is False
    assert bool(mask.loc["od_partial_valid"]) is False
    assert bool(mask.loc["origin_broken"]) is True
    assert bool(mask.loc["origin_out_of_range"]) is True
    assert bool(mask.loc["both_absent"]) is True


def test_mask_invalid_h3_requires_both_od_cells_present_and_valid() -> None:
    """Verifica que H3 exija origen y destino presentes y válidos, sin aceptar parcialidad."""
    valid_h3_a = make_valid_h3(-33.45, -70.66, 8)
    valid_h3_b = make_valid_h3(-33.46, -70.67, 8)

    df = pd.DataFrame(
        {
            "origin_h3_index": [valid_h3_a, valid_h3_a, "", "not_a_real_h3"],
            "destination_h3_index": [valid_h3_b, None, valid_h3_b, valid_h3_b],
        },
        index=["valid_pair", "missing_dest", "empty_origin", "invalid_origin"],
    )

    mask = mask_invalid_h3(df)

    assert bool(mask.loc["valid_pair"]) is False
    assert bool(mask.loc["missing_dest"]) is True
    assert bool(mask.loc["empty_origin"]) is True
    assert bool(mask.loc["invalid_origin"]) is True


def test_mask_origin_after_destination_marks_only_comparable_reversed_intervals() -> None:
    """Verifica que solo se marquen tiempos comparables con origen posterior a destino."""
    df = pd.DataFrame(
        {
            "origin_time_utc": [
                "2026-01-01T08:00:00Z",
                "2026-01-01T10:00:00Z",
                "2026-01-01T09:00:00Z",
                None,
                "not_a_datetime",
            ],
            "destination_time_utc": [
                "2026-01-01T09:00:00Z",
                "2026-01-01T09:30:00Z",
                "2026-01-01T09:00:00Z",
                "2026-01-01T09:00:00Z",
                "2026-01-01T10:00:00Z",
            ],
        },
        index=["valid_order", "reversed", "equal_times", "origin_null", "origin_invalid"],
    )

    mask = mask_origin_after_destination(df)

    assert bool(mask.loc["valid_order"]) is False
    assert bool(mask.loc["reversed"]) is True
    assert bool(mask.loc["equal_times"]) is False
    assert bool(mask.loc["origin_null"]) is False
    assert bool(mask.loc["origin_invalid"]) is False


def test_mask_duplicates_keeps_first_occurrence_and_marks_later_duplicates() -> None:
    """Verifica que la máscara de duplicados conserve la primera ocurrencia por subset."""
    df = pd.DataFrame(
        {
            "user_id": ["u1", "u1", "u2", "u2", "u2"],
            "origin_h3_index": ["a", "a", "b", "b", "c"],
            "destination_h3_index": ["x", "x", "y", "y", "z"],
        }
    )
    subset = ["user_id", "origin_h3_index", "destination_h3_index"]

    mask = mask_duplicates(df, subset=subset)
    expected = df.duplicated(subset=subset, keep="first")

    pd.testing.assert_series_equal(mask, expected)
    assert mask.tolist() == [False, True, False, True, False]


def test_mask_categorical_values_combines_explicit_values_and_null_sentinel_by_or() -> None:
    """Verifica drops categóricos por valores explícitos y None como sentinel de nulos."""
    df = pd.DataFrame(
        {
            "mode": ["walk", "unknown", "bus", None, "metro"],
            "purpose": ["work", "study", None, "unknown", "work"],
        },
        index=["r0", "r1", "r2", "r3", "r4"],
    )
    drop_map = {
        "mode": ["unknown"],
        "purpose": ["unknown", None],
    }

    mask = mask_categorical_values(df, drop_map)
    expected = (
        df["mode"].isin(["unknown"])
        | df["purpose"].isin(["unknown"])
        | df["purpose"].isna()
    )

    pd.testing.assert_series_equal(mask, expected)

    assert bool(mask.loc["r0"]) is False
    assert bool(mask.loc["r1"]) is True
    assert bool(mask.loc["r2"]) is True
    assert bool(mask.loc["r3"]) is True
    assert bool(mask.loc["r4"]) is False