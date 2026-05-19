from __future__ import annotations

import pandas as pd

from pylondrina.transforms.flows import (
    _extract_temporal_tier,
    _extract_validated_flag,
    _infer_h3_resolution_from_columns,
    _make_window_end,
    _make_window_start,
    _normalize_h3_series,
)


def test_extract_validated_flag_and_temporal_tier_use_expected_metadata_fallbacks() -> None:
    """Verifica lectura robusta de validación y tier temporal desde metadata."""
    assert _extract_validated_flag({"is_validated": True}) is True
    assert _extract_validated_flag({"flags": {"validated": True}}) is True
    assert _extract_validated_flag({}) is False
    assert _extract_validated_flag(None) is False

    assert _extract_temporal_tier({"temporal": {"tier": "tier_1"}}) == "tier_1"
    assert _extract_temporal_tier({"temporal": {}}) == "tier_3"
    assert _extract_temporal_tier({}) == "tier_3"
    assert _extract_temporal_tier(None) == "tier_3"


def test_normalize_h3_series_and_infer_resolution_preserve_valid_cells_and_flag_invalid_values() -> None:
    """Verifica normalización H3 e inferencia de resolución sin mezcla."""
    h3_series = pd.Series(
        [
            "8828308281fffff",
            None,
            "not_a_real_h3",
            "882830828dfffff",
        ]
    )

    normalized, missing_mask, invalid_values = _normalize_h3_series(h3_series)

    assert normalized.iloc[0] == "8828308281fffff"
    assert pd.isna(normalized.iloc[1])
    assert pd.isna(normalized.iloc[2])
    assert normalized.iloc[3] == "882830828dfffff"

    assert missing_mask.tolist() == [False, True, False, False]
    assert invalid_values == ["not_a_real_h3"]

    inferred_resolution, mixed_resolutions = _infer_h3_resolution_from_columns(
        pd.Series(["8828308281fffff"]),
        pd.Series(["882830828dfffff"]),
    )

    assert inferred_resolution == 8
    assert mixed_resolutions is False


def test_make_window_start_and_end_construct_hour_day_and_week_windows() -> None:
    """Verifica inicio y cierre de ventanas temporales horarias, diarias y semanales."""
    series_utc = pd.to_datetime(
        [
            "2026-04-01T08:17:00Z",
            "2026-04-02T15:42:00Z",
        ],
        utc=True,
    ).tz_convert(None)

    source_series = pd.Series(series_utc)

    hour_start = _make_window_start(series_utc, "hour")
    hour_end = _make_window_end(hour_start, "hour")

    pd.testing.assert_series_equal(
        hour_start.reset_index(drop=True),
        source_series.dt.floor("h").reset_index(drop=True),
        check_names=False,
    )
    pd.testing.assert_series_equal(
        hour_end.reset_index(drop=True),
        (hour_start + pd.Timedelta(hours=1)).reset_index(drop=True),
        check_names=False,
    )

    day_start = _make_window_start(series_utc, "day")
    day_end = _make_window_end(day_start, "day")

    pd.testing.assert_series_equal(
        day_start.reset_index(drop=True),
        source_series.dt.floor("D").reset_index(drop=True),
        check_names=False,
    )
    pd.testing.assert_series_equal(
        day_end.reset_index(drop=True),
        (day_start + pd.Timedelta(days=1)).reset_index(drop=True),
        check_names=False,
    )

    week_start = _make_window_start(series_utc, "week")
    week_end = _make_window_end(week_start, "week")

    expected_week_start = source_series.dt.floor("D") - pd.to_timedelta(
        source_series.dt.weekday,
        unit="D",
    )

    pd.testing.assert_series_equal(
        week_start.reset_index(drop=True),
        expected_week_start.reset_index(drop=True),
        check_names=False,
    )
    pd.testing.assert_series_equal(
        week_end.reset_index(drop=True),
        (week_start + pd.Timedelta(days=7)).reset_index(drop=True),
        check_names=False,
    )