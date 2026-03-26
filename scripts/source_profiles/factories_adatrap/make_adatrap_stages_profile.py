from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping, Optional

import re
import unicodedata
import numpy as np
import pandas as pd
from pyproj import Transformer

from pylondrina.importing import ImportOptions
from pylondrina.schema import TripSchema
from pylondrina.sources.profile import SourceProfile
from pylondrina.types import FieldCorrespondence, ValueCorrespondence

from scripts.source_profiles.factories_adatrap.stages_defaults import (
    ADATRAP_STAGES_DEFAULT_OPTIONS,
    ADATRAP_STAGES_DEFAULT_FIELD_CORRESPONDENCE,
    make_adatrap_stages_default_schema,
    make_adatrap_stages_default_value_correspondence,
)


DEFAULT_SOURCE_CRS = "EPSG:5361"
DEFAULT_TARGET_CRS = "EPSG:4326"

NULL_LIKE_VALUES = {"", "-", "--", "nan", "none", "null", "na", "n/a"}


def load_yaml_file(path: str | Path) -> dict[str, Any]:
    import yaml
    path = Path(path)
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def resolve_adatrap_stage_layout(
    columns: list[str],
    *,
    stage_layout_yaml: str | Path,
) -> dict[str, list[str]]:
    layout = load_yaml_file(stage_layout_yaml)
    return {
        "trip_level": [c for c in layout.get("trip_level", []) if c in columns],
        "stage_1": [c for c in layout.get("stage_1", []) if c in columns],
        "stage_2": [c for c in layout.get("stage_2", []) if c in columns],
        "stage_3": [c for c in layout.get("stage_3", []) if c in columns],
        "stage_4": [c for c in layout.get("stage_4", []) if c in columns],
    }


def is_missing_like(value: Any) -> bool:
    if pd.isna(value):
        return True
    s = str(value).strip()
    return s.lower() in NULL_LIKE_VALUES


def infer_stage_count_from_row(row: pd.Series) -> int:
    if "etapas" in row.index:
        try:
            val = int(float(str(row["etapas"]).strip()))
            if 0 <= val <= 4:
                return val
        except Exception:
            pass

    stage_presence = 0
    for i, suf in enumerate(["1era", "2da", "3era", "4ta"], start=1):
        possible = [
            row.get(f"paraderosubida_{suf}"),
            row.get(f"paraderobajada_{suf}"),
            row.get(f"tipotransporte_{suf}"),
            row.get(f"t_{suf}_etapa"),
        ]
        if any(not is_missing_like(v) for v in possible):
            stage_presence = i

    return stage_presence


def strip_stage_suffix(col: str) -> str:
    col = re.sub(r"_(1era|2da|3era|4ta)_etapa$", "_etapa", col)
    col = re.sub(r"_(1era|2da|3era|4ta)$", "_etapa", col)
    col = re.sub(r"_(1|2|3|4)$", "_etapa", col)
    return col


def fix_station(station: Any) -> Any:
    if is_missing_like(station):
        return pd.NA

    station = str(station).strip()
    parts = station.split("-")

    if len(parts) < 2:
        return station

    if not parts[-1].isdigit():
        parts[-1], parts[-2] = parts[-2], parts[-1]
        return "-".join(parts)

    return station


def standardize_spanish_station(text: Any) -> Any:
    if is_missing_like(text):
        return pd.NA

    text = str(text).strip()
    parts = text.split("-")

    if len(parts) < 2:
        normalized = unicodedata.normalize("NFKD", text)
        standardized = "".join(c for c in normalized if not unicodedata.combining(c))
        return standardized.replace("`", "").upper().strip()

    return text


def normalize_stop_code(value: Any) -> Any:
    if is_missing_like(value):
        return pd.NA
    v = fix_station(value)
    v = standardize_spanish_station(v)
    if pd.isna(v):
        return pd.NA
    return str(v).strip().upper()


def parse_number_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype(str).str.replace(",", ".", regex=False),
        errors="coerce",
    )


def xy_to_lonlat(
    x: pd.Series,
    y: pd.Series,
    *,
    source_crs: str = DEFAULT_SOURCE_CRS,
    target_crs: str = DEFAULT_TARGET_CRS,
) -> tuple[pd.Series, pd.Series]:
    x_num = parse_number_series(x)
    y_num = parse_number_series(y)

    valid = x_num.notna() & y_num.notna()

    lon = pd.Series(np.nan, index=x.index, dtype="float64")
    lat = pd.Series(np.nan, index=x.index, dtype="float64")

    if valid.any():
        transformer = Transformer.from_crs(source_crs, target_crs, always_xy=True)
        lon_vals, lat_vals = transformer.transform(
            x_num.loc[valid].to_numpy(),
            y_num.loc[valid].to_numpy(),
        )
        lon.loc[valid] = lon_vals
        lat.loc[valid] = lat_vals

    return lon, lat


def prepare_adatrap_stops_table(
    df_stops: pd.DataFrame,
    *,
    stop_code_col: str = "parada/est.metro",
    x_col: str = "x",
    y_col: str = "y",
) -> pd.DataFrame:
    stops = df_stops.copy()

    if stop_code_col not in stops.columns:
        raise KeyError(f"No existe la columna {stop_code_col!r} en df_stops")

    stops["stop_norm"] = stops[stop_code_col].map(normalize_stop_code)
    keep_cols = [c for c in [stop_code_col, x_col, y_col, "stop_norm"] if c in stops.columns]
    stops = stops[keep_cols].copy()
    stops = stops.dropna(subset=["stop_norm"]).drop_duplicates(subset=["stop_norm"])

    return stops


def lookup_stop_xy(
    stop_series: pd.Series,
    stops_df: pd.DataFrame,
    *,
    x_col: str = "x",
    y_col: str = "y",
) -> tuple[pd.Series, pd.Series]:
    stop_norm = stop_series.map(normalize_stop_code)
    stop_indexed = stops_df.set_index("stop_norm")

    x = stop_norm.map(stop_indexed[x_col]) if x_col in stop_indexed.columns else pd.Series(np.nan, index=stop_series.index)
    y = stop_norm.map(stop_indexed[y_col]) if y_col in stop_indexed.columns else pd.Series(np.nan, index=stop_series.index)

    return x, y

def _filter_field_correspondence_to_schema(
    field_corr: Optional[FieldCorrespondence],
    schema: TripSchema,
) -> Optional[dict[str, str]]:
    if field_corr is None:
        return None

    schema_fields = set(schema.fields.keys())
    return {
        canonical: source
        for canonical, source in dict(field_corr).items()
        if canonical in schema_fields
    }

def prepare_adatrap_stagelevel_for_import(
    df_viajes: pd.DataFrame,
    df_stops: pd.DataFrame,
    *,
    stage_layout_yaml: str | Path,
    stop_code_col: str = "parada/est.metro",
    x_col: str = "x",
    y_col: str = "y",
    source_crs: str = DEFAULT_SOURCE_CRS,
    target_crs: str = DEFAULT_TARGET_CRS,
) -> pd.DataFrame:
    """
    Réplica del preprocess manual del notebook para stages ADATRAP.
    """
    df = df_viajes.copy()
    stops = prepare_adatrap_stops_table(
        df_stops,
        stop_code_col=stop_code_col,
        x_col=x_col,
        y_col=y_col,
    )
    layout = resolve_adatrap_stage_layout(
        df.columns.tolist(),
        stage_layout_yaml=stage_layout_yaml,
    )

    trip_level_cols = list(layout["trip_level"])
    stage_cols_by_stage = {
        1: list(layout["stage_1"]),
        2: list(layout["stage_2"]),
        3: list(layout["stage_3"]),
        4: list(layout["stage_4"]),
    }

    rows = []

    for _, row in df.iterrows():
        stage_count = infer_stage_count_from_row(row)
        if stage_count <= 0:
            continue

        for stage_num in range(1, stage_count + 1):
            stage_cols = stage_cols_by_stage.get(stage_num, [])

            stage_values = [row.get(c) for c in stage_cols]
            if not any(not is_missing_like(v) for v in stage_values):
                continue

            out = {}

            for c in trip_level_cols:
                out[c] = row.get(c)

            out["numero_etapa"] = stage_num

            for c in stage_cols:
                out[strip_stage_suffix(c)] = row.get(c)

            rows.append(out)

    stages_df = pd.DataFrame(rows)

    if stages_df.empty:
        return stages_df

    if "paraderosubida_etapa" in stages_df.columns:
        sx, sy = lookup_stop_xy(stages_df["paraderosubida_etapa"], stops, x_col=x_col, y_col=y_col)
        subida_lon, subida_lat = xy_to_lonlat(
            sx,
            sy,
            source_crs=source_crs,
            target_crs=target_crs,
        )
        stages_df["subida_lon"] = subida_lon
        stages_df["subida_lat"] = subida_lat

    if "paraderobajada_etapa" in stages_df.columns:
        bx, by = lookup_stop_xy(stages_df["paraderobajada_etapa"], stops, x_col=x_col, y_col=y_col)
        bajada_lon, bajada_lat = xy_to_lonlat(
            bx,
            by,
            source_crs=source_crs,
            target_crs=target_crs,
        )
        stages_df["bajada_lon"] = bajada_lon
        stages_df["bajada_lat"] = bajada_lat

    return stages_df


def _merge_field_correspondence(
    base: Optional[FieldCorrespondence],
    override: Optional[FieldCorrespondence],
) -> Optional[dict[str, str]]:
    if base is None and override is None:
        return None

    merged: dict[str, str] = {}
    if base is not None:
        merged.update(dict(base))
    if override is not None:
        merged.update(dict(override))
    return merged


def _merge_value_correspondence(
    base: Optional[ValueCorrespondence],
    override: Optional[ValueCorrespondence],
) -> Optional[dict[str, dict[str, str]]]:
    if base is None and override is None:
        return None

    merged: dict[str, dict[str, str]] = {}
    if base is not None:
        for field, mapping in base.items():
            merged[field] = dict(mapping)

    if override is not None:
        for field, mapping in override.items():
            if field not in merged:
                merged[field] = dict(mapping)
            else:
                merged[field].update(dict(mapping))

    return merged


def _merge_import_options(
    base: ImportOptions,
    override: Optional[Mapping[str, Any]],
) -> ImportOptions:
    if not override:
        return base

    payload = asdict(base)
    payload.update(dict(override))
    return ImportOptions(**payload)


def make_adatrap_stages_profile(
    *,
    df_stops: pd.DataFrame,
    stage_layout_yaml: str | Path,
    domains_yaml: str | Path,
    stop_code_col: str = "parada/est.metro",
    stop_x_col: str = "x",
    stop_y_col: str = "y",
    source_crs: str = DEFAULT_SOURCE_CRS,
    target_crs: str = DEFAULT_TARGET_CRS,
    schema_override: Optional[TripSchema] = None,
    field_correspondence_override: Optional[FieldCorrespondence] = None,
    value_correspondence_override: Optional[ValueCorrespondence] = None,
    options_override: Optional[Mapping[str, Any]] = None,
    profile_name: str = "ADATRAP_STAGES",
    description: Optional[str] = None,
) -> SourceProfile:
    """
    Factory nivel 3 para ADATRAP stages.

    Importante:
    - usa stage_layout.yaml para clasificar columnas trip_level / stage_1..stage_4;
    - hace wide->long creando una fila por etapa real;
    - usa domains.yaml para schema y value correspondence.
    """
    effective_schema = schema_override or make_adatrap_stages_default_schema(domains_yaml)
    effective_options = _merge_import_options(ADATRAP_STAGES_DEFAULT_OPTIONS, options_override)
    effective_field_correspondence = _merge_field_correspondence(
        ADATRAP_STAGES_DEFAULT_FIELD_CORRESPONDENCE,
        field_correspondence_override,
    )
    effective_field_correspondence = _filter_field_correspondence_to_schema(
        effective_field_correspondence,
        effective_schema,
    )
    effective_value_correspondence = _merge_value_correspondence(
        make_adatrap_stages_default_value_correspondence(domains_yaml),
        value_correspondence_override,
    )

    effective_description = (
        description
        or "ADATRAP stages: preprocess wide->long usando stage_layout.yaml y domains.yaml."
    )

    def preprocess(df_wide: pd.DataFrame) -> pd.DataFrame:
        return prepare_adatrap_stagelevel_for_import(
            df_viajes=df_wide,
            df_stops=df_stops,
            stage_layout_yaml=stage_layout_yaml,
            stop_code_col=stop_code_col,
            x_col=stop_x_col,
            y_col=stop_y_col,
            source_crs=source_crs,
            target_crs=target_crs,
        )

    return SourceProfile(
        name=profile_name,
        description=effective_description,
        default_field_correspondence=effective_field_correspondence,
        default_value_correspondence=effective_value_correspondence,
        default_options=effective_options,
        preprocess=preprocess,
        schema_override=effective_schema,
    )