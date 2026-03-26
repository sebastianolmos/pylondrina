from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

import numpy as np
import pandas as pd
from pyproj import Transformer

from pylondrina.sources.profile import SourceProfile
from pylondrina.schema import TripSchema
from pylondrina.types import FieldCorrespondence, ValueCorrespondence
from pylondrina.importing import ImportOptions

from scripts.source_profiles.factories_eod.stages_defaults import (
    DEFAULT_EOD_SOURCE_CRS,
    DEFAULT_EOD_TARGET_CRS,
    EOD_TRIP_FACTOR_COLS,
    EOD_STAGE_LOOKUP_TABLES,
    EOD_PERSON_LOOKUP_TABLES,
    EOD_HOUSEHOLD_LOOKUP_TABLES,
    EOD_PERSON_COLS_USEFUL_FOR_STAGES,
    EOD_HOUSEHOLD_COLS_USEFUL_FOR_STAGES,
    EOD_TRIP_COLS_USEFUL_FOR_STAGES,
    EOD_TRIP_CONTEXT_LOOKUP_MAP,
    EOD_STAGES_DEFAULT_SCHEMA,
    EOD_STAGES_DEFAULT_OPTIONS,
    EOD_STAGES_DEFAULT_FIELD_CORRESPONDENCE,
    EOD_STAGES_DEFAULT_VALUE_CORRESPONDENCE,
)


def _normalize_aux_filename(filename: str) -> str:
    return str(filename).strip()


def _read_aux_csv(csv_path: Path) -> pd.DataFrame:
    attempts = [
        {"sep": ";", "encoding": "utf-8", "engine": "c"},
        {"sep": ",", "encoding": "utf-8", "engine": "c"},
        {"sep": ";", "encoding": "latin-1", "engine": "c"},
        {"sep": ",", "encoding": "latin-1", "engine": "c"},
        {"sep": ";", "encoding": "cp1252", "engine": "c"},
        {"sep": ",", "encoding": "cp1252", "engine": "c"},
        {"sep": None, "encoding": "latin-1", "engine": "python"},
    ]

    last_error = None
    for cfg in attempts:
        try:
            kwargs = {"filepath_or_buffer": csv_path, "dtype": str}

            if cfg["engine"] == "python":
                kwargs.update(sep=cfg["sep"], encoding=cfg["encoding"], engine="python")
            else:
                kwargs.update(
                    sep=cfg["sep"],
                    encoding=cfg["encoding"],
                    engine="c",
                    low_memory=False,
                )

            df = pd.read_csv(**kwargs)
            if df.shape[1] <= 1:
                continue
            return df

        except Exception as e:
            last_error = e

    raise RuntimeError(f"No se pudo leer la tabla auxiliar {csv_path.name}: {last_error}")


def load_eod_aux_tables_from_dir(aux_dir: str | Path) -> dict[str, pd.DataFrame]:
    aux_dir = Path(aux_dir)

    if not aux_dir.exists():
        raise FileNotFoundError(f"No existe el directorio de tablas auxiliares: {aux_dir}")
    if not aux_dir.is_dir():
        raise NotADirectoryError(f"La ruta de tablas auxiliares no es un directorio: {aux_dir}")

    tables: dict[str, pd.DataFrame] = {}
    for csv_path in sorted(aux_dir.glob("*.csv")):
        df = _read_aux_csv(csv_path)
        df.columns = [str(c).strip().replace("Código", "Codigo") for c in df.columns]
        tables[_normalize_aux_filename(csv_path.name)] = df

    return tables


def _normalize_lookup_key(value) -> Optional[str]:
    if pd.isna(value):
        return None

    s = str(value).strip().strip('"')
    if not s:
        return None

    if s.endswith(".0"):
        s = s[:-2]

    return s


def build_lookup(
    aux_tables: Mapping[str, pd.DataFrame],
    table_name: str,
) -> dict[str, str]:
    ref = aux_tables[table_name].copy()

    if ref.shape[1] < 2:
        raise ValueError(f"La tabla auxiliar {table_name} no tiene al menos 2 columnas.")

    key_col = ref.columns[0]
    value_col = ref.columns[1]
    alt_key_cols = list(ref.columns[2:])

    lookup: dict[str, str] = {}

    for _, row in ref.iterrows():
        label = row[value_col]
        if pd.isna(label):
            continue

        label = str(label).strip()

        main_key = _normalize_lookup_key(row[key_col])
        if main_key is not None:
            lookup[main_key] = label

        for alt_col in alt_key_cols:
            alt_key = _normalize_lookup_key(row[alt_col])
            if alt_key is not None:
                lookup[alt_key] = label

    return lookup


def decode_column_with_lookup_inplace(
    df: pd.DataFrame,
    column: str,
    lookup: Mapping[str, str],
) -> None:
    if column not in df.columns:
        return

    decoded = df[column].map(lambda x: lookup.get(_normalize_lookup_key(x), pd.NA))
    df[column] = decoded.where(decoded.notna(), df[column])


def decode_list_column_with_lookup_inplace(
    df: pd.DataFrame,
    column: str,
    lookup: Mapping[str, str],
    *,
    sep: str = ";",
) -> None:
    if column not in df.columns:
        return

    def _decode_value(value):
        if pd.isna(value):
            return pd.NA

        raw = str(value).strip().strip('"')
        if not raw:
            return pd.NA

        tokens = [t.strip() for t in raw.split(sep) if t.strip()]
        decoded_tokens = []

        for tok in tokens:
            nk = _normalize_lookup_key(tok)
            if nk is None:
                continue
            decoded_tokens.append(lookup.get(nk, tok))

        if not decoded_tokens:
            return pd.NA

        return sep.join(decoded_tokens)

    df[column] = df[column].map(_decode_value)


def parse_number_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype(str).str.replace(",", ".", regex=False),
        errors="coerce",
    )


def add_wgs84_from_xy(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    out_lon: str,
    out_lat: str,
    *,
    source_crs: str = DEFAULT_EOD_SOURCE_CRS,
    target_crs: str = DEFAULT_EOD_TARGET_CRS,
) -> None:
    if x_col not in df.columns or y_col not in df.columns:
        return

    x = parse_number_series(df[x_col])
    y = parse_number_series(df[y_col])

    valid = x.notna() & y.notna() & (x != 0) & (y != 0)

    lon = pd.Series(np.nan, index=df.index, dtype="float64")
    lat = pd.Series(np.nan, index=df.index, dtype="float64")

    if valid.any():
        transformer = Transformer.from_crs(source_crs, target_crs, always_xy=True)
        lon_vals, lat_vals = transformer.transform(
            x.loc[valid].to_numpy(),
            y.loc[valid].to_numpy(),
        )
        lon.loc[valid] = lon_vals
        lat.loc[valid] = lat_vals

    df[out_lon] = lon
    df[out_lat] = lat
    df.drop(columns=[x_col, y_col], inplace=True)


def first_non_null_numeric(df: pd.DataFrame, columns: Sequence[str]) -> pd.Series:
    out = pd.Series(np.nan, index=df.index, dtype="float64")

    for col in columns:
        if col not in df.columns:
            continue
        vals = parse_number_series(df[col])
        out = out.where(out.notna(), vals)

    return out


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


def _build_stage_seq(
    df: pd.DataFrame,
    *,
    trip_id_col: str,
    stage_order_col: Optional[str],
    origin_time_col: Optional[str],
    movement_seq_col_out: str,
) -> pd.DataFrame:
    out = df.copy()

    if trip_id_col not in out.columns:
        raise ValueError(f"No existe la columna de viaje '{trip_id_col}' para construir movement_seq.")

    sort_cols: list[str] = [trip_id_col]

    if stage_order_col is not None and stage_order_col in out.columns:
        sort_cols.append(stage_order_col)
    elif origin_time_col is not None and origin_time_col in out.columns:
        sort_cols.append(origin_time_col)
    elif "Etapa" in out.columns and "Etapa" != trip_id_col:
        sort_cols.append("Etapa")

    if sort_cols:
        out = out.sort_values(sort_cols, kind="stable").copy()

    out[movement_seq_col_out] = out.groupby(trip_id_col).cumcount() + 1
    return out


def make_eod_stages_profile(
    *,
    aux_dir: str | Path,
    df_viajes: Optional[pd.DataFrame] = None,
    df_personas: Optional[pd.DataFrame] = None,
    df_hogares: Optional[pd.DataFrame] = None,
    trip_id_col: str = "Viaje",
    stage_id_col: str = "Etapa",
    stage_order_col: Optional[str] = "Etapa",
    origin_time_col: Optional[str] = None,
    movement_seq_col_out: str = "NumeroEtapa",
    attach_trip_context: bool = True,
    attach_person_fields: bool = True,
    attach_household_fields: bool = False,
    person_cols_useful: Optional[Sequence[str]] = None,
    household_cols_useful: Optional[Sequence[str]] = None,
    trip_cols_useful: Optional[Sequence[str]] = None,
    source_crs: str = DEFAULT_EOD_SOURCE_CRS,
    target_crs: str = DEFAULT_EOD_TARGET_CRS,
    schema_override: Optional[TripSchema] = None,
    field_correspondence_override: Optional[FieldCorrespondence] = None,
    value_correspondence_override: Optional[ValueCorrespondence] = None,
    options_override: Optional[Mapping[str, Any]] = None,
    profile_name: str = "EOD_STAGES",
    description: Optional[str] = None,
) -> SourceProfile:
    """
    Construye un SourceProfile de nivel 3 para importar etapas EOD
    (1 fila = 1 etapa/movimiento) hacia Pylondrina.

    Filosofía del factory:
    - entrega una configuración recomendada y explícita;
    - incluye schema, options, correspondencias y preprocess por defecto;
    - permite ajustes parciales vía overrides;
    - evita reconstrucción completa desde cero.

    Notes
    -----
    - Este preprocess adapta la fuente, pero no valida Golondrina.
    - No crea colisiones en field_correspondence.
    - movement_seq se asegura dentro del preprocess.
    """
    effective_schema = schema_override or EOD_STAGES_DEFAULT_SCHEMA
    effective_options = _merge_import_options(EOD_STAGES_DEFAULT_OPTIONS, options_override)
    effective_field_correspondence = _merge_field_correspondence(
        EOD_STAGES_DEFAULT_FIELD_CORRESPONDENCE,
        field_correspondence_override,
    )
    effective_value_correspondence = _merge_value_correspondence(
        EOD_STAGES_DEFAULT_VALUE_CORRESPONDENCE,
        value_correspondence_override,
    )

    person_cols_useful = list(person_cols_useful or EOD_PERSON_COLS_USEFUL_FOR_STAGES)
    household_cols_useful = list(household_cols_useful or EOD_HOUSEHOLD_COLS_USEFUL_FOR_STAGES)
    trip_cols_useful = list(trip_cols_useful or EOD_TRIP_COLS_USEFUL_FOR_STAGES)

    effective_description = (
        description
        or "EOD stages: preprocess recomendado con contextos opcionales, secuencia de etapa, decodificación y XY -> WGS84."
    )

    def preprocess(df_etapas: pd.DataFrame) -> pd.DataFrame:
        aux_tables = load_eod_aux_tables_from_dir(aux_dir)

        needed_tables = set(EOD_STAGE_LOOKUP_TABLES.values()) | set(EOD_TRIP_CONTEXT_LOOKUP_MAP.values())

        if attach_person_fields and df_personas is not None:
            needed_tables |= set(EOD_PERSON_LOOKUP_TABLES.values())

        if attach_household_fields and df_hogares is not None:
            needed_tables |= set(EOD_HOUSEHOLD_LOOKUP_TABLES.values())

        missing = [name for name in needed_tables if name not in aux_tables]
        if missing:
            raise ValueError(
                "Faltan tablas auxiliares EOD requeridas para el factory de stages: "
                + ", ".join(sorted(missing))
            )

        lookups = {name: build_lookup(aux_tables, name) for name in needed_tables}

        df = df_etapas.copy()

        # 1) Enriquecimiento opcional
        if attach_person_fields and df_personas is not None:
            cols = [c for c in person_cols_useful if c in df_personas.columns]
            if cols:
                df = df.merge(
                    df_personas[cols].copy(),
                    on=["Hogar", "Persona"],
                    how="left",
                    suffixes=("", "_persona"),
                )

        if attach_household_fields and df_hogares is not None:
            cols = [c for c in household_cols_useful if c in df_hogares.columns]
            if cols:
                df = df.merge(
                    df_hogares[cols].copy(),
                    on=["Hogar"],
                    how="left",
                    suffixes=("", "_hogar"),
                )

        if attach_trip_context and df_viajes is not None:
            cols = [c for c in trip_cols_useful if c in df_viajes.columns]
            if cols:
                trip_ctx = df_viajes[cols].copy()
                rename_map = {c: f"trip_{c}" for c in cols if c not in {"Hogar", "Persona", "Viaje"}}
                trip_ctx = trip_ctx.rename(columns=rename_map)

                df = df.merge(
                    trip_ctx,
                    on=["Hogar", "Persona", "Viaje"],
                    how="left",
                )

        # 2) Decodificación propia de etapas
        if "Autopistas" in df.columns and "Autopista.csv" in lookups:
            decode_list_column_with_lookup_inplace(df, "Autopistas", lookups["Autopista.csv"], sep=";")

        for col, table_name in EOD_STAGE_LOOKUP_TABLES.items():
            if col in df.columns:
                decode_column_with_lookup_inplace(df, col, lookups[table_name])

        # 3) Decodificación de personas / hogares
        if attach_person_fields and df_personas is not None:
            for col, table_name in EOD_PERSON_LOOKUP_TABLES.items():
                if col in df.columns:
                    decode_column_with_lookup_inplace(df, col, lookups[table_name])

        if attach_household_fields and df_hogares is not None:
            for col, table_name in EOD_HOUSEHOLD_LOOKUP_TABLES.items():
                if col in df.columns:
                    decode_column_with_lookup_inplace(df, col, lookups[table_name])

        # 4) Decodificación del contexto de viaje
        if attach_trip_context and df_viajes is not None:
            for col, table_name in EOD_TRIP_CONTEXT_LOOKUP_MAP.items():
                if col in df.columns:
                    decode_column_with_lookup_inplace(df, col, lookups[table_name])

        # 5) Coordenadas
        add_wgs84_from_xy(
            df,
            x_col="OrigenCoordX",
            y_col="OrigenCoordY",
            out_lon="OrigenCoordLon",
            out_lat="OrigenCoordLat",
            source_crs=source_crs,
            target_crs=target_crs,
        )
        add_wgs84_from_xy(
            df,
            x_col="DestinoCoordX",
            y_col="DestinoCoordY",
            out_lon="DestinoCoordLon",
            out_lat="DestinoCoordLat",
            source_crs=source_crs,
            target_crs=target_crs,
        )

        # 6) movement_seq por viaje
        df = _build_stage_seq(
            df,
            trip_id_col=trip_id_col,
            stage_order_col=stage_order_col,
            origin_time_col=origin_time_col,
            movement_seq_col_out=movement_seq_col_out,
        )

        # 7) movement_id sin colisión
        if stage_id_col in df.columns:
            df["movement_id_src"] = df[stage_id_col].astype("string")
        else:
            df["movement_id_src"] = (
                df[trip_id_col].astype("string") + "_" + df[movement_seq_col_out].astype("string")
            )

        # 8) Peso del viaje propagado a etapas
        trip_factor_cols_present = [f"trip_{c}" for c in EOD_TRIP_FACTOR_COLS if f"trip_{c}" in df.columns]
        if trip_factor_cols_present:
            df["factor_expansion"] = first_non_null_numeric(df, trip_factor_cols_present)

        # 9) Contexto útil
        if "trip_Etapas" in df.columns:
            df["cantidad_etapas"] = pd.to_numeric(df["trip_Etapas"], errors="coerce").astype("Int64")

        return df

    return SourceProfile(
        name=profile_name,
        description=effective_description,
        default_field_correspondence=effective_field_correspondence,
        default_value_correspondence=effective_value_correspondence,
        default_options=effective_options,
        preprocess=preprocess,
        schema_override=effective_schema,
    )