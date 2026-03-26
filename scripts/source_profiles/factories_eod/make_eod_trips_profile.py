from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

import re
import numpy as np
import pandas as pd
from pyproj import Transformer

from pylondrina.importing import ImportOptions
from pylondrina.schema import TripSchema
from pylondrina.sources.profile import SourceProfile
from pylondrina.types import FieldCorrespondence, ValueCorrespondence

from scripts.source_profiles.factories_eod.trips_defaults import (
    EOD_TRIPS_DEFAULT_SCHEMA,
    EOD_TRIPS_DEFAULT_OPTIONS,
    EOD_TRIPS_DEFAULT_FIELD_CORRESPONDENCE,
    EOD_TRIPS_DEFAULT_VALUE_CORRESPONDENCE,
    EOD_TRIPS_DEFAULT_PROVENANCE_EXAMPLE,
)

# -----------------------------------------------------------------------------
# Constantes visibles y reutilizables (decisión explícita de diseño)
# -----------------------------------------------------------------------------

DEFAULT_EOD_SOURCE_CRS = "EPSG:5361"
DEFAULT_EOD_TARGET_CRS = "EPSG:4326"

EOD_TRIP_FACTOR_COLS = [
    "FactorLaboralNormal",
    "FactorSabadoNormal",
    "FactorDomingoNormal",
    "FactorLaboralEstival",
    "FactorFindesemanaEstival",
]

EOD_TRIP_LOOKUP_TABLES = {
    "ComunaOrigen": "Comuna.csv",
    "ComunaDestino": "Comuna.csv",
    "SectorOrigen": "Sector.csv",
    "SectorDestino": "Sector.csv",
    "Proposito": "Proposito.csv",
    "PropositoAgregado": "PropositoAgregado.csv",
    "ActividadDestino": "ActividadDestino.csv",
    "ModoAgregado": "ModoAgregado.csv",
    "ModoPriPub": "ModoPriPub.csv",
    "ModoMotor": "ModoMotor.csv",
    "Periodo": "Periodo.csv",
    "TiempoMedio": "TiempoMedio.csv",
    "CodigoTiempo": "CodigoTiempo.csv",
}

EOD_PERSON_LOOKUP_TABLES = {
    "Sexo": "Sexo.csv",
    "Relacion": "Relacion.csv",
    "LicenciaConducir": "LicenciaConducir.csv",
    "PaseEscolar": "PaseEscolar.csv",
    "AdultoMayor": "AdultoMayor.csv",
    "Estudios": "Estudios.csv",
    "Actividad": "Actividad.csv",
    "Ocupacion": "Ocupacion.csv",
    "ActividadEmpresa": "ActividadEmpresa.csv",
    "JornadaTrabajo": "JornadaTrabajo.csv",
    "DondeEstudia": "Donde Estudia.csv",
    "MedioViajeRestricion": "MedioViajeRestriccion.csv",
    "ConoceTransantiago": "ConoceSantiago.csv",
    "NoUsaTransantiago": "NoUsaTransantiago.csv",
    "Discapacidad": "Discapacidad.csv",
    "TieneIngresos": "TieneIngresos.csv",
    "TramoIngreso": "TramoIngreso.csv",
    "TramoIngresoFinal": "TramoIngreso.csv",
    "IngresoImputado": "IngresoImputado.csv",
}

EOD_HOUSEHOLD_LOOKUP_TABLES = {
    "TipoDia": "TipoDia.csv",
    "Temporada": "Temporada.csv",
    "Propiedad": "Propiedad.csv",
}

EOD_PERSON_COLS_USEFUL_FOR_TRIPS = [
    "Hogar",
    "Persona",
    "AnoNac",
    "Sexo",
    "Actividad",
    "Ocupacion",
    "LicenciaConducir",
    "PaseEscolar",
    "AdultoMayor",
    "TieneIngresos",
    "TramoIngreso",
]

EOD_HOUSEHOLD_COLS_USEFUL_FOR_TRIPS = [
    "Hogar",
    "Fecha",
    "TipoDia",
    "Temporada",
    "NumVeh",
    "NumBicAdulto",
    "NumBicNino",
    "IngresoHogar",
    "Comuna",
]

# -----------------------------------------------------------------------------
# Helpers internos
# -----------------------------------------------------------------------------

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
                kwargs.update(
                    sep=cfg["sep"],
                    encoding=cfg["encoding"],
                    engine="python",
                )
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
        valid_x = x.loc[valid].to_numpy()
        valid_y = y.loc[valid].to_numpy()
        lon_vals, lat_vals = transformer.transform(valid_x, valid_y)
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


# -----------------------------------------------------------------------------
# Factory principal nivel 3 para EOD trips
# -----------------------------------------------------------------------------

def make_eod_trips_profile(
    *,
    aux_dir: str | Path,
    df_personas: Optional[pd.DataFrame] = None,
    df_hogares: Optional[pd.DataFrame] = None,
    attach_person_fields: bool = True,
    attach_household_fields: bool = False,
    person_cols_useful: Optional[Sequence[str]] = None,
    household_cols_useful: Optional[Sequence[str]] = None,
    source_crs: str = DEFAULT_EOD_SOURCE_CRS,
    target_crs: str = DEFAULT_EOD_TARGET_CRS,
    schema_override: Optional[TripSchema] = None,
    field_correspondence_override: Optional[FieldCorrespondence] = None,
    value_correspondence_override: Optional[ValueCorrespondence] = None,
    options_override: Optional[Mapping[str, Any]] = None,
    profile_name: str = "EOD_TRIPS",
    description: Optional[str] = None,
) -> SourceProfile:
    """
    Construye un SourceProfile de nivel 3 para importar viajes resumidos EOD
    (1 fila = 1 viaje resumido) hacia Pylondrina.

    Filosofía del factory:
    - entrega una configuración recomendada y explícita;
    - incluye schema, options, correspondencias y preprocess por defecto;
    - permite ajustes parciales vía overrides;
    - evita reconstrucción completa desde cero.

    Parameters
    ----------
    aux_dir : str | Path
        Directorio con tablas auxiliares EOD (p. ej. Tablas_parametros/).
    df_personas, df_hogares : pandas.DataFrame, optional
        Tablas auxiliares para enriquecer viajes con atributos de persona/hogar.
    attach_person_fields, attach_household_fields : bool
        Controlan si se hace merge con Personas/Hogares.
    person_cols_useful, household_cols_useful : sequence of str, optional
        Subconjuntos explícitos de columnas a traer desde Personas/Hogares.
        Si None, se usan listas recomendadas.
    source_crs, target_crs : str
        CRS de entrada y salida para la transformación de coordenadas.
    schema_override : TripSchema, optional
        Permite reemplazar el schema recomendado.
    field_correspondence_override, value_correspondence_override : mapping, optional
        Overrides parciales sobre correspondencias por defecto.
    options_override : mapping, optional
        Overrides parciales sobre ImportOptions por defecto.
        Ejemplo: {"keep_extra_fields": False, "selected_fields": [...]}.
    profile_name : str, default="EOD_TRIPS"
        Nombre del perfil.
    description : str, optional
        Descripción humana del perfil.

    Returns
    -------
    SourceProfile
        Perfil listo para usarse con import_trips_from_profile(...).

    Notes
    -----
    - Este preprocess adapta la fuente, pero no certifica conformidad.
    - No crea colisiones en field_correspondence.
    - trip_id y movement_seq se dejan al import genérico mediante single_stage=True.
    """
    effective_schema = schema_override or EOD_TRIPS_DEFAULT_SCHEMA
    effective_options = _merge_import_options(EOD_TRIPS_DEFAULT_OPTIONS, options_override)
    effective_field_correspondence = _merge_field_correspondence(
        EOD_TRIPS_DEFAULT_FIELD_CORRESPONDENCE,
        field_correspondence_override,
    )
    effective_value_correspondence = _merge_value_correspondence(
        EOD_TRIPS_DEFAULT_VALUE_CORRESPONDENCE,
        value_correspondence_override,
    )

    person_cols_useful = list(person_cols_useful or EOD_PERSON_COLS_USEFUL_FOR_TRIPS)
    household_cols_useful = list(household_cols_useful or EOD_HOUSEHOLD_COLS_USEFUL_FOR_TRIPS)

    effective_description = (
        description
        or "EOD trips: viajes resumidos con preprocess recomendado (joins, decodificación y XY -> WGS84)."
    )

    def preprocess(df_viajes: pd.DataFrame) -> pd.DataFrame:
        aux_tables = load_eod_aux_tables_from_dir(aux_dir)

        needed_tables = (
            set(EOD_TRIP_LOOKUP_TABLES.values())
            | {"Modo.csv"}
        )

        if attach_person_fields and df_personas is not None:
            needed_tables |= set(EOD_PERSON_LOOKUP_TABLES.values())

        if attach_household_fields and df_hogares is not None:
            needed_tables |= set(EOD_HOUSEHOLD_LOOKUP_TABLES.values())

        missing = [name for name in needed_tables if name not in aux_tables]
        if missing:
            raise ValueError(
                "Faltan tablas auxiliares EOD requeridas para el factory: "
                + ", ".join(sorted(missing))
            )

        lookups = {name: build_lookup(aux_tables, name) for name in needed_tables}

        df = df_viajes.copy()

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

        # 2) Decodificación de columnas de viajes
        for col, table_name in EOD_TRIP_LOOKUP_TABLES.items():
            if col in df.columns:
                decode_column_with_lookup_inplace(df, col, lookups[table_name])

        # 3) Decodificación de personas
        if attach_person_fields and df_personas is not None:
            for col, table_name in EOD_PERSON_LOOKUP_TABLES.items():
                if col in df.columns:
                    decode_column_with_lookup_inplace(df, col, lookups[table_name])

        # 4) Decodificación de hogares
        if attach_household_fields and df_hogares is not None:
            for col, table_name in EOD_HOUSEHOLD_LOOKUP_TABLES.items():
                if col in df.columns:
                    decode_column_with_lookup_inplace(df, col, lookups[table_name])

        # 5) Secuencia de modos usada
        if "MediosUsados" in df.columns and "Modo.csv" in lookups:
            decode_list_column_with_lookup_inplace(
                df,
                "MediosUsados",
                lookups["Modo.csv"],
                sep=";",
            )
            df["mode_sequence"] = df["MediosUsados"]

        # 6) XY -> WGS84
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

        # 7) Factor de expansión no canónico pero útil
        df["factor_expansion"] = first_non_null_numeric(df, EOD_TRIP_FACTOR_COLS)

        # 8) Contexto extra útil
        if "Etapas" in df.columns:
            df["cantidad_etapas"] = pd.to_numeric(df["Etapas"], errors="coerce").astype("Int64")

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