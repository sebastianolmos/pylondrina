from __future__ import annotations

"""
Generación determinista de casos experimentales para comparar backends de
persistencia (Parquet vs Feather) en TripDataset Golondrina.

Este módulo construye, para una configuración experimental dada `(n_cols, n_rows, k)`,
un caso completo compuesto por:

- `TripDataset` canónico y estable en memoria.
- `TripSchema` asociado.
- `manifest` JSON-safe con metadatos reproducibles del caso.
- `expected_fingerprint` del dataset (por columna + global).

Diseño adoptado
---------------
- La generación es determinista: misma configuración + misma seed -> mismo dataset lógico.
- Se usa un esquema base común y se agregan columnas extra hasta alcanzar `n_cols`.
- Se definen 2 campos categóricos controlados por `k` y 4 categóricos fijos de baja cardinalidad.
- El fingerprint esperado se calcula siempre para las 9 configuraciones.
- La función pública principal es `generate_experiment_case(...)`.

Notas operativas
----------------
- El dataset se devuelve con `metadata["is_validated"] = True` por construcción,
  para que `write_trips(...)` pueda usarse con sus defaults (`require_validated=True`)
  durante el benchmark.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import hashlib
import json

import numpy as np
import pandas as pd
from pandas.api.types import (
    is_bool_dtype,
    is_categorical_dtype,
    is_datetime64_any_dtype,
    is_float_dtype,
    is_integer_dtype,
)

from pylondrina.datasets import TripDataset
from pylondrina.schema import DomainSpec, FieldSpec, TripSchema, TripSchemaEffective
import h3 


# -----------------------------------------------------------------------------
# Configuración experimental
# -----------------------------------------------------------------------------

DEFAULT_SEED = 20260415
DEFAULT_SCHEMA_VERSION = "exp-persistence-v1"
DEFAULT_H3_RESOLUTION = 8
CONTROLLED_LABEL_WIDTH = 15
FIXED_CATEGORY_LENGTH_HINT = 20


@dataclass(frozen=True)
class ExperimentConfig:
    """
    Configuración experimental mínima.

    Parameters
    ----------
    config_id : str
        Identificador simbólico del caso, por ejemplo ``"C1"``.
    n_cols : int
        Cantidad total de columnas del DataFrame generado.
    n_rows : int
        Cantidad total de filas del DataFrame generado.
    k : int
        Cardinalidad compartida por los 2 campos categóricos controlados.
    h3_resolution : int, default=8
        Resolución H3 deseada. Si la librería `h3` no está disponible, este valor
        igualmente se registra en manifest y en el nombre de los pseudo-H3.
    """

    config_id: str
    n_cols: int
    n_rows: int
    k: int
    h3_resolution: int = DEFAULT_H3_RESOLUTION


@dataclass(frozen=True)
class GeneratedCase:
    """
    Caso experimental completo retornado por `generate_experiment_case`.
    """

    config: ExperimentConfig
    seed: int
    trips: TripDataset
    schema: TripSchema
    manifest: Dict[str, Any]
    expected_fingerprint: Dict[str, Any]
    fingerprint_path: Optional[Path] = None


# -----------------------------------------------------------------------------
# Configuraciones cerradas del experimento
# -----------------------------------------------------------------------------

PREDEFINED_CONFIGS: Dict[str, ExperimentConfig] = {
    "C1": ExperimentConfig("C1", n_cols=36, n_rows=200_000, k=10),
    "C2": ExperimentConfig("C2", n_cols=156, n_rows=200_000, k=10),
    "C3": ExperimentConfig("C3", n_cols=256, n_rows=200_000, k=10),
    "C4": ExperimentConfig("C4", n_cols=156, n_rows=20_000, k=10),
    "C5": ExperimentConfig("C5", n_cols=156, n_rows=1_000_000, k=10),
    "C6": ExperimentConfig("C6", n_cols=156, n_rows=200_000, k=1_000),
    "C7": ExperimentConfig("C7", n_cols=156, n_rows=200_000, k=10_000),
    "C8": ExperimentConfig("C8", n_cols=256, n_rows=1_000_000, k=10),
    "C9": ExperimentConfig("C9", n_cols=156, n_rows=1_000_000, k=10_000),
}


# -----------------------------------------------------------------------------
# Catálogos base
# -----------------------------------------------------------------------------

FIXED_MODE_VALUES: tuple[str, ...] = (
    "walk",
    "bus",
    "metro",
    "car",
    "bike",
    "scooter",
)
FIXED_PURPOSE_VALUES: tuple[str, ...] = (
    "work",
    "study",
    "shopping",
    "health",
    "care",
    "recreation",
    "home",
    "other",
)
FIXED_DAY_TYPE_VALUES: tuple[str, ...] = (
    "weekday",
    "weekend",
)
FIXED_TIME_PERIOD_VALUES: tuple[str, ...] = (
    "early_morning",
    "morning_peak",
    "midday",
    "afternoon",
    "evening_peak",
    "night",
)

# Proporción fija usada para columnas extra. Se privilegian dtypes relativamente livianos
# para que los escenarios grandes sigan siendo factibles en memoria.
EXTRA_DTYPE_CYCLE: tuple[str, ...] = (
    *("float",) * 10,
    *("int",) * 5,
    *("bool",) * 3,
    "string",
    "datetime",
)


# -----------------------------------------------------------------------------
# API pública principal
# -----------------------------------------------------------------------------


def generate_experiment_case(
    config: ExperimentConfig,
    *,
    seed: int = DEFAULT_SEED,
    persist_fingerprint_dir: Optional[str | Path] = None,
) -> GeneratedCase:
    """
    Genera un caso experimental completo y determinista.

    Parameters
    ----------
    config : ExperimentConfig
        Configuración `(n_cols, n_rows, k)` del caso.
    seed : int, default=20260415
        Seed fija del generador aleatorio. Misma seed + misma config + mismo código
        deben producir el mismo dataset lógico.
    persist_fingerprint_dir : str or Path, optional
        Si se entrega, persiste el fingerprint esperado del caso como
        ``<persist_fingerprint_dir>/<config_id>.json``.

    Returns
    -------
    GeneratedCase
        Caso experimental completo, con trips, schema, manifest y fingerprint esperado.

    Raises
    ------
    ValueError
        Si `config.n_cols` es menor que el mínimo de columnas base del experimento.
    """
    _validate_config(config)
    rng = np.random.default_rng(seed)

    controlled_domains = _build_controlled_domains(config.k)

    core_df = _build_core_columns(config, rng, controlled_domains)
    fixed_cat_df = _build_fixed_categoricals(config.n_rows)
    controlled_cat_df = _build_controlled_categoricals(config.n_rows, controlled_domains)

    work = pd.concat([core_df, fixed_cat_df, controlled_cat_df], axis=1)

    extra_specs_needed = config.n_cols - len(work.columns)
    extra_df, extra_field_specs = _build_extra_columns_until_n(
        n_rows=config.n_rows,
        n_extra_columns=extra_specs_needed,
        rng=rng,
    )
    if not extra_df.empty:
        work = pd.concat([work, extra_df], axis=1)

    # Orden fijo de columnas = parte del contrato determinista.
    work = work.loc[:, list(work.columns)]

    schema = _build_schema(
        controlled_domains=controlled_domains,
        extra_field_specs=extra_field_specs,
    )
    schema_effective = _build_schema_effective(schema=schema, df=work)
    metadata = _build_initial_metadata(config=config, seed=seed)
    provenance = _build_provenance(config=config, seed=seed)

    trips = TripDataset(
        data=work,
        schema=schema,
        schema_version=schema.version,
        provenance=provenance,
        field_correspondence={},
        value_correspondence={},
        metadata=metadata,
        schema_effective=schema_effective,
    )

    expected_fingerprint = compute_expected_fingerprint(
        trips.data,
        config=config,
        seed=seed,
    )
    manifest = _build_manifest(
        config=config,
        seed=seed,
        df=trips.data,
        schema=schema,
        schema_effective=schema_effective,
        expected_fingerprint=expected_fingerprint,
    )

    fingerprint_path: Optional[Path] = None
    if persist_fingerprint_dir is not None:
        fingerprint_path = write_expected_fingerprint_json(
            expected_fingerprint,
            persist_fingerprint_dir,
            config_id=config.config_id,
        )

    return GeneratedCase(
        config=config,
        seed=seed,
        trips=trips,
        schema=schema,
        manifest=manifest,
        expected_fingerprint=expected_fingerprint,
        fingerprint_path=fingerprint_path,
    )


def get_predefined_config(config_id: str) -> ExperimentConfig:
    """
    Devuelve una de las 9 configuraciones cerradas del experimento.
    """
    try:
        return PREDEFINED_CONFIGS[config_id]
    except KeyError as exc:
        raise KeyError(f"config_id desconocido: {config_id!r}") from exc


# -----------------------------------------------------------------------------
# Fingerprints
# -----------------------------------------------------------------------------


def compute_expected_fingerprint(
    df: pd.DataFrame,
    *,
    config: ExperimentConfig,
    seed: int,
) -> Dict[str, Any]:
    """
    Calcula el fingerprint esperado del dataset completo.

    El fingerprint siempre se calcula, incluso para configuraciones pequeñas.
    La decisión de usar además comparación exacta (`assert_frame_equal`) corresponde
    al runner del experimento, no al generador.
    """
    columns_order = list(df.columns)
    dtypes_expected = {column: str(df[column].dtype) for column in columns_order}
    column_fingerprints = {
        column: _series_fingerprint(df[column])
        for column in columns_order
    }

    global_payload = {
        "columns_order": columns_order,
        "dtypes_expected": dtypes_expected,
        "column_fingerprints": column_fingerprints,
    }
    dataset_fingerprint = hashlib.sha256(
        json.dumps(global_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()

    return {
        "config_id": config.config_id,
        "seed": int(seed),
        "n_rows": int(config.n_rows),
        "n_cols": int(config.n_cols),
        "k": int(config.k),
        "columns_order": columns_order,
        "dtypes_expected": dtypes_expected,
        "column_fingerprints": column_fingerprints,
        "dataset_fingerprint": dataset_fingerprint,
    }


def write_expected_fingerprint_json(
    expected_fingerprint: Mapping[str, Any],
    output_dir: str | Path,
    *,
    config_id: Optional[str] = None,
) -> Path:
    """
    Persiste el fingerprint esperado del caso como JSON.
    """
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    resolved_config_id = config_id or str(expected_fingerprint.get("config_id", "unknown"))
    path = output_root / f"{resolved_config_id}.json"
    path.write_text(
        json.dumps(expected_fingerprint, ensure_ascii=False, indent=2, sort_keys=False),
        encoding="utf-8",
    )
    return path


# -----------------------------------------------------------------------------
# Construcción del DataFrame
# -----------------------------------------------------------------------------


def _build_core_columns(
    config: ExperimentConfig,
    rng: np.random.Generator,
    controlled_domains: Mapping[str, Sequence[str]],
) -> pd.DataFrame:
    n = config.n_rows
    row_idx = np.arange(n, dtype=np.int64)

    movement_id = pd.Series(
        np.char.mod("m%09d", row_idx),
        dtype="string",
        name="movement_id",
    )
    user_id = pd.Series(
        np.char.mod("u%07d", row_idx % max(10_000, min(max(n // 5, 1), 250_000))),
        dtype="string",
        name="user_id",
    )
    trip_id = pd.Series(
        np.char.mod("t%09d", row_idx),
        dtype="string",
        name="trip_id",
    )
    movement_seq = pd.Series(np.zeros(n, dtype=np.int16), name="movement_seq")

    # Coordenadas OD plausibles en torno a Santiago; deterministas vía RNG.
    origin_lon = pd.Series(rng.uniform(-70.80, -70.45, size=n).astype(np.float64), name="origin_longitude")
    origin_lat = pd.Series(rng.uniform(-33.62, -33.30, size=n).astype(np.float64), name="origin_latitude")

    dest_lon_raw = origin_lon.to_numpy() + rng.normal(loc=0.0, scale=0.035, size=n)
    dest_lat_raw = origin_lat.to_numpy() + rng.normal(loc=0.0, scale=0.025, size=n)
    destination_lon = pd.Series(np.clip(dest_lon_raw, -70.85, -70.40).astype(np.float64), name="destination_longitude")
    destination_lat = pd.Series(np.clip(dest_lat_raw, -33.68, -33.20).astype(np.float64), name="destination_latitude")

    origin_h3 = pd.Series(
        [
            _latlon_to_h3(lat=float(lat), lon=float(lon), resolution=config.h3_resolution)
            for lat, lon in zip(origin_lat.to_numpy(), origin_lon.to_numpy(), strict=False)
        ],
        dtype="string",
        name="origin_h3_index",
    )
    destination_h3 = pd.Series(
        [
            _latlon_to_h3(lat=float(lat), lon=float(lon), resolution=config.h3_resolution)
            for lat, lon in zip(destination_lat.to_numpy(), destination_lon.to_numpy(), strict=False)
        ],
        dtype="string",
        name="destination_h3_index",
    )

    base_time = pd.Timestamp("2024-01-01 00:00:00")
    origin_minutes = (row_idx * 5) % (365 * 24 * 60)
    duration_minutes = rng.integers(5, 95, size=n, dtype=np.int32)
    origin_time_utc = pd.Series(base_time + pd.to_timedelta(origin_minutes, unit="m"), name="origin_time_utc")
    destination_time_utc = pd.Series(
        origin_time_utc.to_numpy() + pd.to_timedelta(duration_minutes, unit="m"),
        name="destination_time_utc",
    )

    trip_weight = pd.Series(
        rng.uniform(0.5, 3.5, size=n).astype(np.float32),
        name="trip_weight",
    )

    return pd.DataFrame(
        {
            "movement_id": movement_id,
            "user_id": user_id,
            "origin_longitude": origin_lon,
            "origin_latitude": origin_lat,
            "destination_longitude": destination_lon,
            "destination_latitude": destination_lat,
            "origin_h3_index": origin_h3,
            "destination_h3_index": destination_h3,
            "origin_time_utc": origin_time_utc,
            "destination_time_utc": destination_time_utc,
            "trip_id": trip_id,
            "movement_seq": movement_seq,
            "trip_weight": trip_weight,
        }
    )


def _build_fixed_categoricals(n_rows: int) -> pd.DataFrame:
    idx = np.arange(n_rows, dtype=np.int64)
    mode = pd.Categorical.from_codes(
        codes=(idx % len(FIXED_MODE_VALUES)).astype(np.int16),
        categories=list(FIXED_MODE_VALUES),
        ordered=False,
    )
    purpose = pd.Categorical.from_codes(
        codes=((idx * 3) % len(FIXED_PURPOSE_VALUES)).astype(np.int16),
        categories=list(FIXED_PURPOSE_VALUES),
        ordered=False,
    )
    day_type = pd.Categorical.from_codes(
        codes=(idx % len(FIXED_DAY_TYPE_VALUES)).astype(np.int8),
        categories=list(FIXED_DAY_TYPE_VALUES),
        ordered=False,
    )
    time_period = pd.Categorical.from_codes(
        codes=((idx * 5) % len(FIXED_TIME_PERIOD_VALUES)).astype(np.int16),
        categories=list(FIXED_TIME_PERIOD_VALUES),
        ordered=False,
    )
    return pd.DataFrame(
        {
            "mode": pd.Series(mode),
            "purpose": pd.Series(purpose),
            "day_type": pd.Series(day_type),
            "time_period": pd.Series(time_period),
        }
    )


def _build_controlled_categoricals(
    n_rows: int,
    controlled_domains: Mapping[str, Sequence[str]],
) -> pd.DataFrame:
    idx = np.arange(n_rows, dtype=np.int64)
    values_a = list(controlled_domains["exp_cat_a_k"])
    values_b = list(controlled_domains["exp_cat_b_k"])
    cat_a = pd.Categorical.from_codes(
        codes=(idx % len(values_a)).astype(np.int32),
        categories=values_a,
        ordered=False,
    )
    cat_b = pd.Categorical.from_codes(
        codes=((idx * 7) % len(values_b)).astype(np.int32),
        categories=values_b,
        ordered=False,
    )
    return pd.DataFrame(
        {
            "exp_cat_a_k": pd.Series(cat_a),
            "exp_cat_b_k": pd.Series(cat_b),
        }
    )


def _build_extra_columns_until_n(
    *,
    n_rows: int,
    n_extra_columns: int,
    rng: np.random.Generator,
) -> tuple[pd.DataFrame, List[FieldSpec]]:
    if n_extra_columns < 0:
        raise ValueError("n_extra_columns no puede ser negativo")
    if n_extra_columns == 0:
        return pd.DataFrame(index=pd.RangeIndex(n_rows)), []

    idx = np.arange(n_rows, dtype=np.int64)
    data: Dict[str, Any] = {}
    specs: List[FieldSpec] = []

    for extra_idx in range(n_extra_columns):
        dtype_name = EXTRA_DTYPE_CYCLE[extra_idx % len(EXTRA_DTYPE_CYCLE)]
        column_name = f"extra_{dtype_name}_{extra_idx + 1:03d}"

        if dtype_name == "float":
            series = pd.Series(
                rng.normal(loc=float((extra_idx % 11) - 5), scale=1.25, size=n_rows).astype(np.float32),
                name=column_name,
            )
            spec = FieldSpec(
                name=column_name,
                dtype="float",
                required=False,
                constraints={"nullable": False},
            )
        elif dtype_name == "int":
            series = pd.Series(
                rng.integers(0, 100_000 + extra_idx, size=n_rows, dtype=np.int32),
                name=column_name,
            )
            spec = FieldSpec(
                name=column_name,
                dtype="int",
                required=False,
                constraints={"nullable": False},
            )
        elif dtype_name == "bool":
            raw = rng.integers(0, 2, size=n_rows, dtype=np.int8).astype(bool)
            series = pd.Series(pd.array(raw, dtype="boolean"), name=column_name)
            spec = FieldSpec(
                name=column_name,
                dtype="bool",
                required=False,
                constraints={"nullable": False},
            )
        elif dtype_name == "datetime":
            base = pd.Timestamp("2024-01-01 00:00:00") + pd.to_timedelta(extra_idx, unit="h")
            series = pd.Series(
                base + pd.to_timedelta((idx * ((extra_idx % 13) + 1)) % 100_000, unit="m"),
                name=column_name,
            )
            spec = FieldSpec(
                name=column_name,
                dtype="datetime",
                required=False,
                constraints={"nullable": False},
            )
        elif dtype_name == "string":
            cardinality = 32
            domain = [f"estr_{extra_idx + 1:03d}_{i:02d}" for i in range(cardinality)]
            values = np.asarray(domain, dtype=object)[idx % cardinality]
            series = pd.Series(pd.array(values, dtype="string"), name=column_name)
            spec = FieldSpec(
                name=column_name,
                dtype="string",
                required=False,
                constraints={"nullable": False},
            )
        else:  # pragma: no cover - protegido por EXTRA_DTYPE_CYCLE
            raise ValueError(f"dtype extra no soportado: {dtype_name!r}")

        data[column_name] = series
        specs.append(spec)

    return pd.DataFrame(data), specs


# -----------------------------------------------------------------------------
# Schema y metadata
# -----------------------------------------------------------------------------


def _build_schema(
    *,
    controlled_domains: Mapping[str, Sequence[str]],
    extra_field_specs: Sequence[FieldSpec],
) -> TripSchema:
    fields: Dict[str, FieldSpec] = {}

    def add_field(spec: FieldSpec) -> None:
        fields[spec.name] = spec

    # Núcleo mínimo canónico de trips para el experimento.
    add_field(FieldSpec("movement_id", "string", required=True, constraints={"nullable": False, "unique": True}))
    add_field(FieldSpec("user_id", "string", required=True, constraints={"nullable": False}))
    add_field(FieldSpec("origin_longitude", "float", required=True, constraints={"nullable": False, "range": {"min": -180.0, "max": 180.0}}))
    add_field(FieldSpec("origin_latitude", "float", required=True, constraints={"nullable": False, "range": {"min": -90.0, "max": 90.0}}))
    add_field(FieldSpec("destination_longitude", "float", required=True, constraints={"nullable": False, "range": {"min": -180.0, "max": 180.0}}))
    add_field(FieldSpec("destination_latitude", "float", required=True, constraints={"nullable": False, "range": {"min": -90.0, "max": 90.0}}))
    add_field(FieldSpec("origin_h3_index", "string", required=True, constraints={"nullable": False}))
    add_field(FieldSpec("destination_h3_index", "string", required=True, constraints={"nullable": False}))
    add_field(FieldSpec("origin_time_utc", "datetime", required=True, constraints={"nullable": False}))
    add_field(FieldSpec("destination_time_utc", "datetime", required=True, constraints={"nullable": False}))
    add_field(FieldSpec("trip_id", "string", required=True, constraints={"nullable": False}))
    add_field(FieldSpec("movement_seq", "int", required=True, constraints={"nullable": False, "range": {"min": 0}}))

    add_field(FieldSpec("trip_weight", "float", required=False, constraints={"nullable": False, "range": {"min": 0.0}}))

    add_field(
        FieldSpec(
            "mode",
            "categorical",
            required=False,
            constraints={"nullable": False},
            domain=DomainSpec(values=list(FIXED_MODE_VALUES), extendable=False),
        )
    )
    add_field(
        FieldSpec(
            "purpose",
            "categorical",
            required=False,
            constraints={"nullable": False},
            domain=DomainSpec(values=list(FIXED_PURPOSE_VALUES), extendable=False),
        )
    )
    add_field(
        FieldSpec(
            "day_type",
            "categorical",
            required=False,
            constraints={"nullable": False},
            domain=DomainSpec(values=list(FIXED_DAY_TYPE_VALUES), extendable=False),
        )
    )
    add_field(
        FieldSpec(
            "time_period",
            "categorical",
            required=False,
            constraints={"nullable": False},
            domain=DomainSpec(values=list(FIXED_TIME_PERIOD_VALUES), extendable=False),
        )
    )
    add_field(
        FieldSpec(
            "exp_cat_a_k",
            "categorical",
            required=False,
            constraints={"nullable": False},
            domain=DomainSpec(values=list(controlled_domains["exp_cat_a_k"]), extendable=False),
        )
    )
    add_field(
        FieldSpec(
            "exp_cat_b_k",
            "categorical",
            required=False,
            constraints={"nullable": False},
            domain=DomainSpec(values=list(controlled_domains["exp_cat_b_k"]), extendable=False),
        )
    )

    for extra_spec in extra_field_specs:
        add_field(extra_spec)

    required = [
        "movement_id",
        "user_id",
        "origin_longitude",
        "origin_latitude",
        "destination_longitude",
        "destination_latitude",
        "origin_h3_index",
        "destination_h3_index",
        "origin_time_utc",
        "destination_time_utc",
        "trip_id",
        "movement_seq",
    ]

    return TripSchema(
        version=DEFAULT_SCHEMA_VERSION,
        fields=fields,
        required=required,
        semantic_rules={
            "generator": "generate_experiment_case",
            "notes": "schema experimental sintético para benchmark de persistencia",
        },
    )


def _build_schema_effective(schema: TripSchema, df: pd.DataFrame) -> TripSchemaEffective:
    dtype_effective = {
        column: schema.fields[column].dtype if column in schema.fields else _infer_logical_dtype_from_series(df[column])
        for column in df.columns
    }

    domains_effective: Dict[str, Any] = {}
    for field_name, field_spec in schema.fields.items():
        if field_spec.dtype == "categorical":
            categories = _extract_categories(df[field_name]) if field_name in df.columns else list(field_spec.domain.values if field_spec.domain else [])
            domains_effective[field_name] = {
                "values": categories,
                "extended": False,
                "added_values": [],
                "strict_applied": True,
            }

    return TripSchemaEffective(
        dtype_effective=dtype_effective,
        overrides={},
        domains_effective=domains_effective,
        temporal={
            "tier": "tier_1",
            "fields_present": ["origin_time_utc", "destination_time_utc"],
            "normalization": "already_utc",
        },
        fields_effective=list(df.columns),
    )


def _build_initial_metadata(config: ExperimentConfig, seed: int) -> Dict[str, Any]:
    return {
        "is_validated": True,
        "events": [],
        "temporal": {
            "tier": "tier_1",
            "fields_present": ["origin_time_utc", "destination_time_utc"],
            "normalization": "already_utc",
            "source_timezone_used": "UTC",
        },
        "experiment_case": {
            "config_id": config.config_id,
            "seed": int(seed),
            "n_rows": int(config.n_rows),
            "n_cols": int(config.n_cols),
            "k": int(config.k),
            "h3_resolution": int(config.h3_resolution),
        },
    }


def _build_provenance(config: ExperimentConfig, seed: int) -> Dict[str, Any]:
    return {
        "source_name": "synthetic_persistence_experiment",
        "config_id": config.config_id,
        "seed": int(seed),
        "generator": "generate_experiment_case",
        "version": "v1",
    }


def _build_manifest(
    *,
    config: ExperimentConfig,
    seed: int,
    df: pd.DataFrame,
    schema: TripSchema,
    schema_effective: TripSchemaEffective,
    expected_fingerprint: Mapping[str, Any],
) -> Dict[str, Any]:
    dtype_counts: Dict[str, int] = {}
    for column in df.columns:
        logical_dtype = schema_effective.dtype_effective.get(column, "unknown")
        dtype_counts[logical_dtype] = dtype_counts.get(logical_dtype, 0) + 1

    fixed_categorical_fields = ["mode", "purpose", "day_type", "time_period"]
    controlled_categorical_fields = ["exp_cat_a_k", "exp_cat_b_k"]

    return {
        "config_id": config.config_id,
        "seed": int(seed),
        "n_rows": int(config.n_rows),
        "n_cols": int(config.n_cols),
        "k": int(config.k),
        "h3_resolution": int(config.h3_resolution),
        "schema_version": schema.version,
        "columns_order": list(df.columns),
        "n_base_columns": 19,
        "n_extra_columns": int(config.n_cols - 19),
        "dtype_counts": dtype_counts,
        "fixed_categorical_fields": fixed_categorical_fields,
        "controlled_categorical_fields": controlled_categorical_fields,
        "controlled_cardinality_per_field": {
            field_name: int(config.k)
            for field_name in controlled_categorical_fields
        },
        "fingerprint_dataset": expected_fingerprint["dataset_fingerprint"],
    }


# -----------------------------------------------------------------------------
# Helpers de dominios y H3
# -----------------------------------------------------------------------------


def _build_controlled_domains(k: int) -> Dict[str, List[str]]:
    return {
        "exp_cat_a_k": [f"catA_{i:0{CONTROLLED_LABEL_WIDTH}d}" for i in range(k)],
        "exp_cat_b_k": [f"catB_{i:0{CONTROLLED_LABEL_WIDTH}d}" for i in range(k)],
    }


def _latlon_to_h3(*, lat: float, lon: float, resolution: int) -> str:
    if h3 is not None:  # pragma: no cover - depende del entorno
        if hasattr(h3, "latlng_to_cell"):
            return str(h3.latlng_to_cell(lat, lon, resolution))
        if hasattr(h3, "geo_to_h3"):
            return str(h3.geo_to_h3(lat, lon, resolution))

    # Fallback determinista, suficiente para benchmark de persistencia.
    lat_bucket = int(round((lat + 90.0) * 1_000))
    lon_bucket = int(round((lon + 180.0) * 1_000))
    return f"exp_h3_r{resolution:02d}_{lat_bucket:06d}_{lon_bucket:06d}"


# -----------------------------------------------------------------------------
# Helpers de fingerprint
# -----------------------------------------------------------------------------


def _series_fingerprint(series: pd.Series) -> str:
    normalized = _normalize_series_for_fingerprint(series)
    row_hashes = pd.util.hash_pandas_object(normalized, index=False, categorize=False)
    payload = row_hashes.to_numpy(dtype="uint64", copy=False).tobytes()
    return hashlib.sha256(payload).hexdigest()


def _normalize_series_for_fingerprint(series: pd.Series) -> pd.Series:
    if is_categorical_dtype(series):
        categories = _extract_categories(series)
        cat_series = series.astype(pd.CategoricalDtype(categories=categories, ordered=False))
        return pd.Series(cat_series.astype("string").fillna("<NA>"), name=series.name, dtype="string")

    if is_datetime64_any_dtype(series):
        dt = pd.to_datetime(series, errors="coerce")
        # Se usa representación int64 con sentinela estable para NaT.
        values = dt.astype("int64", copy=False)
        return pd.Series(values, name=series.name, dtype="Int64")

    if is_bool_dtype(series):
        return pd.Series(series.astype("boolean"), name=series.name)

    if is_integer_dtype(series):
        return pd.Series(series.astype("Int64"), name=series.name)

    if is_float_dtype(series):
        return pd.Series(series.astype("Float64"), name=series.name)

    return pd.Series(series.astype("string").fillna("<NA>"), name=series.name, dtype="string")


# -----------------------------------------------------------------------------
# Helpers generales
# -----------------------------------------------------------------------------


def _validate_config(config: ExperimentConfig) -> None:
    if config.n_rows <= 0:
        raise ValueError("config.n_rows debe ser > 0")
    if config.n_cols <= 0:
        raise ValueError("config.n_cols debe ser > 0")
    if config.k <= 0:
        raise ValueError("config.k debe ser > 0")

    min_columns = 19
    if config.n_cols < min_columns:
        raise ValueError(
            f"config.n_cols={config.n_cols} es insuficiente; el generador requiere al menos {min_columns} columnas base"
        )


def _extract_categories(series: pd.Series) -> List[str]:
    if is_categorical_dtype(series):
        return [str(value) for value in series.cat.categories.tolist()]
    unique_values = pd.Index(series.dropna().astype("string").unique().tolist())
    return [str(value) for value in unique_values.tolist()]


def _infer_logical_dtype_from_series(series: pd.Series) -> str:
    if is_categorical_dtype(series):
        return "categorical"
    if is_datetime64_any_dtype(series):
        return "datetime"
    if is_bool_dtype(series):
        return "bool"
    if is_integer_dtype(series):
        return "int"
    if is_float_dtype(series):
        return "float"
    return "string"


