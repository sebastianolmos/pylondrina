"""
Shared pytest fixtures and helper utilities for OP-06 `write_trips` tests.

The fixtures in this file are intentionally function-scoped by default. OP-06
mutates `TripDataset.metadata` after a successful write by aligning `dataset_id`,
generating a new `artifact_id`, and appending a `write_trips` event. Returning a
fresh object per test avoids cross-test contamination.
"""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import pytest

from pylondrina.datasets import TripDataset
from pylondrina.importing import ImportOptions, import_trips_from_dataframe
from pylondrina.schema import DomainSpec, FieldSpec, TripSchema, TripSchemaEffective
from pylondrina.validation import ValidationOptions, validate_trips
from pylondrina.io.trips import (
    WriteTripsOptions,
    _build_storage_options_snapshot,
    _trip_data_filename_for_storage,
    _trip_schema_effective_to_snapshot,
    _trip_schema_to_snapshot,
)


# -----------------------------------------------------------------------------
# Optional local repository path support
# -----------------------------------------------------------------------------

def _find_repo_root(start: Path) -> Path:
    """Return the nearest repository root that exposes `src/pylondrina`."""
    start = start.resolve()
    for candidate in [start, *start.parents]:
        if (candidate / "src" / "pylondrina").exists():
            return candidate
        if (candidate / "scripts" / "synthetic_data" / "base_generator.py").exists():
            return candidate
    return start


REPO_ROOT = _find_repo_root(Path(__file__).resolve())
SRC_ROOT = REPO_ROOT / "src"

for _path in (SRC_ROOT, REPO_ROOT):
    if _path.exists() and str(_path) not in sys.path:
        sys.path.insert(0, str(_path))


# -----------------------------------------------------------------------------
# Constants used by rich integration fixtures
# -----------------------------------------------------------------------------

REQUIRED_FIELDS_ORDER = [
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

REQUIRED_FIELD_DTYPES = {
    "movement_id": "string",
    "user_id": "string",
    "origin_longitude": "float",
    "origin_latitude": "float",
    "destination_longitude": "float",
    "destination_latitude": "float",
    "origin_h3_index": "string",
    "destination_h3_index": "string",
    "origin_time_utc": "datetime",
    "destination_time_utc": "datetime",
    "trip_id": "string",
    "movement_seq": "int",
}

BASE_FIELD_DTYPES = {
    "origin_municipality": "string",
    "destination_municipality": "string",
    "timezone_offset_min": "int",
    "origin_time_local_hhmm": "string",
    "destination_time_local_hhmm": "string",
    "trip_weight": "float",
    "mode_sequence": "string",
    "mode": "categorical",
    "purpose": "categorical",
    "day_type": "categorical",
    "time_period": "categorical",
    "user_gender": "categorical",
    "user_age_group": "categorical",
    "income_quintile": "categorical",
}

CANONICAL_DOMAINS = {
    "mode": [
        "walk",
        "bicycle",
        "scooter",
        "motorcycle",
        "car",
        "taxi",
        "ride_hailing",
        "bus",
        "metro",
        "train",
        "other",
    ],
    "purpose": [
        "home",
        "work",
        "education",
        "shopping",
        "errand",
        "health",
        "leisure",
        "transfer",
        "other",
    ],
    "day_type": ["weekday", "weekend", "holiday"],
    "time_period": ["night", "morning", "midday", "afternoon", "evening"],
    "user_gender": ["female", "male", "other", "unknown"],
    "user_age_group": [
        "0-14",
        "15-24",
        "25-34",
        "35-44",
        "45-54",
        "55-64",
        "65-plus",
        "unknown",
    ],
    "income_quintile": ["1", "2", "3", "4", "5", "unknown"],
}

RICH_BASE_FIELDS = [
    "origin_municipality",
    "destination_municipality",
    "timezone_offset_min",
    "origin_time_local_hhmm",
    "destination_time_local_hhmm",
    "trip_weight",
    "mode_sequence",
    "mode",
    "purpose",
    "day_type",
    "time_period",
    "user_gender",
    "user_age_group",
    "income_quintile",
]

RICH_EXTRA_COLUMNS = [
    "activity_status",
    "education_level",
    "travel_time_bucket",
    "season",
    "fare_payment_type",
    "bike_lane_usage",
    "home_tenure",
]


# -----------------------------------------------------------------------------
# Assertion and inspection helper functions
# -----------------------------------------------------------------------------

def get_issue_codes(issues: list[Any]) -> list[str]:
    """Return issue codes from Issue objects or dict-like issue payloads."""
    return [issue.code if hasattr(issue, "code") else issue.get("code") for issue in issues]


def assert_issue_present(issues: list[Any], code: str) -> None:
    """Assert that an expected issue code is present in an issue collection."""
    codes = get_issue_codes(issues)
    assert code in codes, f"No se encontró el issue {code}. Codes actuales: {codes}"


def assert_issue_absent(issues: list[Any], code: str) -> None:
    """Assert that an issue code is absent from an issue collection."""
    codes = get_issue_codes(issues)
    assert code not in codes, f"Se encontró inesperadamente {code}. Codes actuales: {codes}"


def assert_json_safe(obj: Any, label: str = "object") -> None:
    """Assert that an object can be serialized as JSON without custom encoders."""
    try:
        json.dumps(obj, ensure_ascii=False)
    except Exception as exc:
        raise AssertionError(f"{label} no es JSON-safe: {exc}") from exc


def load_sidecar(artifact_dir: Path) -> dict[str, Any]:
    """Load the formal trips sidecar from an artifact directory."""
    sidecar_path = artifact_dir / "trips.metadata.json"
    assert sidecar_path.exists(), f"No existe sidecar: {sidecar_path}"
    return json.loads(sidecar_path.read_text(encoding="utf-8"))


def load_trips_sidecar(artifact_dir: Path) -> dict[str, Any]:
    """Alias for loading `trips.metadata.json` in integration tests."""
    return load_sidecar(artifact_dir)


def artifact_data_filename(storage_format: str) -> str:
    """Return the contract file name for the requested trips storage backend."""
    if storage_format == "parquet":
        return "trips.parquet"
    if storage_format == "feather":
        return "trips.feather"
    raise ValueError(f"storage_format no soportado: {storage_format!r}")


def artifact_data_file_path(artifact_dir: Path, storage_format: str) -> Path:
    """Return the expected data file path inside a trips artifact directory."""
    return artifact_dir / artifact_data_filename(storage_format)


def artifact_total_size_bytes(artifact_dir: Path) -> int:
    """Return the total size of files contained in an artifact directory."""
    return sum(path.stat().st_size for path in artifact_dir.rglob("*") if path.is_file())


def selected_categorical_columns(df: pd.DataFrame | None = None) -> list[str]:
    """Return categorical columns used by the OP-06 regression/integration tests."""
    cols = [
        "mode",
        "purpose",
        "day_type",
        "time_period",
        "user_gender",
        "user_age_group",
        "income_quintile",
    ]
    if df is None:
        return cols
    return [col for col in cols if col in df.columns]


def series_as_string_with_na(series: pd.Series) -> pd.Series:
    """Convert a series to pandas string dtype while preserving missing values."""
    return series.astype("string")


def clone_tripdataset(trips: TripDataset) -> TripDataset:
    """Return a deep copy of a TripDataset for mutation-safe test setup."""
    return copy.deepcopy(trips)


# -----------------------------------------------------------------------------
# Factory helpers
# -----------------------------------------------------------------------------

def _make_field(
    name: str,
    dtype: str,
    *,
    required: bool = False,
    constraints: dict[str, Any] | None = None,
    domain: DomainSpec | None = None,
) -> FieldSpec:
    """Build a FieldSpec with the same shape used in the OP-06 notebooks."""
    return FieldSpec(
        name=name,
        dtype=dtype,
        required=required,
        constraints=constraints,
        domain=domain,
    )


def _make_trip_schema(fields: list[FieldSpec], *, version: str = "1.1") -> TripSchema:
    """Build a TripSchema from an ordered list of field specs."""
    return TripSchema(
        version=version,
        fields={field.name: field for field in fields},
        required=[field.name for field in fields if field.required],
        semantic_rules=None,
    )


def _make_trip_schema_effective(
    *,
    dtype_effective: dict[str, str] | None = None,
    overrides: dict[str, dict[str, Any]] | None = None,
    domains_effective: dict[str, Any] | None = None,
    temporal: dict[str, Any] | None = None,
    fields_effective: list[str] | None = None,
) -> TripSchemaEffective:
    """Build a TripSchemaEffective with explicit defaults."""
    return TripSchemaEffective(
        dtype_effective=dtype_effective or {},
        overrides=overrides or {},
        domains_effective=domains_effective or {},
        temporal=temporal or {},
        fields_effective=fields_effective or [],
    )


def _make_trip_df_minimal() -> pd.DataFrame:
    """Build the minimal synthetic trips dataframe used by OP-06 notebooks."""
    return pd.DataFrame(
        {
            "movement_id": ["m1", "m2", "m3"],
            "trip_id": ["t1", "t2", "t3"],
            "movement_seq": [0, 0, 0],
            "user_id": ["u1", "u2", "u3"],
            "origin_latitude": [-33.45, -33.46, -33.47],
            "origin_longitude": [-70.66, -70.67, -70.68],
            "destination_latitude": [-33.41, -33.42, -33.43],
            "destination_longitude": [-70.61, -70.62, -70.63],
            "mode": ["bus", "metro", "bus"],
            "purpose": ["work", "study", "work"],
            "comment": ["a", "b", "c"],
            "trip_weight": [1.0, 2.5, 1.2],
        }
    )


def _make_trip_schema_minimal(*, version: str = "1.1") -> TripSchema:
    """Build the minimal TripSchema used across OP-06 notebooks."""
    return _make_trip_schema(
        [
            _make_field("movement_id", "string", required=True),
            _make_field("trip_id", "string", required=True),
            _make_field("movement_seq", "int", required=True),
            _make_field("user_id", "string", required=True),
            _make_field("origin_latitude", "float", required=True),
            _make_field("origin_longitude", "float", required=True),
            _make_field("destination_latitude", "float", required=True),
            _make_field("destination_longitude", "float", required=True),
            _make_field(
                "mode",
                "categorical",
                domain=DomainSpec(values=["bus", "metro", "walk", "car"], extendable=True),
            ),
            _make_field(
                "purpose",
                "categorical",
                domain=DomainSpec(values=["work", "study", "health"], extendable=True),
            ),
            _make_field("comment", "string"),
            _make_field("trip_weight", "float"),
        ],
        version=version,
    )


def _make_trip_schema_effective_minimal() -> TripSchemaEffective:
    """Build the minimal effective schema used by OP-06 write fixtures."""
    return _make_trip_schema_effective(
        dtype_effective={
            "mode": "categorical",
            "purpose": "categorical",
            "trip_weight": "float",
        },
        domains_effective={
            "mode": {"values": ["bus", "metro", "walk", "car"]},
            "purpose": {"values": ["work", "study", "health"]},
        },
        temporal={"tier": "tier_3"},
        fields_effective=[
            "movement_id",
            "trip_id",
            "movement_seq",
            "user_id",
            "origin_latitude",
            "origin_longitude",
            "destination_latitude",
            "destination_longitude",
            "mode",
            "purpose",
            "comment",
            "trip_weight",
        ],
    )


def _make_tripdataset(
    *,
    validated: bool = True,
    include_dataset_id: bool = True,
    include_artifact_id: bool = False,
) -> TripDataset:
    """Build a fresh minimal TripDataset suitable for OP-06 write tests."""
    schema = _make_trip_schema_minimal()
    schema_effective = _make_trip_schema_effective_minimal()

    metadata: dict[str, Any] = {
        "is_validated": validated,
        "events": [],
        "mappings": {
            "field_correspondence": {
                "movement_id": "movement_id_src",
                "mode": "mode_src",
            },
            "value_correspondence": {
                "mode": {
                    "micro": "bus",
                    "subte": "metro",
                }
            },
        },
        "domains_effective": copy.deepcopy(schema_effective.domains_effective),
        "temporal": {"tier": "tier_3"},
    }

    if include_dataset_id:
        metadata["dataset_id"] = "dset_test_001"
    if include_artifact_id:
        metadata["artifact_id"] = "art_test_001"

    provenance = {
        "source": {"name": "synthetic", "entity": "trips"},
        "ingestion": {"created_at_utc": "2026-04-04T00:00:00Z"},
    }

    return TripDataset(
        data=_make_trip_df_minimal(),
        schema=schema,
        schema_version=schema.version,
        provenance=provenance,
        field_correspondence={"movement_id": "movement_id_src", "mode": "mode_src"},
        value_correspondence={"mode": {"micro": "bus", "subte": "metro"}},
        metadata=metadata,
        schema_effective=schema_effective,
    )


def _make_write_options(
    *,
    storage_format: str = "parquet",
    mode: str = "error_if_exists",
    require_validated: bool = True,
    parquet_compression: str | None = "snappy",
    feather_compression: str | None = "lz4",
    normalize_artifact_dir: bool = True,
) -> WriteTripsOptions:
    """Build WriteTripsOptions with explicit OP-06 defaults."""
    return WriteTripsOptions(
        mode=mode,
        require_validated=require_validated,
        storage_format=storage_format,
        parquet_compression=parquet_compression,
        feather_compression=feather_compression,
        normalize_artifact_dir=normalize_artifact_dir,
    )


def _make_sidecar_payload(
    *,
    schema: TripSchema | None = None,
    schema_effective: TripSchemaEffective | None = None,
    metadata: dict[str, Any] | None = None,
    provenance: dict[str, Any] | None = None,
    dataset_id: str = "dset_sidecar_001",
    artifact_id: str = "art_sidecar_001",
    storage_format: str = "parquet",
    parquet_compression: str | None = "snappy",
    feather_compression: str | None = "lz4",
) -> dict[str, Any]:
    """Build a formal trips sidecar payload for helper-level write tests."""
    schema = schema or _make_trip_schema_minimal()
    schema_effective = schema_effective or _make_trip_schema_effective_minimal()
    options = _make_write_options(
        storage_format=storage_format,
        parquet_compression=parquet_compression,
        feather_compression=feather_compression,
    )

    metadata_for_payload = copy.deepcopy(metadata) if metadata is not None else {
        "dataset_id": dataset_id,
        "artifact_id": artifact_id,
        "is_validated": True,
        "events": [],
        "mappings": {
            "field_correspondence": {"movement_id": "movement_id_src"},
            "value_correspondence": {"mode": {"micro": "bus"}},
        },
        "domains_effective": copy.deepcopy(schema_effective.domains_effective),
    }

    provenance_for_payload = copy.deepcopy(provenance) if provenance is not None else {
        "source": {"name": "synthetic", "entity": "trips"},
        "ingestion": {"created_at_utc": "2026-04-04T00:00:00Z"},
    }

    return {
        "dataset_type": "trips",
        "format": "golondrina",
        "layout_version": "1.1",
        "storage": {
            "format": storage_format,
            "options": _build_storage_options_snapshot(options),
        },
        "dataset_id": dataset_id,
        "artifact_id": artifact_id,
        "files": {
            "data": _trip_data_filename_for_storage(storage_format),
            "metadata": "trips.metadata.json",
        },
        "schema": _trip_schema_to_snapshot(schema),
        "schema_effective": _trip_schema_effective_to_snapshot(schema_effective),
        "provenance": provenance_for_payload,
        "metadata": metadata_for_payload,
    }


def _make_rich_trip_schema() -> TripSchema:
    """Build the rich TripSchema used by OP-06 integration notebooks."""
    fields: dict[str, FieldSpec] = {}

    for field_name in REQUIRED_FIELDS_ORDER:
        fields[field_name] = _make_field(
            field_name,
            REQUIRED_FIELD_DTYPES[field_name],
            required=True,
        )

    for field_name, dtype_name in BASE_FIELD_DTYPES.items():
        domain = None
        if dtype_name == "categorical":
            domain = DomainSpec(values=CANONICAL_DOMAINS[field_name], extendable=True)
        fields[field_name] = _make_field(field_name, dtype_name, domain=domain)

    return TripSchema(
        version="1.1",
        fields=fields,
        required=list(REQUIRED_FIELDS_ORDER),
        semantic_rules=None,
    )


def _build_rich_source_dataframe(seed: int = 20260404, filas: int = 180) -> pd.DataFrame:
    """Build the rich synthetic source dataframe used by OP-06 integration tests."""
    from scripts.synthetic_data.base_generator import generate_synthetic_trip_dataframe

    return generate_synthetic_trip_dataframe(
        filas=filas,
        seed=seed,
        duplicate_mode="none",
        tier_temporal="tier_1",
        tier1_datetime_format="utc_string_z",
        coord_format="numeric",
        h3_mode="provided_valid",
        trip_structure="multistage",
        max_movements_per_trip=3,
        base_fields=RICH_BASE_FIELDS,
        extra_value_domains={
            "mode": ["canon"],
            "purpose": ["canon"],
            "day_type": ["canon"],
            "time_period": ["canon"],
            "user_gender": ["canon"],
            "user_age_group": ["canon"],
            "income_quintile": ["canon"],
        },
        extra_columns=RICH_EXTRA_COLUMNS,
        null_ratio={
            "origin_municipality": 0.03,
            "destination_municipality": 0.03,
        },
    )


# -----------------------------------------------------------------------------
# Path fixtures
# -----------------------------------------------------------------------------

@pytest.fixture
def artifact_root(tmp_path: Path) -> Path:
    """Provide a clean per-test artifact root below pytest's tmp_path."""
    return tmp_path / "artifact"


@pytest.fixture
def artifact_base_path(tmp_path: Path) -> Path:
    """Provide a clean per-test base path that can be normalized to `.golondrina`."""
    return tmp_path / "artifact_bundle"


# -----------------------------------------------------------------------------
# Callable factory fixtures
# -----------------------------------------------------------------------------

@pytest.fixture
def make_field_spec() -> Callable[..., FieldSpec]:
    """Provide a callable factory for FieldSpec objects."""
    return _make_field


@pytest.fixture
def make_trip_schema() -> Callable[..., TripSchema]:
    """Provide a callable factory for TripSchema objects."""
    return _make_trip_schema


@pytest.fixture
def make_trip_schema_effective() -> Callable[..., TripSchemaEffective]:
    """Provide a callable factory for TripSchemaEffective objects."""
    return _make_trip_schema_effective


@pytest.fixture
def make_tripdataset() -> Callable[..., TripDataset]:
    """Provide a callable factory for fresh minimal TripDataset objects."""
    return _make_tripdataset


@pytest.fixture
def make_write_options() -> Callable[..., WriteTripsOptions]:
    """Provide a callable factory for WriteTripsOptions."""
    return _make_write_options


@pytest.fixture
def make_sidecar_payload() -> Callable[..., dict[str, Any]]:
    """Provide a callable factory for formal trips sidecar payloads."""
    return _make_sidecar_payload


# -----------------------------------------------------------------------------
# Minimal TripDataset fixtures
# -----------------------------------------------------------------------------

@pytest.fixture
def trip_df_minimal() -> pd.DataFrame:
    """Provide a fresh minimal trips dataframe for OP-06 unit/smoke tests."""
    return _make_trip_df_minimal()


@pytest.fixture
def trip_schema_minimal() -> TripSchema:
    """Provide the minimal TripSchema used by OP-06 notebooks."""
    return _make_trip_schema_minimal()


@pytest.fixture
def trip_schema_effective_minimal() -> TripSchemaEffective:
    """Provide the minimal TripSchemaEffective used by OP-06 notebooks."""
    return _make_trip_schema_effective_minimal()


@pytest.fixture
def trip_dataset_validated() -> TripDataset:
    """Provide a fresh minimal TripDataset marked as validated."""
    return _make_tripdataset(validated=True, include_dataset_id=True)


@pytest.fixture
def trip_dataset_unvalidated() -> TripDataset:
    """Provide a fresh minimal TripDataset marked as not validated."""
    return _make_tripdataset(validated=False, include_dataset_id=True)


@pytest.fixture
def trip_dataset_without_dataset_id() -> TripDataset:
    """Provide a fresh validated TripDataset without dataset_id in metadata."""
    return _make_tripdataset(validated=True, include_dataset_id=False)


# -----------------------------------------------------------------------------
# Rich integration fixtures
# -----------------------------------------------------------------------------

@pytest.fixture
def rich_trip_schema() -> TripSchema:
    """Provide the rich TripSchema used by OP-06 integration tests."""
    return _make_rich_trip_schema()


@pytest.fixture
def rich_source_df() -> pd.DataFrame:
    """Provide the rich synthetic source dataframe used by OP-06 integration tests."""
    return _build_rich_source_dataframe(filas=180)


@pytest.fixture
def rich_tripdataset_canonical(
    rich_source_df: pd.DataFrame,
    rich_trip_schema: TripSchema,
) -> TripDataset:
    """Import the rich source dataframe into an unvalidated TripDataset."""
    trips, import_report = import_trips_from_dataframe(
        rich_source_df,
        rich_trip_schema,
        source_name="synthetic_rich_trips",
        options=ImportOptions(
            keep_extra_fields=True,
            selected_fields=None,
            strict=False,
            strict_domains=False,
            single_stage=False,
            source_timezone=None,
        ),
        provenance={
            "source": {"name": "synthetic_generator", "entity": "trips"},
            "ingestion": {"created_at_utc": "2026-04-04T00:00:00Z"},
            "notes": ["fixture de integración OP-06 write_trips"],
        },
        h3_resolution=8,
    )
    if not import_report.ok:
        raise RuntimeError(f"La fixture rica no pudo importarse: {import_report.issues}")
    return trips


@pytest.fixture
def rich_tripdataset_unvalidated(rich_tripdataset_canonical: TripDataset) -> TripDataset:
    """Provide a fresh imported rich TripDataset that remains unvalidated."""
    return copy.deepcopy(rich_tripdataset_canonical)


@pytest.fixture
def rich_tripdataset_validated(rich_tripdataset_canonical: TripDataset) -> TripDataset:
    """Provide a fresh imported rich TripDataset validated by validate_trips."""
    trips = copy.deepcopy(rich_tripdataset_canonical)
    validation_report = validate_trips(
        trips,
        options=ValidationOptions(
            strict=False,
            validate_domains="full",
        ),
    )
    if not validation_report.ok:
        raise RuntimeError(f"La fixture rica no pudo validarse: {validation_report.issues}")
    return trips