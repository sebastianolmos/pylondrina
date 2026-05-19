from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import pytest


def _ensure_repo_src_on_path() -> None:
    """Agrega src/ al sys.path cuando los tests se ejecutan desde el repositorio."""
    current = Path(__file__).resolve()
    for parent in (current.parent, *current.parents):
        src_dir = parent / "src"
        if (src_dir / "pylondrina").exists():
            src_text = str(src_dir)
            if src_text not in sys.path:
                sys.path.insert(0, src_text)
            return


_ensure_repo_src_on_path()

pa = pytest.importorskip("pyarrow", reason="OP-07 read_trips tests require pyarrow.")
feather = pytest.importorskip("pyarrow.feather", reason="OP-07 read_trips tests require pyarrow.feather.")

from pylondrina.datasets import TripDataset
from pylondrina.io.trips import (
    WriteTripsOptions,
    _resolve_trips_artifact_paths,
    _trip_data_filename_for_storage,
    _trip_schema_effective_to_snapshot,
    _trip_schema_to_snapshot,
    write_trips,
)
from pylondrina.reports import Issue
from pylondrina.schema import DomainSpec, FieldSpec, TripSchema, TripSchemaEffective


# -----------------------------------------------------------------------------
# Helpers base: schemas, datasets y datos mínimos
# -----------------------------------------------------------------------------


def _make_field(
    name: str,
    dtype: str,
    *,
    required: bool = False,
    constraints: dict[str, Any] | None = None,
    domain: DomainSpec | None = None,
) -> FieldSpec:
    """Construye un FieldSpec con la misma forma usada en los notebooks OP-07."""
    return FieldSpec(
        name=name,
        dtype=dtype,
        required=required,
        constraints=constraints,
        domain=domain,
    )


def _make_trip_schema(fields: list[FieldSpec], *, version: str = "1.1") -> TripSchema:
    """Construye un TripSchema a partir de una lista ordenada de FieldSpec."""
    return TripSchema(
        version=version,
        fields={field.name: field for field in fields},
        required=[field.name for field in fields if field.required],
        semantic_rules=None,
    )


def _make_trip_schema_effective(
    *,
    dtype_effective: dict[str, str] | None = None,
    overrides: dict[str, Any] | None = None,
    domains_effective: dict[str, Any] | None = None,
    temporal: dict[str, Any] | None = None,
    fields_effective: list[str] | None = None,
) -> TripSchemaEffective:
    """Construye un TripSchemaEffective mínimo para artefactos de lectura."""
    return TripSchemaEffective(
        dtype_effective=dtype_effective or {},
        overrides=overrides or {},
        domains_effective=domains_effective or {},
        temporal=temporal or {},
        fields_effective=fields_effective or [],
    )


def _make_trip_df_minimal() -> pd.DataFrame:
    """Entrega el DataFrame mínimo usado en helper-level y smoke tests de OP-07."""
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
    """Construye el schema mínimo compartido por los tests OP-07."""
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
                required=False,
                domain=DomainSpec(values=["bus", "metro", "walk", "car"], extendable=True),
            ),
            _make_field(
                "purpose",
                "categorical",
                required=False,
                domain=DomainSpec(values=["work", "study", "health"], extendable=True),
            ),
            _make_field("comment", "string", required=False),
            _make_field("trip_weight", "float", required=False),
        ],
        version=version,
    )


def _make_trip_schema_effective_minimal() -> TripSchemaEffective:
    """Construye el schema efectivo mínimo persistible en sidecar OP-07."""
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


def _make_tripdataset_minimal(
    *,
    validated: bool = True,
    include_dataset_id: bool = True,
    include_artifact_id: bool = False,
) -> TripDataset:
    """Construye un TripDataset mínimo para setup de write_trips/read_trips."""
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


# -----------------------------------------------------------------------------
# Helpers de sidecar y artefactos mínimos
# -----------------------------------------------------------------------------


def _make_storage_options_snapshot(
    *,
    storage_format: str = "parquet",
    parquet_compression: str | None = "snappy",
    feather_compression: str | None = "lz4",
) -> dict[str, Any]:
    """Construye el bloque storage.options usado por el sidecar formal."""
    if storage_format == "parquet":
        return {"compression": parquet_compression}
    if storage_format == "feather":
        return {"compression": feather_compression, "version": 2}
    return {"compression": None}


def _make_sidecar_payload(
    *,
    schema: TripSchema | None = None,
    schema_effective: TripSchemaEffective | None = None,
    metadata: dict[str, Any] | None = None,
    provenance: dict[str, Any] | None = None,
    dataset_id: str = "dset_sidecar_001",
    artifact_id: str | None = "art_sidecar_001",
    storage_format: str = "parquet",
    parquet_compression: str | None = "snappy",
    feather_compression: str | None = "lz4",
    include_schema_effective: bool = True,
) -> dict[str, Any]:
    """Construye un payload de trips.metadata.json mínimo pero formalmente interpretable."""
    schema = schema or _make_trip_schema_minimal()
    schema_effective = schema_effective or _make_trip_schema_effective_minimal()

    if metadata is None:
        metadata = {
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
    else:
        metadata = copy.deepcopy(metadata)

    if provenance is None:
        provenance = {
            "source": {"name": "synthetic", "entity": "trips"},
            "ingestion": {"created_at_utc": "2026-04-04T00:00:00Z"},
        }
    else:
        provenance = copy.deepcopy(provenance)

    payload: dict[str, Any] = {
        "dataset_type": "trips",
        "format": "golondrina",
        "layout_version": "1.1",
        "storage": {
            "format": storage_format,
            "options": _make_storage_options_snapshot(
                storage_format=storage_format,
                parquet_compression=parquet_compression,
                feather_compression=feather_compression,
            ),
        },
        "dataset_id": dataset_id,
        "artifact_id": artifact_id,
        "files": {
            "data": _trip_data_filename_for_storage(storage_format),
            "metadata": "trips.metadata.json",
        },
        "schema": _trip_schema_to_snapshot(schema),
        "provenance": provenance,
        "metadata": metadata,
    }

    if include_schema_effective:
        payload["schema_effective"] = _trip_schema_effective_to_snapshot(schema_effective)

    return payload


def _write_table_for_read_fixture(
    df: pd.DataFrame,
    data_path: Path,
    *,
    storage_format: str,
    parquet_compression: str | None = "snappy",
    feather_compression: str | None = "lz4",
) -> None:
    """Escribe la tabla física mínima usada por fixtures de lectura OP-07."""
    if storage_format == "parquet":
        df.to_parquet(
            data_path,
            index=False,
            engine="pyarrow",
            compression=None if parquet_compression == "none" else parquet_compression,
        )
        return

    if storage_format == "feather":
        table = pa.Table.from_pandas(df, preserve_index=False)
        feather.write_feather(
            table,
            data_path,
            compression=feather_compression,
            version=2,
        )
        return

    raise ValueError(f"storage_format no soportado en fixture: {storage_format!r}")


def _materialize_minimal_formal_artifact(
    root_dir: Path,
    *,
    df: pd.DataFrame | None = None,
    schema: TripSchema | None = None,
    schema_effective: TripSchemaEffective | None = None,
    metadata: dict[str, Any] | None = None,
    provenance: dict[str, Any] | None = None,
    dataset_id: str = "dset_artifact_001",
    artifact_id: str | None = "art_artifact_001",
    storage_format: str = "parquet",
    parquet_compression: str | None = "snappy",
    feather_compression: str | None = "lz4",
    include_schema_effective: bool = True,
) -> tuple[Any, dict[str, Any]]:
    """Materializa un bundle formal mínimo sin pasar por write_trips."""
    root_dir.mkdir(parents=True, exist_ok=True)

    df = df.copy(deep=True) if df is not None else _make_trip_df_minimal()
    schema = schema or _make_trip_schema_minimal()
    schema_effective = schema_effective or _make_trip_schema_effective_minimal()

    data_filename = _trip_data_filename_for_storage(storage_format)
    data_path = root_dir / data_filename
    sidecar_path = root_dir / "trips.metadata.json"

    _write_table_for_read_fixture(
        df,
        data_path,
        storage_format=storage_format,
        parquet_compression=parquet_compression,
        feather_compression=feather_compression,
    )

    payload = _make_sidecar_payload(
        schema=schema,
        schema_effective=schema_effective,
        metadata=metadata,
        provenance=provenance,
        dataset_id=dataset_id,
        artifact_id=artifact_id,
        storage_format=storage_format,
        parquet_compression=parquet_compression,
        feather_compression=feather_compression,
        include_schema_effective=include_schema_effective,
    )

    sidecar_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return _resolve_trips_artifact_paths(root_dir), payload


# -----------------------------------------------------------------------------
# Helpers generales de asserts y sidecars
# -----------------------------------------------------------------------------


def _clone_tripdataset(trips: TripDataset) -> TripDataset:
    """Clona profundamente un TripDataset para evitar mutación cruzada entre tests."""
    return copy.deepcopy(trips)


def _get_issue_codes(issues: list[Issue] | list[dict[str, Any]]) -> list[str]:
    """Extrae códigos desde Issue o desde diccionarios serializados."""
    codes: list[str] = []
    for issue in issues:
        if hasattr(issue, "code"):
            codes.append(issue.code)
        elif isinstance(issue, dict):
            codes.append(issue["code"])
        else:
            raise TypeError(f"Issue no interpretable: {type(issue)!r}")
    return codes


def _assert_issue_present(issues: list[Issue] | list[dict[str, Any]], code: str) -> None:
    """Verifica que una lista de issues contenga un código esperado."""
    codes = _get_issue_codes(issues)
    assert code in codes, f"No se encontró {code}. Codes actuales: {codes}"


def _assert_issue_absent(issues: list[Issue] | list[dict[str, Any]], code: str) -> None:
    """Verifica que una lista de issues no contenga un código dado."""
    codes = _get_issue_codes(issues)
    assert code not in codes, f"Se encontró inesperadamente {code}. Codes actuales: {codes}"


def _assert_json_safe(obj: Any, label: str = "object") -> None:
    """Verifica que un objeto sea serializable como JSON."""
    try:
        json.dumps(obj, ensure_ascii=False)
    except Exception as exc:
        raise AssertionError(f"{label} no es JSON-safe: {exc}") from exc


def _load_sidecar(artifact_dir: Path) -> dict[str, Any]:
    """Carga trips.metadata.json desde un bundle de trips."""
    sidecar_path = artifact_dir / "trips.metadata.json"
    if not sidecar_path.exists():
        raise FileNotFoundError(f"No existe sidecar: {sidecar_path}")
    return json.loads(sidecar_path.read_text(encoding="utf-8"))


def _write_sidecar(artifact_dir: Path, payload: dict[str, Any]) -> None:
    """Sobrescribe trips.metadata.json para preparar casos degradados."""
    sidecar_path = artifact_dir / "trips.metadata.json"
    sidecar_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _assert_data_equivalent(left: pd.DataFrame, right: pd.DataFrame) -> None:
    """Compara DataFrames evitando acoplar el test a dtypes físicos exactos."""
    pd.testing.assert_frame_equal(
        left.reset_index(drop=True),
        right.reset_index(drop=True),
        check_dtype=False,
        check_categorical=False,
    )


def _artifact_data_filename(storage_format: str) -> str:
    """Entrega el nombre tabular contractual para el backend indicado."""
    if storage_format == "parquet":
        return "trips.parquet"
    if storage_format == "feather":
        return "trips.feather"
    raise ValueError(f"storage_format no soportado: {storage_format!r}")


def _artifact_data_file_path(artifact_dir: Path, storage_format: str) -> Path:
    """Construye la ruta al archivo tabular del bundle OP-07."""
    return artifact_dir / _artifact_data_filename(storage_format)


def _assert_counts_by_level(
    issues: list[Issue],
    *,
    errors: int | None = None,
    warnings: int | None = None,
    info: int | None = None,
) -> None:
    """Verifica conteos de severidad sin depender del orden de issues."""
    counts = {"error": 0, "warning": 0, "info": 0}
    for issue in issues:
        counts[issue.level] = counts.get(issue.level, 0) + 1

    if errors is not None:
        assert counts["error"] == errors, f"errors esperado={errors}, actual={counts['error']}"
    if warnings is not None:
        assert counts["warning"] == warnings, f"warnings esperado={warnings}, actual={counts['warning']}"
    if info is not None:
        assert counts["info"] == info, f"info esperado={info}, actual={counts['info']}"


def _selected_categorical_columns(df: pd.DataFrame | None = None) -> list[str]:
    """Lista columnas categóricas ricas observadas en los tests de integración."""
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


def _observed_non_null_values(series: pd.Series) -> set[str]:
    """Obtiene valores no nulos como strings para comparar categorías preservadas."""
    return set(series.astype("string").dropna().tolist())


def _expected_artifact_dir(base_path: Path) -> Path:
    """Replica la normalización simple de sufijo .golondrina usada por write_trips."""
    if base_path.name.endswith(".golondrina"):
        return base_path
    return base_path.with_name(f"{base_path.name}.golondrina")


# -----------------------------------------------------------------------------
# Fixtures de carpetas temporales
# -----------------------------------------------------------------------------


@pytest.fixture
def artifact_root(tmp_path: Path) -> Path:
    """Entrega una ruta temporal para un bundle OP-07 sin crear carpetas fijas."""
    return tmp_path / "artifact"


@pytest.fixture
def make_case_dir(tmp_path: Path) -> Callable[[str], Path]:
    """Crea subcarpetas de caso siempre bajo tmp_path."""

    def _factory(case_name: str) -> Path:
        case_dir = tmp_path / case_name
        case_dir.mkdir(parents=True, exist_ok=True)
        return case_dir

    return _factory


# -----------------------------------------------------------------------------
# Fixtures mínimas
# -----------------------------------------------------------------------------


@pytest.fixture
def trip_df_minimal() -> pd.DataFrame:
    """Entrega el DataFrame mínimo para escribir y leer tablas de trips."""
    return _make_trip_df_minimal()


@pytest.fixture
def trip_schema_minimal() -> TripSchema:
    """Entrega el TripSchema mínimo usado por sidecars y TripDataset de OP-07."""
    return _make_trip_schema_minimal()


@pytest.fixture
def trip_schema_effective_minimal() -> TripSchemaEffective:
    """Entrega el TripSchemaEffective mínimo persistible en OP-07."""
    return _make_trip_schema_effective_minimal()


@pytest.fixture
def trip_dataset_validated() -> TripDataset:
    """Entrega un TripDataset mínimo marcado como validado para write_trips."""
    return _make_tripdataset_minimal(validated=True)


@pytest.fixture
def trip_dataset_unvalidated() -> TripDataset:
    """Entrega un TripDataset mínimo no validado para setups con require_validated=False."""
    return _make_tripdataset_minimal(validated=False)


# -----------------------------------------------------------------------------
# Fixtures que devuelven helpers reutilizables
# -----------------------------------------------------------------------------


@pytest.fixture
def make_sidecar_payload() -> Callable[..., dict[str, Any]]:
    """Entrega el builder de sidecars mínimos para helper-level tests."""
    return _make_sidecar_payload


@pytest.fixture
def materialize_minimal_formal_artifact() -> Callable[..., tuple[Any, dict[str, Any]]]:
    """Entrega el materializador de artefactos formales mínimos sin API pública."""
    return _materialize_minimal_formal_artifact


@pytest.fixture
def load_sidecar() -> Callable[[Path], dict[str, Any]]:
    """Entrega el helper para cargar trips.metadata.json."""
    return _load_sidecar


@pytest.fixture
def write_sidecar() -> Callable[[Path, dict[str, Any]], None]:
    """Entrega el helper para modificar trips.metadata.json en casos degradados."""
    return _write_sidecar


@pytest.fixture
def assert_issue_present() -> Callable[[list[Issue] | list[dict[str, Any]], str], None]:
    """Entrega el helper para verificar presencia de códigos READ.*."""
    return _assert_issue_present


@pytest.fixture
def assert_issue_absent() -> Callable[[list[Issue] | list[dict[str, Any]], str], None]:
    """Entrega el helper para verificar ausencia de códigos READ.*."""
    return _assert_issue_absent


@pytest.fixture
def get_issue_codes() -> Callable[[list[Issue] | list[dict[str, Any]]], list[str]]:
    """Entrega el helper para extraer códigos de issues."""
    return _get_issue_codes


@pytest.fixture
def assert_counts_by_level() -> Callable[..., None]:
    """Entrega el helper para revisar conteos de severidad."""
    return _assert_counts_by_level


@pytest.fixture
def assert_json_safe() -> Callable[[Any, str], None]:
    """Entrega el helper para verificar serialización JSON."""
    return _assert_json_safe


@pytest.fixture
def assert_data_equivalent() -> Callable[[pd.DataFrame, pd.DataFrame], None]:
    """Entrega el helper para comparar dataframes sin dtype frágil."""
    return _assert_data_equivalent


@pytest.fixture
def clone_tripdataset() -> Callable[[TripDataset], TripDataset]:
    """Entrega el helper para clonar TripDataset sin compartir estado mutable."""
    return _clone_tripdataset


@pytest.fixture
def artifact_data_filename() -> Callable[[str], str]:
    """Entrega el helper para obtener trips.parquet o trips.feather."""
    return _artifact_data_filename


@pytest.fixture
def artifact_data_file_path() -> Callable[[Path, str], Path]:
    """Entrega el helper para ubicar el archivo tabular de un bundle."""
    return _artifact_data_file_path


@pytest.fixture
def selected_categorical_columns() -> Callable[[pd.DataFrame | None], list[str]]:
    """Entrega el helper de columnas categóricas usadas en integración."""
    return _selected_categorical_columns


@pytest.fixture
def observed_non_null_values() -> Callable[[pd.Series], set[str]]:
    """Entrega el helper para comparar dominios observados no nulos."""
    return _observed_non_null_values


@pytest.fixture
def write_valid_artifact_with_backend(
    trip_dataset_validated: TripDataset,
    clone_tripdataset: Callable[[TripDataset], TripDataset],
) -> Callable[..., tuple[TripDataset, Path, Path, Any]]:
    """Entrega un factory que crea bundles reales mediante write_trips."""

    def _factory(
        case_dir: Path,
        artifact_name: str = "artifact_bundle",
        *,
        trips: TripDataset | None = None,
        storage_format: str = "parquet",
        parquet_compression: str = "snappy",
        feather_compression: str = "lz4",
        require_validated: bool = True,
    ) -> tuple[TripDataset, Path, Path, Any]:
        trips_to_write = clone_tripdataset(trips if trips is not None else trip_dataset_validated)
        base_path = case_dir / artifact_name

        report = write_trips(
            trips_to_write,
            base_path,
            options=WriteTripsOptions(
                mode="error_if_exists",
                require_validated=require_validated,
                storage_format=storage_format,
                parquet_compression=parquet_compression,
                feather_compression=feather_compression,
                normalize_artifact_dir=True,
            ),
        )

        artifact_dir = _expected_artifact_dir(base_path)
        data_path = _artifact_data_file_path(artifact_dir, storage_format)
        return trips_to_write, artifact_dir, data_path, report

    return _factory


# -----------------------------------------------------------------------------
# Fixtures ricas de integración
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
    "purpose": ["home", "work", "education", "shopping", "errand", "health", "leisure", "transfer", "other"],
    "day_type": ["weekday", "weekend", "holiday"],
    "time_period": ["night", "morning", "midday", "afternoon", "evening"],
    "user_gender": ["female", "male", "other", "unknown"],
    "user_age_group": ["0-14", "15-24", "25-34", "35-44", "45-54", "55-64", "65-plus", "unknown"],
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


def _make_rich_trip_schema() -> TripSchema:
    """Construye el schema rico usado en los integration tests OP-07."""
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

        fields[field_name] = _make_field(
            field_name,
            dtype_name,
            required=False,
            domain=domain,
        )

    return TripSchema(
        version="1.1",
        fields=fields,
        required=list(REQUIRED_FIELDS_ORDER),
        semantic_rules=None,
    )


def _build_rich_source_dataframe(seed: int = 20260404, filas: int = 180) -> pd.DataFrame:
    """Construye el DataFrame rico usando el generador sintético del repositorio."""
    pytest.importorskip("h3", reason="Rich OP-07 fixtures require h3 through import_trips.")

    try:
        from scripts.synthetic_data.base_generator import generate_synthetic_trip_dataframe
    except ModuleNotFoundError as exc:
        pytest.skip(f"No está disponible scripts.synthetic_data.base_generator: {exc}")

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


@pytest.fixture
def rich_trip_schema() -> TripSchema:
    """Entrega el TripSchema rico de integración."""
    return _make_rich_trip_schema()


@pytest.fixture
def rich_source_df() -> pd.DataFrame:
    """Entrega el DataFrame sintético rico usado por integration tests."""
    return _build_rich_source_dataframe(filas=180)


@pytest.fixture
def rich_tripdataset_unvalidated(
    rich_source_df: pd.DataFrame,
    rich_trip_schema: TripSchema,
) -> TripDataset:
    """Importa la fuente rica y retorna un TripDataset aún no validado."""
    pytest.importorskip("h3", reason="Rich OP-07 fixtures require h3 through import_trips.")

    from pylondrina.importing import ImportOptions, import_trips_from_dataframe

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
            "notes": ["fixture de integración OP-07 read_trips"],
        },
        h3_resolution=8,
    )

    if not import_report.ok:
        raise RuntimeError(f"No se pudo construir rich_tripdataset_unvalidated: {_get_issue_codes(import_report.issues)}")

    return trips


@pytest.fixture
def rich_tripdataset_validated(rich_tripdataset_unvalidated: TripDataset) -> TripDataset:
    """Retorna una copia validada del TripDataset rico de integración."""
    pytest.importorskip("h3", reason="Rich OP-07 fixtures require h3 through validate_trips.")

    from pylondrina.validation import ValidationOptions, validate_trips

    trips = _clone_tripdataset(rich_tripdataset_unvalidated)
    validation_report = validate_trips(
        trips,
        options=ValidationOptions(
            strict=False,
            validate_domains="full",
        ),
    )

    if not validation_report.ok:
        raise RuntimeError(f"No se pudo validar rich_tripdataset_validated: {_get_issue_codes(validation_report.issues)}")

    return trips