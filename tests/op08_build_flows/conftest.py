from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import pytest


# -----------------------------------------------------------------------------
# Resolución de paths del repositorio
# -----------------------------------------------------------------------------


def _ensure_repo_import_paths() -> None:
    """Agrega repo root y src/ al sys.path cuando los tests corren desde el repo."""
    current = Path(__file__).resolve()

    for parent in (current.parent, *current.parents):
        src_dir = parent / "src"
        pylondrina_dir = src_dir / "pylondrina"

        if pylondrina_dir.exists():
            root_text = str(parent)
            src_text = str(src_dir)

            if root_text not in sys.path:
                sys.path.insert(0, root_text)

            if src_text not in sys.path:
                sys.path.insert(0, src_text)

            return


_ensure_repo_import_paths()


# -----------------------------------------------------------------------------
# Imports del módulo bajo prueba
# -----------------------------------------------------------------------------


from pylondrina.datasets import TripDataset
from pylondrina.importing import ImportOptions, import_trips_from_dataframe
from pylondrina.reports import Issue
from pylondrina.schema import DomainSpec, FieldSpec, TripSchema, TripSchemaEffective
from pylondrina.validation import ValidationOptions, validate_trips


# -----------------------------------------------------------------------------
# Helpers privados de construcción de schemas mínimos
# -----------------------------------------------------------------------------


def _make_field(
    name: str,
    dtype: str,
    *,
    required: bool = False,
    constraints: dict[str, Any] | None = None,
    domain: DomainSpec | None = None,
) -> FieldSpec:
    """Construye un FieldSpec con la forma usada en los notebooks OP-08."""
    return FieldSpec(
        name=name,
        dtype=dtype,
        required=required,
        constraints=constraints,
        domain=domain,
    )


def _make_trip_schema_minimal_for_flows() -> TripSchema:
    """Construye el TripSchema mínimo utilizado en tests unitarios de build_flows."""
    fields = {
        "movement_id": _make_field("movement_id", "string", required=True),
        "origin_h3_index": _make_field("origin_h3_index", "string", required=True),
        "destination_h3_index": _make_field("destination_h3_index", "string", required=True),
        "origin_time_utc": _make_field("origin_time_utc", "datetime", required=False),
        "destination_time_utc": _make_field("destination_time_utc", "datetime", required=False),
        "mode": _make_field(
            "mode",
            "categorical",
            required=False,
            domain=DomainSpec(values=["bus", "metro", "walk"], extendable=True),
        ),
        "purpose": _make_field(
            "purpose",
            "categorical",
            required=False,
            domain=DomainSpec(values=["work", "study", "other"], extendable=True),
        ),
        "user_gender": _make_field(
            "user_gender",
            "categorical",
            required=False,
            domain=DomainSpec(values=["M", "F"], extendable=True),
        ),
        "trip_weight": _make_field("trip_weight", "float", required=False),
    }

    return TripSchema(
        version="1.1",
        fields=fields,
        required=["movement_id", "origin_h3_index", "destination_h3_index"],
    )


def _make_trip_schema_effective_for_flows() -> TripSchemaEffective:
    """Construye el TripSchemaEffective mínimo asociado a los fixtures de flows."""
    return TripSchemaEffective(
        dtype_effective={
            "movement_id": "string",
            "origin_h3_index": "string",
            "destination_h3_index": "string",
            "origin_time_utc": "datetime",
            "destination_time_utc": "datetime",
            "mode": "categorical",
            "purpose": "categorical",
            "user_gender": "categorical",
            "trip_weight": "float",
        },
        domains_effective={
            "mode": ["bus", "metro", "walk"],
            "purpose": ["work", "study", "other"],
            "user_gender": ["M", "F"],
        },
        temporal={"tier": "tier_1"},
        fields_effective=[
            "movement_id",
            "origin_h3_index",
            "destination_h3_index",
            "origin_time_utc",
            "destination_time_utc",
            "mode",
            "purpose",
            "user_gender",
            "trip_weight",
        ],
    )


def _make_buildable_trip_df() -> pd.DataFrame:
    """Construye el DataFrame mínimo buildable usado en tests de OP-08."""
    return pd.DataFrame(
        {
            "movement_id": ["m0", "m1", "m2", "m3"],
            "origin_h3_index": [
                "8828308281fffff",
                "8828308281fffff",
                "8828308281fffff",
                "882830828dfffff",
            ],
            "destination_h3_index": [
                "8828308285fffff",
                "8828308285fffff",
                "8828308285fffff",
                "8828308287fffff",
            ],
            "origin_time_utc": pd.to_datetime(
                [
                    "2026-04-01T08:10:00Z",
                    "2026-04-01T08:20:00Z",
                    "2026-04-01T08:45:00Z",
                    "2026-04-01T09:10:00Z",
                ],
                utc=True,
            ),
            "destination_time_utc": pd.to_datetime(
                [
                    "2026-04-01T08:35:00Z",
                    "2026-04-01T08:50:00Z",
                    "2026-04-01T09:05:00Z",
                    "2026-04-01T09:40:00Z",
                ],
                utc=True,
            ),
            "mode": ["bus", "bus", "metro", "bus"],
            "purpose": ["work", "work", "study", "work"],
            "user_gender": ["M", "F", "F", "M"],
            "trip_weight": [1.0, 2.0, 1.5, 3.0],
        }
    )


def _make_tripdataset_for_flows(
    *,
    validated: bool = True,
    tier: str = "tier_1",
    include_dataset_id: bool = True,
) -> TripDataset:
    """Construye un TripDataset mínimo preparado para probar build_flows."""
    metadata: dict[str, Any] = {
        "is_validated": validated,
        "events": [
            {
                "op": "import_trips",
                "ts_utc": "2026-04-01T00:00:00Z",
                "summary": {"rows_out": 4},
            },
            {
                "op": "validate_trips",
                "ts_utc": "2026-04-01T00:10:00Z",
                "summary": {"ok": validated},
            },
        ],
        "temporal": {
            "tier": tier,
            "fields_present": ["origin_time_utc", "destination_time_utc"],
        },
        "h3": {"resolution": 8},
    }

    if include_dataset_id:
        metadata["dataset_id"] = "trips_case_001"

    return TripDataset(
        data=_make_buildable_trip_df(),
        schema=_make_trip_schema_minimal_for_flows(),
        schema_version="1.1",
        provenance={
            "source_name": "demo_source",
            "source": {"name": "demo_source"},
        },
        metadata=metadata,
        schema_effective=_make_trip_schema_effective_for_flows(),
    )


# -----------------------------------------------------------------------------
# Fixtures mínimas para helper-level y smoke tests
# -----------------------------------------------------------------------------


@pytest.fixture
def trip_schema_minimal_for_flows() -> TripSchema:
    """Entrega el TripSchema mínimo usado en tests unitarios de OP-08."""
    return _make_trip_schema_minimal_for_flows()


@pytest.fixture
def trip_schema_effective_for_flows() -> TripSchemaEffective:
    """Entrega el TripSchemaEffective mínimo usado en tests unitarios de OP-08."""
    return _make_trip_schema_effective_for_flows()


@pytest.fixture
def buildable_trip_df() -> pd.DataFrame:
    """Entrega el DataFrame mínimo con movements buildables para flows."""
    return _make_buildable_trip_df()


@pytest.fixture
def make_tripdataset_for_flows() -> Callable[..., TripDataset]:
    """Entrega una factory de TripDataset mínimos configurables para build_flows."""
    return _make_tripdataset_for_flows


# -----------------------------------------------------------------------------
# Helpers reutilizables de testing
# -----------------------------------------------------------------------------


def _clone_tripdataset(trips: TripDataset) -> TripDataset:
    """Clona profundamente un TripDataset para evitar mutación cruzada entre tests."""
    return copy.deepcopy(trips)


def _assert_json_safe(obj: Any, label: str = "object") -> None:
    """Verifica que un objeto sea serializable a JSON."""
    try:
        json.dumps(obj, ensure_ascii=False, default=str)
    except Exception as exc:
        raise AssertionError(f"{label} no es JSON-safe: {exc}") from exc


def _get_issue_codes(issues: list[Issue] | list[dict[str, Any]]) -> list[str]:
    """Extrae códigos de issues desde objetos Issue o diccionarios serializados."""
    codes: list[str] = []

    for issue in issues:
        if hasattr(issue, "code"):
            codes.append(issue.code)
        elif isinstance(issue, dict):
            codes.append(issue["code"])
        else:
            raise TypeError(f"Issue no interpretable: {type(issue)!r}")

    return codes


def _assert_issue_present(
    issues: list[Issue] | list[dict[str, Any]],
    code: str,
) -> None:
    """Verifica que una colección de issues contenga un código esperado."""
    codes = _get_issue_codes(issues)
    assert code in codes, f"No se encontró {code}. Codes actuales: {codes}"


@pytest.fixture
def clone_tripdataset() -> Callable[[TripDataset], TripDataset]:
    """Entrega el helper para clonar TripDataset sin compartir estado mutable."""
    return _clone_tripdataset


@pytest.fixture
def assert_json_safe() -> Callable[[Any, str], None]:
    """Entrega el helper para comprobar serialización JSON-safe."""
    return _assert_json_safe


@pytest.fixture
def get_issue_codes() -> Callable[[list[Issue] | list[dict[str, Any]]], list[str]]:
    """Entrega el helper para extraer códigos de issues."""
    return _get_issue_codes


@pytest.fixture
def assert_issue_present() -> Callable[[list[Issue] | list[dict[str, Any]], str], None]:
    """Entrega el helper para verificar presencia de un código de issue."""
    return _assert_issue_present


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


def _make_rich_trip_schema() -> TripSchema:
    """Construye el schema rico usado en los integration tests de OP-08."""
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
            domain = DomainSpec(
                values=CANONICAL_DOMAINS[field_name],
                extendable=True,
            )

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


def _build_rich_source_dataframe(
    *,
    seed: int,
    filas: int,
) -> pd.DataFrame:
    """Construye una fuente sintética rica usando el generador del repositorio."""
    try:
        from scripts.synthetic_data.base_generator import generate_synthetic_trip_dataframe
    except ModuleNotFoundError as exc:
        pytest.skip(
            f"No está disponible scripts.synthetic_data.base_generator: {exc}"
        )

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


def _build_tripdataset_fixture(
    df: pd.DataFrame,
    schema: TripSchema,
    *,
    source_name: str,
    validate_after_import: bool = True,
) -> TripDataset:
    """Importa y, opcionalmente, valida una fuente rica para integration tests."""
    trips, import_report = import_trips_from_dataframe(
        df,
        schema,
        source_name=source_name,
        options=ImportOptions(
            keep_extra_fields=True,
            selected_fields=None,
            strict=False,
            strict_domains=False,
            single_stage=False,
            source_timezone=None,
        ),
        provenance={
            "source": {"name": source_name, "entity": "trips"},
            "ingestion": {"created_at_utc": "2026-04-06T00:00:00Z"},
            "notes": [f"fixture de integración {source_name}"],
        },
        h3_resolution=12,
    )

    if not import_report.ok:
        raise RuntimeError(
            f"No se pudo construir el TripDataset rico desde importación. "
            f"Issues: {_get_issue_codes(import_report.issues)}"
        )

    if validate_after_import:
        validation_report = validate_trips(
            trips,
            options=ValidationOptions(
                strict=False,
                validate_domains="off",
                validate_temporal_consistency=False,
                validate_duplicates=False,
            ),
        )

        if not validation_report.ok:
            raise RuntimeError(
                f"No se pudo validar el TripDataset rico. "
                f"Issues: {_get_issue_codes(validation_report.issues)}"
            )

        if trips.metadata.get("is_validated") is not True:
            raise RuntimeError(
                "La fixture rica esperaba metadata['is_validated'] is True "
                "después de validate_trips."
            )

    return trips


@pytest.fixture
def rich_trip_schema() -> TripSchema:
    """Entrega el TripSchema rico usado en las pruebas integradas de OP-08."""
    return _make_rich_trip_schema()


@pytest.fixture
def rich_source_df_small() -> pd.DataFrame:
    """Entrega la fuente sintética rica pequeña usada en integración."""
    return _build_rich_source_dataframe(
        seed=20260406,
        filas=60,
    )


@pytest.fixture
def rich_source_df() -> pd.DataFrame:
    """Entrega la fuente sintética rica principal usada en integración."""
    return _build_rich_source_dataframe(
        seed=20260407,
        filas=260,
    )


@pytest.fixture
def tripdataset_validated_small(
    rich_source_df_small: pd.DataFrame,
    rich_trip_schema: TripSchema,
) -> TripDataset:
    """Entrega un TripDataset rico pequeño importado y validado."""
    return _build_tripdataset_fixture(
        rich_source_df_small,
        rich_trip_schema,
        source_name="synthetic_small_for_flows",
        validate_after_import=True,
    )


@pytest.fixture
def tripdataset_ready_for_flows(
    rich_source_df: pd.DataFrame,
    rich_trip_schema: TripSchema,
) -> TripDataset:
    """Entrega un TripDataset rico principal listo para construir flows."""
    return _build_tripdataset_fixture(
        rich_source_df,
        rich_trip_schema,
        source_name="synthetic_rich_for_flows",
        validate_after_import=True,
    )


@pytest.fixture
def tripdataset_non_buildable(
    tripdataset_validated_small: TripDataset,
    clone_tripdataset: Callable[[TripDataset], TripDataset],
) -> TripDataset:
    """Entrega un TripDataset validado pero sin H3 OD utilizable para construir flows."""
    trips = clone_tripdataset(tripdataset_validated_small)

    trips.data["origin_h3_index"] = pd.NA
    trips.data["destination_h3_index"] = pd.NA
    trips.metadata["is_validated"] = True

    return trips