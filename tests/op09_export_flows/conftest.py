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


from pylondrina.datasets import FlowDataset, TripDataset
from pylondrina.export.flows import FlowExportResult
from pylondrina.importing import ImportOptions, import_trips_from_dataframe
from pylondrina.reports import FlowBuildReport, Issue
from pylondrina.schema import DomainSpec, FieldSpec, TripSchema
from pylondrina.transforms.flows import FlowBuildOptions, build_flows
from pylondrina.validation import ValidationOptions, validate_trips


# -----------------------------------------------------------------------------
# Fixtures mínimas: FlowDataset exportables de OP-09
# -----------------------------------------------------------------------------


def _make_flowdataset_for_export(
    *,
    include_dataset_id: bool = True,
    with_extra_fields: bool = True,
) -> FlowDataset:
    """Construye un FlowDataset mínimo exportable para pruebas de OP-09."""
    flows_df = pd.DataFrame(
        {
            "flow_id": ["flow_0000000", "flow_0000001"],
            "origin_h3_index": ["8828308281fffff", "882830828dfffff"],
            "destination_h3_index": ["8828308285fffff", "8828308287fffff"],
            "flow_count": [2, 1],
            "flow_value": [3.0, 3.0],
            "window_start_utc": pd.to_datetime(
                [
                    "2026-04-01T08:00:00Z",
                    "2026-04-01T09:00:00Z",
                ],
                utc=True,
            ),
            "window_end_utc": pd.to_datetime(
                [
                    "2026-04-01T09:00:00Z",
                    "2026-04-01T10:00:00Z",
                ],
                utc=True,
            ),
            "mode": ["bus", "metro"],
            "purpose": ["work", "study"],
            "user_gender": ["M", "F"],
        }
    )

    if not with_extra_fields:
        flows_df = flows_df[
            [
                "flow_id",
                "origin_h3_index",
                "destination_h3_index",
                "flow_count",
                "flow_value",
            ]
        ].copy()

    metadata: dict[str, Any] = {
        "events": [],
        "is_validated": False,
    }

    if include_dataset_id:
        metadata["dataset_id"] = "flows_case_001"

    return FlowDataset(
        flows=flows_df,
        flow_to_trips=pd.DataFrame(
            {
                "flow_id": ["flow_0000000", "flow_0000000", "flow_0000001"],
                "movement_id": ["m0", "m1", "m3"],
            }
        ),
        aggregation_spec={
            "h3_resolution": 8,
            "group_by": ["mode"],
            "time_aggregation": "hour",
            "time_basis": "origin",
            "min_trips_per_flow": 1,
            "keep_flow_to_trips": True,
            "require_validated": True,
            "strict": False,
            "max_issues": 1000,
            "effective_flow_keys": [
                "origin_h3_index",
                "destination_h3_index",
                "window_start_utc",
                "window_end_utc",
                "mode",
            ],
        },
        source_trips=None,
        metadata=metadata,
        provenance={
            "source_name": "demo_source",
            "derived_from": [
                {
                    "source_type": "trips",
                    "dataset_id": "trips_case_001",
                }
            ],
        },
    )


@pytest.fixture
def make_flowdataset_for_export() -> Callable[..., FlowDataset]:
    """Entrega una factory de FlowDataset mínimos exportables para OP-09."""
    return _make_flowdataset_for_export


@pytest.fixture
def flowdataset_minimal_for_export() -> FlowDataset:
    """Entrega un FlowDataset exportable sin columnas extra solicitables."""
    return _make_flowdataset_for_export(
        include_dataset_id=True,
        with_extra_fields=False,
    )


@pytest.fixture
def flowdataset_with_extras_for_export() -> FlowDataset:
    """Entrega un FlowDataset exportable con columnas extra planas disponibles."""
    return _make_flowdataset_for_export(
        include_dataset_id=True,
        with_extra_fields=True,
    )


# -----------------------------------------------------------------------------
# Helpers reutilizables de clonación e issues
# -----------------------------------------------------------------------------


def _clone_flowdataset(flows: FlowDataset) -> FlowDataset:
    """Clona profundamente un FlowDataset para evitar mutación cruzada entre tests."""
    return copy.deepcopy(flows)


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
def clone_flowdataset() -> Callable[[FlowDataset], FlowDataset]:
    """Entrega el helper para clonar FlowDataset sin compartir estado mutable."""
    return _clone_flowdataset


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
# Helpers de filesystem temporal para exportaciones
# -----------------------------------------------------------------------------


@pytest.fixture
def export_root(tmp_path: Path) -> Path:
    """Entrega una raíz temporal exclusiva para exports de OP-09."""
    root = tmp_path / "exports"
    root.mkdir(parents=True, exist_ok=True)
    return root


@pytest.fixture
def make_export_case_dir(export_root: Path) -> Callable[[str], Path]:
    """Entrega una factory de subdirectorios temporales por caso de export."""

    def _factory(case_name: str) -> Path:
        case_dir = export_root / case_name
        case_dir.mkdir(parents=True, exist_ok=True)
        return case_dir

    return _factory


# -----------------------------------------------------------------------------
# Helpers de lectura de artefactos exportados
# -----------------------------------------------------------------------------


def _load_export_sidecar(export_result: FlowExportResult) -> dict[str, Any]:
    """Carga el metadata.json producido por export_flows."""
    metadata_path = Path(export_result.artifacts["metadata"])
    return json.loads(metadata_path.read_text(encoding="utf-8"))


def _read_export_flows_csv(export_result: FlowExportResult) -> pd.DataFrame:
    """Lee el flows.csv producido por export_flows."""
    flows_path = Path(export_result.artifacts["flows"])
    return pd.read_csv(flows_path)


def _read_export_locations_csv(export_result: FlowExportResult) -> pd.DataFrame:
    """Lee el locations.csv producido por export_flows."""
    locations_path = Path(export_result.artifacts["locations"])
    return pd.read_csv(locations_path)


@pytest.fixture
def load_export_sidecar() -> Callable[[FlowExportResult], dict[str, Any]]:
    """Entrega el helper para leer metadata.json de un export."""
    return _load_export_sidecar


@pytest.fixture
def read_export_flows_csv() -> Callable[[FlowExportResult], pd.DataFrame]:
    """Entrega el helper para leer flows.csv de un export."""
    return _read_export_flows_csv


@pytest.fixture
def read_export_locations_csv() -> Callable[[FlowExportResult], pd.DataFrame]:
    """Entrega el helper para leer locations.csv de un export."""
    return _read_export_locations_csv


# -----------------------------------------------------------------------------
# Infraestructura rica para builders de FlowDataset vía OP-08
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


def _make_field(
    name: str,
    dtype: str,
    *,
    required: bool = False,
    domain: DomainSpec | None = None,
) -> FieldSpec:
    """Construye un FieldSpec para los TripDataset ricos de integración."""
    return FieldSpec(
        name=name,
        dtype=dtype,
        required=required,
        constraints=None,
        domain=domain,
    )


def _make_rich_trip_schema() -> TripSchema:
    """Construye el TripSchema rico usado para derivar FlowDataset de integración."""
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
    """Importa y valida una fuente rica para producir trips aptos para build_flows."""
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
            "No se pudo construir el TripDataset rico desde importación. "
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
                "No se pudo validar el TripDataset rico. "
                f"Issues: {_get_issue_codes(validation_report.issues)}"
            )

        if trips.metadata.get("is_validated") is not True:
            raise RuntimeError(
                "La fixture rica esperaba metadata['is_validated'] is True "
                "después de validate_trips."
            )

    return trips


def _clone_tripdataset(trips: TripDataset) -> TripDataset:
    """Clona profundamente un TripDataset para builders de FlowDataset."""
    return copy.deepcopy(trips)


@pytest.fixture
def _rich_trip_schema_for_export_integration() -> TripSchema:
    """Entrega el TripSchema rico interno usado por builders de integración OP-09."""
    return _make_rich_trip_schema()


@pytest.fixture
def _rich_source_df_small_for_export_integration() -> pd.DataFrame:
    """Entrega la fuente sintética rica pequeña usada para builders OP-09."""
    return _build_rich_source_dataframe(
        seed=20260406,
        filas=60,
    )


@pytest.fixture
def _rich_source_df_for_export_integration() -> pd.DataFrame:
    """Entrega la fuente sintética rica principal usada para builders OP-09."""
    return _build_rich_source_dataframe(
        seed=20260407,
        filas=260,
    )


@pytest.fixture
def _tripdataset_validated_small_for_export_integration(
    _rich_source_df_small_for_export_integration: pd.DataFrame,
    _rich_trip_schema_for_export_integration: TripSchema,
) -> TripDataset:
    """Entrega un TripDataset rico pequeño validado para construir flows exportables."""
    return _build_tripdataset_fixture(
        _rich_source_df_small_for_export_integration,
        _rich_trip_schema_for_export_integration,
        source_name="synthetic_small_for_flows",
        validate_after_import=True,
    )


@pytest.fixture
def _tripdataset_ready_for_export_integration(
    _rich_source_df_for_export_integration: pd.DataFrame,
    _rich_trip_schema_for_export_integration: TripSchema,
) -> TripDataset:
    """Entrega un TripDataset rico principal validado para construir flows exportables."""
    return _build_tripdataset_fixture(
        _rich_source_df_for_export_integration,
        _rich_trip_schema_for_export_integration,
        source_name="synthetic_rich_for_flows",
        validate_after_import=True,
    )


# -----------------------------------------------------------------------------
# Builders de FlowDataset reales mediante OP-08
# -----------------------------------------------------------------------------


@pytest.fixture
def make_flowdataset_small(
    _tripdataset_validated_small_for_export_integration: TripDataset,
) -> Callable[[], tuple[FlowDataset, FlowBuildReport]]:
    """Entrega un builder de FlowDataset pequeño construido con build_flows."""

    def _factory() -> tuple[FlowDataset, FlowBuildReport]:
        trips = _clone_tripdataset(
            _tripdataset_validated_small_for_export_integration
        )

        return build_flows(
            trips,
            options=FlowBuildOptions(
                h3_resolution=8,
                group_by=None,
                time_aggregation="none",
                min_trips_per_flow=1,
                keep_flow_to_trips=False,
                require_validated=True,
            ),
        )

    return _factory


@pytest.fixture
def make_flowdataset_segmented(
    _tripdataset_ready_for_export_integration: TripDataset,
) -> Callable[..., tuple[FlowDataset, FlowBuildReport]]:
    """Entrega un builder parametrizable de FlowDataset segmentado construido con build_flows."""

    def _factory(
        *,
        h3_res: int = 7,
        g_by: list[str] | None = None,
        t_agg: str = "day",
        t_basis: str = "origin",
    ) -> tuple[FlowDataset, FlowBuildReport]:
        trips = _clone_tripdataset(
            _tripdataset_ready_for_export_integration
        )

        group_by_effective = (
            ["mode", "purpose", "user_gender"]
            if g_by is None
            else list(g_by)
        )

        return build_flows(
            trips,
            options=FlowBuildOptions(
                h3_resolution=h3_res,
                group_by=group_by_effective,
                time_aggregation=t_agg,
                time_basis=t_basis,
                min_trips_per_flow=2,
                keep_flow_to_trips=False,
                require_validated=True,
            ),
        )

    return _factory