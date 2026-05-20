from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from typing import Any

import pandas as pd
import pytest

from pylondrina.datasets import TraceDataset
from pylondrina.reports import Issue
from pylondrina.schema import FieldSpec, TraceSchema


TRACE_CORE_COLUMNS: tuple[str, ...] = (
    "point_id",
    "user_id",
    "time_utc",
    "latitude",
    "longitude",
)


def _issue_code(issue: Issue | Mapping[str, Any]) -> str | None:
    """Extrae el código de un issue representado como Issue o como mapping."""
    if hasattr(issue, "code"):
        return issue.code
    if isinstance(issue, Mapping):
        value = issue.get("code")
        return str(value) if value is not None else None
    return None


@pytest.fixture
def trace_core_columns() -> tuple[str, ...]:
    """Entrega el núcleo canónico que validate_traces exige en TraceDataset.data."""
    return TRACE_CORE_COLUMNS


@pytest.fixture
def make_trace_field() -> Callable[..., FieldSpec]:
    """Construye FieldSpec para schemas de validación de traces."""

    def _make_trace_field(
        name: str,
        dtype: str,
        *,
        required: bool = False,
        constraints: dict[str, Any] | None = None,
    ) -> FieldSpec:
        return FieldSpec(
            name=name,
            dtype=dtype,
            required=required,
            constraints=constraints or {},
        )

    return _make_trace_field


@pytest.fixture
def make_trace_schema() -> Callable[..., TraceSchema]:
    """Construye TraceSchema con campos, required, CRS, timezone y versión controlados."""

    def _make_trace_schema(
        fields: Sequence[FieldSpec],
        *,
        required: Sequence[str] | None = None,
        timezone: str | None = None,
        crs: str | None = "EPSG:4326",
        version: str = "trace-1.1-test",
    ) -> TraceSchema:
        return TraceSchema(
            version=version,
            fields={field.name: field for field in fields},
            required=list(required or []),
            timezone=timezone,
            crs=crs,
        )

    return _make_trace_schema


@pytest.fixture
def validate_trace_fields(make_trace_field: Callable[..., FieldSpec]) -> list[FieldSpec]:
    """Entrega campos base para validar traces con constraints representativas de OP-15."""
    return [
        make_trace_field("point_id", "string", required=True, constraints={"unique": True}),
        make_trace_field(
            "user_id",
            "string",
            required=True,
            constraints={"pattern": r"^u_\d{2}$"},
        ),
        make_trace_field(
            "time_utc",
            "datetime",
            required=True,
            constraints={"datetime": {"allow_naive": True}},
        ),
        make_trace_field(
            "latitude",
            "float",
            required=True,
            constraints={"range": {"min": -90.0, "max": 90.0}},
        ),
        make_trace_field(
            "longitude",
            "float",
            required=True,
            constraints={"range": {"min": -180.0, "max": 180.0}},
        ),
        make_trace_field(
            "visit_code",
            "string",
            required=False,
            constraints={"pattern": r"^v\d{3}$", "length": {"min": 4, "max": 4}},
        ),
        make_trace_field(
            "battery_pct",
            "int",
            required=False,
            constraints={"range": {"min": 0, "max": 100}},
        ),
        make_trace_field(
            "speed_mps",
            "float",
            required=False,
            constraints={"range": {"min": 0.0, "max": 80.0}},
        ),
        make_trace_field(
            "is_home",
            "bool",
            required=False,
            constraints={"nullable": False},
        ),
    ]


@pytest.fixture
def validate_trace_schema_base(
    validate_trace_fields: list[FieldSpec],
    make_trace_schema: Callable[..., TraceSchema],
    trace_core_columns: tuple[str, ...],
) -> TraceSchema:
    """Entrega el TraceSchema base para tests públicos y helper-level de OP-15."""
    return make_trace_schema(
        validate_trace_fields,
        required=trace_core_columns,
        timezone="UTC",
        version="trace-validate-1.1-test",
    )


@pytest.fixture
def valid_trace_df() -> pd.DataFrame:
    """Entrega un dataframe canónico válido y suficientemente rico para validate_traces."""
    rows: list[dict[str, Any]] = []

    for user_idx in range(2):
        for point_idx in range(4):
            row_idx = user_idx * 4 + point_idx
            rows.append(
                {
                    "point_id": f"p{row_idx:03d}",
                    "user_id": f"u_{user_idx + 1:02d}",
                    "time_utc": pd.Timestamp("2026-01-01T08:00:00")
                    + pd.Timedelta(days=user_idx, hours=point_idx),
                    "latitude": -33.45 - 0.002 * row_idx,
                    "longitude": -70.66 - 0.002 * row_idx,
                    "visit_code": f"v{row_idx:03d}",
                    "battery_pct": 95 - row_idx,
                    "speed_mps": float(point_idx) + 0.25,
                    "is_home": point_idx % 2 == 0,
                }
            )

    return pd.DataFrame(rows)


@pytest.fixture
def valid_trace_metadata() -> dict[str, Any]:
    """Entrega metadata inicial mínima para un TraceDataset aún no validado."""
    return {
        "dataset_id": "traces_validate_pytest",
        "events": [],
        "is_validated": False,
    }


@pytest.fixture
def valid_trace_provenance() -> dict[str, Any]:
    """Entrega provenance mínimo y JSON-safe para fixtures de OP-15."""
    return {
        "source": "pytest",
        "operation": "op15_validate_traces",
    }


@pytest.fixture
def make_trace_dataset() -> Callable[..., TraceDataset]:
    """Construye TraceDataset con copias defensivas de data, metadata y provenance."""

    def _make_trace_dataset(
        df: pd.DataFrame,
        schema: TraceSchema,
        *,
        metadata: dict[str, Any] | None = None,
        provenance: dict[str, Any] | None = None,
    ) -> TraceDataset:
        return TraceDataset(
            data=df.copy(deep=True),
            schema=schema,
            metadata=deepcopy(metadata or {}),
            provenance=deepcopy(provenance or {}),
        )

    return _make_trace_dataset


@pytest.fixture
def valid_traces(
    valid_trace_df: pd.DataFrame,
    validate_trace_schema_base: TraceSchema,
    valid_trace_metadata: dict[str, Any],
    valid_trace_provenance: dict[str, Any],
    make_trace_dataset: Callable[..., TraceDataset],
) -> TraceDataset:
    """Entrega un TraceDataset canónico válido para ejecutar validate_traces."""
    return make_trace_dataset(
        valid_trace_df,
        validate_trace_schema_base,
        metadata=valid_trace_metadata,
        provenance=valid_trace_provenance,
    )


@pytest.fixture
def clone_tracedataset() -> Callable[[TraceDataset], TraceDataset]:
    """Entrega un clon profundo de TraceDataset para evitar contaminación entre tests."""

    def _clone_tracedataset(traces: TraceDataset) -> TraceDataset:
        return TraceDataset(
            data=traces.data.copy(deep=True),
            schema=deepcopy(traces.schema),
            metadata=deepcopy(traces.metadata),
            provenance=deepcopy(traces.provenance),
        )

    return _clone_tracedataset


@pytest.fixture
def get_issue_codes() -> Callable[[Sequence[Issue | Mapping[str, Any]]], list[str | None]]:
    """Entrega un helper para extraer códigos desde una secuencia de issues."""

    def _get_issue_codes(issues: Sequence[Issue | Mapping[str, Any]]) -> list[str | None]:
        return [_issue_code(issue) for issue in issues]

    return _get_issue_codes


@pytest.fixture
def assert_issue_present(
    get_issue_codes: Callable[[Sequence[Issue | Mapping[str, Any]]], list[str | None]],
) -> Callable[[Sequence[Issue | Mapping[str, Any]], str], None]:
    """Entrega un assert reutilizable para exigir la presencia de un issue code."""

    def _assert_issue_present(issues: Sequence[Issue | Mapping[str, Any]], code: str) -> None:
        codes = get_issue_codes(issues)
        assert code in codes, f"No se encontró el issue {code}. Codes actuales: {codes}"

    return _assert_issue_present


@pytest.fixture
def assert_issue_absent(
    get_issue_codes: Callable[[Sequence[Issue | Mapping[str, Any]]], list[str | None]],
) -> Callable[[Sequence[Issue | Mapping[str, Any]], str], None]:
    """Entrega un assert reutilizable para exigir ausencia de un issue code."""

    def _assert_issue_absent(issues: Sequence[Issue | Mapping[str, Any]], code: str) -> None:
        codes = get_issue_codes(issues)
        assert code not in codes, (
            f"Se encontró inesperadamente el issue {code}. Codes actuales: {codes}"
        )

    return _assert_issue_absent


@pytest.fixture
def assert_json_safe() -> Callable[[Any, str], None]:
    """Entrega un assert para verificar serialización JSON sin coerción externa."""

    def _assert_json_safe(obj: Any, label: str = "object") -> None:
        try:
            json.dumps(obj)
        except TypeError as exc:
            raise AssertionError(f"{label} no es JSON-safe: {exc}") from exc

    return _assert_json_safe