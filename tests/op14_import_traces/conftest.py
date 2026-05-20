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
    """Extrae el código de un issue, tanto si viene como objeto Issue como si viene como mapping."""
    if hasattr(issue, "code"):
        return issue.code
    if isinstance(issue, Mapping):
        value = issue.get("code")
        return str(value) if value is not None else None
    return None


@pytest.fixture
def trace_core_columns() -> tuple[str, ...]:
    """Entrega el núcleo canónico de columnas que OP-14 debe materializar."""
    return TRACE_CORE_COLUMNS


@pytest.fixture
def make_trace_field() -> Callable[..., FieldSpec]:
    """Construye FieldSpec de traces con la misma forma usada en los notebooks."""

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
    """Construye TraceSchema desde una lista de FieldSpec para fixtures de OP-14."""

    def _make_trace_schema(
        fields: Sequence[FieldSpec],
        *,
        required: Sequence[str] | None = None,
        timezone: str | None = "UTC",
        crs: str | None = "EPSG:4326",
        version: str = "golondrina-trace-1.1-test",
    ) -> TraceSchema:
        return TraceSchema(
            version=version,
            fields={field.name: field for field in fields},
            required=list(required or []),
            crs=crs,
            timezone=timezone,
        )

    return _make_trace_schema


@pytest.fixture
def trace_schema_base(
    make_trace_field: Callable[..., FieldSpec],
    make_trace_schema: Callable[..., TraceSchema],
) -> TraceSchema:
    """Entrega el TraceSchema rico usado por los tests públicos de integración de OP-14."""
    fields = [
        make_trace_field(
            "point_id",
            "string",
            required=True,
            constraints={"unique": True, "length": {"min": 2, "max": 20}},
        ),
        make_trace_field(
            "user_id",
            "string",
            required=True,
            constraints={"pattern": r"^u_\d{2}$", "length": {"min": 4, "max": 4}},
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
            constraints={"pattern": r"^v\d{3}$", "length": {"min": 4, "max": 4}},
        ),
        make_trace_field(
            "battery_pct",
            "int",
            constraints={"range": {"min": 0, "max": 100}},
        ),
        make_trace_field(
            "speed_mps",
            "float",
            constraints={"range": {"min": 0.0, "max": 80.0}},
        ),
        make_trace_field(
            "is_home",
            "bool",
            constraints={"nullable": False},
        ),
    ]

    return make_trace_schema(
        fields,
        required=TRACE_CORE_COLUMNS,
        timezone="UTC",
    )


@pytest.fixture
def base_import_fields(make_trace_field: Callable[..., FieldSpec]) -> list[FieldSpec]:
    """Entrega campos mínimos de OP-14 usados en tests helper-level."""
    return [
        make_trace_field("point_id", "string", required=True, constraints={"unique": True}),
        make_trace_field("user_id", "string", required=True),
        make_trace_field("time_utc", "datetime", required=True),
        make_trace_field(
            "latitude",
            "float",
            required=True,
            constraints={"range": {"min": -90, "max": 90}},
        ),
        make_trace_field(
            "longitude",
            "float",
            required=True,
            constraints={"range": {"min": -180, "max": 180}},
        ),
        make_trace_field(
            "location_category",
            "string",
            required=False,
            constraints={"length": {"max": 40}},
        ),
    ]


@pytest.fixture
def base_import_schema(
    base_import_fields: list[FieldSpec],
    make_trace_schema: Callable[..., TraceSchema],
) -> TraceSchema:
    """Entrega el TraceSchema mínimo del notebook helper-level de OP-14."""
    return make_trace_schema(
        base_import_fields,
        required=TRACE_CORE_COLUMNS,
        timezone=None,
        version="trace-1.1",
    )


@pytest.fixture
def field_correspondence_base() -> dict[str, str]:
    """Entrega la correspondencia fuente-canónico usada en los tests públicos de OP-14."""
    return {
        "user_id": "uid",
        "time_utc": "ts_local",
        "latitude": "lat_src",
        "longitude": "lon_src",
    }


@pytest.fixture
def raw_to_canonical_helper() -> dict[str, str]:
    """Entrega la correspondencia fuente-canónico usada en los tests helper-level de OP-14."""
    return {
        "user_id": "uid",
        "time_utc": "when",
        "latitude": "lat",
        "longitude": "lon",
        "location_category": "poi_cat",
    }


@pytest.fixture
def make_raw_points_df() -> Callable[..., pd.DataFrame]:
    """Construye dataframes crudos de puntos con núcleo mapeable y campos extra."""

    def _make_raw_points_df(n_users: int = 3, points_per_user: int = 4) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []

        for user_idx in range(n_users):
            for point_idx in range(points_per_user):
                row_idx = user_idx * points_per_user + point_idx

                rows.append(
                    {
                        "uid": f"u_{user_idx + 1:02d}",
                        "ts_local": (
                            f"2026-01-{user_idx + 1:02d} "
                            f"{8 + point_idx:02d}:{row_idx % 6:02d}:00"
                        ),
                        "lat_src": -33.45 - 0.002 * row_idx,
                        "lon_src": -70.66 - 0.002 * row_idx,
                        "visit_code": f"v{row_idx:03d}",
                        "battery_pct": 95 - row_idx,
                        "speed_mps": float(point_idx) + 0.25,
                        "is_home": "true" if point_idx % 2 == 0 else "false",
                        "device_vendor": "acme",
                        "poi_name": f"poi_{row_idx}",
                        "sample_weight": round(1.0 + 0.1 * point_idx, 2),
                    }
                )

        return pd.DataFrame(rows)

    return _make_raw_points_df


@pytest.fixture
def raw_points_df(make_raw_points_df: Callable[..., pd.DataFrame]) -> pd.DataFrame:
    """Entrega el dataframe crudo base de integración para OP-14."""
    return make_raw_points_df(n_users=3, points_per_user=5)


@pytest.fixture
def raw_points_df_large(make_raw_points_df: Callable[..., pd.DataFrame]) -> pd.DataFrame:
    """Entrega un dataframe crudo más grande para el camino principal de OP-14."""
    return make_raw_points_df(n_users=4, points_per_user=6)


@pytest.fixture
def raw_points_helper_df() -> pd.DataFrame:
    """Entrega el dataframe crudo mínimo usado por los tests helper-level de OP-14."""
    return pd.DataFrame(
        {
            "uid": ["u1", "u2"],
            "when": ["2026-01-01 08:00:00", "2026-01-01 09:30:00"],
            "lat": [-33.45, -33.46],
            "lon": [-70.66, -70.67],
            "poi_cat": ["home", "work"],
            "noise": ["x", "y"],
        }
    )


@pytest.fixture
def make_trace_dataset() -> Callable[..., TraceDataset]:
    """Construye TraceDataset defensivamente copiado para pruebas de OP-14."""

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
    """Entrega un assert para verificar que un objeto ya sea serializable a JSON sin coerción externa."""

    def _assert_json_safe(obj: Any, label: str = "object") -> None:
        try:
            json.dumps(obj)
        except TypeError as exc:
            raise AssertionError(f"{label} no es JSON-safe: {exc}") from exc

    return _assert_json_safe