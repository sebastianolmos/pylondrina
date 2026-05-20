from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Sequence

import pandas as pd
import pytest

from pylondrina.datasets import FlowDataset
from pylondrina.io.flows import (
    _flow_data_filename_for_storage,
    _flow_to_trips_filename_for_storage,
    _resolve_flows_artifact_paths,
)


# -----------------------------------------------------------------------------
# Factories mínimas reutilizables para helper-level y smoke tests
# -----------------------------------------------------------------------------


def _make_minimal_flows_df(*, n_repeat: int = 1) -> pd.DataFrame:
    """Construye una tabla pequeña de flows con contrato interno canónico."""
    base = pd.DataFrame(
        {
            "flow_id": ["f_0001", "f_0002", "f_0003"],
            "origin_h3_index": [
                "8828308281fffff",
                "8828308281fffff",
                "8828308285fffff",
            ],
            "destination_h3_index": [
                "8828308287fffff",
                "8828308289fffff",
                "8828308287fffff",
            ],
            "flow_count": [10, 6, 4],
            "flow_value": [10.0, 6.0, 4.0],
            "mode": ["bus", "metro", "bus"],
            "window_start_utc": pd.to_datetime(
                [
                    "2026-01-01T08:00:00Z",
                    "2026-01-01T08:00:00Z",
                    "2026-01-01T09:00:00Z",
                ],
                utc=True,
            ),
            "window_end_utc": pd.to_datetime(
                [
                    "2026-01-01T09:00:00Z",
                    "2026-01-01T09:00:00Z",
                    "2026-01-01T10:00:00Z",
                ],
                utc=True,
            ),
        }
    )

    if n_repeat <= 1:
        return base

    parts = []
    for i in range(n_repeat):
        part = base.copy(deep=True)
        part["flow_id"] = [
            f"{flow_id}_r{i:04d}"
            for flow_id in part["flow_id"]
        ]
        parts.append(part)

    return pd.concat(parts, ignore_index=True)


def _make_minimal_flow_to_trips_df(
    flow_ids: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Construye el auxiliar mínimo flow_to_trips ligado a flow_id existentes."""
    if flow_ids is None:
        flow_ids = ["f_0001", "f_0002", "f_0003"]

    rows = []
    for idx, flow_id in enumerate(flow_ids):
        rows.append(
            {
                "flow_id": flow_id,
                "movement_id": f"m_{idx * 2 + 1:04d}",
            }
        )
        rows.append(
            {
                "flow_id": flow_id,
                "movement_id": f"m_{idx * 2 + 2:04d}",
            }
        )

    return pd.DataFrame(rows)


def _make_minimal_flowdataset(
    *,
    validated: bool = True,
    with_flow_to_trips: bool = False,
    dataset_id: str = "flow-dset-smoke-001",
) -> FlowDataset:
    """Construye un FlowDataset pequeño para pruebas públicas de OP-11."""
    flows_df = _make_minimal_flows_df()

    flow_to_trips_df = (
        _make_minimal_flow_to_trips_df(flows_df["flow_id"].tolist())
        if with_flow_to_trips
        else None
    )

    aggregation_spec = {
        "h3_resolution": 8,
        "group_by": ["mode"],
        "time_aggregation": "hour",
        "time_basis": "origin",
        "min_trips_per_flow": 1,
    }

    metadata = {
        "dataset_id": dataset_id,
        "is_validated": bool(validated),
        "events": [],
        "notes": {"smoke_case": True},
    }

    provenance = {
        "derived_from": [
            {
                "source_type": "trips",
                "dataset_id": "trip-dset-origin-001",
            }
        ],
        "prior_events_summary": {"n_events": 2},
    }

    return FlowDataset(
        flows=flows_df,
        flow_to_trips=flow_to_trips_df,
        aggregation_spec=aggregation_spec,
        source_trips={"debug_only": True},
        metadata=metadata,
        provenance=provenance,
    )


# -----------------------------------------------------------------------------
# Helpers para artefactos formales mínimos leídos por helpers de OP-11
# -----------------------------------------------------------------------------


def _write_df_with_backend(
    df: pd.DataFrame,
    path: Path,
    *,
    storage_format: str,
) -> None:
    """Escribe un DataFrame en Parquet o Feather para materializar artefactos de test."""
    if storage_format == "parquet":
        df.to_parquet(path, index=False)
        return

    if storage_format == "feather":
        df.reset_index(drop=True).to_feather(path)
        return

    raise ValueError(f"storage_format inesperado: {storage_format!r}")


def _make_sidecar_payload(
    *,
    storage_format: str = "feather",
    include_flow_to_trips: bool = True,
    flows_df: pd.DataFrame | None = None,
    flow_to_trips_df: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Construye un sidecar formal mínimo coherente con el backend solicitado."""
    if storage_format == "parquet":
        data_name = "flows.parquet"
        aux_name = "flow_to_trips.parquet"
        storage_options = {"compression": "snappy"}
    elif storage_format == "feather":
        data_name = "flows.feather"
        aux_name = "flow_to_trips.feather"
        storage_options = {"compression": "lz4", "version": 2}
    else:
        raise ValueError(f"storage_format inesperado: {storage_format!r}")

    flows_df = (
        flows_df.copy(deep=True)
        if flows_df is not None
        else _make_minimal_flows_df()
    )

    flow_to_trips_df = (
        flow_to_trips_df.copy(deep=True)
        if flow_to_trips_df is not None
        else _make_minimal_flow_to_trips_df(flows_df["flow_id"].tolist())
    )

    payload: dict[str, Any] = {
        "dataset_type": "flows",
        "format": "golondrina",
        "layout_version": "1.1",
        "storage": {
            "format": storage_format,
            "options": storage_options,
        },
        "dataset_id": "dset_sidecar",
        "artifact_id": "art_sidecar",
        "files": {
            "data": data_name,
            "metadata": "flows.metadata.json",
            "flow_to_trips": (
                aux_name if include_flow_to_trips else None
            ),
        },
        "aggregation_spec": {
            "h3_resolution": 8,
            "group_by": ["mode"],
            "time_aggregation": "hour",
            "time_basis": "origin",
            "min_trips_per_flow": 1,
        },
        "provenance": {
            "derived_from": [
                {
                    "type": "trips",
                    "dataset_id": "trip_dset_001",
                }
            ],
        },
        "metadata": {
            "dataset_id": "dset_sidecar",
            "artifact_id": "art_sidecar",
            "is_validated": True,
            "events": [],
        },
        "tables": {
            "flows": {
                "n_rows": int(len(flows_df)),
                "n_cols": int(len(flows_df.columns)),
                "columns": list(flows_df.columns),
            },
            "flow_to_trips": {
                "n_rows": int(len(flow_to_trips_df)),
                "n_cols": int(len(flow_to_trips_df.columns)),
                "columns": list(flow_to_trips_df.columns),
            } if include_flow_to_trips else None,
        },
    }
    return payload


def _materialize_minimal_formal_flow_artifact(
    root: Path,
    *,
    storage_format: str = "feather",
    with_aux: bool = True,
) -> dict[str, Any]:
    """Materializa un bundle formal mínimo de flows dentro del directorio recibido."""
    root.mkdir(parents=True, exist_ok=True)
    paths = _resolve_flows_artifact_paths(root)

    data_filename = _flow_data_filename_for_storage(storage_format)
    aux_filename = _flow_to_trips_filename_for_storage(storage_format)

    data_path = root / data_filename
    aux_path = root / aux_filename

    flows_df = _make_minimal_flows_df()
    flow_to_trips_df = _make_minimal_flow_to_trips_df(
        flows_df["flow_id"].tolist()
    )

    _write_df_with_backend(
        flows_df,
        data_path,
        storage_format=storage_format,
    )

    if with_aux:
        _write_df_with_backend(
            flow_to_trips_df,
            aux_path,
            storage_format=storage_format,
        )

    payload = _make_sidecar_payload(
        storage_format=storage_format,
        include_flow_to_trips=with_aux,
        flows_df=flows_df,
        flow_to_trips_df=flow_to_trips_df,
    )

    paths.sidecar_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return {
        "paths": paths,
        "payload": payload,
        "data_path": data_path,
        "aux_path": aux_path,
    }


# -----------------------------------------------------------------------------
# Factories ricas para integración pública de OP-11
# -----------------------------------------------------------------------------


ORIGINS = [
    "8828308281fffff",
    "8828308283fffff",
    "8828308285fffff",
    "8828308287fffff",
]

DESTINATIONS = [
    "8828308291fffff",
    "8828308293fffff",
    "8828308295fffff",
    "8828308297fffff",
    "8828308299fffff",
]

MODES = ["bus", "metro", "car"]
PURPOSES = ["work", "education", "shopping", "leisure"]
DAY_TYPES = ["weekday", "weekend"]
GENDERS = ["female", "male"]
INCOME_Q = ["1", "3", "5"]
TIME_PERIODS = ["morning_peak", "midday", "afternoon_peak"]


def _make_rich_flows_df(*, repeat_blocks: int = 1) -> pd.DataFrame:
    """Construye una tabla de flows rica para escenarios de integración."""
    rows = []
    base_ts = pd.Timestamp("2026-04-01T06:00:00Z")

    idx = 0
    for rep in range(repeat_blocks):
        for origin in ORIGINS:
            for destination in DESTINATIONS:
                for mode in MODES:
                    for day_type in DAY_TYPES:
                        for gender in GENDERS:
                            purpose = PURPOSES[idx % len(PURPOSES)]
                            income_q = INCOME_Q[idx % len(INCOME_Q)]
                            time_period = TIME_PERIODS[idx % len(TIME_PERIODS)]

                            window_start = (
                                base_ts
                                + pd.Timedelta(hours=(idx % 10))
                                + pd.Timedelta(days=rep)
                            )
                            window_end = window_start + pd.Timedelta(hours=1)

                            flow_count = 5 + (idx % 17)
                            flow_value = round(
                                flow_count
                                * (
                                    1.0
                                    + (
                                        0.15
                                        if mode == "metro"
                                        else 0.05
                                        if mode == "bus"
                                        else 0.25
                                    )
                                ),
                                3,
                            )

                            rows.append(
                                {
                                    "flow_id": f"f_{rep:02d}_{idx:05d}",
                                    "origin_h3_index": origin,
                                    "destination_h3_index": destination,
                                    "flow_count": int(flow_count),
                                    "flow_value": float(flow_value),
                                    "mode": mode,
                                    "purpose": purpose,
                                    "day_type": day_type,
                                    "user_gender": gender,
                                    "income_quintile": income_q,
                                    "time_period": time_period,
                                    "window_start_utc": window_start,
                                    "window_end_utc": window_end,
                                    "avg_trip_weight": round(
                                        0.8 + (idx % 9) * 0.21,
                                        3,
                                    ),
                                    "segment_label": f"{mode}|{day_type}|{gender}",
                                }
                            )
                            idx += 1

    return pd.DataFrame(rows)


def _make_rich_flow_to_trips_df(
    flows_df: pd.DataFrame,
    *,
    links_per_flow: int = 3,
) -> pd.DataFrame:
    """Construye un auxiliar flow_to_trips amplio y alineado con flows ricos."""
    rows = []
    movement_counter = 0

    for _, row in flows_df.iterrows():
        for _ in range(links_per_flow):
            movement_counter += 1
            rows.append(
                {
                    "flow_id": row["flow_id"],
                    "movement_id": f"m_{movement_counter:07d}",
                }
            )

    return pd.DataFrame(rows)


def _make_rich_flowdataset(
    *,
    repeat_blocks: int = 1,
    with_trip_links: bool = False,
    validated: bool = False,
    dataset_id: str = "flow-dset-integration-001",
) -> FlowDataset:
    """Construye un FlowDataset rico con metadata, provenance y source_trips vivo."""
    flows_df = _make_rich_flows_df(repeat_blocks=repeat_blocks)

    flow_to_trips_df = (
        _make_rich_flow_to_trips_df(flows_df)
        if with_trip_links
        else None
    )

    aggregation_spec = {
        "h3_resolution": 8,
        "group_by": ["mode", "day_type", "user_gender"],
        "time_aggregation": "hour",
        "time_basis": "origin",
        "min_trips_per_flow": 1,
    }

    metadata = {
        "dataset_id": dataset_id,
        "is_validated": bool(validated),
        "events": [
            {
                "op": "build_flows",
                "ts_utc": "2026-04-01T12:00:00Z",
                "parameters": {
                    "h3_resolution": 8,
                    "group_by": ["mode", "day_type", "user_gender"],
                    "time_aggregation": "hour",
                    "time_basis": "origin",
                    "min_trips_per_flow": 1,
                },
                "summary": {
                    "n_flows": int(len(flows_df)),
                    "n_trips_in": int(len(flows_df) * 4),
                    "n_trips_aggregated": int(len(flows_df) * 4),
                    "n_trips_dropped": 0,
                    "n_flow_to_trips_rows": (
                        int(len(flow_to_trips_df))
                        if flow_to_trips_df is not None
                        else None
                    ),
                },
                "issues_summary": {
                    "counts": {
                        "info": 0,
                        "warning": 0,
                        "error": 0,
                    },
                    "top_codes": [],
                },
            }
        ],
        "notes": {"fixture": "integration_rich_flowdataset"},
    }

    provenance = {
        "derived_from": [
            {
                "source_type": "trips",
                "dataset_id": "trip-dset-origin-001",
                "schema_version": "1.1",
            }
        ],
        "prior_events_summary": {"n_events": 3},
    }

    return FlowDataset(
        flows=flows_df,
        flow_to_trips=flow_to_trips_df,
        aggregation_spec=aggregation_spec,
        source_trips={"debug": "in_memory_only"},
        metadata=metadata,
        provenance=provenance,
    )


# -----------------------------------------------------------------------------
# Utilidades de comparación, sidecar e issues
# -----------------------------------------------------------------------------


def _read_json(path: Path) -> dict[str, Any]:
    """Lee un JSON UTF-8 usado como sidecar o payload manipulado en tests."""
    return json.loads(path.read_text(encoding="utf-8"))


def _sort_df(df: pd.DataFrame, by: list[str]) -> pd.DataFrame:
    """Ordena un DataFrame por llaves estables antes de comparar."""
    return df.sort_values(by=by).reset_index(drop=True)


def _assert_df_equal_untyped(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    by: list[str],
) -> None:
    """Compara DataFrames preservando contenido sin acoplarse a dtype físico."""
    pd.testing.assert_frame_equal(
        _sort_df(left, by),
        _sort_df(right, by),
        check_dtype=False,
        check_categorical=False,
    )


def _issue_codes(report_or_issues: Any) -> list[str]:
    """Extrae códigos de issue desde un reporte o desde una secuencia de issues."""
    issues = (
        report_or_issues.issues
        if hasattr(report_or_issues, "issues")
        else report_or_issues
    )

    return [
        issue.code if hasattr(issue, "code") else issue.get("code")
        for issue in issues
    ]


def _assert_issue_present(issues_or_report: Any, code: str) -> None:
    """Verifica que un código de issue esté presente en la evidencia emitida."""
    codes = _issue_codes(issues_or_report)

    assert code in codes, (
        f"No se encontró el issue {code}. "
        f"Codes actuales: {codes}"
    )


def _assert_issue_absent(issues_or_report: Any, code: str) -> None:
    """Verifica que un código de issue no aparezca en la evidencia emitida."""
    codes = _issue_codes(issues_or_report)

    assert code not in codes, (
        f"Se encontró inesperadamente el issue {code}. "
        f"Codes actuales: {codes}"
    )


def _assert_json_dumpable(obj: Any, label: str = "object") -> None:
    """Verifica que un objeto pueda serializarse como JSON sin coerciones externas."""
    try:
        json.dumps(obj, ensure_ascii=False)
    except Exception as exc:
        raise AssertionError(f"{label} no es JSON-safe: {exc}") from exc


def _artifact_aux_filename(storage_format: str) -> str:
    """Retorna el nombre físico esperado del auxiliar flow_to_trips por backend."""
    if storage_format == "parquet":
        return "flow_to_trips.parquet"

    if storage_format == "feather":
        return "flow_to_trips.feather"

    raise ValueError(f"storage_format no soportado: {storage_format!r}")


# -----------------------------------------------------------------------------
# Fixtures expuestas a los archivos de tests de OP-11
# -----------------------------------------------------------------------------


@pytest.fixture
def minimal_flows_df() -> pd.DataFrame:
    """Entrega una tabla pequeña de flows para helper-level y smoke tests."""
    return _make_minimal_flows_df()


@pytest.fixture
def minimal_flow_to_trips_df(
    minimal_flows_df: pd.DataFrame,
) -> pd.DataFrame:
    """Entrega un auxiliar mínimo alineado con los flow_id del fixture base."""
    return _make_minimal_flow_to_trips_df(
        minimal_flows_df["flow_id"].tolist()
    )


@pytest.fixture
def minimal_flowdataset_factory() -> Callable[..., FlowDataset]:
    """Entrega una factory de FlowDataset pequeños para pruebas públicas de OP-11."""
    return _make_minimal_flowdataset


@pytest.fixture
def write_df_with_backend() -> Callable[..., None]:
    """Entrega el helper de escritura física Parquet/Feather para artefactos mínimos."""
    return _write_df_with_backend


@pytest.fixture
def sidecar_payload_factory() -> Callable[..., dict[str, Any]]:
    """Entrega una factory de sidecars mínimos coherentes con el backend de lectura."""
    return _make_sidecar_payload


@pytest.fixture
def formal_flow_artifact_factory() -> Callable[..., dict[str, Any]]:
    """Entrega una factory que materializa bundles formales mínimos en un Path dado."""
    return _materialize_minimal_formal_flow_artifact


@pytest.fixture
def rich_flows_df_factory() -> Callable[..., pd.DataFrame]:
    """Entrega una factory de tablas de flows ricas para integración."""
    return _make_rich_flows_df


@pytest.fixture
def rich_flow_to_trips_df_factory() -> Callable[..., pd.DataFrame]:
    """Entrega una factory de auxiliares flow_to_trips ligados a flows ricos."""
    return _make_rich_flow_to_trips_df


@pytest.fixture
def rich_flowdataset_factory() -> Callable[..., FlowDataset]:
    """Entrega una factory de FlowDataset ricos para integración pública de OP-11."""
    return _make_rich_flowdataset


@pytest.fixture
def rich_flowdataset_small(
    rich_flowdataset_factory: Callable[..., FlowDataset],
) -> FlowDataset:
    """Entrega el FlowDataset rico pequeño sin auxiliar usado en integración."""
    return rich_flowdataset_factory(
        repeat_blocks=1,
        with_trip_links=False,
        validated=False,
        dataset_id="flow-dset-small-001",
    )


@pytest.fixture
def rich_flowdataset_with_trip_links(
    rich_flowdataset_factory: Callable[..., FlowDataset],
) -> FlowDataset:
    """Entrega el FlowDataset rico pequeño con auxiliar usado en integración."""
    return rich_flowdataset_factory(
        repeat_blocks=1,
        with_trip_links=True,
        validated=False,
        dataset_id="flow-dset-links-001",
    )


@pytest.fixture
def read_json() -> Callable[[Path], dict[str, Any]]:
    """Entrega el helper de lectura de sidecars JSON usado en regresiones públicas."""
    return _read_json


@pytest.fixture
def sort_df() -> Callable[[pd.DataFrame, list[str]], pd.DataFrame]:
    """Entrega el helper de orden estable de DataFrames antes de compararlos."""
    return _sort_df


@pytest.fixture
def assert_df_equal_untyped() -> Callable[..., None]:
    """Entrega el helper de comparación de DataFrames sin acoplamiento a dtypes físicos."""
    return _assert_df_equal_untyped


@pytest.fixture
def issue_codes() -> Callable[[Any], list[str]]:
    """Entrega el helper unificado para extraer códigos de issue."""
    return _issue_codes


@pytest.fixture
def assert_issue_present() -> Callable[[Any, str], None]:
    """Entrega el helper para exigir la presencia de un código de issue."""
    return _assert_issue_present


@pytest.fixture
def assert_issue_absent() -> Callable[[Any, str], None]:
    """Entrega el helper para exigir la ausencia de un código de issue."""
    return _assert_issue_absent


@pytest.fixture
def assert_json_dumpable() -> Callable[[Any, str], None]:
    """Entrega el helper para verificar serialización JSON-safe."""
    return _assert_json_dumpable


@pytest.fixture
def artifact_aux_filename() -> Callable[[str], str]:
    """Entrega el helper que resuelve el nombre de flow_to_trips por backend."""
    return _artifact_aux_filename