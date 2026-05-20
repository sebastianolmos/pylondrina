from __future__ import annotations

import copy
import json
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest

from pylondrina.datasets import FlowDataset
from pylondrina.errors import ExportError
from pylondrina.io.flows import (
    WriteFlowsOptions,
    write_flows,
)


# ---------------------------------------------------------------------------
# Helpers locales de lectura / inspección
# ---------------------------------------------------------------------------

def _read_json(path: Path) -> dict:
    """Lee un archivo JSON y retorna su contenido como diccionario."""
    return json.loads(path.read_text(encoding="utf-8"))


def _parquet_has_dictionary_encoding(
    parquet_path: Path,
    column_name: str,
) -> bool:
    """Indica si una columna Parquet usa alguna codificación dictionary."""
    parquet_file = pq.ParquetFile(parquet_path)

    try:
        names = parquet_file.schema_arrow.names
        column_idx = names.index(column_name)

        encodings = {
            str(encoding).upper()
            for encoding in parquet_file.metadata.row_group(0)
            .column(column_idx)
            .encodings
        }

        return any("DICTIONARY" in encoding for encoding in encodings)
    finally:
        parquet_file.close()


# ---------------------------------------------------------------------------
# Factories ricas locales al archivo de integración
# ---------------------------------------------------------------------------

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
    """Construye una tabla de flows rica y suficientemente variada para integración."""
    rows: list[dict] = []
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
                            time_period = TIME_PERIODS[
                                idx % len(TIME_PERIODS)
                            ]

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
                                    "segment_label": (
                                        f"{mode}|{day_type}|{gender}"
                                    ),
                                }
                            )
                            idx += 1

    return pd.DataFrame(rows)


def _make_flow_to_trips_df(
    flows_df: pd.DataFrame,
    *,
    links_per_flow: int = 3,
) -> pd.DataFrame:
    """Construye una tabla auxiliar flow_to_trips alineada con los flows de entrada."""
    rows: list[dict[str, str]] = []
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
    """Construye un FlowDataset rico para pruebas públicas de integración de OP-10."""
    flows_df = _make_rich_flows_df(repeat_blocks=repeat_blocks)

    flow_to_trips_df = (
        _make_flow_to_trips_df(flows_df)
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
        "notes": {
            "fixture": "integration_rich_flowdataset",
        },
    }

    provenance = {
        "derived_from": [
            {
                "source_type": "trips",
                "dataset_id": "trip-dset-origin-001",
                "schema_version": "1.1",
            }
        ],
        "prior_events_summary": {
            "n_events": 3,
        },
    }

    return FlowDataset(
        flows=flows_df,
        flow_to_trips=flow_to_trips_df,
        aggregation_spec=aggregation_spec,
        source_trips={"debug": "in_memory_only"},
        metadata=metadata,
        provenance=provenance,
    )


# ---------------------------------------------------------------------------
# Tests públicos de integración OP-10 write_flows
# ---------------------------------------------------------------------------

def test_write_flows_persists_rich_parquet_bundle_and_aligns_report_metadata(
    tmp_path: Path,
) -> None:
    """Verifica escritura Parquet rica, bundle formal, sidecar y evento alineado."""
    artifact_path = tmp_path / "flows_write_happy"

    flows = _make_rich_flowdataset(
        repeat_blocks=1,
        with_trip_links=False,
        validated=False,
        dataset_id="flow-dset-small-001",
    )
    flows_before = flows.flows.copy(deep=True)
    source_trips_before = copy.deepcopy(flows.source_trips)

    report = write_flows(
        flows,
        artifact_path,
        options=WriteFlowsOptions(
            mode="error_if_exists",
            storage_format="parquet",
            parquet_compression="snappy",
            normalize_artifact_dir=True,
            write_flow_to_trips=False,
        ),
    )

    effective_root = artifact_path.with_name(
        artifact_path.name + ".golondrina"
    )
    sidecar_path = effective_root / "flows.metadata.json"
    sidecar = _read_json(sidecar_path)

    assert report.ok is True

    # Layout final.
    assert effective_root.exists()
    assert (effective_root / "flows.parquet").exists()
    assert sidecar_path.exists()
    assert not (effective_root / "flow_to_trips.parquet").exists()

    # Summary.
    assert report.summary["n_flows"] == len(flows.flows)
    assert report.summary["n_flow_to_trips"] is None
    assert report.summary["dataset_id"] == flows.metadata["dataset_id"]
    assert report.summary["artifact_id"] == flows.metadata["artifact_id"]
    assert report.summary["path"] == str(effective_root)
    assert set(report.summary["files_written"]) == {
        "flows.parquet",
        "flows.metadata.json",
    }

    # Parameters efectivos.
    assert report.parameters["path"] == str(effective_root)
    assert report.parameters["mode"] == "error_if_exists"
    assert report.parameters["storage_format"] == "parquet"
    assert report.parameters["parquet_compression"] == "snappy"
    assert report.parameters["normalize_artifact_dir"] is True
    assert report.parameters["write_flow_to_trips"] is False

    # Side effects en memoria.
    assert "artifact_id" in flows.metadata
    assert flows.source_trips == source_trips_before
    pd.testing.assert_frame_equal(flows.flows, flows_before)

    # Sidecar formal.
    assert sidecar["dataset_type"] == "flows"
    assert sidecar["format"] == "golondrina"
    assert sidecar["layout_version"] == "1.1"
    assert sidecar["storage"]["format"] == "parquet"
    assert sidecar["files"]["data"] == "flows.parquet"
    assert sidecar["files"]["metadata"] == "flows.metadata.json"
    assert sidecar["files"]["flow_to_trips"] is None
    assert sidecar["dataset_id"] == flows.metadata["dataset_id"]
    assert sidecar["artifact_id"] == flows.metadata["artifact_id"]

    # source_trips es referencia viva y no debe persistirse.
    assert "source_trips" not in sidecar
    assert "source_trips" not in sidecar["metadata"]

    # Evento write alineado con el reporte.
    event = flows.metadata["events"][-1]
    assert event["op"] == "write_flows"
    assert event["parameters"] == report.parameters
    assert event["summary"] == report.summary
    assert "issues_summary" in event


def test_write_flows_persists_auxiliary_and_dictionary_encoded_group_fields_in_parquet(
    tmp_path: Path,
) -> None:
    """Verifica escritura con auxiliar y dictionary encoding en campos group_by."""
    artifact_path = tmp_path / "flows_with_aux"

    flows = _make_rich_flowdataset(
        repeat_blocks=20,
        with_trip_links=True,
        validated=False,
        dataset_id="flow-dset-dict-001",
    )

    report = write_flows(
        flows,
        artifact_path,
        options=WriteFlowsOptions(
            mode="error_if_exists",
            storage_format="parquet",
            parquet_compression="snappy",
            normalize_artifact_dir=False,
            write_flow_to_trips=True,
        ),
    )

    good_parquet = artifact_path / "flows.parquet"
    aux_parquet = artifact_path / "flow_to_trips.parquet"

    assert report.ok is True
    assert good_parquet.exists()
    assert aux_parquet.exists()

    assert report.summary["n_flow_to_trips"] == len(flows.flow_to_trips)
    assert "flow_to_trips.parquet" in report.summary["files_written"]

    # Dictionary encoding observable en columnas categóricas de group_by.
    assert _parquet_has_dictionary_encoding(good_parquet, "mode") is True
    assert _parquet_has_dictionary_encoding(good_parquet, "day_type") is True
    assert _parquet_has_dictionary_encoding(
        good_parquet,
        "user_gender",
    ) is True


def test_write_flows_rejects_destination_collision_in_error_if_exists_mode(
    tmp_path: Path,
) -> None:
    """Verifica error público al intentar escribir dos veces sobre el mismo destino."""
    artifact_path = tmp_path / "flows_collision"

    flows = _make_rich_flowdataset(
        repeat_blocks=1,
        with_trip_links=False,
        validated=False,
        dataset_id="flow-dset-collision-001",
    )

    report_ok = write_flows(
        flows,
        artifact_path,
        options=WriteFlowsOptions(
            mode="error_if_exists",
            storage_format="parquet",
            normalize_artifact_dir=False,
            write_flow_to_trips=False,
        ),
    )

    assert report_ok.ok is True
    assert (artifact_path / "flows.parquet").exists()

    with pytest.raises(ExportError):
        write_flows(
            _make_rich_flowdataset(
                repeat_blocks=1,
                with_trip_links=False,
                validated=False,
                dataset_id="flow-dset-collision-002",
            ),
            artifact_path,
            options=WriteFlowsOptions(
                mode="error_if_exists",
                storage_format="parquet",
                normalize_artifact_dir=False,
                write_flow_to_trips=False,
            ),
        )


def test_write_flows_uses_default_feather_backend_and_preserves_live_state(
    tmp_path: Path,
) -> None:
    """Verifica backend Feather por defecto, sidecar coherente y no mutación tabular."""
    artifact_path = tmp_path / "flows_write_default_feather"

    flows = _make_rich_flowdataset(
        repeat_blocks=1,
        with_trip_links=False,
        validated=False,
        dataset_id="flow-dset-default-feather-001",
    )
    flows_before = flows.flows.copy(deep=True)
    source_trips_before = copy.deepcopy(flows.source_trips)

    report = write_flows(
        flows,
        artifact_path,
        options=WriteFlowsOptions(
            mode="error_if_exists",
            normalize_artifact_dir=False,
            write_flow_to_trips=False,
            feather_compression="uncompressed",
        ),
    )

    sidecar_path = artifact_path / "flows.metadata.json"
    sidecar = _read_json(sidecar_path)

    assert report.ok is True

    # Layout físico esperado.
    assert artifact_path.exists()
    assert (artifact_path / "flows.feather").exists()
    assert not (artifact_path / "flows.parquet").exists()
    assert sidecar_path.exists()
    assert not (artifact_path / "flow_to_trips.feather").exists()

    # Parameters.
    assert report.parameters["storage_format"] == "feather"
    assert report.parameters["feather_compression"] == "uncompressed"
    assert report.parameters["write_flow_to_trips"] is False

    # Summary.
    assert report.summary["n_flows"] == len(flows.flows)
    assert report.summary["n_flow_to_trips"] is None
    assert report.summary["dataset_id"] == flows.metadata["dataset_id"]
    assert report.summary["artifact_id"] == flows.metadata["artifact_id"]
    assert report.summary["path"] == str(artifact_path)
    assert set(report.summary["files_written"]) == {
        "flows.feather",
        "flows.metadata.json",
    }

    # No mutación del estado vivo relevante.
    pd.testing.assert_frame_equal(flows.flows, flows_before)
    assert flows.source_trips == source_trips_before

    # Sidecar consistente con backend Feather.
    assert sidecar["dataset_type"] == "flows"
    assert sidecar["format"] == "golondrina"
    assert sidecar["layout_version"] == "1.1"
    assert sidecar["storage"]["format"] == "feather"
    assert sidecar["storage"]["options"]["compression"] == "uncompressed"
    assert sidecar["storage"]["options"]["version"] == 2
    assert sidecar["files"]["data"] == "flows.feather"
    assert sidecar["files"]["metadata"] == "flows.metadata.json"
    assert sidecar["files"]["flow_to_trips"] is None
    assert sidecar["dataset_id"] == flows.metadata["dataset_id"]
    assert sidecar["artifact_id"] == flows.metadata["artifact_id"]

    # Evento write alineado.
    event = flows.metadata["events"][-1]
    assert event["op"] == "write_flows"
    assert event["parameters"] == report.parameters
    assert event["summary"] == report.summary
    assert "issues_summary" in event


def test_write_flows_explicit_parquet_backend_remains_supported(
    tmp_path: Path,
) -> None:
    """Verifica compatibilidad explícita de Parquet tras adoptar Feather como default."""
    artifact_path = tmp_path / "flows_write_explicit_parquet"

    flows = _make_rich_flowdataset(
        repeat_blocks=1,
        with_trip_links=False,
        validated=False,
        dataset_id="flow-dset-explicit-parquet-001",
    )
    flows_before = flows.flows.copy(deep=True)

    report = write_flows(
        flows,
        artifact_path,
        options=WriteFlowsOptions(
            mode="error_if_exists",
            storage_format="parquet",
            parquet_compression="snappy",
            normalize_artifact_dir=False,
            write_flow_to_trips=False,
        ),
    )

    sidecar_path = artifact_path / "flows.metadata.json"
    sidecar = _read_json(sidecar_path)

    assert report.ok is True

    # Layout físico esperado.
    assert artifact_path.exists()
    assert (artifact_path / "flows.parquet").exists()
    assert not (artifact_path / "flows.feather").exists()
    assert sidecar_path.exists()

    # Parameters.
    assert report.parameters["storage_format"] == "parquet"
    assert report.parameters["parquet_compression"] == "snappy"
    assert report.parameters["write_flow_to_trips"] is False

    # Summary.
    assert report.summary["n_flows"] == len(flows.flows)
    assert report.summary["n_flow_to_trips"] is None
    assert set(report.summary["files_written"]) == {
        "flows.parquet",
        "flows.metadata.json",
    }

    # No mutación de tabla principal.
    pd.testing.assert_frame_equal(flows.flows, flows_before)

    # Sidecar consistente.
    assert sidecar["storage"]["format"] == "parquet"
    assert sidecar["storage"]["options"]["compression"] == "snappy"
    assert sidecar["files"]["data"] == "flows.parquet"
    assert sidecar["files"]["metadata"] == "flows.metadata.json"
    assert sidecar["files"]["flow_to_trips"] is None

    # Evento write alineado.
    event = flows.metadata["events"][-1]
    assert event["op"] == "write_flows"
    assert event["parameters"] == report.parameters
    assert event["summary"] == report.summary
    assert "issues_summary" in event