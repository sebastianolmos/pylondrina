from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from pylondrina.datasets import FlowDataset
from pylondrina.errors import ExportError
from pylondrina.io.flows import (
    WriteFlowsOptions,
    _build_write_flows_summary,
    _collect_flow_arrow_categorical_fields,
    _freeze_flow_write_snapshot,
    _prepare_flows_df_for_arrow_write,
    _resolve_flows_artifact_paths,
    _validate_write_contract,
)
from pylondrina.reports import Issue


def _issue_codes(issues: list[Issue]) -> list[str]:
    """Retorna los códigos de issue emitidos por los helpers bajo prueba."""
    return [issue.code for issue in issues]


def _assert_json_dumpable(obj: Any) -> None:
    """Verifica que un objeto pueda serializarse a JSON sin error."""
    json.dumps(obj, ensure_ascii=False)


def test_collect_flow_arrow_categorical_fields_detects_valid_segment_columns(
    flows_df_minimal: pd.DataFrame,
) -> None:
    """Verifica selección de campos categóricos de segmentación para escritura Arrow."""
    categorical_fields = _collect_flow_arrow_categorical_fields(
        flows_df_minimal,
        aggregation_spec={
            "group_by": [
                "mode",          # sí corresponde
                "flow_id",       # excluido por contrato
                "flow_count",    # numérico
                "missing_field", # inexistente
            ]
        },
    )

    categorical_fields_str = _collect_flow_arrow_categorical_fields(
        flows_df_minimal,
        aggregation_spec={"group_by": "mode"},
    )

    assert categorical_fields == ["mode"]
    assert categorical_fields_str == ["mode"]


def test_prepare_flows_df_for_arrow_write_casts_categoricals_without_mutating_input(
    flows_df_minimal: pd.DataFrame,
) -> None:
    """Verifica copia defensiva, conversión categórica y remoción de categorías no usadas."""
    df_with_categorical = flows_df_minimal.copy(deep=True)
    df_with_categorical["mode"] = pd.Categorical(
        df_with_categorical["mode"],
        categories=["bus", "metro", "train"],
    )

    prepared = _prepare_flows_df_for_arrow_write(
        df_with_categorical,
        categorical_fields=["mode"],
    )

    assert prepared is not df_with_categorical
    assert isinstance(prepared["mode"].dtype, pd.CategoricalDtype)
    assert list(prepared["mode"].cat.categories) == ["bus", "metro"]

    # El input categórico original no debe mutarse.
    assert list(df_with_categorical["mode"].cat.categories) == [
        "bus",
        "metro",
        "train",
    ]

    df_plain = flows_df_minimal.copy(deep=True)
    df_plain_before = df_plain.copy(deep=True)

    prepared_plain = _prepare_flows_df_for_arrow_write(
        df_plain,
        categorical_fields=["mode"],
    )

    assert isinstance(prepared_plain["mode"].dtype, pd.CategoricalDtype)
    pd.testing.assert_frame_equal(df_plain, df_plain_before)


def test_validate_write_contract_accepts_supported_backends(
    tmp_path: Path,
    make_flowdataset_minimal: Callable[..., FlowDataset],
) -> None:
    """Verifica que el contrato de escritura acepte Feather y Parquet válidos."""
    flows = make_flowdataset_minimal()

    issues_feather: list[Issue] = []
    _validate_write_contract(
        flows,
        tmp_path / "fake_feather.golondrina",
        WriteFlowsOptions(storage_format="feather"),
        issues=issues_feather,
    )

    issues_parquet: list[Issue] = []
    _validate_write_contract(
        flows,
        tmp_path / "fake_parquet.golondrina",
        WriteFlowsOptions(
            storage_format="parquet",
            parquet_compression="snappy",
        ),
        issues=issues_parquet,
    )

    assert issues_feather == []
    assert issues_parquet == []


def test_validate_write_contract_rejects_invalid_dataset(
    tmp_path: Path,
) -> None:
    """Verifica abort temprano cuando el input no es un FlowDataset interpretable."""
    issues: list[Issue] = []

    with pytest.raises(ExportError) as exc_info:
        _validate_write_contract(
            object(),  # type: ignore[arg-type]
            tmp_path / "fake_artifact.golondrina",
            WriteFlowsOptions(),
            issues=issues,
        )

    assert exc_info.value.code == "WRITE_FLOWS.INPUT.INVALID_DATASET"
    assert _issue_codes(issues) == ["WRITE_FLOWS.INPUT.INVALID_DATASET"]


def test_validate_write_contract_rejects_invalid_mode(
    tmp_path: Path,
    make_flowdataset_minimal: Callable[..., FlowDataset],
) -> None:
    """Verifica rechazo de una política de colisión fuera del contrato cerrado."""
    flows = make_flowdataset_minimal()
    issues: list[Issue] = []

    with pytest.raises(ExportError) as exc_info:
        _validate_write_contract(
            flows,
            tmp_path / "fake_artifact.golondrina",
            WriteFlowsOptions(mode="append"),  # type: ignore[arg-type]
            issues=issues,
        )

    assert exc_info.value.code == "WRITE_FLOWS.OPTIONS.INVALID_MODE"
    assert _issue_codes(issues) == ["WRITE_FLOWS.OPTIONS.INVALID_MODE"]


def test_validate_write_contract_rejects_unsupported_backend_and_compressions(
    tmp_path: Path,
    make_flowdataset_minimal: Callable[..., FlowDataset],
) -> None:
    """Verifica rechazo de backend no soportado y compresiones inválidas."""
    flows = make_flowdataset_minimal()

    issues_backend: list[Issue] = []
    with pytest.raises(ExportError) as exc_backend:
        _validate_write_contract(
            flows,
            tmp_path / "fake_invalid_backend.golondrina",
            WriteFlowsOptions(storage_format="orc"),  # type: ignore[arg-type]
            issues=issues_backend,
        )

    assert exc_backend.value.code == "WRITE_FLOWS.OPTIONS.UNSUPPORTED_STORAGE_FORMAT"
    assert _issue_codes(issues_backend) == [
        "WRITE_FLOWS.OPTIONS.UNSUPPORTED_STORAGE_FORMAT"
    ]

    issues_parquet_comp: list[Issue] = []
    with pytest.raises(ExportError) as exc_parquet:
        _validate_write_contract(
            flows,
            tmp_path / "fake_invalid_parquet_comp.golondrina",
            WriteFlowsOptions(
                storage_format="parquet",
                parquet_compression="xz",  # type: ignore[arg-type]
            ),
            issues=issues_parquet_comp,
        )

    assert exc_parquet.value.code == "WRITE_FLOWS.OPTIONS.UNSUPPORTED_STORAGE_FORMAT"
    assert _issue_codes(issues_parquet_comp) == [
        "WRITE_FLOWS.OPTIONS.UNSUPPORTED_STORAGE_FORMAT"
    ]

    issues_feather_comp: list[Issue] = []
    with pytest.raises(ExportError) as exc_feather:
        _validate_write_contract(
            flows,
            tmp_path / "fake_invalid_feather_comp.golondrina",
            WriteFlowsOptions(
                storage_format="feather",
                feather_compression="gzip",  # type: ignore[arg-type]
            ),
            issues=issues_feather_comp,
        )

    assert exc_feather.value.code == "WRITE_FLOWS.OPTIONS.UNSUPPORTED_STORAGE_FORMAT"
    assert _issue_codes(issues_feather_comp) == [
        "WRITE_FLOWS.OPTIONS.UNSUPPORTED_STORAGE_FORMAT"
    ]


def test_freeze_flow_write_snapshot_uses_default_feather_and_persists_auxiliary(
    tmp_path: Path,
    flow_to_trips_df_minimal: pd.DataFrame,
    make_flowdataset_minimal: Callable[..., FlowDataset],
) -> None:
    """Verifica snapshot Feather, preservación de dataset_id y persistencia del auxiliar."""
    flows = make_flowdataset_minimal(
        include_dataset_id=True,
        include_artifact_id=True,
        include_aux=True,
    )

    paths = _resolve_flows_artifact_paths(
        tmp_path / "fake_snapshot_feather.golondrina"
    )

    snapshot = _freeze_flow_write_snapshot(
        flows,
        paths,
        WriteFlowsOptions(),
        existing_issues=[],
    )

    assert snapshot.dataset_id_status == "preserved"
    assert snapshot.dataset_id == flows.metadata["dataset_id"]

    assert snapshot.artifact_id.startswith("art_")
    assert snapshot.artifact_id != "art_existing"

    assert snapshot.files_written == [
        "flows.feather",
        "flows.metadata.json",
        "flow_to_trips.feather",
    ]
    assert snapshot.n_flow_to_trips == len(flow_to_trips_df_minimal)

    assert snapshot.sidecar_payload["storage"]["format"] == "feather"
    assert snapshot.sidecar_payload["storage"]["options"] == {
        "compression": "lz4",
        "version": 2,
    }

    assert snapshot.sidecar_payload["files"]["data"] == "flows.feather"
    assert snapshot.sidecar_payload["files"]["metadata"] == "flows.metadata.json"
    assert (
        snapshot.sidecar_payload["files"]["flow_to_trips"]
        == "flow_to_trips.feather"
    )

    assert (
        snapshot.sidecar_payload["tables"]["flows"]["n_rows"]
        == len(flows.flows)
    )
    assert (
        snapshot.sidecar_payload["tables"]["flow_to_trips"]["n_rows"]
        == len(flow_to_trips_df_minimal)
    )

    assert snapshot.metadata_for_persist["dataset_id"] == flows.metadata["dataset_id"]
    assert snapshot.metadata_for_persist["artifact_id"] == snapshot.artifact_id
    assert snapshot.metadata_for_persist["events"][-1]["op"] == "write_flows"

    assert "source_trips" not in json.dumps(
        snapshot.sidecar_payload,
        ensure_ascii=False,
    )

    assert snapshot.issues == []
    _assert_json_dumpable(snapshot.sidecar_payload)


def test_freeze_flow_write_snapshot_creates_dataset_id_and_omits_missing_auxiliary(
    tmp_path: Path,
    make_flowdataset_minimal: Callable[..., FlowDataset],
) -> None:
    """Verifica snapshot Parquet con dataset_id creado y auxiliar solicitado pero ausente."""
    flows = make_flowdataset_minimal(
        include_dataset_id=False,
        include_artifact_id=False,
        include_aux=False,
        validated=False,
    )

    paths = _resolve_flows_artifact_paths(
        tmp_path / "fake_snapshot_parquet.golondrina"
    )

    snapshot = _freeze_flow_write_snapshot(
        flows,
        paths,
        WriteFlowsOptions(
            storage_format="parquet",
            parquet_compression="snappy",
            write_flow_to_trips=True,
        ),
        existing_issues=[],
    )

    assert snapshot.dataset_id_status == "created"
    assert snapshot.dataset_id.startswith("dset_")
    assert snapshot.artifact_id.startswith("art_")

    assert snapshot.files_written == [
        "flows.parquet",
        "flows.metadata.json",
    ]
    assert snapshot.n_flow_to_trips is None

    assert snapshot.sidecar_payload["storage"]["format"] == "parquet"
    assert snapshot.sidecar_payload["storage"]["options"] == {
        "compression": "snappy",
    }

    assert snapshot.sidecar_payload["files"]["data"] == "flows.parquet"
    assert snapshot.sidecar_payload["files"]["flow_to_trips"] is None

    assert _issue_codes(snapshot.issues) == [
        "WRITE_FLOWS.METADATA.DATASET_ID_CREATED",
        "WRITE_FLOWS.FLOW_TO_TRIPS.REQUESTED_BUT_MISSING",
    ]

    _assert_json_dumpable(snapshot.sidecar_payload)


def test_freeze_flow_write_snapshot_rejects_invalid_aggregation_spec(
    tmp_path: Path,
    make_flowdataset_minimal: Callable[..., FlowDataset],
) -> None:
    """Verifica abort temprano cuando aggregation_spec no es interpretable."""
    flows = make_flowdataset_minimal()
    flows.aggregation_spec = None

    paths = _resolve_flows_artifact_paths(
        tmp_path / "fake_invalid_aggregation_spec.golondrina"
    )

    with pytest.raises(ExportError) as exc_info:
        _freeze_flow_write_snapshot(
            flows,
            paths,
            WriteFlowsOptions(),
            existing_issues=[],
        )

    assert exc_info.value.code == "WRITE_FLOWS.SNAPSHOT.AGGREGATION_SPEC_INVALID"


def test_build_write_flows_summary_serializes_stable_operation_contract(
    tmp_path: Path,
) -> None:
    """Verifica la forma estable del summary de escritura formal de flows."""
    path = tmp_path / "artifact.golondrina"
    files_written = [
        "flows.feather",
        "flows.metadata.json",
        "flow_to_trips.feather",
    ]

    summary = _build_write_flows_summary(
        n_flows=3,
        n_flow_to_trips=7,
        path=path,
        dataset_id="dset_001",
        artifact_id="art_001",
        files_written=files_written,
    )

    assert summary == {
        "n_flows": 3,
        "n_flow_to_trips": 7,
        "files_written": files_written,
        "dataset_id": "dset_001",
        "artifact_id": "art_001",
        "path": str(path),
    }

    _assert_json_dumpable(summary)