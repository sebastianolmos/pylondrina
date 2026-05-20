from __future__ import annotations

import json
from pathlib import Path

import pytest

from pylondrina.errors import ExportError
from pylondrina.io.flows import (
    WriteFlowsOptions,
    _append_event,
    _assert_json_safe,
    _build_flow_storage_options_snapshot,
    _build_io_event,
    _build_issues_summary,
    _ensure_dataset_id,
    _flow_data_filename_for_storage,
    _flow_to_trips_filename_for_storage,
    _new_artifact_id,
    _normalize_flows_artifact_root_for_write,
    _options_to_write_parameters,
    _resolve_flows_artifact_paths,
)
from pylondrina.reports import Issue


def _issue_codes(issues: list[Issue]) -> list[str]:
    """Retorna los códigos de issue emitidos por un helper."""
    return [issue.code for issue in issues]


def test_normalize_flows_artifact_root_for_write_adds_suffix_without_duplication(
    tmp_path: Path,
) -> None:
    """Verifica la normalización opcional del root al sufijo `.golondrina`."""
    raw_root = tmp_path / "demo_flows"
    already_normalized_root = tmp_path / "demo_flows.golondrina"

    normalized = _normalize_flows_artifact_root_for_write(
        raw_root,
        normalize_artifact_dir=True,
    )
    preserved = _normalize_flows_artifact_root_for_write(
        already_normalized_root,
        normalize_artifact_dir=True,
    )
    unchanged = _normalize_flows_artifact_root_for_write(
        raw_root,
        normalize_artifact_dir=False,
    )

    assert normalized == tmp_path / "demo_flows.golondrina"
    assert preserved == already_normalized_root
    assert unchanged == raw_root


def test_resolve_flows_artifact_paths_exposes_only_root_and_sidecar(
    tmp_path: Path,
) -> None:
    """Verifica que el layout base solo resuelva root y sidecar formal."""
    root = tmp_path / "artifact.golondrina"

    paths = _resolve_flows_artifact_paths(root)

    assert paths.root_dir == root
    assert paths.sidecar_path == root / "flows.metadata.json"

    # En el diseño backend-aware, las tablas no se fijan en FlowsArtifactPaths.
    assert not hasattr(paths, "data_path")
    assert not hasattr(paths, "flow_to_trips_path")


def test_flow_filenames_follow_storage_backend_and_reject_unknown_backend() -> None:
    """Verifica nombres físicos canónicos por backend y rechazo de formatos no soportados."""
    assert _flow_data_filename_for_storage("parquet") == "flows.parquet"
    assert _flow_to_trips_filename_for_storage("parquet") == "flow_to_trips.parquet"

    assert _flow_data_filename_for_storage("feather") == "flows.feather"
    assert _flow_to_trips_filename_for_storage("feather") == "flow_to_trips.feather"

    with pytest.raises(ValueError):
        _flow_data_filename_for_storage("csv")

    with pytest.raises(ValueError):
        _flow_to_trips_filename_for_storage("csv")


def test_build_flow_storage_options_snapshot_reflects_effective_backend() -> None:
    """Verifica el bloque `storage.options` persistible para Parquet y Feather."""
    parquet_options = WriteFlowsOptions(
        storage_format="parquet",
        parquet_compression="zstd",
    )
    feather_options = WriteFlowsOptions(
        storage_format="feather",
        feather_compression="lz4",
    )

    parquet_storage = _build_flow_storage_options_snapshot(parquet_options)
    feather_storage = _build_flow_storage_options_snapshot(feather_options)

    assert parquet_storage == {"compression": "zstd"}
    assert feather_storage == {"compression": "lz4", "version": 2}


def test_options_to_write_parameters_serializes_effective_request(
    tmp_path: Path,
) -> None:
    """Verifica que los parámetros efectivos de write queden serializables y completos."""
    path = tmp_path / "demo_flows.golondrina"
    options = WriteFlowsOptions(
        mode="overwrite",
        storage_format="feather",
        parquet_compression="snappy",
        feather_compression="zstd",
        normalize_artifact_dir=True,
        write_flow_to_trips=False,
    )

    parameters = _options_to_write_parameters(
        path=path,
        options=options,
    )

    assert parameters == {
        "path": str(path),
        "mode": "overwrite",
        "storage_format": "feather",
        "parquet_compression": "snappy",
        "feather_compression": "zstd",
        "normalize_artifact_dir": True,
        "write_flow_to_trips": False,
    }

    # Debe ser apto para report/evento JSON-safe.
    json.dumps(parameters, ensure_ascii=False)


def test_assert_json_safe_accepts_serializable_payload_and_rejects_invalid_snapshot(
    tmp_path: Path,
) -> None:
    """Verifica JSON-safety y emisión del issue contractual ante payload no serializable."""
    artifact_path = tmp_path / "artifact.golondrina"

    issues_ok: list[Issue] = []
    _assert_json_safe(
        {"a": 1, "b": ["x", "y"]},
        label="payload_ok",
        issues=issues_ok,
        path=artifact_path,
        artifact="flows.metadata.json",
    )

    assert issues_ok == []

    issues_bad: list[Issue] = []
    with pytest.raises(ExportError):
        _assert_json_safe(
            {"bad": {1, 2, 3}},
            label="payload_bad",
            issues=issues_bad,
            path=artifact_path,
            artifact="flows.metadata.json",
        )

    assert _issue_codes(issues_bad) == [
        "WRITE_FLOWS.SNAPSHOT.NOT_JSON_SERIALIZABLE"
    ]


def test_dataset_and_artifact_identifiers_follow_write_persistence_policy() -> None:
    """Verifica preservación, creación y regeneración de IDs usados por persistencia."""
    dataset_id_preserved, status_preserved = _ensure_dataset_id(
        {"dataset_id": "dset_ok"}
    )
    dataset_id_created, status_created = _ensure_dataset_id({})
    dataset_id_regenerated, status_regenerated = _ensure_dataset_id(
        {"dataset_id": ""}
    )
    artifact_id = _new_artifact_id()

    assert dataset_id_preserved == "dset_ok"
    assert status_preserved == "preserved"

    assert isinstance(dataset_id_created, str)
    assert dataset_id_created.startswith("dset_")
    assert status_created == "created"

    assert isinstance(dataset_id_regenerated, str)
    assert dataset_id_regenerated.startswith("dset_")
    assert status_regenerated == "regenerated"

    assert isinstance(artifact_id, str)
    assert artifact_id.startswith("art_")


def test_io_reporting_helpers_build_compact_summary_event_and_append_without_mutation() -> None:
    """Verifica resumen de issues, evento IO mínimo y append sin mutar metadata original."""
    issues = [
        Issue(
            level="warning",
            code="WRITE_FLOWS.TEST.WARNING_A",
            message="warning A",
        ),
        Issue(
            level="warning",
            code="WRITE_FLOWS.TEST.WARNING_A",
            message="warning A repeated",
        ),
        Issue(
            level="info",
            code="WRITE_FLOWS.TEST.INFO_B",
            message="info B",
        ),
    ]

    issues_summary = _build_issues_summary(issues)

    assert issues_summary["counts"] == {
        "info": 1,
        "warning": 2,
        "error": 0,
    }
    assert issues_summary["top_codes"][0] == {
        "code": "WRITE_FLOWS.TEST.WARNING_A",
        "count": 2,
    }

    event = _build_io_event(
        op="write_flows",
        parameters={"storage_format": "feather"},
        summary={"n_flows": 3},
        issues_summary=issues_summary,
    )

    assert event["op"] == "write_flows"
    assert isinstance(event["ts_utc"], str)
    assert event["parameters"] == {"storage_format": "feather"}
    assert event["summary"] == {"n_flows": 3}
    assert event["issues_summary"] == issues_summary

    metadata = {
        "events": [{"op": "build_flows"}],
        "x": 1,
    }

    metadata_out = _append_event(metadata, event)

    assert metadata_out is not metadata
    assert metadata["events"] == [{"op": "build_flows"}]
    assert metadata_out["events"] == [
        {"op": "build_flows"},
        event,
    ]
    assert metadata_out["x"] == metadata["x"]