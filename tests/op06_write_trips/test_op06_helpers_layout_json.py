from __future__ import annotations

import json
from pathlib import Path

import pytest

from pylondrina.errors import ExportError
from pylondrina.io.trips import (
    WriteTripsOptions,
    _append_event,
    _append_golondrina_artifact_suffix,
    _assert_json_safe,
    _build_io_event,
    _build_issues_summary,
    _build_storage_options_snapshot,
    _build_write_trips_summary,
    _has_golondrina_artifact_suffix,
    _normalize_trips_artifact_root_for_write,
    _options_to_write_parameters,
    _resolve_trips_artifact_paths,
    _trip_data_filename_for_storage,
)
from pylondrina.reports import Issue


def _issue_codes(issues: list[Issue]) -> list[str]:
    """Retorna los códigos de issues emitidos durante el test."""
    return [issue.code for issue in issues]


def _assert_issue_present(issues: list[Issue], code: str) -> None:
    """Verifica que un código de issue esperado esté presente."""
    codes = _issue_codes(issues)
    assert code in codes, f"No se encontró {code}. Codes actuales: {codes}"


def _assert_json_serializable(value: object) -> None:
    """Verifica que un objeto sea serializable directamente como JSON."""
    json.dumps(value, ensure_ascii=False)


def test_golondrina_suffix_helpers_and_write_root_normalization(tmp_path: Path):
    """Verifica helpers de sufijo `.golondrina` y normalización del root de escritura."""
    base_path = tmp_path / "artifact"
    suffixed_path = tmp_path / "artifact.golondrina"

    assert _has_golondrina_artifact_suffix(base_path) is False
    assert _has_golondrina_artifact_suffix(suffixed_path) is True

    assert _append_golondrina_artifact_suffix(base_path) == suffixed_path
    assert _append_golondrina_artifact_suffix(suffixed_path) == suffixed_path

    assert (
        _normalize_trips_artifact_root_for_write(
            base_path,
            normalize_artifact_dir=True,
        )
        == suffixed_path
    )

    assert (
        _normalize_trips_artifact_root_for_write(
            base_path,
            normalize_artifact_dir=False,
        )
        == base_path
    )

    assert (
        _normalize_trips_artifact_root_for_write(
            suffixed_path,
            normalize_artifact_dir=True,
        )
        == suffixed_path
    )


def test_trip_data_filename_for_storage_resolves_supported_backends():
    """Verifica nombres contractuales del archivo tabular por backend."""
    assert _trip_data_filename_for_storage("parquet") == "trips.parquet"
    assert _trip_data_filename_for_storage("feather") == "trips.feather"

    with pytest.raises(ValueError):
        _trip_data_filename_for_storage("csv")


def test_build_storage_options_snapshot_for_parquet_and_feather():
    """Verifica snapshot persistible de opciones de storage para Parquet y Feather."""
    parquet_options = WriteTripsOptions(
        storage_format="parquet",
        parquet_compression="snappy",
        feather_compression="lz4",
    )
    feather_options = WriteTripsOptions(
        storage_format="feather",
        parquet_compression="snappy",
        feather_compression="lz4",
    )

    parquet_snapshot = _build_storage_options_snapshot(parquet_options)
    feather_snapshot = _build_storage_options_snapshot(feather_options)

    assert parquet_snapshot == {"compression": "snappy"}
    assert feather_snapshot == {"compression": "lz4", "version": 2}

    _assert_json_serializable(parquet_snapshot)
    _assert_json_serializable(feather_snapshot)


def test_resolve_trips_artifact_paths_uses_formal_sidecar_layout(tmp_path: Path):
    """Verifica layout formal de root, sidecar oficial y sidecar legacy."""
    root = tmp_path / "artifact"
    paths = _resolve_trips_artifact_paths(root)

    assert paths.root_dir == root
    assert paths.sidecar_path == root / "trips.metadata.json"
    assert paths.legacy_sidecar_path == root / "metadata.json"

    assert not hasattr(paths, "data_path"), (
        "El layout vigente no debe asumir un data_path fijo; "
        "el archivo tabular depende de storage.format."
    )


def test_options_to_write_parameters_serializes_effective_options(tmp_path: Path):
    """Verifica serialización estable de parámetros efectivos de escritura."""
    artifact_path = tmp_path / "artifact.golondrina"

    options = WriteTripsOptions(
        mode="overwrite",
        require_validated=False,
        storage_format="feather",
        parquet_compression="snappy",
        feather_compression="zstd",
        normalize_artifact_dir=False,
    )

    parameters = _options_to_write_parameters(
        path=artifact_path,
        options=options,
    )

    assert set(parameters) == {
        "path",
        "mode",
        "require_validated",
        "storage_format",
        "parquet_compression",
        "feather_compression",
        "normalize_artifact_dir",
    }

    assert Path(parameters["path"]) == artifact_path
    assert parameters["mode"] == "overwrite"
    assert parameters["require_validated"] is False
    assert parameters["storage_format"] == "feather"
    assert parameters["parquet_compression"] == "snappy"
    assert parameters["feather_compression"] == "zstd"
    assert parameters["normalize_artifact_dir"] is False

    _assert_json_serializable(parameters)


def test_assert_json_safe_accepts_serializable_payloads():
    """Verifica que `_assert_json_safe` acepte payloads serializables."""
    issues: list[Issue] = []

    _assert_json_safe(
        {"ok": [1, 2, 3], "nested": {"value": "x"}},
        label="payload_ok",
        issues=issues,
    )

    assert issues == []


def test_assert_json_safe_raises_export_error_for_non_serializable_payloads():
    """Verifica que `_assert_json_safe` aborte y emita issue ante payload inválido."""
    issues: list[Issue] = []

    with pytest.raises(ExportError) as excinfo:
        _assert_json_safe(
            {"bad": {1, 2, 3}},
            label="payload_bad",
            issues=issues,
        )

    assert excinfo.value.code == "WRT.JSON.NOT_SERIALIZABLE"
    _assert_issue_present(issues, "WRT.JSON.NOT_SERIALIZABLE")
    assert issues[-1].level == "error"


def test_build_issues_summary_counts_levels_and_top_codes():
    """Verifica resumen de issues por severidad y frecuencia de códigos."""
    issues = [
        Issue(level="info", code="CODE.INFO", message="info"),
        Issue(level="warning", code="CODE.WARN", message="warning"),
        Issue(level="warning", code="CODE.WARN", message="warning again"),
    ]

    summary = _build_issues_summary(issues)

    assert summary["counts"] == {
        "info": 1,
        "warning": 2,
        "error": 0,
    }

    assert summary["top_codes"][0] == {
        "code": "CODE.WARN",
        "count": 2,
    }

    _assert_json_serializable(summary)


def test_build_io_event_and_append_event_preserve_append_only_semantics():
    """Verifica forma mínima del evento IO y append sin mutar metadata de entrada."""
    issues_summary = {
        "counts": {"info": 0, "warning": 0, "error": 0},
        "top_codes": [],
    }
    parameters = {
        "storage_format": "parquet",
        "mode": "error_if_exists",
    }
    summary = {
        "n_rows": 3,
        "storage_format": "parquet",
    }

    event = _build_io_event(
        op="write_trips",
        parameters=parameters,
        summary=summary,
        issues_summary=issues_summary,
    )

    assert event["op"] == "write_trips"
    assert isinstance(event["ts_utc"], str)
    assert event["parameters"] == parameters
    assert event["summary"] == summary
    assert event["issues_summary"] == issues_summary

    metadata_in = {"events": [{"op": "previous"}]}
    metadata_out = _append_event(metadata_in, event)

    assert metadata_in == {"events": [{"op": "previous"}]}
    assert len(metadata_out["events"]) == 2
    assert metadata_out["events"][0]["op"] == "previous"
    assert metadata_out["events"][-1]["op"] == "write_trips"
    assert metadata_out["events"][-1] == event

    _assert_json_serializable(event)
    _assert_json_serializable(metadata_out)


@pytest.mark.parametrize(
    ("storage_format", "files_written"),
    [
        ("parquet", ["trips.parquet", "trips.metadata.json"]),
        ("feather", ["trips.feather", "trips.metadata.json"]),
    ],
)
def test_build_write_trips_summary_serializes_contract_fields(
    tmp_path: Path,
    storage_format: str,
    files_written: list[str],
):
    """Verifica summary mínimo de `write_trips` para ambos backends soportados."""
    artifact_path = tmp_path / f"artifact_{storage_format}.golondrina"

    summary = _build_write_trips_summary(
        n_rows=3,
        path=artifact_path,
        artifact_id="art_001",
        dataset_id_status="preserved",
        dataset_id="dset_001",
        storage_format=storage_format,
        files_written=files_written,
    )

    assert set(summary) == {
        "n_rows",
        "files_written",
        "path",
        "dataset_id",
        "artifact_id",
        "dataset_id_status",
        "storage_format",
    }

    assert summary["n_rows"] == 3
    assert summary["files_written"] == files_written
    assert Path(summary["path"]) == artifact_path
    assert summary["dataset_id"] == "dset_001"
    assert summary["artifact_id"] == "art_001"
    assert summary["dataset_id_status"] == "preserved"
    assert summary["storage_format"] == storage_format

    _assert_json_serializable(summary)