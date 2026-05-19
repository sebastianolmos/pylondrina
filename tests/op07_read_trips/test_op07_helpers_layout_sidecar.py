from __future__ import annotations

import json
from pathlib import Path

import pytest

from pylondrina.errors import ExportError
from pylondrina.io.trips import (
    _load_sidecar_json,
    _resolve_trips_artifact_paths,
    _resolve_trips_artifact_root_for_read,
    _validate_read_root_and_sidecar,
)


def test_resolve_trips_artifact_paths_builds_formal_sidecar_paths(
    tmp_path: Path,
) -> None:
    """Verifica resolución estable del root, sidecar formal y sidecar legacy."""
    root = tmp_path / "tmp_demo_artifact"

    paths = _resolve_trips_artifact_paths(root)

    assert paths.root_dir == root
    assert paths.sidecar_path == root / "trips.metadata.json"
    assert paths.legacy_sidecar_path == root / "metadata.json"

    assert not hasattr(paths, "data_path"), (
        "TripsArtifactPaths no debe asumir un data_path fijo; "
        "el archivo tabular se resuelve posteriormente desde el sidecar."
    )


def test_resolve_trips_artifact_root_for_read_prefers_exact_and_falls_back_to_golondrina(
    make_case_dir,
) -> None:
    """Verifica uso del path exacto, fallback .golondrina y retorno del path faltante."""
    case_dir = make_case_dir("test_02_02_resolve_root_for_read")

    exact_root = case_dir / "exact_artifact"
    exact_root.mkdir()

    assert _resolve_trips_artifact_root_for_read(exact_root) == exact_root

    base_path = case_dir / "canonical_artifact"
    canonical_path = case_dir / "canonical_artifact.golondrina"
    canonical_path.mkdir()

    assert _resolve_trips_artifact_root_for_read(base_path) == canonical_path
    assert _resolve_trips_artifact_root_for_read(canonical_path) == canonical_path

    missing_path = case_dir / "missing_artifact"
    assert _resolve_trips_artifact_root_for_read(missing_path) == missing_path


def test_validate_read_root_and_sidecar_accepts_minimal_formal_layout(
    make_case_dir,
    materialize_minimal_formal_artifact,
) -> None:
    """Verifica que un layout formal mínimo con sidecar oficial sea aceptado."""
    case_dir = make_case_dir("test_02_03_validate_root_happy")
    paths, _ = materialize_minimal_formal_artifact(case_dir / "artifact")

    issues = []

    _validate_read_root_and_sidecar(
        paths.root_dir,
        paths,
        issues=issues,
    )

    assert issues == []
    assert paths.sidecar_path.exists()
    assert paths.sidecar_path.name == "trips.metadata.json"


def test_validate_read_root_and_sidecar_raises_for_invalid_root(
    make_case_dir,
    assert_issue_present,
) -> None:
    """Verifica error fatal cuando el root no existe o no es directorio."""
    case_dir = make_case_dir("test_02_04_validate_root_invalid")
    missing_root = case_dir / "missing_artifact"
    paths = _resolve_trips_artifact_paths(missing_root)

    issues = []

    with pytest.raises(ExportError):
        _validate_read_root_and_sidecar(
            missing_root,
            paths,
            issues=issues,
        )

    assert_issue_present(issues, "READ.PATH.INVALID_ROOT")


def test_validate_read_root_and_sidecar_raises_when_formal_sidecar_is_missing(
    make_case_dir,
    materialize_minimal_formal_artifact,
    assert_issue_present,
) -> None:
    """Verifica que trips.metadata.json sea obligatorio para lectura formal."""
    case_dir = make_case_dir("test_02_05_missing_sidecar")
    paths, _ = materialize_minimal_formal_artifact(case_dir / "artifact")

    paths.sidecar_path.unlink()
    assert not paths.sidecar_path.exists()

    issues = []

    with pytest.raises(ExportError):
        _validate_read_root_and_sidecar(
            paths.root_dir,
            paths,
            issues=issues,
        )

    assert_issue_present(issues, "READ.LAYOUT.MISSING_SIDECAR")


def test_validate_read_root_and_sidecar_raises_when_only_legacy_sidecar_exists(
    make_case_dir,
    materialize_minimal_formal_artifact,
    assert_issue_present,
) -> None:
    """Verifica rechazo explícito de metadata.json legacy sin sidecar formal."""
    case_dir = make_case_dir("test_02_06_legacy_sidecar")
    paths, _ = materialize_minimal_formal_artifact(case_dir / "artifact")

    paths.sidecar_path.unlink()
    paths.legacy_sidecar_path.write_text("{}", encoding="utf-8")

    assert not paths.sidecar_path.exists()
    assert paths.legacy_sidecar_path.exists()

    issues = []

    with pytest.raises(ExportError):
        _validate_read_root_and_sidecar(
            paths.root_dir,
            paths,
            issues=issues,
        )

    assert_issue_present(issues, "READ.LAYOUT.LEGACY_SIDECAR_DETECTED")


def test_load_sidecar_json_returns_payload_for_valid_formal_sidecar(
    make_case_dir,
    materialize_minimal_formal_artifact,
) -> None:
    """Verifica carga correcta de trips.metadata.json como payload dict."""
    case_dir = make_case_dir("test_02_07_load_sidecar_happy")
    paths, payload = materialize_minimal_formal_artifact(case_dir / "artifact")

    issues = []

    loaded = _load_sidecar_json(
        paths.sidecar_path,
        issues=issues,
        destination_path=paths.root_dir,
    )

    assert issues == []
    assert loaded == payload

    assert loaded["dataset_type"] == "trips"
    assert loaded["format"] == "golondrina"
    assert loaded["layout_version"] == "1.1"
    assert loaded["storage"]["format"] == "parquet"


def test_load_sidecar_json_raises_for_invalid_json(
    make_case_dir,
    assert_issue_present,
) -> None:
    """Verifica error fatal cuando el sidecar no puede parsearse como JSON."""
    case_dir = make_case_dir("test_02_08_sidecar_invalid_json")
    root = case_dir / "artifact"
    root.mkdir(parents=True)

    sidecar_path = root / "trips.metadata.json"
    sidecar_path.write_text("{ invalid json", encoding="utf-8")

    issues = []

    with pytest.raises(ExportError):
        _load_sidecar_json(
            sidecar_path,
            issues=issues,
            destination_path=root,
        )

    assert_issue_present(issues, "READ.JSON.LOAD_FAILED")


def test_load_sidecar_json_raises_for_incomplete_top_level_payload(
    make_case_dir,
    make_sidecar_payload,
    assert_issue_present,
) -> None:
    """Verifica error fatal cuando faltan claves top-level obligatorias del sidecar."""
    case_dir = make_case_dir("test_02_09_sidecar_invalid_top_level")
    root = case_dir / "artifact"
    root.mkdir(parents=True)

    payload = make_sidecar_payload()
    payload.pop("storage")

    sidecar_path = root / "trips.metadata.json"
    sidecar_path.write_text(
        json.dumps(payload, ensure_ascii=False),
        encoding="utf-8",
    )

    issues = []

    with pytest.raises(ExportError):
        _load_sidecar_json(
            sidecar_path,
            issues=issues,
            destination_path=root,
        )

    assert_issue_present(issues, "READ.SIDECAR.INVALID_TOP_LEVEL")