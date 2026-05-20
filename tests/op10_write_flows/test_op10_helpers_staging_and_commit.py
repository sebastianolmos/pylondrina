from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from pylondrina.errors import ExportError
from pylondrina.io.flows import (
    _assert_flows_staging_complete,
    _cleanup_staging_dir,
    _commit_staged_flow_bundle,
    _create_flows_staging_dir,
    _resolve_flows_artifact_paths,
    _write_flow_sidecar_to_staging,
    _write_flows_table_to_staging,
    _write_optional_flow_to_trips_to_staging,
)
from pylondrina.reports import Issue


def _issue_codes(issues: list[Issue]) -> list[str]:
    """Retorna los códigos de issue emitidos por los helpers bajo prueba."""
    return [issue.code for issue in issues]


def _read_written_table(
    path: Path,
    *,
    storage_format: str,
) -> pd.DataFrame:
    """Lee una tabla persistida por backend Parquet o Feather."""
    if storage_format == "parquet":
        return pd.read_parquet(path, engine="pyarrow")

    if storage_format == "feather":
        return pd.read_feather(path)

    raise ValueError(f"storage_format inesperado: {storage_format!r}")


def test_create_and_cleanup_flows_staging_dir(
    tmp_path: Path,
) -> None:
    """Verifica creación de staging hermano del destino final y cleanup normal."""
    final_dir = tmp_path / "artifact_final.golondrina"
    issues: list[Issue] = []

    staging_dir = _create_flows_staging_dir(
        final_dir,
        issues=issues,
    )

    assert staging_dir.exists()
    assert staging_dir.is_dir()
    assert staging_dir.parent == final_dir.parent
    assert issues == []

    _cleanup_staging_dir(
        staging_dir,
        final_dir,
        ["flows.feather", "flows.metadata.json"],
        issues,
    )

    assert not staging_dir.exists()
    assert issues == []


@pytest.mark.parametrize(
    ("storage_format", "filename"),
    [
        ("parquet", "flows.parquet"),
        ("feather", "flows.feather"),
    ],
)
def test_write_flows_table_to_staging_materializes_readable_main_table(
    tmp_path: Path,
    flows_df_minimal: pd.DataFrame,
    storage_format: str,
    filename: str,
) -> None:
    """Verifica escritura física de la tabla principal en Parquet y Feather."""
    staging_dir = tmp_path / "staging"
    data_path = staging_dir / filename
    issues: list[Issue] = []

    _write_flows_table_to_staging(
        flows_df_minimal,
        data_path,
        storage_format=storage_format,
        parquet_compression="snappy",
        feather_compression="lz4",
        aggregation_spec={"group_by": ["mode"]},
        issues=issues,
        destination_path=tmp_path,
    )

    assert data_path.exists()
    assert issues == []

    loaded = _read_written_table(
        data_path,
        storage_format=storage_format,
    )

    pd.testing.assert_frame_equal(
        loaded.reset_index(drop=True),
        flows_df_minimal.reset_index(drop=True),
        check_dtype=False,
        check_categorical=False,
    )


@pytest.mark.parametrize(
    ("storage_format", "filename"),
    [
        ("parquet", "flow_to_trips.parquet"),
        ("feather", "flow_to_trips.feather"),
    ],
)
def test_write_optional_flow_to_trips_to_staging_materializes_requested_auxiliary(
    tmp_path: Path,
    flow_to_trips_df_minimal: pd.DataFrame,
    storage_format: str,
    filename: str,
) -> None:
    """Verifica escritura del auxiliar flow_to_trips cuando existe y fue solicitado."""
    staging_dir = tmp_path / "staging"
    aux_path = staging_dir / filename
    issues: list[Issue] = []

    _write_optional_flow_to_trips_to_staging(
        flow_to_trips_df_minimal,
        aux_path,
        write_flow_to_trips=True,
        storage_format=storage_format,
        parquet_compression="snappy",
        feather_compression="lz4",
        issues=issues,
        destination_path=tmp_path,
    )

    assert aux_path.exists()
    assert issues == []

    loaded = _read_written_table(
        aux_path,
        storage_format=storage_format,
    )

    pd.testing.assert_frame_equal(
        loaded.reset_index(drop=True),
        flow_to_trips_df_minimal.reset_index(drop=True),
        check_dtype=False,
        check_categorical=False,
    )


def test_write_optional_flow_to_trips_to_staging_skips_absent_or_disabled_auxiliary(
    tmp_path: Path,
    flow_to_trips_df_minimal: pd.DataFrame,
) -> None:
    """Verifica omisión limpia del auxiliar si falta o si no fue solicitado."""
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)

    # Caso 1: se solicita, pero el auxiliar es None.
    missing_aux_path = staging_dir / "flow_to_trips_missing.feather"
    missing_issues: list[Issue] = []

    _write_optional_flow_to_trips_to_staging(
        None,
        missing_aux_path,
        write_flow_to_trips=True,
        storage_format="feather",
        parquet_compression="snappy",
        feather_compression="lz4",
        issues=missing_issues,
        destination_path=tmp_path,
    )

    assert not missing_aux_path.exists()
    assert missing_issues == []

    # Caso 2: el auxiliar existe, pero no se solicitó persistirlo.
    disabled_aux_path = staging_dir / "flow_to_trips_disabled.parquet"
    disabled_issues: list[Issue] = []

    _write_optional_flow_to_trips_to_staging(
        flow_to_trips_df_minimal,
        disabled_aux_path,
        write_flow_to_trips=False,
        storage_format="parquet",
        parquet_compression="snappy",
        feather_compression="lz4",
        issues=disabled_issues,
        destination_path=tmp_path,
    )

    assert not disabled_aux_path.exists()
    assert disabled_issues == []


def test_write_flow_sidecar_to_staging_persists_payload_as_json(
    tmp_path: Path,
    make_sidecar_payload: Callable[..., dict[str, Any]],
) -> None:
    """Verifica escritura íntegra de flows.metadata.json en staging."""
    staging_dir = tmp_path / "staging"
    sidecar_path = staging_dir / "flows.metadata.json"
    payload = make_sidecar_payload(
        storage_format="feather",
        include_flow_to_trips=True,
    )
    issues: list[Issue] = []

    _write_flow_sidecar_to_staging(
        payload,
        sidecar_path,
        issues=issues,
        destination_path=tmp_path,
    )

    assert sidecar_path.exists()
    assert issues == []

    loaded = json.loads(
        sidecar_path.read_text(encoding="utf-8")
    )

    assert loaded == payload


def test_assert_flows_staging_complete_accepts_complete_bundle_and_rejects_missing_file(
    tmp_path: Path,
) -> None:
    """Verifica completitud mínima del staging y error si falta un archivo esperado."""
    expected_files = [
        "flows.feather",
        "flows.metadata.json",
        "flow_to_trips.feather",
    ]

    # Caso completo.
    complete_root = tmp_path / "staging_ok"
    complete_root.mkdir(parents=True, exist_ok=True)

    for filename in expected_files:
        (complete_root / filename).touch()

    complete_paths = _resolve_flows_artifact_paths(complete_root)
    complete_issues: list[Issue] = []

    _assert_flows_staging_complete(
        complete_paths,
        expected_files=expected_files,
        issues=complete_issues,
        destination_path=complete_root,
    )

    assert complete_issues == []

    # Caso incompleto.
    incomplete_root = tmp_path / "staging_bad"
    incomplete_root.mkdir(parents=True, exist_ok=True)

    (incomplete_root / "flows.feather").touch()
    (incomplete_root / "flows.metadata.json").touch()
    # Falta flow_to_trips.feather.

    incomplete_paths = _resolve_flows_artifact_paths(incomplete_root)
    incomplete_issues: list[Issue] = []

    with pytest.raises(ExportError):
        _assert_flows_staging_complete(
            incomplete_paths,
            expected_files=expected_files,
            issues=incomplete_issues,
            destination_path=incomplete_root,
        )

    assert "WRITE_FLOWS.IO.STAGING_INCOMPLETE" in _issue_codes(
        incomplete_issues
    )


def test_commit_staged_flow_bundle_promotes_complete_staging(
    tmp_path: Path,
) -> None:
    """Verifica promoción exitosa del staging al bundle final."""
    final_dir = tmp_path / "artifact_final.golondrina"
    staging_dir = tmp_path / "staging"

    staging_dir.mkdir(parents=True, exist_ok=True)
    (staging_dir / "flows.feather").write_text(
        "dummy data",
        encoding="utf-8",
    )
    (staging_dir / "flows.metadata.json").write_text(
        "{}",
        encoding="utf-8",
    )

    issues: list[Issue] = []

    _commit_staged_flow_bundle(
        staging_dir,
        final_dir,
        mode="error_if_exists",
        files_written=[
            "flows.feather",
            "flows.metadata.json",
        ],
        issues=issues,
    )

    assert final_dir.exists()
    assert (final_dir / "flows.feather").exists()
    assert (final_dir / "flows.metadata.json").exists()
    assert not staging_dir.exists()
    assert issues == []


def test_commit_staged_flow_bundle_overwrites_existing_bundle_with_observable_issue(
    tmp_path: Path,
) -> None:
    """Verifica overwrite explícito, reemplazo del bundle y emisión del issue informativo."""
    final_dir = tmp_path / "artifact_final.golondrina"
    staging_dir = tmp_path / "staging"

    final_dir.mkdir(parents=True, exist_ok=True)
    (final_dir / "old_file.txt").write_text(
        "legacy",
        encoding="utf-8",
    )

    staging_dir.mkdir(parents=True, exist_ok=True)
    (staging_dir / "flows.parquet").write_text(
        "dummy parquet",
        encoding="utf-8",
    )
    (staging_dir / "flows.metadata.json").write_text(
        "{}",
        encoding="utf-8",
    )

    issues: list[Issue] = []

    _commit_staged_flow_bundle(
        staging_dir,
        final_dir,
        mode="overwrite",
        files_written=[
            "flows.parquet",
            "flows.metadata.json",
        ],
        issues=issues,
    )

    assert final_dir.exists()
    assert (final_dir / "flows.parquet").exists()
    assert (final_dir / "flows.metadata.json").exists()
    assert not (final_dir / "old_file.txt").exists()
    assert not staging_dir.exists()

    assert "WRITE_FLOWS.LAYOUT.BUNDLE_OVERWRITTEN" in _issue_codes(
        issues
    )


def test_commit_staged_flow_bundle_rejects_collision_in_error_if_exists_mode(
    tmp_path: Path,
) -> None:
    """Verifica que error_if_exists no permita sobrescribir silenciosamente el bundle."""
    final_dir = tmp_path / "artifact_final.golondrina"
    staging_dir = tmp_path / "staging"

    final_dir.mkdir(parents=True, exist_ok=True)
    (final_dir / "old_file.txt").write_text(
        "legacy",
        encoding="utf-8",
    )

    staging_dir.mkdir(parents=True, exist_ok=True)
    (staging_dir / "flows.feather").write_text(
        "dummy feather",
        encoding="utf-8",
    )
    (staging_dir / "flows.metadata.json").write_text(
        "{}",
        encoding="utf-8",
    )

    issues: list[Issue] = []

    with pytest.raises(ExportError):
        _commit_staged_flow_bundle(
            staging_dir,
            final_dir,
            mode="error_if_exists",
            files_written=[
                "flows.feather",
                "flows.metadata.json",
            ],
            issues=issues,
        )

    assert "WRITE_FLOWS.LAYOUT.BUNDLE_EXISTS" in _issue_codes(
        issues
    )