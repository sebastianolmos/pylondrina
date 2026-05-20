from __future__ import annotations

import pytest

from pylondrina.errors import ExportError
from pylondrina.io.flows import (
    ReadFlowsOptions,
    _options_to_read_parameters,
    _resolve_flow_to_trips_path_from_sidecar,
    _resolve_flows_artifact_paths,
    _resolve_flows_artifact_root_for_read,
    _resolve_flows_data_path_from_sidecar,
    _validate_read_layout,
)


# -----------------------------------------------------------------------------
# Bloque 3. Helpers de root, paths y parameters
# -----------------------------------------------------------------------------


def test_resolve_flows_artifact_root_for_read_handles_exact_fallback_and_missing(
    tmp_path,
):
    """Verifica root exacto, fallback `.golondrina` y preservación de ruta faltante."""
    exact_root = tmp_path / "exact_bundle.golondrina"
    exact_root.mkdir(parents=True, exist_ok=True)

    resolved_exact = _resolve_flows_artifact_root_for_read(exact_root)

    assert resolved_exact == exact_root

    fallback_base = tmp_path / "bundle_without_suffix"
    fallback_true = tmp_path / "bundle_without_suffix.golondrina"
    fallback_true.mkdir(parents=True, exist_ok=True)

    resolved_fallback = _resolve_flows_artifact_root_for_read(fallback_base)

    assert resolved_fallback == fallback_true

    missing_root = tmp_path / "missing_bundle"

    resolved_missing = _resolve_flows_artifact_root_for_read(missing_root)

    assert resolved_missing == missing_root


def test_resolve_flows_artifact_paths_exposes_only_base_read_paths(
    tmp_path,
):
    """Verifica que los paths base de lectura incluyan solo root y sidecar."""
    root = tmp_path / "artifact.golondrina"

    paths = _resolve_flows_artifact_paths(root)

    assert paths.root_dir == root
    assert paths.sidecar_path == root / "flows.metadata.json"

    assert not hasattr(paths, "data_path")
    assert not hasattr(paths, "flow_to_trips_path")


def test_options_to_read_parameters_serializes_effective_read_options(
    tmp_path,
    assert_json_dumpable,
):
    """Verifica la serialización estable de path y opciones públicas de lectura."""
    options = ReadFlowsOptions(
        strict=True,
        keep_metadata=False,
        read_flow_to_trips=False,
    )

    input_path = tmp_path / "demo_read_flows.golondrina"

    parameters = _options_to_read_parameters(
        path=input_path,
        options=options,
    )

    assert parameters["path"] == str(input_path.expanduser())
    assert parameters["strict"] is True
    assert parameters["keep_metadata"] is False
    assert parameters["read_flow_to_trips"] is False

    assert_json_dumpable(parameters, "read_parameters")


def test_validate_read_layout_accepts_existing_root_with_sidecar(
    tmp_path,
):
    """Verifica que el layout mínimo válido no emita issues."""
    root = tmp_path / "artifact.golondrina"
    root.mkdir(parents=True, exist_ok=True)

    paths = _resolve_flows_artifact_paths(root)
    paths.sidecar_path.touch()

    issues = []

    _validate_read_layout(
        root,
        paths,
        strict=False,
        issues=issues,
    )

    assert issues == []


def test_validate_read_layout_raises_for_invalid_root(
    tmp_path,
    assert_issue_present,
):
    """Verifica que una ruta raíz inexistente aborte con issue trazable."""
    root = tmp_path / "missing_artifact.golondrina"

    paths = _resolve_flows_artifact_paths(root)
    issues = []

    with pytest.raises(ExportError) as excinfo:
        _validate_read_layout(
            root,
            paths,
            strict=False,
            issues=issues,
        )

    assert excinfo.value.code == "READ_FLOWS.PATH.INVALID_ROOT"
    assert_issue_present(
        issues,
        "READ_FLOWS.PATH.INVALID_ROOT",
    )


def test_validate_read_layout_raises_for_missing_sidecar(
    tmp_path,
    assert_issue_present,
):
    """Verifica que un bundle sin sidecar formal aborte con issue trazable."""
    root = tmp_path / "artifact.golondrina"
    root.mkdir(parents=True, exist_ok=True)

    paths = _resolve_flows_artifact_paths(root)
    issues = []

    with pytest.raises(ExportError) as excinfo:
        _validate_read_layout(
            root,
            paths,
            strict=False,
            issues=issues,
        )

    assert excinfo.value.code == "READ_FLOWS.LAYOUT.MISSING_SIDECAR"
    assert_issue_present(
        issues,
        "READ_FLOWS.LAYOUT.MISSING_SIDECAR",
    )


# -----------------------------------------------------------------------------
# Bloque 5. Resolución de rutas físicas desde sidecar
# -----------------------------------------------------------------------------


@pytest.mark.parametrize("storage_format", ["parquet", "feather"])
def test_resolve_flows_data_path_from_sidecar_accepts_supported_backends(
    tmp_path,
    formal_flow_artifact_factory,
    storage_format,
):
    """Verifica la resolución correcta del archivo principal en Parquet y Feather."""
    artifact = formal_flow_artifact_factory(
        tmp_path / "artifact.golondrina",
        storage_format=storage_format,
        with_aux=True,
    )

    root = artifact["paths"].root_dir
    payload = artifact["payload"]

    issues = []

    data_path = _resolve_flows_data_path_from_sidecar(
        root,
        payload,
        storage_format=storage_format,
        strict=False,
        issues=issues,
    )

    assert data_path == artifact["data_path"]
    assert data_path.exists()
    assert issues == []


def test_resolve_flows_data_path_from_sidecar_raises_on_backend_filename_mismatch(
    tmp_path,
    sidecar_payload_factory,
    assert_issue_present,
):
    """Verifica abort por inconsistencia entre backend Feather y `files.data` Parquet."""
    root = tmp_path / "artifact.golondrina"
    root.mkdir(parents=True, exist_ok=True)

    payload = sidecar_payload_factory(
        storage_format="feather",
        include_flow_to_trips=False,
    )
    payload["files"]["data"] = "flows.parquet"

    issues = []

    with pytest.raises(ExportError) as excinfo:
        _resolve_flows_data_path_from_sidecar(
            root,
            payload,
            storage_format="feather",
            strict=False,
            issues=issues,
        )

    assert excinfo.value.code == "READ_FLOWS.LAYOUT.MISSING_DATA_FILE"
    assert_issue_present(
        issues,
        "READ_FLOWS.LAYOUT.MISSING_DATA_FILE",
    )


def test_resolve_flow_to_trips_path_from_sidecar_returns_canonical_path_when_not_requested(
    tmp_path,
    sidecar_payload_factory,
):
    """Verifica que el auxiliar no solicitado resuelva al path canónico del backend."""
    root = tmp_path / "artifact.golondrina"
    root.mkdir(parents=True, exist_ok=True)

    payload = sidecar_payload_factory(
        storage_format="feather",
        include_flow_to_trips=False,
    )

    issues = []

    aux_path = _resolve_flow_to_trips_path_from_sidecar(
        root,
        payload,
        storage_format="feather",
        requested=False,
        strict=False,
        issues=issues,
    )

    assert aux_path == root / "flow_to_trips.feather"
    assert issues == []


@pytest.mark.parametrize("storage_format", ["parquet", "feather"])
def test_resolve_flow_to_trips_path_from_sidecar_accepts_supported_backends_when_requested(
    tmp_path,
    sidecar_payload_factory,
    storage_format,
):
    """Verifica la resolución correcta del auxiliar declarado para Parquet y Feather."""
    root = tmp_path / "artifact.golondrina"
    root.mkdir(parents=True, exist_ok=True)

    payload = sidecar_payload_factory(
        storage_format=storage_format,
        include_flow_to_trips=True,
    )

    issues = []

    aux_path = _resolve_flow_to_trips_path_from_sidecar(
        root,
        payload,
        storage_format=storage_format,
        requested=True,
        strict=False,
        issues=issues,
    )

    assert aux_path == root / payload["files"]["flow_to_trips"]
    assert issues == []


def test_resolve_flow_to_trips_path_from_sidecar_raises_on_backend_filename_mismatch(
    tmp_path,
    sidecar_payload_factory,
    assert_issue_present,
):
    """Verifica abort por inconsistencia entre backend Feather y auxiliar Parquet."""
    root = tmp_path / "artifact.golondrina"
    root.mkdir(parents=True, exist_ok=True)

    payload = sidecar_payload_factory(
        storage_format="feather",
        include_flow_to_trips=True,
    )
    payload["files"]["flow_to_trips"] = "flow_to_trips.parquet"

    issues = []

    with pytest.raises(ExportError) as excinfo:
        _resolve_flow_to_trips_path_from_sidecar(
            root,
            payload,
            storage_format="feather",
            requested=True,
            strict=False,
            issues=issues,
        )

    assert excinfo.value.code == "READ_FLOWS.IO.FLOW_TO_TRIPS_READ_FAILED"
    assert_issue_present(
        issues,
        "READ_FLOWS.IO.FLOW_TO_TRIPS_READ_FAILED",
    )