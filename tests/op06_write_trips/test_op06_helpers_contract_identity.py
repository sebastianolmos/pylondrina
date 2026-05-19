from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from pylondrina.errors import ExportError, ValidationError
from pylondrina.io.trips import (
    WriteTripsOptions,
    _extract_validated_flag,
    _resolve_trips_artifact_paths,
    _resolve_write_identity_and_sidecar,
    _validate_write_contract,
    _write_sidecar_json,
)
from pylondrina.reports import Issue


def _issue_codes(issues: list[Issue]) -> list[str]:
    """Retorna los códigos de issues emitidos durante el test."""
    return [issue.code for issue in issues]


def _assert_issue_present(issues: list[Issue], code: str) -> None:
    """Verifica que un código de issue esperado esté presente."""
    codes = _issue_codes(issues)
    assert code in codes, f"No se encontró {code}. Codes actuales: {codes}"


def _assert_issue_absent(issues: list[Issue], code: str) -> None:
    """Verifica que un código de issue no esté presente."""
    codes = _issue_codes(issues)
    assert code not in codes, f"Se encontró inesperadamente {code}. Codes actuales: {codes}"


def _load_json(path: Path) -> dict:
    """Carga un archivo JSON desde disco."""
    return json.loads(path.read_text(encoding="utf-8"))


def test_extract_validated_flag_reads_current_and_legacy_metadata():
    """Verifica lectura del flag de validación desde metadata actual y legacy."""
    assert _extract_validated_flag({"is_validated": True}) is True
    assert _extract_validated_flag({"is_validated": False}) is False
    assert _extract_validated_flag({"flags": {"validated": True}}) is True
    assert _extract_validated_flag({"flags": {"validated": False}}) is False
    assert _extract_validated_flag({}) is False
    assert _extract_validated_flag(None) is False


def test_validate_write_contract_accepts_validated_tripdataset(
    tmp_path: Path,
    trip_dataset_validated,
):
    """Verifica el precheck exitoso para un TripDataset validado."""
    issues: list[Issue] = []

    _validate_write_contract(
        trip_dataset_validated,
        tmp_path / "artifact",
        WriteTripsOptions(),
        issues=issues,
    )

    _assert_issue_absent(issues, "WRT.VALIDATION.REQUIRED_NOT_VALIDATED")
    _assert_issue_absent(issues, "WRT.CORE.INVALID_TRIPDATASET")
    _assert_issue_absent(issues, "WRT.CORE.INVALID_DATA_SURFACE")
    _assert_issue_absent(issues, "WRT.OPTIONS.UNSUPPORTED_STORAGE_FORMAT")


def test_validate_write_contract_raises_when_dataset_is_not_validated(
    tmp_path: Path,
    trip_dataset_unvalidated,
):
    """Verifica fatal por dataset no validado cuando `require_validated=True`."""
    issues: list[Issue] = []

    with pytest.raises(ValidationError) as excinfo:
        _validate_write_contract(
            trip_dataset_unvalidated,
            tmp_path / "artifact",
            WriteTripsOptions(require_validated=True),
            issues=issues,
        )

    assert excinfo.value.code == "WRT.VALIDATION.REQUIRED_NOT_VALIDATED"
    _assert_issue_present(issues, "WRT.VALIDATION.REQUIRED_NOT_VALIDATED")


def test_validate_write_contract_allows_empty_dataframe_with_info_issue(
    tmp_path: Path,
    trip_dataset_validated,
):
    """Verifica que un dataframe vacío sea persistible con issue informativo."""
    trips = trip_dataset_validated
    trips.data = trips.data.iloc[0:0].copy()

    issues: list[Issue] = []

    _validate_write_contract(
        trips,
        tmp_path / "artifact",
        WriteTripsOptions(),
        issues=issues,
    )

    _assert_issue_present(issues, "WRT.CORE.EMPTY_DATAFRAME")

    empty_issue = next(issue for issue in issues if issue.code == "WRT.CORE.EMPTY_DATAFRAME")
    assert empty_issue.level == "info"


def test_validate_write_contract_raises_for_unsupported_storage_format(
    tmp_path: Path,
    trip_dataset_validated,
):
    """Verifica fatal por `storage_format` no soportado."""
    issues: list[Issue] = []

    with pytest.raises(ExportError) as excinfo:
        _validate_write_contract(
            trip_dataset_validated,
            tmp_path / "artifact",
            WriteTripsOptions(storage_format="csv"),  # type: ignore[arg-type]
            issues=issues,
        )

    assert excinfo.value.code == "WRT.OPTIONS.UNSUPPORTED_STORAGE_FORMAT"
    _assert_issue_present(issues, "WRT.OPTIONS.UNSUPPORTED_STORAGE_FORMAT")


def test_validate_write_contract_raises_for_invalid_parquet_compression(
    tmp_path: Path,
    trip_dataset_validated,
):
    """Verifica fatal por compresión Parquet no soportada."""
    issues: list[Issue] = []

    with pytest.raises(ExportError) as excinfo:
        _validate_write_contract(
            trip_dataset_validated,
            tmp_path / "artifact",
            WriteTripsOptions(
                storage_format="parquet",
                parquet_compression="invalid_codec",  # type: ignore[arg-type]
            ),
            issues=issues,
        )

    assert excinfo.value.code == "WRT.OPTIONS.UNSUPPORTED_PARQUET_COMPRESSION"
    _assert_issue_present(issues, "WRT.OPTIONS.UNSUPPORTED_PARQUET_COMPRESSION")


def test_validate_write_contract_raises_for_invalid_feather_compression(
    tmp_path: Path,
    trip_dataset_validated,
):
    """Verifica fatal por compresión Feather no soportada."""
    issues: list[Issue] = []

    with pytest.raises(ExportError) as excinfo:
        _validate_write_contract(
            trip_dataset_validated,
            tmp_path / "artifact",
            WriteTripsOptions(
                storage_format="feather",
                feather_compression="gzip",  # type: ignore[arg-type]
            ),
            issues=issues,
        )

    assert excinfo.value.code == "WRT.OPTIONS.UNSUPPORTED_FEATHER_COMPRESSION"
    _assert_issue_present(issues, "WRT.OPTIONS.UNSUPPORTED_FEATHER_COMPRESSION")


def test_validate_write_contract_raises_when_destination_exists_in_error_if_exists_mode(
    tmp_path: Path,
    trip_dataset_validated,
):
    """Verifica fatal temprano si el destino existe y `mode='error_if_exists'`."""
    artifact_dir = tmp_path / "artifact"
    artifact_dir.mkdir(parents=True)
    sentinel = artifact_dir / "sentinel.txt"
    sentinel.write_text("do not overwrite", encoding="utf-8")

    issues: list[Issue] = []

    with pytest.raises(ExportError) as excinfo:
        _validate_write_contract(
            trip_dataset_validated,
            artifact_dir,
            WriteTripsOptions(
                mode="error_if_exists",
                normalize_artifact_dir=False,
            ),
            issues=issues,
        )

    assert excinfo.value.code == "WRT.DEST.ALREADY_EXISTS"
    _assert_issue_present(issues, "WRT.DEST.ALREADY_EXISTS")

    assert sentinel.exists()
    assert sentinel.read_text(encoding="utf-8") == "do not overwrite"


def test_validate_write_contract_raises_for_non_json_safe_metadata(
    tmp_path: Path,
    trip_dataset_validated,
):
    """Verifica fatal si metadata contiene objetos no serializables a JSON."""
    trips = trip_dataset_validated
    trips.metadata = copy.deepcopy(trips.metadata)
    trips.metadata["not_json_safe"] = {1, 2, 3}

    issues: list[Issue] = []

    with pytest.raises(ExportError) as excinfo:
        _validate_write_contract(
            trips,
            tmp_path / "artifact",
            WriteTripsOptions(),
            issues=issues,
        )

    assert excinfo.value.code == "WRT.JSON.NOT_SERIALIZABLE"
    _assert_issue_present(issues, "WRT.JSON.NOT_SERIALIZABLE")


def test_resolve_write_identity_and_sidecar_preserves_existing_dataset_id(
    tmp_path: Path,
    trip_dataset_validated,
):
    """Verifica preservación de `dataset_id` y generación de nuevo `artifact_id`."""
    trips = trip_dataset_validated
    paths = _resolve_trips_artifact_paths(tmp_path / "artifact")

    resolved = _resolve_write_identity_and_sidecar(
        trips,
        paths,
        WriteTripsOptions(storage_format="parquet"),
        existing_issues=[],
    )

    assert resolved.dataset_id_status == "preserved"
    assert resolved.dataset_id == trips.metadata["dataset_id"]
    assert resolved.artifact_id.startswith("art_")

    assert resolved.issues == []

    assert resolved.metadata_for_persist["dataset_id"] == resolved.dataset_id
    assert resolved.metadata_for_persist["artifact_id"] == resolved.artifact_id
    assert resolved.metadata_for_persist["is_validated"] is True
    assert resolved.metadata_for_persist["events"][-1]["op"] == "write_trips"

    assert resolved.sidecar_payload["dataset_id"] == resolved.dataset_id
    assert resolved.sidecar_payload["artifact_id"] == resolved.artifact_id
    assert resolved.sidecar_payload["storage"]["format"] == "parquet"
    assert resolved.sidecar_payload["files"]["data"] == "trips.parquet"
    assert resolved.sidecar_payload["files"]["metadata"] == "trips.metadata.json"

    assert resolved.files_written == ["trips.parquet", "trips.metadata.json"]


def test_resolve_write_identity_and_sidecar_creates_missing_dataset_id(
    tmp_path: Path,
    trip_dataset_without_dataset_id,
):
    """Verifica creación de `dataset_id` faltante y issue informativo."""
    trips = trip_dataset_without_dataset_id
    paths = _resolve_trips_artifact_paths(tmp_path / "artifact")

    assert "dataset_id" not in trips.metadata

    resolved = _resolve_write_identity_and_sidecar(
        trips,
        paths,
        WriteTripsOptions(storage_format="parquet"),
        existing_issues=[],
    )

    assert resolved.dataset_id_status == "created"
    assert resolved.dataset_id.startswith("dset_")
    assert resolved.artifact_id.startswith("art_")

    _assert_issue_present(resolved.issues, "WRT.METADATA.DATASET_ID_CREATED")

    created_issue = next(
        issue for issue in resolved.issues
        if issue.code == "WRT.METADATA.DATASET_ID_CREATED"
    )
    assert created_issue.level == "info"

    assert resolved.metadata_for_persist["dataset_id"] == resolved.dataset_id
    assert resolved.metadata_for_persist["artifact_id"] == resolved.artifact_id
    assert resolved.sidecar_payload["dataset_id"] == resolved.dataset_id
    assert resolved.sidecar_payload["artifact_id"] == resolved.artifact_id
    assert resolved.metadata_for_persist["events"][-1]["op"] == "write_trips"


def test_resolve_write_identity_and_sidecar_regenerates_invalid_dataset_id(
    tmp_path: Path,
    trip_dataset_validated,
):
    """Verifica regeneración de `dataset_id` vacío con issue recuperable."""
    trips = trip_dataset_validated
    trips.metadata = copy.deepcopy(trips.metadata)
    trips.metadata["dataset_id"] = ""

    paths = _resolve_trips_artifact_paths(tmp_path / "artifact")

    resolved = _resolve_write_identity_and_sidecar(
        trips,
        paths,
        WriteTripsOptions(storage_format="parquet"),
        existing_issues=[],
    )

    assert resolved.dataset_id_status == "regenerated"
    assert resolved.dataset_id.startswith("dset_")
    assert resolved.artifact_id.startswith("art_")

    _assert_issue_present(resolved.issues, "WRT.METADATA.DATASET_ID_REGENERATED")

    regenerated_issue = next(
        issue for issue in resolved.issues
        if issue.code == "WRT.METADATA.DATASET_ID_REGENERATED"
    )
    assert regenerated_issue.level == "warning"

    assert resolved.metadata_for_persist["dataset_id"] == resolved.dataset_id
    assert resolved.sidecar_payload["dataset_id"] == resolved.dataset_id
    assert resolved.metadata_for_persist["events"][-1]["op"] == "write_trips"


def test_resolve_write_identity_and_sidecar_builds_feather_backend_sidecar(
    tmp_path: Path,
    trip_dataset_validated,
):
    """Verifica que el sidecar Feather declare backend, archivo y opciones correctas."""
    trips = trip_dataset_validated
    paths = _resolve_trips_artifact_paths(tmp_path / "artifact")

    resolved = _resolve_write_identity_and_sidecar(
        trips,
        paths,
        WriteTripsOptions(
            storage_format="feather",
            feather_compression="lz4",
        ),
        existing_issues=[],
    )

    sidecar = resolved.sidecar_payload

    assert sidecar["storage"]["format"] == "feather"
    assert sidecar["storage"]["options"]["compression"] == "lz4"
    assert sidecar["storage"]["options"]["version"] == 2

    assert sidecar["files"]["data"] == "trips.feather"
    assert sidecar["files"]["metadata"] == "trips.metadata.json"
    assert resolved.files_written == ["trips.feather", "trips.metadata.json"]

    assert sidecar["dataset_id"] == resolved.dataset_id
    assert sidecar["artifact_id"] == resolved.artifact_id
    assert sidecar["metadata"]["events"][-1]["op"] == "write_trips"


def test_resolve_write_identity_and_sidecar_event_summarizes_existing_issues(
    tmp_path: Path,
    trip_dataset_validated,
):
    """Verifica que el evento `write_trips` incluya issues previos en `issues_summary`."""
    trips = trip_dataset_validated
    paths = _resolve_trips_artifact_paths(tmp_path / "artifact")

    prior_issues = [
        Issue(
            level="info",
            code="CUSTOM.PRIOR.INFO",
            message="Issue previo de prueba",
        )
    ]

    resolved = _resolve_write_identity_and_sidecar(
        trips,
        paths,
        WriteTripsOptions(storage_format="parquet"),
        existing_issues=prior_issues,
    )

    event = resolved.metadata_for_persist["events"][-1]

    assert event["op"] == "write_trips"
    assert event["issues_summary"]["counts"]["info"] >= 1

    top_codes = {entry["code"] for entry in event["issues_summary"]["top_codes"]}
    assert "CUSTOM.PRIOR.INFO" in top_codes

    assert resolved.sidecar_payload["metadata"]["events"][-1] == event


def test_write_sidecar_json_writes_formal_metadata_file(
    tmp_path: Path,
    make_sidecar_payload,
):
    """Verifica escritura física mínima de `trips.metadata.json`."""
    artifact_dir = tmp_path / "artifact"
    artifact_dir.mkdir(parents=True)

    paths = _resolve_trips_artifact_paths(artifact_dir)

    payload = make_sidecar_payload(
        storage_format="feather",
        feather_compression="lz4",
        dataset_id="dset_sidecar_test",
        artifact_id="art_sidecar_test",
    )

    issues: list[Issue] = []

    _write_sidecar_json(
        payload,
        paths.sidecar_path,
        issues=issues,
        destination_path=paths.root_dir,
        dataset_id=payload["dataset_id"],
        artifact_id=payload["artifact_id"],
    )

    assert issues == []
    assert paths.sidecar_path.exists()

    loaded = _load_json(paths.sidecar_path)

    assert loaded["dataset_type"] == "trips"
    assert loaded["format"] == "golondrina"
    assert loaded["layout_version"] == "1.1"

    assert loaded["storage"]["format"] == "feather"
    assert loaded["storage"]["options"]["compression"] == "lz4"
    assert loaded["storage"]["options"]["version"] == 2

    assert loaded["files"]["data"] == "trips.feather"
    assert loaded["files"]["metadata"] == "trips.metadata.json"

    assert loaded["dataset_id"] == payload["dataset_id"]
    assert loaded["artifact_id"] == payload["artifact_id"]
    assert "schema" in loaded
    assert "schema_effective" in loaded
    assert "provenance" in loaded
    assert "metadata" in loaded