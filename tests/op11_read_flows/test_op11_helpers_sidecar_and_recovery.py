from __future__ import annotations

import json

import pytest

from pylondrina.errors import ExportError
from pylondrina.io.flows import (
    _load_flow_sidecar,
    _recover_flow_read_state,
)


# -----------------------------------------------------------------------------
# Bloque 4. Carga de sidecar y recuperación de estado
# -----------------------------------------------------------------------------


def test_load_flow_sidecar_returns_formal_payload_without_issues(
    tmp_path,
    sidecar_payload_factory,
):
    """Verifica que un sidecar formal válido se lea completo y sin issues."""
    case_dir = tmp_path / "case_load_sidecar_happy"
    case_dir.mkdir(parents=True, exist_ok=True)

    sidecar_path = case_dir / "flows.metadata.json"

    payload = sidecar_payload_factory(
        storage_format="feather",
        include_flow_to_trips=True,
    )

    sidecar_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    issues = []

    loaded = _load_flow_sidecar(
        sidecar_path,
        strict=False,
        issues=issues,
        destination_path=case_dir,
    )

    assert loaded["dataset_type"] == payload["dataset_type"]
    assert loaded["storage"]["format"] == payload["storage"]["format"]
    assert loaded["files"]["data"] == payload["files"]["data"]
    assert (
        loaded["files"]["flow_to_trips"]
        == payload["files"]["flow_to_trips"]
    )
    assert issues == []


def test_load_flow_sidecar_raises_when_json_is_invalid(
    tmp_path,
    assert_issue_present,
):
    """Verifica que un sidecar con JSON ilegible aborte con issue de lectura."""
    case_dir = tmp_path / "case_load_sidecar_invalid_json"
    case_dir.mkdir(parents=True, exist_ok=True)

    sidecar_path = case_dir / "flows.metadata.json"
    sidecar_path.write_text(
        "{ invalid json ",
        encoding="utf-8",
    )

    issues = []

    with pytest.raises(ExportError) as excinfo:
        _load_flow_sidecar(
            sidecar_path,
            strict=False,
            issues=issues,
            destination_path=case_dir,
        )

    assert excinfo.value.code == "READ_FLOWS.IO.SIDECAR_READ_FAILED"
    assert_issue_present(
        issues,
        "READ_FLOWS.IO.SIDECAR_READ_FAILED",
    )


def test_load_flow_sidecar_raises_when_top_level_is_incomplete(
    tmp_path,
    assert_issue_present,
):
    """Verifica que un sidecar sin top-level obligatorio aborte formalmente."""
    case_dir = tmp_path / "case_load_sidecar_bad_top_level"
    case_dir.mkdir(parents=True, exist_ok=True)

    sidecar_path = case_dir / "flows.metadata.json"

    bad_payload = {
        "dataset_type": "flows",
        "format": "golondrina",
    }

    sidecar_path.write_text(
        json.dumps(bad_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    issues = []

    with pytest.raises(ExportError) as excinfo:
        _load_flow_sidecar(
            sidecar_path,
            strict=False,
            issues=issues,
            destination_path=case_dir,
        )

    assert excinfo.value.code == "READ_FLOWS.SIDECAR.INVALID_TOP_LEVEL"
    assert_issue_present(
        issues,
        "READ_FLOWS.SIDECAR.INVALID_TOP_LEVEL",
    )


@pytest.mark.parametrize("storage_format", ["parquet", "feather"])
def test_recover_flow_read_state_preserves_valid_sidecar_state(
    tmp_path,
    sidecar_payload_factory,
    storage_format,
):
    """Verifica recuperación completa y sin degradación desde sidecar válido."""
    payload = sidecar_payload_factory(
        storage_format=storage_format,
        include_flow_to_trips=True,
    )

    issues = []

    state = _recover_flow_read_state(
        payload,
        strict=False,
        issues=issues,
        destination_path=(
            tmp_path / f"fake_{storage_format}.golondrina"
        ),
    )

    assert state["storage_format"] == storage_format
    assert state["dataset_id"] == payload["dataset_id"]
    assert state["artifact_id"] == payload["artifact_id"]
    assert state["aggregation_spec"] == payload["aggregation_spec"]
    assert state["provenance"] == payload["provenance"]
    assert state["metadata"] == payload["metadata"]
    assert issues == []


def test_recover_flow_read_state_degrades_invalid_blocks_when_strict_false(
    tmp_path,
    sidecar_payload_factory,
    assert_issue_present,
):
    """Verifica la matriz de recuperación degradada bajo `strict=False`."""
    payload = sidecar_payload_factory(
        storage_format="feather",
        include_flow_to_trips=False,
    )

    payload["dataset_id"] = ""
    payload["artifact_id"] = None
    payload["aggregation_spec"] = None
    payload["provenance"] = None
    payload["metadata"] = None

    issues = []

    state = _recover_flow_read_state(
        payload,
        strict=False,
        issues=issues,
        destination_path=tmp_path / "fake_degraded_read.golondrina",
    )

    assert state["storage_format"] == payload["storage"]["format"]

    assert isinstance(state["dataset_id"], str)
    assert state["dataset_id"].startswith("dset_")

    assert state["artifact_id"] is None
    assert state["aggregation_spec"] == {}
    assert state["provenance"] == {}
    assert state["metadata"] == {}

    assert_issue_present(
        issues,
        "READ_FLOWS.METADATA.DATASET_ID_REGENERATED",
    )
    assert_issue_present(
        issues,
        "READ_FLOWS.METADATA.ARTIFACT_ID_SET_NONE",
    )
    assert_issue_present(
        issues,
        "READ_FLOWS.SIDECAR.AGGREGATION_SPEC_DEFAULTED",
    )
    assert_issue_present(
        issues,
        "READ_FLOWS.SIDECAR.PROVENANCE_DEFAULTED",
    )
    assert_issue_present(
        issues,
        "READ_FLOWS.SIDECAR.METADATA_DEFAULTED",
    )


def test_recover_flow_read_state_raises_for_unsupported_storage_format(
    tmp_path,
    sidecar_payload_factory,
    assert_issue_present,
):
    """Verifica que un backend no soportado impida la recuperación formal."""
    payload = sidecar_payload_factory(
        storage_format="feather",
        include_flow_to_trips=True,
    )

    payload["storage"]["format"] = "csv"

    issues = []

    with pytest.raises(ExportError) as excinfo:
        _recover_flow_read_state(
            payload,
            strict=False,
            issues=issues,
            destination_path=tmp_path / "fake_invalid_storage.golondrina",
        )

    assert excinfo.value.code == "READ_FLOWS.STORAGE.UNSUPPORTED_FORMAT"
    assert_issue_present(
        issues,
        "READ_FLOWS.STORAGE.UNSUPPORTED_FORMAT",
    )


def test_recover_flow_read_state_raises_on_invalid_dataset_id_when_strict_true(
    tmp_path,
    sidecar_payload_factory,
    assert_issue_present,
):
    """Verifica que `strict=True` impida regenerar un `dataset_id` inválido."""
    payload = sidecar_payload_factory(
        storage_format="parquet",
        include_flow_to_trips=False,
    )

    payload["dataset_id"] = ""

    issues = []

    with pytest.raises(ExportError) as excinfo:
        _recover_flow_read_state(
            payload,
            strict=True,
            issues=issues,
            destination_path=(
                tmp_path / "fake_strict_invalid_dataset_id.golondrina"
            ),
        )

    assert excinfo.value.code == "READ_FLOWS.SIDECAR.INVALID_TOP_LEVEL"
    assert_issue_present(
        issues,
        "READ_FLOWS.SIDECAR.INVALID_TOP_LEVEL",
    )