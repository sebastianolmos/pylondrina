from __future__ import annotations

import copy
from pathlib import Path

import pytest

from pylondrina.errors import ExportError
from pylondrina.io.trips import (
    ReadTripsOptions,
    _append_event,
    _build_io_event,
    _build_issues_summary,
    _build_read_trips_summary,
    _compare_schema_snapshots,
    _extract_correspondence_from_metadata,
    _finalize_loaded_metadata_state,
    _options_to_read_parameters,
    _resolve_read_schema_state,
)
from pylondrina.reports import Issue
from pylondrina.schema import TripSchema, TripSchemaEffective


def _make_two_field_schema_from_minimal(
    trip_schema_minimal: TripSchema,
    *,
    version: str,
) -> TripSchema:
    """Construye un schema reducido para forzar mismatch observable en lectura."""
    selected_fields = ("movement_id", "trip_id")

    return TripSchema(
        version=version,
        fields={
            field_name: copy.deepcopy(trip_schema_minimal.fields[field_name])
            for field_name in selected_fields
        },
        required=list(selected_fields),
        semantic_rules=None,
    )


# -----------------------------------------------------------------------------
# Bloque 4. Helpers de reconstrucción de schema
# -----------------------------------------------------------------------------


def test_resolve_read_schema_state_reconstructs_schema_from_metadata(
    trip_schema_minimal,
    trip_schema_effective_minimal,
    make_sidecar_payload,
) -> None:
    """Verifica reconstrucción de schema y schema_effective desde el sidecar."""
    payload = make_sidecar_payload(
        schema=trip_schema_minimal,
        schema_effective=trip_schema_effective_minimal,
    )

    state = _resolve_read_schema_state(
        payload,
        ReadTripsOptions(
            schema=None,
            strict=False,
            keep_metadata=True,
        ),
    )

    assert isinstance(state.schema, TripSchema)
    assert state.schema.version == trip_schema_minimal.version
    assert state.schema_source == "metadata"
    assert state.schema_mismatch is False

    assert isinstance(state.schema_effective, TripSchemaEffective)
    assert state.schema_effective.to_dict() == trip_schema_effective_minimal.to_dict()

    assert state.issues == []


def test_resolve_read_schema_state_prioritizes_options_schema_and_reports_mismatch(
    trip_schema_minimal,
    make_sidecar_payload,
    assert_issue_present,
) -> None:
    """Verifica precedencia de options.schema y warning por mismatch observable."""
    schema_options = _make_two_field_schema_from_minimal(
        trip_schema_minimal,
        version="9.9",
    )

    payload = make_sidecar_payload(schema=trip_schema_minimal)

    state = _resolve_read_schema_state(
        payload,
        ReadTripsOptions(
            schema=schema_options,
            strict=False,
            keep_metadata=True,
        ),
    )

    assert state.schema is schema_options
    assert state.schema.version == schema_options.version
    assert state.schema_source == "options"
    assert state.schema_mismatch is True

    assert_issue_present(state.issues, "READ.SCHEMA.MISMATCH")


def test_resolve_read_schema_state_raises_on_schema_mismatch_when_strict(
    trip_schema_minimal,
    make_sidecar_payload,
) -> None:
    """Verifica que strict=True vuelva fatal el mismatch entre schemas."""
    schema_options = _make_two_field_schema_from_minimal(
        trip_schema_minimal,
        version="9.9",
    )

    payload = make_sidecar_payload(schema=trip_schema_minimal)

    with pytest.raises(ExportError) as excinfo:
        _resolve_read_schema_state(
            payload,
            ReadTripsOptions(
                schema=schema_options,
                strict=True,
                keep_metadata=True,
            ),
        )

    assert excinfo.value.code == "READ.SCHEMA.MISMATCH"


def test_resolve_read_schema_state_ignores_invalid_metadata_schema_when_explicit_schema_exists(
    trip_schema_minimal,
    make_sidecar_payload,
    assert_issue_present,
) -> None:
    """Verifica recovery cuando el schema del sidecar es inválido pero options.schema es usable."""
    payload = make_sidecar_payload()
    payload["schema"] = {
        "version": "bad",
        "fields": "not-a-mapping",
    }

    state = _resolve_read_schema_state(
        payload,
        ReadTripsOptions(
            schema=trip_schema_minimal,
            strict=False,
            keep_metadata=True,
        ),
    )

    assert state.schema is trip_schema_minimal
    assert state.schema_source == "options"
    assert state.schema_mismatch is False

    assert_issue_present(state.issues, "READ.SCHEMA.METADATA_INVALID_IGNORED")


def test_resolve_read_schema_state_raises_when_no_schema_can_be_recovered(
    make_sidecar_payload,
) -> None:
    """Verifica error fatal cuando no existe schema explícito ni snapshot recuperable."""
    payload = make_sidecar_payload()
    payload.pop("schema")

    with pytest.raises(ExportError) as excinfo:
        _resolve_read_schema_state(
            payload,
            ReadTripsOptions(
                schema=None,
                strict=False,
                keep_metadata=True,
            ),
        )

    assert excinfo.value.code == "READ.SCHEMA.UNAVAILABLE"


def test_resolve_read_schema_state_defaults_schema_effective_when_missing_and_not_strict(
    make_sidecar_payload,
    assert_issue_present,
) -> None:
    """Verifica recovery de schema_effective faltante con strict=False."""
    payload = make_sidecar_payload()
    payload.pop("schema_effective")

    state = _resolve_read_schema_state(
        payload,
        ReadTripsOptions(
            schema=None,
            strict=False,
            keep_metadata=True,
        ),
    )

    assert isinstance(state.schema_effective, TripSchemaEffective)
    assert state.schema_effective.to_dict() == TripSchemaEffective().to_dict()

    assert_issue_present(state.issues, "READ.SCHEMA_EFFECTIVE.DEFAULTED")


def test_resolve_read_schema_state_raises_when_schema_effective_is_missing_and_strict(
    make_sidecar_payload,
) -> None:
    """Verifica error fatal por schema_effective faltante con strict=True."""
    payload = make_sidecar_payload()
    payload.pop("schema_effective")

    with pytest.raises(ExportError) as excinfo:
        _resolve_read_schema_state(
            payload,
            ReadTripsOptions(
                schema=None,
                strict=True,
                keep_metadata=True,
            ),
        )

    assert excinfo.value.code == "READ.SCHEMA_EFFECTIVE.DEFAULTED"


def test_compare_schema_snapshots_detects_mismatch_and_equality(
    trip_schema_minimal,
) -> None:
    """Verifica diferencias observables entre schemas y caso idéntico sin mismatch."""
    schema_variant = _make_two_field_schema_from_minimal(
        trip_schema_minimal,
        version="2.0",
    )

    diff = _compare_schema_snapshots(schema_variant, trip_schema_minimal)

    assert diff["schema_mismatch"] is True
    assert "movement_seq" in diff["required_diff"]
    assert diff["fields_diff_total"] > 0
    assert isinstance(diff["fields_diff_sample"], list)

    same = _compare_schema_snapshots(trip_schema_minimal, trip_schema_minimal)

    assert same["schema_mismatch"] is False
    assert same["required_diff"] == []
    assert same["fields_diff_total"] == 0


# -----------------------------------------------------------------------------
# Bloque 5. Helpers de metadata, identidad y correspondencias
# -----------------------------------------------------------------------------


def test_finalize_loaded_metadata_state_preserves_ids_and_forces_unvalidated(
    tmp_path: Path,
    assert_issue_present,
    assert_counts_by_level,
) -> None:
    """Verifica finalización normal de metadata con IDs válidos y validación forzada a False."""
    metadata = {
        "dataset_id": "dset_ok",
        "artifact_id": "art_ok",
        "is_validated": True,
        "events": [],
    }

    sidecar_payload = {
        "dataset_id": "dset_ok",
        "artifact_id": "art_ok",
    }

    state = _finalize_loaded_metadata_state(
        metadata,
        sidecar_payload=sidecar_payload,
        strict=False,
        destination_path=tmp_path / "fake_artifact",
    )

    assert state.dataset_id == "dset_ok"
    assert state.dataset_id_status == "loaded"
    assert state.artifact_id == "art_ok"
    assert state.artifact_id_status == "loaded"

    assert state.metadata["dataset_id"] == "dset_ok"
    assert state.metadata["artifact_id"] == "art_ok"
    assert state.metadata["is_validated"] is False

    assert_issue_present(state.issues, "READ.METADATA.VALIDATED_FORCED_FALSE")
    assert_counts_by_level(state.issues, info=1)


def test_finalize_loaded_metadata_state_prioritizes_top_level_sidecar_ids(
    tmp_path: Path,
    assert_issue_present,
) -> None:
    """Verifica que dataset_id y artifact_id top-level del sidecar tengan precedencia."""
    metadata = {
        "dataset_id": "dset_from_metadata",
        "artifact_id": "art_from_metadata",
        "is_validated": True,
        "events": [],
    }

    sidecar_payload = {
        "dataset_id": "dset_top_level",
        "artifact_id": "art_top_level",
    }

    state = _finalize_loaded_metadata_state(
        metadata,
        sidecar_payload=sidecar_payload,
        strict=False,
        destination_path=tmp_path / "fake_artifact",
    )

    assert state.dataset_id == "dset_top_level"
    assert state.artifact_id == "art_top_level"

    assert state.metadata["dataset_id"] == "dset_top_level"
    assert state.metadata["artifact_id"] == "art_top_level"
    assert state.metadata["is_validated"] is False

    assert_issue_present(state.issues, "READ.METADATA.VALIDATED_FORCED_FALSE")


def test_finalize_loaded_metadata_state_recovers_invalid_ids_when_not_strict(
    tmp_path: Path,
    assert_issue_present,
) -> None:
    """Verifica regeneración de dataset_id y artifact_id=None bajo strict=False."""
    metadata = {
        "is_validated": True,
        "events": [],
    }

    sidecar_payload = {
        "dataset_id": "",
        "artifact_id": None,
    }

    state = _finalize_loaded_metadata_state(
        metadata,
        sidecar_payload=sidecar_payload,
        strict=False,
        destination_path=tmp_path / "fake_artifact",
    )

    assert state.dataset_id_status == "regenerated"
    assert isinstance(state.dataset_id, str)
    assert state.dataset_id

    assert state.artifact_id is None
    assert state.artifact_id_status == "missing_or_invalid"

    assert state.metadata["dataset_id"] == state.dataset_id
    assert state.metadata["artifact_id"] is None
    assert state.metadata["is_validated"] is False

    assert_issue_present(state.issues, "READ.METADATA.DATASET_ID_REGENERATED")
    assert_issue_present(state.issues, "READ.METADATA.ARTIFACT_ID_SET_NONE")
    assert_issue_present(state.issues, "READ.METADATA.VALIDATED_FORCED_FALSE")


def test_finalize_loaded_metadata_state_raises_for_invalid_dataset_id_when_strict(
    tmp_path: Path,
) -> None:
    """Verifica error fatal si dataset_id es inválido en modo estricto."""
    metadata = {
        "is_validated": True,
        "events": [],
    }

    sidecar_payload = {
        "dataset_id": "",
        "artifact_id": "art_ok",
    }

    with pytest.raises(ExportError) as excinfo:
        _finalize_loaded_metadata_state(
            metadata,
            sidecar_payload=sidecar_payload,
            strict=True,
            destination_path=tmp_path / "fake_artifact",
        )

    assert excinfo.value.code == "READ.SIDECAR.INVALID_TOP_LEVEL"


def test_finalize_loaded_metadata_state_raises_for_invalid_artifact_id_when_strict(
    tmp_path: Path,
) -> None:
    """Verifica error fatal si artifact_id es inválido en modo estricto."""
    metadata = {
        "dataset_id": "dset_ok",
        "is_validated": True,
        "events": [],
    }

    sidecar_payload = {
        "dataset_id": "dset_ok",
        "artifact_id": None,
    }

    with pytest.raises(ExportError) as excinfo:
        _finalize_loaded_metadata_state(
            metadata,
            sidecar_payload=sidecar_payload,
            strict=True,
            destination_path=tmp_path / "fake_artifact",
        )

    assert excinfo.value.code == "READ.SIDECAR.INVALID_TOP_LEVEL"


def test_extract_correspondence_from_metadata_returns_safe_mappings(
) -> None:
    """Verifica extracción segura de field_correspondence y value_correspondence."""
    metadata = {
        "mappings": {
            "field_correspondence": {
                "movement_id": "id_original",
                "mode": "modo_original",
            },
            "value_correspondence": {
                "mode": {
                    "micro": "bus",
                    "metrotren": "train",
                }
            },
        }
    }

    field_corr, value_corr = _extract_correspondence_from_metadata(metadata)

    assert field_corr == {
        "movement_id": "id_original",
        "mode": "modo_original",
    }
    assert value_corr == {
        "mode": {
            "micro": "bus",
            "metrotren": "train",
        }
    }

    field_corr_empty, value_corr_empty = _extract_correspondence_from_metadata({})
    assert field_corr_empty == {}
    assert value_corr_empty == {}

    field_corr_bad, value_corr_bad = _extract_correspondence_from_metadata(
        {"mappings": "bad"}
    )
    assert field_corr_bad == {}
    assert value_corr_bad == {}


# -----------------------------------------------------------------------------
# Bloque 6. Helpers de parámetros, summary y evento de lectura
# -----------------------------------------------------------------------------


def test_options_to_read_parameters_serializes_default_and_explicit_schema(
    tmp_path: Path,
    trip_schema_minimal,
    assert_json_safe,
) -> None:
    """Verifica serialización estable de ReadTripsOptions para reportes y eventos."""
    artifact_path = tmp_path / "artifact.golondrina"

    params_default = _options_to_read_parameters(
        path=artifact_path,
        options=ReadTripsOptions(),
    )

    assert params_default["path"] == str(artifact_path.expanduser())
    assert params_default["strict"] is False
    assert params_default["keep_metadata"] is True
    assert params_default["schema"] is None

    params_schema = _options_to_read_parameters(
        path=artifact_path,
        options=ReadTripsOptions(
            schema=trip_schema_minimal,
            strict=True,
            keep_metadata=False,
        ),
    )

    assert params_schema["path"] == str(artifact_path.expanduser())
    assert params_schema["strict"] is True
    assert params_schema["keep_metadata"] is False
    assert params_schema["schema"]["source"] == "options"
    assert params_schema["schema"]["version"] == trip_schema_minimal.version

    assert_json_safe(params_default, "params_default")
    assert_json_safe(params_schema, "params_schema")


def test_build_read_trips_summary_returns_compact_and_json_safe_summary(
    tmp_path: Path,
    assert_json_safe,
) -> None:
    """Verifica summary mínimo y estable de OP-07."""
    artifact_path = tmp_path / "artifact"

    n_rows = 3
    n_columns = 12
    dataset_id = "dset_001"
    artifact_id = "art_001"

    summary = _build_read_trips_summary(
        n_rows=n_rows,
        n_columns=n_columns,
        path=artifact_path,
        storage_format="feather",
        schema_source="metadata",
        schema_mismatch=False,
        dataset_id_status="loaded",
        dataset_id=dataset_id,
        artifact_id_status="loaded",
        artifact_id=artifact_id,
    )

    assert summary["n_rows"] == n_rows
    assert summary["n_columns"] == n_columns
    assert Path(summary["path"]) == artifact_path

    assert summary["storage_format"] == "feather"
    assert summary["schema_source"] == "metadata"
    assert summary["schema_mismatch"] is False

    assert summary["dataset_id"] == dataset_id
    assert summary["dataset_id_status"] == "loaded"
    assert summary["artifact_id"] == artifact_id
    assert summary["artifact_id_status"] == "loaded"

    assert_json_safe(summary, "read_summary")


def test_build_issues_summary_io_event_and_append_event_preserve_operational_traceability(
    assert_json_safe,
) -> None:
    """Verifica resumen de issues, forma del evento IO y append sin mutar metadata original."""
    issues = [
        Issue(
            level="info",
            code="READ.METADATA.VALIDATED_FORCED_FALSE",
            message="forced false",
        ),
        Issue(
            level="warning",
            code="READ.SCHEMA.MISMATCH",
            message="schema mismatch",
        ),
        Issue(
            level="warning",
            code="READ.SCHEMA.MISMATCH",
            message="schema mismatch again",
        ),
    ]

    issues_summary = _build_issues_summary(issues)

    assert issues_summary["counts"]["info"] == 1
    assert issues_summary["counts"]["warning"] == 2
    assert issues_summary["counts"]["error"] == 0
    assert issues_summary["top_codes"][0]["code"] == "READ.SCHEMA.MISMATCH"
    assert issues_summary["top_codes"][0]["count"] == 2

    parameters = {
        "path": "artifact.golondrina",
        "strict": False,
        "keep_metadata": True,
        "schema": None,
    }

    summary = {
        "n_rows": 3,
        "n_columns": 12,
        "path": "artifact.golondrina",
        "storage_format": "parquet",
        "schema_source": "metadata",
        "schema_mismatch": False,
        "dataset_id": "dset_001",
        "dataset_id_status": "loaded",
        "artifact_id": "art_001",
        "artifact_id_status": "loaded",
    }

    event = _build_io_event(
        op="read_trips",
        parameters=parameters,
        summary=summary,
        issues_summary=issues_summary,
    )

    assert event["op"] == "read_trips"
    assert "ts_utc" in event
    assert event["parameters"] == parameters
    assert event["summary"] == summary
    assert event["issues_summary"]["counts"]["warning"] == 2

    metadata_in = {
        "events": [
            {"op": "previous"},
        ]
    }

    metadata_out = _append_event(metadata_in, event)

    assert len(metadata_in["events"]) == 1
    assert len(metadata_out["events"]) == 2
    assert metadata_out["events"][-1]["op"] == "read_trips"

    assert_json_safe(event, "read_event")
    assert_json_safe(metadata_out, "metadata_with_read_event")