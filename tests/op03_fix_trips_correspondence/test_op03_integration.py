from __future__ import annotations

from copy import deepcopy
from typing import Any

import pandas as pd
import pytest

from pylondrina.datasets import TripDataset
from pylondrina.errors import FixError
from pylondrina.fixing import FixCorrespondenceOptions, fix_trips_correspondence
from pylondrina.reports import Issue, OperationReport
from pylondrina.schema import DomainSpec, FieldSpec, TripSchema, TripSchemaEffective


def make_field(
    name: str,
    dtype: str,
    *,
    required: bool = False,
    constraints: dict | None = None,
    domain: DomainSpec | None = None,
) -> FieldSpec:
    """Construye un FieldSpec mínimo equivalente al usado en el notebook de integración."""
    return FieldSpec(
        name=name,
        dtype=dtype,
        required=required,
        constraints=constraints,
        domain=domain,
    )


@pytest.fixture()
def base_fix_schema() -> TripSchema:
    """Schema mínimo compartido por los tests de integración de OP-03."""
    fields = {
        "movement_id": make_field("movement_id", "string", required=True),
        "user_id": make_field("user_id", "string", required=True),
        "mode": make_field(
            "mode",
            "categorical",
            domain=DomainSpec(
                values=["walk", "bus", "metro", "car", "unknown"],
                extendable=True,
            ),
        ),
        "purpose": make_field(
            "purpose",
            "categorical",
            domain=DomainSpec(
                values=["work", "study", "shopping", "health", "unknown"],
                extendable=True,
            ),
        ),
        "trip_weight": make_field("trip_weight", "float"),
    }
    return TripSchema(
        version="1.1",
        fields=fields,
        required=[name for name, field in fields.items() if field.required],
        semantic_rules=None,
    )


def _base_domains_effective(
    schema: TripSchema,
    fields_present: list[str],
) -> dict[str, dict[str, Any]]:
    """Construye el snapshot mínimo de dominios efectivos usado por las fixtures."""
    out: dict[str, dict[str, Any]] = {}

    for field_name in fields_present:
        field_spec = schema.fields.get(field_name)
        if field_spec is None or field_spec.domain is None:
            continue

        values = sorted(list(field_spec.domain.values))
        out[field_name] = {
            "values": values,
            "extended": False,
            "added_values": [],
            "unknown_value": "unknown" if "unknown" in values else None,
            "strict_applied": False,
        }

    return out


@pytest.fixture()
def make_tripdataset_fixture(base_fix_schema: TripSchema):
    """Factory de TripDataset sintético equivalente a la usada en el notebook de integración."""

    def _make(
        df: pd.DataFrame,
        *,
        schema: TripSchema | None = None,
        dataset_id: str = "ds-op03-it-001",
        is_validated: bool = True,
        field_correspondence: dict[str, str] | None = None,
        value_correspondence: dict[str, dict[Any, Any]] | None = None,
        events: list[dict[str, Any]] | None = None,
    ) -> TripDataset:
        schema_eff = schema or base_fix_schema

        if field_correspondence is None:
            field_correspondence = {
                column: column
                for column in df.columns
                if column in schema_eff.fields
            }

        if value_correspondence is None:
            value_correspondence = {}

        base_domains = _base_domains_effective(
            schema=schema_eff,
            fields_present=[
                column
                for column in df.columns
                if column in schema_eff.fields
            ],
        )

        metadata = {
            "dataset_id": dataset_id,
            "events": deepcopy(events) if events is not None else [],
            "is_validated": is_validated,
            "mappings": {
                "field_correspondence": deepcopy(field_correspondence),
                "value_correspondence": deepcopy(value_correspondence),
            },
            "domains_effective": deepcopy(base_domains),
        }

        schema_effective = TripSchemaEffective(
            dtype_effective={},
            overrides={},
            domains_effective=deepcopy(base_domains),
            temporal={},
            fields_effective=list(df.columns),
        )

        provenance = {
            "source": {
                "name": "synthetic",
                "entity": "trips",
                "version": "op03-integration-tests-v1",
            },
            "notes": ["fixture manual para tests de integración de OP-03"],
        }

        return TripDataset(
            data=df.copy(deep=True),
            schema=schema_eff,
            schema_version=schema_eff.version,
            provenance=provenance,
            field_correspondence=deepcopy(field_correspondence),
            value_correspondence=deepcopy(value_correspondence),
            metadata=metadata,
            schema_effective=schema_effective,
        )

    return _make


@pytest.fixture()
def tripdataset_canonical_small(make_tripdataset_fixture) -> TripDataset:
    """TripDataset canónico pequeño, ya validado y sin correcciones pendientes."""
    df = pd.DataFrame(
        {
            "movement_id": ["m1", "m2"],
            "user_id": ["u1", "u2"],
            "mode": ["bus", "walk"],
            "purpose": ["work", "study"],
            "trip_weight": [1.0, 2.0],
        }
    )
    return make_tripdataset_fixture(
        df,
        dataset_id="tripdataset_canonical_small",
        is_validated=True,
    )


@pytest.fixture()
def tripdataset_with_categories_to_fix(make_tripdataset_fixture) -> TripDataset:
    """TripDataset con columnas y categorías crudas que deben corregirse por OP-03."""
    df = pd.DataFrame(
        {
            "movement_id": ["m1", "m2", "m3"],
            "user_id": ["u1", "u2", "u3"],
            "modo_raw": ["BUS", "WALK", "BUS"],
            "purpose_raw": ["WORK", "STUDY", "WORK"],
            "trip_weight": [1.0, 2.0, 3.0],
        }
    )
    return make_tripdataset_fixture(
        df,
        dataset_id="tripdataset_with_categories_to_fix",
        is_validated=True,
        field_correspondence={
            "movement_id": "movement_id",
            "user_id": "user_id",
        },
    )


@pytest.fixture()
def tripdataset_unvalidated_small(make_tripdataset_fixture) -> TripDataset:
    """TripDataset pequeño no validado y con un evento previo de importación."""
    df = pd.DataFrame(
        {
            "movement_id": ["m1", "m2"],
            "user_id": ["u1", "u2"],
            "mode": ["BUS", "WALK"],
            "purpose": ["WORK", "STUDY"],
            "trip_weight": [1.0, 2.0],
        }
    )

    prior_events = [
        {
            "op": "import_trips",
            "ts_utc": "2026-04-02T20:00:00Z",
            "parameters": {"source_name": "synthetic"},
            "summary": {"rows_in": len(df), "rows_out": len(df)},
            "issues_summary": {"counts": {"info": 0, "warning": 0, "error": 0}},
        }
    ]

    return make_tripdataset_fixture(
        df,
        dataset_id="tripdataset_unvalidated_small",
        is_validated=False,
        events=prior_events,
    )


def _issue_codes(report_or_issues: OperationReport | list[Issue]) -> list[str]:
    """Retorna los códigos de issues desde un reporte o lista de issues."""
    issues = report_or_issues.issues if hasattr(report_or_issues, "issues") else report_or_issues
    return [issue.code for issue in issues]


def _assert_issue_present(report_or_issues: OperationReport | list[Issue], code: str) -> None:
    """Falla explícitamente si no aparece el código de issue esperado."""
    codes = _issue_codes(report_or_issues)
    assert code in codes, f"No se encontró {code}. Códigos actuales: {codes}"


def _assert_report_event_alignment(
    fixed: TripDataset,
    report: OperationReport,
) -> dict[str, Any]:
    """Verifica alineación mínima entre reporte y último evento, y retorna el evento."""
    assert fixed.metadata["events"]

    event = fixed.metadata["events"][-1]
    assert event["op"] == "fix_trips_correspondence"
    assert event["summary"] == report.summary
    assert event["parameters"] == report.parameters
    assert "issues_summary" in event

    return event


def test_fix_trips_correspondence_integration_applies_fields_then_values_and_updates_traces(
    tripdataset_with_categories_to_fix: TripDataset,
) -> None:
    """Verifica el caso principal: tabla corregida, mappings, dominios, evento y no mutación."""
    trips = tripdataset_with_categories_to_fix
    input_data_before = trips.data.copy(deep=True)
    input_metadata_before = deepcopy(trips.metadata)
    input_schema_effective_before = deepcopy(trips.schema_effective)

    field_corrections = {
        "modo_raw": "mode",
        "purpose_raw": "purpose",
    }
    value_corrections = {
        "mode": {"BUS": "bus", "WALK": "walk"},
        "purpose": {"WORK": "work", "STUDY": "study"},
    }

    fixed, report = fix_trips_correspondence(
        trips,
        field_corrections=field_corrections,
        value_corrections=value_corrections,
        options=FixCorrespondenceOptions(),
        correspondence_context={
            "reason": "normalización manual mínima",
            "notes": "caso principal correcto OP-03",
        },
    )

    expected = input_data_before.rename(columns=field_corrections)
    for field, mapping in value_corrections.items():
        expected[field] = expected[field].replace(mapping)

    data_after_field_fix = input_data_before.rename(columns=field_corrections)
    expected_replacements = sum(
        int(data_after_field_fix[field].isin(mapping.keys()).sum())
        for field, mapping in value_corrections.items()
    )

    assert isinstance(fixed, TripDataset)
    assert isinstance(report, OperationReport)
    assert report.ok is True
    assert fixed is not trips

    pd.testing.assert_frame_equal(trips.data, input_data_before)
    assert trips.metadata == input_metadata_before
    pd.testing.assert_frame_equal(fixed.data, expected)
    assert list(fixed.data.columns) == list(expected.columns)

    assert report.summary["n_rows"] == len(input_data_before)
    assert report.summary["n_field_corrections_requested"] == len(field_corrections)
    assert report.summary["n_field_corrections_applied"] == len(field_corrections)
    assert report.summary["n_value_corrections_fields_requested"] == len(value_corrections)
    assert report.summary["n_value_corrections_fields_applied"] == len(value_corrections)
    assert report.summary["n_value_replacements_applied"] == expected_replacements
    assert report.summary["noop"] is False
    assert set(report.summary["domains_effective_updated_fields"]) == set(value_corrections.keys())

    assert fixed.metadata["is_validated"] is False
    assert fixed.metadata["dataset_id"] == trips.metadata["dataset_id"]
    assert fixed.provenance == trips.provenance

    assert fixed.field_correspondence["mode"] == "modo_raw"
    assert fixed.field_correspondence["purpose"] == "purpose_raw"
    assert fixed.value_correspondence["mode"]["BUS"] == value_corrections["mode"]["BUS"]
    assert fixed.value_correspondence["purpose"]["WORK"] == value_corrections["purpose"]["WORK"]

    assert fixed.metadata["mappings"]["field_correspondence"] == fixed.field_correspondence
    assert fixed.metadata["mappings"]["value_correspondence"] == fixed.value_correspondence

    for field in value_corrections:
        observed_values = set(expected[field].dropna().tolist())
        assert set(fixed.metadata["domains_effective"][field]["values"]) == observed_values
        assert set(fixed.schema_effective.domains_effective[field]["values"]) == observed_values

    assert fixed.schema_effective.dtype_effective == input_schema_effective_before.dtype_effective
    assert fixed.schema_effective.overrides == input_schema_effective_before.overrides
    assert fixed.schema_effective.temporal == input_schema_effective_before.temporal
    assert set(fixed.schema_effective.fields_effective) == set(expected.columns)

    event = _assert_report_event_alignment(fixed, report)
    assert len(fixed.metadata["events"]) == len(input_metadata_before["events"]) + 1
    assert event["context"]["reason"] == "normalización manual mínima"


def test_fix_trips_correspondence_integration_invalid_context_aborts_without_side_effects(
    tripdataset_canonical_small: TripDataset,
) -> None:
    """Verifica que un correspondence_context inválido aborte sin tocar data ni metadata."""
    trips = tripdataset_canonical_small
    input_data_before = trips.data.copy(deep=True)
    input_metadata_before = deepcopy(trips.metadata)

    with pytest.raises(FixError) as exc_info:
        fix_trips_correspondence(
            trips,
            field_corrections=None,
            value_corrections=None,
            options=FixCorrespondenceOptions(),
            correspondence_context=["not", "a", "dict"],
        )

    assert exc_info.value.code == "FIX.CONTEXT.INVALID_ROOT"
    pd.testing.assert_frame_equal(trips.data, input_data_before)
    assert trips.metadata == input_metadata_before
    assert trips.metadata["events"] == []


def test_fix_trips_correspondence_integration_partial_apply_sanitizes_context_and_reports_warnings(
    tripdataset_with_categories_to_fix: TripDataset,
) -> None:
    """Verifica aplicación parcial con warnings, recodificación válida y contexto saneado."""
    trips = tripdataset_with_categories_to_fix
    input_data_before = trips.data.copy(deep=True)

    field_corrections = {
        "purpose_raw": "purpose",
        "missing_mode": "mode",
    }
    value_corrections = {
        "purpose": {
            "WORK": "work",
            "MISSING_VALUE": "health",
        }
    }

    fixed, report = fix_trips_correspondence(
        trips,
        field_corrections=field_corrections,
        value_corrections=value_corrections,
        options=FixCorrespondenceOptions(strict=False),
        correspondence_context={
            "reason": "ajuste parcial",
            "unknown_key": "debe descartarse",
            "notes": {"ok": "texto", "bad": object()},
        },
    )

    applicable_field_corrections = {"purpose_raw": field_corrections["purpose_raw"]}
    applicable_value_corrections = {"purpose": {"WORK": value_corrections["purpose"]["WORK"]}}

    expected = input_data_before.rename(columns=applicable_field_corrections)
    expected["purpose"] = expected["purpose"].replace(applicable_value_corrections["purpose"])
    expected_replacements = int(
        input_data_before["purpose_raw"].isin(applicable_value_corrections["purpose"].keys()).sum()
    )

    pd.testing.assert_frame_equal(trips.data, input_data_before)
    pd.testing.assert_frame_equal(fixed.data, expected)

    assert fixed.metadata["is_validated"] is False
    assert report.ok is True
    assert report.summary["noop"] is False
    assert report.summary["n_field_corrections_requested"] == len(field_corrections)
    assert report.summary["n_field_corrections_applied"] == len(applicable_field_corrections)
    assert report.summary["n_value_corrections_fields_requested"] == len(value_corrections)
    assert report.summary["n_value_corrections_fields_applied"] == len(applicable_value_corrections)
    assert report.summary["n_value_replacements_applied"] == expected_replacements

    for code in [
        "FIX.FIELD.SOURCE_COLUMN_MISSING",
        "FIX.FIELD.PARTIAL_APPLY",
        "FIX.VALUE.SOURCE_VALUES_NOT_FOUND",
        "FIX.CONTEXT.UNKNOWN_KEYS_DROPPED",
        "FIX.CONTEXT.NON_SERIALIZABLE_DROPPED",
    ]:
        _assert_issue_present(report, code)

    event = _assert_report_event_alignment(fixed, report)
    assert "unknown_key" not in event["context"]
    assert event["context"]["notes"]["ok"] == "texto"
    assert "bad" not in event["context"]["notes"]


def test_fix_trips_correspondence_integration_appends_event_and_preserves_identity(
    tripdataset_unvalidated_small: TripDataset,
) -> None:
    """Verifica append-only de eventos, preservación de identidad/provenance y parámetros efectivos."""
    trips = tripdataset_unvalidated_small
    prior_events_before = deepcopy(trips.metadata["events"])
    dataset_id_before = trips.metadata["dataset_id"]
    provenance_before = deepcopy(trips.provenance)

    value_corrections = {
        "mode": {"BUS": "bus", "WALK": "walk"},
        "purpose": {"WORK": "work", "STUDY": "study"},
    }

    fixed, report = fix_trips_correspondence(
        trips,
        field_corrections=None,
        value_corrections=value_corrections,
        options=FixCorrespondenceOptions(),
        correspondence_context={"reason": "normalización categórica"},
    )

    assert fixed is not trips
    assert fixed.metadata["dataset_id"] == dataset_id_before
    assert fixed.provenance == provenance_before
    assert "artifact_id" not in fixed.metadata

    assert len(trips.metadata["events"]) == len(prior_events_before)
    assert len(fixed.metadata["events"]) == len(prior_events_before) + 1
    assert fixed.metadata["events"][:-1] == prior_events_before

    event = _assert_report_event_alignment(fixed, report)
    assert "context" in event

    assert report.summary["n_rows"] == len(fixed.data)
    assert report.summary["noop"] is False
    assert report.parameters["strict"] is False
    assert report.parameters["field_corrections"] is None
    assert report.parameters["value_corrections"] == value_corrections
    assert fixed.metadata["is_validated"] is False


def test_fix_trips_correspondence_integration_noop_preserves_validated_state_and_leaves_evidence(
    tripdataset_canonical_small: TripDataset,
) -> None:
    """Verifica que sin cambios efectivos se preserve is_validated y se registre evidencia."""
    trips = tripdataset_canonical_small
    input_data_before = trips.data.copy(deep=True)
    validated_before = trips.metadata["is_validated"]

    fixed, report = fix_trips_correspondence(
        trips,
        field_corrections=None,
        value_corrections=None,
        options=FixCorrespondenceOptions(),
    )

    pd.testing.assert_frame_equal(trips.data, input_data_before)
    pd.testing.assert_frame_equal(fixed.data, input_data_before)

    assert report.ok is True
    assert report.summary["noop"] is True
    _assert_issue_present(report, "FIX.NO_EFFECTIVE_CHANGES.NO_CORRECTIONS")
    assert fixed.metadata["is_validated"] == validated_before
    assert len(fixed.metadata["events"]) == len(trips.metadata["events"]) + 1

    _assert_report_event_alignment(fixed, report)


def test_fix_trips_correspondence_integration_strict_raises_on_operation_error(
    make_tripdataset_fixture,
) -> None:
    """Verifica que strict=True escale un error operacional recuperable a FixError."""
    trips_with_collision = make_tripdataset_fixture(
        pd.DataFrame(
            {
                "movement_id": ["m1", "m2"],
                "user_id": ["u1", "u2"],
                "mode": ["bus", "walk"],
                "modo_raw": ["BUS", "WALK"],
                "purpose": ["work", "study"],
                "trip_weight": [1.0, 2.0],
            }
        ),
        dataset_id="tripdataset_strict_collision",
        is_validated=True,
    )

    data_before = trips_with_collision.data.copy(deep=True)
    metadata_before = deepcopy(trips_with_collision.metadata)

    with pytest.raises(FixError) as exc_info:
        fix_trips_correspondence(
            trips_with_collision,
            field_corrections={"modo_raw": "mode"},
            value_corrections=None,
            options=FixCorrespondenceOptions(strict=True),
            correspondence_context={"reason": "test strict"},
        )

    assert exc_info.value.code == "FIX.FIELD.TARGET_ALREADY_EXISTS"
    assert exc_info.value.issues is not None
    _assert_issue_present(list(exc_info.value.issues), "FIX.FIELD.TARGET_ALREADY_EXISTS")

    pd.testing.assert_frame_equal(trips_with_collision.data, data_before)
    assert trips_with_collision.metadata == metadata_before