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
    """Construye un FieldSpec mínimo equivalente al usado en el notebook helper-level."""
    return FieldSpec(
        name=name,
        dtype=dtype,
        required=required,
        constraints=constraints,
        domain=domain,
    )


@pytest.fixture()
def base_fix_schema() -> TripSchema:
    """Schema mínimo usado por los smoke tests públicos de OP-03."""
    fields = {
        "movement_id": make_field("movement_id", "string", required=True),
        "user_id": make_field("user_id", "string", required=True),
        "mode": make_field(
            "mode",
            "categorical",
            domain=DomainSpec(values=["walk", "bus", "metro", "car", "unknown"], extendable=True),
        ),
        "purpose": make_field(
            "purpose",
            "categorical",
            domain=DomainSpec(values=["work", "study", "shopping", "health", "unknown"], extendable=True),
        ),
        "trip_weight": make_field("trip_weight", "float"),
    }
    return TripSchema(
        version="1.1",
        fields=fields,
        required=[name for name, field in fields.items() if field.required],
        semantic_rules=None,
    )


def _build_base_domains_effective(schema: TripSchema, fields_present: list[str]) -> dict[str, dict[str, Any]]:
    """Construye dominios efectivos base para columnas categóricas presentes."""
    out: dict[str, dict[str, Any]] = {}
    for field_name in fields_present:
        field_spec = schema.fields.get(field_name)
        if field_spec is None or field_spec.domain is None:
            continue
        out[field_name] = {
            "values": sorted(list(field_spec.domain.values)),
            "extended": False,
            "added_values": [],
            "unknown_value": "unknown" if "unknown" in field_spec.domain.values else None,
            "strict_applied": False,
        }
    return out


@pytest.fixture()
def make_tripdataset_for_fix(base_fix_schema: TripSchema):
    """Factory mínima de TripDataset para smoke tests de fix_trips_correspondence."""

    def _make(
        df: pd.DataFrame,
        *,
        schema: TripSchema | None = None,
        is_validated: bool = True,
        dataset_id: str = "ds-fix-smoke-001",
        field_correspondence: dict[str, str] | None = None,
        value_correspondence: dict[str, dict[Any, Any]] | None = None,
    ) -> TripDataset:
        schema_eff = schema or base_fix_schema

        if field_correspondence is None:
            field_correspondence = {column: column for column in df.columns if column in schema_eff.fields}
        if value_correspondence is None:
            value_correspondence = {}

        base_domains_effective = _build_base_domains_effective(
            schema=schema_eff,
            fields_present=[column for column in df.columns if column in schema_eff.fields],
        )
        metadata = {
            "dataset_id": dataset_id,
            "events": [],
            "is_validated": is_validated,
            "mappings": {
                "field_correspondence": deepcopy(field_correspondence),
                "value_correspondence": deepcopy(value_correspondence),
            },
            "domains_effective": deepcopy(base_domains_effective),
        }
        schema_effective = TripSchemaEffective(
            dtype_effective={},
            overrides={},
            domains_effective=deepcopy(base_domains_effective),
            temporal={},
            fields_effective=list(df.columns),
        )
        provenance = {
            "source": {
                "name": "synthetic",
                "entity": "trips",
                "version": "smoke-tests-op03-v1",
            },
            "notes": ["dataset sintético mínimo para smoke tests de OP-03"],
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


def _issue_codes(issues: list[Issue]) -> list[str]:
    """Retorna los códigos de issues en el orden emitido por OP-03."""
    return [issue.code for issue in issues]


def _assert_report_and_event_are_aligned(fixed: TripDataset, report: OperationReport) -> None:
    """Verifica alineación mínima entre reporte y último evento registrado."""
    assert fixed.metadata["events"]
    event = fixed.metadata["events"][-1]
    assert event["op"] == "fix_trips_correspondence"
    assert event["summary"] == report.summary
    assert event["parameters"] == report.parameters
    assert "issues_summary" in event


def test_fix_trips_correspondence_smoke_happy_path_fields_then_values(make_tripdataset_for_fix) -> None:
    """Verifica que la función pública aplique corrección de campos y valores en un caso mínimo."""
    df_source = pd.DataFrame(
        {
            "movement_id": ["m1", "m2"],
            "user_id": ["u1", "u2"],
            "modo_raw": ["BUS", "WALK"],
            "proposito_raw": ["WORK", "STUDY"],
            "trip_weight": [1.0, 2.0],
        }
    )
    field_corrections = {
        "modo_raw": "mode",
        "proposito_raw": "purpose",
    }
    value_corrections = {
        "mode": {"BUS": "bus", "WALK": "walk"},
        "purpose": {"WORK": "work", "STUDY": "study"},
    }
    trips = make_tripdataset_for_fix(df_source, is_validated=True)
    data_before = trips.data.copy(deep=True)
    metadata_before = deepcopy(trips.metadata)

    fixed, report = fix_trips_correspondence(
        trips,
        field_corrections=field_corrections,
        value_corrections=value_corrections,
        options=FixCorrespondenceOptions(),
        correspondence_context={
            "reason": "ajuste smoke test",
            "notes": "happy path mínimo integrado",
        },
    )

    expected = data_before.rename(columns=field_corrections)
    for field, mapping in value_corrections.items():
        expected[field] = expected[field].replace(mapping)

    data_after_field_fix = data_before.rename(columns=field_corrections)
    expected_replacements = sum(
        int(data_after_field_fix[field].isin(mapping.keys()).sum())
        for field, mapping in value_corrections.items()
    )

    assert isinstance(fixed, TripDataset)
    assert isinstance(report, OperationReport)
    assert report.ok is True
    pd.testing.assert_frame_equal(trips.data, data_before)
    assert trips.metadata == metadata_before
    pd.testing.assert_frame_equal(fixed.data, expected)

    assert report.summary["n_rows"] == len(data_before)
    assert report.summary["n_field_corrections_requested"] == len(field_corrections)
    assert report.summary["n_field_corrections_applied"] == len(field_corrections)
    assert report.summary["n_value_corrections_fields_requested"] == len(value_corrections)
    assert report.summary["n_value_corrections_fields_applied"] == len(value_corrections)
    assert report.summary["n_value_replacements_applied"] == expected_replacements
    assert report.summary["noop"] is False
    assert set(report.summary["domains_effective_updated_fields"]) == set(value_corrections.keys())
    assert fixed.metadata["is_validated"] is False
    _assert_report_and_event_are_aligned(fixed, report)
    assert fixed.metadata["events"][-1]["context"]["reason"] == "ajuste smoke test"


def test_fix_trips_correspondence_smoke_noop_preserves_validated_state(make_tripdataset_for_fix) -> None:
    """Verifica que una llamada sin correcciones retorne NOOP, evento y estado validado preservado."""
    df_source = pd.DataFrame(
        {
            "movement_id": ["m1", "m2"],
            "user_id": ["u1", "u2"],
            "mode": ["bus", "walk"],
            "purpose": ["work", "study"],
        }
    )
    trips = make_tripdataset_for_fix(df_source, is_validated=True)
    data_before = trips.data.copy(deep=True)
    metadata_before = deepcopy(trips.metadata)

    fixed, report = fix_trips_correspondence(
        trips,
        field_corrections=None,
        value_corrections=None,
        options=FixCorrespondenceOptions(),
    )

    assert isinstance(fixed, TripDataset)
    assert isinstance(report, OperationReport)
    assert report.ok is True
    pd.testing.assert_frame_equal(trips.data, data_before)
    assert trips.metadata == metadata_before
    pd.testing.assert_frame_equal(fixed.data, data_before)
    assert "FIX.NO_EFFECTIVE_CHANGES.NO_CORRECTIONS" in _issue_codes(report.issues)
    assert report.summary["noop"] is True
    assert fixed.metadata["is_validated"] is True
    _assert_report_and_event_are_aligned(fixed, report)


def test_fix_trips_correspondence_smoke_partial_apply_with_context_warnings(make_tripdataset_for_fix) -> None:
    """Verifica que OP-03 degrade reglas recuperables y aplique los cambios válidos."""
    df_source = pd.DataFrame(
        {
            "movement_id": ["m1", "m2"],
            "user_id": ["u1", "u2"],
            "purpose_raw": ["WORK", "WORK"],
            "trip_weight": [1.0, 2.0],
        }
    )
    field_corrections = {
        "purpose_raw": "purpose",
        "missing_mode": "mode",
    }
    value_corrections = {
        "purpose": {"WORK": "work"},
    }
    trips = make_tripdataset_for_fix(df_source, is_validated=True)
    data_before = trips.data.copy(deep=True)

    fixed, report = fix_trips_correspondence(
        trips,
        field_corrections=field_corrections,
        value_corrections=value_corrections,
        options=FixCorrespondenceOptions(strict=False),
        correspondence_context={
            "reason": "ajuste parcial smoke",
            "unknown_key": "debe descartarse",
            "notes": {"ok": "texto", "bad": object()},
        },
    )

    applicable_field_corrections = {"purpose_raw": field_corrections["purpose_raw"]}
    expected = data_before.rename(columns=applicable_field_corrections)
    expected["purpose"] = expected["purpose"].replace(value_corrections["purpose"])
    expected_replacements = int(
        data_before["purpose_raw"].isin(value_corrections["purpose"].keys()).sum()
    )

    assert report.ok is True
    pd.testing.assert_frame_equal(trips.data, data_before)
    pd.testing.assert_frame_equal(fixed.data, expected)
    assert fixed.metadata["is_validated"] is False

    codes = _issue_codes(report.issues)
    assert "FIX.FIELD.SOURCE_COLUMN_MISSING" in codes
    assert "FIX.FIELD.PARTIAL_APPLY" in codes
    assert "FIX.CONTEXT.UNKNOWN_KEYS_DROPPED" in codes
    assert "FIX.CONTEXT.NON_SERIALIZABLE_DROPPED" in codes
    assert report.summary["n_field_corrections_requested"] == len(field_corrections)
    assert report.summary["n_field_corrections_applied"] == len(applicable_field_corrections)
    assert report.summary["n_value_corrections_fields_requested"] == len(value_corrections)
    assert report.summary["n_value_corrections_fields_applied"] == len(value_corrections)
    assert report.summary["n_value_replacements_applied"] == expected_replacements
    assert report.summary["noop"] is False

    _assert_report_and_event_are_aligned(fixed, report)
    event_context = fixed.metadata["events"][-1]["context"]
    assert "unknown_key" not in event_context
    assert event_context["notes"]["ok"] == "texto"
    assert "bad" not in event_context["notes"]


def test_fix_trips_correspondence_smoke_fatal_precondition_has_no_side_effects(make_tripdataset_for_fix) -> None:
    """Verifica que una precondición fatal aborte sin mutar data ni metadata del input."""
    df_source = pd.DataFrame(
        {
            "movement_id": ["m1"],
            "user_id": ["u1"],
            "mode": ["bus"],
        }
    )
    trips = make_tripdataset_for_fix(df_source, is_validated=True)
    data_before = trips.data.copy(deep=True)
    metadata_before = deepcopy(trips.metadata)

    with pytest.raises(FixError) as exc_info:
        fix_trips_correspondence(
            trips,
            field_corrections={"mode": "purpose"},
            value_corrections=None,
            options=FixCorrespondenceOptions(),
            correspondence_context=["not", "a", "dict"],
        )

    assert exc_info.value.code == "FIX.CONTEXT.INVALID_ROOT"
    pd.testing.assert_frame_equal(trips.data, data_before)
    assert trips.metadata == metadata_before
    assert trips.metadata["events"] == []


def test_fix_trips_correspondence_smoke_truncates_issues_and_aligns_event(make_tripdataset_for_fix) -> None:
    """Verifica que max_issues se refleje en el reporte y en el evento de salida."""
    df_source = pd.DataFrame(
        {
            "movement_id": ["m1", "m2"],
            "user_id": ["u1", "u2"],
            "mode": ["bus", "walk"],
            "purpose": ["work", "study"],
        }
    )
    field_corrections = {
        "missing_1": "mode",
        "missing_2": "purpose",
        "missing_3": "trip_weight",
    }
    max_issues = 2
    trips = make_tripdataset_for_fix(df_source, is_validated=True)

    fixed, report = fix_trips_correspondence(
        trips,
        field_corrections=field_corrections,
        value_corrections=None,
        options=FixCorrespondenceOptions(
            strict=False,
            max_issues=max_issues,
            sample_rows_per_issue=5,
        ),
    )

    assert isinstance(fixed, TripDataset)
    assert isinstance(report, OperationReport)
    assert "FIX.CORE.ISSUES_TRUNCATED" in _issue_codes(report.issues)
    assert report.summary["limits"]["issues_truncated"] is True
    assert report.summary["limits"]["n_issues_emitted"] <= max_issues
    assert len(report.issues) <= max_issues
    _assert_report_and_event_are_aligned(fixed, report)