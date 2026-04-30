import json

import pytest

from pylondrina.errors import ImportError as PylondrinaImportError
from pylondrina.importing import _build_import_metadata, _build_import_report
from pylondrina.reports import Issue
from pylondrina.schema import DomainSpec, FieldSpec, TripSchema, TripSchemaEffective


# ---------------------------------------------------------------------
# Helpers locales de test
# ---------------------------------------------------------------------


def assert_json_safe(obj, label: str = "object") -> None:
    try:
        json.dumps(obj, default=str)
    except Exception as exc:
        raise AssertionError(f"{label} no es JSON-safe: {exc}") from exc


def issue_codes(issues) -> list[str]:
    return [issue.code for issue in issues]


def assert_issue_present(issues, code: str) -> None:
    codes = issue_codes(issues)
    assert code in codes, f"No se encontró issue {code!r}. Issues encontrados: {codes!r}"


def assert_issue_absent(issues, code: str) -> None:
    codes = issue_codes(issues)
    assert code not in codes, f"No se esperaba issue {code!r}. Issues encontrados: {codes!r}"


# ---------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------


@pytest.fixture
def schema_g10() -> TripSchema:
    return TripSchema(
        version="0.1.0",
        fields={
            "movement_id": FieldSpec(name="movement_id", dtype="string", required=True),
            "trip_id": FieldSpec(name="trip_id", dtype="string", required=False),
            "movement_seq": FieldSpec(name="movement_seq", dtype="int", required=False),
            "purpose": FieldSpec(
                name="purpose",
                dtype="categorical",
                required=False,
                domain=DomainSpec(values=["work", "study"], extendable=True, aliases=None),
            ),
            "origin_time_utc": FieldSpec(name="origin_time_utc", dtype="datetime", required=False),
        },
        required=["movement_id"],
        semantic_rules=None,
    )


@pytest.fixture
def schema_effective_g10() -> TripSchemaEffective:
    return TripSchemaEffective(
        dtype_effective={
            "movement_id": "string",
            "trip_id": "string",
            "movement_seq": "int",
            "purpose": "categorical",
            "origin_time_utc": "datetime",
        },
        overrides={
            "purpose": {"reasons": ["domain_extended"], "added_values": ["home"]},
            "origin_time_utc": {
                "reasons": ["datetime_normalized"],
                "status": "string_tzaware_to_utc",
            },
        },
        domains_effective={
            "purpose": {
                "values": ["unknown", "work", "study", "home"],
                "extended": True,
                "added_values": ["home"],
                "unknown_value": "unknown",
                "unknown_values": [],
            },
        },
        temporal={"tier": "tier_1"},
        fields_effective=[
            "movement_id",
            "trip_id",
            "movement_seq",
            "purpose",
            "origin_time_utc",
        ],
    )


@pytest.fixture
def field_corr_g10() -> dict[str, str]:
    return {"purpose": "motivo"}


@pytest.fixture
def value_corr_g10() -> dict[str, dict[str, str]]:
    return {"purpose": {"trabajo": "work"}}


@pytest.fixture
def domains_extended_g10() -> list[str]:
    return ["purpose"]


@pytest.fixture
def extra_fields_kept_g10() -> list[str]:
    return ["raw_source_col"]


@pytest.fixture
def h3_meta_g10() -> dict:
    return {
        "resolution": 8,
        "source_fields": [["origin_latitude", "origin_longitude"]],
        "derived_fields": ["origin_h3_index"],
    }


@pytest.fixture
def provenance_g10() -> dict:
    return {
        "source_name": "eod_santiago_2012",
        "created_by_op": "import_trips",
        "created_at_utc": "2026-03-20T15:00:00+00:00",
    }


@pytest.fixture
def event_import_g10() -> dict:
    return {
        "op": "import_trips",
        "ts_utc": "2026-03-20T15:00:00+00:00",
        "parameters": {
            "keep_extra_fields": True,
            "selected_fields": ["purpose"],
            "strict": False,
            "strict_domains": False,
            "single_stage": True,
            "h3_resolution": 8,
            "source_name": "eod_santiago_2012",
        },
        "summary": {
            "input_rows": 10,
            "output_rows": 10,
            "rows_dropped": 0,
            "n_fields_mapped": 1,
            "n_domain_mappings_applied": 1,
            "columns_added": ["movement_id", "trip_id", "movement_seq"],
            "columns_deleted": [],
            "domains_extended_count": 1,
            "temporal_tier": "tier_1",
            "temporal_notes": "tier_1_datetime_normalized",
        },
        "issues_summary": {
            "counts": {"info": 1, "warning": 1},
            "by_code": {"DOM.EXTENSION.APPLIED": 1, "IMP.TEMPORAL.TIER_LIMITED": 1},
        },
    }


def make_base_issues_g10() -> list[Issue]:
    return [
        Issue(level="info", code="DOM.EXTENSION.APPLIED", message="domain extended"),
        Issue(level="warning", code="IMP.TEMPORAL.TIER_LIMITED", message="tier limited"),
    ]


# ---------------------------------------------------------------------
# Tests de _build_import_metadata
# ---------------------------------------------------------------------


def test_build_import_metadata_minimal_happy_path(
    schema_g10,
    schema_effective_g10,
    field_corr_g10,
    value_corr_g10,
    domains_extended_g10,
    extra_fields_kept_g10,
    h3_meta_g10,
    provenance_g10,
    event_import_g10,
):
    """Verifica que la metadata de import quede completa, serializable y con evento import_trips embebido."""
    issues = make_base_issues_g10()
    domains_effective = schema_effective_g10.domains_effective

    metadata = _build_import_metadata(
        schema=schema_g10,
        schema_effective=schema_effective_g10,
        field_correspondence_applied=field_corr_g10,
        value_correspondence_applied=value_corr_g10,
        domains_effective=domains_effective,
        domains_extended=domains_extended_g10,
        extra_fields_kept=extra_fields_kept_g10,
        h3_meta=h3_meta_g10,
        provenance=provenance_g10,
        temporal_tier_detected="tier_1",
        temporal_fields_present=["origin_time_utc", "destination_time_utc"],
        datetime_normalization_status_by_field={
            "origin_time_utc": {"status": "string_tzaware_to_utc", "n_nat": 0},
            "destination_time_utc": {"status": "string_tzaware_to_utc", "n_nat": 0},
        },
        datetime_normalization_stats_t2={},
        source_timezone_used="UTC",
        event_import=event_import_g10,
        issues=issues,
        strict=False,
    )

    assert "dataset_id" in metadata
    assert isinstance(metadata["dataset_id"], str)
    assert metadata["dataset_id"].startswith("tripds_")
    assert metadata["is_validated"] is False

    assert metadata["schema"]["version"] == "0.1.0"
    assert metadata["schema_effective"]["fields_effective"] == schema_effective_g10.fields_effective

    assert metadata["mappings"]["field_correspondence"] == field_corr_g10
    assert metadata["mappings"]["value_correspondence"] == value_corr_g10

    assert metadata["domains_effective"] == domains_effective
    assert metadata["domains_extended"] == domains_extended_g10
    assert metadata["extra_fields_kept"] == extra_fields_kept_g10

    assert metadata["events"] == [event_import_g10]
    assert metadata["events"][0]["op"] == "import_trips"
    assert metadata["provenance"] == provenance_g10

    assert metadata["temporal"]["tier"] == "tier_1"
    assert metadata["temporal"]["fields_present"] == ["origin_time_utc", "destination_time_utc"]
    assert "normalization" in metadata["temporal"]
    assert metadata["temporal"]["source_timezone_used"] == "UTC"

    assert metadata["h3"] == h3_meta_g10

    assert_issue_present(issues, "IMP.METADATA.DATASET_ID_CREATED")
    assert_json_safe(metadata, "metadata")


def test_build_import_metadata_tier2_records_hhmm_normalization_without_h3(
    schema_g10,
    schema_effective_g10,
    provenance_g10,
    event_import_g10,
):
    """Verifica que Tier 2 registre normalización HH:MM y omita h3/source_timezone cuando no aplican."""
    issues: list[Issue] = []

    metadata = _build_import_metadata(
        schema=schema_g10,
        schema_effective=schema_effective_g10,
        field_correspondence_applied={},
        value_correspondence_applied={},
        domains_effective={},
        domains_extended=[],
        extra_fields_kept=[],
        h3_meta=None,
        provenance=provenance_g10,
        temporal_tier_detected="tier_2",
        temporal_fields_present=["origin_time_local_hhmm", "destination_time_local_hhmm"],
        datetime_normalization_status_by_field={},
        datetime_normalization_stats_t2={
            "origin_time_local_hhmm": {"n_total": 5, "n_invalid": 1, "n_na": 2},
            "destination_time_local_hhmm": {"n_total": 5, "n_invalid": 2, "n_na": 1},
        },
        source_timezone_used=None,
        event_import=event_import_g10,
        issues=issues,
        strict=False,
    )

    assert metadata["temporal"]["tier"] == "tier_2"
    assert metadata["temporal"]["fields_present"] == [
        "origin_time_local_hhmm",
        "destination_time_local_hhmm",
    ]
    assert metadata["temporal"]["normalization"] == {
        "origin_time_local_hhmm": {"n_total": 5, "n_invalid": 1, "n_na": 2},
        "destination_time_local_hhmm": {"n_total": 5, "n_invalid": 2, "n_na": 1},
    }
    assert "source_timezone_used" not in metadata["temporal"]
    assert "h3" not in metadata
    assert metadata["is_validated"] is False
    assert_issue_present(issues, "IMP.METADATA.DATASET_ID_CREATED")
    assert_json_safe(metadata, "metadata_tier2")


@pytest.mark.parametrize("strict", [False, True])
def test_build_import_metadata_nonserializable_provenance_always_raises(
    strict,
    schema_g10,
    schema_effective_g10,
    event_import_g10,
):
    """Verifica que provenance no serializable aborte y reporte PRV.INPUT.NOT_JSON_SERIALIZABLE."""
    issues: list[Issue] = []

    with pytest.raises(PylondrinaImportError) as exc_info:
        _build_import_metadata(
            schema=schema_g10,
            schema_effective=schema_effective_g10,
            field_correspondence_applied={},
            value_correspondence_applied={},
            domains_effective={},
            domains_extended=[],
            extra_fields_kept=[],
            h3_meta=None,
            provenance={"bad": {1, 2, 3}},
            temporal_tier_detected="tier_3",
            temporal_fields_present=[],
            datetime_normalization_status_by_field={},
            datetime_normalization_stats_t2={},
            source_timezone_used=None,
            event_import=event_import_g10,
            issues=issues,
            strict=strict,
        )

    assert_issue_present(exc_info.value.issues, "PRV.INPUT.NOT_JSON_SERIALIZABLE")

    issue = exc_info.value.issue
    assert issue.code == "PRV.INPUT.NOT_JSON_SERIALIZABLE"
    assert issue.details["reason"] == "not_json_serializable"
    assert issue.details["action"] == "abort"


# ---------------------------------------------------------------------
# Tests de _build_import_report
# ---------------------------------------------------------------------


def test_build_import_report_happy_path_without_errors(
    field_corr_g10,
    value_corr_g10,
):
    """Verifica que ImportReport quede ok=True, con summary mínimo y metadata operacional coherente."""
    metadata_for_report = {
        "dataset_id": "tripds_dummy_001",
        "is_validated": False,
        "temporal": {"tier": "tier_1"},
    }
    issues = [
        Issue(level="info", code="DOM.EXTENSION.APPLIED", message="domain extended"),
        Issue(level="warning", code="IMP.TEMPORAL.TIER_LIMITED", message="tier limited"),
    ]
    parameters_effective = {
        "keep_extra_fields": True,
        "selected_fields": ["purpose"],
        "strict": False,
        "strict_domains": False,
        "single_stage": True,
    }

    report = _build_import_report(
        issues=issues,
        field_correspondence_applied=field_corr_g10,
        value_correspondence_applied=value_corr_g10,
        schema_version="0.1.0",
        source_name="eod_santiago_2012",
        dataset_id="tripds_dummy_001",
        parameters_effective=parameters_effective,
        rows_in=10,
        rows_out=10,
        n_fields_mapped=1,
        n_domain_mappings_applied=1,
        metadata=metadata_for_report,
    )

    assert report.ok is True
    assert report.issues == issues
    assert report.summary == {
        "rows_in": 10,
        "rows_out": 10,
        "n_fields_mapped": 1,
        "n_domain_mappings_applied": 1,
    }
    assert report.parameters == parameters_effective
    assert report.field_correspondence == field_corr_g10
    assert report.value_correspondence == value_corr_g10
    assert report.schema_version == "0.1.0"

    assert report.metadata["schema_version"] == "0.1.0"
    assert report.metadata["dataset_id"] == "tripds_dummy_001"
    assert report.metadata["source_name"] == "eod_santiago_2012"
    assert report.metadata["parameters_effective"] == parameters_effective
    assert report.metadata["summary"] == report.summary
    assert report.metadata["metadata"] == metadata_for_report

    assert_json_safe(report.metadata, "report.metadata")


def test_build_import_report_ok_false_when_error_issue_exists():
    """Verifica que ImportReport quede ok=False cuando existe al menos un issue de nivel error."""
    issues = [
        Issue(level="warning", code="IMP.TEMPORAL.TIER_LIMITED", message="tier limited"),
        Issue(level="error", code="IMP.INPUT.MISSING_REQUIRED_FIELD", message="missing required"),
    ]

    report = _build_import_report(
        issues=issues,
        field_correspondence_applied={},
        value_correspondence_applied={},
        schema_version="0.1.0",
        source_name="eod_santiago_2012",
        dataset_id="tripds_dummy_002",
        parameters_effective={"strict": False},
        rows_in=5,
        rows_out=4,
        n_fields_mapped=0,
        n_domain_mappings_applied=0,
        metadata={"dataset_id": "tripds_dummy_002", "is_validated": False},
    )

    assert report.ok is False
    assert report.issues == issues
    assert len(report.issues) == 2
    assert report.summary["rows_in"] == 5
    assert report.summary["rows_out"] == 4
    assert report.metadata["dataset_id"] == "tripds_dummy_002"


# ---------------------------------------------------------------------
# Test integrado pequeño
# ---------------------------------------------------------------------


def test_metadata_and_report_integrated_small_case(
    schema_g10,
    schema_effective_g10,
    field_corr_g10,
    value_corr_g10,
    domains_extended_g10,
    provenance_g10,
    event_import_g10,
):
    """Verifica el encadenamiento metadata + ImportReport preservando dataset_id, evento y tier temporal."""
    issues = [Issue(level="warning", code="IMP.TEMPORAL.TIER_LIMITED", message="tier limited")]

    metadata = _build_import_metadata(
        schema=schema_g10,
        schema_effective=schema_effective_g10,
        field_correspondence_applied=field_corr_g10,
        value_correspondence_applied=value_corr_g10,
        domains_effective=schema_effective_g10.domains_effective,
        domains_extended=domains_extended_g10,
        extra_fields_kept=[],
        h3_meta=None,
        provenance=provenance_g10,
        temporal_tier_detected="tier_3",
        temporal_fields_present=[],
        datetime_normalization_status_by_field={},
        datetime_normalization_stats_t2={},
        source_timezone_used=None,
        event_import=event_import_g10,
        issues=issues,
        strict=False,
    )

    dataset_id = metadata["dataset_id"]

    report = _build_import_report(
        issues=issues,
        field_correspondence_applied=field_corr_g10,
        value_correspondence_applied=value_corr_g10,
        schema_version=schema_g10.version,
        source_name=provenance_g10["source_name"],
        dataset_id=dataset_id,
        parameters_effective=event_import_g10["parameters"],
        rows_in=10,
        rows_out=10,
        n_fields_mapped=1,
        n_domain_mappings_applied=1,
        metadata=metadata,
    )

    assert report.ok is True
    assert report.metadata["dataset_id"] == dataset_id
    assert report.metadata["metadata"]["dataset_id"] == dataset_id
    assert report.metadata["metadata"]["is_validated"] is False
    assert report.metadata["metadata"]["events"][0]["op"] == "import_trips"
    assert report.metadata["metadata"]["temporal"]["tier"] == "tier_3"
    assert_issue_present(report.issues, "IMP.METADATA.DATASET_ID_CREATED")
    assert_json_safe(report.metadata, "report_integrated.metadata")