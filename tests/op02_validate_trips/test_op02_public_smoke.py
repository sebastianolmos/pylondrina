from copy import deepcopy

import pandas as pd
import pytest

from pylondrina.datasets import TripDataset
from pylondrina.errors import ValidationError
from pylondrina.reports import ValidationReport
from pylondrina.schema import DomainSpec, FieldSpec, TripSchema, TripSchemaEffective
from pylondrina.validation import ValidationOptions, validate_trips


# ---------------------------------------------------------------------
# Helpers locales de test
# ---------------------------------------------------------------------


def make_field(
    name: str,
    dtype: str,
    *,
    required: bool = False,
    constraints: dict | None = None,
    domain: DomainSpec | None = None,
) -> FieldSpec:
    return FieldSpec(
        name=name,
        dtype=dtype,
        required=required,
        constraints=constraints,
        domain=domain,
    )


def issue_codes(issues) -> list[str]:
    return [issue.code for issue in issues]


def assert_issue_present(issues, code: str) -> None:
    codes = issue_codes(issues)
    assert code in codes, f"No se encontró issue {code!r}. Issues encontrados: {codes!r}"


def assert_issue_absent(issues, code: str) -> None:
    codes = issue_codes(issues)
    assert code not in codes, f"No se esperaba issue {code!r}. Issues encontrados: {codes!r}"


def issues_with_code(issues, code: str):
    return [issue for issue in issues if issue.code == code]


def assert_last_event_matches_report(trips: TripDataset, report: ValidationReport) -> None:
    event = trips.metadata["events"][-1]

    assert event["op"] == "validate_trips"
    assert event["summary"] == report.summary
    assert "parameters" in event
    assert "issues_summary" in event
    assert isinstance(event["issues_summary"]["counts"], dict)
    assert isinstance(event["issues_summary"]["top_codes"], list)


def assert_not_mutated_after_validation(
    trips: TripDataset,
    data_before: pd.DataFrame,
) -> None:
    pd.testing.assert_frame_equal(
        trips.data,
        data_before,
        check_dtype=True,
        check_like=False,
    )


# ---------------------------------------------------------------------
# Fixtures locales del smoke test
# ---------------------------------------------------------------------


BASE_VALIDATE_SCHEMA = TripSchema(
    version="1.1",
    fields={
        "movement_id": make_field("movement_id", "string", required=True),
        "user_id": make_field("user_id", "string", required=True),
        "origin_latitude": make_field("origin_latitude", "float", required=True),
        "origin_longitude": make_field("origin_longitude", "float", required=True),
        "destination_latitude": make_field("destination_latitude", "float", required=True),
        "destination_longitude": make_field("destination_longitude", "float", required=True),
        "origin_time_utc": make_field("origin_time_utc", "datetime", required=False),
        "destination_time_utc": make_field("destination_time_utc", "datetime", required=False),
        "mode": make_field(
            "mode",
            "categorical",
            required=False,
            domain=DomainSpec(
                values=["walk", "bus", "metro", "unknown"],
                extendable=False,
            ),
        ),
        "purpose": make_field(
            "purpose",
            "categorical",
            required=False,
            domain=DomainSpec(
                values=["work", "study", "health", "unknown"],
                extendable=False,
            ),
        ),
    },
    required=[
        "movement_id",
        "user_id",
        "origin_latitude",
        "origin_longitude",
        "destination_latitude",
        "destination_longitude",
    ],
    semantic_rules=None,
)

BASE_SCHEMA_EFFECTIVE = TripSchemaEffective(
    domains_effective={
        "mode": {
            "values": ["walk", "bus", "metro", "unknown"],
            "extendable": False,
            "extended": False,
            "added_values": [],
            "unknown_value": "unknown",
        },
        "purpose": {
            "values": ["work", "study", "health", "unknown"],
            "extendable": False,
            "extended": False,
            "added_values": [],
            "unknown_value": "unknown",
        },
    },
    temporal={"tier": "tier_1"},
)


def make_tripdataset_for_validate(
    df: pd.DataFrame,
    *,
    schema: TripSchema = BASE_VALIDATE_SCHEMA,
    schema_effective: TripSchemaEffective = BASE_SCHEMA_EFFECTIVE,
    metadata: dict | None = None,
) -> TripDataset:
    base_metadata = {
        "dataset_id": "ds-smoke-validate-001",
        "events": [],
        "is_validated": False,
        "domains_effective": deepcopy(getattr(schema_effective, "domains_effective", {})),
        "temporal": {
            "tier": "tier_1",
            "fields_present": ["origin_time_utc", "destination_time_utc"],
        },
    }

    if metadata:
        base_metadata.update(deepcopy(metadata))

    return TripDataset(
        data=df.copy(deep=True),
        schema=schema,
        schema_version=schema.version,
        provenance={
            "source": {
                "name": "synthetic",
                "entity": "trips",
                "version": "helper-smoke-validate-v1",
            },
            "notes": ["dataset sintético para smoke tests integrados de validate_trips"],
        },
        field_correspondence={},
        value_correspondence={},
        metadata=base_metadata,
        schema_effective=schema_effective,
    )


def make_valid_smoke_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "movement_id": ["m1", "m2"],
            "user_id": ["u1", "u2"],
            "origin_latitude": [-33.45, -33.46],
            "origin_longitude": [-70.66, -70.67],
            "destination_latitude": [-33.43, -33.44],
            "destination_longitude": [-70.61, -70.62],
            "origin_time_utc": ["2026-04-01T08:00:00Z", "2026-04-01T09:00:00Z"],
            "destination_time_utc": ["2026-04-01T08:30:00Z", "2026-04-01T09:20:00Z"],
            "mode": ["walk", "bus"],
            "purpose": ["work", "study"],
        }
    )


# ---------------------------------------------------------------------
# Smoke tests de validate_trips
# ---------------------------------------------------------------------


def test_validate_trips_public_smoke_happy_path_minimal():
    """Verifica happy path mínimo: report ok, evento, is_validated=True y no mutación de data."""
    df = make_valid_smoke_dataframe()
    trips = make_tripdataset_for_validate(df)
    data_before = trips.data.copy(deep=True)

    report = validate_trips(
        trips,
        options=ValidationOptions(),
    )

    assert isinstance(report, ValidationReport)
    assert report.ok is True
    assert report.summary["ok"] is True
    assert report.issues == []

    assert trips.metadata["is_validated"] is True
    assert len(trips.metadata["events"]) == 1

    assert report.summary["n_rows"] == len(df)
    assert report.summary["n_issues"] == 0
    assert report.summary["n_errors"] == 0
    assert report.summary["n_warnings"] == 0
    assert report.summary["n_info"] == 0

    assert report.summary["checks_executed"] == {
        "required_fields": True,
        "types_and_formats": True,
        "constraints": True,
        "domains": False,
        "temporal_consistency": False,
        "duplicates": False,
    }

    assert_last_event_matches_report(trips, report)
    assert_not_mutated_after_validation(trips, data_before)


def test_validate_trips_public_smoke_partial_od_spatial_rule():
    """Verifica que OD parcial permita origen o destino completo, pero falle si faltan ambos extremos."""
    df = pd.DataFrame(
        {
            "movement_id": ["m1", "m2", "m3"],
            "user_id": ["u1", "u2", "u3"],
            "origin_latitude": [-33.45, None, None],
            "origin_longitude": [-70.66, None, None],
            "destination_latitude": [None, -33.44, None],
            "destination_longitude": [None, -70.62, None],
            "origin_time_utc": ["2026-04-01T08:00:00Z"] * 3,
            "destination_time_utc": ["2026-04-01T08:30:00Z"] * 3,
            "mode": ["walk", "bus", "metro"],
            "purpose": ["work", "study", "health"],
        }
    )

    trips = make_tripdataset_for_validate(df)
    data_before = trips.data.copy(deep=True)

    report = validate_trips(
        trips,
        options=ValidationOptions(
            validate_constraints=True,
            allow_partial_od_spatial=True,
        ),
    )

    assert isinstance(report, ValidationReport)
    assert report.ok is False
    assert report.summary["ok"] is False
    assert trips.metadata["is_validated"] is False
    assert len(trips.metadata["events"]) == 1

    assert_issue_present(report.issues, "VAL.CORE.OD_SPATIAL_BOTH_MISSING")
    assert_issue_absent(report.issues, "VAL.CORE.NULLABILITY_VIOLATION")

    od_issues = issues_with_code(report.issues, "VAL.CORE.OD_SPATIAL_BOTH_MISSING")
    assert len(od_issues) == 1

    od_issue = od_issues[0]
    assert od_issue.level == "error"
    assert od_issue.row_count == 1
    assert od_issue.details["check"] == "constraints"
    assert od_issue.details["n_rows_total"] == len(df)
    assert od_issue.details["n_violations"] == 1
    assert od_issue.details["allow_partial_od_spatial"] is True
    assert od_issue.details["action"] == "report_error"

    assert_last_event_matches_report(trips, report)
    assert_not_mutated_after_validation(trips, data_before)


def test_validate_trips_public_smoke_malformed_known_constraint_is_warning_and_skipped():
    """Verifica que constraint conocida con params mal formados emita warning, se omita y no invalide el dataset."""
    schema = TripSchema(
        version="1.1",
        fields={
            **BASE_VALIDATE_SCHEMA.fields,
            "score": make_field(
                "score",
                "float",
                required=False,
                constraints={"range": {"minimum": 0}},
            ),
        },
        required=list(BASE_VALIDATE_SCHEMA.required),
        semantic_rules=None,
    )

    df = make_valid_smoke_dataframe().head(1).copy(deep=True)
    df["score"] = [12.5]

    trips = make_tripdataset_for_validate(df, schema=schema)
    data_before = trips.data.copy(deep=True)

    report = validate_trips(
        trips,
        options=ValidationOptions(validate_constraints=True),
    )

    assert isinstance(report, ValidationReport)
    assert report.ok is True
    assert report.summary["ok"] is True
    assert trips.metadata["is_validated"] is True
    assert len(trips.metadata["events"]) == 1

    assert_issue_present(report.issues, "VAL.SCHEMA.CONSTRAINT_PARAMS_INVALID")

    issue = issues_with_code(report.issues, "VAL.SCHEMA.CONSTRAINT_PARAMS_INVALID")[0]
    assert issue.level == "warning"
    assert issue.field == "score"
    assert issue.details["check"] == "constraints"
    assert issue.details["field"] == "score"
    assert issue.details["constraint"] == "range"
    assert issue.details["expected_params"] == ["min", "max"]
    assert issue.details["received_params"] == ["minimum"]
    assert issue.details["reason"] == "invalid_or_incomplete_params"
    assert issue.details["action"] == "skip_constraint"

    assert_last_event_matches_report(trips, report)
    assert_not_mutated_after_validation(trips, data_before)


def test_validate_trips_public_smoke_strict_true_records_event_before_raising_on_data_error():
    """Verifica que strict=True registre evidencia y evento antes de lanzar por error de datos."""
    df = make_valid_smoke_dataframe().head(1).copy(deep=True)
    df["origin_time_utc"] = ["2026-04-01T09:00:00Z"]
    df["destination_time_utc"] = ["2026-04-01T08:30:00Z"]

    trips = make_tripdataset_for_validate(df)
    data_before = trips.data.copy(deep=True)

    with pytest.raises(ValidationError) as exc_info:
        validate_trips(
            trips,
            options=ValidationOptions(
                strict=True,
                validate_temporal_consistency=True,
            ),
        )

    exc = exc_info.value

    assert exc.code == "VAL.TEMPORAL.ORIGIN_AFTER_DESTINATION"
    assert exc.issue is not None
    assert exc.issue.code == "VAL.TEMPORAL.ORIGIN_AFTER_DESTINATION"
    assert exc.issue.level == "error"
    assert exc.issues is not None
    assert_issue_present(exc.issues, "VAL.TEMPORAL.ORIGIN_AFTER_DESTINATION")

    assert trips.metadata["is_validated"] is False
    assert len(trips.metadata["events"]) == 1

    event = trips.metadata["events"][-1]
    assert event["op"] == "validate_trips"
    assert event["summary"]["ok"] is False
    assert event["summary"]["n_errors"] >= 1
    assert "issues_summary" in event
    assert event["issues_summary"]["counts"]["error"] >= 1
    assert event["issues_summary"]["top_codes"][0]["code"] == "VAL.TEMPORAL.ORIGIN_AFTER_DESTINATION"

    assert_not_mutated_after_validation(trips, data_before)


def test_validate_trips_public_smoke_fatal_config_error_does_not_register_event():
    """Verifica que un error fatal de configuración aborte antes del pipeline normal y no registre evento."""
    df = make_valid_smoke_dataframe().head(1).copy(deep=True)
    trips = make_tripdataset_for_validate(df)

    events_before = deepcopy(trips.metadata["events"])
    validated_before = trips.metadata["is_validated"]
    data_before = trips.data.copy(deep=True)

    with pytest.raises(ValidationError) as exc_info:
        validate_trips(
            trips,
            options=ValidationOptions(
                validate_duplicates=True,
                duplicates_subset=None,
            ),
        )

    exc = exc_info.value

    assert exc.code == "VAL.CONFIG.DUPLICATES_SUBSET_NOT_PROVIDED"
    assert exc.issue is not None
    assert exc.issue.code == "VAL.CONFIG.DUPLICATES_SUBSET_NOT_PROVIDED"
    assert exc.issue.level == "error"
    assert exc.details["validate_duplicates"] is True
    assert exc.details["duplicates_subset"] is None
    assert exc.details["action"] == "abort"

    assert trips.metadata["events"] == events_before
    assert trips.metadata["is_validated"] == validated_before
    assert_not_mutated_after_validation(trips, data_before)


def test_validate_trips_public_smoke_truncation_keeps_summary_and_event_consistent():
    """Verifica truncamiento por max_issues y consistencia entre report.summary y evento validate_trips."""
    df = pd.DataFrame(
        {
            "movement_id": [None, "m2", "m3", "m4"],
            "user_id": ["u1", "u2", "u3", "u4"],
            "origin_latitude": [-33.45, -33.45, -33.45, -33.45],
            "origin_longitude": [-70.66, -70.66, -70.66, -70.66],
            "destination_latitude": [-33.43, -33.43, -33.43, -33.43],
            "destination_longitude": [-70.61, -70.61, -70.61, -70.61],
            "origin_time_utc": [
                "2026-04-01T09:00:00Z",
                "2026-04-01T09:00:00Z",
                "2026-04-01T09:00:00Z",
                "2026-04-01T09:00:00Z",
            ],
            "destination_time_utc": [
                "2026-04-01T08:00:00Z",
                "2026-04-01T08:00:00Z",
                "2026-04-01T08:00:00Z",
                "2026-04-01T08:00:00Z",
            ],
            "mode": ["xxx", "yyy", "walk", "bus"],
            "purpose": ["zzz", "work", "study", "aaa"],
        }
    )

    trips = make_tripdataset_for_validate(df)
    data_before = trips.data.copy(deep=True)

    report = validate_trips(
        trips,
        options=ValidationOptions(
            max_issues=2,
            validate_temporal_consistency=True,
            validate_domains="full",
        ),
    )

    assert isinstance(report, ValidationReport)
    assert report.ok is False
    assert report.summary["ok"] is False

    assert len(report.issues) <= 2
    assert_issue_present(report.issues, "VAL.CORE.ISSUES_TRUNCATED")

    assert report.summary["limits"]["issues_truncated"] is True
    assert report.summary["limits"]["n_issues_emitted"] == len(report.issues)
    assert report.summary["limits"]["n_issues_emitted"] <= 2
    assert (
        report.summary["limits"]["n_issues_detected_total"]
        >= report.summary["limits"]["n_issues_emitted"]
    )

    assert trips.metadata["is_validated"] is False
    assert len(trips.metadata["events"]) == 1

    event = trips.metadata["events"][-1]
    assert event["op"] == "validate_trips"
    assert event["summary"] == report.summary
    assert "issues_summary" in event

    assert event["issues_summary"]["counts"]["warning"] >= 1

    top_codes = event["issues_summary"]["top_codes"]
    top_code_map = {item["code"]: item["count"] for item in top_codes}

    assert "VAL.CORE.ISSUES_TRUNCATED" in top_code_map
    assert top_code_map["VAL.CORE.ISSUES_TRUNCATED"] == 1

    assert_not_mutated_after_validation(trips, data_before)