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


def issues_with_code(issues, code: str):
    return [issue for issue in issues if issue.code == code]


def assert_issue_present(issues, code: str) -> None:
    codes = issue_codes(issues)
    assert code in codes, f"No se encontró issue {code!r}. Issues encontrados: {codes!r}"


def assert_counts_by_level(
    issues,
    *,
    errors: int | None = None,
    warnings: int | None = None,
    info: int | None = None,
) -> None:
    counts = {"error": 0, "warning": 0, "info": 0}
    for issue in issues:
        counts[issue.level] = counts.get(issue.level, 0) + 1

    if errors is not None:
        assert counts["error"] == errors
    if warnings is not None:
        assert counts["warning"] == warnings
    if info is not None:
        assert counts["info"] == info


def assert_last_event_matches_report(trips: TripDataset, report: ValidationReport) -> None:
    event = trips.metadata["events"][-1]

    assert event["op"] == "validate_trips"
    assert event["summary"] == report.summary
    assert "ts_utc" in event
    assert "parameters" in event
    assert "issues_summary" in event
    assert isinstance(event["issues_summary"]["counts"], dict)
    assert isinstance(event["issues_summary"]["top_codes"], list)


def assert_not_mutated_after_validation(trips: TripDataset, data_before: pd.DataFrame) -> None:
    pd.testing.assert_frame_equal(
        trips.data,
        data_before,
        check_dtype=True,
        check_like=False,
    )


def expected_temporal_violations(df: pd.DataFrame) -> int:
    origin_dt = pd.to_datetime(df["origin_time_utc"], errors="coerce", utc=False)
    destination_dt = pd.to_datetime(df["destination_time_utc"], errors="coerce", utc=False)
    comparable = origin_dt.notna() & destination_dt.notna()
    return int((comparable & (origin_dt > destination_dt)).sum())


def expected_out_of_domain_count(series: pd.Series, domain_values: set[str]) -> int:
    non_null = series[series.notna()].astype(str)
    return int((~non_null.isin(domain_values)).sum())


def top_code_map_from_event(event: dict) -> dict[str, int]:
    return {item["code"]: item["count"] for item in event["issues_summary"]["top_codes"]}


# ---------------------------------------------------------------------
# Fixtures locales de integración directa
# ---------------------------------------------------------------------


CANONICAL_MODE_VALUES = [
    "walk",
    "bicycle",
    "scooter",
    "motorcycle",
    "car",
    "taxi",
    "ride_hailing",
    "bus",
    "metro",
    "train",
    "other",
]

CANONICAL_PURPOSE_VALUES = [
    "home",
    "work",
    "education",
    "shopping",
    "errand",
    "health",
    "leisure",
    "transfer",
    "other",
]

SCHEMA_VALIDATE_DIRECT = TripSchema(
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
            domain=DomainSpec(values=CANONICAL_MODE_VALUES, extendable=False),
        ),
        "purpose": make_field(
            "purpose",
            "categorical",
            required=False,
            domain=DomainSpec(values=CANONICAL_PURPOSE_VALUES, extendable=True),
        ),
        "survey_wave": make_field(
            "survey_wave",
            "string",
            required=False,
            constraints={"nullable": False},
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

SCHEMA_EFFECTIVE_VALIDATE_DIRECT = TripSchemaEffective(
    domains_effective={
        "mode": {
            "values": CANONICAL_MODE_VALUES,
            "extendable": False,
            "extended": False,
            "added_values": [],
            "unknown_value": "unknown",
        },
        "purpose": {
            "values": CANONICAL_PURPOSE_VALUES,
            "extendable": True,
            "extended": False,
            "added_values": [],
            "unknown_value": "unknown",
        },
    },
    temporal={"tier": "tier_1"},
    fields_effective=list(SCHEMA_VALIDATE_DIRECT.fields.keys()),
)


def make_tripdataset_direct(
    df: pd.DataFrame,
    *,
    dataset_id: str,
    schema: TripSchema = SCHEMA_VALIDATE_DIRECT,
    schema_effective: TripSchemaEffective = SCHEMA_EFFECTIVE_VALIDATE_DIRECT,
    metadata: dict | None = None,
) -> TripDataset:
    base_metadata = {
        "dataset_id": dataset_id,
        "events": [],
        "is_validated": False,
        "domains_effective": deepcopy(schema_effective.domains_effective),
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
                "name": "synthetic_direct",
                "entity": "trips",
                "version": "validate_op02_integration",
            }
        },
        field_correspondence={},
        value_correspondence={},
        metadata=base_metadata,
        schema_effective=deepcopy(schema_effective),
    )


def clone_tripdataset(trips: TripDataset) -> TripDataset:
    return TripDataset(
        data=trips.data.copy(deep=True),
        schema=trips.schema,
        schema_version=trips.schema_version,
        provenance=deepcopy(trips.provenance),
        field_correspondence=deepcopy(trips.field_correspondence),
        value_correspondence=deepcopy(trips.value_correspondence),
        metadata=deepcopy(trips.metadata),
        schema_effective=deepcopy(trips.schema_effective),
    )


def make_tripdataset_unvalidated_small() -> TripDataset:
    return make_tripdataset_direct(
        pd.DataFrame(
            {
                "movement_id": ["m1", "m2", "m3"],
                "user_id": ["u1", "u2", "u3"],
                "origin_latitude": [-33.45, -33.46, -33.47],
                "origin_longitude": [-70.66, -70.67, -70.68],
                "destination_latitude": [-33.43, -33.44, -33.45],
                "destination_longitude": [-70.61, -70.62, -70.63],
                "origin_time_utc": [
                    "2026-04-02T08:00:00Z",
                    "2026-04-02T09:00:00Z",
                    "2026-04-02T10:00:00Z",
                ],
                "destination_time_utc": [
                    "2026-04-02T08:30:00Z",
                    "2026-04-02T09:20:00Z",
                    "2026-04-02T10:25:00Z",
                ],
                "mode": ["walk", "bus", "metro"],
                "purpose": ["work", "education", "health"],
                "survey_wave": ["w1", "w1", "w1"],
            }
        ),
        dataset_id="tds-unvalidated-small",
    )


def make_tripdataset_with_domain_issues() -> TripDataset:
    return make_tripdataset_direct(
        pd.DataFrame(
            {
                "movement_id": ["m1", "m2", "m3", "m4"],
                "user_id": ["u1", "u2", "u3", "u4"],
                "origin_latitude": [-33.45, -33.46, -33.47, -33.48],
                "origin_longitude": [-70.66, -70.67, -70.68, -70.69],
                "destination_latitude": [-33.43, -33.44, -33.45, -33.46],
                "destination_longitude": [-70.61, -70.62, -70.63, -70.64],
                "origin_time_utc": [
                    "2026-04-02T08:00:00Z",
                    "2026-04-02T09:00:00Z",
                    "2026-04-02T10:00:00Z",
                    "2026-04-02T11:00:00Z",
                ],
                "destination_time_utc": [
                    "2026-04-02T08:30:00Z",
                    "2026-04-02T09:30:00Z",
                    "2026-04-02T10:30:00Z",
                    "2026-04-02T11:30:00Z",
                ],
                "mode": ["walk", "teleport", "bus", "metro"],
                "purpose": ["work", "Otra actividad", "education", "health"],
                "survey_wave": ["w1", "w1", "w1", "w1"],
            }
        ),
        dataset_id="tds-domain-issues",
    )


def make_tripdataset_with_temporal_issues() -> TripDataset:
    return make_tripdataset_direct(
        pd.DataFrame(
            {
                "movement_id": ["m1", "m2"],
                "user_id": ["u1", "u2"],
                "origin_latitude": [-33.45, -33.46],
                "origin_longitude": [-70.66, -70.67],
                "destination_latitude": [-33.43, -33.44],
                "destination_longitude": [-70.61, -70.62],
                "origin_time_utc": [
                    "2026-04-02T09:00:00Z",
                    "2026-04-02T10:00:00Z",
                ],
                "destination_time_utc": [
                    "2026-04-02T08:30:00Z",
                    "2026-04-02T09:50:00Z",
                ],
                "mode": ["walk", "bus"],
                "purpose": ["work", "education"],
                "survey_wave": ["w1", "w1"],
            }
        ),
        dataset_id="tds-temporal-issues",
    )


# ---------------------------------------------------------------------
# Integration tests directos de validate_trips
# ---------------------------------------------------------------------


def test_validate_trips_integration_happy_path_direct_tripdataset():
    """Verifica el caso principal correcto sobre TripDataset en memoria, incluyendo evento, summary y no mutación."""
    trips = clone_tripdataset(make_tripdataset_unvalidated_small())
    data_before = trips.data.copy(deep=True)

    report = validate_trips(
        trips,
        options=ValidationOptions(
            validate_domains="full",
            validate_temporal_consistency=True,
            validate_duplicates=False,
            allow_partial_od_spatial=True,
        ),
    )

    assert isinstance(report, ValidationReport)
    assert report.ok is True
    assert report.summary["ok"] is True
    assert report.issues == []

    assert trips.metadata["is_validated"] is True
    assert len(trips.metadata["events"]) == 1

    assert report.summary["n_rows"] == len(trips.data)
    assert report.summary["n_errors"] == 0
    assert report.summary["n_warnings"] == 0
    assert report.summary["checks_executed"]["required_fields"] is True
    assert report.summary["checks_executed"]["constraints"] is True
    assert report.summary["checks_executed"]["types_and_formats"] is True
    assert report.summary["checks_executed"]["domains"] is True
    assert report.summary["checks_executed"]["temporal_consistency"] is True

    assert_last_event_matches_report(trips, report)
    assert_not_mutated_after_validation(trips, data_before)


def test_validate_trips_integration_required_column_missing_returns_report_without_strict():
    """Verifica que una columna requerida ausente genere report ok=False, evento e is_validated=False sin strict."""
    trips = clone_tripdataset(make_tripdataset_unvalidated_small())
    trips.data = trips.data.drop(columns=["user_id"])

    report = validate_trips(
        trips,
        options=ValidationOptions(
            strict=False,
            validate_required_fields=True,
            validate_constraints=False,
            validate_types_and_formats=False,
            validate_domains="off",
            validate_temporal_consistency=False,
            validate_duplicates=False,
        ),
    )

    assert isinstance(report, ValidationReport)
    assert report.ok is False
    assert report.summary["ok"] is False
    assert_issue_present(report.issues, "VAL.CORE.REQUIRED_COLUMNS_MISSING")
    assert_counts_by_level(report.issues, errors=1, warnings=0, info=0)

    issue = issues_with_code(report.issues, "VAL.CORE.REQUIRED_COLUMNS_MISSING")[0]
    assert issue.level == "error"
    assert issue.details["missing_required"] == ["user_id"]
    assert issue.details["action"] == "report_error"

    assert trips.metadata["is_validated"] is False
    assert len(trips.metadata["events"]) == 1
    assert_last_event_matches_report(trips, report)


def test_validate_trips_integration_effective_nullability_violation():
    """Verifica nullabilidad efectiva en la API pública usando survey_wave con nullable=False."""
    trips = clone_tripdataset(make_tripdataset_unvalidated_small())
    data_before = trips.data.copy(deep=True)
    trips.data.loc[trips.data.index[1], "survey_wave"] = None

    expected_nulls = int(trips.data["survey_wave"].isna().sum())

    report = validate_trips(
        trips,
        options=ValidationOptions(
            strict=False,
            validate_required_fields=True,
            validate_constraints=True,
            validate_types_and_formats=False,
            validate_domains="off",
            validate_temporal_consistency=False,
            validate_duplicates=False,
        ),
    )

    assert isinstance(report, ValidationReport)
    assert report.ok is False
    assert report.summary["ok"] is False
    assert_issue_present(report.issues, "VAL.CORE.NULLABILITY_VIOLATION")
    assert_counts_by_level(report.issues, errors=1, warnings=0, info=0)

    issue = issues_with_code(report.issues, "VAL.CORE.NULLABILITY_VIOLATION")[0]
    assert issue.field == "survey_wave"
    assert issue.row_count == expected_nulls
    assert issue.details["nullable_effective"] is False
    assert issue.details["action"] == "report_error"

    assert trips.metadata["is_validated"] is False
    assert len(trips.metadata["events"]) == 1
    assert_last_event_matches_report(trips, report)

    data_expected = data_before.copy(deep=True)
    data_expected.loc[data_expected.index[1], "survey_wave"] = None
    assert_not_mutated_after_validation(trips, data_expected)


def test_validate_trips_integration_domain_warnings_keep_dataset_valid():
    """Verifica caso degradado con warnings de dominio: ok=True, evento e is_validated=True."""
    trips = clone_tripdataset(make_tripdataset_with_domain_issues())
    data_before = trips.data.copy(deep=True)

    mode_domain = set(trips.schema_effective.domains_effective["mode"]["values"])
    purpose_domain = set(trips.schema_effective.domains_effective["purpose"]["values"])
    expected_mode_out = expected_out_of_domain_count(trips.data["mode"], mode_domain)
    expected_purpose_out = expected_out_of_domain_count(trips.data["purpose"], purpose_domain)

    report = validate_trips(
        trips,
        options=ValidationOptions(
            strict=False,
            validate_required_fields=True,
            validate_constraints=True,
            validate_types_and_formats=True,
            validate_domains="full",
            domains_min_in_domain_ratio=0.50,
            validate_temporal_consistency=False,
            validate_duplicates=False,
        ),
    )

    assert isinstance(report, ValidationReport)
    assert report.ok is True
    assert report.summary["ok"] is True
    assert_issue_present(report.issues, "VAL.DOMAIN.PARTIAL_COVERAGE")
    assert_counts_by_level(report.issues, errors=0, warnings=2, info=0)

    assert "domains" in report.summary
    assert report.summary["domains"]["mode"] == "full"
    assert report.summary["domains"]["min_required_ratio"] == 0.50

    domain_issues_by_field = {
        issue.field: issue
        for issue in report.issues
        if issue.code == "VAL.DOMAIN.PARTIAL_COVERAGE"
    }
    assert domain_issues_by_field["mode"].row_count == expected_mode_out
    assert domain_issues_by_field["purpose"].row_count == expected_purpose_out

    assert trips.metadata["is_validated"] is True
    assert len(trips.metadata["events"]) == 1
    assert_last_event_matches_report(trips, report)
    assert_not_mutated_after_validation(trips, data_before)


def test_validate_trips_integration_temporal_inconsistency_without_strict_returns_report():
    """Verifica error temporal con strict=False: retorna reporte, registra evento y marca is_validated=False."""
    trips = clone_tripdataset(make_tripdataset_with_temporal_issues())
    data_before = trips.data.copy(deep=True)
    expected_violations = expected_temporal_violations(trips.data)

    report = validate_trips(
        trips,
        options=ValidationOptions(
            strict=False,
            validate_required_fields=True,
            validate_constraints=True,
            validate_types_and_formats=True,
            validate_domains="off",
            validate_temporal_consistency=True,
            validate_duplicates=False,
        ),
    )

    assert isinstance(report, ValidationReport)
    assert report.ok is False
    assert report.summary["ok"] is False
    assert_issue_present(report.issues, "VAL.TEMPORAL.ORIGIN_AFTER_DESTINATION")
    assert_counts_by_level(report.issues, errors=1, warnings=0, info=0)

    assert "temporal" in report.summary
    assert report.summary["temporal"]["evaluated"] is True
    assert report.summary["temporal"]["n_violations"] == expected_violations

    temporal_issue = issues_with_code(report.issues, "VAL.TEMPORAL.ORIGIN_AFTER_DESTINATION")[0]
    assert temporal_issue.row_count == expected_violations
    assert temporal_issue.details["n_violations"] == expected_violations
    assert temporal_issue.details["action"] == "report_error"

    assert trips.metadata["is_validated"] is False
    assert len(trips.metadata["events"]) == 1
    assert_last_event_matches_report(trips, report)
    assert_not_mutated_after_validation(trips, data_before)


def test_validate_trips_integration_strict_true_records_event_before_raising_on_temporal_error():
    """Verifica política strict=True: deja evidencia en metadata y luego lanza ValidationError por error de datos."""
    trips = clone_tripdataset(make_tripdataset_with_temporal_issues())
    data_before = trips.data.copy(deep=True)

    with pytest.raises(ValidationError) as exc_info:
        validate_trips(
            trips,
            options=ValidationOptions(
                strict=True,
                validate_required_fields=True,
                validate_constraints=True,
                validate_types_and_formats=True,
                validate_domains="off",
                validate_temporal_consistency=True,
                validate_duplicates=False,
            ),
        )

    exc = exc_info.value
    assert exc.code == "VAL.TEMPORAL.ORIGIN_AFTER_DESTINATION"
    assert exc.issue is not None
    assert exc.issue.code == "VAL.TEMPORAL.ORIGIN_AFTER_DESTINATION"
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

    top_code_map = top_code_map_from_event(event)
    assert "VAL.TEMPORAL.ORIGIN_AFTER_DESTINATION" in top_code_map

    assert_not_mutated_after_validation(trips, data_before)


def test_validate_trips_integration_duplicates_subset_not_usable_aborts_without_event():
    """Verifica precondición fatal de duplicates_subset=None: lanza ValidationError sin registrar evento nuevo."""
    trips = clone_tripdataset(make_tripdataset_unvalidated_small())
    events_before = deepcopy(trips.metadata.get("events", []))
    validated_before = trips.metadata.get("is_validated", False)
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
    assert exc.details["duplicates_subset"] is None
    assert exc.details["action"] == "abort"

    assert trips.metadata.get("events", []) == events_before
    assert trips.metadata.get("is_validated", False) == validated_before
    assert_not_mutated_after_validation(trips, data_before)


def test_validate_trips_integration_metadata_event_and_summary_are_consistent():
    """Verifica consistencia observable entre ValidationReport, metadata, evento e issues_summary."""
    trips = clone_tripdataset(make_tripdataset_with_domain_issues())

    report = validate_trips(
        trips,
        options=ValidationOptions(
            validate_domains="full",
            domains_min_in_domain_ratio=0.50,
            validate_temporal_consistency=False,
            validate_duplicates=False,
        ),
    )

    assert isinstance(report, ValidationReport)
    assert report.ok is True
    assert trips.metadata["is_validated"] is True
    assert len(trips.metadata["events"]) == 1

    event = trips.metadata["events"][-1]
    assert event["summary"] == report.summary
    assert "issues_summary" in event
    assert isinstance(event["issues_summary"]["counts"], dict)
    assert isinstance(event["issues_summary"]["top_codes"], list)

    assert event["issues_summary"]["counts"]["warning"] == report.summary["n_warnings"]
    assert event["issues_summary"]["counts"]["error"] == report.summary["n_errors"]
    assert event["issues_summary"]["counts"]["info"] == report.summary["n_info"]

    top_code_map = top_code_map_from_event(event)
    for code, count in report.summary["counts_by_code"].items():
        assert top_code_map[code] == count