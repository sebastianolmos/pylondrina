import pandas as pd

from pylondrina.schema import DomainSpec, FieldSpec, TripSchema
from pylondrina.validation import (
    check_domains,
    check_duplicates,
    check_temporal_consistency,
)


# ---------------------------------------------------------------------
# Helpers locales de test
# ---------------------------------------------------------------------


def make_field(
    name: str,
    dtype: str,
    *,
    required: bool = False,
    domain: DomainSpec | None = None,
) -> FieldSpec:
    return FieldSpec(
        name=name,
        dtype=dtype,
        required=required,
        domain=domain,
    )


def make_trip_schema(fields: list[FieldSpec], *, version: str = "1.1") -> TripSchema:
    return TripSchema(
        version=version,
        fields={field.name: field for field in fields},
        required=[field.name for field in fields if field.required],
        semantic_rules=None,
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


def assert_counts_by_level(
    issues,
    *,
    errors: int = 0,
    warnings: int = 0,
    info: int = 0,
) -> None:
    levels = [issue.level for issue in issues]
    assert levels.count("error") == errors
    assert levels.count("warning") == warnings
    assert levels.count("info") == info


# ---------------------------------------------------------------------
# Fixtures locales simples
# ---------------------------------------------------------------------


def make_domain_schema() -> TripSchema:
    return make_trip_schema(
        [
            make_field(
                "mode",
                "categorical",
                domain=DomainSpec(values=["walk", "bus", "metro"]),
            ),
        ]
    )


def make_domain_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "mode": ["walk", "bus", "bike", None],
        }
    )


# ---------------------------------------------------------------------
# Tests de check_domains
# ---------------------------------------------------------------------


def test_check_domains_mode_off_returns_no_issues_and_no_summary_block():
    """Verifica que mode='off' desactive la validación de dominios y no construya bloque de summary."""
    schema = make_domain_schema()
    df = make_domain_dataframe()

    issues, block = check_domains(
        df,
        schema=schema,
        effective_domains_by_field={"mode": {"walk", "bus", "metro"}},
        mode="off",
        sample_frac=0.5,
        min_in_domain_ratio=1.0,
        sample_rows_per_issue=5,
    )

    assert issues == []
    assert block is None


def test_check_domains_full_mode_reports_partial_coverage_when_ratio_is_above_minimum():
    """Verifica que cobertura parcial por sobre el mínimo emita warning VAL.DOMAIN.PARTIAL_COVERAGE."""
    schema = make_domain_schema()
    df = make_domain_dataframe()
    effective_domain = {"walk", "bus", "metro"}

    issues, block = check_domains(
        df,
        schema=schema,
        effective_domains_by_field={"mode": effective_domain},
        mode="full",
        sample_frac=0.5,
        min_in_domain_ratio=0.5,
        sample_rows_per_issue=5,
    )

    assert len(issues) == 1
    assert_issue_present(issues, "VAL.DOMAIN.PARTIAL_COVERAGE")
    assert_counts_by_level(issues, errors=0, warnings=1, info=0)

    issue = issues_with_code(issues, "VAL.DOMAIN.PARTIAL_COVERAGE")[0]
    assert issue.level == "warning"
    assert issue.field == "mode"
    assert issue.row_count == 1
    assert issue.details["field"] == "mode"
    assert issue.details["mode"] == "full"
    assert issue.details["n_checked_non_null"] == int(df["mode"].notna().sum())
    assert issue.details["n_in_domain"] == int(df["mode"].isin(effective_domain).sum())
    assert issue.details["ratio_in_domain"] >= issue.details["min_required_ratio"]
    assert issue.details["action"] == "report_warning"

    assert block["mode"] == "full"
    assert block["min_required_ratio"] == 0.5
    assert "mode" in block["fields"]
    assert block["fields"]["mode"]["n_checked_non_null"] == int(df["mode"].notna().sum())
    assert block["fields"]["mode"]["n_in_domain"] == int(df["mode"].isin(effective_domain).sum())
    assert block["fields"]["mode"]["ratio_in_domain"] >= block["min_required_ratio"]


def test_check_domains_full_mode_reports_ratio_below_minimum_as_error():
    """Verifica que cobertura bajo el mínimo emita error VAL.DOMAIN.RATIO_BELOW_MIN."""
    schema = make_domain_schema()
    df = make_domain_dataframe()
    effective_domain = {"walk", "bus", "metro"}

    issues, block = check_domains(
        df,
        schema=schema,
        effective_domains_by_field={"mode": effective_domain},
        mode="full",
        sample_frac=0.5,
        min_in_domain_ratio=0.9,
        sample_rows_per_issue=5,
    )

    assert len(issues) == 1
    assert_issue_present(issues, "VAL.DOMAIN.RATIO_BELOW_MIN")
    assert_counts_by_level(issues, errors=1, warnings=0, info=0)

    issue = issues_with_code(issues, "VAL.DOMAIN.RATIO_BELOW_MIN")[0]
    assert issue.level == "error"
    assert issue.field == "mode"
    assert issue.row_count == 1
    assert issue.details["field"] == "mode"
    assert issue.details["mode"] == "full"
    assert issue.details["n_checked_non_null"] == int(df["mode"].notna().sum())
    assert issue.details["n_in_domain"] == int(df["mode"].isin(effective_domain).sum())
    assert issue.details["ratio_in_domain"] < issue.details["min_required_ratio"]
    assert issue.details["action"] == "report_error"

    assert block["mode"] == "full"
    assert block["min_required_ratio"] == 0.9
    assert "mode" in block["fields"]
    assert block["fields"]["mode"]["ratio_in_domain"] < block["min_required_ratio"]


def test_check_domains_missing_domain_info_emits_warning_and_skips_field_summary():
    """Verifica que un campo categórico sin dominio usable emita VAL.DOMAIN.MISSING_DOMAIN_INFO y no entre al bloque fields."""
    schema = make_trip_schema(
        [
            make_field("purpose", "categorical"),
        ]
    )
    df = pd.DataFrame(
        {
            "purpose": ["work", "study", "health"],
        }
    )

    issues, block = check_domains(
        df,
        schema=schema,
        effective_domains_by_field={"purpose": None},
        mode="full",
        sample_frac=0.5,
        min_in_domain_ratio=1.0,
        sample_rows_per_issue=5,
    )

    assert len(issues) == 1
    assert_issue_present(issues, "VAL.DOMAIN.MISSING_DOMAIN_INFO")
    assert_counts_by_level(issues, errors=0, warnings=1, info=0)

    issue = issues_with_code(issues, "VAL.DOMAIN.MISSING_DOMAIN_INFO")[0]
    assert issue.level == "warning"
    assert issue.field == "purpose"
    assert issue.details["field"] == "purpose"
    assert issue.details["reason"] == "missing_domain_values"
    assert issue.details["action"] == "skip_field"

    assert block["mode"] == "full"
    assert block["min_required_ratio"] == 1.0
    assert block["fields"] == {}


# ---------------------------------------------------------------------
# Tests de check_temporal_consistency
# ---------------------------------------------------------------------


def test_check_temporal_consistency_tier2_is_not_evaluated():
    """Verifica que Tier 2 no aplique regla origin_time_utc <= destination_time_utc y retorne evaluated=False."""
    df = pd.DataFrame(
        {
            "origin_time_local_hhmm": ["08:00", "09:00"],
            "destination_time_local_hhmm": ["08:30", "09:20"],
        }
    )

    issues, block = check_temporal_consistency(
        df,
        temporal_context={"tier": "tier_2"},
        sample_rows_per_issue=5,
    )

    assert issues == []
    assert block["evaluated"] is False
    assert block["reason"] == "temporal_tier_not_1"
    assert block["tier"] == "tier_2"


def test_check_temporal_consistency_tier1_happy_path_reports_zero_violations():
    """Verifica que Tier 1 sin inversiones temporales retorne bloque evaluado sin issues."""
    df = pd.DataFrame(
        {
            "origin_time_utc": ["2026-01-01T08:00:00Z", "2026-01-01T09:00:00Z"],
            "destination_time_utc": ["2026-01-01T08:30:00Z", "2026-01-01T09:20:00Z"],
        }
    )

    issues, block = check_temporal_consistency(
        df,
        temporal_context={"tier": "tier_1"},
        sample_rows_per_issue=5,
    )

    assert issues == []
    assert block["evaluated"] is True
    assert block["tier"] == "tier_1"
    assert block["n_checked"] == len(df)
    assert block["n_violations"] == 0
    assert block["origin_field"] == "origin_time_utc"
    assert block["destination_field"] == "destination_time_utc"


def test_check_temporal_consistency_tier1_origin_after_destination_reports_error():
    """Verifica que Tier 1 con origen posterior a destino emita VAL.TEMPORAL.ORIGIN_AFTER_DESTINATION."""
    df = pd.DataFrame(
        {
            "origin_time_utc": ["2026-01-01T09:00:00Z", "2026-01-01T10:00:00Z"],
            "destination_time_utc": ["2026-01-01T08:30:00Z", "2026-01-01T09:50:00Z"],
        }
    )

    issues, block = check_temporal_consistency(
        df,
        temporal_context={"tier": "tier_1"},
        sample_rows_per_issue=5,
    )

    assert len(issues) == 1
    assert_issue_present(issues, "VAL.TEMPORAL.ORIGIN_AFTER_DESTINATION")
    assert_counts_by_level(issues, errors=1, warnings=0, info=0)

    issue = issues_with_code(issues, "VAL.TEMPORAL.ORIGIN_AFTER_DESTINATION")[0]
    assert issue.level == "error"
    assert issue.row_count == len(df)
    assert issue.details["n_rows_total"] == len(df)
    assert issue.details["n_violations"] == len(df)
    assert issue.details["row_indices_sample"] == list(df.index)
    assert issue.details["origin_field"] == "origin_time_utc"
    assert issue.details["destination_field"] == "destination_time_utc"
    assert issue.details["action"] == "report_error"

    assert block["evaluated"] is True
    assert block["tier"] == "tier_1"
    assert block["n_checked"] == len(df)
    assert block["n_violations"] == len(df)
    assert block["origin_field"] == "origin_time_utc"
    assert block["destination_field"] == "destination_time_utc"


# ---------------------------------------------------------------------
# Tests de check_duplicates
# ---------------------------------------------------------------------


def test_check_duplicates_happy_path_reports_zero_duplicate_rows():
    """Verifica que un subset sin filas repetidas retorne bloque evaluado sin issues."""
    df = pd.DataFrame(
        {
            "user_id": ["u1", "u2", "u3"],
            "origin_time_utc": [
                "2026-01-01T08:00:00Z",
                "2026-01-01T09:00:00Z",
                "2026-01-01T10:00:00Z",
            ],
        }
    )
    duplicates_subset = ("user_id", "origin_time_utc")

    issues, block = check_duplicates(
        df,
        duplicates_subset=duplicates_subset,
        sample_rows_per_issue=5,
    )

    assert issues == []
    assert block["evaluated"] is True
    assert block["n_duplicate_rows"] == 0
    assert block["duplicates_subset"] == list(duplicates_subset)


def test_check_duplicates_detects_rows_repeated_under_explicit_subset():
    """Verifica que filas duplicadas bajo duplicates_subset emitan VAL.DUPLICATES.ROWS_FOUND."""
    df = pd.DataFrame(
        {
            "user_id": ["u1", "u1", "u2", "u3"],
            "origin_time_utc": [
                "2026-01-01T08:00:00Z",
                "2026-01-01T08:00:00Z",
                "2026-01-01T09:00:00Z",
                "2026-01-01T10:00:00Z",
            ],
        }
    )
    duplicates_subset = ("user_id", "origin_time_utc")
    expected_duplicate_mask = df.duplicated(subset=list(duplicates_subset), keep=False)
    expected_duplicate_count = int(expected_duplicate_mask.sum())

    issues, block = check_duplicates(
        df,
        duplicates_subset=duplicates_subset,
        sample_rows_per_issue=5,
    )

    assert len(issues) == 1
    assert_issue_present(issues, "VAL.DUPLICATES.ROWS_FOUND")
    assert_counts_by_level(issues, errors=1, warnings=0, info=0)

    issue = issues_with_code(issues, "VAL.DUPLICATES.ROWS_FOUND")[0]
    assert issue.level == "error"
    assert issue.row_count == expected_duplicate_count
    assert issue.details["n_rows_total"] == len(df)
    assert issue.details["n_violations"] == expected_duplicate_count
    assert issue.details["duplicates_subset"] == list(duplicates_subset)
    assert issue.details["row_indices_sample"] == df.index[expected_duplicate_mask].tolist()
    assert issue.details["action"] == "report_error"

    assert block["evaluated"] is True
    assert block["n_duplicate_rows"] == expected_duplicate_count
    assert block["duplicates_subset"] == list(duplicates_subset)