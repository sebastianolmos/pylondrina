import json

from pylondrina.reports import Issue
from pylondrina.schema import FieldSpec, TripSchema
from pylondrina.validation import (
    _build_issues_summary,
    apply_issue_truncation,
    build_validation_summary,
)


# ---------------------------------------------------------------------
# Helpers locales de test
# ---------------------------------------------------------------------


def assert_json_safe(obj, label: str = "object") -> None:
    try:
        json.dumps(obj, default=str)
    except Exception as exc:
        raise AssertionError(f"{label} no es JSON-safe: {exc}") from exc


def make_field(
    name: str,
    dtype: str,
    *,
    required: bool = False,
) -> FieldSpec:
    return FieldSpec(
        name=name,
        dtype=dtype,
        required=required,
    )


def make_trip_schema(fields: list[FieldSpec], *, version: str = "1.1") -> TripSchema:
    return TripSchema(
        version=version,
        fields={field.name: field for field in fields},
        required=[field.name for field in fields if field.required],
        semantic_rules=None,
    )


def issue_codes(issues: list[Issue]) -> list[str]:
    return [issue.code for issue in issues]


def assert_issue_present(issues: list[Issue], code: str) -> None:
    codes = issue_codes(issues)
    assert code in codes, f"No se encontró issue {code!r}. Issues encontrados: {codes!r}"


def issues_with_code(issues: list[Issue], code: str) -> list[Issue]:
    return [issue for issue in issues if issue.code == code]


def make_summary_schema() -> TripSchema:
    return make_trip_schema(
        [
            make_field("movement_id", "string", required=True),
            make_field("mode", "categorical", required=False),
        ]
    )


def make_checks_executed(
    *,
    domains: bool = True,
    temporal_consistency: bool = False,
    duplicates: bool = False,
) -> dict[str, bool]:
    return {
        "required_fields": True,
        "types_and_formats": True,
        "constraints": True,
        "domains": domains,
        "temporal_consistency": temporal_consistency,
        "duplicates": duplicates,
    }


# ---------------------------------------------------------------------
# Tests de _build_issues_summary
# ---------------------------------------------------------------------


def test_build_issues_summary_counts_by_level_and_orders_top_codes():
    """Verifica conteos por severidad y orden de top_codes por frecuencia descendente."""
    issues = [
        Issue(level="warning", code="VAL.DOMAIN.PARTIAL_COVERAGE", message="warn 1"),
        Issue(level="warning", code="VAL.DOMAIN.PARTIAL_COVERAGE", message="warn 2"),
        Issue(level="error", code="VAL.CORE.NULLABILITY_VIOLATION", message="err 1"),
        Issue(level="info", code="VAL.DEBUG.SOMETHING", message="info 1"),
    ]

    summary = _build_issues_summary(issues)

    assert summary["counts"] == {
        "info": 1,
        "warning": 2,
        "error": 1,
    }
    assert summary["top_codes"][0] == {
        "code": "VAL.DOMAIN.PARTIAL_COVERAGE",
        "count": 2,
    }
    assert {"code": "VAL.CORE.NULLABILITY_VIOLATION", "count": 1} in summary["top_codes"]
    assert {"code": "VAL.DEBUG.SOMETHING", "count": 1} in summary["top_codes"]
    assert_json_safe(summary, "issues_summary")


def test_build_issues_summary_empty_input_returns_zero_counts_and_empty_top_codes():
    """Verifica que una lista vacía de issues produzca conteos en cero y top_codes vacío."""
    summary = _build_issues_summary([])

    assert summary == {
        "counts": {
            "info": 0,
            "warning": 0,
            "error": 0,
        },
        "top_codes": [],
    }
    assert_json_safe(summary, "empty_issues_summary")


def test_build_issues_summary_orders_ties_by_code_for_stability():
    """Verifica que empates de frecuencia se ordenen alfabéticamente por código."""
    issues = [
        Issue(level="warning", code="VAL.Z_WARNING", message="warn z"),
        Issue(level="warning", code="VAL.A_WARNING", message="warn a"),
        Issue(level="error", code="VAL.M_ERROR", message="err m"),
    ]

    summary = _build_issues_summary(issues)

    assert summary["top_codes"] == [
        {"code": "VAL.A_WARNING", "count": 1},
        {"code": "VAL.M_ERROR", "count": 1},
        {"code": "VAL.Z_WARNING", "count": 1},
    ]
    assert_json_safe(summary, "issues_summary_tie_order")


# ---------------------------------------------------------------------
# Tests de apply_issue_truncation
# ---------------------------------------------------------------------


def test_apply_issue_truncation_without_truncation_preserves_issues_and_limits_block():
    """Verifica que si no se supera max_issues, los issues se preserven y limits indique no truncado."""
    issues_input = [
        Issue(level="warning", code="VAL.DOMAIN.PARTIAL_COVERAGE", message="warn 1"),
        Issue(level="error", code="VAL.CORE.NULLABILITY_VIOLATION", message="err 1"),
    ]

    issues_out, limits = apply_issue_truncation(
        issues_input,
        max_issues=5,
    )

    assert issues_out == issues_input
    assert limits == {
        "max_issues": 5,
        "issues_truncated": False,
        "n_issues_emitted": len(issues_input),
        "n_issues_detected_total": len(issues_input),
    }
    assert_json_safe(limits, "limits_without_truncation")


def test_apply_issue_truncation_with_truncation_keeps_prefix_and_appends_truncation_issue():
    """Verifica que el truncamiento conserve los primeros issues y agregue VAL.CORE.ISSUES_TRUNCATED al final."""
    issues_input = [
        Issue(level="error", code=f"VAL.TEST.CODE_{i}", message=f"issue {i}")
        for i in range(5)
    ]

    issues_out, limits = apply_issue_truncation(
        issues_input,
        max_issues=3,
    )

    assert len(issues_out) == 3
    assert issue_codes(issues_out[:2]) == ["VAL.TEST.CODE_0", "VAL.TEST.CODE_1"]
    assert issues_out[-1].code == "VAL.CORE.ISSUES_TRUNCATED"
    assert issues_out[-1].level == "warning"

    assert limits == {
        "max_issues": 3,
        "issues_truncated": True,
        "n_issues_emitted": len(issues_out),
        "n_issues_detected_total": len(issues_input),
    }

    truncation_issue = issues_with_code(issues_out, "VAL.CORE.ISSUES_TRUNCATED")[0]
    assert truncation_issue.details["check"] == "limits"
    assert truncation_issue.details["max_issues"] == 3
    assert truncation_issue.details["n_issues_emitted"] == len(issues_out)
    assert truncation_issue.details["n_issues_detected_total"] == len(issues_input)
    assert truncation_issue.details["issues_truncated"] is True
    assert truncation_issue.details["action"] == "truncate_report"
    assert_json_safe(limits, "limits_with_truncation")


def test_apply_issue_truncation_with_max_issues_one_returns_only_truncation_issue():
    """Verifica el borde max_issues=1: solo se emite el issue explícito de truncamiento."""
    issues_input = [
        Issue(level="error", code=f"VAL.TEST.CODE_{i}", message=f"issue {i}")
        for i in range(4)
    ]

    issues_out, limits = apply_issue_truncation(
        issues_input,
        max_issues=1,
    )

    assert len(issues_out) == 1
    assert issues_out[0].code == "VAL.CORE.ISSUES_TRUNCATED"
    assert issues_out[0].level == "warning"

    assert limits["max_issues"] == 1
    assert limits["issues_truncated"] is True
    assert limits["n_issues_emitted"] == 1
    assert limits["n_issues_detected_total"] == len(issues_input)


# ---------------------------------------------------------------------
# Tests de build_validation_summary
# ---------------------------------------------------------------------


def test_build_validation_summary_minimal_without_optional_blocks():
    """Verifica el summary mínimo: ok, conteos, checks ejecutados, campos revisados y versión de schema."""
    schema = make_summary_schema()
    issues = [
        Issue(level="warning", code="VAL.DOMAIN.PARTIAL_COVERAGE", message="warn 1"),
        Issue(level="warning", code="VAL.DOMAIN.PARTIAL_COVERAGE", message="warn 2"),
        Issue(level="error", code="VAL.CORE.NULLABILITY_VIOLATION", message="err 1"),
    ]
    checked_fields = ["movement_id", "mode"]
    checks_executed = make_checks_executed(
        domains=True,
        temporal_consistency=False,
        duplicates=False,
    )

    summary = build_validation_summary(
        n_rows=10,
        issues=issues,
        schema=schema,
        checks_executed=checks_executed,
        checked_fields=checked_fields,
    )

    assert summary["ok"] is False
    assert summary["n_rows"] == 10
    assert summary["n_issues"] == len(issues)
    assert summary["n_errors"] == 1
    assert summary["n_warnings"] == 2
    assert summary["n_info"] == 0

    assert summary["counts_by_level"] == {
        "error": 1,
        "warning": 2,
        "info": 0,
    }
    assert summary["counts_by_code"] == {
        "VAL.DOMAIN.PARTIAL_COVERAGE": 2,
        "VAL.CORE.NULLABILITY_VIOLATION": 1,
    }
    assert summary["checked_fields"] == checked_fields
    assert summary["checks_executed"] == checks_executed
    assert summary["schema_version"] == schema.version

    assert "domains" not in summary
    assert "temporal" not in summary
    assert "duplicates" not in summary
    assert "limits" not in summary

    assert_json_safe(summary, "validation_summary_minimal")


def test_build_validation_summary_without_error_keeps_ok_as_placeholder_false():
    """Verifica que el helper construya el summary base, dejando ok=False para que validate_trips lo actualice."""
    schema = make_summary_schema()
    issues = [
        Issue(level="warning", code="VAL.DOMAIN.PARTIAL_COVERAGE", message="warn 1"),
        Issue(level="info", code="VAL.DEBUG.SOMETHING", message="info 1"),
    ]

    summary = build_validation_summary(
        n_rows=5,
        issues=issues,
        schema=schema,
        checks_executed=make_checks_executed(),
        checked_fields=["movement_id", "mode"],
    )

    # Este helper no calcula el ok final. validate_trips lo actualiza después.
    assert summary["ok"] is False
    assert summary["n_errors"] == 0
    assert summary["n_warnings"] == 1
    assert summary["n_info"] == 1
    assert summary["n_issues"] == len(issues)
    assert_json_safe(summary, "validation_summary_without_errors")


def test_build_validation_summary_includes_optional_blocks_when_provided():
    """Verifica que domains, temporal, duplicates y limits se incluyan sin alteración cuando se entregan."""
    schema = make_summary_schema()

    domains_block = {
        "mode": "full",
        "min_required_ratio": 1.0,
        "fields": {
            "mode": {
                "ratio_in_domain": 1.0,
            }
        },
    }
    temporal_block = {
        "evaluated": True,
        "tier": "tier_1",
        "n_checked": 5,
        "n_violations": 0,
    }
    duplicates_block = {
        "evaluated": True,
        "duplicates_subset": ["user_id", "origin_time_utc"],
        "n_duplicate_rows": 0,
    }
    limits_block = {
        "max_issues": 500,
        "issues_truncated": False,
        "n_issues_emitted": 0,
        "n_issues_detected_total": 0,
    }

    summary = build_validation_summary(
        n_rows=5,
        issues=[],
        schema=schema,
        checks_executed=make_checks_executed(
            domains=True,
            temporal_consistency=True,
            duplicates=True,
        ),
        checked_fields=["movement_id", "mode"],
        domains_block=domains_block,
        temporal_block=temporal_block,
        duplicates_block=duplicates_block,
        limits_block=limits_block,
    )

    # Este helper no calcula el ok final. validate_trips lo actualiza después.
    assert summary["ok"] is False

    assert summary["n_errors"] == 0
    assert summary["n_warnings"] == 0
    assert summary["n_info"] == 0
    assert summary["n_issues"] == 0

    assert summary["domains"] == domains_block
    assert summary["temporal"] == temporal_block
    assert summary["duplicates"] == duplicates_block
    assert summary["limits"] == limits_block

    assert summary["domains"]["mode"] == "full"
    assert summary["temporal"]["evaluated"] is True
    assert summary["duplicates"]["n_duplicate_rows"] == 0
    assert summary["limits"]["issues_truncated"] is False

    assert_json_safe(summary, "validation_summary_with_optionals")