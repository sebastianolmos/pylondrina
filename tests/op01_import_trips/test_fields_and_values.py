import pandas as pd
import pytest

from pylondrina.errors import ImportError as PylondrinaImportError
from pylondrina.importing import import_trips_from_dataframe
from pylondrina.schema import DomainSpec, FieldSpec, TripSchema


SOFT_CAP = 256
HARD_CAP = 1024


# ---------------------------------------------------------------------
# Helpers locales de test
# ---------------------------------------------------------------------


def issue_codes(issues) -> list[str]:
    return [issue.code for issue in issues]


def cap_issue_codes(issues) -> list[str]:
    return [issue.code for issue in issues if issue.code.startswith("IMP.COLUMNS.")]


def issues_with_code(issues, code: str):
    return [issue for issue in issues if issue.code == code]


def make_category_inference_schema() -> TripSchema:
    return TripSchema(
        version="1.0-test",
        fields={
            "movement_id": FieldSpec(name="movement_id", dtype="string", required=True),
            "user_id": FieldSpec(name="user_id", dtype="string", required=True),
            "origin_time_utc": FieldSpec(name="origin_time_utc", dtype="datetime", required=False),
            "destination_time_utc": FieldSpec(name="destination_time_utc", dtype="datetime", required=False),
            "cat_infer_ok": FieldSpec(
                name="cat_infer_ok",
                dtype="categorical",
                required=False,
                domain=DomainSpec(values=[], extendable=True),
            ),
            "cat_infer_bad": FieldSpec(
                name="cat_infer_bad",
                dtype="categorical",
                required=False,
                domain=DomainSpec(values=[], extendable=True),
            ),
            "cat_defined": FieldSpec(
                name="cat_defined",
                dtype="categorical",
                required=False,
                domain=DomainSpec(values=["A", "B", "C"], extendable=False),
            ),
        },
        required=["movement_id", "user_id"],
    )


def make_category_inference_dataframe(
    *,
    n_rows: int,
    ok_cardinality: int,
    bad_cardinality: int,
    label_width: int,
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "movement_id": [f"m{i}" for i in range(n_rows)],
            "user_id": [f"user_{i}" for i in range(n_rows)],
            "origin_time_utc": ["2024-01-01T08:00:00Z"] * n_rows,
            "destination_time_utc": ["2024-01-01T08:10:00Z"] * n_rows,
            "cat_infer_ok": [f"ok_{i % ok_cardinality:0{label_width}d}" for i in range(n_rows)],
            "cat_infer_bad": [f"bad_{i % bad_cardinality:0{label_width}d}" for i in range(n_rows)],
            "cat_defined": [["A", "B", "C"][i % 3] for i in range(n_rows)],
        }
    )


def assert_category_inference_result(
    *,
    trips,
    report,
    n_rows: int,
    ok_cardinality: int,
    bad_cardinality: int,
    cardinality_limit: float,
) -> None:
    issue_codes_set = set(issue_codes(report.issues))
    empty_domain_fields = {
        issue.field
        for issue in report.issues
        if issue.code == "SCH.DOMAIN.EMPTY_VALUES"
    }

    applied_issues = [
        issue
        for issue in report.issues
        if issue.code == "DOM.INFERENCE.APPLIED" and issue.field == "cat_infer_ok"
    ]
    degraded_issues = [
        issue
        for issue in report.issues
        if issue.code == "DOM.INFERENCE.DEGRADED_TO_STRING" and issue.field == "cat_infer_bad"
    ]
    defined_field_inference_issues = [
        issue
        for issue in report.issues
        if issue.field == "cat_defined" and issue.code.startswith("DOM.INFERENCE.")
    ]

    assert report.ok is True

    assert {"cat_infer_ok", "cat_infer_bad"}.issubset(empty_domain_fields)
    assert "DOM.INFERENCE.APPLIED" in issue_codes_set
    assert "DOM.INFERENCE.DEGRADED_TO_STRING" in issue_codes_set

    assert applied_issues, "No se encontró DOM.INFERENCE.APPLIED para cat_infer_ok."
    assert degraded_issues, "No se encontró DOM.INFERENCE.DEGRADED_TO_STRING para cat_infer_bad."

    applied_issue = applied_issues[0]
    degraded_issue = degraded_issues[0]

    assert applied_issue.details["alpha"] == 0.05
    assert applied_issue.details["n_rows_non_null"] == n_rows
    assert applied_issue.details["n_unique_observed"] == ok_cardinality
    assert applied_issue.details["cardinality_limit"] == cardinality_limit

    assert degraded_issue.details["alpha"] == 0.05
    assert degraded_issue.details["n_rows_non_null"] == n_rows
    assert degraded_issue.details["n_unique_observed"] == bad_cardinality
    assert degraded_issue.details["cardinality_limit"] == cardinality_limit
    assert degraded_issue.details["fallback_dtype"] == "string"

    assert trips.schema_effective.dtype_effective["cat_infer_ok"] == "categorical"
    assert "cat_infer_ok" in trips.metadata["domains_effective"]
    assert len(trips.metadata["domains_effective"]["cat_infer_ok"]["values"]) == ok_cardinality

    assert trips.schema_effective.dtype_effective["cat_infer_bad"] == "string"
    assert "cat_infer_bad" not in trips.metadata["domains_effective"]

    assert trips.schema_effective.dtype_effective["cat_defined"] == "categorical"
    assert "cat_defined" in trips.metadata["domains_effective"]
    assert trips.metadata["domains_effective"]["cat_defined"]["base_values"] == ["A", "B", "C"]
    assert not defined_field_inference_issues


# ---------------------------------------------------------------------
# Tests de caps de columnas
# ---------------------------------------------------------------------


def test_import_normal_width_does_not_emit_column_cap_issues():
    """Verifica que un import angosto no emita issues de soft cap ni hard cap de columnas."""
    schema = TripSchema(
        version="1.0-test",
        fields={
            "movement_id": FieldSpec(name="movement_id", dtype="string", required=True),
            "user_note": FieldSpec(name="user_note", dtype="string", required=True),
            "origin_time_utc": FieldSpec(name="origin_time_utc", dtype="datetime", required=False),
            "destination_time_utc": FieldSpec(name="destination_time_utc", dtype="datetime", required=False),
            "mode_hint": FieldSpec(name="mode_hint", dtype="string", required=False),
        },
        required=["movement_id", "user_note"],
    )

    df = pd.DataFrame(
        {
            "movement_id": ["m0", "m1"],
            "user_note": ["fila_a", "fila_b"],
            "origin_time_utc": ["2024-01-01T08:00:00Z", "2024-01-01T09:00:00Z"],
            "destination_time_utc": ["2024-01-01T08:10:00Z", "2024-01-01T09:10:00Z"],
            "mode_hint": ["bus", "metro"],
        }
    )

    trips, report = import_trips_from_dataframe(
        df=df,
        schema=schema,
    )

    codes = cap_issue_codes(report.issues)

    assert report.ok is True
    assert len(trips.data.columns) < SOFT_CAP
    assert "IMP.COLUMNS.WIDE_TABLE" not in codes
    assert "IMP.COLUMNS.HARD_CAP_EXCEEDED" not in codes


def test_import_soft_cap_exceeded_with_schema_fields_emits_wide_table_warning():
    """Verifica que superar el soft cap con campos del schema permita continuar y emita IMP.COLUMNS.WIDE_TABLE."""
    n_wide_schema_fields = 260

    fields = {
        "movement_id": FieldSpec(name="movement_id", dtype="string", required=True),
        "user_note": FieldSpec(name="user_note", dtype="string", required=True),
        "origin_time_utc": FieldSpec(name="origin_time_utc", dtype="datetime", required=False),
        "destination_time_utc": FieldSpec(name="destination_time_utc", dtype="datetime", required=False),
    }

    for i in range(n_wide_schema_fields):
        field_name = f"wide_{i:03d}"
        fields[field_name] = FieldSpec(name=field_name, dtype="string", required=False)

    schema = TripSchema(
        version="1.0-test",
        fields=fields,
        required=["movement_id", "user_note"],
    )

    row = {
        "movement_id": "m0",
        "user_note": "ok",
        "origin_time_utc": "2024-01-01T08:00:00Z",
        "destination_time_utc": "2024-01-01T08:10:00Z",
    }

    for i in range(n_wide_schema_fields):
        row[f"wide_{i:03d}"] = f"valor_{i}"

    trips, report = import_trips_from_dataframe(
        df=pd.DataFrame([row]),
        schema=schema,
    )

    codes = cap_issue_codes(report.issues)
    wide_issues = issues_with_code(report.issues, "IMP.COLUMNS.WIDE_TABLE")

    assert report.ok is True
    assert SOFT_CAP < len(trips.data.columns) < HARD_CAP
    assert "IMP.COLUMNS.WIDE_TABLE" in codes
    assert "IMP.COLUMNS.HARD_CAP_EXCEEDED" not in codes
    assert len(wide_issues) == 1

    wide_issue = wide_issues[0]
    assert wide_issue.details["n_columns"] == len(trips.data.columns)
    assert wide_issue.details["soft_cap"] == SOFT_CAP
    assert wide_issue.details["hard_cap"] == HARD_CAP
    assert wide_issue.details["extra_fields_kept_total"] == 0
    assert wide_issue.details["action"] == "allow_with_warning"

    assert trips.metadata.get("extra_fields_kept") == []


def test_import_hard_cap_exceeded_with_extra_fields_raises_import_error():
    """Verifica que superar el hard cap mediante campos extra aborte con IMP.COLUMNS.HARD_CAP_EXCEEDED."""
    n_extra_fields_hard = 1022

    schema = TripSchema(
        version="1.0-test",
        fields={
            "movement_id": FieldSpec(name="movement_id", dtype="string", required=True),
            "core_col": FieldSpec(name="core_col", dtype="string", required=True),
            "origin_time_utc": FieldSpec(name="origin_time_utc", dtype="datetime", required=False),
            "destination_time_utc": FieldSpec(name="destination_time_utc", dtype="datetime", required=False),
        },
        required=["movement_id", "core_col"],
    )

    row = {
        "movement_id": "m0",
        "core_col": "ok",
        "origin_time_utc": "2024-01-01T08:00:00Z",
        "destination_time_utc": "2024-01-01T08:10:00Z",
    }

    for i in range(n_extra_fields_hard):
        row[f"extra_{i:04d}"] = f"valor_{i}"

    with pytest.raises(PylondrinaImportError) as exc_info:
        import_trips_from_dataframe(
            df=pd.DataFrame([row]),
            schema=schema,
        )

    exc = exc_info.value

    assert exc.code == "IMP.COLUMNS.HARD_CAP_EXCEEDED"
    assert exc.issue is not None
    assert exc.issue.code == "IMP.COLUMNS.HARD_CAP_EXCEEDED"
    assert exc.details["n_columns"] > HARD_CAP
    assert exc.details["soft_cap"] == SOFT_CAP
    assert exc.details["hard_cap"] == HARD_CAP
    assert exc.details["extra_fields_kept_total"] == n_extra_fields_hard
    assert exc.details["action"] == "abort"
    assert issue_codes(exc.issues or []) == ["IMP.COLUMNS.HARD_CAP_EXCEEDED"]


# ---------------------------------------------------------------------
# Tests de inferencia categórica con DomainSpec(values=[])
# ---------------------------------------------------------------------


def test_category_inference_policy_with_normal_size_dataset():
    """Verifica inferencia categórica aplicada/degradada en 200 filas y preservación de dominio explícito."""
    n_rows = 200
    cardinality_limit = 0.05 * n_rows

    schema = make_category_inference_schema()
    df = make_category_inference_dataframe(
        n_rows=n_rows,
        ok_cardinality=10,
        bad_cardinality=11,
        label_width=2,
    )

    trips, report = import_trips_from_dataframe(
        df=df,
        schema=schema,
    )

    assert_category_inference_result(
        trips=trips,
        report=report,
        n_rows=n_rows,
        ok_cardinality=10,
        bad_cardinality=11,
        cardinality_limit=cardinality_limit,
    )


def test_category_inference_policy_with_large_size_dataset():
    """Verifica que la política de inferencia categórica se mantenga al escalar a 20.000 filas."""
    n_rows = 20_000
    cardinality_limit = 0.05 * n_rows

    schema = make_category_inference_schema()
    df = make_category_inference_dataframe(
        n_rows=n_rows,
        ok_cardinality=1000,
        bad_cardinality=1001,
        label_width=4,
    )

    trips, report = import_trips_from_dataframe(
        df=df,
        schema=schema,
    )

    assert_category_inference_result(
        trips=trips,
        report=report,
        n_rows=n_rows,
        ok_cardinality=1000,
        bad_cardinality=1001,
        cardinality_limit=cardinality_limit,
    )