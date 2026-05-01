from pathlib import Path

import pandas as pd
import pytest

from pylondrina.datasets import TripDataset
from pylondrina.importing import ImportOptions, import_trips_from_dataframe
from pylondrina.reports import ImportReport, ValidationReport
from pylondrina.schema import DomainSpec, FieldSpec, TripSchema
from pylondrina.validation import ValidationOptions, validate_trips


# ---------------------------------------------------------------------
# Constantes y helpers locales
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

PURPOSE_FINOS = {
    "Al trabajo",
    "Por trabajo",
    "Al estudio",
    "Por estudio",
    "volver a casa",
    "Visitar a alguien",
    "Buscar o Dejar a alguien",
    "Buscar o dejar algo",
    "Comer o Tomar algo",
    "De compras",
    "Trámites",
    "Recreación",
    "Otra actividad",
}


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


def assert_issue_absent(issues, code: str) -> None:
    codes = issue_codes(issues)
    assert code not in codes, f"No se esperaba issue {code!r}. Issues encontrados: {codes!r}"


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def synthetic_csv_path() -> Path:
    return repo_root() / "data" / "synthetic" / "tripdataset_real_canonical.csv"


def read_real_canonical_csv_or_skip() -> pd.DataFrame:
    path = synthetic_csv_path()
    if not path.exists():
        pytest.skip(f"No existe el CSV sintético requerido por el notebook: {path}")
    return pd.read_csv(path)


def make_schema_trips_canonical() -> TripSchema:
    return TripSchema(
        version="1.1",
        fields={
            "movement_id": make_field("movement_id", "string", required=True),
            "user_id": make_field("user_id", "string", required=True),
            "origin_longitude": make_field("origin_longitude", "float", required=True),
            "origin_latitude": make_field("origin_latitude", "float", required=True),
            "destination_longitude": make_field("destination_longitude", "float", required=True),
            "destination_latitude": make_field("destination_latitude", "float", required=True),
            "origin_h3_index": make_field("origin_h3_index", "string", required=True),
            "destination_h3_index": make_field("destination_h3_index", "string", required=True),
            "trip_id": make_field("trip_id", "string", required=True),
            "movement_seq": make_field("movement_seq", "int", required=True),
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
            "user_gender": make_field(
                "user_gender",
                "categorical",
                required=False,
                domain=DomainSpec(
                    values=["female", "male", "other", "unknown"],
                    extendable=False,
                ),
            ),
            "origin_municipality": make_field(
                "origin_municipality",
                "string",
                required=False,
            ),
            "destination_municipality": make_field(
                "destination_municipality",
                "string",
                required=False,
            ),
            "trip_weight": make_field("trip_weight", "float", required=False),
            "timezone_offset_min": make_field("timezone_offset_min", "int", required=False),
        },
        required=[
            "movement_id",
            "user_id",
            "origin_longitude",
            "origin_latitude",
            "destination_longitude",
            "destination_latitude",
            "origin_h3_index",
            "destination_h3_index",
            "trip_id",
            "movement_seq",
        ],
        semantic_rules=None,
    )


def make_import_options_bridge() -> ImportOptions:
    return ImportOptions(
        keep_extra_fields=True,
        strict=False,
        strict_domains=False,
        single_stage=False,
    )


def make_validation_options_bridge() -> ValidationOptions:
    return ValidationOptions(
        strict=False,
        validate_required_fields=True,
        validate_types_and_formats=True,
        validate_constraints=True,
        validate_domains="full",
        validate_temporal_consistency=True,
        validate_duplicates=True,
        duplicates_subset=(
            "user_id",
            "origin_time_utc",
            "origin_h3_index",
            "destination_h3_index",
        ),
        allow_partial_od_spatial=True,
    )


def import_real_canonical_bridge() -> tuple[TripDataset, ImportReport]:
    df = read_real_canonical_csv_or_skip()

    return import_trips_from_dataframe(
        df,
        schema=make_schema_trips_canonical(),
        source_name="tripdataset_real_canonical",
        options=make_import_options_bridge(),
        provenance={
            "source": {
                "name": "tripdataset_real_canonical",
                "entity": "trips",
                "version": "synthetic_v1",
            }
        },
        h3_resolution=8,
    )


def assert_import_handoff_contract(trips: TripDataset, import_report: ImportReport) -> None:
    assert isinstance(trips, TripDataset)
    assert isinstance(import_report, ImportReport)
    assert import_report.ok is True

    assert trips.schema_effective is not None
    assert "domains_effective" in trips.metadata
    assert "temporal" in trips.metadata
    assert "h3" in trips.metadata

    assert isinstance(trips.metadata["domains_effective"], dict)
    assert isinstance(trips.metadata["temporal"], dict)
    assert isinstance(trips.metadata["h3"], dict)

    assert trips.metadata["temporal"]["tier"] == "tier_1"
    assert trips.metadata["h3"]["resolution"] == 8

    assert len(trips.metadata["events"]) >= 1
    assert trips.metadata["events"][0]["op"] == "import_trips"

    assert import_report.metadata["dataset_id"] == trips.metadata["dataset_id"]
    assert trips.metadata["is_validated"] is False


def assert_validate_event_matches_report(
    trips: TripDataset,
    report: ValidationReport,
) -> None:
    validate_event = trips.metadata["events"][-1]

    assert validate_event["op"] == "validate_trips"
    assert validate_event["summary"] == report.summary
    assert "issues_summary" in validate_event
    assert isinstance(validate_event["issues_summary"]["counts"], dict)
    assert isinstance(validate_event["issues_summary"]["top_codes"], list)


# ---------------------------------------------------------------------
# Tests puente import -> validate
# ---------------------------------------------------------------------


def test_integration_import_bridge_real_canonical_import_then_validate():
    """Verifica el handoff real OP-01 -> OP-02 usando el CSV canónico sintético del notebook."""
    tripdataset_bridge, import_report = import_real_canonical_bridge()

    assert_import_handoff_contract(tripdataset_bridge, import_report)

    data_before_validate = tripdataset_bridge.data.copy(deep=True)

    validation_report = validate_trips(
        tripdataset_bridge,
        options=make_validation_options_bridge(),
    )

    assert isinstance(validation_report, ValidationReport)
    assert validation_report.ok is True
    assert validation_report.summary["ok"] is True
    assert not any(issue.level == "error" for issue in validation_report.issues)

    assert tripdataset_bridge.metadata["is_validated"] is True
    assert len(tripdataset_bridge.metadata["events"]) == 2
    assert tripdataset_bridge.metadata["events"][0]["op"] == "import_trips"
    assert tripdataset_bridge.metadata["events"][1]["op"] == "validate_trips"

    assert_validate_event_matches_report(tripdataset_bridge, validation_report)

    assert "domains" in validation_report.summary
    assert "temporal" in validation_report.summary
    assert "duplicates" in validation_report.summary

    assert validation_report.summary["temporal"]["evaluated"] is True
    assert validation_report.summary["duplicates"]["evaluated"] is True

    pd.testing.assert_frame_equal(
        tripdataset_bridge.data,
        data_before_validate,
        check_dtype=True,
        check_like=False,
    )


def test_integration_import_bridge_validate_uses_effective_purpose_domain():
    """Verifica que validate_trips use el dominio efectivo de purpose generado por import."""
    tripdataset_bridge, import_report = import_real_canonical_bridge()

    assert_import_handoff_contract(tripdataset_bridge, import_report)

    purpose_effective = set(
        tripdataset_bridge.schema_effective.domains_effective.get("purpose", {}).get("values", [])
    )

    has_finos_extension = len(purpose_effective.intersection(PURPOSE_FINOS)) > 0
    if not has_finos_extension:
        pytest.skip(
            "El dataset importado no quedó con extensión efectiva en purpose; "
            "el notebook también trata este caso como skip explícito."
        )

    data_before_validate = tripdataset_bridge.data.copy(deep=True)

    report = validate_trips(
        tripdataset_bridge,
        options=ValidationOptions(
            strict=False,
            validate_required_fields=True,
            validate_types_and_formats=True,
            validate_constraints=True,
            validate_domains="full",
            validate_temporal_consistency=True,
            validate_duplicates=False,
            allow_partial_od_spatial=True,
        ),
    )

    assert isinstance(report, ValidationReport)
    assert report.ok is True
    assert report.summary["ok"] is True

    assert_issue_absent(report.issues, "VAL.DOMAIN.RATIO_BELOW_MIN")

    assert tripdataset_bridge.metadata["is_validated"] is True
    assert len(tripdataset_bridge.metadata["events"]) == 2
    assert tripdataset_bridge.metadata["events"][0]["op"] == "import_trips"
    assert tripdataset_bridge.metadata["events"][-1]["op"] == "validate_trips"

    assert_validate_event_matches_report(tripdataset_bridge, report)

    pd.testing.assert_frame_equal(
        tripdataset_bridge.data,
        data_before_validate,
        check_dtype=True,
        check_like=False,
    )