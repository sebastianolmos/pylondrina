import pandas as pd
import pytest

from pylondrina.datasets import TripDataset
from pylondrina.errors import ImportError as PylondrinaImportError
from pylondrina.importing import ImportOptions, import_trips_from_dataframe
from pylondrina.reports import ImportReport
from pylondrina.schema import DomainSpec, FieldSpec, TripSchema


# ---------------------------------------------------------------------
# Helpers locales de test
# ---------------------------------------------------------------------


def assert_columns_equal(
    df: pd.DataFrame,
    expected_columns: list[str],
    label: str = "columns",
) -> None:
    assert list(df.columns) == expected_columns, f"{label}: columnas inesperadas"


def assert_dtype(df: pd.DataFrame, column: str, expected_dtype: str) -> None:
    assert str(df[column].dtype) == expected_dtype


def issue_codes(issues) -> list[str]:
    return [issue.code for issue in issues]


def assert_issue_present(issues, code: str) -> None:
    codes = issue_codes(issues)
    assert code in codes, f"No se encontró issue {code!r}. Issues encontrados: {codes!r}"


def assert_issue_absent(issues, code: str) -> None:
    codes = issue_codes(issues)
    assert code not in codes, f"No se esperaba issue {code!r}. Issues encontrados: {codes!r}"


# ---------------------------------------------------------------------
# Smoke tests públicos de import_trips_from_dataframe
# ---------------------------------------------------------------------


def test_public_smoke_simple_happy_path_creates_tripdataset_and_report():
    """Verifica un import mínimo exitoso y la creación automática de movement_id."""
    schema = TripSchema(
        version="0.1.0",
        fields={
            "user_id": FieldSpec(name="user_id", dtype="string", required=True),
            "purpose": FieldSpec(
                name="purpose",
                dtype="categorical",
                required=False,
                domain=DomainSpec(
                    values=["work", "study", "unknown"],
                    extendable=False,
                    aliases=None,
                ),
            ),
            "movement_id": FieldSpec(name="movement_id", dtype="string", required=False),
        },
        required=["user_id"],
        semantic_rules=None,
    )

    df = pd.DataFrame(
        {
            "user_id": ["u1", "u2", "u3"],
            "purpose": ["work", "study", "work"],
        }
    )

    tripds, report = import_trips_from_dataframe(
        df,
        schema,
        source_name="smoke_simple",
        options=ImportOptions(strict=False, strict_domains=False, single_stage=False),
    )

    assert isinstance(tripds, TripDataset)
    assert isinstance(report, ImportReport)
    assert report.ok is True

    assert_columns_equal(
        tripds.data,
        ["movement_id", "user_id", "purpose"],
        "smoke simple columns",
    )

    assert tripds.data["movement_id"].tolist() == ["m0", "m1", "m2"]
    assert tripds.data["user_id"].tolist() == ["u1", "u2", "u3"]
    assert tripds.data["purpose"].tolist() == ["work", "study", "work"]

    assert_dtype(tripds.data, "movement_id", "string")
    assert_dtype(tripds.data, "user_id", "string")
    assert_dtype(tripds.data, "purpose", "string")

    assert tripds.metadata["is_validated"] is False
    assert tripds.metadata["temporal"]["tier"] == "tier_3"
    assert tripds.metadata["events"][0]["op"] == "import_trips"

    assert report.summary["rows_in"] == 3
    assert report.summary["rows_out"] == 3
    assert report.summary["n_fields_mapped"] == 0
    assert report.summary["n_domain_mappings_applied"] == 0

    assert_issue_present(report.issues, "IMP.ID.MOVEMENT_ID_CREATED")
    assert_issue_present(report.issues, "IMP.TEMPORAL.TIER_LIMITED")


def test_public_smoke_with_field_and_value_correspondence_keeps_extra_fields():
    """Verifica un import público con field_correspondence, value_correspondence y preservación de extras."""
    schema = TripSchema(
        version="0.1.0",
        fields={
            "user_id": FieldSpec(name="user_id", dtype="string", required=True),
            "purpose": FieldSpec(
                name="purpose",
                dtype="categorical",
                required=False,
                domain=DomainSpec(
                    values=["work", "study", "unknown"],
                    extendable=False,
                    aliases=None,
                ),
            ),
            "mode": FieldSpec(
                name="mode",
                dtype="categorical",
                required=False,
                domain=DomainSpec(
                    values=["bus", "metro", "walk", "unknown"],
                    extendable=False,
                    aliases=None,
                ),
            ),
            "movement_id": FieldSpec(name="movement_id", dtype="string", required=False),
        },
        required=["user_id"],
        semantic_rules=None,
    )

    df = pd.DataFrame(
        {
            "uid": ["u1", "u2"],
            "motivo": ["trabajo", "estudio"],
            "modo": ["autobus", "metro"],
            "raw_extra": ["A", "B"],
        }
    )

    field_correspondence = {
        "user_id": "uid",
        "purpose": "motivo",
        "mode": "modo",
    }
    value_correspondence = {
        "purpose": {"trabajo": "work", "estudio": "study"},
        "mode": {"autobus": "bus"},
    }

    tripds, report = import_trips_from_dataframe(
        df,
        schema,
        source_name="smoke_corr",
        options=ImportOptions(strict=False, strict_domains=False, keep_extra_fields=True),
        field_correspondence=field_correspondence,
        value_correspondence=value_correspondence,
    )

    assert isinstance(tripds, TripDataset)
    assert isinstance(report, ImportReport)
    assert report.ok is True

    assert_columns_equal(
        tripds.data,
        ["movement_id", "user_id", "purpose", "mode", "raw_extra"],
        "smoke correspondences columns",
    )

    assert tripds.data["movement_id"].tolist() == ["m0", "m1"]
    assert tripds.data["user_id"].tolist() == ["u1", "u2"]
    assert tripds.data["purpose"].tolist() == ["work", "study"]
    assert tripds.data["mode"].tolist() == ["bus", "metro"]
    assert tripds.data["raw_extra"].tolist() == ["A", "B"]

    assert tripds.field_correspondence == field_correspondence
    assert report.field_correspondence == field_correspondence
    assert tripds.value_correspondence == value_correspondence
    assert report.value_correspondence == value_correspondence

    assert report.summary["n_fields_mapped"] == 3
    assert report.summary["n_domain_mappings_applied"] == 3
    assert tripds.metadata["extra_fields_kept"] == ["raw_extra"]

    assert_issue_present(report.issues, "IMP.ID.MOVEMENT_ID_CREATED")


def test_public_smoke_extendable_domain_adds_out_of_domain_value():
    """Verifica que un dominio categórico extendible incorpore valores observados fuera del dominio base."""
    schema = TripSchema(
        version="0.1.0",
        fields={
            "user_id": FieldSpec(name="user_id", dtype="string", required=True),
            "purpose": FieldSpec(
                name="purpose",
                dtype="categorical",
                required=False,
                domain=DomainSpec(values=["work", "study"], extendable=True, aliases=None),
            ),
            "movement_id": FieldSpec(name="movement_id", dtype="string", required=False),
        },
        required=["user_id"],
        semantic_rules=None,
    )

    df = pd.DataFrame(
        {
            "user_id": ["u1", "u2", "u3"],
            "purpose": ["work", "home", "study"],
        }
    )

    tripds, report = import_trips_from_dataframe(
        df,
        schema,
        source_name="smoke_extend",
        options=ImportOptions(strict=False, strict_domains=False),
    )

    assert isinstance(tripds, TripDataset)
    assert isinstance(report, ImportReport)
    assert report.ok is True

    assert_columns_equal(
        tripds.data,
        ["movement_id", "user_id", "purpose"],
        "smoke extend columns",
    )
    assert tripds.data["purpose"].tolist() == ["work", "home", "study"]

    assert "purpose" in tripds.metadata["domains_effective"]
    assert tripds.metadata["domains_effective"]["purpose"]["extended"] is True
    assert tripds.metadata["domains_effective"]["purpose"]["added_values"] == ["home"]
    assert "home" in tripds.metadata["domains_effective"]["purpose"]["values"]
    assert "purpose" in tripds.metadata["domains_extended"]

    assert_issue_present(report.issues, "DOM.EXTENSION.APPLIED")
    assert_issue_present(report.issues, "IMP.ID.MOVEMENT_ID_CREATED")


def test_public_smoke_missing_required_field_raises_import_error():
    """Verifica que la API pública aborte cuando falta un campo requerido no derivable."""
    schema = TripSchema(
        version="0.1.0",
        fields={
            "user_id": FieldSpec(name="user_id", dtype="string", required=True),
            "purpose": FieldSpec(name="purpose", dtype="string", required=False),
        },
        required=["user_id"],
        semantic_rules=None,
    )

    df = pd.DataFrame(
        {
            "purpose": ["work", "study"],
        }
    )

    with pytest.raises(PylondrinaImportError) as exc_info:
        import_trips_from_dataframe(
            df,
            schema,
            source_name="smoke_missing",
            options=ImportOptions(strict=False),
        )

    assert_issue_present(exc_info.value.issues, "IMP.INPUT.MISSING_REQUIRED_FIELD")

    issue = exc_info.value.issue
    assert issue.code == "IMP.INPUT.MISSING_REQUIRED_FIELD"
    assert issue.details["missing_required"] == ["user_id"]
    assert issue.details["source_columns"] == ["purpose"]


def test_public_smoke_tier2_hhmm_normalization_records_temporal_metadata():
    """Verifica import Tier 2 con HH:MM, normalización de inválidos a NA y metadata temporal."""
    schema = TripSchema(
        version="0.1.0",
        fields={
            "user_id": FieldSpec(name="user_id", dtype="string", required=True),
            "origin_time_local_hhmm": FieldSpec(
                name="origin_time_local_hhmm",
                dtype="string",
                required=False,
            ),
            "destination_time_local_hhmm": FieldSpec(
                name="destination_time_local_hhmm",
                dtype="string",
                required=False,
            ),
            "movement_id": FieldSpec(name="movement_id", dtype="string", required=False),
        },
        required=["user_id"],
        semantic_rules=None,
    )

    df = pd.DataFrame(
        {
            "user_id": ["u1", "u2", "u3"],
            "origin_time_local_hhmm": ["08:00", "24:00", "07:05"],
            "destination_time_local_hhmm": ["08:30", "13:00", "99:99"],
        }
    )

    tripds, report = import_trips_from_dataframe(
        df,
        schema,
        source_name="smoke_t2",
        options=ImportOptions(strict=False, single_stage=False),
    )

    assert isinstance(tripds, TripDataset)
    assert isinstance(report, ImportReport)
    assert report.ok is True

    assert_columns_equal(
        tripds.data,
        ["movement_id", "user_id", "origin_time_local_hhmm", "destination_time_local_hhmm"],
        "smoke tier2 columns",
    )

    assert tripds.metadata["temporal"]["tier"] == "tier_2"
    assert tripds.metadata["temporal"]["fields_present"] == [
        "origin_time_local_hhmm",
        "destination_time_local_hhmm",
    ]
    assert "normalization" in tripds.metadata["temporal"]
    assert tripds.metadata["temporal"]["normalization"]["origin_time_local_hhmm"]["n_invalid"] == 1
    assert tripds.metadata["temporal"]["normalization"]["destination_time_local_hhmm"]["n_invalid"] == 1

    assert tripds.data.loc[0, "origin_time_local_hhmm"] == "08:00"
    assert tripds.data.loc[0, "destination_time_local_hhmm"] == "08:30"
    assert tripds.data.loc[1, "destination_time_local_hhmm"] == "13:00"
    assert tripds.data.loc[2, "origin_time_local_hhmm"] == "07:05"

    assert pd.isna(tripds.data.loc[1, "origin_time_local_hhmm"])
    assert pd.isna(tripds.data.loc[2, "destination_time_local_hhmm"])

    assert_issue_present(report.issues, "IMP.TEMPORAL.TIER_LIMITED")
    assert_issue_present(report.issues, "IMP.TYPE.COERCE_PARTIAL")
    assert_issue_present(report.issues, "IMP.ID.MOVEMENT_ID_CREATED")


def test_public_smoke_single_stage_generates_trip_id_and_movement_seq():
    """Verifica que single_stage=True complete movement_id, trip_id y movement_seq en la API pública."""
    schema = TripSchema(
        version="0.1.0",
        fields={
            "user_id": FieldSpec(name="user_id", dtype="string", required=True),
            "movement_id": FieldSpec(name="movement_id", dtype="string", required=True),
            "trip_id": FieldSpec(name="trip_id", dtype="string", required=True),
            "movement_seq": FieldSpec(name="movement_seq", dtype="int", required=True),
            "purpose": FieldSpec(name="purpose", dtype="string", required=False),
        },
        required=["user_id", "movement_id", "trip_id", "movement_seq"],
        semantic_rules=None,
    )

    df = pd.DataFrame(
        {
            "user_id": ["u1", "u2"],
            "purpose": ["work", "study"],
        }
    )

    tripds, report = import_trips_from_dataframe(
        df,
        schema,
        source_name="smoke_single",
        options=ImportOptions(strict=False, single_stage=True),
    )

    assert isinstance(tripds, TripDataset)
    assert isinstance(report, ImportReport)
    assert report.ok is True

    assert_columns_equal(
        tripds.data,
        ["movement_id", "trip_id", "movement_seq", "user_id", "purpose"],
        "smoke single_stage columns",
    )

    assert tripds.data["movement_id"].tolist() == ["m0", "m1"]
    assert tripds.data["trip_id"].tolist() == ["m0", "m1"]
    assert tripds.data["movement_seq"].tolist() == [0, 0]
    assert tripds.data["user_id"].tolist() == ["u1", "u2"]
    assert tripds.data["purpose"].tolist() == ["work", "study"]

    assert_dtype(tripds.data, "movement_id", "string")
    assert_dtype(tripds.data, "trip_id", "string")
    assert_dtype(tripds.data, "movement_seq", "Int64")
    assert_dtype(tripds.data, "user_id", "string")
    assert_dtype(tripds.data, "purpose", "string")

    assert_issue_present(report.issues, "IMP.ID.MOVEMENT_ID_CREATED")
    assert_issue_present(report.issues, "IMP.ID.TRIP_ID_CREATED")
    assert_issue_present(report.issues, "IMP.ID.MOVEMENT_SEQ_CREATED")
    assert_issue_absent(report.issues, "IMP.INPUT.MISSING_REQUIRED_FIELD")