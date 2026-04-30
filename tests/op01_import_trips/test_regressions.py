import pandas as pd

from pylondrina.datasets import TripDataset
from pylondrina.importing import ImportOptions, import_trips_from_dataframe
from pylondrina.reports import ImportReport
from pylondrina.schema import FieldSpec, TripSchema


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
# Fixtures locales
# ---------------------------------------------------------------------


def make_spatial_schema_without_ids(version: str = "test-1") -> TripSchema:
    return TripSchema(
        version=version,
        fields={
            "user_id": FieldSpec("user_id", "string", required=True),
            "origin_longitude": FieldSpec("origin_longitude", "float", required=True),
            "origin_latitude": FieldSpec("origin_latitude", "float", required=True),
            "destination_longitude": FieldSpec("destination_longitude", "float", required=True),
            "destination_latitude": FieldSpec("destination_latitude", "float", required=True),
        },
        required=[
            "user_id",
            "origin_longitude",
            "origin_latitude",
            "destination_longitude",
            "destination_latitude",
        ],
    )


def make_spatial_schema_with_movement_id(version: str = "test-2") -> TripSchema:
    return TripSchema(
        version=version,
        fields={
            "movement_id": FieldSpec("movement_id", "string", required=True),
            "user_id": FieldSpec("user_id", "string", required=True),
            "origin_longitude": FieldSpec("origin_longitude", "float", required=True),
            "origin_latitude": FieldSpec("origin_latitude", "float", required=True),
            "destination_longitude": FieldSpec("destination_longitude", "float", required=True),
            "destination_latitude": FieldSpec("destination_latitude", "float", required=True),
        },
        required=[
            "movement_id",
            "user_id",
            "origin_longitude",
            "origin_latitude",
            "destination_longitude",
            "destination_latitude",
        ],
    )


def make_spatial_df_without_ids() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "user_id": ["u1", "u2"],
            "origin_longitude": [-70.65, -70.66],
            "origin_latitude": [-33.45, -33.46],
            "destination_longitude": [-70.61, -70.62],
            "destination_latitude": [-33.41, -33.42],
        }
    )


def make_single_stage_options() -> ImportOptions:
    return ImportOptions(
        keep_extra_fields=False,
        selected_fields=None,
        strict=False,
        strict_domains=False,
        single_stage=True,
        source_timezone=None,
    )


def make_non_single_stage_options() -> ImportOptions:
    return ImportOptions(
        keep_extra_fields=False,
        selected_fields=None,
        strict=False,
        strict_domains=False,
        single_stage=False,
        source_timezone=None,
    )


# ---------------------------------------------------------------------
# Regression tests: single_stage=True
# ---------------------------------------------------------------------


def test_regression_single_stage_true_generates_runtime_ids_even_if_ids_are_not_in_schema():
    """Verifica que single_stage=True genere movement_id, trip_id y movement_seq aunque esos campos no estén declarados en el schema."""
    schema = make_spatial_schema_without_ids(version="test-1")
    df = make_spatial_df_without_ids()
    opts = make_single_stage_options()

    tripds, report = import_trips_from_dataframe(
        df,
        schema=schema,
        source_name="test_single_stage_no_ids_in_schema",
        options=opts,
        h3_resolution=8,
    )

    assert isinstance(tripds, TripDataset)
    assert isinstance(report, ImportReport)
    assert report.ok is True

    assert_columns_equal(
        tripds.data,
        [
            "movement_id",
            "trip_id",
            "movement_seq",
            "user_id",
            "origin_longitude",
            "origin_latitude",
            "destination_longitude",
            "destination_latitude",
        ],
        "single_stage=True sin IDs en schema",
    )

    assert tripds.data["movement_id"].tolist() == ["m0", "m1"]
    assert tripds.data["trip_id"].tolist() == ["m0", "m1"]
    assert tripds.data["movement_seq"].tolist() == [0, 0]

    assert_dtype(tripds.data, "movement_id", "string")
    assert_dtype(tripds.data, "trip_id", "string")
    assert_dtype(tripds.data, "movement_seq", "Int64")

    assert tripds.metadata["is_validated"] is False
    assert tripds.metadata["events"][0]["op"] == "import_trips"

    assert_issue_present(report.issues, "IMP.ID.MOVEMENT_ID_CREATED")
    assert_issue_present(report.issues, "IMP.ID.TRIP_ID_CREATED")
    assert_issue_present(report.issues, "IMP.ID.MOVEMENT_SEQ_CREATED")
    assert_issue_absent(report.issues, "IMP.INPUT.MISSING_REQUIRED_FIELD")


def test_regression_single_stage_true_preserves_mapped_movement_id_and_derives_trip_fields():
    """Verifica que single_stage=True respete movement_id mapeado por field_correspondence y derive trip_id/movement_seq desde él."""
    schema = make_spatial_schema_with_movement_id(version="test-2")

    df = pd.DataFrame(
        {
            "Viaje": ["v1", "v2"],
            "Persona": ["p1", "p2"],
            "origin_longitude": [-70.65, -70.66],
            "origin_latitude": [-33.45, -33.46],
            "destination_longitude": [-70.61, -70.62],
            "destination_latitude": [-33.41, -33.42],
        }
    )

    opts = make_single_stage_options()

    field_corr = {
        "movement_id": "Viaje",
        "user_id": "Persona",
    }

    tripds, report = import_trips_from_dataframe(
        df,
        schema=schema,
        source_name="test_single_stage_with_movement_mapping",
        options=opts,
        field_correspondence=field_corr,
        h3_resolution=8,
    )

    assert isinstance(tripds, TripDataset)
    assert isinstance(report, ImportReport)
    assert report.ok is True

    assert_columns_equal(
        tripds.data,
        [
            "movement_id",
            "trip_id",
            "movement_seq",
            "user_id",
            "origin_longitude",
            "origin_latitude",
            "destination_longitude",
            "destination_latitude",
        ],
        "single_stage=True con movement_id mapeado",
    )

    assert tripds.data["movement_id"].tolist() == ["v1", "v2"]
    assert tripds.data["trip_id"].tolist() == ["v1", "v2"]
    assert tripds.data["movement_seq"].tolist() == [0, 0]
    assert tripds.data["user_id"].tolist() == ["p1", "p2"]

    assert_dtype(tripds.data, "movement_id", "string")
    assert_dtype(tripds.data, "trip_id", "string")
    assert_dtype(tripds.data, "movement_seq", "Int64")
    assert_dtype(tripds.data, "user_id", "string")

    assert tripds.field_correspondence == field_corr
    assert report.field_correspondence == field_corr

    assert_issue_absent(report.issues, "IMP.ID.MOVEMENT_ID_CREATED")
    assert_issue_present(report.issues, "IMP.ID.TRIP_ID_CREATED")
    assert_issue_present(report.issues, "IMP.ID.MOVEMENT_SEQ_CREATED")
    assert_issue_absent(report.issues, "IMP.INPUT.MISSING_REQUIRED_FIELD")


def test_regression_single_stage_false_does_not_force_trip_id_or_movement_seq():
    """Verifica que single_stage=False no fuerce trip_id ni movement_seq cuando el schema no los declara."""
    schema = make_spatial_schema_without_ids(version="test-3")
    df = make_spatial_df_without_ids()
    opts = make_non_single_stage_options()

    tripds, report = import_trips_from_dataframe(
        df,
        schema=schema,
        source_name="test_non_single_stage",
        options=opts,
        h3_resolution=8,
    )

    assert isinstance(tripds, TripDataset)
    assert isinstance(report, ImportReport)
    assert report.ok is True

    assert_columns_equal(
        tripds.data,
        [
            "movement_id",
            "user_id",
            "origin_longitude",
            "origin_latitude",
            "destination_longitude",
            "destination_latitude",
        ],
        "single_stage=False no fuerza campos de viaje",
    )

    assert tripds.data["movement_id"].tolist() == ["m0", "m1"]
    assert "trip_id" not in tripds.data.columns
    assert "movement_seq" not in tripds.data.columns

    assert_dtype(tripds.data, "movement_id", "string")

    assert_issue_present(report.issues, "IMP.ID.MOVEMENT_ID_CREATED")
    assert_issue_absent(report.issues, "IMP.ID.TRIP_ID_CREATED")
    assert_issue_absent(report.issues, "IMP.ID.MOVEMENT_SEQ_CREATED")
    assert_issue_absent(report.issues, "IMP.INPUT.MISSING_REQUIRED_FIELD")