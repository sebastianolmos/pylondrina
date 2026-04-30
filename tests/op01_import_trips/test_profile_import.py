import pandas as pd

from pylondrina.datasets import TripDataset
from pylondrina.importing import ImportOptions
from pylondrina.reports import ImportReport
from pylondrina.schema import FieldSpec, TripSchema
from pylondrina.sources.helpers import import_trips_from_profile
from pylondrina.sources.profile import SourceProfile


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


def issue_codes(report: ImportReport) -> list[str]:
    return [issue.code for issue in report.issues]


def assert_issue_present(report: ImportReport, code: str) -> None:
    codes = issue_codes(report)
    assert code in codes, f"No se encontró issue {code!r}. Issues encontrados: {codes!r}"


def assert_issue_absent(report: ImportReport, code: str) -> None:
    codes = issue_codes(report)
    assert code not in codes, f"No se esperaba issue {code!r}. Issues encontrados: {codes!r}"


def assert_series_equal_ignoring_name(left: pd.Series, right: pd.Series) -> None:
    pd.testing.assert_series_equal(
        left.reset_index(drop=True),
        right.reset_index(drop=True),
        check_names=False,
    )


def assert_generated_movement_ids(series: pd.Series, expected_len: int) -> None:
    assert len(series) == expected_len
    assert series.notna().all()
    assert series.is_unique
    assert series.astype(str).str.match(r"^m\d+$").all()


def make_profile_import_schema() -> TripSchema:
    return TripSchema(
        version="test-0.1",
        fields={
            "origin_longitude": FieldSpec(
                name="origin_longitude",
                dtype="float",
                required=True,
            ),
            "origin_latitude": FieldSpec(
                name="origin_latitude",
                dtype="float",
                required=True,
            ),
            "destination_longitude": FieldSpec(
                name="destination_longitude",
                dtype="float",
                required=True,
            ),
            "destination_latitude": FieldSpec(
                name="destination_latitude",
                dtype="float",
                required=True,
            ),
            "trip_id": FieldSpec(
                name="trip_id",
                dtype="string",
                required=False,
            ),
            "movement_id": FieldSpec(
                name="movement_id",
                dtype="string",
                required=False,
            ),
            "movement_seq": FieldSpec(
                name="movement_seq",
                dtype="int",
                required=False,
            ),
        },
        required=[
            "origin_longitude",
            "origin_latitude",
            "destination_longitude",
            "destination_latitude",
        ],
        semantic_rules=None,
    )


# ---------------------------------------------------------------------
# Tests de import_trips_from_profile
# ---------------------------------------------------------------------


def test_import_trips_from_profile_direct_profile_with_default_field_correspondence():
    """Verifica importación desde SourceProfile simple, usando default_field_correspondence y options del perfil."""
    schema = make_profile_import_schema()

    df = pd.DataFrame(
        {
            "x_o": [-70.65, -70.66],
            "y_o": [-33.45, -33.44],
            "x_d": [-70.61, -70.62],
            "y_d": [-33.41, -33.42],
            "id_viaje": ["v1", "v2"],
        }
    )

    default_field_correspondence = {
        "origin_longitude": "x_o",
        "origin_latitude": "y_o",
        "destination_longitude": "x_d",
        "destination_latitude": "y_d",
        "trip_id": "id_viaje",
    }

    profile = SourceProfile(
        name="TEST_SIMPLE",
        description="Perfil simple de prueba",
        default_field_correspondence=default_field_correspondence,
        default_options=ImportOptions(
            keep_extra_fields=True,
            strict=False,
            strict_domains=False,
            single_stage=False,
        ),
    )

    dataset, report = import_trips_from_profile(
        profile=profile,
        df=df,
        schema=schema,
    )

    assert isinstance(dataset, TripDataset)
    assert isinstance(report, ImportReport)
    assert report.ok is True

    assert_columns_equal(
        dataset.data,
        [
            "movement_id",
            "origin_longitude",
            "origin_latitude",
            "destination_longitude",
            "destination_latitude",
            "trip_id",
            "origin_h3_index",
            "destination_h3_index",
        ],
        "profile simple columns",
    )

    assert_generated_movement_ids(dataset.data["movement_id"], expected_len=len(df))

    assert_series_equal_ignoring_name(dataset.data["origin_longitude"], df["x_o"])
    assert_series_equal_ignoring_name(dataset.data["origin_latitude"], df["y_o"])
    assert_series_equal_ignoring_name(dataset.data["destination_longitude"], df["x_d"])
    assert_series_equal_ignoring_name(dataset.data["destination_latitude"], df["y_d"])
    assert_series_equal_ignoring_name(dataset.data["trip_id"], df["id_viaje"].astype("string"))

    assert dataset.data["origin_h3_index"].notna().all()
    assert dataset.data["destination_h3_index"].notna().all()

    assert_dtype(dataset.data, "movement_id", "string")
    assert_dtype(dataset.data, "origin_longitude", "float64")
    assert_dtype(dataset.data, "origin_latitude", "float64")
    assert_dtype(dataset.data, "destination_longitude", "float64")
    assert_dtype(dataset.data, "destination_latitude", "float64")
    assert_dtype(dataset.data, "trip_id", "string")
    assert_dtype(dataset.data, "origin_h3_index", "string")
    assert_dtype(dataset.data, "destination_h3_index", "string")

    assert report.field_correspondence == default_field_correspondence
    assert dataset.field_correspondence == default_field_correspondence
    assert report.value_correspondence == {}
    assert dataset.value_correspondence == {}

    assert report.summary["rows_in"] == len(df)
    assert report.summary["rows_out"] == len(df)
    assert report.summary["n_fields_mapped"] == len(default_field_correspondence)
    assert report.summary["n_domain_mappings_applied"] == 0

    assert dataset.provenance == {
        "source_profile": {
            "name": profile.name,
            "description": profile.description,
        }
    }

    assert dataset.metadata["provenance"] == dataset.provenance
    assert dataset.metadata["mappings"]["field_correspondence"] == default_field_correspondence
    assert dataset.metadata["mappings"]["value_correspondence"] == {}
    assert dataset.metadata["is_validated"] is False
    assert dataset.metadata["temporal"]["tier"] == "tier_3"

    event = dataset.metadata["events"][-1]
    assert event["op"] == "import_trips"
    assert event["parameters"]["source_name"] == profile.name
    assert event["parameters"]["keep_extra_fields"] is True
    assert event["parameters"]["single_stage"] is False
    assert event["summary"]["input_rows"] == len(df)
    assert event["summary"]["output_rows"] == len(df)
    assert event["summary"]["n_fields_mapped"] == len(default_field_correspondence)

    assert_issue_present(report, "IMP.ID.MOVEMENT_ID_CREATED")
    assert_issue_present(report, "IMP.TEMPORAL.TIER_LIMITED")


def test_import_trips_from_profile_with_preprocess_closure():
    """Verifica que SourceProfile pueda aplicar preprocess antes del import y conservar provenance del perfil."""
    schema = make_profile_import_schema()

    df_raw = pd.DataFrame(
        {
            "ox": [-70.65],
            "oy": [-33.45],
            "dx": [-70.61],
            "dy": [-33.41],
            "viaje": ["v100"],
        }
    )

    def preprocess(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out["trip_id"] = out["viaje"]
        out["movement_id"] = out["viaje"]
        out["movement_seq"] = 0
        return out

    df_preprocessed = preprocess(df_raw)

    default_field_correspondence = {
        "origin_longitude": "ox",
        "origin_latitude": "oy",
        "destination_longitude": "dx",
        "destination_latitude": "dy",
        "trip_id": "trip_id",
        "movement_id": "movement_id",
        "movement_seq": "movement_seq",
    }

    expected_applied_field_correspondence = {
        "origin_longitude": "ox",
        "origin_latitude": "oy",
        "destination_longitude": "dx",
        "destination_latitude": "dy",
    }

    profile = SourceProfile(
        name="TEST_WITH_PREPROCESS",
        description="Perfil con preprocess de prueba",
        default_field_correspondence=default_field_correspondence,
        preprocess=preprocess,
    )

    dataset, report = import_trips_from_profile(
        profile=profile,
        df=df_raw,
        schema=schema,
    )

    assert isinstance(dataset, TripDataset)
    assert isinstance(report, ImportReport)
    assert report.ok is True

    assert_columns_equal(
        dataset.data,
        [
            "origin_longitude",
            "origin_latitude",
            "destination_longitude",
            "destination_latitude",
            "viaje",
            "trip_id",
            "movement_id",
            "movement_seq",
            "origin_h3_index",
            "destination_h3_index",
        ],
        "profile preprocess columns",
    )

    assert_series_equal_ignoring_name(dataset.data["origin_longitude"], df_preprocessed["ox"])
    assert_series_equal_ignoring_name(dataset.data["origin_latitude"], df_preprocessed["oy"])
    assert_series_equal_ignoring_name(dataset.data["destination_longitude"], df_preprocessed["dx"])
    assert_series_equal_ignoring_name(dataset.data["destination_latitude"], df_preprocessed["dy"])
    assert_series_equal_ignoring_name(dataset.data["viaje"], df_preprocessed["viaje"])
    assert_series_equal_ignoring_name(dataset.data["trip_id"], df_preprocessed["trip_id"].astype("string"))
    assert_series_equal_ignoring_name(dataset.data["movement_id"], df_preprocessed["movement_id"].astype("string"))
    assert_series_equal_ignoring_name(dataset.data["movement_seq"], df_preprocessed["movement_seq"].astype("Int64"))

    assert dataset.data["origin_h3_index"].notna().all()
    assert dataset.data["destination_h3_index"].notna().all()

    assert_dtype(dataset.data, "origin_longitude", "float64")
    assert_dtype(dataset.data, "origin_latitude", "float64")
    assert_dtype(dataset.data, "destination_longitude", "float64")
    assert_dtype(dataset.data, "destination_latitude", "float64")
    assert_dtype(dataset.data, "trip_id", "string")
    assert_dtype(dataset.data, "movement_id", "string")
    assert_dtype(dataset.data, "movement_seq", "Int64")
    assert_dtype(dataset.data, "origin_h3_index", "string")
    assert_dtype(dataset.data, "destination_h3_index", "string")

    # trip_id, movement_id y movement_seq no aparecen como mappings aplicados
    # porque preprocess ya los deja como columnas canónicas.
    assert report.field_correspondence == expected_applied_field_correspondence
    assert dataset.field_correspondence == expected_applied_field_correspondence
    assert report.value_correspondence == {}
    assert dataset.value_correspondence == {}

    assert report.summary["rows_in"] == len(df_raw)
    assert report.summary["rows_out"] == len(df_raw)
    assert report.summary["n_fields_mapped"] == len(expected_applied_field_correspondence)
    assert report.summary["n_domain_mappings_applied"] == 0

    assert dataset.provenance == {
        "source_profile": {
            "name": profile.name,
            "description": profile.description,
        }
    }

    assert dataset.metadata["provenance"] == dataset.provenance
    assert dataset.metadata["mappings"]["field_correspondence"] == expected_applied_field_correspondence
    assert dataset.metadata["mappings"]["value_correspondence"] == {}
    assert dataset.metadata["extra_fields_kept"] == [
        "viaje",
        "origin_h3_index",
        "destination_h3_index",
    ]
    assert dataset.metadata["is_validated"] is False
    assert dataset.metadata["temporal"]["tier"] == "tier_3"

    event = dataset.metadata["events"][-1]
    assert event["op"] == "import_trips"
    assert event["parameters"]["source_name"] == profile.name
    assert event["parameters"]["keep_extra_fields"] is True
    assert event["summary"]["input_rows"] == len(df_raw)
    assert event["summary"]["output_rows"] == len(df_raw)
    assert event["summary"]["n_fields_mapped"] == len(expected_applied_field_correspondence)

    assert_issue_absent(report, "IMP.ID.MOVEMENT_ID_CREATED")
    assert_issue_absent(report, "IMP.ID.TRIP_ID_CREATED")
    assert_issue_absent(report, "IMP.ID.MOVEMENT_SEQ_CREATED")
    assert_issue_present(report, "IMP.TEMPORAL.TIER_LIMITED")