from pathlib import Path

import pandas as pd
import pytest

from pylondrina.datasets import TripDataset
from pylondrina.errors import ImportError as PylondrinaImportError
from pylondrina.errors import SchemaError
from pylondrina.importing import ImportOptions, import_trips_from_dataframe
from pylondrina.reports import ImportReport
from pylondrina.schema import DomainSpec, FieldSpec, TripSchema


base_generator = pytest.importorskip("scripts.synthetic_data.base_generator")
gen_wrappers = pytest.importorskip("scripts.synthetic_data.gen_wrappers")

generate_synthetic_trip_dataframe = base_generator.generate_synthetic_trip_dataframe

make_happy_path_minimal = gen_wrappers.make_happy_path_minimal
make_happy_path_rich = gen_wrappers.make_happy_path_rich
make_h3_derivable = gen_wrappers.make_h3_derivable
make_tier2_valid = gen_wrappers.make_tier2_valid
make_tier2_mixed_invalid = gen_wrappers.make_tier2_mixed_invalid
make_extended_domains = gen_wrappers.make_extended_domains
make_missing_required = gen_wrappers.make_missing_required
make_duplicate_movement_id = gen_wrappers.make_duplicate_movement_id


# ---------------------------------------------------------------------
# Schemas, options y fixtures globales
# ---------------------------------------------------------------------


BASE_TRIP_SCHEMA = TripSchema(
    version="1.1",
    fields={
        "movement_id": FieldSpec("movement_id", "string", required=True),
        "user_id": FieldSpec("user_id", "string", required=True),
        "origin_longitude": FieldSpec("origin_longitude", "float", required=True),
        "origin_latitude": FieldSpec("origin_latitude", "float", required=True),
        "destination_longitude": FieldSpec("destination_longitude", "float", required=True),
        "destination_latitude": FieldSpec("destination_latitude", "float", required=True),
        "origin_h3_index": FieldSpec("origin_h3_index", "string", required=True),
        "destination_h3_index": FieldSpec("destination_h3_index", "string", required=True),
        "origin_time_utc": FieldSpec("origin_time_utc", "datetime", required=False),
        "destination_time_utc": FieldSpec("destination_time_utc", "datetime", required=False),
        "trip_id": FieldSpec("trip_id", "string", required=True),
        "movement_seq": FieldSpec("movement_seq", "int", required=True),
        "mode": FieldSpec(
            "mode",
            "categorical",
            required=False,
            domain=DomainSpec(
                values=[
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
                ],
                extendable=True,
            ),
        ),
        "purpose": FieldSpec(
            "purpose",
            "categorical",
            required=False,
            domain=DomainSpec(
                values=[
                    "home",
                    "work",
                    "education",
                    "shopping",
                    "errand",
                    "health",
                    "leisure",
                    "transfer",
                    "other",
                ],
                extendable=True,
            ),
        ),
        "day_type": FieldSpec(
            "day_type",
            "categorical",
            required=False,
            domain=DomainSpec(values=["weekday", "weekend", "holiday"], extendable=True),
        ),
        "time_period": FieldSpec(
            "time_period",
            "categorical",
            required=False,
            domain=DomainSpec(
                values=["night", "morning", "midday", "afternoon", "evening"],
                extendable=True,
            ),
        ),
        "user_gender": FieldSpec(
            "user_gender",
            "categorical",
            required=False,
            domain=DomainSpec(values=["female", "male", "other", "unknown"], extendable=True),
        ),
        "origin_time_local_hhmm": FieldSpec("origin_time_local_hhmm", "string", required=False),
        "destination_time_local_hhmm": FieldSpec(
            "destination_time_local_hhmm",
            "string",
            required=False,
        ),
        "origin_municipality": FieldSpec("origin_municipality", "string", required=False),
        "destination_municipality": FieldSpec("destination_municipality", "string", required=False),
        "trip_weight": FieldSpec("trip_weight", "float", required=False),
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
)

NON_EXTENDABLE_DOMAIN_SCHEMA = TripSchema(
    version="1.1",
    fields={
        **BASE_TRIP_SCHEMA.fields,
        "mode": FieldSpec(
            "mode",
            "categorical",
            required=False,
            domain=DomainSpec(
                values=[
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
                    "unknown",
                ],
                extendable=False,
            ),
        ),
    },
    required=list(BASE_TRIP_SCHEMA.required),
)

COORD_PARTIAL_NO_H3_SCHEMA = TripSchema(
    version="1.1",
    fields={
        "movement_id": FieldSpec("movement_id", "string", required=True),
        "user_id": FieldSpec("user_id", "string", required=True),
        "origin_longitude": FieldSpec("origin_longitude", "float", required=True),
        "origin_latitude": FieldSpec("origin_latitude", "float", required=True),
        "destination_longitude": FieldSpec("destination_longitude", "float", required=True),
        "destination_latitude": FieldSpec("destination_latitude", "float", required=True),
        "origin_time_utc": FieldSpec("origin_time_utc", "datetime", required=False),
        "destination_time_utc": FieldSpec("destination_time_utc", "datetime", required=False),
        "trip_id": FieldSpec("trip_id", "string", required=True),
        "movement_seq": FieldSpec("movement_seq", "int", required=True),
        "mode": BASE_TRIP_SCHEMA.fields["mode"],
        "purpose": BASE_TRIP_SCHEMA.fields["purpose"],
    },
    required=[
        "movement_id",
        "user_id",
        "origin_longitude",
        "origin_latitude",
        "destination_longitude",
        "destination_latitude",
        "trip_id",
        "movement_seq",
    ],
)

H3_REQUIRED_NO_COORDS_SCHEMA = TripSchema(
    version="1.1",
    fields={
        "movement_id": FieldSpec("movement_id", "string", required=True),
        "user_id": FieldSpec("user_id", "string", required=True),
        "trip_id": FieldSpec("trip_id", "string", required=True),
        "movement_seq": FieldSpec("movement_seq", "int", required=True),
        "origin_latitude": FieldSpec("origin_latitude", "float", required=False),
        "origin_longitude": FieldSpec("origin_longitude", "float", required=False),
        "destination_latitude": FieldSpec("destination_latitude", "float", required=False),
        "destination_longitude": FieldSpec("destination_longitude", "float", required=False),
        "origin_h3_index": FieldSpec("origin_h3_index", "string", required=True),
        "destination_h3_index": FieldSpec("destination_h3_index", "string", required=True),
        "origin_time_utc": FieldSpec("origin_time_utc", "datetime", required=False),
        "destination_time_utc": FieldSpec("destination_time_utc", "datetime", required=False),
    },
    required=[
        "movement_id",
        "user_id",
        "trip_id",
        "movement_seq",
        "origin_h3_index",
        "destination_h3_index",
    ],
)

BASE_PROVENANCE = {
    "source": {
        "name": "synthetic",
        "entity": "trips",
        "version": "integration-tests-v1",
    },
    "notes": ["dataset sintético para tests de integración OP-01"],
}

FIELD_CORR_STANDARD = {
    "movement_id": "id_mov_fuente",
    "user_id": "id_persona_fuente",
    "origin_longitude": "lon_o_fuente",
    "origin_latitude": "lat_o_fuente",
    "destination_longitude": "lon_d_fuente",
    "destination_latitude": "lat_d_fuente",
    "origin_h3_index": "h3_o_fuente",
    "destination_h3_index": "h3_d_fuente",
    "origin_time_utc": "t_origen_fuente",
    "destination_time_utc": "t_destino_fuente",
    "trip_id": "id_viaje_fuente",
    "movement_seq": "seq_fuente",
    "mode": "modo_fuente",
    "purpose": "proposito_fuente",
}

VALUE_CORR_STANDARD = {
    "mode": {
        "A PIE": "walk",
        "BUS": "bus",
        "AUTO": "car",
        "METRO": "metro",
    },
    "purpose": {
        "TRABAJO": "work",
        "ESTUDIO": "education",
        "HOGAR": "home",
        "COMPRAS": "shopping",
    },
}

SCHEMA_OPTIONAL_PRESENT_IN_SOURCE = {
    "mode",
    "purpose",
    "day_type",
    "time_period",
    "user_gender",
    "trip_weight",
    "origin_municipality",
    "destination_municipality",
}

EXTRA_COLS_PRESENT_IN_SOURCE = {
    "household_id",
    "source_person_id",
    "stage_count",
    "activity_destination",
    "travel_time_min",
    "fare_amount",
}

SUBSET_SELECTED = ["mode", "purpose", "trip_weight"]


# ---------------------------------------------------------------------
# Helpers locales de test
# ---------------------------------------------------------------------


def issue_codes(report: ImportReport) -> list[str]:
    return [issue.code for issue in report.issues]


def assert_issue_code(report: ImportReport, expected_code: str) -> None:
    codes = issue_codes(report)
    assert expected_code in codes, (
        f"No se encontró el issue code esperado {expected_code!r}. "
        f"Codes presentes: {codes}"
    )


def assert_no_issue_code(report: ImportReport, forbidden_code: str) -> None:
    codes = issue_codes(report)
    assert forbidden_code not in codes, (
        f"Se encontró el issue code prohibido {forbidden_code!r}. "
        f"Codes presentes: {codes}"
    )


def assert_tripdataset_and_report(trips, report) -> None:
    assert isinstance(trips, TripDataset)
    assert isinstance(report, ImportReport)


def assert_common_contract(trips, report, expected_rows: int) -> None:
    assert_tripdataset_and_report(trips, report)
    assert report.ok is True

    assert isinstance(trips.metadata, dict)
    assert "dataset_id" in trips.metadata
    assert isinstance(trips.metadata["dataset_id"], str)
    assert trips.metadata["dataset_id"] != ""

    assert trips.metadata["is_validated"] is False

    assert "schema" in trips.metadata
    assert "schema_effective" in trips.metadata
    assert "events" in trips.metadata
    assert isinstance(trips.metadata["events"], list)
    assert len(trips.metadata["events"]) >= 1

    event = trips.metadata["events"][-1]
    assert event["op"] == "import_trips"
    assert isinstance(event["parameters"], dict)
    assert isinstance(event["summary"], dict)
    assert isinstance(event["issues_summary"], dict)
    assert isinstance(event["issues_summary"]["counts"], dict)
    assert isinstance(event["issues_summary"]["by_code"], dict)

    assert isinstance(report.issues, list)
    assert isinstance(report.summary, dict)
    assert isinstance(report.parameters, dict)
    assert isinstance(report.metadata, dict)

    assert report.metadata["dataset_id"] == trips.metadata["dataset_id"]
    assert report.metadata["summary"]["rows_out"] == expected_rows
    assert report.summary["rows_out"] == expected_rows


def make_rich_selection_dataframe(rows: int = 12) -> pd.DataFrame:
    return make_happy_path_rich(
        filas=rows,
        seed=42,
        base_fields=[
            "mode",
            "purpose",
            "day_type",
            "time_period",
            "user_gender",
            "trip_weight",
            "origin_municipality",
            "destination_municipality",
        ],
        extra_columns=[
            "household_id",
            "source_person_id",
            "stage_count",
            "activity_destination",
            "travel_time_min",
            "fare_amount",
        ],
    )


def get_synthetic_data_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "synthetic"


def read_synthetic_csv_or_skip(filename: str) -> pd.DataFrame:
    path = get_synthetic_data_path() / filename
    if not path.exists():
        pytest.skip(f"No existe el archivo sintético requerido por el notebook: {path}")
    return pd.read_csv(path)


# ---------------------------------------------------------------------
# Sección 2 - Caminos felices mínimos
# ---------------------------------------------------------------------


def test_integration_happy_path_canonical_minimal():
    """Verifica el happy path canónico mínimo con salida tabular, reporte y trazabilidad básica."""
    df = make_happy_path_minimal(filas=6, seed=42)

    trips, report = import_trips_from_dataframe(
        df,
        schema=BASE_TRIP_SCHEMA,
        source_name="synthetic_a1",
        options=ImportOptions(),
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    assert_tripdataset_and_report(trips, report)
    assert report.ok is True

    assert len(trips.data) == 6

    for col in [
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
    ]:
        assert col in trips.data.columns

    assert report.field_correspondence == {}
    assert report.value_correspondence == {}

    assert "dataset_id" in trips.metadata
    assert trips.metadata["is_validated"] is False
    assert trips.metadata["events"][-1]["op"] == "import_trips"
    assert "schema_effective" in trips.metadata
    assert trips.metadata["temporal"]["tier"] == "tier_1"


def test_integration_happy_path_with_field_and_value_correspondence():
    """Verifica importación con correspondencia de campos y valores categóricos deterministas."""
    df = make_happy_path_minimal(
        filas=6,
        seed=42,
        base_fields=["mode", "purpose"],
        field_correspondence=FIELD_CORR_STANDARD,
    )

    df["modo_fuente"] = ["A PIE", "BUS", "AUTO", "METRO", "A PIE", "BUS"]
    df["proposito_fuente"] = ["TRABAJO", "ESTUDIO", "HOGAR", "COMPRAS", "TRABAJO", "HOGAR"]

    trips, report = import_trips_from_dataframe(
        df,
        schema=BASE_TRIP_SCHEMA,
        source_name="synthetic_a2",
        options=ImportOptions(),
        field_correspondence=FIELD_CORR_STANDARD,
        value_correspondence=VALUE_CORR_STANDARD,
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    assert report.ok is True
    assert "mode" in trips.data.columns
    assert "purpose" in trips.data.columns
    assert "modo_fuente" not in trips.data.columns
    assert "proposito_fuente" not in trips.data.columns
    assert set(trips.data["mode"].dropna().unique()) <= {"walk", "bus", "car", "metro"}
    assert set(trips.data["purpose"].dropna().unique()) <= {
        "work",
        "education",
        "home",
        "shopping",
    }

    assert report.field_correspondence == FIELD_CORR_STANDARD
    assert report.value_correspondence == VALUE_CORR_STANDARD
    assert trips.field_correspondence == FIELD_CORR_STANDARD
    assert trips.value_correspondence == VALUE_CORR_STANDARD
    assert trips.metadata["mappings"]["field_correspondence"] == FIELD_CORR_STANDARD
    assert trips.metadata["mappings"]["value_correspondence"] == VALUE_CORR_STANDARD


def test_integration_happy_path_single_stage_true():
    """Verifica que single_stage=True derive trip_id y movement_seq en un caso end-to-end."""
    df = generate_synthetic_trip_dataframe(
        filas=6,
        seed=42,
        tier_temporal="tier_1",
        tier1_datetime_format="utc_string_z",
        coord_format="numeric",
        h3_mode="provided_valid",
        trip_structure="single_stage_like",
        base_fields=["mode", "purpose"],
        omit_required_fields=["trip_id", "movement_seq"],
    )

    trips, report = import_trips_from_dataframe(
        df,
        schema=BASE_TRIP_SCHEMA,
        source_name="synthetic_a3",
        options=ImportOptions(single_stage=True),
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    assert report.ok is True
    assert len(trips.data) == 6
    assert "trip_id" in trips.data.columns
    assert "movement_seq" in trips.data.columns
    assert (trips.data["movement_seq"] == 0).all()
    assert trips.data["trip_id"].notna().all()
    assert trips.data["movement_id"].notna().all()
    assert trips.metadata["events"][-1]["parameters"]["single_stage"] is True


def test_integration_happy_path_tier2():
    """Verifica un import Tier 2 con horas locales HH:MM válidas y metadata temporal correspondiente."""
    df = make_tier2_valid(filas=6, seed=42)

    trips, report = import_trips_from_dataframe(
        df,
        schema=BASE_TRIP_SCHEMA,
        source_name="synthetic_a4",
        options=ImportOptions(),
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    assert report.ok is True
    assert len(trips.data) == 6
    assert "origin_time_local_hhmm" in trips.data.columns
    assert "destination_time_local_hhmm" in trips.data.columns
    assert trips.data["origin_time_local_hhmm"].notna().all()
    assert trips.data["destination_time_local_hhmm"].notna().all()

    assert trips.metadata["temporal"]["tier"] == "tier_2"
    assert "origin_time_local_hhmm" in trips.metadata["temporal"]["fields_present"]
    assert "destination_time_local_hhmm" in trips.metadata["temporal"]["fields_present"]


def test_integration_happy_path_h3_derivable():
    """Verifica que el import derive H3 cuando las coordenadas OD están presentes y los H3 no vienen de fuente."""
    df = make_h3_derivable(filas=8, seed=42)

    trips, report = import_trips_from_dataframe(
        df,
        schema=BASE_TRIP_SCHEMA,
        source_name="synthetic_a5",
        options=ImportOptions(),
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    assert report.ok is True
    assert len(trips.data) == 8
    assert "origin_h3_index" in trips.data.columns
    assert "destination_h3_index" in trips.data.columns
    assert trips.data["origin_h3_index"].notna().all()
    assert trips.data["destination_h3_index"].notna().all()

    assert trips.metadata["h3"]["resolution"] == 8
    assert trips.metadata["events"][-1]["parameters"]["h3_resolution"] == 8


def test_integration_happy_path_rich():
    """Verifica un happy path rico con más filas y metadata mínima de trazabilidad."""
    df = make_happy_path_rich(filas=15, seed=42)

    trips, report = import_trips_from_dataframe(
        df,
        schema=BASE_TRIP_SCHEMA,
        source_name="synthetic_a1r",
        options=ImportOptions(),
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    assert report.ok is True
    assert len(trips.data) == 15
    assert "dataset_id" in trips.metadata
    assert "events" in trips.metadata
    assert "schema_effective" in trips.metadata


# ---------------------------------------------------------------------
# Sección 3 - Errores estructurales
# ---------------------------------------------------------------------


def test_integration_invalid_schema_version_raises_schema_error():
    """Verifica que un schema con versión inválida aborte con SchemaError."""
    df = make_happy_path_minimal(filas=6, seed=42)

    invalid_schema = TripSchema(
        version="",
        fields={
            "movement_id": FieldSpec("movement_id", "string", required=True),
            "user_id": FieldSpec("user_id", "string", required=True),
        },
        required=["movement_id", "user_id"],
    )

    with pytest.raises(SchemaError) as exc_info:
        import_trips_from_dataframe(
            df,
            schema=invalid_schema,
            source_name="synthetic_b1",
            options=ImportOptions(),
            provenance=BASE_PROVENANCE,
            h3_resolution=8,
        )

    exc = exc_info.value
    assert exc.code == "SCH.TRIP_SCHEMA.INVALID_VERSION"
    assert exc.issue.code == "SCH.TRIP_SCHEMA.INVALID_VERSION"
    assert exc.details["schema_version"] == ""
    assert "expected" in exc.details
    assert exc.issues[-1].code == "SCH.TRIP_SCHEMA.INVALID_VERSION"


def test_integration_missing_required_non_derivable_raises_import_error():
    """Verifica que un required no derivable ausente aborte con IMP.INPUT.MISSING_REQUIRED_FIELD."""
    df = make_missing_required(
        filas=6,
        seed=42,
        missing_fields=["user_id"],
    )

    with pytest.raises(PylondrinaImportError) as exc_info:
        import_trips_from_dataframe(
            df,
            schema=BASE_TRIP_SCHEMA,
            source_name="synthetic_b2",
            options=ImportOptions(),
            provenance=BASE_PROVENANCE,
            h3_resolution=8,
        )

    exc = exc_info.value
    assert exc.code == "IMP.INPUT.MISSING_REQUIRED_FIELD"
    assert exc.issue.code == "IMP.INPUT.MISSING_REQUIRED_FIELD"
    assert "user_id" in exc.details["missing_required"]
    assert "required" in exc.details
    assert "source_columns" in exc.details
    assert exc.issues[-1].code == "IMP.INPUT.MISSING_REQUIRED_FIELD"


def test_integration_invalid_field_correspondence_unknown_canonical_raises_import_error():
    """Verifica que field_correspondence con campo canónico inexistente aborte."""
    df = make_happy_path_minimal(filas=6, seed=42)

    field_corr = {
        "campo_que_no_existe_en_schema": "col_fuente_x",
    }

    with pytest.raises(PylondrinaImportError) as exc_info:
        import_trips_from_dataframe(
            df,
            schema=BASE_TRIP_SCHEMA,
            source_name="synthetic_b3",
            options=ImportOptions(),
            field_correspondence=field_corr,
            provenance=BASE_PROVENANCE,
            h3_resolution=8,
        )

    exc = exc_info.value
    assert exc.code == "MAP.FIELDS.UNKNOWN_CANONICAL_FIELD"
    assert exc.issue.code == "MAP.FIELDS.UNKNOWN_CANONICAL_FIELD"
    assert exc.details["field"] == "campo_que_no_existe_en_schema"
    assert "schema_fields_sample" in exc.details
    assert "schema_fields_total" in exc.details
    assert exc.issues[-1].code == "MAP.FIELDS.UNKNOWN_CANONICAL_FIELD"


def test_integration_strict_domains_blocks_mapping_that_requires_extension():
    """Verifica que strict_domains=True bloquee un mapping cuyo valor destino requeriría extender dominio."""
    field_corr = {"mode": "modo_fuente"}

    df = make_happy_path_minimal(
        filas=6,
        seed=42,
        base_fields=["mode"],
        field_correspondence=field_corr,
    )
    df["modo_fuente"] = ["PATIN"] * 6

    value_corr = {
        "mode": {
            "PATIN": "skateboard",
        }
    }

    with pytest.raises(PylondrinaImportError) as exc_info:
        import_trips_from_dataframe(
            df,
            schema=BASE_TRIP_SCHEMA,
            source_name="synthetic_b4",
            options=ImportOptions(strict_domains=True),
            field_correspondence=field_corr,
            value_correspondence=value_corr,
            provenance=BASE_PROVENANCE,
            h3_resolution=8,
        )

    exc = exc_info.value
    assert exc.code == "DOM.POLICY.MAPPING_REQUIRES_EXTENSION_BLOCKED"
    assert exc.issue.code == "DOM.POLICY.MAPPING_REQUIRES_EXTENSION_BLOCKED"
    assert exc.details["field"] == "mode"
    assert exc.details["strict_domains"] is True
    assert exc.details["domain_extendable"] is True
    assert "skateboard" in exc.details["unmapped_examples"]
    assert exc.issues[-1].code == "DOM.POLICY.MAPPING_REQUIRES_EXTENSION_BLOCKED"


def test_integration_duplicate_movement_id_raises_import_error():
    """Verifica que movement_id duplicado aborte el import."""
    df = make_duplicate_movement_id(
        filas=8,
        seed=42,
        full_rows=False,
    )

    with pytest.raises(PylondrinaImportError) as exc_info:
        import_trips_from_dataframe(
            df,
            schema=BASE_TRIP_SCHEMA,
            source_name="synthetic_b5",
            options=ImportOptions(),
            provenance=BASE_PROVENANCE,
            h3_resolution=8,
        )

    exc = exc_info.value
    assert exc.code == "IMP.ID.MOVEMENT_ID_DUPLICATE"
    assert exc.issue.code == "IMP.ID.MOVEMENT_ID_DUPLICATE"
    assert exc.details["duplicate_count"] >= 2
    assert len(exc.details["duplicate_examples"]) >= 1
    assert exc.issues[-1].code == "IMP.ID.MOVEMENT_ID_DUPLICATE"


def test_integration_required_h3_without_coordinates_raises_import_error():
    """Verifica que H3 requerido no materializable por falta de coordenadas aborte."""
    df = generate_synthetic_trip_dataframe(
        filas=6,
        seed=42,
        tier_temporal="tier_1",
        tier1_datetime_format="utc_string_z",
        coord_format="numeric",
        h3_mode="omitted_derivable",
        trip_structure="independent",
        omit_required_fields=[
            "origin_h3_index",
            "destination_h3_index",
            "origin_latitude",
            "origin_longitude",
            "destination_latitude",
            "destination_longitude",
        ],
    )

    with pytest.raises(PylondrinaImportError) as exc_info:
        import_trips_from_dataframe(
            df,
            schema=H3_REQUIRED_NO_COORDS_SCHEMA,
            source_name="synthetic_b6",
            options=ImportOptions(),
            provenance=BASE_PROVENANCE,
            h3_resolution=8,
        )

    exc = exc_info.value
    assert exc.code == "IMP.H3.REQUIRED_FIELDS_UNAVAILABLE"
    assert exc.issue.code == "IMP.H3.REQUIRED_FIELDS_UNAVAILABLE"
    assert "missing_pairs" in exc.details
    assert "required_h3_fields" in exc.details
    assert "origin_h3_index" in exc.details["required_h3_fields"]
    assert "destination_h3_index" in exc.details["required_h3_fields"]
    assert exc.issues[-1].code == "IMP.H3.REQUIRED_FIELDS_UNAVAILABLE"


# ---------------------------------------------------------------------
# Sección 4 - Calidad no fatal y políticas
# ---------------------------------------------------------------------


def test_integration_non_extendable_domain_maps_unknown_and_records_overrides():
    """Verifica que valores fuera de dominio no extendible se mapeen a unknown y queden trazados."""
    df = make_extended_domains(
        filas=10,
        seed=42,
        include_noise=False,
        extra_value_domains={
            "mode": ["canon", "submodes"],
        },
        base_fields=["mode", "purpose"],
    )

    trips, report = import_trips_from_dataframe(
        df,
        schema=NON_EXTENDABLE_DOMAIN_SCHEMA,
        source_name="synthetic_c1",
        options=ImportOptions(),
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    assert_tripdataset_and_report(trips, report)
    assert report.ok is True
    assert_issue_code(report, "DOM.POLICY.FIELD_NOT_EXTENDABLE")

    assert "mode" in trips.data.columns
    assert "unknown" in set(trips.data["mode"].dropna().astype(str).unique())

    mode_dom = trips.metadata["domains_effective"]["mode"]
    assert mode_dom["extendable"] is False
    assert mode_dom["unknown_value"] == "unknown"
    assert len(mode_dom["unknown_values"]) >= 1
    assert "unknown" in mode_dom["values"]

    mode_overrides = trips.metadata["schema_effective"]["overrides"].get("mode", {})
    assert "out_of_domain_mapped_to_unknown" in mode_overrides["reasons"]

    assert report.metadata["dataset_id"] == trips.metadata["dataset_id"]
    assert report.metadata["summary"]["rows_out"] == len(trips.data)

    event = trips.metadata["events"][-1]
    assert event["op"] == "import_trips"
    assert event["issues_summary"]["by_code"]["DOM.POLICY.FIELD_NOT_EXTENDABLE"] >= 1


def test_integration_extendable_domain_records_domains_effective():
    """Verifica que dominios extendibles agreguen valores y actualicen metadata/schema_effective."""
    df = make_extended_domains(
        filas=10,
        seed=42,
        include_noise=False,
        extra_value_domains={
            "mode": ["canon", "submodes"],
            "purpose": ["canon", "finos"],
        },
    )

    trips, report = import_trips_from_dataframe(
        df,
        schema=BASE_TRIP_SCHEMA,
        source_name="synthetic_c2",
        options=ImportOptions(),
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    assert report.ok is True
    assert_issue_code(report, "DOM.EXTENSION.APPLIED")

    assert isinstance(trips.metadata["domains_extended"], list)
    assert len(trips.metadata["domains_extended"]) >= 1

    some_field = next(iter(set(trips.metadata["domains_extended"])))
    dom_eff = trips.metadata["domains_effective"][some_field]

    assert dom_eff["extendable"] is True
    assert dom_eff["extended"] is True
    assert dom_eff["n_added"] >= 1
    assert len(dom_eff["added_values"]) >= 1
    assert dom_eff["strict_applied"] is False

    overrides = trips.metadata["schema_effective"]["overrides"][some_field]
    assert "domain_extended" in overrides["reasons"]
    assert len(overrides["added_values"]) >= 1

    assert report.metadata["metadata"]["domains_extended"] == trips.metadata["domains_extended"]
    assert report.metadata["metadata"]["dataset_id"] == trips.metadata["dataset_id"]


def test_integration_tier2_mixed_invalid_hhmm_records_partial_coercion():
    """Verifica Tier 2 con HH:MM inválidos recuperables y normalización temporal trazada."""
    df = make_tier2_mixed_invalid(
        filas=12,
        seed=42,
        mostly_invalid=False,
    )

    trips, report = import_trips_from_dataframe(
        df,
        schema=BASE_TRIP_SCHEMA,
        source_name="synthetic_c3",
        options=ImportOptions(),
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    assert report.ok is True
    assert_issue_code(report, "IMP.TEMPORAL.TIER_LIMITED")
    assert_issue_code(report, "IMP.TYPE.COERCE_PARTIAL")

    assert "origin_time_local_hhmm" in trips.data.columns
    assert "destination_time_local_hhmm" in trips.data.columns
    assert (
        trips.data["origin_time_local_hhmm"].isna().sum()
        + trips.data["destination_time_local_hhmm"].isna().sum()
        >= 1
    )

    temporal = trips.metadata["temporal"]
    assert temporal["tier"] == "tier_2"
    assert "origin_time_local_hhmm" in temporal["normalization"]
    assert "destination_time_local_hhmm" in temporal["normalization"]

    assert "n_invalid" in temporal["normalization"]["origin_time_local_hhmm"]
    assert "n_total" in temporal["normalization"]["origin_time_local_hhmm"]
    assert "n_invalid" in temporal["normalization"]["destination_time_local_hhmm"]
    assert "n_total" in temporal["normalization"]["destination_time_local_hhmm"]

    assert report.metadata["metadata"]["temporal"]["tier"] == "tier_2"
    assert report.metadata["summary"]["rows_in"] == 12
    assert report.metadata["summary"]["rows_out"] == 12


def test_integration_tier3_without_od_time_records_limited_temporal_metadata():
    """Verifica Tier 3 sin tiempo OD y ausencia de bloque de normalización temporal."""
    df = generate_synthetic_trip_dataframe(
        filas=10,
        seed=42,
        tier_temporal="tier_3",
        coord_format="numeric",
        h3_mode="provided_valid",
        trip_structure="independent",
        base_fields=["mode", "purpose"],
    )

    trips, report = import_trips_from_dataframe(
        df,
        schema=BASE_TRIP_SCHEMA,
        source_name="synthetic_c4",
        options=ImportOptions(),
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    assert report.ok is True
    assert_issue_code(report, "IMP.TEMPORAL.TIER_LIMITED")

    temporal = trips.metadata["temporal"]
    assert temporal["tier"] == "tier_3"
    assert temporal["fields_present"] == []
    assert "normalization" not in temporal

    assert report.metadata["metadata"]["temporal"]["tier"] == "tier_3"
    assert trips.metadata["is_validated"] is False


def test_integration_tier1_partial_datetime_records_normalization():
    """Verifica Tier 1 con datetimes parcialmente inválidos y metadata de normalización."""
    df = generate_synthetic_trip_dataframe(
        filas=12,
        seed=42,
        tier_temporal="tier_1",
        tier1_datetime_format="mixed_with_invalids",
        coord_format="numeric",
        h3_mode="provided_valid",
        trip_structure="independent",
        base_fields=["mode", "purpose"],
    )

    trips, report = import_trips_from_dataframe(
        df,
        schema=BASE_TRIP_SCHEMA,
        source_name="synthetic_c5",
        options=ImportOptions(),
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    assert report.ok is True
    assert_issue_code(report, "IMP.TYPE.COERCE_PARTIAL")

    assert "origin_time_utc" in trips.data.columns
    assert "destination_time_utc" in trips.data.columns
    assert trips.data["origin_time_utc"].isna().sum() + trips.data["destination_time_utc"].isna().sum() >= 1

    temporal = trips.metadata["temporal"]
    assert temporal["tier"] == "tier_1"
    assert "origin_time_utc" in temporal["normalization"]
    assert "destination_time_utc" in temporal["normalization"]
    assert "status" in temporal["normalization"]["origin_time_utc"]
    assert "status" in temporal["normalization"]["destination_time_utc"]
    assert "n_nat" in temporal["normalization"]["origin_time_utc"]
    assert "n_nat" in temporal["normalization"]["destination_time_utc"]

    assert isinstance(report.metadata["metadata"]["temporal"]["normalization"], dict)
    assert report.metadata["dataset_id"] == trips.metadata["dataset_id"]


def test_integration_partial_coordinates_continues_and_records_h3_traceability():
    """Verifica coordenadas parcialmente corruptas, coerción parcial y trazabilidad H3."""
    df = generate_synthetic_trip_dataframe(
        filas=12,
        seed=42,
        tier_temporal="tier_1",
        tier1_datetime_format="utc_string_z",
        coord_format="numeric",
        h3_mode="provided_valid",
        trip_structure="independent",
        base_fields=["mode", "purpose"],
        type_corruption={
            "origin_latitude": {"mode": "non_numeric_text", "ratio": 0.25},
            "destination_longitude": {"mode": "non_numeric_text", "ratio": 0.30},
        },
    )

    trips, report = import_trips_from_dataframe(
        df,
        schema=COORD_PARTIAL_NO_H3_SCHEMA,
        source_name="synthetic_c6",
        options=ImportOptions(),
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    assert report.ok is True
    assert_issue_code(report, "IMP.TYPE.COERCE_PARTIAL")

    assert trips.data["origin_latitude"].isna().sum() >= 1
    assert trips.data["destination_longitude"].isna().sum() >= 1
    assert trips.metadata["h3"]["resolution"] == 8
    assert "origin_h3_index" in trips.data.columns
    assert "destination_h3_index" in trips.data.columns

    event = trips.metadata["events"][-1]
    assert event["issues_summary"]["by_code"]["IMP.TYPE.COERCE_PARTIAL"] >= 1
    assert report.metadata["summary"]["rows_out"] == len(trips.data)
    assert report.metadata["metadata"]["dataset_id"] == trips.metadata["dataset_id"]


def test_integration_partial_h3_derivation_records_issue_and_metadata():
    """Verifica derivación H3 parcial desde coordenadas incompletas."""
    df = generate_synthetic_trip_dataframe(
        filas=12,
        seed=42,
        tier_temporal="tier_1",
        tier1_datetime_format="utc_string_z",
        coord_format="numeric",
        h3_mode="omitted_derivable",
        trip_structure="independent",
        base_fields=["mode", "purpose"],
        paired_missingness={
            "origin_incomplete": 0.20,
            "destination_incomplete": 0.25,
        },
        omit_required_fields=["origin_h3_index", "destination_h3_index"],
    )

    trips, report = import_trips_from_dataframe(
        df,
        schema=BASE_TRIP_SCHEMA,
        source_name="synthetic_c7",
        options=ImportOptions(),
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    assert report.ok is True
    assert_issue_code(report, "IMP.H3.PARTIAL_DERIVATION")

    assert "origin_h3_index" in trips.data.columns
    assert "destination_h3_index" in trips.data.columns
    assert trips.data["origin_h3_index"].isna().sum() + trips.data["destination_h3_index"].isna().sum() >= 1

    h3_meta = trips.metadata["h3"]
    assert h3_meta["resolution"] == 8
    assert "source_fields" in h3_meta
    assert "derived_fields" in h3_meta
    assert (
        "origin_h3_index" in h3_meta["derived_fields"]
        or "destination_h3_index" in h3_meta["derived_fields"]
    )

    assert report.metadata["metadata"]["h3"]["resolution"] == 8

    event = trips.metadata["events"][-1]
    assert event["parameters"]["h3_resolution"] == 8
    assert event["issues_summary"]["by_code"]["IMP.H3.PARTIAL_DERIVATION"] >= 1


def test_integration_rich_extendable_domains():
    """Verifica dominio extendible en dataset rico, con metadata y evento consistentes."""
    df = make_extended_domains(
        filas=50,
        seed=42,
        include_noise=True,
        extra_value_domains={
            "mode": ["canon", "submodes"],
            "purpose": ["canon", "finos"],
            "time_period": ["canon", "eod_finos"],
        },
        base_fields=["mode", "purpose", "time_period", "user_gender", "trip_weight"],
        extra_columns=["household_id", "travel_time_min", "survey_date"],
    )

    trips, report = import_trips_from_dataframe(
        df,
        schema=BASE_TRIP_SCHEMA,
        source_name="synthetic_c2r",
        options=ImportOptions(),
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    assert report.ok is True
    assert_issue_code(report, "DOM.EXTENSION.APPLIED")
    assert len(trips.data) == 50
    assert len(trips.metadata["domains_extended"]) >= 1

    assert report.metadata["summary"]["rows_in"] == 50
    assert report.metadata["summary"]["rows_out"] == 50
    assert report.metadata["metadata"]["dataset_id"] == trips.metadata["dataset_id"]
    assert isinstance(report.metadata["metadata"]["domains_effective"], dict)

    event = trips.metadata["events"][-1]
    assert event["summary"]["input_rows"] == 50
    assert event["summary"]["output_rows"] == 50


def test_integration_rich_tier2_mixed_invalid():
    """Verifica Tier 2 rico con muchos HH:MM inválidos y normalización trazada."""
    df = make_tier2_mixed_invalid(
        filas=50,
        seed=42,
        mostly_invalid=True,
        base_fields=["mode", "purpose", "trip_weight"],
        extra_columns=["household_id", "travel_time_min", "survey_date"],
    )

    trips, report = import_trips_from_dataframe(
        df,
        schema=BASE_TRIP_SCHEMA,
        source_name="synthetic_c3r",
        options=ImportOptions(),
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    assert report.ok is True
    assert_issue_code(report, "IMP.TEMPORAL.TIER_LIMITED")
    assert_issue_code(report, "IMP.TYPE.COERCE_PARTIAL")

    assert len(trips.data) == 50
    assert trips.metadata["temporal"]["tier"] == "tier_2"

    norm = trips.metadata["temporal"]["normalization"]
    assert norm["origin_time_local_hhmm"]["n_total"] == 50
    assert norm["destination_time_local_hhmm"]["n_total"] == 50
    assert (
        norm["origin_time_local_hhmm"]["n_invalid"]
        + norm["destination_time_local_hhmm"]["n_invalid"]
        >= 1
    )

    assert report.metadata["metadata"]["temporal"]["tier"] == "tier_2"


def test_integration_rich_h3_partial_derivation():
    """Verifica H3 parcial en dataset rico con coordenadas incompletas."""
    df = generate_synthetic_trip_dataframe(
        filas=50,
        seed=42,
        tier_temporal="tier_1",
        tier1_datetime_format="offset_string",
        coord_format="numeric",
        h3_mode="omitted_derivable",
        trip_structure="independent",
        base_fields=[
            "mode",
            "purpose",
            "trip_weight",
            "origin_municipality",
            "destination_municipality",
        ],
        extra_columns=["household_id", "travel_time_min", "survey_date"],
        paired_missingness={
            "origin_incomplete": 0.18,
            "destination_incomplete": 0.22,
        },
        omit_required_fields=["origin_h3_index", "destination_h3_index"],
    )

    trips, report = import_trips_from_dataframe(
        df,
        schema=BASE_TRIP_SCHEMA,
        source_name="synthetic_c7r",
        options=ImportOptions(),
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    assert report.ok is True
    assert_issue_code(report, "IMP.H3.PARTIAL_DERIVATION")

    assert len(trips.data) == 50
    assert trips.data["origin_h3_index"].isna().sum() + trips.data["destination_h3_index"].isna().sum() >= 1
    assert trips.metadata["h3"]["resolution"] == 8
    assert len(trips.metadata["h3"]["derived_fields"]) >= 1

    event = trips.metadata["events"][-1]
    assert event["summary"]["input_rows"] == 50
    assert event["summary"]["output_rows"] == 50
    assert event["issues_summary"]["by_code"]["IMP.H3.PARTIAL_DERIVATION"] >= 1

    assert report.metadata["metadata"]["h3"]["resolution"] == 8


# ---------------------------------------------------------------------
# Sección 5 - Selección final y salida observable
# ---------------------------------------------------------------------


def test_integration_selection_none_drop_extras():
    """Verifica selected_fields=None con keep_extra_fields=False: conserva schema presente y elimina extras."""
    df = make_rich_selection_dataframe(rows=12)

    trips, report = import_trips_from_dataframe(
        df,
        schema=BASE_TRIP_SCHEMA,
        source_name="synthetic_d1",
        options=ImportOptions(selected_fields=None, keep_extra_fields=False),
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    required_cols = set(BASE_TRIP_SCHEMA.required)
    expected_all_schema_cols = required_cols | SCHEMA_OPTIONAL_PRESENT_IN_SOURCE | {
        "destination_time_utc",
        "origin_time_utc",
    }

    assert report.ok is True
    final_cols = set(trips.data.columns)

    assert expected_all_schema_cols.issubset(final_cols)
    assert final_cols.isdisjoint(EXTRA_COLS_PRESENT_IN_SOURCE)
    assert_issue_code(report, "IMP.OPTIONS.EXTRA_FIELDS_DROPPED")
    assert trips.metadata["extra_fields_kept"] == []

    fields_eff = set(trips.metadata["schema_effective"]["fields_effective"])
    assert fields_eff == expected_all_schema_cols

    assert report.metadata["summary"]["rows_in"] == 12
    assert report.metadata["summary"]["rows_out"] == 12
    assert report.metadata["metadata"]["extra_fields_kept"] == []
    assert set(report.metadata["metadata"]["schema_effective"]["fields_effective"]) == expected_all_schema_cols

    event = trips.metadata["events"][-1]
    assert set(event["summary"]["columns_deleted"]) >= EXTRA_COLS_PRESENT_IN_SOURCE
    assert event["summary"]["output_rows"] == 12
    assert event["issues_summary"]["by_code"]["IMP.OPTIONS.EXTRA_FIELDS_DROPPED"] >= 1


def test_integration_selection_none_keep_extras():
    """Verifica selected_fields=None con keep_extra_fields=True: conserva schema presente y extras."""
    df = make_rich_selection_dataframe(rows=12)

    trips, report = import_trips_from_dataframe(
        df,
        schema=BASE_TRIP_SCHEMA,
        source_name="synthetic_d2",
        options=ImportOptions(selected_fields=None, keep_extra_fields=True),
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    required_cols = set(BASE_TRIP_SCHEMA.required)
    expected_all_schema_cols = required_cols | SCHEMA_OPTIONAL_PRESENT_IN_SOURCE | {
        "destination_time_utc",
        "origin_time_utc",
    }

    assert report.ok is True
    final_cols = set(trips.data.columns)

    assert expected_all_schema_cols.issubset(final_cols)
    assert EXTRA_COLS_PRESENT_IN_SOURCE.issubset(final_cols)
    assert_no_issue_code(report, "IMP.OPTIONS.EXTRA_FIELDS_DROPPED")

    assert set(trips.metadata["extra_fields_kept"]) == EXTRA_COLS_PRESENT_IN_SOURCE

    fields_eff = set(trips.metadata["schema_effective"]["fields_effective"])
    assert fields_eff == expected_all_schema_cols
    assert fields_eff.isdisjoint(EXTRA_COLS_PRESENT_IN_SOURCE)

    assert report.metadata["metadata"]["dataset_id"] == trips.metadata["dataset_id"]
    assert set(report.metadata["metadata"]["extra_fields_kept"]) == EXTRA_COLS_PRESENT_IN_SOURCE

    event = trips.metadata["events"][-1]
    assert event["summary"]["columns_deleted"] == []
    assert event["summary"]["output_rows"] == 12


def test_integration_selection_subset_drop_extras():
    """Verifica selected_fields=subconjunto con keep_extra_fields=False."""
    df = make_rich_selection_dataframe(rows=12)

    trips, report = import_trips_from_dataframe(
        df,
        schema=BASE_TRIP_SCHEMA,
        source_name="synthetic_d3",
        options=ImportOptions(selected_fields=SUBSET_SELECTED, keep_extra_fields=False),
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    required_cols = set(BASE_TRIP_SCHEMA.required)
    expected_subset_schema_cols = required_cols | set(SUBSET_SELECTED)
    dropped_optional_schema = SCHEMA_OPTIONAL_PRESENT_IN_SOURCE - set(SUBSET_SELECTED)

    assert report.ok is True
    final_cols = set(trips.data.columns)

    assert expected_subset_schema_cols.issubset(final_cols)
    assert final_cols.isdisjoint(dropped_optional_schema)
    assert final_cols.isdisjoint(EXTRA_COLS_PRESENT_IN_SOURCE)
    assert_issue_code(report, "IMP.OPTIONS.EXTRA_FIELDS_DROPPED")

    assert trips.metadata["extra_fields_kept"] == []
    assert set(trips.metadata["schema_effective"]["fields_effective"]) == expected_subset_schema_cols
    assert set(report.metadata["metadata"]["schema_effective"]["fields_effective"]) == expected_subset_schema_cols
    assert report.parameters["selected_fields"] == SUBSET_SELECTED
    assert report.parameters["keep_extra_fields"] is False

    event = trips.metadata["events"][-1]
    assert set(event["summary"]["columns_deleted"]) >= (
        EXTRA_COLS_PRESENT_IN_SOURCE | dropped_optional_schema
    )


def test_integration_selection_subset_keep_extras():
    """Verifica selected_fields=subconjunto con keep_extra_fields=True."""
    df = make_rich_selection_dataframe(rows=12)

    trips, report = import_trips_from_dataframe(
        df,
        schema=BASE_TRIP_SCHEMA,
        source_name="synthetic_d4",
        options=ImportOptions(selected_fields=SUBSET_SELECTED, keep_extra_fields=True),
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    required_cols = set(BASE_TRIP_SCHEMA.required)
    expected_subset_schema_cols = required_cols | set(SUBSET_SELECTED)
    dropped_optional_schema = SCHEMA_OPTIONAL_PRESENT_IN_SOURCE - set(SUBSET_SELECTED)

    assert report.ok is True
    final_cols = set(trips.data.columns)

    assert expected_subset_schema_cols.issubset(final_cols)
    assert final_cols.isdisjoint(dropped_optional_schema)
    assert EXTRA_COLS_PRESENT_IN_SOURCE.issubset(final_cols)
    assert_no_issue_code(report, "IMP.OPTIONS.EXTRA_FIELDS_DROPPED")

    assert set(trips.metadata["extra_fields_kept"]) == EXTRA_COLS_PRESENT_IN_SOURCE
    assert set(trips.metadata["schema_effective"]["fields_effective"]) == expected_subset_schema_cols
    assert report.parameters["selected_fields"] == SUBSET_SELECTED
    assert report.parameters["keep_extra_fields"] is True
    assert set(report.metadata["metadata"]["extra_fields_kept"]) == EXTRA_COLS_PRESENT_IN_SOURCE
    assert set(report.metadata["metadata"]["schema_effective"]["fields_effective"]) == expected_subset_schema_cols

    event = trips.metadata["events"][-1]
    assert set(event["summary"]["columns_deleted"]) >= dropped_optional_schema
    assert isinstance(event["summary"]["columns_deleted"], list)
    assert event["summary"]["output_rows"] == 12


def test_integration_selection_rich_none_keep_extras():
    """Verifica selección rica con selected_fields=None y conservación de extras."""
    df = make_rich_selection_dataframe(rows=50)

    trips, report = import_trips_from_dataframe(
        df,
        schema=BASE_TRIP_SCHEMA,
        source_name="synthetic_d2r",
        options=ImportOptions(selected_fields=None, keep_extra_fields=True),
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    required_cols = set(BASE_TRIP_SCHEMA.required)
    expected_all_schema_cols = required_cols | SCHEMA_OPTIONAL_PRESENT_IN_SOURCE | {
        "destination_time_utc",
        "origin_time_utc",
    }

    assert report.ok is True
    assert len(trips.data) == 50
    assert expected_all_schema_cols.issubset(set(trips.data.columns))
    assert EXTRA_COLS_PRESENT_IN_SOURCE.issubset(set(trips.data.columns))

    assert set(trips.metadata["extra_fields_kept"]) == EXTRA_COLS_PRESENT_IN_SOURCE
    assert set(report.metadata["metadata"]["extra_fields_kept"]) == EXTRA_COLS_PRESENT_IN_SOURCE
    assert report.metadata["summary"]["rows_in"] == 50
    assert report.metadata["summary"]["rows_out"] == 50

    event = trips.metadata["events"][-1]
    assert event["summary"]["input_rows"] == 50
    assert event["summary"]["output_rows"] == 50
    assert event["summary"]["columns_deleted"] == []


def test_integration_selection_rich_subset_keep_extras():
    """Verifica selección rica con subconjunto de campos y extras preservadas."""
    df = make_rich_selection_dataframe(rows=50)

    trips, report = import_trips_from_dataframe(
        df,
        schema=BASE_TRIP_SCHEMA,
        source_name="synthetic_d4r",
        options=ImportOptions(selected_fields=SUBSET_SELECTED, keep_extra_fields=True),
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    required_cols = set(BASE_TRIP_SCHEMA.required)
    expected_subset_schema_cols = required_cols | set(SUBSET_SELECTED)
    dropped_optional_schema = SCHEMA_OPTIONAL_PRESENT_IN_SOURCE - set(SUBSET_SELECTED)

    assert report.ok is True
    assert len(trips.data) == 50

    final_cols = set(trips.data.columns)
    assert expected_subset_schema_cols.issubset(final_cols)
    assert final_cols.isdisjoint(dropped_optional_schema)
    assert EXTRA_COLS_PRESENT_IN_SOURCE.issubset(final_cols)

    assert set(trips.metadata["schema_effective"]["fields_effective"]) == expected_subset_schema_cols
    assert set(trips.metadata["extra_fields_kept"]) == EXTRA_COLS_PRESENT_IN_SOURCE
    assert set(report.metadata["metadata"]["extra_fields_kept"]) == EXTRA_COLS_PRESENT_IN_SOURCE
    assert set(report.metadata["metadata"]["schema_effective"]["fields_effective"]) == expected_subset_schema_cols

    event = trips.metadata["events"][-1]
    assert event["summary"]["input_rows"] == 50
    assert event["summary"]["output_rows"] == 50
    assert set(event["summary"]["columns_deleted"]) >= dropped_optional_schema


# ---------------------------------------------------------------------
# Sección 6 - Trazabilidad y contrato final
# ---------------------------------------------------------------------


def test_integration_traceability_simple_case():
    """Verifica contrato observable común: metadata, evento, report, schema_effective y summaries."""
    df = make_happy_path_minimal(filas=6, seed=42)

    trips, report = import_trips_from_dataframe(
        df,
        schema=BASE_TRIP_SCHEMA,
        source_name="synthetic_e1",
        options=ImportOptions(),
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    assert_common_contract(trips, report, expected_rows=6)

    assert trips.metadata["provenance"] == BASE_PROVENANCE
    assert isinstance(trips.metadata["schema"], dict)
    assert isinstance(trips.metadata["schema_effective"], dict)
    assert isinstance(trips.metadata["mappings"], dict)
    assert isinstance(trips.metadata["domains_effective"], dict)
    assert isinstance(trips.metadata["domains_extended"], list)
    assert isinstance(trips.metadata["extra_fields_kept"], list)
    assert isinstance(trips.metadata["temporal"], dict)

    event = trips.metadata["events"][-1]
    assert event["summary"]["input_rows"] == 6
    assert event["summary"]["output_rows"] == 6
    assert isinstance(event["summary"]["columns_deleted"], list)
    assert isinstance(event["issues_summary"]["counts"], dict)
    assert isinstance(event["issues_summary"]["by_code"], dict)

    assert report.summary["rows_in"] == 6
    assert report.summary["rows_out"] == 6
    assert isinstance(report.summary["n_fields_mapped"], int)
    assert isinstance(report.summary["n_domain_mappings_applied"], int)

    assert report.parameters["keep_extra_fields"] is True
    assert report.parameters["strict"] is False
    assert report.parameters["strict_domains"] is False
    assert report.parameters["single_stage"] is False

    schema_eff = trips.metadata["schema_effective"]
    assert isinstance(schema_eff["fields_effective"], list)
    assert isinstance(schema_eff["overrides"], dict)
    assert isinstance(schema_eff["domains_effective"], dict)
    assert isinstance(schema_eff["temporal"], dict)


def test_integration_traceability_with_warnings():
    """Verifica trazabilidad cuando el import completa con warnings de coerción parcial."""
    df = generate_synthetic_trip_dataframe(
        filas=10,
        seed=42,
        tier_temporal="tier_1",
        tier1_datetime_format="mixed_with_invalids",
        coord_format="numeric",
        h3_mode="provided_valid",
        trip_structure="independent",
        base_fields=["mode", "purpose"],
    )

    trips, report = import_trips_from_dataframe(
        df,
        schema=BASE_TRIP_SCHEMA,
        source_name="synthetic_e2",
        options=ImportOptions(),
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    assert_common_contract(trips, report, expected_rows=10)
    assert_issue_code(report, "IMP.TYPE.COERCE_PARTIAL")

    for issue in report.issues:
        assert hasattr(issue, "level")
        assert hasattr(issue, "code")
        assert hasattr(issue, "message")
        assert hasattr(issue, "details")

    event = trips.metadata["events"][-1]
    assert event["issues_summary"]["counts"]["warning"] >= 1
    assert event["issues_summary"]["by_code"]["IMP.TYPE.COERCE_PARTIAL"] >= 1

    assert trips.metadata["temporal"]["tier"] == "tier_1"
    assert isinstance(trips.metadata["temporal"]["normalization"], dict)
    assert report.metadata["metadata"]["temporal"]["tier"] == trips.metadata["temporal"]["tier"]
    assert report.metadata["metadata"]["dataset_id"] == trips.metadata["dataset_id"]


def test_integration_traceability_with_domains_extended():
    """Verifica trazabilidad completa de dominios extendidos."""
    df = make_extended_domains(
        filas=10,
        seed=42,
        include_noise=False,
        extra_value_domains={
            "mode": ["canon", "submodes"],
            "purpose": ["canon", "finos"],
        },
    )

    trips, report = import_trips_from_dataframe(
        df,
        schema=BASE_TRIP_SCHEMA,
        source_name="synthetic_e3",
        options=ImportOptions(),
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    assert_common_contract(trips, report, expected_rows=10)
    assert_issue_code(report, "DOM.EXTENSION.APPLIED")

    assert isinstance(trips.metadata["domains_extended"], list)
    assert len(trips.metadata["domains_extended"]) >= 1

    for field in trips.metadata["domains_extended"]:
        dom = trips.metadata["domains_effective"][field]
        assert dom["extendable"] is True
        assert dom["extended"] is True
        assert isinstance(dom["added_values"], list)
        assert dom["n_added"] >= 1

        assert field in trips.metadata["schema_effective"]["overrides"]
        assert "domain_extended" in trips.metadata["schema_effective"]["overrides"][field]["reasons"]

    assert report.metadata["metadata"]["domains_extended"] == trips.metadata["domains_extended"]

    event = trips.metadata["events"][-1]
    assert event["issues_summary"]["by_code"]["DOM.EXTENSION.APPLIED"] >= 1


def test_integration_traceability_tier2():
    """Verifica trazabilidad temporal de un caso Tier 2."""
    df = make_tier2_valid(filas=8, seed=42)

    trips, report = import_trips_from_dataframe(
        df,
        schema=BASE_TRIP_SCHEMA,
        source_name="synthetic_e4",
        options=ImportOptions(),
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    assert_common_contract(trips, report, expected_rows=8)

    temporal = trips.metadata["temporal"]
    assert temporal["tier"] == "tier_2"
    assert isinstance(temporal["fields_present"], list)
    assert "origin_time_local_hhmm" in temporal["fields_present"]
    assert "destination_time_local_hhmm" in temporal["fields_present"]

    assert isinstance(trips.metadata["schema_effective"]["temporal"], dict)
    assert report.metadata["metadata"]["temporal"]["tier"] == "tier_2"
    assert report.summary["rows_in"] == 8
    assert report.summary["rows_out"] == 8

    event = trips.metadata["events"][-1]
    assert event["summary"]["input_rows"] == 8
    assert event["summary"]["output_rows"] == 8


def test_integration_traceability_with_correspondences():
    """Verifica trazabilidad de field_correspondence y value_correspondence en dataset, report y evento."""
    df = make_happy_path_minimal(
        filas=8,
        seed=42,
        base_fields=["mode", "purpose"],
        field_correspondence=FIELD_CORR_STANDARD,
    )

    df["modo_fuente"] = ["A PIE", "BUS", "AUTO", "METRO", "A PIE", "BUS", "AUTO", "METRO"]
    df["proposito_fuente"] = [
        "TRABAJO",
        "ESTUDIO",
        "HOGAR",
        "COMPRAS",
        "TRABAJO",
        "ESTUDIO",
        "HOGAR",
        "COMPRAS",
    ]

    trips, report = import_trips_from_dataframe(
        df,
        schema=BASE_TRIP_SCHEMA,
        source_name="synthetic_e5",
        options=ImportOptions(),
        field_correspondence=FIELD_CORR_STANDARD,
        value_correspondence=VALUE_CORR_STANDARD,
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    assert_common_contract(trips, report, expected_rows=8)

    assert report.field_correspondence == FIELD_CORR_STANDARD
    assert report.value_correspondence == VALUE_CORR_STANDARD
    assert trips.metadata["mappings"]["field_correspondence"] == FIELD_CORR_STANDARD
    assert trips.metadata["mappings"]["value_correspondence"] == VALUE_CORR_STANDARD
    assert trips.field_correspondence == FIELD_CORR_STANDARD
    assert trips.value_correspondence == VALUE_CORR_STANDARD

    event = trips.metadata["events"][-1]
    assert event["op"] == "import_trips"
    assert event["summary"]["input_rows"] == 8
    assert event["summary"]["output_rows"] == 8

    assert report.summary["rows_in"] == 8
    assert report.summary["rows_out"] == 8
    assert report.metadata["metadata"]["mappings"]["field_correspondence"] == FIELD_CORR_STANDARD
    assert report.metadata["metadata"]["mappings"]["value_correspondence"] == VALUE_CORR_STANDARD


def test_integration_traceability_rich_warnings():
    """Verifica trazabilidad con warnings en dataset rico de 50 filas."""
    df = generate_synthetic_trip_dataframe(
        filas=50,
        seed=42,
        tier_temporal="tier_1",
        tier1_datetime_format="mixed_with_invalids",
        coord_format="numeric",
        h3_mode="provided_valid",
        trip_structure="independent",
        base_fields=["mode", "purpose", "day_type", "time_period", "user_gender", "trip_weight"],
        extra_columns=[
            "household_id",
            "source_person_id",
            "stage_count",
            "activity_destination",
            "travel_time_min",
            "fare_amount",
        ],
    )

    trips, report = import_trips_from_dataframe(
        df,
        schema=BASE_TRIP_SCHEMA,
        source_name="synthetic_e2r",
        options=ImportOptions(),
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    assert_common_contract(trips, report, expected_rows=50)
    assert_issue_code(report, "IMP.TYPE.COERCE_PARTIAL")

    assert report.summary["rows_in"] == 50
    assert report.summary["rows_out"] == 50

    event = trips.metadata["events"][-1]
    assert event["summary"]["input_rows"] == 50
    assert event["summary"]["output_rows"] == 50
    assert event["issues_summary"]["counts"]["warning"] >= 1

    assert report.metadata["metadata"]["dataset_id"] == trips.metadata["dataset_id"]
    assert isinstance(report.metadata["metadata"]["temporal"], dict)


def test_integration_traceability_rich_correspondences():
    """Verifica correspondencias y trazabilidad en dataset rico de 50 filas."""
    df = make_happy_path_rich(
        filas=50,
        seed=42,
        field_correspondence=FIELD_CORR_STANDARD,
        base_fields=["mode", "purpose", "day_type", "time_period", "user_gender", "trip_weight"],
        extra_columns=[
            "household_id",
            "source_person_id",
            "stage_count",
            "activity_destination",
            "travel_time_min",
            "fare_amount",
        ],
    )

    pattern_mode = ["A PIE", "BUS", "AUTO", "METRO"] * 13
    pattern_purpose = ["TRABAJO", "ESTUDIO", "HOGAR", "COMPRAS"] * 13
    df["modo_fuente"] = pattern_mode[:50]
    df["proposito_fuente"] = pattern_purpose[:50]

    trips, report = import_trips_from_dataframe(
        df,
        schema=BASE_TRIP_SCHEMA,
        source_name="synthetic_e5r",
        options=ImportOptions(keep_extra_fields=True),
        field_correspondence=FIELD_CORR_STANDARD,
        value_correspondence=VALUE_CORR_STANDARD,
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    assert_common_contract(trips, report, expected_rows=50)

    assert report.field_correspondence == FIELD_CORR_STANDARD
    assert report.value_correspondence == VALUE_CORR_STANDARD
    assert trips.metadata["mappings"]["field_correspondence"] == FIELD_CORR_STANDARD
    assert trips.metadata["mappings"]["value_correspondence"] == VALUE_CORR_STANDARD

    assert report.summary["rows_in"] == 50
    assert report.summary["rows_out"] == 50
    assert report.metadata["metadata"]["dataset_id"] == trips.metadata["dataset_id"]

    event = trips.metadata["events"][-1]
    assert event["summary"]["input_rows"] == 50
    assert event["summary"]["output_rows"] == 50


# ---------------------------------------------------------------------
# Sección 7 - CSVs sintéticos grandes del notebook
# ---------------------------------------------------------------------


def test_integration_large_csv_happy_path_minimal_1200():
    """Verifica importación del CSV sintético happy_path_minimal_1200.csv si está disponible."""
    df = read_synthetic_csv_or_skip("happy_path_minimal_1200.csv")

    trips, report = import_trips_from_dataframe(
        df,
        schema=BASE_TRIP_SCHEMA,
        source_name="synthetic_g1",
        options=ImportOptions(),
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    assert_tripdataset_and_report(trips, report)
    assert report.ok is True
    assert len(trips.data) == 1200
    assert report.summary["rows_in"] == 1200
    assert report.summary["rows_out"] == 1200

    assert trips.metadata["is_validated"] is False
    assert isinstance(trips.metadata["events"], list)

    event = trips.metadata["events"][-1]
    assert event["op"] == "import_trips"
    assert event["summary"]["input_rows"] == 1200
    assert event["summary"]["output_rows"] == 1200

    assert report.metadata["dataset_id"] == trips.metadata["dataset_id"]
    assert report.field_correspondence == {}
    assert report.value_correspondence == {}


def test_integration_large_csv_correspondence_1200():
    """Verifica importación del CSV large_correspondence_1200.csv con correspondencias si está disponible."""
    df = read_synthetic_csv_or_skip("large_correspondence_1200.csv")

    trips, report = import_trips_from_dataframe(
        df,
        schema=BASE_TRIP_SCHEMA,
        source_name="synthetic_g2",
        options=ImportOptions(keep_extra_fields=True),
        field_correspondence=FIELD_CORR_STANDARD,
        value_correspondence=VALUE_CORR_STANDARD,
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    assert report.ok is True
    assert len(trips.data) == 1200

    assert report.field_correspondence == FIELD_CORR_STANDARD
    assert report.value_correspondence == VALUE_CORR_STANDARD
    assert trips.field_correspondence == FIELD_CORR_STANDARD
    assert trips.value_correspondence == VALUE_CORR_STANDARD

    assert trips.metadata["mappings"]["field_correspondence"] == FIELD_CORR_STANDARD
    assert trips.metadata["mappings"]["value_correspondence"] == VALUE_CORR_STANDARD
    assert report.metadata["metadata"]["mappings"]["field_correspondence"] == FIELD_CORR_STANDARD
    assert report.metadata["metadata"]["mappings"]["value_correspondence"] == VALUE_CORR_STANDARD

    assert set(trips.data["mode"].dropna().unique()) <= {"walk", "bus", "car", "metro"}
    assert set(trips.data["purpose"].dropna().unique()) <= {
        "work",
        "education",
        "home",
        "shopping",
    }

    event = trips.metadata["events"][-1]
    assert event["op"] == "import_trips"
    assert event["summary"]["input_rows"] == 1200
    assert event["summary"]["output_rows"] == 1200

    assert report.metadata["dataset_id"] == trips.metadata["dataset_id"]


def test_integration_large_csv_datetime_partial_1200():
    """Verifica importación del CSV datetime_partial_1200.csv con coerción parcial si está disponible."""
    df = read_synthetic_csv_or_skip("datetime_partial_1200.csv")

    trips, report = import_trips_from_dataframe(
        df,
        schema=BASE_TRIP_SCHEMA,
        source_name="synthetic_g3",
        options=ImportOptions(),
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    assert report.ok is True
    assert len(trips.data) == 1200
    assert report.summary["rows_in"] == 1200
    assert report.summary["rows_out"] == 1200
    assert_issue_code(report, "IMP.TYPE.COERCE_PARTIAL")

    assert "origin_time_utc" in trips.data.columns
    assert "destination_time_utc" in trips.data.columns
    assert trips.data["origin_time_utc"].isna().sum() + trips.data["destination_time_utc"].isna().sum() >= 1

    assert trips.metadata["temporal"]["tier"] == "tier_1"
    assert isinstance(trips.metadata["temporal"]["normalization"], dict)

    event = trips.metadata["events"][-1]
    assert event["issues_summary"]["counts"]["warning"] >= 1
    assert event["issues_summary"]["by_code"]["IMP.TYPE.COERCE_PARTIAL"] >= 1

    assert report.metadata["dataset_id"] == trips.metadata["dataset_id"]
    assert report.metadata["metadata"]["temporal"]["tier"] == "tier_1"


def test_integration_large_csv_selection_output_1200():
    """Verifica importación del CSV selection_output_1200.csv con selected_fields y extras si está disponible."""
    df = read_synthetic_csv_or_skip("selection_output_1200.csv")

    trips, report = import_trips_from_dataframe(
        df,
        schema=BASE_TRIP_SCHEMA,
        source_name="synthetic_g4",
        options=ImportOptions(
            selected_fields=SUBSET_SELECTED,
            keep_extra_fields=True,
        ),
        provenance=BASE_PROVENANCE,
        h3_resolution=8,
    )

    required_cols = set(BASE_TRIP_SCHEMA.required)
    expected_subset_schema_cols = required_cols | set(SUBSET_SELECTED)
    dropped_optional_schema = SCHEMA_OPTIONAL_PRESENT_IN_SOURCE - set(SUBSET_SELECTED)

    assert report.ok is True
    assert len(trips.data) == 1200

    final_cols = set(trips.data.columns)
    assert expected_subset_schema_cols.issubset(final_cols)
    assert final_cols.isdisjoint(dropped_optional_schema)
    assert EXTRA_COLS_PRESENT_IN_SOURCE.issubset(final_cols)

    assert set(trips.metadata["schema_effective"]["fields_effective"]) == expected_subset_schema_cols
    assert set(trips.metadata["extra_fields_kept"]) == EXTRA_COLS_PRESENT_IN_SOURCE
    assert set(report.metadata["metadata"]["extra_fields_kept"]) == EXTRA_COLS_PRESENT_IN_SOURCE

    event = trips.metadata["events"][-1]
    assert event["summary"]["input_rows"] == 1200
    assert event["summary"]["output_rows"] == 1200
    assert set(event["summary"]["columns_deleted"]) >= dropped_optional_schema

    assert report.parameters["selected_fields"] == SUBSET_SELECTED
    assert report.parameters["keep_extra_fields"] is True