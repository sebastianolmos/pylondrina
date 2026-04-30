import math

import pandas as pd
import pytest
import h3

from pylondrina.errors import ImportError as PylondrinaImportError
from pylondrina.importing import _derive_h3_indices, _parse_od_coordinate_columns
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


def get_issue(issues, code: str):
    for issue in issues:
        if issue.code == code:
            return issue
    raise AssertionError(f"No se encontró issue {code!r}. Issues encontrados: {issue_codes(issues)!r}")


# ---------------------------------------------------------------------
# Fixtures: coordenadas
# ---------------------------------------------------------------------


@pytest.fixture
def schema_coords_optional() -> TripSchema:
    return TripSchema(
        version="0.1.0",
        fields={
            "origin_latitude": FieldSpec(name="origin_latitude", dtype="float", required=False),
            "origin_longitude": FieldSpec(name="origin_longitude", dtype="float", required=False),
            "destination_latitude": FieldSpec(name="destination_latitude", dtype="float", required=False),
            "destination_longitude": FieldSpec(name="destination_longitude", dtype="float", required=False),
        },
        required=[],
        semantic_rules=None,
    )


@pytest.fixture
def schema_coords_required() -> TripSchema:
    return TripSchema(
        version="0.1.0",
        fields={
            "origin_latitude": FieldSpec(name="origin_latitude", dtype="float", required=True),
            "origin_longitude": FieldSpec(name="origin_longitude", dtype="float", required=False),
        },
        required=["origin_latitude"],
        semantic_rules=None,
    )


@pytest.fixture
def target_fields_coords() -> set[str]:
    return {
        "origin_latitude",
        "origin_longitude",
        "destination_latitude",
        "destination_longitude",
    }


# ---------------------------------------------------------------------
# Fixtures: H3
# ---------------------------------------------------------------------


@pytest.fixture
def schema_h3_optional() -> TripSchema:
    return TripSchema(
        version="0.1.0",
        fields={
            "origin_latitude": FieldSpec(name="origin_latitude", dtype="float", required=False),
            "origin_longitude": FieldSpec(name="origin_longitude", dtype="float", required=False),
            "destination_latitude": FieldSpec(name="destination_latitude", dtype="float", required=False),
            "destination_longitude": FieldSpec(name="destination_longitude", dtype="float", required=False),
            "origin_h3_index": FieldSpec(name="origin_h3_index", dtype="string", required=False),
            "destination_h3_index": FieldSpec(name="destination_h3_index", dtype="string", required=False),
        },
        required=[],
        semantic_rules=None,
    )


@pytest.fixture
def schema_h3_required_origin() -> TripSchema:
    return TripSchema(
        version="0.1.0",
        fields={
            "origin_latitude": FieldSpec(name="origin_latitude", dtype="float", required=False),
            "origin_longitude": FieldSpec(name="origin_longitude", dtype="float", required=False),
            "origin_h3_index": FieldSpec(name="origin_h3_index", dtype="string", required=True),
        },
        required=["origin_h3_index"],
        semantic_rules=None,
    )


@pytest.fixture
def schema_h3_required_destination() -> TripSchema:
    return TripSchema(
        version="0.1.0",
        fields={
            "destination_latitude": FieldSpec(name="destination_latitude", dtype="float", required=False),
            "destination_longitude": FieldSpec(name="destination_longitude", dtype="float", required=False),
            "destination_h3_index": FieldSpec(name="destination_h3_index", dtype="string", required=True),
        },
        required=["destination_h3_index"],
        semantic_rules=None,
    )


# ---------------------------------------------------------------------
# Tests de _parse_od_coordinate_columns
# ---------------------------------------------------------------------


def test_parse_od_coordinate_columns_supports_dd_dm_dms_and_invalid_values(
    schema_coords_optional,
    target_fields_coords,
):
    """Verifica parseo OD de coordenadas en DD, DM y DMS, dejando valores no parseables como NaN."""
    df = pd.DataFrame(
        {
            "origin_latitude": ["-33.446160", "33 27.0000 S", "33 27 00 S", "abc"],
            "origin_longitude": ["-70.572755", "70 34.3653 W", "70 30 00 W", "xyz"],
            "destination_latitude": ["-33.392693", pd.NA, "33 20 00 S", ""],
            "destination_longitude": ["-70.517930", None, "70 31 00 W", "  "],
        }
    )

    work, coord_stats, issues = _parse_od_coordinate_columns(
        df.copy(deep=True),
        schema=schema_coords_optional,
        target_schema_fields=target_fields_coords,
        strict=False,
    )

    assert_dtype(work, "origin_latitude", "float64")
    assert_dtype(work, "origin_longitude", "float64")
    assert_dtype(work, "destination_latitude", "float64")
    assert_dtype(work, "destination_longitude", "float64")

    assert math.isclose(work.loc[0, "origin_latitude"], -33.446160, rel_tol=0, abs_tol=1e-9)
    assert math.isclose(work.loc[0, "origin_longitude"], -70.572755, rel_tol=0, abs_tol=1e-9)

    assert math.isclose(work.loc[1, "origin_latitude"], -33.45, rel_tol=0, abs_tol=1e-9)
    assert math.isclose(work.loc[1, "origin_longitude"], -(70 + 34.3653 / 60.0), rel_tol=0, abs_tol=1e-9)

    assert math.isclose(work.loc[2, "origin_latitude"], -33.45, rel_tol=0, abs_tol=1e-9)
    assert math.isclose(work.loc[2, "origin_longitude"], -70.5, rel_tol=0, abs_tol=1e-9)

    assert pd.isna(work.loc[3, "origin_latitude"])
    assert pd.isna(work.loc[3, "origin_longitude"])
    assert pd.isna(work.loc[3, "destination_latitude"])
    assert pd.isna(work.loc[3, "destination_longitude"])

    assert "origin_latitude" in coord_stats
    assert "origin_longitude" in coord_stats
    assert coord_stats["origin_latitude"]["parse_fail_count"] == 1
    assert coord_stats["origin_longitude"]["parse_fail_count"] == 1

    assert_issue_present(issues, "IMP.TYPE.COERCE_PARTIAL")


def test_parse_od_coordinate_columns_required_field_fully_unusable_raises(
    schema_coords_required,
):
    """Verifica que una coordenada requerida totalmente inutilizable produzca ImportError."""
    df = pd.DataFrame(
        {
            "origin_latitude": ["abc", "def", "ghi"],
            "origin_longitude": ["-70.6", "-70.7", "-70.8"],
        }
    )

    with pytest.raises(PylondrinaImportError) as exc_info:
        _parse_od_coordinate_columns(
            df.copy(deep=True),
            schema=schema_coords_required,
            target_schema_fields={"origin_latitude", "origin_longitude"},
            strict=False,
        )

    assert_issue_present(exc_info.value.issues, "IMP.TYPE.COERCE_FAILED_REQUIRED")

    issue = exc_info.value.issue
    assert issue.code == "IMP.TYPE.COERCE_FAILED_REQUIRED"
    assert issue.field == "origin_latitude"
    assert issue.details["dtype_expected"] == "float"
    assert issue.details["parse_fail_count"] == 3
    assert issue.details["rows_in"] == 3


def test_parse_od_coordinate_columns_respects_target_schema_fields(
    schema_coords_optional,
):
    """Verifica que solo se parseen coordenadas incluidas en target_schema_fields."""
    df = pd.DataFrame(
        {
            "origin_latitude": ["33 27 00 S"],
            "origin_longitude": ["70 30 00 W"],
            "destination_latitude": ["abc"],
            "destination_longitude": ["xyz"],
        }
    )

    work, coord_stats, issues = _parse_od_coordinate_columns(
        df.copy(deep=True),
        schema=schema_coords_optional,
        target_schema_fields={"origin_latitude", "origin_longitude"},
        strict=False,
    )

    assert math.isclose(work.loc[0, "origin_latitude"], -33.45, rel_tol=0, abs_tol=1e-9)
    assert math.isclose(work.loc[0, "origin_longitude"], -70.5, rel_tol=0, abs_tol=1e-9)

    assert work.loc[0, "destination_latitude"] == "abc"
    assert work.loc[0, "destination_longitude"] == "xyz"

    assert set(coord_stats.keys()) == {"origin_latitude", "origin_longitude"}
    assert issues == []


# ---------------------------------------------------------------------
# Tests de _derive_h3_indices
# ---------------------------------------------------------------------


def test_derive_h3_indices_with_complete_origin_and_destination_coordinates(schema_h3_optional):
    """Verifica derivación H3 de origen y destino cuando todos los pares lat/lon están completos."""
    df = pd.DataFrame(
        {
            "origin_latitude": [-33.446160, -33.392693],
            "origin_longitude": [-70.572755, -70.517930],
            "destination_latitude": [-33.447000, -33.390000],
            "destination_longitude": [-70.570000, -70.510000],
        }
    )

    work, h3_meta, columns_added, issues = _derive_h3_indices(
        df.copy(deep=True),
        schema=schema_h3_optional,
        h3_resolution=8,
        strict=False,
    )

    assert "origin_h3_index" in work.columns
    assert "destination_h3_index" in work.columns
    assert_dtype(work, "origin_h3_index", "string")
    assert_dtype(work, "destination_h3_index", "string")

    expected_o0 = h3.latlng_to_cell(-33.446160, -70.572755, 8)
    expected_o1 = h3.latlng_to_cell(-33.392693, -70.517930, 8)
    expected_d0 = h3.latlng_to_cell(-33.447000, -70.570000, 8)
    expected_d1 = h3.latlng_to_cell(-33.390000, -70.510000, 8)

    assert work["origin_h3_index"].tolist() == [expected_o0, expected_o1]
    assert work["destination_h3_index"].tolist() == [expected_d0, expected_d1]

    assert columns_added == ["origin_h3_index", "destination_h3_index"]
    assert h3_meta["resolution"] == 8
    assert h3_meta["source_fields"] == [
        ["origin_latitude", "origin_longitude"],
        ["destination_latitude", "destination_longitude"],
    ]
    assert h3_meta["derived_fields"] == ["origin_h3_index", "destination_h3_index"]
    assert issues == []


def test_derive_h3_indices_with_partial_null_coordinates_emits_partial_derivation(schema_h3_optional):
    """Verifica que coordenadas parcialmente nulas produzcan H3 nulos e issue de derivación parcial."""
    df = pd.DataFrame(
        {
            "origin_latitude": [-33.446160, pd.NA, -33.392693],
            "origin_longitude": [-70.572755, -70.517930, pd.NA],
            "destination_latitude": [-33.447000, -33.390000, pd.NA],
            "destination_longitude": [-70.570000, pd.NA, -70.510000],
        }
    )

    work, h3_meta, columns_added, issues = _derive_h3_indices(
        df.copy(deep=True),
        schema=schema_h3_optional,
        h3_resolution=8,
        strict=False,
    )

    assert "origin_h3_index" in work.columns
    assert "destination_h3_index" in work.columns
    assert_dtype(work, "origin_h3_index", "string")
    assert_dtype(work, "destination_h3_index", "string")

    assert pd.notna(work.loc[0, "origin_h3_index"])
    assert pd.notna(work.loc[0, "destination_h3_index"])

    assert pd.isna(work.loc[1, "origin_h3_index"])
    assert pd.isna(work.loc[2, "origin_h3_index"])
    assert pd.isna(work.loc[1, "destination_h3_index"])
    assert pd.isna(work.loc[2, "destination_h3_index"])

    assert columns_added == ["origin_h3_index", "destination_h3_index"]
    assert h3_meta["derived_fields"] == ["origin_h3_index", "destination_h3_index"]
    assert_issue_present(issues, "IMP.H3.PARTIAL_DERIVATION")


def test_derive_h3_indices_required_origin_h3_without_coordinate_pair_raises(
    schema_h3_required_origin,
):
    """Verifica que un H3 de origen requerido aborte cuando no está disponible el par lat/lon de origen."""
    df = pd.DataFrame(
        {
            "origin_latitude": [-33.446160, -33.392693],
        }
    )

    with pytest.raises(PylondrinaImportError) as exc_info:
        _derive_h3_indices(
            df.copy(deep=True),
            schema=schema_h3_required_origin,
            h3_resolution=8,
            strict=False,
        )

    assert_issue_present(exc_info.value.issues, "IMP.H3.REQUIRED_FIELDS_UNAVAILABLE")

    issue = exc_info.value.issue
    assert issue.code == "IMP.H3.REQUIRED_FIELDS_UNAVAILABLE"
    assert issue.details["required_h3_fields"] == ["origin_h3_index"]
    assert ["origin_latitude", "origin_longitude"] in issue.details["missing_pairs"]


def test_derive_h3_indices_required_destination_h3_without_coordinate_pair_raises(
    schema_h3_required_destination,
):
    """Verifica que un H3 de destino requerido aborte cuando no está disponible el par lat/lon de destino."""
    df = pd.DataFrame(
        {
            "destination_latitude": [-33.447000, -33.390000],
        }
    )

    with pytest.raises(PylondrinaImportError) as exc_info:
        _derive_h3_indices(
            df.copy(deep=True),
            schema=schema_h3_required_destination,
            h3_resolution=8,
            strict=False,
        )

    assert_issue_present(exc_info.value.issues, "IMP.H3.REQUIRED_FIELDS_UNAVAILABLE")

    issue = exc_info.value.issue
    assert issue.code == "IMP.H3.REQUIRED_FIELDS_UNAVAILABLE"
    assert issue.details["required_h3_fields"] == ["destination_h3_index"]
    assert ["destination_latitude", "destination_longitude"] in issue.details["missing_pairs"]


def test_derive_h3_indices_optional_h3_without_complete_coordinate_pairs_is_passthrough(
    schema_h3_optional,
):
    """Verifica que H3 opcionales no se creen cuando faltan pares completos de coordenadas."""
    df = pd.DataFrame(
        {
            "origin_latitude": [-33.446160, -33.392693],
            "destination_latitude": [-33.447000, -33.390000],
        }
    )

    work, h3_meta, columns_added, issues = _derive_h3_indices(
        df.copy(deep=True),
        schema=schema_h3_optional,
        h3_resolution=8,
        strict=False,
    )

    assert_columns_equal(
        work,
        ["origin_latitude", "destination_latitude"],
        "H3 opcional sin pares completos",
    )
    assert h3_meta == {}
    assert columns_added == []
    assert issues == []


def test_derive_h3_indices_invalid_resolution_current_behavior_emits_partial_derivation(
    schema_h3_optional,
):
    """Verifica el comportamiento vigente: resolución H3 inválida deja NA y emite derivación parcial."""
    df = pd.DataFrame(
        {
            "origin_latitude": [-33.446160, -33.392693],
            "origin_longitude": [-70.572755, -70.517930],
        }
    )

    work, h3_meta, columns_added, issues = _derive_h3_indices(
        df.copy(deep=True),
        schema=schema_h3_optional,
        h3_resolution=99,
        strict=False,
    )

    assert "origin_h3_index" in work.columns
    assert_dtype(work, "origin_h3_index", "string")

    assert pd.isna(work.loc[0, "origin_h3_index"])
    assert pd.isna(work.loc[1, "origin_h3_index"])

    assert columns_added == ["origin_h3_index"]
    assert h3_meta["derived_fields"] == ["origin_h3_index"]
    assert_issue_present(issues, "IMP.H3.PARTIAL_DERIVATION")
    assert_issue_absent(issues, "IMP.H3.INVALID_RESOLUTION")


def test_derive_h3_indices_integrated_origin_and_destination_with_partial_nulls(schema_h3_optional):
    """Verifica un caso integrado con H3 de origen y destino, incluyendo derivaciones parciales por nulos."""
    df = pd.DataFrame(
        {
            "origin_latitude": [-33.446160, -33.392693, pd.NA],
            "origin_longitude": [-70.572755, -70.517930, -70.510000],
            "destination_latitude": [-33.447000, pd.NA, -33.390000],
            "destination_longitude": [-70.570000, -70.515000, -70.510000],
        }
    )

    work, h3_meta, columns_added, issues = _derive_h3_indices(
        df.copy(deep=True),
        schema=schema_h3_optional,
        h3_resolution=8,
        strict=False,
    )

    assert_columns_equal(
        work,
        [
            "origin_latitude",
            "origin_longitude",
            "destination_latitude",
            "destination_longitude",
            "origin_h3_index",
            "destination_h3_index",
        ],
        "integrated H3 columns",
    )

    assert_dtype(work, "origin_h3_index", "string")
    assert_dtype(work, "destination_h3_index", "string")

    assert pd.notna(work.loc[0, "origin_h3_index"])
    assert pd.notna(work.loc[0, "destination_h3_index"])

    assert pd.isna(work.loc[2, "origin_h3_index"])
    assert pd.isna(work.loc[1, "destination_h3_index"])

    assert columns_added == ["origin_h3_index", "destination_h3_index"]
    assert h3_meta["resolution"] == 8
    assert h3_meta["derived_fields"] == ["origin_h3_index", "destination_h3_index"]
    assert_issue_present(issues, "IMP.H3.PARTIAL_DERIVATION")