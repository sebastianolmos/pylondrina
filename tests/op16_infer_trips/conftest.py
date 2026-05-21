from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from typing import Any

import pandas as pd
import pytest

from pylondrina.datasets import TraceDataset, TripDataset
from pylondrina.reports import Issue
from pylondrina.schema import DomainSpec, FieldSpec, TraceSchema, TripSchema
from pylondrina.transforms.inference import InferTripsOptions


TRACE_MIN_FIELDS: tuple[str, ...] = (
    "point_id",
    "user_id",
    "time_utc",
    "latitude",
    "longitude",
)

TRIP_MIN_FIELDS: tuple[str, ...] = (
    "movement_id",
    "user_id",
    "origin_longitude",
    "origin_latitude",
    "destination_longitude",
    "destination_latitude",
    "origin_time_utc",
    "destination_time_utc",
    "origin_h3_index",
    "destination_h3_index",
    "trip_id",
    "movement_seq",
)


def _issue_code(issue: Issue | Mapping[str, Any]) -> str | None:
    """Extrae el código de un issue representado como Issue o como mapping."""
    if hasattr(issue, "code"):
        return issue.code
    if isinstance(issue, Mapping):
        value = issue.get("code")
        return str(value) if value is not None else None
    return None


def _base_trip_fields() -> dict[str, FieldSpec]:
    """Construye los FieldSpec mínimos del TripSchema usado por OP-16."""
    return {
        "movement_id": FieldSpec(name="movement_id", dtype="string", required=True),
        "user_id": FieldSpec(name="user_id", dtype="string", required=True),
        "origin_longitude": FieldSpec(name="origin_longitude", dtype="float", required=True),
        "origin_latitude": FieldSpec(name="origin_latitude", dtype="float", required=True),
        "destination_longitude": FieldSpec(name="destination_longitude", dtype="float", required=True),
        "destination_latitude": FieldSpec(name="destination_latitude", dtype="float", required=True),
        "origin_time_utc": FieldSpec(name="origin_time_utc", dtype="datetime", required=True),
        "destination_time_utc": FieldSpec(name="destination_time_utc", dtype="datetime", required=True),
        "origin_h3_index": FieldSpec(name="origin_h3_index", dtype="string", required=True),
        "destination_h3_index": FieldSpec(name="destination_h3_index", dtype="string", required=True),
        "trip_id": FieldSpec(name="trip_id", dtype="string", required=True),
        "movement_seq": FieldSpec(name="movement_seq", dtype="int", required=True),
    }


@pytest.fixture
def trace_min_fields() -> tuple[str, ...]:
    """Entrega el núcleo canónico mínimo que OP-16 exige en TraceDataset.data."""
    return TRACE_MIN_FIELDS


@pytest.fixture
def trip_min_fields() -> tuple[str, ...]:
    """Entrega el núcleo mínimo de trips que OP-16 debe materializar."""
    return TRIP_MIN_FIELDS


@pytest.fixture
def make_issue() -> Callable[..., Issue]:
    """Construye Issue para tests helper-level de OP-16."""

    def _make_issue(level: str, code: str, message: str = "dummy") -> Issue:
        return Issue(level=level, code=code, message=message)

    return _make_issue


@pytest.fixture
def make_trace_schema_rich(trace_min_fields: tuple[str, ...]) -> Callable[[], TraceSchema]:
    """Construye el TraceSchema rico usado por tests directos y puente de OP-16."""

    def _make_trace_schema_rich() -> TraceSchema:
        fields = {
            "point_id": FieldSpec(
                name="point_id",
                dtype="string",
                required=True,
                constraints={"unique": True},
            ),
            "user_id": FieldSpec(name="user_id", dtype="string", required=True),
            "time_utc": FieldSpec(
                name="time_utc",
                dtype="datetime",
                required=True,
                constraints={"datetime": {"allow_naive": False}},
            ),
            "latitude": FieldSpec(
                name="latitude",
                dtype="float",
                required=True,
                constraints={"range": {"min": -90, "max": 90}},
            ),
            "longitude": FieldSpec(
                name="longitude",
                dtype="float",
                required=True,
                constraints={"range": {"min": -180, "max": 180}},
            ),
            "location_ref": FieldSpec(name="location_ref", dtype="string", required=False),
            "poi_cat": FieldSpec(name="poi_cat", dtype="string", required=False),
            "accuracy": FieldSpec(name="accuracy", dtype="float", required=False),
            "device_type": FieldSpec(name="device_type", dtype="string", required=False),
            "source_app": FieldSpec(name="source_app", dtype="string", required=False),
            "confidence": FieldSpec(name="confidence", dtype="float", required=False),
            "note": FieldSpec(name="note", dtype="string", required=False),
            "provider": FieldSpec(name="provider", dtype="string", required=False),
        }
        return TraceSchema(
            version="trace-v1-rich",
            fields=fields,
            required=list(trace_min_fields),
            crs="EPSG:4326",
            timezone="America/Santiago",
        )

    return _make_trace_schema_rich


@pytest.fixture
def make_trace_schema_min(trace_min_fields: tuple[str, ...]) -> Callable[[], TraceSchema]:
    """Construye un TraceSchema mínimo útil para helper-level de OP-16."""

    def _make_trace_schema_min() -> TraceSchema:
        fields = {
            "point_id": FieldSpec(name="point_id", dtype="string", required=True),
            "user_id": FieldSpec(name="user_id", dtype="string", required=True),
            "time_utc": FieldSpec(name="time_utc", dtype="datetime", required=True),
            "latitude": FieldSpec(
                name="latitude",
                dtype="float",
                required=True,
                constraints={"range": {"min": -90, "max": 90}},
            ),
            "longitude": FieldSpec(
                name="longitude",
                dtype="float",
                required=True,
                constraints={"range": {"min": -180, "max": 180}},
            ),
            "location_ref": FieldSpec(name="location_ref", dtype="string", required=False),
            "poi_cat": FieldSpec(name="poi_cat", dtype="string", required=False),
        }
        return TraceSchema(
            version="trace-v1",
            fields=fields,
            required=list(trace_min_fields),
            crs="EPSG:4326",
            timezone=None,
        )

    return _make_trace_schema_min


@pytest.fixture
def make_trip_schema_min(trip_min_fields: tuple[str, ...]) -> Callable[..., TripSchema]:
    """Construye el TripSchema mínimo de salida esperado por OP-16."""

    def _make_trip_schema_min(
        *,
        include_propagated_categoricals: bool = False,
        version: str = "trip-v1-min",
    ) -> TripSchema:
        fields = _base_trip_fields()

        if include_propagated_categoricals:
            fields["origin_poi_cat"] = FieldSpec(
                name="origin_poi_cat",
                dtype="categorical",
                required=False,
                domain=DomainSpec(values=[], extendable=True),
            )
            fields["destination_poi_cat"] = FieldSpec(
                name="destination_poi_cat",
                dtype="categorical",
                required=False,
                domain=DomainSpec(values=[], extendable=True),
            )

        return TripSchema(
            version=version,
            fields=fields,
            required=list(trip_min_fields),
        )

    return _make_trip_schema_min


@pytest.fixture
def make_trip_schema_rich_bootstrap(trip_min_fields: tuple[str, ...]) -> Callable[[], TripSchema]:
    """Construye un TripSchema rico con categóricos propagados extendibles desde dominio vacío."""

    def _make_trip_schema_rich_bootstrap() -> TripSchema:
        fields = _base_trip_fields()
        fields.update(
            {
                "origin_location_ref": FieldSpec(
                    name="origin_location_ref", dtype="string", required=False
                ),
                "destination_location_ref": FieldSpec(
                    name="destination_location_ref", dtype="string", required=False
                ),
                "origin_device_type": FieldSpec(
                    name="origin_device_type", dtype="string", required=False
                ),
                "destination_device_type": FieldSpec(
                    name="destination_device_type", dtype="string", required=False
                ),
                "origin_accuracy": FieldSpec(
                    name="origin_accuracy", dtype="float", required=False
                ),
                "destination_accuracy": FieldSpec(
                    name="destination_accuracy", dtype="float", required=False
                ),
                "origin_poi_cat": FieldSpec(
                    name="origin_poi_cat",
                    dtype="categorical",
                    required=False,
                    domain=DomainSpec(values=[], extendable=True),
                ),
                "destination_poi_cat": FieldSpec(
                    name="destination_poi_cat",
                    dtype="categorical",
                    required=False,
                    domain=DomainSpec(values=[], extendable=True),
                ),
            }
        )
        return TripSchema(
            version="trip-v1-rich-bootstrap",
            fields=fields,
            required=list(trip_min_fields),
        )

    return _make_trip_schema_rich_bootstrap


@pytest.fixture
def make_trip_schema_rich_extendable(trip_min_fields: tuple[str, ...]) -> Callable[[], TripSchema]:
    """Construye TripSchema rico con dominios base extendibles para tests de dominios."""

    def _make_trip_schema_rich_extendable() -> TripSchema:
        fields = _base_trip_fields()
        fields.update(
            {
                "origin_location_ref": FieldSpec(
                    name="origin_location_ref", dtype="string", required=False
                ),
                "destination_location_ref": FieldSpec(
                    name="destination_location_ref", dtype="string", required=False
                ),
                "origin_poi_cat": FieldSpec(
                    name="origin_poi_cat",
                    dtype="categorical",
                    required=False,
                    domain=DomainSpec(values=["home", "work"], extendable=True),
                ),
                "destination_poi_cat": FieldSpec(
                    name="destination_poi_cat",
                    dtype="categorical",
                    required=False,
                    domain=DomainSpec(values=["home", "work"], extendable=True),
                ),
            }
        )
        return TripSchema(
            version="trip-v1-rich-extendable",
            fields=fields,
            required=list(trip_min_fields),
        )

    return _make_trip_schema_rich_extendable


@pytest.fixture
def make_trip_schema_rich_blocked(trip_min_fields: tuple[str, ...]) -> Callable[[], TripSchema]:
    """Construye TripSchema rico con dominios no extendibles para strict_domains."""

    def _make_trip_schema_rich_blocked() -> TripSchema:
        fields = _base_trip_fields()
        fields.update(
            {
                "origin_poi_cat": FieldSpec(
                    name="origin_poi_cat",
                    dtype="categorical",
                    required=False,
                    domain=DomainSpec(values=["home", "work"], extendable=False),
                ),
                "destination_poi_cat": FieldSpec(
                    name="destination_poi_cat",
                    dtype="categorical",
                    required=False,
                    domain=DomainSpec(values=["home", "work"], extendable=False),
                ),
            }
        )
        return TripSchema(
            version="trip-v1-rich-blocked",
            fields=fields,
            required=list(trip_min_fields),
        )

    return _make_trip_schema_rich_blocked


@pytest.fixture
def make_trace_points_rich_df() -> Callable[[], pd.DataFrame]:
    """Construye una traza canónica rica para inferencia directa consecutive_points."""

    def _make_trace_points_rich_df() -> pd.DataFrame:
        return pd.DataFrame(
            {
                "point_id": ["p0", "p1", "p2", "p3", "p4", "q0", "q1", "q2", "q3"],
                "user_id": ["u1", "u1", "u1", "u1", "u1", "u2", "u2", "u2", "u2"],
                "time_utc": pd.to_datetime(
                    [
                        "2026-03-10T08:00:00Z",
                        "2026-03-10T08:25:00Z",
                        "2026-03-10T09:10:00Z",
                        "2026-03-10T12:30:00Z",
                        "2026-03-10T18:10:00Z",
                        "2026-03-10T07:40:00Z",
                        "2026-03-10T08:15:00Z",
                        "2026-03-10T17:35:00Z",
                        "2026-03-10T19:05:00Z",
                    ],
                    utc=True,
                ),
                "latitude": [
                    -33.4500,
                    -33.4560,
                    -33.4600,
                    -33.4550,
                    -33.4505,
                    -33.4700,
                    -33.4725,
                    -33.4680,
                    -33.4705,
                ],
                "longitude": [
                    -70.6600,
                    -70.6500,
                    -70.6400,
                    -70.6450,
                    -70.6605,
                    -70.6800,
                    -70.6720,
                    -70.6760,
                    -70.6805,
                ],
                "location_ref": [
                    "home_u1_am",
                    "cafe_u1",
                    "work_u1",
                    "lunch_u1",
                    "home_u1_pm",
                    "home_u2_am",
                    "school_u2",
                    "gym_u2",
                    "home_u2_pm",
                ],
                "poi_cat": [
                    "home",
                    "food",
                    "work",
                    "food",
                    "home",
                    "home",
                    "education",
                    "leisure",
                    "home",
                ],
                "accuracy": [5.0, 8.0, 7.0, 9.0, 6.0, 6.0, 7.0, 5.0, 6.0],
                "device_type": ["phone"] * 5 + ["watch"] * 4,
                "source_app": ["foursquare"] * 5 + ["app_b"] * 4,
                "confidence": [0.95, 0.92, 0.93, 0.91, 0.94, 0.90, 0.89, 0.88, 0.90],
                "note": [
                    "u1_start",
                    "u1_coffee",
                    "u1_work",
                    "u1_lunch",
                    "u1_return",
                    "u2_start",
                    "u2_school",
                    "u2_gym",
                    "u2_return",
                ],
                "provider": ["provider_a"] * 5 + ["provider_b"] * 4,
            }
        )

    return _make_trace_points_rich_df


@pytest.fixture
def make_trace_clusters_rich_df() -> Callable[[], pd.DataFrame]:
    """Construye trazas con bursts secuenciales para inferencia consecutive_clusters."""

    def _make_trace_clusters_rich_df() -> pd.DataFrame:
        return pd.DataFrame(
            {
                "point_id": [
                    "p0",
                    "p1",
                    "p2",
                    "p3",
                    "p4",
                    "p5",
                    "q0",
                    "q1",
                    "q2",
                    "q3",
                    "q4",
                    "q5",
                ],
                "user_id": ["u1"] * 6 + ["u2"] * 6,
                "time_utc": pd.to_datetime(
                    [
                        "2026-03-11T08:00:00Z",
                        "2026-03-11T08:03:00Z",
                        "2026-03-11T08:40:00Z",
                        "2026-03-11T08:41:00Z",
                        "2026-03-11T09:20:00Z",
                        "2026-03-11T09:23:00Z",
                        "2026-03-11T07:30:00Z",
                        "2026-03-11T07:32:00Z",
                        "2026-03-11T08:15:00Z",
                        "2026-03-11T08:16:00Z",
                        "2026-03-11T18:10:00Z",
                        "2026-03-11T18:12:00Z",
                    ],
                    utc=True,
                ),
                "latitude": [
                    -33.4500,
                    -33.4501,
                    -33.4550,
                    -33.4551,
                    -33.4600,
                    -33.4601,
                    -33.4700,
                    -33.4701,
                    -33.4725,
                    -33.4726,
                    -33.4703,
                    -33.4704,
                ],
                "longitude": [
                    -70.6600,
                    -70.6601,
                    -70.6500,
                    -70.6501,
                    -70.6400,
                    -70.6401,
                    -70.6800,
                    -70.6801,
                    -70.6720,
                    -70.6721,
                    -70.6803,
                    -70.6804,
                ],
                "location_ref": [
                    "home_u1_a",
                    "home_u1_b",
                    "cafe_u1_a",
                    "cafe_u1_b",
                    "work_u1_a",
                    "work_u1_b",
                    "home_u2_a",
                    "home_u2_b",
                    "school_u2_a",
                    "school_u2_b",
                    "home_u2_pm_a",
                    "home_u2_pm_b",
                ],
                "poi_cat": [
                    "home",
                    "home",
                    "food",
                    "food",
                    "work",
                    "work",
                    "home",
                    "home",
                    "education",
                    "education",
                    "home",
                    "home",
                ],
                "accuracy": [4.0, 5.0, 6.0, 6.5, 5.5, 5.0, 4.5, 4.8, 6.2, 6.1, 4.9, 5.2],
                "device_type": ["phone"] * 6 + ["watch"] * 6,
                "source_app": ["foursquare"] * 6 + ["app_b"] * 6,
                "confidence": [0.97, 0.96, 0.93, 0.92, 0.95, 0.94, 0.91, 0.90, 0.89, 0.88, 0.92, 0.91],
                "note": [
                    "u1_home_a",
                    "u1_home_b",
                    "u1_cafe_a",
                    "u1_cafe_b",
                    "u1_work_a",
                    "u1_work_b",
                    "u2_home_a",
                    "u2_home_b",
                    "u2_school_a",
                    "u2_school_b",
                    "u2_home_pm_a",
                    "u2_home_pm_b",
                ],
                "provider": ["provider_a"] * 6 + ["provider_b"] * 6,
            }
        )

    return _make_trace_clusters_rich_df


@pytest.fixture
def make_trace_points_df() -> Callable[[], pd.DataFrame]:
    """Construye una traza mínima usada por tests helper-level de OP-16."""

    def _make_trace_points_df() -> pd.DataFrame:
        return pd.DataFrame(
            {
                "point_id": ["p0", "p1", "p2", "p3"],
                "user_id": ["u1", "u1", "u1", "u2"],
                "time_utc": [
                    "2026-01-01T08:00:00Z",
                    "2026-01-01T08:10:00Z",
                    "2026-01-01T09:00:00Z",
                    "2026-01-01T08:05:00Z",
                ],
                "latitude": [-33.45, -33.46, -33.461, -33.40],
                "longitude": [-70.66, -70.67, -70.671, -70.65],
                "location_ref": ["A", "B", "B", "C"],
                "poi_cat": ["home", "work", "work", "shop"],
            }
        )

    return _make_trace_points_df


@pytest.fixture
def make_trace_clusters_df() -> Callable[[], pd.DataFrame]:
    """Construye una traza mínima con dos bursts claros para helper-level."""

    def _make_trace_clusters_df() -> pd.DataFrame:
        return pd.DataFrame(
            {
                "point_id": ["p0", "p1", "p2", "p3"],
                "user_id": ["u1", "u1", "u1", "u1"],
                "time_utc": [
                    "2026-01-01T08:00:00Z",
                    "2026-01-01T08:02:00Z",
                    "2026-01-01T08:30:00Z",
                    "2026-01-01T08:31:00Z",
                ],
                "latitude": [-33.45, -33.4501, -33.46, -33.4601],
                "longitude": [-70.66, -70.6601, -70.67, -70.6701],
                "location_ref": ["A", "A", "B", "B"],
                "poi_cat": ["home", "home", "work", "work"],
            }
        )

    return _make_trace_clusters_df


@pytest.fixture
def make_trace_dataset(
    make_trace_points_df: Callable[[], pd.DataFrame],
    make_trace_schema_rich: Callable[[], TraceSchema],
) -> Callable[..., TraceDataset]:
    """Construye TraceDataset con copias defensivas para tests directos y helper-level."""

    def _make_trace_dataset(
        df: pd.DataFrame | None = None,
        *,
        validated: bool = True,
        dataset_id: str = "trace_ds_001",
        events: list[dict[str, Any]] | None = None,
        schema: TraceSchema | None = None,
        provenance: dict[str, Any] | None = None,
        metadata_extra: dict[str, Any] | None = None,
    ) -> TraceDataset:
        if df is None:
            df = make_trace_points_df()

        schema_eff = schema or make_trace_schema_rich()

        if events is None:
            events = [{"op": "import_traces"}, {"op": "validate_traces"}] if validated else []

        metadata = {
            "dataset_id": dataset_id,
            "schema_version": schema_eff.version,
            "is_validated": validated,
            "events": deepcopy(events),
        }
        metadata.update(deepcopy(metadata_extra or {}))

        return TraceDataset(
            data=df.copy(deep=True),
            schema=schema_eff,
            provenance=deepcopy(
                provenance
                if provenance is not None
                else {"source_name": "synthetic", "fixture": dataset_id}
            ),
            metadata=metadata,
        )

    return _make_trace_dataset


@pytest.fixture
def trace_points_validated(
    make_trace_points_rich_df: Callable[[], pd.DataFrame],
    make_trace_dataset: Callable[..., TraceDataset],
) -> TraceDataset:
    """Entrega un TraceDataset rico y validado para tests directos en consecutive_points."""
    return make_trace_dataset(
        make_trace_points_rich_df(),
        validated=True,
        dataset_id="trace_points_validated_rich",
    )


@pytest.fixture
def trace_clusters_validated(
    make_trace_clusters_rich_df: Callable[[], pd.DataFrame],
    make_trace_dataset: Callable[..., TraceDataset],
) -> TraceDataset:
    """Entrega un TraceDataset rico y validado para tests directos en consecutive_clusters."""
    return make_trace_dataset(
        make_trace_clusters_rich_df(),
        validated=True,
        dataset_id="trace_clusters_validated_rich",
    )


@pytest.fixture
def trace_points_unvalidated(
    make_trace_points_rich_df: Callable[[], pd.DataFrame],
    make_trace_dataset: Callable[..., TraceDataset],
) -> TraceDataset:
    """Entrega un TraceDataset no validado para precondiciones y bypass de OP-16."""
    return make_trace_dataset(
        make_trace_points_rich_df(),
        validated=False,
        dataset_id="trace_points_unvalidated",
        events=[{"op": "import_traces"}],
    )


@pytest.fixture
def raw_field_map_no_point_id() -> dict[str, str]:
    """Entrega field_correspondence para raw traces sin point_id en tests puente."""
    return {
        "user_id": "uid",
        "time_utc": "observed_local",
        "latitude": "lat_src",
        "longitude": "lon_src",
        "location_ref": "venue_id",
        "poi_cat": "venue_cat",
        "accuracy": "accuracy_m",
        "device_type": "device_src",
        "source_app": "source_app_raw",
        "confidence": "confidence_score",
        "note": "note_raw",
        "provider": "provider_name",
    }


@pytest.fixture
def raw_field_map_with_point_id() -> dict[str, str]:
    """Entrega field_correspondence para raw traces con point_id preservado."""
    return {
        "point_id": "raw_pid",
        "user_id": "uid",
        "time_utc": "observed_local",
        "latitude": "lat_src",
        "longitude": "lon_src",
        "location_ref": "venue_id",
        "poi_cat": "venue_cat",
        "accuracy": "accuracy_m",
        "device_type": "device_src",
        "source_app": "source_app_raw",
        "confidence": "confidence_score",
        "note": "note_raw",
        "provider": "provider_name",
    }


@pytest.fixture
def make_raw_points_no_pointid_df(
    make_trace_points_rich_df: Callable[[], pd.DataFrame],
) -> Callable[[], pd.DataFrame]:
    """Construye raw dataframe sin point_id para puente import -> validate -> infer."""

    def _make_raw_points_no_pointid_df() -> pd.DataFrame:
        base = make_trace_points_rich_df().copy(deep=True)
        local_naive = (
            base["time_utc"]
            .dt.tz_convert("America/Santiago")
            .dt.strftime("%Y-%m-%d %H:%M:%S")
        )
        return pd.DataFrame(
            {
                "uid": base["user_id"],
                "observed_local": local_naive,
                "lat_src": base["latitude"],
                "lon_src": base["longitude"],
                "venue_id": base["location_ref"],
                "venue_cat": base["poi_cat"],
                "accuracy_m": base["accuracy"],
                "device_src": base["device_type"],
                "source_app_raw": base["source_app"],
                "confidence_score": base["confidence"],
                "note_raw": base["note"],
                "provider_name": base["provider"],
                "raw_batch": ["batch_A"] * len(base),
            }
        )

    return _make_raw_points_no_pointid_df


@pytest.fixture
def make_raw_clusters_with_pointid_df(
    make_trace_clusters_rich_df: Callable[[], pd.DataFrame],
) -> Callable[[], pd.DataFrame]:
    """Construye raw dataframe con point_id para puente clusters de OP-16."""

    def _make_raw_clusters_with_pointid_df() -> pd.DataFrame:
        base = make_trace_clusters_rich_df().copy(deep=True)
        local_naive = (
            base["time_utc"]
            .dt.tz_convert("America/Santiago")
            .dt.strftime("%Y-%m-%d %H:%M:%S")
        )
        return pd.DataFrame(
            {
                "raw_pid": base["point_id"],
                "uid": base["user_id"],
                "observed_local": local_naive,
                "lat_src": base["latitude"],
                "lon_src": base["longitude"],
                "venue_id": base["location_ref"],
                "venue_cat": base["poi_cat"],
                "accuracy_m": base["accuracy"],
                "device_src": base["device_type"],
                "source_app_raw": base["source_app"],
                "confidence_score": base["confidence"],
                "note_raw": base["note"],
                "provider_name": base["provider"],
                "raw_batch": ["batch_B"] * len(base),
                "raw_quality_flag": ["ok"] * len(base),
            }
        )

    return _make_raw_clusters_with_pointid_df


@pytest.fixture
def make_points_options() -> Callable[..., InferTripsOptions]:
    """Construye InferTripsOptions para modo consecutive_points con defaults de OP-16."""

    def _make_points_options(**overrides: Any) -> InferTripsOptions:
        payload = {
            "infer_mode": "consecutive_points",
            "strict": False,
            "strict_domains": False,
            "require_validated_traces": True,
            "drop_invalid": True,
            "h3_resolution": 8,
            "max_time_delta_s": None,
            "min_time_delta_s": None,
            "min_distance_m": None,
            "cluster_radius_m": None,
            "cluster_max_time_gap_s": None,
            "propagate_trace_fields": None,
        }
        payload.update(overrides)
        return InferTripsOptions(**payload)

    return _make_points_options


@pytest.fixture
def make_cluster_options() -> Callable[..., InferTripsOptions]:
    """Construye InferTripsOptions para modo consecutive_clusters con defaults completos."""

    def _make_cluster_options(**overrides: Any) -> InferTripsOptions:
        payload = {
            "infer_mode": "consecutive_clusters",
            "strict": False,
            "strict_domains": False,
            "require_validated_traces": True,
            "drop_invalid": True,
            "h3_resolution": 8,
            "max_time_delta_s": None,
            "min_time_delta_s": None,
            "min_distance_m": None,
            "cluster_radius_m": 50.0,
            "cluster_max_time_gap_s": 300.0,
            "propagate_trace_fields": None,
        }
        payload.update(overrides)
        return InferTripsOptions(**payload)

    return _make_cluster_options


@pytest.fixture
def clone_tracedataset() -> Callable[[TraceDataset], TraceDataset]:
    """Entrega un clon profundo de TraceDataset para evitar contaminación entre tests."""

    def _clone_tracedataset(traces: TraceDataset) -> TraceDataset:
        return TraceDataset(
            data=traces.data.copy(deep=True),
            schema=deepcopy(traces.schema),
            provenance=deepcopy(traces.provenance),
            metadata=deepcopy(traces.metadata),
        )

    return _clone_tracedataset


@pytest.fixture
def get_issue_codes() -> Callable[[Sequence[Issue | Mapping[str, Any]]], list[str | None]]:
    """Entrega un helper para extraer códigos desde una secuencia de issues."""

    def _get_issue_codes(issues: Sequence[Issue | Mapping[str, Any]]) -> list[str | None]:
        return [_issue_code(issue) for issue in issues]

    return _get_issue_codes


@pytest.fixture
def assert_issue_present(
    get_issue_codes: Callable[[Sequence[Issue | Mapping[str, Any]]], list[str | None]],
) -> Callable[[Any, str], None]:
    """Entrega un assert reutilizable para exigir la presencia de un issue code."""

    def _assert_issue_present(report_or_issues: Any, code: str) -> None:
        issues = report_or_issues.issues if hasattr(report_or_issues, "issues") else report_or_issues
        codes = get_issue_codes(issues)
        assert code in codes, f"No se encontró el issue {code}. Codes actuales: {codes}"

    return _assert_issue_present


@pytest.fixture
def assert_issue_absent(
    get_issue_codes: Callable[[Sequence[Issue | Mapping[str, Any]]], list[str | None]],
) -> Callable[[Any, str], None]:
    """Entrega un assert reutilizable para exigir ausencia de un issue code."""

    def _assert_issue_absent(report_or_issues: Any, code: str) -> None:
        issues = report_or_issues.issues if hasattr(report_or_issues, "issues") else report_or_issues
        codes = get_issue_codes(issues)
        assert code not in codes, (
            f"Se encontró inesperadamente el issue {code}. Codes actuales: {codes}"
        )

    return _assert_issue_absent


@pytest.fixture
def assert_json_safe() -> Callable[[Any, str], None]:
    """Entrega un assert para verificar serialización JSON sin coerción externa."""

    def _assert_json_safe(obj: Any, label: str = "object") -> None:
        try:
            json.dumps(obj)
        except TypeError as exc:
            raise AssertionError(f"{label} no es JSON-safe: {exc}") from exc

    return _assert_json_safe