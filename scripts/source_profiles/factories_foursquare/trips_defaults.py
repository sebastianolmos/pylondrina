from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from pylondrina.importing import ImportOptions
from pylondrina.schema import TripSchema, FieldSpec, DomainSpec
from pylondrina.types import FieldCorrespondence, ValueCorrespondence


def load_yaml_file(path: str | Path) -> dict[str, Any]:
    path = Path(path)
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def clean_domain_values(values: list[Any]) -> list[str]:
    out = []
    seen = set()

    for v in values:
        s = str(v).strip()
        if not s:
            continue
        if s not in seen:
            seen.add(s)
            out.append(s)

    return out


def clean_domain_dict(domains_raw: dict[str, list[Any]]) -> dict[str, list[str]]:
    return {k: clean_domain_values(v) for k, v in domains_raw.items()}


def invert_grouped_category_yaml(grouped_yaml: dict[str, list[str]]) -> dict[str, str]:
    inverted: dict[str, str] = {}

    for group_name, values in grouped_yaml.items():
        for raw_value in values:
            raw_value = str(raw_value).strip()
            if not raw_value:
                continue
            inverted[raw_value] = group_name

    return inverted


BASE_GOLONDRINA_IMPORT_SCHEMA = TripSchema(
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
                    "walk", "bicycle", "scooter", "motorcycle", "car",
                    "taxi", "ride_hailing", "bus", "metro", "train", "other"
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
                    "home", "work", "education", "shopping",
                    "errand", "health", "leisure", "transfer", "other"
                ],
                extendable=True,
            ),
        ),
        "day_type": FieldSpec(
            "day_type",
            "categorical",
            required=False,
            domain=DomainSpec(
                values=["weekday", "weekend", "holiday"],
                extendable=True,
            ),
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
            domain=DomainSpec(
                values=["female", "male", "other", "unknown"],
                extendable=True,
            ),
        ),

        "origin_time_local_hhmm": FieldSpec("origin_time_local_hhmm", "string", required=False),
        "destination_time_local_hhmm": FieldSpec("destination_time_local_hhmm", "string", required=False),
        "origin_municipality": FieldSpec("origin_municipality", "string", required=False),
        "destination_municipality": FieldSpec("destination_municipality", "string", required=False),
        "trip_weight": FieldSpec("trip_weight", "float", required=False),
        "mode_sequence": FieldSpec("mode_sequence", "string", required=False),
        "user_age_group": FieldSpec("user_age_group", "categorical", required=False),
        "income_quintile": FieldSpec("income_quintile", "categorical", required=False),
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


def make_foursquare_trips_default_schema(foursquare_categories_yaml: str | Path) -> TripSchema:
    grouped = clean_domain_dict(load_yaml_file(foursquare_categories_yaml))
    reduced_groups = list(grouped.keys())

    extra_fields = {
        "origin_category": FieldSpec(
            "origin_category",
            "categorical",
            required=False,
            domain=DomainSpec(values=reduced_groups, extendable=True),
        ),
        "destination_category": FieldSpec(
            "destination_category",
            "categorical",
            required=False,
            domain=DomainSpec(values=reduced_groups, extendable=True),
        ),
    }

    return TripSchema(
        version=BASE_GOLONDRINA_IMPORT_SCHEMA.version,
        fields={**BASE_GOLONDRINA_IMPORT_SCHEMA.fields, **extra_fields},
        required=list(BASE_GOLONDRINA_IMPORT_SCHEMA.required),
    )


FOURSQUARE_TRIPS_DEFAULT_OPTIONS = ImportOptions(
    keep_extra_fields=True,
    selected_fields=None,
    strict=False,
    strict_domains=False,
    single_stage=True,
    source_timezone=None,
)

# Sin colisiones:
# movement_id usa la columna source "movement_id_src"
# trip_id y movement_seq se derivan por single_stage=True
FOURSQUARE_TRIPS_DEFAULT_FIELD_CORRESPONDENCE: FieldCorrespondence = {
    "user_id": "user_id",
    "movement_id": "movement_id_src",
    "origin_longitude": "origin_lon",
    "origin_latitude": "origin_lat",
    "destination_longitude": "destination_lon",
    "destination_latitude": "destination_lat",
    "origin_time_utc": "origin_datetime",
    "destination_time_utc": "destination_datetime",
    "origin_category": "origin_category",
    "destination_category": "destination_category",
}


def make_foursquare_trips_default_value_correspondence(
    foursquare_categories_yaml: str | Path,
) -> ValueCorrespondence:
    grouped = load_yaml_file(foursquare_categories_yaml)
    raw_to_group = invert_grouped_category_yaml(grouped)

    return {
        "origin_category": dict(raw_to_group),
        "destination_category": dict(raw_to_group),
    }


FOURSQUARE_TRIPS_DEFAULT_PROVENANCE_EXAMPLE = {
    "source": {
        "name": "Foursquare",
        "profile": "FOURSQUARE_TRIPS",
        "entity": "trips",
        "version": "checkins-pois-level3",
    },
    "notes": [
        "factory nivel 3 para Foursquare trips",
        "preprocess: pares consecutivos de check-ins del mismo usuario",
        "usa categories.yaml para reducir categorías",
    ],
}


# -------------------------------------------------------------------------
# Objetos custom independientes
# -------------------------------------------------------------------------

def make_foursquare_trips_custom_schema(foursquare_categories_yaml: str | Path) -> TripSchema:
    grouped = clean_domain_dict(load_yaml_file(foursquare_categories_yaml))
    reduced_groups = list(grouped.keys())

    return TripSchema(
        version="1.1-foursquare-custom",
        fields={
            "movement_id": FieldSpec("movement_id", "string", required=True),
            "user_id": FieldSpec("user_id", "string", required=True),
            "origin_longitude": FieldSpec("origin_longitude", "float", required=True),
            "origin_latitude": FieldSpec("origin_latitude", "float", required=True),
            "destination_longitude": FieldSpec("destination_longitude", "float", required=True),
            "destination_latitude": FieldSpec("destination_latitude", "float", required=True),
            "origin_h3_index": FieldSpec("origin_h3_index", "string", required=True),
            "destination_h3_index": FieldSpec("destination_h3_index", "string", required=True),
            "trip_id": FieldSpec("trip_id", "string", required=True),
            "movement_seq": FieldSpec("movement_seq", "int", required=True),
            "origin_time_utc": FieldSpec("origin_time_utc", "datetime", required=False),
            "destination_time_utc": FieldSpec("destination_time_utc", "datetime", required=False),
            "origin_category": FieldSpec(
                "origin_category",
                "categorical",
                required=False,
                domain=DomainSpec(values=reduced_groups, extendable=True),
            ),
            "destination_category": FieldSpec(
                "destination_category",
                "categorical",
                required=False,
                domain=DomainSpec(values=reduced_groups, extendable=True),
            ),
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


FOURSQUARE_TRIPS_CUSTOM_OPTIONS = ImportOptions(
    keep_extra_fields=False,
    selected_fields=[
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
        "origin_time_utc",
        "destination_time_utc",
        "origin_category",
        "destination_category",
    ],
    strict=False,
    strict_domains=False,
    single_stage=True,
    source_timezone=None,
)

FOURSQUARE_TRIPS_CUSTOM_FIELD_CORRESPONDENCE: FieldCorrespondence = {
    "user_id": "user_id",
    "movement_id": "movement_id_src",
    "origin_longitude": "origin_lon",
    "origin_latitude": "origin_lat",
    "destination_longitude": "destination_lon",
    "destination_latitude": "destination_lat",
    "origin_time_utc": "origin_datetime",
    "destination_time_utc": "destination_datetime",
    "origin_category": "origin_category",
    "destination_category": "destination_category",
}

def make_foursquare_trips_custom_value_correspondence(
    foursquare_categories_yaml: str | Path,
) -> ValueCorrespondence:
    grouped = load_yaml_file(foursquare_categories_yaml)
    raw_to_group = invert_grouped_category_yaml(grouped)

    return {
        "origin_category": dict(raw_to_group),
        "destination_category": dict(raw_to_group),
    }

FOURSQUARE_TRIPS_CUSTOM_PROVENANCE_EXAMPLE = {
    "source": {
        "name": "Foursquare",
        "profile": "FOURSQUARE_TRIPS_CUSTOM",
        "entity": "trips",
        "version": "checkins-pois-level3",
    },
    "notes": [
        "factory nivel 3 Foursquare custom",
        "schema y mappings definidos explícitamente",
    ],
}