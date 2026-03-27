from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Iterable, Mapping, Optional, Any

import pandas as pd
import geopandas as gpd
import osmnx as ox

from pylondrina.importing import ImportOptions
from pylondrina.schema import TripSchema
from pylondrina.sources.profile import SourceProfile
from pylondrina.types import FieldCorrespondence, ValueCorrespondence

from scripts.source_profiles.factories_foursquare.trips_defaults import (
    FOURSQUARE_TRIPS_DEFAULT_OPTIONS,
    FOURSQUARE_TRIPS_DEFAULT_FIELD_CORRESPONDENCE,
    make_foursquare_trips_default_schema,
    make_foursquare_trips_default_value_correspondence,
)


def read_foursquare_csv(path: str | Path) -> pd.DataFrame:
    return pd.read_csv(path, sep="\t", dtype=str, low_memory=False)


def get_municipalities_gdf(municipalities: Iterable[str]) -> gpd.GeoDataFrame:
    amb = ox.geocoder.geocode_to_gdf(list(municipalities))
    return amb.to_crs("EPSG:4326")


def prepare_foursquare_triplevel_for_import(
    df_venues: pd.DataFrame,
    df_checkins: pd.DataFrame,
    municipalities: list[str],
) -> pd.DataFrame:
    """
    Réplica del preprocess manual del notebook para Foursquare trips.
    """
    municipalities_gdf = get_municipalities_gdf(municipalities)

    venues = df_venues.copy()
    venues["lat"] = pd.to_numeric(venues["lat"], errors="coerce")
    venues["lon"] = pd.to_numeric(venues["lon"], errors="coerce")

    bbox = municipalities_gdf.total_bounds
    venues_bbox = venues[
        venues["lon"].between(bbox[0], bbox[2])
        & venues["lat"].between(bbox[1], bbox[3])
    ].copy()

    venues_gdf = gpd.GeoDataFrame(
        venues_bbox[["venue_id", "lat", "lon", "category", "country"]].copy(),
        geometry=gpd.points_from_xy(venues_bbox["lon"], venues_bbox["lat"]),
        crs="EPSG:4326",
    )

    venues_in_area = gpd.sjoin(
        venues_gdf,
        municipalities_gdf[["geometry"]],
        predicate="within",
        how="inner",
    )

    venues_in_area = (
        venues_in_area
        .drop(columns=["index_right"], errors="ignore")
        .groupby("venue_id", as_index=False)
        .first()
    )

    checkins = df_checkins.copy()
    checkins_in_area = checkins[checkins["venue_id"].isin(venues_in_area["venue_id"])].copy()

    checkins_enriched = checkins_in_area.merge(
        venues_in_area[["venue_id", "lat", "lon", "category"]],
        on="venue_id",
        how="left",
    )

    checkins_enriched = checkins_enriched.sort_values(
        by=["user_id", "datetime"],
        ascending=[True, True],
        kind="stable",
    ).reset_index(drop=True)

    origin = checkins_enriched.rename(
        columns={
            "lat": "origin_lat",
            "lon": "origin_lon",
            "datetime": "origin_datetime",
            "category": "origin_category",
            "venue_id": "origin_venue_id",
        }
    )[["user_id", "origin_venue_id", "origin_lat", "origin_lon", "origin_datetime", "origin_category"]]

    destination = checkins_enriched.rename(
        columns={
            "user_id": "user_id_dest",
            "lat": "destination_lat",
            "lon": "destination_lon",
            "datetime": "destination_datetime",
            "category": "destination_category",
            "venue_id": "destination_venue_id",
        }
    )[["user_id_dest", "destination_venue_id", "destination_lat", "destination_lon", "destination_datetime", "destination_category"]]

    trips = (
        origin.shift(1)
        .join(destination)
        .dropna(subset=["user_id", "user_id_dest"])
        .pipe(lambda x: x[x["user_id"] == x["user_id_dest"]])
        .drop(columns=["user_id_dest"])
        .reset_index(drop=True)
    )

    trips.index.name = "trip_id"
    trips = trips.reset_index()

    trips = trips[
        [
            "trip_id",
            "user_id",
            "origin_lat",
            "origin_lon",
            "destination_lat",
            "destination_lon",
            "origin_datetime",
            "destination_datetime",
            "origin_category",
            "destination_category",
        ]
    ].copy()

    # columna separada para evitar colisión y permitir single_stage=True
    trips["movement_id_src"] = trips["trip_id"].astype(str)

    return trips


def _merge_field_correspondence(
    base: Optional[FieldCorrespondence],
    override: Optional[FieldCorrespondence],
) -> Optional[dict[str, str]]:
    if base is None and override is None:
        return None

    merged: dict[str, str] = {}
    if base is not None:
        merged.update(dict(base))
    if override is not None:
        merged.update(dict(override))
    return merged


def _merge_value_correspondence(
    base: Optional[ValueCorrespondence],
    override: Optional[ValueCorrespondence],
) -> Optional[dict[str, dict[str, str]]]:
    if base is None and override is None:
        return None

    merged: dict[str, dict[str, str]] = {}
    if base is not None:
        for field, mapping in base.items():
            merged[field] = dict(mapping)

    if override is not None:
        for field, mapping in override.items():
            if field not in merged:
                merged[field] = dict(mapping)
            else:
                merged[field].update(dict(mapping))

    return merged


def _merge_import_options(
    base: ImportOptions,
    override: Optional[Mapping[str, Any]],
) -> ImportOptions:
    if not override:
        return base

    payload = asdict(base)
    payload.update(dict(override))
    return ImportOptions(**payload)


def _filter_field_correspondence_to_schema(
    field_corr: Optional[FieldCorrespondence],
    schema: TripSchema,
) -> Optional[dict[str, str]]:
    if field_corr is None:
        return None

    schema_fields = set(schema.fields.keys())
    return {
        canonical: source
        for canonical, source in dict(field_corr).items()
        if canonical in schema_fields
    }


def make_foursquare_trips_profile(
    *,
    df_venues: pd.DataFrame,
    municipalities: list[str],
    categories_yaml: str | Path,
    schema_override: Optional[TripSchema] = None,
    field_correspondence_override: Optional[FieldCorrespondence] = None,
    value_correspondence_override: Optional[ValueCorrespondence] = None,
    options_override: Optional[Mapping[str, Any]] = None,
    profile_name: str = "FOURSQUARE_TRIPS",
    description: Optional[str] = None,
) -> SourceProfile:
    """
    Factory nivel 3 para Foursquare trips.

    Importante:
    - reproduce el preprocess del notebook,
    - construye viajes como pares consecutivos de check-ins del mismo usuario,
    - usa categories.yaml para reducir categorías en el import.
    """
    effective_schema = schema_override or make_foursquare_trips_default_schema(categories_yaml)
    effective_options = _merge_import_options(FOURSQUARE_TRIPS_DEFAULT_OPTIONS, options_override)

    effective_field_correspondence = _merge_field_correspondence(
        FOURSQUARE_TRIPS_DEFAULT_FIELD_CORRESPONDENCE,
        field_correspondence_override,
    )
    effective_field_correspondence = _filter_field_correspondence_to_schema(
        effective_field_correspondence,
        effective_schema,
    )

    effective_value_correspondence = _merge_value_correspondence(
        make_foursquare_trips_default_value_correspondence(categories_yaml),
        value_correspondence_override,
    )

    effective_description = (
        description
        or "Foursquare trips: pares consecutivos de check-ins del mismo usuario dentro del área indicada."
    )

    def preprocess(df_checkins: pd.DataFrame) -> pd.DataFrame:
        return prepare_foursquare_triplevel_for_import(
            df_venues=df_venues,
            df_checkins=df_checkins,
            municipalities=municipalities,
        )

    return SourceProfile(
        name=profile_name,
        description=effective_description,
        default_field_correspondence=effective_field_correspondence,
        default_value_correspondence=effective_value_correspondence,
        default_options=effective_options,
        preprocess=preprocess,
        schema_override=effective_schema,
    )