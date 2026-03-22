from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Literal, Optional, Sequence

import math
import re

import numpy as np
import pandas as pd

try:
    import h3  # type: ignore
except Exception:  # pragma: no cover
    h3 = None


# -----------------------------------------------------------------------------
# Pools globales - requeridos, base/opcionales, dominios y extras
# -----------------------------------------------------------------------------

REQUIRED_FIELDS_ORDER: list[str] = [
    "movement_id",
    "user_id",
    "origin_longitude",
    "origin_latitude",
    "destination_longitude",
    "destination_latitude",
    "origin_h3_index",
    "destination_h3_index",
    "origin_time_utc",
    "destination_time_utc",
    "trip_id",
    "movement_seq",
]

REQUIRED_FIELD_DTYPES: dict[str, str] = {
    "movement_id": "string",
    "user_id": "string",
    "origin_longitude": "float",
    "origin_latitude": "float",
    "destination_longitude": "float",
    "destination_latitude": "float",
    "origin_h3_index": "string",
    "destination_h3_index": "string",
    "origin_time_utc": "datetime",
    "destination_time_utc": "datetime",
    "trip_id": "string",
    "movement_seq": "int",
}

BASE_FIELD_DTYPES: dict[str, str] = {
    "origin_municipality": "string",
    "destination_municipality": "string",
    "timezone_offset_min": "int",
    "origin_time_local_hhmm": "string",
    "destination_time_local_hhmm": "string",
    "trip_weight": "float",
    "mode_sequence": "string",
    "mode": "categorical",
    "purpose": "categorical",
    "day_type": "categorical",
    "time_period": "categorical",
    "user_gender": "categorical",
    "user_age_group": "categorical",
    "income_quintile": "categorical",
}

CANONICAL_DOMAINS: dict[str, list[str]] = {
    "mode": [
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
    "purpose": [
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
    "day_type": ["weekday", "weekend", "holiday"],
    "time_period": ["night", "morning", "midday", "afternoon", "evening"],
    "user_gender": ["female", "male", "other", "unknown"],
    "user_age_group": ["0-14", "15-24", "25-34", "35-44", "45-54", "55-64", "65-plus", "unknown"],
    "income_quintile": ["1", "2", "3", "4", "5", "unknown"],
}

EXTENDABLE_DOMAIN_GROUPS: dict[str, dict[str, list[str]]] = {
    "mode": {
        "submodes": [
            "Auto Chofer",
            "Auto Acompañante",
            "Bus alimentador",
            "Bus troncal",
            "Bus institucional",
            "Bus interurbano o rural",
            "Bus urbano pago conductor",
            "Taxi colectivo",
            "Taxi o radiotaxi",
            "Furgón escolar pasajero",
            "Furgón escolar chofer",
            "Servicio informal",
            "METROTREN",
            "ZP",
        ],
        "multimodal": [
            "Bus TS",
            "Bus no TS",
            "Auto - Metro",
            "Bus TS - Metro",
            "Bus no TS - Metro",
            "Taxi Colectivo - Metro",
            "Taxi - Metro",
            "Otros - Bus TS",
            "Otros - Metro",
            "Otros - Bus TS - Metro",
        ],
    },
    "purpose": {
        "finos": [
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
        ],
        "actividad_destino": [
            "Industria",
            "Comercio",
            "Salud",
            "Educación",
            "Servicios",
            "Habitacional",
            "Sector público",
            "Otros",
        ],
        "operacionales": ["HOGAR", "TRABAJO", "OTROS", "SINBAJADA", "MENOS1MINUTO"],
        "foursquare": [
            "University",
            "Office",
            "Mall",
            "Grocery Store",
            "Coffee Shop",
            "Bar",
            "Movie Theater",
            "Hospital",
            "Medical Center",
            "Gym",
            "Bank",
        ],
    },
    "day_type": {
        "etiquetas_fuente": ["Laboral", "Fin de Semana", "DOMINGO"],
        "dia_especifico": ["lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo"],
    },
    "time_period": {
        "eod_finos": [
            "Punta Mañana 1",
            "Punta Mañana 2",
            "Fuera de Punta 1",
            "Punta Tarde",
            "Fuera de Punta 2",
            "Noche",
        ],
        "adatrap_domingo": [
            "03 - TRANSICION DOMINGO MANANA",
            "04 - MANANA DOMINGO",
            "05 - MEDIODIA DOMINGO",
            "06 - TARDE DOMINGO",
            "07 - TRANSICION DOMINGO NOCTURNO",
            "08 - PRE NOCTURNO DOMINGO",
        ],
        "metro": ["Punta", "Valle", "Bajo"],
    },
    "user_gender": {
        "es": ["Hombre", "Mujer"],
    },
    "user_age_group": {
        "fino": ["0-4", "5-13", "14-17", "18-24", "25-29", "30-44", "45-59", "60-64", "65-74", "75+", "Adulto mayor"],
    },
    "income_quintile": {
        "tramos": [
            "Menos de 200.000 pesos",
            "Entre 200.001 y 400.000 pesos",
            "Entre 400.001 y 800.000 pesos",
            "Entre 800.001 y 1.600.000 pesos",
            "Entre 1.600.001 y 2.400.000 pesos",
            "Más de 2.400.000 pesos",
        ],
        "estados": [
            "No contesta",
            "Sin Imputar",
            "Imputación por ausencia de ingreso",
            "Imputación por ausencia de ingreso y tramo",
        ],
    },
}

# Alias útiles para que el argumento extra_value_domains sea más cómodo.
DOMAIN_GROUP_ALIASES: dict[str, dict[str, str]] = {
    "mode": {
        "canon": "canon",
        "submodes": "submodes",
        "multimodal": "multimodal",
        "finos": "submodes",
    },
    "purpose": {
        "canon": "canon",
        "finos": "finos",
        "actividad_destino": "actividad_destino",
        "operacionales": "operacionales",
        "foursquare": "foursquare",
    },
    "day_type": {
        "canon": "canon",
        "etiquetas_fuente": "etiquetas_fuente",
        "finos": "etiquetas_fuente",
        "dia_especifico": "dia_especifico",
    },
    "time_period": {
        "canon": "canon",
        "eod_finos": "eod_finos",
        "finos": "eod_finos",
        "adatrap_domingo": "adatrap_domingo",
        "operacionales": "adatrap_domingo",
        "metro": "metro",
    },
    "user_gender": {
        "canon": "canon",
        "es": "es",
        "finos": "es",
    },
    "user_age_group": {
        "canon": "canon",
        "fino": "fino",
        "finos": "fino",
    },
    "income_quintile": {
        "canon": "canon",
        "tramos": "tramos",
        "estados": "estados",
        "finos": "tramos",
    },
}

CATEGORY_FIELDS: set[str] = set(CANONICAL_DOMAINS.keys())

SANTIAGO_MUNICIPALITIES = [
    "Santiago",
    "Providencia",
    "Las Condes",
    "Ñuñoa",
    "Maipú",
    "Puente Alto",
    "La Florida",
    "San Miguel",
    "Recoleta",
    "Independencia",
    "Pudahuel",
    "Quilicura",
    "Vitacura",
    "Estación Central",
]

EXTRA_CATEGORICAL_DEFAULTS: dict[str, list[str]] = {
    "activity_destination": ["trabajo", "estudio", "compra", "salud", "recreación"],
    "season": ["summer", "winter", "normal_period", "vacation"],
    "travel_time_quality_code": ["reported_exact", "reported_approx", "imputed", "missing_end"],
    "fare_payment_type": ["cash", "card", "integrated_fare", "free_transfer", "not_applicable"],
    "highway_avoid_reason": ["too_expensive", "traffic", "no_need", "prefers_local_roads"],
    "bike_lane_usage": ["always", "sometimes", "never", "not_applicable"],
    "bike_lane_issue": ["missing_segments", "unsafe", "poor_maintenance", "crowded", "none"],
    "bike_parking_type": ["none", "street_rack", "guarded", "private_building"],
    "cut_type_stage_trip": ["complete_trip", "partial_trip", "cut_at_missing_stage", "single_stage_only"],
    "travel_time_bucket": ["0-10", "11-20", "21-40", "41-60", "60+"],
    "home_tenure": ["owned", "rented", "loaned", "other"],
    "relation_to_household_head": ["head", "spouse", "child", "parent", "other_relative", "non_relative"],
    "education_level": ["none", "primary", "secondary", "technical", "university", "postgraduate"],
    "school_level": ["preschool", "primary", "secondary", "higher_ed", "not_studying"],
    "activity_status": ["working", "studying", "homemaker", "retired", "unemployed", "other"],
    "occupation_type": ["employee", "self_employed", "employer", "public_sector", "informal"],
    "job_sector": ["services", "commerce", "industry", "education", "health", "public_sector"],
    "work_schedule": ["full_time", "part_time", "shift", "flexible", "night_shift"],
    "driver_license_class": ["none", "B", "A2", "A3", "professional_other"],
    "disability_type": ["none", "mobility", "visual", "hearing", "cognitive", "other"],
    "has_income": ["yes", "no", "unknown", "imputed"],
    "personal_income_band": ["<200k", "200k-400k", "400k-800k", "800k+", "no_income"],
    "income_imputed_flag": ["not_imputed", "imputed_missing_income", "imputed_missing_band", "fully_imputed"],
}


@dataclass(frozen=True)
class ExtraFieldSpec:
    dtype: str
    generator_kind: str
    example_values: tuple[Any, ...] = ()
    choices: tuple[Any, ...] = ()


EXTRA_FIELD_SPECS: dict[str, ExtraFieldSpec] = {
    "household_id": ExtraFieldSpec("string", "household_id", ("1023", "H00045")),
    "source_person_id": ExtraFieldSpec("string", "source_person_id", ("7781", "P0342")),
    "source_trip_number": ExtraFieldSpec("int", "source_trip_number", (1, 2, 3)),
    "source_stage_number": ExtraFieldSpec("int", "stage_number", (1, 2, 3, 4)),
    "stage_count": ExtraFieldSpec("int", "stage_count", (1, 2, 3, 4)),
    "origin_zone_id": ExtraFieldSpec("string", "zone_id", ("912", "Z_120")),
    "destination_zone_id": ExtraFieldSpec("string", "zone_id", ("913", "Z_121")),
    "origin_sector_id": ExtraFieldSpec("string", "sector_id", ("SEC_03", "44012")),
    "destination_sector_id": ExtraFieldSpec("string", "sector_id", ("SEC_04", "44015")),
    "origin_stop_id": ExtraFieldSpec("string", "stop_id", ("PA432", "STOP_1021")),
    "destination_stop_id": ExtraFieldSpec("string", "stop_id", ("PA433", "STOP_1022")),
    "origin_station_id": ExtraFieldSpec("string", "station_id", ("L1_SAN_PABLO", "EST_034")),
    "destination_station_id": ExtraFieldSpec("string", "station_id", ("L1_UNIVERSIDAD_DE_CHILE", "EST_101")),
    "origin_venue_category": ExtraFieldSpec("string", "venue_category", ("Home", "Office", "Coffee Shop")),
    "destination_venue_category": ExtraFieldSpec("string", "venue_category", ("University", "Mall", "Hospital")),
    "activity_destination": ExtraFieldSpec("categorical", "categorical", ("trabajo", "estudio"), tuple(EXTRA_CATEGORICAL_DEFAULTS["activity_destination"])),
    "survey_date": ExtraFieldSpec("date", "survey_date", ("2012-08-14", "2024-03-21")),
    "assigned_weekday_name": ExtraFieldSpec("string", "weekday_name", ("lunes", "martes", "domingo")),
    "season": ExtraFieldSpec("categorical", "categorical", ("summer", "winter"), tuple(EXTRA_CATEGORICAL_DEFAULTS["season"])),
    "travel_time_min": ExtraFieldSpec("float", "travel_time_min", (12, 37.5, 84)),
    "fare_amount": ExtraFieldSpec("float", "fare_amount", (0, 760, 1520.0)),
    "public_transport_route_code": ExtraFieldSpec("string", "route_code", ("210", "L4", "B07")),
    "travel_time_quality_code": ExtraFieldSpec("categorical", "categorical", ("reported_exact", "imputed"), tuple(EXTRA_CATEGORICAL_DEFAULTS["travel_time_quality_code"])),
    "distance_euclidean_m": ExtraFieldSpec("float", "distance_m", (850.2, 4320.0)),
    "distance_route_m": ExtraFieldSpec("float", "distance_route_m", (1000.2, 5100.0)),
    "in_vehicle_travel_time_min": ExtraFieldSpec("float", "travel_time_min", (8.5, 21, 55)),
    "fare_payment_type": ExtraFieldSpec("categorical", "categorical", ("cash", "card"), tuple(EXTRA_CATEGORICAL_DEFAULTS["fare_payment_type"])),
    "parking_cost": ExtraFieldSpec("float", "parking_cost", (0, 500, 2000)),
    "highway_list": ExtraFieldSpec("string", "highway_list", ("Costanera Norte|Vespucio Sur",)),
    "highway_avoid_reason": ExtraFieldSpec("categorical", "categorical", ("traffic", "too_expensive"), tuple(EXTRA_CATEGORICAL_DEFAULTS["highway_avoid_reason"])),
    "bike_lane_usage": ExtraFieldSpec("categorical", "categorical", ("always", "never"), tuple(EXTRA_CATEGORICAL_DEFAULTS["bike_lane_usage"])),
    "bike_lane_issue": ExtraFieldSpec("categorical", "categorical", ("missing_segments", "none"), tuple(EXTRA_CATEGORICAL_DEFAULTS["bike_lane_issue"])),
    "bike_parking_type": ExtraFieldSpec("categorical", "categorical", ("none", "street_rack"), tuple(EXTRA_CATEGORICAL_DEFAULTS["bike_parking_type"])),
    "has_missing_alighting": ExtraFieldSpec("boolish", "boolish", (True, False, "unknown")),
    "cut_type_stage_trip": ExtraFieldSpec("categorical", "categorical", ("complete_trip", "partial_trip"), tuple(EXTRA_CATEGORICAL_DEFAULTS["cut_type_stage_trip"])),
    "transport_type_stage_1": ExtraFieldSpec("string", "transport_type_stage", ("walk", "bus", "metro")),
    "transport_type_stage_2": ExtraFieldSpec("string", "transport_type_stage", ("walk", "bus", "metro")),
    "transport_type_stage_3": ExtraFieldSpec("string", "transport_type_stage", ("walk", "bus", "metro")),
    "transport_type_stage_4": ExtraFieldSpec("string", "transport_type_stage", ("walk", "bus", "metro")),
    "wait_time_stage_1_min": ExtraFieldSpec("float", "wait_time_stage", (0, 3, 11.5)),
    "wait_time_stage_2_min": ExtraFieldSpec("float", "wait_time_stage", (0, 4, 13.2)),
    "wait_time_stage_3_min": ExtraFieldSpec("float", "wait_time_stage", (0, 5, 9.4)),
    "transfer_time_stage_1_min": ExtraFieldSpec("float", "transfer_time_stage", (0, 4, 12)),
    "walk_time_stage_1_min": ExtraFieldSpec("float", "walk_time_stage", (0, 6, 15)),
    "metro_line_boarding_stage_1": ExtraFieldSpec("string", "metro_line", ("L1", "L4A", "L6")),
    "metro_line_alighting_stage_1": ExtraFieldSpec("string", "metro_line", ("L1", "L4A", "L6")),
    "expansion_factor_workday_normal": ExtraFieldSpec("float", "expansion_factor", (84.2, 312.6)),
    "expansion_factor_saturday_normal": ExtraFieldSpec("float", "expansion_factor", (42.1, 145.8)),
    "expansion_factor_sunday_normal": ExtraFieldSpec("float", "expansion_factor", (38.7, 121.4)),
    "expansion_factor_workday_summer": ExtraFieldSpec("float", "expansion_factor", (64.2, 211.6)),
    "expansion_factor_weekend_summer": ExtraFieldSpec("float", "expansion_factor", (29.2, 98.6)),
    "travel_time_bucket": ExtraFieldSpec("categorical", "categorical", ("0-10", "21-40"), tuple(EXTRA_CATEGORICAL_DEFAULTS["travel_time_bucket"])),
    "household_size": ExtraFieldSpec("int", "household_size", (1, 4, 6)),
    "household_vehicle_count": ExtraFieldSpec("int", "vehicle_count", (0, 1, 2, 3)),
    "household_bicycle_count_adult": ExtraFieldSpec("int", "bicycle_count", (0, 1, 2)),
    "household_bicycle_count_child": ExtraFieldSpec("int", "bicycle_count", (0, 1, 2)),
    "household_income_clp": ExtraFieldSpec("float", "household_income", (450000, 1250000)),
    "home_tenure": ExtraFieldSpec("categorical", "categorical", ("owned", "rented"), tuple(EXTRA_CATEGORICAL_DEFAULTS["home_tenure"])),
    "relation_to_household_head": ExtraFieldSpec("categorical", "categorical", ("head", "child"), tuple(EXTRA_CATEGORICAL_DEFAULTS["relation_to_household_head"])),
    "education_level": ExtraFieldSpec("categorical", "categorical", ("secondary", "university"), tuple(EXTRA_CATEGORICAL_DEFAULTS["education_level"])),
    "school_level": ExtraFieldSpec("categorical", "categorical", ("primary", "higher_ed"), tuple(EXTRA_CATEGORICAL_DEFAULTS["school_level"])),
    "activity_status": ExtraFieldSpec("categorical", "categorical", ("working", "studying"), tuple(EXTRA_CATEGORICAL_DEFAULTS["activity_status"])),
    "occupation_type": ExtraFieldSpec("categorical", "categorical", ("employee", "self_employed"), tuple(EXTRA_CATEGORICAL_DEFAULTS["occupation_type"])),
    "job_sector": ExtraFieldSpec("categorical", "categorical", ("services", "health"), tuple(EXTRA_CATEGORICAL_DEFAULTS["job_sector"])),
    "work_schedule": ExtraFieldSpec("categorical", "categorical", ("full_time", "shift"), tuple(EXTRA_CATEGORICAL_DEFAULTS["work_schedule"])),
    "driver_license_class": ExtraFieldSpec("categorical", "categorical", ("none", "B"), tuple(EXTRA_CATEGORICAL_DEFAULTS["driver_license_class"])),
    "has_school_pass": ExtraFieldSpec("boolish", "boolish", (True, False, "unknown")),
    "is_senior": ExtraFieldSpec("boolish", "boolish", (True, False)),
    "disability_type": ExtraFieldSpec("categorical", "categorical", ("none", "mobility"), tuple(EXTRA_CATEGORICAL_DEFAULTS["disability_type"])),
    "has_income": ExtraFieldSpec("categorical", "categorical", ("yes", "no"), tuple(EXTRA_CATEGORICAL_DEFAULTS["has_income"])),
    "personal_income_clp": ExtraFieldSpec("float", "personal_income", (0, 350000, 1100000)),
    "personal_income_band": ExtraFieldSpec("categorical", "categorical", ("<200k", "400k-800k"), tuple(EXTRA_CATEGORICAL_DEFAULTS["personal_income_band"])),
    "income_imputed_flag": ExtraFieldSpec("categorical", "categorical", ("not_imputed", "fully_imputed"), tuple(EXTRA_CATEGORICAL_DEFAULTS["income_imputed_flag"])),
}

ALL_SCHEMA_FIELDS: set[str] = set(REQUIRED_FIELDS_ORDER) | set(BASE_FIELD_DTYPES.keys())
ALL_KNOWN_FIELDS: set[str] = ALL_SCHEMA_FIELDS | set(EXTRA_FIELD_SPECS.keys())

TIER1_FORMATS = {
    "utc_string_z",
    "offset_string",
    "naive_string",
    "utc_datetime",
    "tzaware_datetime",
    "naive_datetime",
    "numeric_invalid",
    "mixed_parseable",
    "mixed_with_invalids",
}
TIER2_FORMATS = {"valid_hhmm", "mixed_hhmm", "mostly_invalid_hhmm"}
COORD_FORMATS = {"numeric", "dd_string", "dd_comma", "dm", "dms", "mixed"}
H3_MODES = {"provided_valid", "omitted_derivable", "partial_missing", "invalid_strings"}
TRIP_STRUCTURES = {"independent", "single_stage_like", "multistage"}
DUPLICATE_MODES = {"none", "movement_id_only", "full_rows"}

# -----------------------------------------------------------------------------
# Función pública principal
# -----------------------------------------------------------------------------


def generate_synthetic_trip_dataframe(
    filas: int,
    *,
    seed: Optional[int] = None,
    omit_required_fields: Optional[Sequence[str]] = None,
    n_random_missing_required: int = 0,
    field_correspondence: Optional[dict[str, str]] = None,
    duplicate_mode: Literal["none", "movement_id_only", "full_rows"] = "none",
    tier_temporal: Literal["tier_1", "tier_2", "tier_3"] = "tier_1",
    tier1_datetime_format: Literal[
        "utc_string_z",
        "offset_string",
        "naive_string",
        "utc_datetime",
        "tzaware_datetime",
        "naive_datetime",
        "numeric_invalid",
        "mixed_parseable",
        "mixed_with_invalids",
    ] = "utc_string_z",
    tier2_hhmm_format: Literal["valid_hhmm", "mixed_hhmm", "mostly_invalid_hhmm"] = "valid_hhmm",
    coord_format: Literal["numeric", "dd_string", "dd_comma", "dm", "dms", "mixed"] = "numeric",
    h3_mode: Literal["provided_valid", "omitted_derivable", "partial_missing", "invalid_strings"] = "provided_valid",
    trip_structure: Literal["independent", "single_stage_like", "multistage"] = "independent",
    max_movements_per_trip: int = 1,
    base_fields: Optional[Sequence[str]] = None,
    extra_value_domains: Optional[dict[str, Any]] = None,
    categorical_sampling_policy: Optional[dict[str, Any]] = None,
    extra_columns: Optional[int | Sequence[str]] = None,
    null_ratio: Optional[float | dict[str, float]] = None,
    paired_missingness: Any = None,
    noise_ratio: Optional[dict[str, float]] = None,
    type_corruption: Optional[dict[str, Any]] = None,
) -> pd.DataFrame:
    """
    Genera un DataFrame sintético de movements/trips orientado a tests de integración de OP-01.

    La función parte desde un dataset canónico razonablemente válido y luego aplica degradaciones
    controladas según los argumentos. Todos los argumentos, salvo `field_correspondence`, se refieren
    a nombres canónicos Golondrina.

    Parameters
    ----------
    filas : int
        Cantidad de filas del dataset generado.
    seed : int, optional
        Semilla para reproducibilidad total.
    omit_required_fields : sequence of str, optional
        Lista explícita de campos requeridos que se omiten del resultado final.
        Además, la función omite automáticamente ciertos requeridos cuando corresponde:
        - `origin_time_utc`, `destination_time_utc` si `tier_temporal` es tier_2 o tier_3.
        - `origin_h3_index`, `destination_h3_index` si `h3_mode='omitted_derivable'`.
    n_random_missing_required : int, default=0
        Cantidad adicional de required a omitir al azar después de aplicar `omit_required_fields`.
    field_correspondence : dict[str, str], optional
        Mapeo canónico -> nombre fuente. Se aplica al final de la generación.
    duplicate_mode : {"none", "movement_id_only", "full_rows"}, default="none"
        Estrategia de duplicación controlada.
    tier_temporal : {"tier_1", "tier_2", "tier_3"}, default="tier_1"
        Tier temporal del dataset de entrada.
    tier1_datetime_format : str, default="utc_string_z"
        Formato para campos tier 1.
    tier2_hhmm_format : str, default="valid_hhmm"
        Formato para campos HH:MM cuando el dataset es tier 2.
    coord_format : {"numeric", "dd_string", "dd_comma", "dm", "dms", "mixed"}, default="numeric"
        Formato de coordenadas de entrada.
    h3_mode : {"provided_valid", "omitted_derivable", "partial_missing", "invalid_strings"}, default="provided_valid"
        Política de generación de campos H3.
    trip_structure : {"independent", "single_stage_like", "multistage"}, default="independent"
        Estructura viaje-etapa del dataset.
    max_movements_per_trip : int, default=1
        Máximo de movements por trip cuando `trip_structure='multistage'`.
    base_fields : sequence of str, optional
        Campos base/opcionales de Golondrina a agregar.
    extra_value_domains : dict[str, Any], optional
        Controla qué grupos de dominio usar en campos categóricos.
        Formas soportadas:
        - `{"purpose": 3}` -> toma 3 valores extra desde un solo grupo elegido al azar.
        - `{"purpose": ["canon", "finos"]}` -> combina explícitamente grupos.
    categorical_sampling_policy : dict[str, Any], optional
        Controla cómo se muestrean valores categóricos ya resueltos.
        Ejemplo:
        {
            "purpose": {
                "canonical_ratio": 0.7,
                "ensure_presence": True,
                "distribution": "uniform",
            }
        }
    extra_columns : int | sequence of str, optional
        Extras no oficiales a agregar. Si es int, se eligen aleatoriamente desde el pool.
    null_ratio : float | dict[str, float], optional
        Proporción de nulos por columna. En columnas string puede materializarse como `""` o `NA`.
    paired_missingness : Any, optional
        Patrones de nulidad acoplada. Se aceptan:
        - str
        - list[str]
        - dict[str, float]
        - list[dict] con forma {"pattern": ..., "ratio": ...}
        Patrones soportados:
        `destination_coords_missing`, `origin_coords_missing`,
        `destination_lat_only_missing`, `destination_lon_only_missing`,
        `origin_lat_only_missing`, `origin_lon_only_missing`,
        `destination_time_missing`, `origin_time_missing`,
        `destination_h3_missing`, `origin_h3_missing`,
        `destination_incomplete`, `origin_incomplete`.
    noise_ratio : dict[str, float], optional
        Proporción de valores fuera de dominio a inyectar por campo categórico.
    type_corruption : dict[str, Any], optional
        Corrupción semántica por campo. Cada valor puede ser:
        - str con el modo de corrupción
        - dict con `mode` y `ratio`
        Modos útiles:
        `numeric_as_string`, `string_as_int`, `non_numeric_text`, `mixed_bad_values`, `stringified`.

    Returns
    -------
    pandas.DataFrame
        DataFrame sintético listo para usarse como input fuente en `import_trips_from_dataframe()`.

    Notes
    -----
    Orden general aplicado por la función:
    1. generar estructura base válida;
    2. aplicar tier temporal y formato espacial;
    3. agregar campos base;
    4. agregar extras;
    5. aplicar degradaciones controladas sobre nombres canónicos;
    6. omitir required finales;
    7. renombrar columnas según `field_correspondence`.

    Elegí dejar `field_correspondence` para el final para que el resto de argumentos pueda referirse
    siempre a nombres canónicos y no a nombres fuente, lo que simplifica mucho el uso del generador.
    """
    if filas < 0:
        raise ValueError("filas debe ser >= 0")

    rng = _make_rng(seed)

    _validate_choice("duplicate_mode", duplicate_mode, DUPLICATE_MODES)
    _validate_choice("tier_temporal", tier_temporal, {"tier_1", "tier_2", "tier_3"})
    _validate_choice("coord_format", coord_format, COORD_FORMATS)
    _validate_choice("h3_mode", h3_mode, H3_MODES)
    _validate_choice("trip_structure", trip_structure, TRIP_STRUCTURES)

    base_fields_resolved = _resolve_base_fields(base_fields)
    omit_required_resolved = _resolve_omit_required(
        omit_required_fields=omit_required_fields,
        n_random_missing_required=n_random_missing_required,
        tier_temporal=tier_temporal,
        h3_mode=h3_mode,
        rng=rng,
    )
    extra_columns_resolved = _resolve_extra_columns(
        extra_columns,
        rng=rng,
        excluded=set(REQUIRED_FIELDS_ORDER) | set(base_fields_resolved),
    )

    _validate_cross_argument_constraints(
        omit_required_fields=omit_required_resolved,
        field_correspondence=field_correspondence,
        duplicate_mode=duplicate_mode,
        tier_temporal=tier_temporal,
        tier1_datetime_format=tier1_datetime_format,
        tier2_hhmm_format=tier2_hhmm_format,
        h3_mode=h3_mode,
        trip_structure=trip_structure,
        max_movements_per_trip=max_movements_per_trip,
        base_fields=base_fields_resolved,
    )

    # 1) Estructura base canónica, antes de degradaciones.
    movement_id, trip_id, movement_seq = _build_trip_structure(
        filas,
        trip_structure=trip_structure,
        max_movements_per_trip=max_movements_per_trip,
        rng=rng,
    )
    user_id = _build_user_ids(filas, rng=rng, trip_ids=trip_id)
    coords_ref = _build_reference_coordinates(filas, rng=rng)
    origin_dt, destination_dt = _build_base_datetimes(filas, rng=rng)

    df = pd.DataFrame(index=pd.RangeIndex(filas))
    df["movement_id"] = movement_id
    df["user_id"] = user_id
    df["trip_id"] = trip_id
    df["movement_seq"] = movement_seq

    # Coordenadas base en formato de input deseado.
    for field in ["origin_longitude", "origin_latitude", "destination_longitude", "destination_latitude"]:
        is_lat = field.endswith("latitude")
        raw = coords_ref[field]
        formatted = [_format_coord(v, is_lat=is_lat, coord_format=coord_format, rng=rng) for v in raw]
        if coord_format == "numeric":
            df[field] = _as_float_series(formatted)
        else:
            df[field] = pd.Series(formatted, dtype="object")

    # Tier temporal.
    if tier_temporal == "tier_1":
        o_time, d_time = _format_tier1_datetimes(
            origin_dt,
            destination_dt,
            fmt=tier1_datetime_format,
            rng=rng,
        )
        df["origin_time_utc"] = o_time
        df["destination_time_utc"] = d_time
    elif tier_temporal == "tier_2":
        o_hhmm, d_hhmm = _build_tier2_hhmm(origin_dt, destination_dt, fmt=tier2_hhmm_format, rng=rng)
        df["origin_time_local_hhmm"] = o_hhmm
        df["destination_time_local_hhmm"] = d_hhmm
    elif tier_temporal == "tier_3":
        pass

    # H3 usando coordenadas de referencia internas para asegurar consistencia cuando se piden válidos.
    h3_origin, h3_dest = _build_h3_columns(coords_ref, h3_mode=h3_mode, filas=filas, rng=rng)
    if h3_mode != "omitted_derivable":
        df["origin_h3_index"] = h3_origin
        df["destination_h3_index"] = h3_dest

    # 2) Campos base/opcionales.
    for field in base_fields_resolved:
        if field in df.columns:
            continue
        df[field] = _build_base_field(
            field,
            n=filas,
            rng=rng,
            origin_dt=origin_dt,
            destination_dt=destination_dt,
            trip_structure=trip_structure,
            extra_value_domains=extra_value_domains,
            categorical_sampling_policy=categorical_sampling_policy,
        )

    # 3) Extras.
    for field in extra_columns_resolved:
        df[field] = _build_extra_field(field, n=filas, rng=rng, base_df=df)

    # 4) Degradaciones controladas en nombres canónicos.
    df = _apply_duplicate_mode(df, duplicate_mode=duplicate_mode, rng=rng)
    df = _apply_paired_missingness(df, paired_missingness=paired_missingness, rng=rng)

    protected_for_nulls: set[str] = set()
    df = _apply_null_ratio(df, null_ratio=null_ratio, protected_columns=protected_for_nulls, rng=rng)
    df = _apply_noise_ratio(df, noise_ratio=noise_ratio, rng=rng)
    df = _apply_type_corruption(df, type_corruption=type_corruption, protected_columns=set(), rng=rng)

    # 5) Omisión final de required. Se hace al final para poder generar primero y degradar después.
    required_to_drop = [c for c in omit_required_resolved if c in df.columns]
    if required_to_drop:
        df = df.drop(columns=required_to_drop)

    # 6) Renombre final según correspondencia de campos.
    if field_correspondence:
        rename_map = {canon: source for canon, source in field_correspondence.items() if canon in df.columns}
        if len(rename_map) != len(set(rename_map.values())):
            raise ValueError("field_correspondence produce nombres fuente duplicados o colisiones de columnas.")
        df = df.rename(columns=rename_map)

    return df


def save_synthetic_trip_csv(
    output_path: str,
    *,
    sep: str = ",",
    index: bool = False,
    **generator_kwargs: Any,
) -> pd.DataFrame:
    """
    Helper simple para persistir a CSV un dataset generado por `generate_synthetic_trip_dataframe`.

    Retorna el DataFrame generado para no tener que re-leerlo en el notebook.
    """
    df = generate_synthetic_trip_dataframe(**generator_kwargs)
    df.to_csv(output_path, sep=sep, index=index)
    return df

# -----------------------------------------------------------------------------
# Helpers generales
# -----------------------------------------------------------------------------


def _make_rng(seed: Optional[int]) -> np.random.Generator:
    return np.random.default_rng(seed)


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set, pd.Index, np.ndarray)):
        return list(value)
    return [value]


def _validate_choice(name: str, value: Any, allowed: set[str]) -> None:
    if value not in allowed:
        raise ValueError(f"{name}={value!r} no es válido. Permitidos: {sorted(allowed)}")


def _ensure_known_fields(fields: Iterable[str], *, argument_name: str) -> None:
    unknown = sorted(set(fields) - ALL_KNOWN_FIELDS)
    if unknown:
        raise ValueError(f"{argument_name} contiene campos desconocidos: {unknown}")


def _dedupe_keep_order(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for v in values:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _sample_indices(n: int, ratio: float, rng: np.random.Generator) -> np.ndarray:
    ratio = max(0.0, min(1.0, float(ratio)))
    if n <= 0 or ratio <= 0:
        return np.array([], dtype=int)
    k = min(n, int(round(n * ratio)))
    if k <= 0:
        return np.array([], dtype=int)
    return np.sort(rng.choice(np.arange(n), size=k, replace=False))


def _random_choice(rng: np.random.Generator, values: Sequence[Any], size: int, *, p: Optional[Sequence[float]] = None) -> list[Any]:
    if size <= 0:
        return []
    if len(values) == 0:
        raise ValueError("No se puede muestrear desde una secuencia vacía.")
    return list(rng.choice(np.array(list(values), dtype=object), size=size, replace=True, p=p))


def _as_string_series(values: Sequence[Any], index: Optional[pd.Index] = None) -> pd.Series:
    return pd.Series(values, index=index, dtype="string")


def _as_int_series(values: Sequence[Any], index: Optional[pd.Index] = None) -> pd.Series:
    return pd.Series(values, index=index, dtype="Int64")


def _as_float_series(values: Sequence[Any], index: Optional[pd.Index] = None) -> pd.Series:
    return pd.Series(values, index=index, dtype="float64")


def _as_datetime_series(values: Sequence[Any], index: Optional[pd.Index] = None) -> pd.Series:
    return pd.Series(values, index=index)


def _infer_string_like(series: pd.Series) -> bool:
    return str(series.dtype) == "string" or series.dtype == object


# -----------------------------------------------------------------------------
# Helpers de coordenadas y tiempo
# -----------------------------------------------------------------------------


def _decimal_to_dm(value: float, *, is_lat: bool) -> str:
    hem = "N" if is_lat else "E"
    if value < 0:
        hem = "S" if is_lat else "W"
    abs_value = abs(float(value))
    deg = int(abs_value)
    minutes = (abs_value - deg) * 60.0
    return f"{deg}° {minutes:.4f}' {hem}"


def _decimal_to_dms(value: float, *, is_lat: bool) -> str:
    hem = "N" if is_lat else "E"
    if value < 0:
        hem = "S" if is_lat else "W"
    abs_value = abs(float(value))
    deg = int(abs_value)
    minutes_full = (abs_value - deg) * 60.0
    minutes = int(minutes_full)
    seconds = (minutes_full - minutes) * 60.0
    return f'{deg}° {minutes} {seconds:.2f} {hem}'


def _format_coord(value: float, *, is_lat: bool, coord_format: str, rng: np.random.Generator) -> Any:
    if pd.isna(value):
        return np.nan
    if coord_format == "numeric":
        return float(value)
    if coord_format == "dd_string":
        return f"{float(value):.6f}"
    if coord_format == "dd_comma":
        return f"{float(value):.6f}".replace(".", ",")
    if coord_format == "dm":
        return _decimal_to_dm(float(value), is_lat=is_lat)
    if coord_format == "dms":
        return _decimal_to_dms(float(value), is_lat=is_lat)
    if coord_format == "mixed":
        chosen = rng.choice(["numeric", "dd_string", "dd_comma", "dm", "dms"])
        return _format_coord(float(value), is_lat=is_lat, coord_format=str(chosen), rng=rng)
    raise ValueError(f"coord_format no soportado: {coord_format!r}")


_HHMM_RE = re.compile(r"^(?P<h>\d{2}):(?P<m>\d{2})$")


def _valid_hhmm_from_datetime(ts: pd.Timestamp) -> str:
    return ts.strftime("%H:%M")


def _random_invalid_hhmm(rng: np.random.Generator) -> str:
    pool = ["24:00", "24:03", "7:63", "99:99", "ab:cd", "-1:10", "3:5", "25:61"]
    return str(rng.choice(pool))


def _build_base_datetimes(filas: int, rng: np.random.Generator) -> tuple[pd.DatetimeIndex, pd.DatetimeIndex]:
    base_date = pd.Timestamp("2026-03-01 06:00:00")
    start_offsets = rng.integers(0, 7 * 24 * 60, size=filas)
    durations = rng.integers(5, 120, size=filas)
    origin_dt = pd.to_datetime(base_date) + pd.to_timedelta(start_offsets, unit="m")
    dest_dt = origin_dt + pd.to_timedelta(durations, unit="m")
    return pd.DatetimeIndex(origin_dt), pd.DatetimeIndex(dest_dt)


def _format_tier1_datetimes(
    origin_dt: pd.DatetimeIndex,
    destination_dt: pd.DatetimeIndex,
    *,
    fmt: str,
    rng: np.random.Generator,
) -> tuple[pd.Series, pd.Series]:
    if fmt == "utc_datetime":
        return pd.Series(origin_dt.tz_localize("UTC")), pd.Series(destination_dt.tz_localize("UTC"))
    if fmt == "tzaware_datetime":
        return (
            pd.Series(origin_dt.tz_localize("America/Santiago")),
            pd.Series(destination_dt.tz_localize("America/Santiago")),
        )
    if fmt == "naive_datetime":
        return pd.Series(origin_dt), pd.Series(destination_dt)
    if fmt == "utc_string_z":
        o = origin_dt.tz_localize("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
        d = destination_dt.tz_localize("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
        return _as_string_series(o), _as_string_series(d)
    if fmt == "offset_string":
        o = origin_dt.tz_localize("America/Santiago").strftime("%Y-%m-%dT%H:%M:%S%z")
        d = destination_dt.tz_localize("America/Santiago").strftime("%Y-%m-%dT%H:%M:%S%z")
        o = [v[:-2] + ":" + v[-2:] for v in o]
        d = [v[:-2] + ":" + v[-2:] for v in d]
        return _as_string_series(o), _as_string_series(d)
    if fmt == "naive_string":
        return _as_string_series(origin_dt.strftime("%Y-%m-%d %H:%M:%S")), _as_string_series(destination_dt.strftime("%Y-%m-%d %H:%M:%S"))
    if fmt == "numeric_invalid":
        return _as_float_series(rng.integers(1_700_000_000, 1_800_000_000, size=len(origin_dt))), _as_float_series(rng.integers(1_700_000_000, 1_800_000_000, size=len(origin_dt)))
    if fmt in {"mixed_parseable", "mixed_with_invalids"}:
        valid_modes = ["utc_string_z", "offset_string", "naive_string", "utc_datetime", "tzaware_datetime", "naive_datetime"]
        invalid_pool = ["not-a-date", "2026/99/99 25:61:00", "abc", 123456]
        origin_values: list[Any] = []
        dest_values: list[Any] = []
        for o, d in zip(origin_dt, destination_dt):
            mode = str(rng.choice(valid_modes))
            so, sd = _format_tier1_datetimes(pd.DatetimeIndex([o]), pd.DatetimeIndex([d]), fmt=mode, rng=rng)
            origin_values.append(so.iloc[0])
            dest_values.append(sd.iloc[0])
        if fmt == "mixed_with_invalids" and len(origin_values) > 0:
            idx = _sample_indices(len(origin_values), 0.2, rng)
            for i in idx:
                origin_values[i] = rng.choice(invalid_pool)
            idx2 = _sample_indices(len(dest_values), 0.2, rng)
            for i in idx2:
                dest_values[i] = rng.choice(invalid_pool)
        return pd.Series(origin_values), pd.Series(dest_values)
    raise ValueError(f"tier1_datetime_format no soportado: {fmt!r}")


def _build_tier2_hhmm(
    origin_dt: pd.DatetimeIndex,
    destination_dt: pd.DatetimeIndex,
    *,
    fmt: str,
    rng: np.random.Generator,
) -> tuple[pd.Series, pd.Series]:
    origin = [_valid_hhmm_from_datetime(ts) for ts in origin_dt]
    dest = [_valid_hhmm_from_datetime(ts) for ts in destination_dt]
    if fmt == "valid_hhmm":
        return _as_string_series(origin), _as_string_series(dest)
    origin_values = origin.copy()
    dest_values = dest.copy()
    ratio = 0.25 if fmt == "mixed_hhmm" else 0.7
    for idx in _sample_indices(len(origin_values), ratio, rng):
        origin_values[int(idx)] = _random_invalid_hhmm(rng)
    for idx in _sample_indices(len(dest_values), ratio, rng):
        dest_values[int(idx)] = _random_invalid_hhmm(rng)
    return _as_string_series(origin_values), _as_string_series(dest_values)


# -----------------------------------------------------------------------------
# Generación estructural base
# -----------------------------------------------------------------------------


def _build_trip_structure(
    filas: int,
    *,
    trip_structure: str,
    max_movements_per_trip: int,
    rng: np.random.Generator,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    if filas <= 0:
        return _as_string_series([]), _as_string_series([]), _as_int_series([])

    if trip_structure == "independent":
        trip_ids = [f"t{i:05d}" for i in range(filas)]
        movement_seq = [0] * filas
    elif trip_structure == "single_stage_like":
        trip_ids = [f"trip_single_{i:05d}" for i in range(filas)]
        movement_seq = [0] * filas
    elif trip_structure == "multistage":
        trip_ids = []
        movement_seq = []
        trip_counter = 0
        row_counter = 0
        while row_counter < filas:
            size = int(rng.integers(1, max_movements_per_trip + 1))
            size = min(size, filas - row_counter)
            trip_id = f"tm_{trip_counter:05d}"
            for seq in range(size):
                trip_ids.append(trip_id)
                movement_seq.append(seq)
                row_counter += 1
            trip_counter += 1
    else:
        raise ValueError(f"trip_structure no soportado: {trip_structure!r}")

    movement_ids = [f"m{i:05d}" for i in range(filas)]
    return _as_string_series(movement_ids), _as_string_series(trip_ids), _as_int_series(movement_seq)


def _build_user_ids(filas: int, *, rng: np.random.Generator, trip_ids: pd.Series) -> pd.Series:
    unique_trips = trip_ids.drop_duplicates().tolist()
    n_users = max(1, math.ceil(len(unique_trips) / 2))
    user_pool = [f"u{i:04d}" for i in range(n_users)]
    trip_to_user: dict[str, str] = {}
    for trip_id in unique_trips:
        trip_to_user[str(trip_id)] = str(rng.choice(user_pool))
    return _as_string_series([trip_to_user[str(t)] for t in trip_ids.tolist()])


def _build_reference_coordinates(filas: int, *, rng: np.random.Generator) -> dict[str, np.ndarray]:
    # Centro aproximado de Santiago; suficiente para pruebas sintéticas.
    base_lat = -33.45
    base_lon = -70.66
    origin_lat = base_lat + rng.normal(0, 0.08, size=filas)
    origin_lon = base_lon + rng.normal(0, 0.10, size=filas)
    dest_lat = origin_lat + rng.normal(0, 0.03, size=filas)
    dest_lon = origin_lon + rng.normal(0, 0.04, size=filas)
    return {
        "origin_latitude": origin_lat,
        "origin_longitude": origin_lon,
        "destination_latitude": dest_lat,
        "destination_longitude": dest_lon,
    }


def _fake_h3_from_coords(lat: float, lon: float, resolution: int = 8) -> str:
    text = f"{resolution}_{lat:.6f}_{lon:.6f}"
    return f"fake_{abs(hash(text)) % 10**12:012d}"


def _coord_to_h3(lat: float, lon: float, resolution: int = 8) -> str:
    if pd.isna(lat) or pd.isna(lon):
        return pd.NA  # type: ignore[return-value]
    if h3 is not None:
        try:
            return str(h3.latlng_to_cell(float(lat), float(lon), resolution))
        except Exception:
            return _fake_h3_from_coords(float(lat), float(lon), resolution)
    return _fake_h3_from_coords(float(lat), float(lon), resolution)


def _build_h3_columns(
    coords_ref: dict[str, np.ndarray],
    *,
    h3_mode: str,
    filas: int,
    rng: np.random.Generator,
) -> tuple[pd.Series, pd.Series]:
    origin = [_coord_to_h3(lat, lon) for lat, lon in zip(coords_ref["origin_latitude"], coords_ref["origin_longitude"])]
    dest = [_coord_to_h3(lat, lon) for lat, lon in zip(coords_ref["destination_latitude"], coords_ref["destination_longitude"])]
    origin_s = _as_string_series(origin)
    dest_s = _as_string_series(dest)

    if h3_mode == "provided_valid":
        return origin_s, dest_s
    if h3_mode == "partial_missing":
        for idx in _sample_indices(filas, 0.25, rng):
            origin_s.iloc[int(idx)] = pd.NA
        for idx in _sample_indices(filas, 0.25, rng):
            dest_s.iloc[int(idx)] = pd.NA
        return origin_s, dest_s
    if h3_mode == "invalid_strings":
        invalid_pool = ["BAD_H3", "xyz", "123", "not_an_h3", "8invalidffff"]
        for idx in _sample_indices(filas, 0.40, rng):
            origin_s.iloc[int(idx)] = str(rng.choice(invalid_pool))
        for idx in _sample_indices(filas, 0.40, rng):
            dest_s.iloc[int(idx)] = str(rng.choice(invalid_pool))
        return origin_s, dest_s
    if h3_mode == "omitted_derivable":
        return origin_s, dest_s
    raise ValueError(f"h3_mode no soportado: {h3_mode!r}")


# -----------------------------------------------------------------------------
# Dominios categóricos
# -----------------------------------------------------------------------------


def _resolve_domain_values(
    field: str,
    extra_value_domains: Optional[dict[str, Any]],
    rng: np.random.Generator,
) -> tuple[list[str], list[str]]:
    canonical = list(CANONICAL_DOMAINS[field])
    config = (extra_value_domains or {}).get(field)
    if config is None:
        return canonical, ["canon"]

    groups_for_field = EXTENDABLE_DOMAIN_GROUPS.get(field, {})
    aliases = DOMAIN_GROUP_ALIASES.get(field, {"canon": "canon"})

    if isinstance(config, int):
        if config <= 0 or not groups_for_field:
            return canonical, ["canon"]
        chosen_group = str(rng.choice(list(groups_for_field.keys())))
        available = list(groups_for_field[chosen_group])
        k = min(int(config), len(available))
        extra_vals = list(rng.choice(np.array(available, dtype=object), size=k, replace=False)) if k > 0 else []
        return canonical + extra_vals, ["canon", chosen_group]

    if isinstance(config, (list, tuple, set)):
        requested_groups = []
        values = list(canonical)
        for raw_name in config:
            name = str(raw_name)
            resolved = aliases.get(name)
            if resolved is None:
                raise ValueError(f"Grupo de dominio desconocido para {field!r}: {name!r}")
            if resolved == "canon":
                requested_groups.append("canon")
                continue
            requested_groups.append(resolved)
            values.extend(groups_for_field.get(resolved, []))
        if not requested_groups:
            requested_groups = ["canon"]
        return _dedupe_keep_order(values), _dedupe_keep_order(requested_groups)

    raise TypeError(
        "extra_value_domains debe usar valores int o list[str] por campo. "
        f"Se recibió {type(config).__name__} para {field!r}."
    )


def _build_categorical_values(
    field: str,
    n: int,
    *,
    extra_value_domains: Optional[dict[str, Any]],
    categorical_sampling_policy: Optional[dict[str, Any]],
    rng: np.random.Generator,
) -> pd.Series:
    values, groups_used = _resolve_domain_values(field, extra_value_domains, rng)
    policy_raw = (categorical_sampling_policy or {}).get(field, {})
    if isinstance(policy_raw, (int, float)):
        policy: dict[str, Any] = {"canonical_ratio": float(policy_raw)}
    else:
        policy = dict(policy_raw or {})

    ensure_presence = bool(policy.get("ensure_presence", False))
    canonical_ratio = policy.get("canonical_ratio")
    distribution = str(policy.get("distribution", "uniform"))
    explicit_weights = dict(policy.get("weights", {}))

    canonical_values = list(CANONICAL_DOMAINS[field])
    extended_values = [v for v in values if v not in canonical_values]

    if distribution == "uniform" and not explicit_weights:
        probabilities = None
    else:
        probs: list[float] = []
        if explicit_weights:
            for v in values:
                probs.append(float(explicit_weights.get(v, 1.0)))
        elif canonical_ratio is not None and extended_values:
            canonical_ratio = max(0.0, min(1.0, float(canonical_ratio)))
            canon_p = canonical_ratio / max(len(canonical_values), 1)
            extra_p = (1.0 - canonical_ratio) / max(len(extended_values), 1)
            for v in values:
                probs.append(canon_p if v in canonical_values else extra_p)
        else:
            probs = [1.0] * len(values)
        total = sum(probs)
        probabilities = [p / total for p in probs] if total > 0 else None

    sampled = _random_choice(rng, values, n, p=probabilities)

    if ensure_presence and n >= len(values):
        shuffled = list(values)
        rng.shuffle(shuffled)
        for i, v in enumerate(shuffled):
            sampled[i] = v

    # Cuando solo se pidió canon, evitamos mezclar grupos extra por accidente.
    if groups_used == ["canon"]:
        sampled = _random_choice(rng, canonical_values, n)

    return _as_string_series(sampled)


def _build_mode_sequence(n: int, *, rng: np.random.Generator) -> pd.Series:
    pool = [
        "walk",
        "bus",
        "metro",
        "car",
        "bicycle",
        "train",
    ]
    out = []
    for _ in range(n):
        k = int(rng.integers(1, 4))
        chosen = list(rng.choice(np.array(pool, dtype=object), size=k, replace=True))
        out.append("+".join(chosen))
    return _as_string_series(out)


# -----------------------------------------------------------------------------
# Generación de campos base y extras
# -----------------------------------------------------------------------------


def _build_base_field(
    field: str,
    *,
    n: int,
    rng: np.random.Generator,
    origin_dt: pd.DatetimeIndex,
    destination_dt: pd.DatetimeIndex,
    trip_structure: str,
    extra_value_domains: Optional[dict[str, Any]],
    categorical_sampling_policy: Optional[dict[str, Any]],
) -> pd.Series:
    if field in CATEGORY_FIELDS:
        return _build_categorical_values(
            field,
            n,
            extra_value_domains=extra_value_domains,
            categorical_sampling_policy=categorical_sampling_policy,
            rng=rng,
        )
    if field == "origin_municipality":
        return _as_string_series(_random_choice(rng, SANTIAGO_MUNICIPALITIES, n))
    if field == "destination_municipality":
        return _as_string_series(_random_choice(rng, SANTIAGO_MUNICIPALITIES, n))
    if field == "timezone_offset_min":
        return _as_int_series([-180] * n)
    if field == "origin_time_local_hhmm":
        return _as_string_series([ts.strftime("%H:%M") for ts in origin_dt])
    if field == "destination_time_local_hhmm":
        return _as_string_series([ts.strftime("%H:%M") for ts in destination_dt])
    if field == "trip_weight":
        return _as_float_series(np.round(rng.uniform(0.5, 5.0, size=n), 3))
    if field == "mode_sequence":
        return _build_mode_sequence(n, rng=rng)
    raise ValueError(f"Campo base no soportado: {field!r}")


def _build_extra_field(
    field: str,
    *,
    n: int,
    rng: np.random.Generator,
    base_df: pd.DataFrame,
) -> pd.Series:
    spec = EXTRA_FIELD_SPECS[field]

    if spec.generator_kind == "household_id":
        pool = [f"H{i:05d}" for i in range(max(2, math.ceil(n / 2)))]
        return _as_string_series(_random_choice(rng, pool, n))
    if spec.generator_kind == "source_person_id":
        pool = [f"P{i:05d}" for i in range(max(2, math.ceil(n / 2)))]
        return _as_string_series(_random_choice(rng, pool, n))
    if spec.generator_kind == "source_trip_number":
        return _as_int_series(rng.integers(1, 5, size=n))
    if spec.generator_kind == "stage_number":
        return _as_int_series(rng.integers(1, 5, size=n))
    if spec.generator_kind == "stage_count":
        return _as_int_series(rng.integers(1, 5, size=n))
    if spec.generator_kind == "zone_id":
        return _as_string_series([f"Z_{int(v):03d}" for v in rng.integers(1, 300, size=n)])
    if spec.generator_kind == "sector_id":
        return _as_string_series([f"SEC_{int(v):03d}" for v in rng.integers(1, 600, size=n)])
    if spec.generator_kind == "stop_id":
        return _as_string_series([f"STOP_{int(v):04d}" for v in rng.integers(1, 5000, size=n)])
    if spec.generator_kind == "station_id":
        return _as_string_series([f"EST_{int(v):03d}" for v in rng.integers(1, 300, size=n)])
    if spec.generator_kind == "venue_category":
        pool = ["Home", "Office", "Coffee Shop", "University", "Mall", "Hospital", "Gym", "Bank"]
        return _as_string_series(_random_choice(rng, pool, n))
    if spec.generator_kind == "categorical":
        return _as_string_series(_random_choice(rng, spec.choices or spec.example_values, n))
    if spec.generator_kind == "survey_date":
        base = pd.Timestamp("2024-03-01")
        days = rng.integers(0, 120, size=n)
        return _as_string_series([(base + pd.Timedelta(days=int(d))).strftime("%Y-%m-%d") for d in days])
    if spec.generator_kind == "weekday_name":
        pool = ["lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo"]
        return _as_string_series(_random_choice(rng, pool, n))
    if spec.generator_kind == "travel_time_min":
        if {"origin_time_utc", "destination_time_utc"}.issubset(base_df.columns):
            o = pd.to_datetime(base_df["origin_time_utc"], errors="coerce")
            d = pd.to_datetime(base_df["destination_time_utc"], errors="coerce")
            delta = (d - o).dt.total_seconds() / 60.0
            out = delta.fillna(pd.Series(np.round(rng.uniform(5, 90, size=n), 1)))
            return _as_float_series(out)
        return _as_float_series(np.round(rng.uniform(5, 90, size=n), 1))
    if spec.generator_kind == "fare_amount":
        pool = [0, 760, 800, 900, 1200, 1520.0]
        return _as_float_series(_random_choice(rng, pool, n))
    if spec.generator_kind == "route_code":
        pool = ["210", "L4", "B07", "I09c", "401", "L1"]
        return _as_string_series(_random_choice(rng, pool, n))
    if spec.generator_kind == "distance_m":
        return _as_float_series(np.round(rng.uniform(150, 9000, size=n), 1))
    if spec.generator_kind == "distance_route_m":
        base = np.round(rng.uniform(200, 12000, size=n), 1)
        return _as_float_series(base)
    if spec.generator_kind == "parking_cost":
        pool = [0, 0, 500, 800, 1200, 2500]
        return _as_float_series(_random_choice(rng, pool, n))
    if spec.generator_kind == "highway_list":
        pool = [
            "Costanera Norte",
            "Vespucio Sur",
            "Autopista Central",
            "Costanera Norte|Vespucio Sur",
            "No aplica",
        ]
        return _as_string_series(_random_choice(rng, pool, n))
    if spec.generator_kind == "transport_type_stage":
        pool = ["walk", "bus", "metro", "car", "bicycle", "taxi"]
        return _as_string_series(_random_choice(rng, pool, n))
    if spec.generator_kind == "wait_time_stage":
        return _as_float_series(np.round(rng.uniform(0, 20, size=n), 1))
    if spec.generator_kind == "transfer_time_stage":
        return _as_float_series(np.round(rng.uniform(0, 25, size=n), 1))
    if spec.generator_kind == "walk_time_stage":
        return _as_float_series(np.round(rng.uniform(0, 30, size=n), 1))
    if spec.generator_kind == "metro_line":
        pool = ["L1", "L2", "L4", "L4A", "L5", "L6"]
        return _as_string_series(_random_choice(rng, pool, n))
    if spec.generator_kind == "boolish":
        pool = [True, False, True, False, "unknown"]
        return pd.Series(_random_choice(rng, pool, n), dtype="object")
    if spec.generator_kind == "expansion_factor":
        return _as_float_series(np.round(rng.uniform(10, 500, size=n), 2))
    if spec.generator_kind == "household_size":
        return _as_int_series(rng.integers(1, 9, size=n))
    if spec.generator_kind == "vehicle_count":
        return _as_int_series(rng.integers(0, 4, size=n))
    if spec.generator_kind == "bicycle_count":
        return _as_int_series(rng.integers(0, 4, size=n))
    if spec.generator_kind == "household_income":
        return _as_float_series(np.round(rng.uniform(250_000, 2_500_000, size=n), 0))
    if spec.generator_kind == "personal_income":
        return _as_float_series(np.round(rng.uniform(0, 1_800_000, size=n), 0))

    raise ValueError(f"No hay generador implementado para extra field: {field!r}")


# -----------------------------------------------------------------------------
# Degradaciones controladas
# -----------------------------------------------------------------------------


def _apply_duplicate_mode(df: pd.DataFrame, *, duplicate_mode: str, rng: np.random.Generator) -> pd.DataFrame:
    work = df.copy(deep=True)
    if len(work) == 0 or duplicate_mode == "none":
        return work

    if duplicate_mode == "movement_id_only":
        if "movement_id" not in work.columns:
            raise ValueError("duplicate_mode='movement_id_only' requiere que exista la columna 'movement_id'.")
        idx = _sample_indices(len(work), 0.25, rng)
        if len(idx) == 0:
            return work
        source_idx = rng.choice(work.index.to_numpy(), size=len(idx), replace=True)
        dup_values = work.loc[source_idx, "movement_id"].astype("string").tolist()
        for target, dup_value in zip(idx.tolist(), dup_values):
            work.iloc[int(target), work.columns.get_loc("movement_id")] = dup_value
        return work

    if duplicate_mode == "full_rows":
        idx = _sample_indices(len(work), 0.25, rng)
        if len(idx) == 0:
            return work
        source_idx = rng.choice(work.index.to_numpy(), size=len(idx), replace=True)
        target_idx = work.index.to_numpy()[idx]
        copied = work.loc[source_idx].copy()
        copied.index = target_idx
        work.loc[target_idx] = copied
        return work

    raise ValueError(f"duplicate_mode no soportado: {duplicate_mode!r}")


def _null_value_for_series(series: pd.Series, rng: np.random.Generator) -> Any:
    if _infer_string_like(series):
        return "" if float(rng.random()) < 0.5 else pd.NA
    return np.nan


def _apply_null_ratio(
    df: pd.DataFrame,
    *,
    null_ratio: Optional[float | dict[str, float]],
    protected_columns: Optional[set[str]],
    rng: np.random.Generator,
) -> pd.DataFrame:
    work = df.copy(deep=True)
    if null_ratio is None:
        return work

    protected = set(protected_columns or set())
    if isinstance(null_ratio, (int, float)):
        ratio_map = {c: float(null_ratio) for c in work.columns}
    else:
        ratio_map = {str(k): float(v) for k, v in dict(null_ratio).items() if k in work.columns}

    for col, ratio in ratio_map.items():
        if col in protected:
            continue
        idx = _sample_indices(len(work), ratio, rng)
        if len(idx) == 0:
            continue
        fill_value = _null_value_for_series(work[col], rng)
        work.loc[work.index[idx], col] = fill_value
    return work


def _normalize_paired_missingness_config(paired_missingness: Any) -> list[tuple[str, float]]:
    if paired_missingness is None:
        return []
    if isinstance(paired_missingness, str):
        return [(paired_missingness, 0.25)]
    if isinstance(paired_missingness, dict):
        return [(str(k), float(v)) for k, v in paired_missingness.items()]
    out: list[tuple[str, float]] = []
    for item in _as_list(paired_missingness):
        if isinstance(item, str):
            out.append((item, 0.25))
        elif isinstance(item, dict):
            name = str(item.get("pattern"))
            ratio = float(item.get("ratio", 0.25))
            out.append((name, ratio))
        else:
            raise TypeError("paired_missingness debe usar str, list[str], dict[str, float] o list[dict].")
    return out


def _apply_paired_missingness(df: pd.DataFrame, *, paired_missingness: Any, rng: np.random.Generator) -> pd.DataFrame:
    work = df.copy(deep=True)
    patterns = _normalize_paired_missingness_config(paired_missingness)
    if not patterns:
        return work

    pattern_to_columns: dict[str, list[str]] = {
        "destination_coords_missing": ["destination_latitude", "destination_longitude"],
        "origin_coords_missing": ["origin_latitude", "origin_longitude"],
        "destination_lat_only_missing": ["destination_latitude"],
        "destination_lon_only_missing": ["destination_longitude"],
        "origin_lat_only_missing": ["origin_latitude"],
        "origin_lon_only_missing": ["origin_longitude"],
        "destination_time_missing": ["destination_time_utc", "destination_time_local_hhmm"],
        "origin_time_missing": ["origin_time_utc", "origin_time_local_hhmm"],
        "destination_h3_missing": ["destination_h3_index"],
        "origin_h3_missing": ["origin_h3_index"],
        "destination_incomplete": ["destination_latitude", "destination_longitude", "destination_h3_index", "destination_time_utc", "destination_time_local_hhmm"],
        "origin_incomplete": ["origin_latitude", "origin_longitude", "origin_h3_index", "origin_time_utc", "origin_time_local_hhmm"],
    }

    for pattern, ratio in patterns:
        cols = [c for c in pattern_to_columns.get(pattern, []) if c in work.columns]
        if not cols:
            continue
        idx = _sample_indices(len(work), ratio, rng)
        for col in cols:
            fill_value = _null_value_for_series(work[col], rng)
            work.loc[work.index[idx], col] = fill_value
    return work


def _build_noise_token(field: str, rng: np.random.Generator) -> Any:
    pool = [
        f"__noise__{field}",
        f"BAD_{field.upper()}",
        "???",
        "999999",
        "NO_MATCH",
    ]
    return rng.choice(pool)


def _apply_noise_ratio(
    df: pd.DataFrame,
    *,
    noise_ratio: Optional[dict[str, float]],
    rng: np.random.Generator,
) -> pd.DataFrame:
    work = df.copy(deep=True)
    if not noise_ratio:
        return work
    for col, ratio in noise_ratio.items():
        if col not in work.columns:
            continue
        idx = _sample_indices(len(work), float(ratio), rng)
        for i in idx:
            work.iloc[int(i), work.columns.get_loc(col)] = _build_noise_token(col, rng)
    return work


def _normalize_type_corruption_spec(spec: Any) -> tuple[str, float]:
    if isinstance(spec, str):
        return spec, 1.0
    if isinstance(spec, dict):
        mode = str(spec.get("mode", "mixed_bad_values"))
        ratio = float(spec.get("ratio", 1.0))
        return mode, ratio
    raise TypeError("Cada entrada de type_corruption debe ser str o dict.")


def _corrupt_value(value: Any, *, expected_field: str, mode: str, rng: np.random.Generator) -> Any:
    if mode == "numeric_as_string":
        if pd.isna(value):
            return value
        return str(value)
    if mode == "string_as_int":
        return int(rng.integers(1, 999))
    if mode == "non_numeric_text":
        return str(rng.choice(["abc", "no_num", "texto", "xx"] ))
    if mode == "mixed_bad_values":
        field_dtype = REQUIRED_FIELD_DTYPES.get(expected_field) or BASE_FIELD_DTYPES.get(expected_field)
        if expected_field in {"origin_latitude", "origin_longitude", "destination_latitude", "destination_longitude"}:
            return rng.choice(["abc", "33° xx S", "--70,66", "coord?", "nan?" ])
        if field_dtype in {"int", "float"}:
            return rng.choice(["abc", "no_num", "-", "valor" ])
        if field_dtype == "datetime":
            return rng.choice(["not-a-date", "2026/99/99", "31-31-2026", 12345])
        return int(rng.integers(1, 9999))
    if mode == "stringified":
        return str(value)
    return value


def _apply_type_corruption(
    df: pd.DataFrame,
    *,
    type_corruption: Optional[dict[str, Any]],
    protected_columns: Optional[set[str]],
    rng: np.random.Generator,
) -> pd.DataFrame:
    work = df.copy(deep=True)
    if not type_corruption:
        return work
    protected = set(protected_columns or set())
    for col, spec in type_corruption.items():
        if col not in work.columns or col in protected:
            continue
        mode, ratio = _normalize_type_corruption_spec(spec)
        idx = _sample_indices(len(work), ratio, rng)
        if len(idx) > 0 and not _infer_string_like(work[col]):
            work[col] = work[col].astype("object")
        for i in idx:
            current = work.iloc[int(i), work.columns.get_loc(col)]
            work.iloc[int(i), work.columns.get_loc(col)] = _corrupt_value(current, expected_field=col, mode=mode, rng=rng)
    return work


# -----------------------------------------------------------------------------
# Resolución de selección de columnas
# -----------------------------------------------------------------------------


def _resolve_base_fields(base_fields: Optional[Sequence[str]]) -> list[str]:
    if base_fields is None:
        return []
    fields = _dedupe_keep_order([str(f) for f in base_fields])
    unknown = sorted(set(fields) - set(BASE_FIELD_DTYPES.keys()))
    if unknown:
        raise ValueError(f"base_fields contiene campos no soportados: {unknown}")
    return fields


def _resolve_extra_columns(extra_columns: Optional[int | Sequence[str]], *, rng: np.random.Generator, excluded: set[str]) -> list[str]:
    if extra_columns is None:
        return []
    if isinstance(extra_columns, int):
        pool = sorted(set(EXTRA_FIELD_SPECS.keys()) - set(excluded))
        k = min(max(0, int(extra_columns)), len(pool))
        if k == 0:
            return []
        return list(rng.choice(np.array(pool, dtype=object), size=k, replace=False))
    fields = _dedupe_keep_order([str(f) for f in extra_columns])
    unknown = sorted(set(fields) - set(EXTRA_FIELD_SPECS.keys()))
    if unknown:
        raise ValueError(f"extra_columns contiene extras desconocidos: {unknown}")
    overlap = sorted(set(fields) & set(excluded))
    if overlap:
        raise ValueError(f"extra_columns no debe solaparse con required/base_fields. Solapados: {overlap}")
    return fields


def _resolve_omit_required(
    *,
    omit_required_fields: Optional[Sequence[str]],
    n_random_missing_required: int,
    tier_temporal: str,
    h3_mode: str,
    rng: np.random.Generator,
) -> list[str]:
    explicit = _dedupe_keep_order([str(f) for f in (omit_required_fields or [])])
    unknown = sorted(set(explicit) - set(REQUIRED_FIELDS_ORDER))
    if unknown:
        raise ValueError(f"omit_required_fields contiene requeridos desconocidos: {unknown}")

    auto = []
    if tier_temporal == "tier_2":
        auto.extend(["origin_time_utc", "destination_time_utc"])
    elif tier_temporal == "tier_3":
        auto.extend(["origin_time_utc", "destination_time_utc"])
    if h3_mode == "omitted_derivable":
        auto.extend(["origin_h3_index", "destination_h3_index"])

    current = _dedupe_keep_order(explicit + auto)
    remaining = [f for f in REQUIRED_FIELDS_ORDER if f not in current]
    k = min(max(0, int(n_random_missing_required)), len(remaining))
    if k > 0:
        sampled = list(rng.choice(np.array(remaining, dtype=object), size=k, replace=False))
        current = _dedupe_keep_order(current + sampled)
    return current


def _validate_cross_argument_constraints(
    *,
    omit_required_fields: list[str],
    field_correspondence: Optional[dict[str, str]],
    duplicate_mode: str,
    tier_temporal: str,
    tier1_datetime_format: str,
    tier2_hhmm_format: str,
    h3_mode: str,
    trip_structure: str,
    max_movements_per_trip: int,
    base_fields: list[str],
) -> None:
    fc = dict(field_correspondence or {})
    _ensure_known_fields(fc.keys(), argument_name="field_correspondence.keys")

    forbidden = sorted(set(omit_required_fields) & set(fc.keys()))
    if forbidden:
        raise ValueError(
            "field_correspondence no debe referenciar campos ya omitidos. "
            f"Campos conflictivos: {forbidden}"
        )

    if duplicate_mode == "movement_id_only" and "movement_id" in omit_required_fields:
        raise ValueError("duplicate_mode='movement_id_only' no es compatible con omitir 'movement_id'.")

    if h3_mode in {"provided_valid", "partial_missing", "invalid_strings"}:
        if "origin_h3_index" in omit_required_fields or "destination_h3_index" in omit_required_fields:
            raise ValueError(
                f"h3_mode={h3_mode!r} requiere mantener columnas H3; no es compatible con omitirlas."
            )

    if tier_temporal == "tier_1":
        _validate_choice("tier1_datetime_format", tier1_datetime_format, TIER1_FORMATS)
    if tier_temporal == "tier_2":
        _validate_choice("tier2_hhmm_format", tier2_hhmm_format, TIER2_FORMATS)
    if trip_structure != "multistage" and max_movements_per_trip != 1:
        # No es un error fatal, pero aquí lo dejamos explícito para evitar confusión silenciosa.
        raise ValueError(
            "max_movements_per_trip solo tiene efecto real con trip_structure='multistage'. "
            "Para otros casos usa 1."
        )

    # HH:MM puede existir como base también en tier_1/tier_3. Eso se permite.
    overlap = sorted(set(base_fields) & set(REQUIRED_FIELDS_ORDER))
    if overlap:
        raise ValueError(f"base_fields no debe incluir campos requeridos. Solapados: {overlap}")
