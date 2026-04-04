from utils import fix_and_get_path, save_csv

ROOT = fix_and_get_path(2)
OUT_DIR = ROOT / "data" / "synthetic" 

from scripts.synthetic_data.base_generator import save_synthetic_trip_csv


if __name__ == "__main__":

    SOURCE_FIELD_CORRESPONDENCE = {
        "movement_id": "id_movimiento",
        "user_id": "id_usuario",
        "origin_longitude": "lon_origen",
        "origin_latitude": "lat_origen",
        "destination_longitude": "lon_destino",
        "destination_latitude": "lat_destino",
        "origin_h3_index": "h3_origen",
        "destination_h3_index": "h3_destino",
        "origin_time_utc": "fecha_hora_origen",
        "destination_time_utc": "fecha_hora_destino",
        "trip_id": "id_viaje",
        "movement_seq": "seq_movimiento",
        "origin_municipality": "comuna_origen",
        "destination_municipality": "comuna_destino",
        "timezone_offset_min": "offset_tz_min",
        "origin_time_local_hhmm": "hora_local_origen",
        "destination_time_local_hhmm": "hora_local_destino",
        "trip_weight": "factor_expansion",
        "mode_sequence": "secuencia_modos",
        "mode": "modo_fuente",
        "purpose": "proposito_fuente",
        "day_type": "tipo_dia",
        "time_period": "franja_horaria",
        "user_gender": "genero_usuario",
        "user_age_group": "grupo_etario",
        "income_quintile": "quintil_ingreso",
    }

    GENERATOR_KWARGS = {
        "filas": 10_000,
        "seed": 20260404,
        "omit_required_fields": None,
        "n_random_missing_required": 0,
        "field_correspondence": SOURCE_FIELD_CORRESPONDENCE,
        "duplicate_mode": "none",
        "tier_temporal": "tier_1",
        "tier1_datetime_format": "utc_string_z",
        "coord_format": "numeric",
        "h3_mode": "provided_valid",
        "trip_structure": "multistage",
        "max_movements_per_trip": 3,
        "base_fields": [
            "origin_municipality",
            "destination_municipality",
            "timezone_offset_min",
            "origin_time_local_hhmm",
            "destination_time_local_hhmm",
            "trip_weight",
            "mode_sequence",
            "mode",
            "purpose",
            "day_type",
            "time_period",
            "user_gender",
            "user_age_group",
            "income_quintile",
        ],
        "extra_value_domains": {
            "mode": ["submodes"],
            "purpose": ["finos"],
            "day_type": ["canon"],
            "time_period": ["canon"],
            "user_gender": ["canon"],
            "user_age_group": ["canon"],
            "income_quintile": ["canon"],
        },
        "categorical_sampling_policy": {
            "mode": {
                "canonical_ratio": 0.70,
                "distribution": "weighted",
                "ensure_presence": True,
            },
            "purpose": {
                "canonical_ratio": 0.75,
                "distribution": "weighted",
                "ensure_presence": True,
            },
        },
        "extra_columns": [
            "activity_destination",
            "season",
            "travel_time_quality_code",
            "fare_payment_type",
            "highway_avoid_reason",
            "bike_lane_usage",
            "bike_lane_issue",
            "bike_parking_type",
            "cut_type_stage_trip",
            "travel_time_bucket",
            "home_tenure",
            "education_level",
            "activity_status",
            "occupation_type",
            "travel_time_min",
            "fare_amount",
            "distance_route_m",
            "in_vehicle_travel_time_min",
            "household_id",
            "public_transport_route_code",
        ],
        "null_ratio": None,
        "paired_missingness": None,
        "noise_ratio": None,
        "type_corruption": None,
    }

    OUTPUT_PATH = OUT_DIR / "demo_trips_happy_path_source.csv"

    df = save_synthetic_trip_csv(
        output_path=str(OUTPUT_PATH),
        sep=",",
        index=False,
        **GENERATOR_KWARGS,
    )

    print("Archivo generado:", OUTPUT_PATH.resolve())
    print("Shape:", df.shape)
    print("Columnas:", len(df.columns))
    print("Tamaño CSV (bytes):", OUTPUT_PATH.stat().st_size)
    print(df.head())
    