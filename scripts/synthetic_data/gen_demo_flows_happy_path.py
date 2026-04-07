from utils import fix_and_get_path, save_csv

ROOT = fix_and_get_path(2)
OUT_DIR = ROOT / "data" / "synthetic" 

from scripts.synthetic_data.base_generator import generate_synthetic_trip_dataframe


if __name__ == "__main__":

    OUTPUT_CSV = OUT_DIR / "demo_trips_for_flows_hp.csv"

    FIELD_CORRESPONDENCE_DEMO = {
        "movement_id": "id_movimiento",
        "user_id": "id_usuario",
        "origin_longitude": "lon_origen",
        "origin_latitude": "lat_origen",
        "destination_longitude": "lon_destino",
        "destination_latitude": "lat_destino",
        "origin_time_utc": "ts_salida_utc",
        "destination_time_utc": "ts_llegada_utc",
        "trip_id": "id_viaje",
        "movement_seq": "orden_movimiento",
        "origin_municipality": "comuna_origen",
        "destination_municipality": "comuna_destino",
        "timezone_offset_min": "utc_offset_min",
        "trip_weight": "factor_expansion",
        "mode_sequence": "secuencia_modos",
        "mode": "modo",
        "purpose": "proposito",
        "day_type": "tipo_dia",
        "time_period": "periodo_horario",
        "user_gender": "genero",
        "user_age_group": "tramo_edad",
        "income_quintile": "quintil_ingreso",
        "travel_time_min": "tiempo_viaje_min",
        "fare_amount": "tarifa_clp",
        "public_transport_route_code": "codigo_ruta_tp",
        "distance_route_m": "distancia_ruta_m",
        "household_income_clp": "ingreso_hogar_clp",
        "personal_income_clp": "ingreso_personal_clp",
        "activity_status": "estado_actividad",
        "occupation_type": "tipo_ocupacion",
    }

    GENERATOR_KWARGS_DEMO = {
        "filas": 15_000,
        "seed": 20260407,
        "duplicate_mode": "none",
        "tier_temporal": "tier_1",
        "tier1_datetime_format": "utc_string_z",
        "coord_format": "numeric",
        "od_spatial_pattern": "clustered_for_flows",
        "h3_mode": "omitted_derivable",
        "trip_structure": "independent",
        "max_movements_per_trip": 1,
        "base_fields": [
            "origin_municipality",
            "destination_municipality",
            "timezone_offset_min",
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
        "categorical_sampling_policy": {
            "mode": {
                "distribution": "weighted",
                "ensure_presence": True,
                "weights": {
                    "walk": 0.10,
                    "bicycle": 0.02,
                    "scooter": 0.01,
                    "motorcycle": 0.01,
                    "car": 0.18,
                    "taxi": 0.02,
                    "ride_hailing": 0.02,
                    "bus": 0.36,
                    "metro": 0.24,
                    "train": 0.02,
                    "other": 0.02,
                },
            },
            "purpose": {
                "distribution": "weighted",
                "ensure_presence": True,
                "weights": {
                    "home": 0.08,
                    "work": 0.34,
                    "education": 0.20,
                    "shopping": 0.12,
                    "errand": 0.08,
                    "health": 0.04,
                    "leisure": 0.09,
                    "transfer": 0.03,
                    "other": 0.02,
                },
            },
            "day_type": {
                "distribution": "weighted",
                "ensure_presence": True,
                "weights": {
                    "weekday": 0.82,
                    "weekend": 0.16,
                    "holiday": 0.02,
                },
            },
            "time_period": {
                "distribution": "weighted",
                "ensure_presence": True,
                "weights": {
                    "night": 0.05,
                    "morning": 0.34,
                    "midday": 0.20,
                    "afternoon": 0.25,
                    "evening": 0.16,
                },
            },
            "user_gender": {
                "distribution": "weighted",
                "ensure_presence": True,
                "weights": {
                    "female": 0.51,
                    "male": 0.48,
                    "other": 0.005,
                    "unknown": 0.005,
                },
            },
            "user_age_group": {
                "distribution": "weighted",
                "ensure_presence": True,
                "weights": {
                    "0-14": 0.04,
                    "15-24": 0.16,
                    "25-34": 0.24,
                    "35-44": 0.20,
                    "45-54": 0.16,
                    "55-64": 0.11,
                    "65-plus": 0.07,
                    "unknown": 0.02,
                },
            },
            "income_quintile": {
                "distribution": "weighted",
                "ensure_presence": True,
                "weights": {
                    "1": 0.18,
                    "2": 0.21,
                    "3": 0.24,
                    "4": 0.21,
                    "5": 0.14,
                    "unknown": 0.02,
                },
            },
        },
        "extra_columns": [
            "household_id",
            "source_person_id",
            "source_trip_number",
            "stage_count",
            "origin_zone_id",
            "destination_zone_id",
            "origin_sector_id",
            "destination_sector_id",
            "travel_time_min",
            "fare_amount",
            "public_transport_route_code",
            "distance_route_m",
            "household_income_clp",
            "personal_income_clp",
            "activity_status",
            "occupation_type",
            "job_sector",
            "work_schedule",
            "travel_time_bucket",
            "income_imputed_flag",
        ],
        "field_correspondence": FIELD_CORRESPONDENCE_DEMO,
    }

    df_source = generate_synthetic_trip_dataframe(**GENERATOR_KWARGS_DEMO)
    df_source.to_csv(OUTPUT_CSV, sep=",", index=False)

    print("CSV generado en:", OUTPUT_CSV.resolve())
    print("shape:", df_source.shape)
    print("columnas:", len(df_source.columns))
    print(df_source.head(5))
    