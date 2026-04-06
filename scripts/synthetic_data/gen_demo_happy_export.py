from utils import fix_and_get_path, save_csv

ROOT = fix_and_get_path(2)
OUT_DIR = ROOT / "data" / "synthetic" 

from scripts.synthetic_data.base_generator import save_synthetic_trip_csv


if __name__ == "__main__":

    # Mapeo canónico -> nombre fuente.
    SOURCE_FIELD_CORRESPONDENCE = {
        "movement_id": "id_movimiento",
        "user_id": "id_usuario",
        "origin_longitude": "lon_origen",
        "origin_latitude": "lat_origen",
        "destination_longitude": "lon_destino",
        "destination_latitude": "lat_destino",
        "origin_time_utc": "ts_origen",
        "destination_time_utc": "ts_destino",
        "trip_id": "id_viaje",
        "movement_seq": "orden_movimiento",
        "origin_municipality": "comuna_origen",
        "destination_municipality": "comuna_destino",
        "timezone_offset_min": "offset_tz_min",
        "origin_time_local_hhmm": "hora_origen_local",
        "destination_time_local_hhmm": "hora_destino_local",
        "trip_weight": "peso_viaje",
        "mode_sequence": "secuencia_modos",
        "mode": "modo_fuente",
        "purpose": "proposito_fuente",
        "day_type": "tipo_dia_fuente",
        "time_period": "franja_horaria_fuente",
        "user_gender": "sexo_fuente",
        "user_age_group": "tramo_edad_fuente",
        "income_quintile": "quintil_ingreso_fuente",
    }

    GENERATOR_KWARGS = dict(
        filas=18000,
        seed=20260406,
        field_correspondence=SOURCE_FIELD_CORRESPONDENCE,
        duplicate_mode="none",
        tier_temporal="tier_1",
        tier1_datetime_format="utc_string_z",
        coord_format="numeric",
        # Importante: se omiten los H3 para que el import los derive a resolución 12.
        h3_mode="omitted_derivable",
        trip_structure="multistage",
        max_movements_per_trip=3,
        base_fields=[
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
        # Quiero que algunas categorías vengan en etiquetas "fuente" para demostrar
        # la normalización con value_correspondence, pero sin volver la demo inmanejable.
        extra_value_domains={
            "day_type": ["etiquetas_fuente"],
            "user_gender": ["es"],
        },
        categorical_sampling_policy={
            "mode": {
                "distribution": "uniform",
                "ensure_presence": True,
                "weights": {
                    "bus": 8.0,
                    "metro": 6.0,
                    "car": 4.0,
                    "walk": 3.0,
                    "ride_hailing": 2.0,
                    "taxi": 1.5,
                    "bicycle": 1.0,
                    "scooter": 0.8,
                    "motorcycle": 0.7,
                    "train": 0.5,
                    "other": 0.5,
                },
            },
            "purpose": {
                "distribution": "uniform",
                "ensure_presence": True,
                "weights": {
                    "work": 8.0,
                    "home": 6.0,
                    "education": 4.0,
                    "shopping": 2.0,
                    "errand": 1.5,
                    "health": 1.2,
                    "leisure": 1.8,
                    "transfer": 0.8,
                    "other": 0.5,
                },
            },
            # Con canonical_ratio=0.0 fuerzo valores de "fuente" para que la demo use
            # value_correspondence de verdad.
            "day_type": {
                "canonical_ratio": 0.0,
                "distribution": "uniform",
                "ensure_presence": True,
                "weights": {
                    "Laboral": 8.0,
                    "Fin de Semana": 2.0,
                    "DOMINGO": 1.0,
                },
            },
            "user_gender": {
                "canonical_ratio": 0.0,
                "distribution": "uniform",
                "ensure_presence": True,
                "weights": {
                    "Hombre": 1.0,
                    "Mujer": 1.0,
                },
            },
            "time_period": {
                "distribution": "uniform",
                "ensure_presence": True,
                "weights": {
                    "morning": 5.0,
                    "afternoon": 4.0,
                    "evening": 3.0,
                    "midday": 2.0,
                    "night": 1.0,
                },
            },
        },
        extra_columns=[
            "activity_status",
            "education_level",
            "travel_time_bucket",
            "season",
            "fare_payment_type",
            "home_tenure",
            "travel_time_min",
            "fare_amount",
            "public_transport_route_code",
            "distance_route_m",
            "household_income_clp",
            "occupation_type",
            "job_sector",
            "work_schedule",
            "origin_stop_id",
            "destination_stop_id",
        ],
    )


    OUTPUT_PATH = OUT_DIR / "demo_happy_export.csv"

    df = save_synthetic_trip_csv(
        output_path=str(OUTPUT_PATH),
        sep=",",
        index=False,
        **GENERATOR_KWARGS,
    )

    print("CSV generado en:", OUTPUT_PATH.resolve())
    print("Shape:", df.shape)
    print("Columnas:", len(df.columns))
    print(df.head())
    