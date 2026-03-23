from utils import fix_and_get_path, save_csv

ROOT = fix_and_get_path(2)
OUT_DIR = ROOT / "data" / "synthetic" 

from scripts.synthetic_data.gen_wrappers import make_happy_path_rich


if __name__ == "__main__":
    # G2: correspondencias grande
    field_corr_g2 = {
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

    df_g2 = make_happy_path_rich(
        filas=1200,
        seed=42,
        field_correspondence=field_corr_g2,
        base_fields=[
            "mode",
            "purpose",
            "day_type",
            "time_period",
            "user_gender",
            "trip_weight",
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

    pattern_mode = ["A PIE", "BUS", "AUTO", "METRO"]
    pattern_purpose = ["TRABAJO", "ESTUDIO", "HOGAR", "COMPRAS"]

    df_g2["modo_fuente"] = [pattern_mode[i % 4] for i in range(len(df_g2))]
    df_g2["proposito_fuente"] = [pattern_purpose[i % 4] for i in range(len(df_g2))]
    save_csv(df_g2, OUT_DIR, "large_correpondence_1200")