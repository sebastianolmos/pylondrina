from utils import fix_and_get_path, save_csv

ROOT = fix_and_get_path(2)
OUT_DIR = ROOT / "data" / "synthetic" 

from scripts.synthetic_data.gen_wrappers import make_happy_path_rich


if __name__ == "__main__":
    # G4: selección final / extras grande
    df_g4 = make_happy_path_rich(
        filas=1200,
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
    save_csv(df_g4, OUT_DIR, "selection_output_1200")