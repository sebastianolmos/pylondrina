from utils import fix_and_get_path, save_csv

ROOT = fix_and_get_path(2)
OUT_DIR = ROOT / "data" / "synthetic" 

from scripts.synthetic_data.base_generator import generate_synthetic_trip_dataframe


if __name__ == "__main__":

    # G3: warnings no fatales grandes (datetime parcial Tier 1)
    df_g3 = generate_synthetic_trip_dataframe(
        filas=1200,
        seed=42,
        tier_temporal="tier_1",
        tier1_datetime_format="mixed_with_invalids",
        coord_format="numeric",
        h3_mode="provided_valid",
        trip_structure="independent",
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
    
    save_csv(df_g3, OUT_DIR, "datetime_partial_1200")