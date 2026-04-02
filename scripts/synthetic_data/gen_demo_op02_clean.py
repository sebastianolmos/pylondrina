from utils import fix_and_get_path, save_csv

ROOT = fix_and_get_path(2)
OUT_DIR = ROOT / "data" / "synthetic" 

from scripts.synthetic_data.base_generator import generate_synthetic_trip_dataframe


if __name__ == "__main__":

    df_demo_validate_clean = generate_synthetic_trip_dataframe(
        300,
        seed=20260402,
        duplicate_mode="none",
        tier_temporal="tier_1",
        tier1_datetime_format="utc_string_z",
        coord_format="numeric",
        h3_mode="provided_valid",
        trip_structure="independent",
        max_movements_per_trip=1,
        base_fields=[
            "mode",
            "purpose",
            "user_gender",
            "origin_municipality",
            "destination_municipality",
            "trip_weight",
            "timezone_offset_min",
        ],
        categorical_sampling_policy={
            "mode": {
                "canonical_ratio": 1.0,
                "ensure_presence": True,
                "distribution": "uniform",
            },
            "purpose": {
                "canonical_ratio": 1.0,
                "ensure_presence": True,
                "distribution": "uniform",
            },
            "user_gender": {
                "canonical_ratio": 1.0,
                "ensure_presence": True,
                "distribution": "uniform",
            },
        },
        extra_columns=2,
    )
    
    save_csv(df_demo_validate_clean, OUT_DIR, "demo_op02_clean")