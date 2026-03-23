from utils import fix_and_get_path, save_csv

ROOT = fix_and_get_path(2)
OUT_DIR = ROOT / "data" / "synthetic" 

from scripts.synthetic_data.gen_wrappers import make_happy_path_minimal


if __name__ == "__main__":
    # G1: happy path canónico grande
    df_g1 = make_happy_path_minimal(
        filas=1200,
        seed=42,
    )
    save_csv(df_g1, OUT_DIR, "happy_path_minimal_1200")