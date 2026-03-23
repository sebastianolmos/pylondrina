from pathlib import Path
import sys
import pandas as pd

def fix_and_get_path(ascend: int):
    REPO_ROOT = Path(__file__).resolve().parents[ascend]
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    return REPO_ROOT

def save_csv(df: pd.DataFrame, path ,name: str) -> None:
    path = path / f"{name}.csv"
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"[OK] {name}: {df.shape} -> {path}")
