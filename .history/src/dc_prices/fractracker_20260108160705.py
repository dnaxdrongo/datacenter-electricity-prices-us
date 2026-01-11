from __future__ import annotations

from pathlib import Path
import pandas as pd
import re

def snake_case(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^\w\s]+", "", s)     # drop punctuation
    s = re.sub(r"\s+", "_", s)         # spaces -> underscore
    s = s.replace("__", "_")
    return s

def load_fractracker_csv(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df.columns = [snake_case(c) for c in df.columns]
    return df
