from __future__ import annotations

import pandas as pd

def qa_overview(df: pd.DataFrame, name: str) -> dict:
    return {
        "dataset": name,
        "rows": int(df.shape[0]),
        "cols": int(df.shape[1]),
        "duplicate_rows": int(df.duplicated().sum()),
        "missing_cells": int(df.isna().sum().sum()),
        "missing_pct": float(df.isna().mean().mean()) if df.size else 0.0,
    }

def qa_column_profile(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame({
        "dtype": df.dtypes.astype(str),
        "n_missing": df.isna().sum(),
        "pct_missing": df.isna().mean(),
        "n_unique": df.nunique(dropna=True),
    })
    return out.sort_values(["pct_missing", "n_unique"], ascending=[False, False])

def assert_required_columns(df: pd.DataFrame, required: list[str], name: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{name}: missing required columns: {missing}")

def check_state_codes(series: pd.Series, name: str) -> pd.Series:
    bad = ~series.astype(str).str.match(r"^[A-Za-z]{2}$", na=False)
    return series[bad].dropna()
