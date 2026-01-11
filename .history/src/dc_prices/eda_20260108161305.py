from __future__ import annotations

from pathlib import Path
import pandas as pd

def write_profile_report(df: pd.DataFrame, out_html: Path, title: str, sample: int | None = 200_000) -> None:
    from ydata_profiling import ProfileReport

    if sample and len(df) > sample:
        df_use = df.sample(sample, random_state=42)
    else:
        df_use = df

    profile = ProfileReport(df_use, title=title, minimal=True)
    out_html.parent.mkdir(parents=True, exist_ok=True)
    profile.to_file(str(out_html))
