from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os

@dataclass(frozen=True)
class ProjectPaths:
    repo_root: Path
    data_raw: Path
    data_interim: Path
    reports_qa: Path
    reports_eda: Path

    @staticmethod
    def from_repo_root(repo_root: Path) -> "ProjectPaths":
        return ProjectPaths(
            repo_root=repo_root,
            data_raw=repo_root / "data" / "raw",
            data_interim=repo_root / "data" / "interim",
            reports_qa=repo_root / "reports" / "qa",
            reports_eda=repo_root / "reports" / "eda",
        )

@dataclass(frozen=True)
class Settings:
    eia_api_key: str
    eia_timeout_s: int = 60
    eia_page_size: int = 5000  # EIA API max rows per response (JSON) :contentReference[oaicite:1]{index=1}

def load_settings() -> Settings:
    api_key = os.getenv("EIA_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError(
            "Missing EIA_API_KEY environment variable. "
            "Set it in your shell or a .env file before running."
        )
    return Settings(eia_api_key=api_key)
