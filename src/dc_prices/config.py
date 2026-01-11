from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os


# ---------- Paths ----------

@dataclass(frozen=True)
class ProjectPaths:
    repo_root: Path
    data_raw: Path
    data_interim: Path
    reports_qa: Path
    reports_eda: Path

    @staticmethod
    def from_repo_root(repo_root: Path) -> "ProjectPaths":
        repo_root = repo_root.resolve()
        return ProjectPaths(
            repo_root=repo_root,
            data_raw=repo_root / "data" / "raw",
            data_interim=repo_root / "data" / "interim",
            reports_qa=repo_root / "reports" / "qa",
            reports_eda=repo_root / "reports" / "eda",
        )

    def ensure_dirs(self) -> "ProjectPaths":
        """Create expected directories (safe to call repeatedly)."""
        for p in (self.data_raw, self.data_interim, self.reports_qa, self.reports_eda):
            p.mkdir(parents=True, exist_ok=True)
        return self


def find_repo_root(start: Path | None = None) -> Path:
    """
    Walk upward from CWD until finding a repo marker.
    Works from notebooks, scripts, tests, and CI without hardcoding paths.
    """
    start_path = (start or Path.cwd()).resolve()

    markers = ("pyproject.toml", ".git", "setup.cfg")
    for candidate in (start_path, *start_path.parents):
        if any((candidate / m).exists() for m in markers):
            return candidate

    raise RuntimeError(
        "Could not locate repo root. Expected one of: pyproject.toml, .git, setup.cfg "
        f"while searching from: {start_path}"
    )


def get_paths(start: Path | None = None, create_dirs: bool = True) -> ProjectPaths:
    paths = ProjectPaths.from_repo_root(find_repo_root(start))
    return paths.ensure_dirs() if create_dirs else paths


# ---------- Settings ----------

@dataclass(frozen=True)
class Settings:
    eia_api_key: str
    eia_timeout_s: int = 60
    eia_page_size: int = 5000  


def _try_load_dotenv() -> None:
    """
    Optional .env support (recommended for local dev).
    If python-dotenv isn't installed, this silently does nothing.
    Environment variables still override .env values.
    """
    try:
        from dotenv import load_dotenv, find_dotenv  
    except Exception:
        return

    dotenv_path = find_dotenv(usecwd=True)
    if dotenv_path:
        load_dotenv(dotenv_path, override=False)


def load_settings(load_dotenv: bool = True) -> Settings:
    if load_dotenv:
        _try_load_dotenv()

    api_key = os.getenv("EIA_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError(
            "Missing EIA_API_KEY environment variable. "
            "Set it in your shell or create a .env file in the repo root with:\n"
        )
    return Settings(eia_api_key=api_key)
