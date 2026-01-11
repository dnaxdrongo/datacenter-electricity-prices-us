from __future__ import annotations

from dataclasses import dataclass
import pandas as pd

from .http import get_json

@dataclass(frozen=True)
class EIAResult:
    df: pd.DataFrame
    total: int

def fetch_eia_v2_all_pages(
    session,
    base_url: str,
    base_params: dict,
    api_key: str,
    page_size: int = 5000,
    timeout_s: int = 60,
) -> EIAResult:
    """
    Fetch all pages for an EIA API v2 endpoint that uses offset/length pagination.
    Assumes response structure includes: {"response": {"total": int, "data": [...]}}
    """
    frames = []
    offset = 0
    total = None

    while True:
        params = dict(base_params)
        params["api_key"] = api_key
        params["offset"] = offset
        params["length"] = page_size

        payload = get_json(session, base_url, params=params, timeout_s=timeout_s)
        resp = payload.get("response", {})
        if total is None:
            total = int(resp.get("total", 0))

        data = resp.get("data", [])
        if not data:
            break

        frames.append(pd.DataFrame(data))

        offset += page_size
        if offset >= total:
            break

    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return EIAResult(df=df, total=(total or 0))
