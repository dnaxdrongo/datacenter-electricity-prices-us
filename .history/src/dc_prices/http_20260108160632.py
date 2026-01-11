from __future__ import annotations

from requests_cache import CachedSession
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import requests

def make_cached_session(cache_name: str = "http_cache", expire_after_s: int = 24 * 3600) -> CachedSession:
    # SQLite cache under current working directory by default
    return CachedSession(
        cache_name=cache_name,
        backend="sqlite",
        expire_after=expire_after_s,
        allowable_methods=("GET",),
        stale_if_error=True,
    )

@retry(
    reraise=True,
    stop=stop_after_attempt(6),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    retry=retry_if_exception_type((requests.RequestException, TimeoutError)),
)
def get_json(session: requests.Session, url: str, params: dict, timeout_s: int = 60) -> dict:
    resp = session.get(url, params=params, timeout=timeout_s)
    resp.raise_for_status()
    return resp.json()
