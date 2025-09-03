# mlb_http.py
import os, json, time, threading
import requests
from typing import Optional, Dict, Any, Tuple
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

MLB_API_BASE = "https://statsapi.mlb.com/api/v1"
MLB_QPS = float(os.getenv("MLB_QPS", "5"))        # max ~5 req/sec
MLB_CONNECT_TIMEOUT = int(os.getenv("MLB_CONNECT_TIMEOUT", "5"))
MLB_READ_TIMEOUT = int(os.getenv("MLB_READ_TIMEOUT", "10"))
MLB_CACHE_TTL = int(os.getenv("MLB_CACHE_TTL", str(60*60*12)))  # 12h

_session = requests.Session()
_session.headers.update({"Accept": "application/json"})
_session.mount("https://", HTTPAdapter(max_retries=Retry(
    total=4, backoff_factor=0.3,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET"])
)))

# naive in-proc cache; Redis is better if available
_cache: Dict[str, Tuple[float, Any]] = {}
_lock = threading.Lock()
_last_ts = 0.0

def _rate_limit():
    global _last_ts
    min_interval = 1.0 / max(MLB_QPS, 1.0)
    with _lock:
        now = time.time()
        wait = _last_ts + min_interval - now
        if wait > 0:
            time.sleep(wait)
        _last_ts = time.time()

def _ckey(path: str, params: Optional[Dict[str, Any]]) -> str:
    return f"{path}?{json.dumps(params, sort_keys=True)}"

def get_json(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    key = _ckey(path, params)
    now = time.time()
    with _lock:
        hit = _cache.get(key)
        if hit and (now - hit[0] < MLB_CACHE_TTL):
            return hit[1]
    _rate_limit()
    resp = _session.get(f"{MLB_API_BASE}/{path.lstrip('/')}",
                        params=params, timeout=(MLB_CONNECT_TIMEOUT, MLB_READ_TIMEOUT))
    resp.raise_for_status()
    data = resp.json()
    with _lock:
        _cache[key] = (time.time(), data)
    return data
