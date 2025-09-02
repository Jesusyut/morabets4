# universal_cache.py
import os, json, time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional, Dict
try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

PHX_TZ = ZoneInfo("America/Phoenix") if ZoneInfo else None

# Optional Redis
_redis = None
REDIS_URL = os.getenv("REDIS_URL", "")
if REDIS_URL:
    try:
        import redis
        _redis = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    except Exception:
        _redis = None

# Fallback memory cache: {key: {"exp": epoch_sec, "val": json_str}}
_mem: Dict[str, Dict[str, Any]] = {}

SLOTS = [8, 13, 18]  # local hours in America/Phoenix

def _now_local() -> datetime:
    utc = datetime.now(timezone.utc)
    return utc.astimezone(PHX_TZ) if PHX_TZ else utc

def current_slot(dt: Optional[datetime] = None) -> tuple[str, datetime]:
    """Return (slot_name, next_boundary_local) based on Phoenix local time."""
    dt = dt or _now_local()
    h = dt.hour
    # Boundaries: 08, 13, 18; everything before 08 belongs to 'night'
    if h < 8:
        slot = "night"
        next_b = dt.replace(hour=8, minute=0, second=0, microsecond=0)
    elif h < 13:
        slot = "morning"
        next_b = dt.replace(hour=13, minute=0, second=0, microsecond=0)
    elif h < 18:
        slot = "afternoon"
        next_b = dt.replace(hour=18, minute=0, second=0, microsecond=0)
    else:
        slot = "night"
        # next is tomorrow 08:00
        next_day = (dt + timedelta(days=1)).replace(hour=8, minute=0, second=0, microsecond=0)
        next_b = next_day
    return slot, next_b

def _key(league: str, dt: Optional[datetime] = None) -> str:
    dt = dt or _now_local()
    d = dt.date().isoformat()
    slot, _ = current_slot(dt)
    return f"props:{league}:{d}:{slot}"

def _ttl_seconds(next_boundary: datetime) -> int:
    now = _now_local()
    ttl = int((next_boundary - now).total_seconds())
    return max(ttl, 60)  # at least 60s

def get_cached(league: str) -> Optional[Any]:
    k = _key(league)
    if _redis:
        raw = _redis.get(k)
        if raw: return json.loads(raw)
    else:
        rec = _mem.get(k)
        if rec and rec["exp"] > time.time():
            return json.loads(rec["val"])
    return None

def set_cached(league: str, value: Any) -> None:
    k = _key(league)
    _, next_b = current_slot()
    ttl = _ttl_seconds(next_b)
    raw = json.dumps(value)
    if _redis:
        _redis.setex(k, ttl, raw)
    else:
        _mem[k] = {"exp": time.time() + ttl, "val": raw}

def get_or_set(league: str, fetcher: Callable[[], Any]) -> Any:
    data = get_cached(league)
    if data is not None:
        return data
    data = fetcher()
    set_cached(league, data)
    return data
