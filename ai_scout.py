# ai_scout.py
import os, json, datetime as dt
from typing import List, Dict, Any, Optional

# --- Redis (optional) ---
REDIS_URL = os.getenv("REDIS_URL", "")
_redis = None
if REDIS_URL:
    try:
        import redis  # already in requirements for the app
        _redis = redis.from_url(REDIS_URL)
    except Exception:
        _redis = None

def _cache_get(key: str) -> Optional[dict]:
    if _redis:
        v = _redis.get(key)
        return json.loads(v) if v else None
    return None

def _cache_set(key: str, value: dict, ttl: int = 60*60*8) -> None:  # default 8h
    payload = json.dumps(value)
    if _redis:
        _redis.setex(key, ttl, payload)

# ---- Deterministic overlay (no LLM) ----
from novig import american_to_prob  # existing util in repo

def attach_ai_edges(rows: List[Dict[str, Any]], min_edge: float = 0.06, cap: int = 120) -> int:
    """
    edge_over = fair_prob_over - book_implied_over
    Adds r['ai'] = {'edge_over': float}. Returns count of rows processed.
    """
    if not rows:
        return 0
    attached = 0
    n = 0
    for r in rows:
        if n >= cap:
            break
        try:
            fair = (r.get("fair") or {}).get("prob") or {}
            p_over = float(fair.get("over", 0.0))
            shop_over = ((r.get("shop") or {}).get("over") or {})
            amer = shop_over.get("american", None)
            if amer is None:
                n += 1
                continue
            q_over = float(american_to_prob(int(amer)))
            edge = max(0.0, p_over - q_over)
            r.setdefault("ai", {})["edge_over"] = round(edge, 4)
            if edge >= (min_edge or 0.06):
                attached += 1
            n += 1
        except Exception:
            n += 1
            continue
    return attached

# ---- LLM Picks (cached) ----
_OPENAI_MODEL = os.getenv("AI_MODEL", "gpt-4o-mini")
_AI_TTL = int(os.getenv("AI_CACHE_TTL_SECONDS", str(60*60*8)))  # 8h default
_AI_PICK_VERSION = os.getenv("AI_PICK_VERSION", "v1")

def build_llm_prompt(slate: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Construct a compact messages payload from enriched props.
    Only include the top ~200 rows for cost control.
    """
    take = min(len(slate), 200)
    slim = []
    for r in slate[:take]:
        slim.append({
            "player": r.get("player"),
            "team": r.get("team"),
            "matchup": r.get("matchup"),
            "stat": (r.get("stat") or r.get("type")),
            "line": r.get("line"),
            "fair_over": (r.get("fair") or {}).get("prob", {}).get("over"),
            "book_over_american": ((r.get("shop") or {}).get("over") or {}).get("american"),
            "ai_edge_over": (r.get("ai") or {}).get("edge_over", 0.0),
        })
    sys = (
        "You are a cautious betting analyst. From JSON props, return a JSON with "
        "fields: picks (array of up to 10 items), each {player, stat, line, reason, "
        "edge_over (0-1), confidence (0-100)}, and notes (1-2 bullet lines). "
        "Prefer edges >= 0.06, realistic markets, diversify teams/positions. "
        "Never invent players; only use given rows. Keep reasons short and data-backed."
    )
    user = {"league":"MLB","rows": slim}
    return {
        "model": _OPENAI_MODEL,
        "messages": [
            {"role": "system", "content": sys},
            {"role": "user", "content": json.dumps(user) }
        ],
        "temperature": 0.2,
        "response_format": {"type": "json_object"}
    }

def fetch_ai_picks_openai(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    import openai  # official SDK (v1)
    client = openai.OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    payload = build_llm_prompt(rows)
    resp = client.chat.completions.create(**payload)
    raw = resp.choices[0].message.content
    try:
        data = json.loads(raw)
    except Exception:
        data = {"picks": [], "notes": ["LLM output parse failure"], "_raw": raw}
    return data

def get_ai_picks_cached(league: str, today: Optional[str], rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not today:
        today = dt.date.today().isoformat()
    key = f"ai:picks:{league}:{today}:{_AI_PICK_VERSION}"
    cached = _cache_get(key)
    if cached:
        return cached
    data = fetch_ai_picks_openai(rows)
    data["cached_at"] = dt.datetime.utcnow().isoformat() + "Z"
    _cache_set(key, data, ttl=_AI_TTL)
    return data
