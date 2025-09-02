# mlb_trends.py
import time
import requests
from typing import Dict, Any, Optional, List, Tuple
from universal_cache import current_slot, get_cached, set_cached

BASE = "https://statsapi.mlb.com/api/v1"

def _cache_key_player_id(name: str) -> str:
    return f"mlb:pid:{name.strip().lower()}"

def _cache_key_trends(pid: int) -> str:
    # cache trends to the current Phoenix slot
    slot, _ = current_slot()
    return f"mlb:trends:{slot}:{pid}"

def _http_json(url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

def lookup_player_id(name: str, team_hint: Optional[str] = None) -> Optional[int]:
    """
    Resolve MLBAM personId from a player name (optionally bias with team name).
    Cached by name for the current process lifetime via universal_cache.
    """
    name_key = _cache_key_player_id(name)
    cached = get_cached(name_key)
    if cached is not None:
        return cached

    # Search by text; MLB Stats API will fuzzy-match
    data = _http_json(f"{BASE}/people", params={"search": name})
    candidates = data.get("people", []) or []

    if not candidates:
        set_cached(name_key, None)
        return None

    # If team_hint provided, try to pick the candidate with that team in lastPlayedTeam
    if team_hint:
        team_hint_l = team_hint.strip().lower()
        for c in candidates:
            last_team = (c.get("lastPlayedTeam", {}) or {}).get("name", "") or c.get("fullFMLName", "")
            if team_hint_l in (last_team or "").lower():
                set_cached(name_key, c.get("id"))
                return c.get("id")

    # Fallback: first candidate
    pid = candidates[0].get("id")
    set_cached(name_key, pid)
    return pid

def _get_batting_gamelogs(pid: int, season: Optional[int] = None) -> List[Dict[str, Any]]:
    """Return recent hitting game logs for the given player id."""
    from datetime import datetime
    year = season or datetime.now().year
    # gameLog stats group=hitting
    data = _http_json(
        f"{BASE}/people/{pid}/stats",
        params={"stats": "gameLog", "group": "hitting", "season": year},
    )
    splits = (((data.get("stats") or [{}])[0]).get("splits") or [])
    return splits  # each split has 'stat' and 'date' etc.

def _tb_from_stat(stat: Dict[str, Any]) -> int:
    """
    Compute Total Bases if not provided:
    1B + 2*2B + 3*3B + 4*HR; where 1B = H - 2B - 3B - HR
    """
    h  = int(stat.get("hits", 0))
    d2 = int(stat.get("doubles", 0))
    d3 = int(stat.get("triples", 0))
    hr = int(stat.get("homeRuns", 0))
    one_b = max(h - d2 - d3 - hr, 0)
    return one_b + 2*d2 + 3*d3 + 4*hr

def last10_trends_for(pid: int) -> Optional[Dict[str, Any]]:
    """
    Compute last-10 trends for a batter:
      - l10_hit_rate:   % of games with >=1 hit
      - l10_tb_avg:     average total bases
      - multi_hit_rate: % of games with >=2 hits
      - xbh_rate:       % of games with >=1 extra-base hit (2B/3B/HR)
      - games:          number of games considered
    Cached until next slot boundary.
    """
    ck = _cache_key_trends(pid)
    cached = get_cached(ck)
    if cached is not None:
        return cached

    logs = _get_batting_gamelogs(pid)
    if not logs:
        set_cached(ck, None)
        return None

    # Most responses are chronological; sort by date desc just in case
    try:
        logs.sort(key=lambda s: s.get("date",""), reverse=True)
    except Exception:
        pass
    last10 = logs[:10]

    if not last10:
        set_cached(ck, None)
        return None

    hit_g = 0; multi_g = 0; xbh_g = 0; tb_sum = 0
    for g in last10:
        st = g.get("stat") or {}
        hits = int(st.get("hits", 0))
        d2 = int(st.get("doubles", 0))
        d3 = int(st.get("triples", 0))
        hr = int(st.get("homeRuns", 0))

        if hits >= 1: hit_g += 1
        if hits >= 2: multi_g += 1
        if (d2 + d3 + hr) >= 1: xbh_g += 1

        tb = int(st.get("totalBases", _tb_from_stat(st)))
        tb_sum += tb

    n = len(last10)
    trends = {
        "games": n,
        "l10_hit_rate": round(hit_g / n, 3),
        "l10_tb_avg": round(tb_sum / n, 3),
        "multi_hit_rate": round(multi_g / n, 3),
        "xbh_rate": round(xbh_g / n, 3),
    }
    set_cached(ck, trends)
    return trends

def trends_by_player_names(names: List[str], team_lookup: Optional[Dict[str, str]] = None) -> Dict[str, Dict[str, Any]]:
    """
    Bulk helper: map player name -> trends dict (best-effort).
    team_lookup optional mapping: name -> team name (improves ID resolution).
    """
    out: Dict[str, Dict[str, Any]] = {}
    for name in names:
        pid = lookup_player_id(name, (team_lookup or {}).get(name))
        if not pid: 
            continue
        t = last10_trends_for(pid)
        if t: out[name] = t
    return out
