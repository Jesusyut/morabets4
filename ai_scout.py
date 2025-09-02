# ai_scout.py
from typing import Dict, Any, List, Tuple
import math

# MLB trends integration
from mlb_trends import trends_by_player_names

# ---------- EV utilities ----------
def american_profit(odds: int) -> float:
    return odds / 100.0 if odds >= 0 else 100.0 / abs(odds)

def ev_per_unit(p: float, odds: int) -> float:
    # EV per 1u stake (stake excluded from profit term by convention)
    return p * american_profit(odds) - (1.0 - p)

# ---------- Payload builder ----------
def build_llm_payload(rows: List[Dict[str, Any]], top_k: int = 30) -> Dict[str, Any]:
    """
    Compress raw rows (from odds_api.fetch_player_props) into a compact
    per-player set of markets with the best side (by EV) for each (stat,line).
    Expected row fields (already in your repo):
      - 'player', 'stat', 'line'
      - 'shop': {'over': {'american', 'book'}, 'under': {...}}
      - 'fair': {'prob': {'over', 'under'}}
    """
    by_player: Dict[str, Dict[str, Any]] = {}

    for r in rows or []:
        player = (r.get("player") or "").strip()
        stat   = (r.get("stat") or "").strip()
        line   = r.get("line")
        if not player or not stat:
            continue

        fair = (r.get("fair") or {}).get("prob") or {}
        p_over  = float(fair.get("over", 0.0))
        p_under = float(fair.get("under", 0.0))
        shop    = (r.get("shop") or {})
        over    = (shop.get("over")  or {})
        under   = (shop.get("under") or {})

        opts = []
        if over and isinstance(over.get("american"), (int, float)):
            opts.append({
                "market": stat, "side": "over", "line": line,
                "best_book": over.get("book",""),
                "best_odds": int(over.get("american")),
                "prob": p_over,
            })
        if under and isinstance(under.get("american"), (int, float)):
            opts.append({
                "market": stat, "side": "under", "line": line,
                "best_book": under.get("book",""),
                "best_odds": int(under.get("american")),
                "prob": p_under,
            })
        if not opts:
            continue

        # compute EV and keep the side with the best EV for this (stat,line)
        for o in opts:
            o["ev"] = ev_per_unit(o["prob"], o["best_odds"])

        key = (stat, line)
        rec = by_player.setdefault(player, {"player": player, "available_markets": {}})
        current = rec["available_markets"].get(key)
        best = max([current] + opts if current else opts, key=lambda d: d["ev"])
        rec["available_markets"][key] = best

    # Flatten & keep top_k players by their best EV
    flat = []
    for p, rec in by_player.items():
        markets = list(rec["available_markets"].values())
        if not markets:
            continue
        rec["markets"] = sorted(markets, key=lambda d: d["ev"], reverse=True)[:6]
        rec.pop("available_markets", None)
        rec["max_ev"] = max(m["ev"] for m in rec["markets"])
        flat.append(rec)

    flat = sorted(flat, key=lambda d: d["max_ev"], reverse=True)[:top_k]
    
    # Collect player names for trends
    names = [rec["player"] for rec in flat]
    name_to_trend = {}
    try:
        name_to_trend = trends_by_player_names(names)
    except Exception:
        name_to_trend = {}

    # Attach a compact trend subset per player (only what the model needs)
    for rec in flat:
        t = name_to_trend.get(rec["player"])
        if t:
            rec["trends"] = {
                "l10_hit_rate": t.get("l10_hit_rate"),
                "l10_tb_avg": t.get("l10_tb_avg"),
                "multi_hit_rate": t.get("multi_hit_rate"),
                "xbh_rate": t.get("xbh_rate"),
            }
    
    return {"players": flat}

# ---------- LLM call ----------
SYSTEM = (
  "You are a sportsbook trading assistant. "
  "Use ONLY the provided JSON. Never invent markets or players. "
  "Return compact JSON with undervalued plays, not prose."
)

USER_TEMPLATE = """\
Scan today's props and trends. For each player, pick at most two undervalued markets:
- One 'base' (higher hit rate) and optionally one 'upgrade' (higher payout).
- Recommend ONLY markets present in the input payload.
- Require positive EV at the best price (provided in payload).
- Keep rationale <= 25 words.

INPUT JSON:
{payload}

Return JSON:
{{
  "picks": [
    {{
      "player": "...",
      "market": "...",        // exactly as in payload
      "side": "over|under",
      "line": 1.5,
      "best_book": "book",
      "best_odds": 120,
      "prob": 0.54,
      "type": "base|upgrade",
      "value_score": 0-100,
      "rationale": "short reason"
    }}
  ]
}}
"""

def llm_scout(client, payload: Dict[str, Any]) -> Dict[str, Any]:
    from json import loads
    msg = [
        {"role": "system", "content": SYSTEM},
        {"role": "user", "content": USER_TEMPLATE.format(payload=payload)}
    ]
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=msg,
        temperature=0.2,
        response_format={"type": "json_object"},
        max_tokens=700,
    )
    return loads(resp.choices[0].message.content)

# ---------- Final guardrails ----------
def merge_and_gate(llm_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Final safety: keep only +EV by our math; sort by EV desc."""
    out = []
    for p in (llm_json or {}).get("picks", []):
        try:
            odds = int(p["best_odds"]); prob = float(p["prob"])
            ev = ev_per_unit(prob, odds)
            if ev > 0.0:
                p["ev"] = ev
                out.append(p)
        except Exception:
            continue
    return sorted(out, key=lambda d: d["ev"], reverse=True)
