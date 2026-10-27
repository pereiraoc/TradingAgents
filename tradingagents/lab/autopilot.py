"""Automatic paper portfolios: one round of analyses per business day, after the close.

A round re-analyzes what the portfolio holds, the user's base list and the radar's
strongest bullish names in the portfolio's currency; each decision then trades at the
next close like any other. A round only starts while the model is serving: nothing here
turns a GPU on, and API keys are never stored, so only key-less (local) models qualify.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from . import book, jobs, radar
from .store import Store

TZ = ZoneInfo("America/Sao_Paulo")
DEFAULTS = {"enabled": True, "tickers": [], "radar_top": 3, "max_positions": 8, "hour": 19,
            "analysts": [], "model": None, "last_round": None}


def weights_for(max_positions: int) -> dict:
    """Spread the cash over ``max_positions`` names and size by the rating, not by the trader's percent."""
    n = max(1, int(max_positions))
    return {"Buy": round(1 / n, 4), "Overweight": round(0.7 / n, 4), "Underweight": round(0.3 / n, 4),
            "Sell": 0.0, "ai_sizing": False}


def get(store: Store, pid: int) -> dict | None:
    row = store.one("select auto_json from portfolios where id=?", (pid,))
    raw = json.loads(row["auto_json"] or "null") if row else None
    return {**DEFAULTS, **raw} if raw else None


def save(store: Store, pid: int, auto: dict) -> dict:
    auto = {**DEFAULTS, **(get(store, pid) or {}), **auto}
    store.run("update portfolios set auto_json=?, weights_json=? where id=?",
              (json.dumps(auto), json.dumps(weights_for(auto["max_positions"])), pid))
    return auto


def last_slot(at: datetime, hour: int) -> str:
    """The latest business day whose round time has passed (local time)."""
    d = at.date() if at.hour >= hour else at.date() - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.isoformat()


def next_slot(at: datetime, hour: int) -> str:
    d = at.date() if at.hour < hour else at.date() + timedelta(days=1)
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return f"{d.isoformat()}T{hour:02d}:00"


def due(auto: dict | None, at: datetime) -> bool:
    """The first round runs right away; later ones once per business day after ``hour``."""
    if not auto or not auto["enabled"] or not auto["model"]:
        return False
    return auto["last_round"] is None or auto["last_round"] < last_slot(at, auto["hour"])


def radar_candidates(store: Store, currency: str, currency_of, top: int, skip: set[str],
                     at: datetime | None = None) -> list[str]:
    if top <= 0:
        return []
    cfg = radar.get_config(store)
    out = []
    for b in radar.ranking(store, at):
        if b["score"] < cfg["min_score"] or b["ticker"] in skip:
            continue
        if (currency_of(b["ticker"]) or "").upper() != currency:
            continue
        out.append(b["ticker"])
        if len(out) >= top:
            break
    return out


def universe(store: Store, pid: int, auto: dict, currency_of, today: str) -> list[str]:
    """Held names, then the base list, then fresh radar names not analyzed here in the radar's cooldown."""
    p = book.get(store, pid)
    names = list(dict.fromkeys([*book.holdings(store, pid), *auto["tickers"]]))
    since = (date.fromisoformat(today) - timedelta(days=radar.get_config(store)["cooldown_days"])).isoformat()
    recent = {r["ticker"] for r in store.all(
        "select ticker from jobs where portfolio_id=? and trade_date>=?", (pid, since))}
    return names + radar_candidates(store, p["currency"], currency_of, auto["radar_top"], set(names) | recent)


def run_round(store: Store, pid: int, currency_of, at: datetime) -> list[int]:
    """Queue today's analyses; a name already queued, running or analyzed today is not queued again."""
    auto = get(store, pid)
    today = at.date().isoformat()
    busy = {r["ticker"] for r in store.all(
        "select ticker from jobs where portfolio_id=? and (status in ('queued','running')"
        " or (trade_date=? and status!='failed'))", (pid, today))}
    ids = [jobs.enqueue(store, "analysis", ticker=t, trade_date=today, analysts=auto["analysts"],
                        model=auto["model"], portfolio_id=pid, payload={"from": "auto"})
           for t in universe(store, pid, auto, currency_of, today) if t not in busy]
    save(store, pid, {"last_round": last_slot(at, auto["hour"])})
    return ids


def now_local() -> datetime:
    return datetime.now(TZ).replace(tzinfo=None)
