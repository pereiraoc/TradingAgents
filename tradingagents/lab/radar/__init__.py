"""Radar: which stocks are worth an analysis right now.

Collect signals (headlines, news about public figures, disclosed trades), let the
model read the free-text ones into (ticker, direction, strength), rank tickers by
recent agreeing signals, and feed the top ones to a paper portfolio so the idea
itself gets measured.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from datetime import UTC, datetime, timedelta

from .. import jobs
from ..llm import scrub
from ..store import Store, now
from . import sources

logger = logging.getLogger(__name__)

DEFAULT_CONFIG = {
    "figures": [
        {"name": "Donald Trump", "lang": "en"}, {"name": "Elon Musk", "lang": "en"},
        {"name": "Jerome Powell", "lang": "en"}, {"name": "Lula", "lang": "pt"},
        {"name": "Fernando Haddad", "lang": "pt"}, {"name": "Gabriel Galípolo", "lang": "pt"},
    ],
    "sources": {"bloomberg": True, "figure_news": True, "congress": True, "insider": True},
    "portfolio_id": None, "top_n": 3, "min_score": 0.5, "cooldown_days": 3,
}
SOURCE_WEIGHT = {"bloomberg": 1.0, "figure_news": 1.0, "insider": 0.8, "congress": 0.6}
HALF_LIFE_DAYS = 3.0
PROCESS_LIMIT = 40
STALE_DAYS = 2

PROMPT = """You read one market news item and list the publicly traded companies it is likely to move.
Answer ONLY with JSON: {{"hits": [{{"ticker": "<Yahoo Finance symbol, e.g. TSLA or PETR4.SA>", "direction": 1 or -1, "strength": <0 to 1>, "reason": "<at most 12 words>"}}]}}
direction 1 = price likely up, -1 = likely down. Use an empty list when no specific listed company is clearly affected.
Never invent a symbol you are not sure exists.

Source: {source}{actor}
Published: {ts}
Headline: {title}
{body}"""


def utcnow() -> datetime:
    """Naive UTC: signal timestamps are stored as UTC wall time."""
    return datetime.now(UTC).replace(tzinfo=None)


def get_config(store: Store) -> dict:
    row = store.one("select json from radar_config where id=1")
    saved = json.loads(row["json"]) if row else {}
    srcs = saved.get("sources") if isinstance(saved.get("sources"), dict) else {}
    return {**DEFAULT_CONFIG, **saved, "sources": {**DEFAULT_CONFIG["sources"], **srcs}}


def set_config(store: Store, patch: dict) -> dict:
    cfg = {**get_config(store), **patch}
    store.run("insert into radar_config(id, json) values (1, ?) on conflict(id) do update set json=excluded.json",
              (json.dumps(cfg),))
    return cfg


def _insert(store: Store, sig: dict) -> bool:
    if store.one("select id from radar_signals where uid=?", (sig["uid"],)):
        return False
    hits = sig.get("hits")
    try:
        sid = store.run("insert into radar_signals(source, uid, ts, actor, title, url, body, processed, created_at)"
                        " values (?,?,?,?,?,?,?,?,?)",
                        (sig["source"], sig["uid"], sig["ts"], sig.get("actor"), sig.get("title"), sig.get("url"),
                         sig.get("body"), 1 if hits is not None else 0, now()))
    except sqlite3.IntegrityError:  # collected concurrently by the worker and a manual run
        return False
    for h in hits or []:
        store.run("insert into radar_hits(signal_id, ticker, direction, strength, reason) values (?,?,?,?,?)",
                  (sid, h["ticker"], h["direction"], h["strength"], h.get("reason")))
    return True


def collect(store: Store, fetch: sources.Fetch | None = None) -> dict:
    """Pull every enabled source; a source that fails is reported, not fatal."""
    fetch = fetch or sources.http_fetch
    cfg = get_config(store)
    plan = {
        "bloomberg": lambda: sources.bloomberg(fetch),
        "figure_news": lambda: sources.figure_news(fetch, cfg["figures"]),
        "congress": lambda: sources.congress(fetch),
        "insider": lambda: sources.insiders(fetch),
    }
    report = {}
    for name, run in plan.items():
        if not cfg["sources"].get(name):
            continue
        try:
            report[name] = sum(_insert(store, s) for s in run())
        except Exception as exc:  # offline, rate limited, format change
            logger.warning("radar source %s failed: %s", name, exc)
            report[name] = f"falhou: {exc}"[:200]
    set_config(store, {"last_collect": now(), "last_report": report})
    return report


def parse_hits(text: str) -> list[dict]:
    """The model's JSON answer, tolerant of prose or think-blocks around it.

    Raises ValueError when there is no readable JSON object, so the caller can tell
    "nothing affected" (empty list) from "garbled answer" (try again later).
    """
    text = re.sub(r"<think>.*?</think>", "", text or "", flags=re.S)
    decoder, data, pos = json.JSONDecoder(), None, text.find("{")
    while pos >= 0:
        try:
            data = decoder.raw_decode(text, pos)[0]
        except ValueError:
            pos = text.find("{", pos + 1)
            continue
        if isinstance(data, dict) and "hits" in data:
            break
        data, pos = None, text.find("{", pos + 1)
    if data is None:
        raise ValueError("resposta sem JSON legível")
    out = []
    for h in data.get("hits", []) if isinstance(data, dict) else []:
        try:
            ticker = str(h["ticker"]).strip().upper()
            direction = 1 if int(h["direction"]) > 0 else -1
            strength = max(0.0, min(1.0, float(h.get("strength", 0.5))))
        except (KeyError, TypeError, ValueError):
            continue
        if re.fullmatch(r"[A-Z0-9.\-^=]{1,15}", ticker):
            out.append({"ticker": ticker, "direction": direction, "strength": strength,
                        "reason": str(h.get("reason") or "")[:120]})
    return out


def process(store: Store, ask, symbol_ok, limit: int = PROCESS_LIMIT) -> int:
    """Have the model read unprocessed free-text signals, newest first. Returns hits stored."""
    stored = 0
    # past the ranking window a signal no longer matters: don't let it pile up as "pending"
    stale = (utcnow() - timedelta(days=STALE_DAYS)).strftime("%Y-%m-%dT%H:%M:%S")
    store.run("update radar_signals set processed=1 where processed=0 and ts<?", (stale,))
    rows = store.all("select * from radar_signals where processed=0 order by ts desc limit ?", (limit,))
    for s in rows:
        prompt = PROMPT.format(source=s["source"], actor=f" — about {s['actor']}" if s["actor"] else "",
                               ts=s["ts"], title=s["title"], body=(s["body"] or "")[:1200])
        try:
            hits = [h for h in parse_hits(ask(prompt)) if symbol_ok(h["ticker"])]
        except Exception as exc:  # model hiccup or garbled answer: leave it for the next run
            logger.warning("radar: model failed on signal %s: %s", s["id"], scrub(str(exc), None))
            continue
        for h in hits:
            store.run("insert into radar_hits(signal_id, ticker, direction, strength, reason) values (?,?,?,?,?)",
                      (s["id"], h["ticker"], h["direction"], h["strength"], h["reason"]))
        store.run("update radar_signals set processed=1 where id=?", (s["id"],))
        stored += len(hits)
    return stored


def ranking(store: Store, at: datetime | None = None, days: int = 7, limit: int = 25) -> list[dict]:
    at = at or utcnow()
    since = (at - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S")
    rows = store.all(
        "select h.ticker, h.direction, h.strength, h.reason, s.id sid, s.source, s.actor, s.title, s.url, s.ts"
        " from radar_hits h join radar_signals s on s.id=h.signal_id where s.ts>=? order by s.ts desc", (since,))
    board: dict[str, dict] = {}
    for r in rows:
        try:
            age = max(0.0, (at - datetime.fromisoformat(r["ts"][:19])).total_seconds() / 86400)
        except ValueError:
            age = days
        weight = SOURCE_WEIGHT.get(r["source"], 0.5) * 0.5 ** (age / HALF_LIFE_DAYS)
        b = board.setdefault(r["ticker"], {"ticker": r["ticker"], "score": 0.0, "n": 0, "by_source": {},
                                           "last_ts": r["ts"], "signals": []})
        b["score"] += r["direction"] * r["strength"] * weight
        b["n"] += 1
        b["by_source"][r["source"]] = b["by_source"].get(r["source"], 0.0) + r["direction"] * r["strength"] * weight
        if len(b["signals"]) < 6:
            b["signals"].append({k: r[k] for k in ("sid", "source", "actor", "title", "url", "ts", "direction", "reason")})
    out = []
    for b in board.values():
        # agreement across independent sources counts more than many items from one feed;
        # a source pointing the other way is not agreement
        net = b.pop("by_source")
        agree = sum(1 for v in net.values() if v * b["score"] > 0)
        b["score"] = round(b["score"] * (1 + 0.25 * max(0, agree - 1)), 3)
        b["sources"] = sorted(net)
        out.append(b)
    return sorted(out, key=lambda b: abs(b["score"]), reverse=True)[:limit]


def pick(store: Store, currency_of, at: datetime | None = None) -> list[str]:
    """Tickers the radar portfolio should analyze now: bullish, strong, in its currency, not seen lately."""
    cfg = get_config(store)
    pid = cfg.get("portfolio_id")
    if not pid:
        return []
    p = store.one("select currency from portfolios where id=?", (pid,))
    if not p:
        return []
    at = at or utcnow()
    since = (at - timedelta(days=cfg["cooldown_days"])).strftime("%Y-%m-%d")
    recent = {r["ticker"] for r in store.all(
        "select ticker from jobs where portfolio_id=? and trade_date>=?", (pid, since))}
    chosen = []
    for b in ranking(store, at):
        if b["score"] < cfg["min_score"] or b["ticker"] in recent:
            continue
        if (currency_of(b["ticker"]) or "").upper() != p["currency"]:
            continue
        chosen.append(b["ticker"])
        if len(chosen) >= cfg["top_n"]:
            break
    return chosen


def enqueue_picks(store: Store, tickers: list[str], model: dict, analysts: list[str], day: str) -> list[int]:
    pid = get_config(store)["portfolio_id"]
    return [jobs.enqueue(store, "analysis", ticker=t, trade_date=day, analysts=analysts, model=model,
                         portfolio_id=pid, payload={"from": "radar"}) for t in tickers]
