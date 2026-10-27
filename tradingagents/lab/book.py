"""Paper portfolios: decisions become trades filled at the next close."""

from __future__ import annotations

import json

from tradingagents.portfolio import PortfolioContext, Position

from . import sizing
from .fills import PriceSource, _shift, close_on_or_before, next_close
from .store import Store, now

BENCHMARKS = {"BRL": "^BVSP", "USD": "SPY"}


def create(store: Store, name: str, currency: str, initial_cash: float, benchmark: str | None = None,
           mode: str = "live", replay: dict | None = None, weights: dict | None = None) -> int:
    currency = currency.upper()
    return store.run(
        "insert into portfolios(name, currency, initial_cash, benchmark, mode, replay_json, weights_json, created_at)"
        " values (?,?,?,?,?,?,?,?)",
        (name, currency, float(initial_cash), benchmark or BENCHMARKS.get(currency, "SPY"), mode,
         json.dumps(replay), json.dumps(weights or {}), now()))


def _decode(row: dict | None) -> dict | None:
    if row:
        row["replay"] = json.loads(row.pop("replay_json") or "null")
        row["weights"] = json.loads(row.pop("weights_json") or "{}")
    return row


def get(store: Store, pid: int) -> dict | None:
    return _decode(store.one("select * from portfolios where id=?", (pid,)))


def listing(store: Store) -> list[dict]:
    return [_decode(r) for r in store.all("select * from portfolios order by id")]


def _filled(store: Store, pid: int, until: str | None = None) -> list[dict]:
    sql, args = "select * from trades where portfolio_id=? and status='filled'", [pid]
    if until:
        sql, args = sql + " and fill_date<=?", args + [until]
    return store.all(sql + " order by fill_date, id", args)


def holdings(store: Store, pid: int, until: str | None = None) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for t in _filled(store, pid, until):
        h = out.setdefault(t["ticker"], {"quantity": 0.0, "avg_price": 0.0})
        if t["side"] == "buy":
            cost = h["quantity"] * h["avg_price"] + t["quantity"] * t["price"]
            h["quantity"] += t["quantity"]
            h["avg_price"] = cost / h["quantity"]
        else:
            h["quantity"] -= t["quantity"]
    return {k: v for k, v in out.items() if v["quantity"] > 0}


def cash(store: Store, pid: int, until: str | None = None) -> float:
    value = get(store, pid)["initial_cash"]
    for t in _filled(store, pid, until):
        value += t["quantity"] * t["price"] * (-1 if t["side"] == "buy" else 1)
    return value


def context(store: Store, pid: int) -> PortfolioContext:
    p = get(store, pid)
    return PortfolioContext(
        cash=cash(store, pid), currency=p["currency"],
        positions=[Position(ticker=k, quantity=v["quantity"], average_price=v["avg_price"])
                   for k, v in holdings(store, pid).items()])


def record_decision(store: Store, pid: int, job_id: int, ticker: str, trade_date: str,
                    rating: str, trader_plan: str | None) -> int | None:
    if rating == "REVIEW":
        return None
    status, reason = ("skipped", "hold") if rating == "Hold" else ("pending", None)
    return store.run(
        "insert into trades(portfolio_id, job_id, ticker, trade_date, rating, trader_plan, status, reason)"
        " values (?,?,?,?,?,?,?,?)", (pid, job_id, ticker, trade_date, rating, trader_plan, status, reason))


def _value_on(store: Store, pid: int, prices: PriceSource, day: str) -> float:
    total = cash(store, pid, day)
    for ticker, h in holdings(store, pid, day).items():
        total += h["quantity"] * (close_on_or_before(prices, ticker, day) or h["avg_price"])
    return total


def fill_pending(store: Store, prices: PriceSource, pid: int | None = None) -> int:
    sql, args = "select * from trades where status='pending'", []
    if pid is not None:
        sql, args = sql + " and portfolio_id=?", [pid]
    filled = 0
    for t in store.all(sql + " order by trade_date, id", args):
        nc = next_close(prices, t["ticker"], t["trade_date"])
        if nc is None:
            continue
        fill_date, price = nc
        p = get(store, t["portfolio_id"])
        target = sizing.decide_target(t["rating"], t["trader_plan"], p["weights"])
        held = holdings(store, p["id"]).get(t["ticker"], {}).get("quantity", 0)
        delta = sizing.order_delta(target, held, cash(store, p["id"]),
                                   _value_on(store, p["id"], prices, fill_date), price,
                                   t["rating"] in sizing.BULLISH)
        if delta == 0:
            store.run("update trades set status='skipped', reason=?, sizing_source=?, fill_date=?, price=? where id=?",
                      ("sem mudança (alvo atingido ou sem caixa)", target.source, fill_date, price, t["id"]))
            continue
        store.run("update trades set status='filled', side=?, quantity=?, price=?, fill_date=?, sizing_source=?"
                  " where id=?", ("buy" if delta > 0 else "sell", abs(delta), price, fill_date, target.source, t["id"]))
        filled += 1
    return filled


def snapshot(store: Store, pid: int, prices: PriceSource, today: str) -> dict:
    p = get(store, pid)
    first = store.one("select min(trade_date) d from trades where portfolio_id=?", (pid,))["d"]
    start = min(filter(None, (p["created_at"][:10], (p["replay"] or {}).get("start"), first)))
    bench = prices.closes(p["benchmark"], start, _shift(today, 1))
    curve = [{"date": d, "value": round(_value_on(store, pid, prices, d), 2)} for d, _ in bench]
    base = bench[0][1] if bench else None
    positions = []
    for ticker, h in holdings(store, pid).items():
        last = close_on_or_before(prices, ticker, today) or h["avg_price"]
        positions.append({"ticker": ticker, **h, "last": last, "value": h["quantity"] * last})
    return {
        "portfolio": p, "cash": cash(store, pid), "positions": positions,
        "value": cash(store, pid) + sum(x["value"] for x in positions),
        "curve": curve,
        "benchmark_curve": [{"date": d, "value": p["initial_cash"] * c / base} for d, c in bench] if base else [],
        "trades": store.all("select * from trades where portfolio_id=? order by trade_date desc, id desc", (pid,)),
    }
