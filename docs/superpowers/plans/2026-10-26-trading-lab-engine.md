# Trading Lab — Motor (fork) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Pacote `tradingagents/lab/` com fila, carteiras fictícias, reencenação, backtest e API HTTP, servido por `tradingagents lab serve`.

**Architecture:** SQLite único (`lab.sqlite`) compartilhado por uma API FastAPI e uma thread trabalhadora que roda um job por vez. O grafo do TradingAgents é chamado pelo caminho de *stream* (eventos por agente) e é injetável para testes; preços vêm de um `PriceSource` injetável (Yahoo em produção).

**Tech Stack:** Python ≥3.11, sqlite3, FastAPI + uvicorn (extra `[lab]`), yfinance, pytest.

**Spec:** `docs/superpowers/specs/2026-10-26-trading-lab-design.md`

## Global Constraints

- Nenhuma ordem real: só tabelas locais.
- Chave de API nunca em SQLite, log, evento ou argv — só no `KeyVault` em memória e, durante o job, numa env var restaurada no `finally`.
- Uma análise por vez (thread única).
- Preço de execução = fechamento do primeiro pregão **após** `trade_date`.
- Sem venda a descoberto; quantidade inteira; sem alavancagem.
- Pesos padrão: Buy 0.10, Overweight 0.07, Underweight 0.03, Sell 0.0; Hold não opera; REVIEW não opera.
- `ruff check .` limpo (CI do fork é estrito) e `pytest -q` verde.
- Dados em `TRADINGAGENTS_LAB_DIR` (padrão `~/.tradingagents`).

---

### Task 1: Store + fila de jobs

**Files:**
- Create: `tradingagents/lab/__init__.py`, `tradingagents/lab/store.py`, `tradingagents/lab/jobs.py`
- Test: `tests/lab/__init__.py`, `tests/lab/test_jobs.py`

**Interfaces:**
- Produces: `Store(path)` com `.run(sql, args) -> int`, `.all(sql, args) -> list[dict]`, `.one(sql, args) -> dict | None`; `now() -> str`.
- Produces (`jobs`): `enqueue(store, kind, *, ticker=None, trade_date=None, analysts=(), model=None, portfolio_id=None, after_id=None, payload=None) -> int`; `get(store, id) -> dict | None`; `listing(store, status=None, limit=100) -> list[dict]`; `next_runnable(store) -> dict | None`; `mark(store, id, status, **fields)`; `add_event(store, id, agent, phase, note="")`; `events(store, id) -> list[dict]`; `recover(store, has_key: Callable[[int], bool])`.

- [ ] **Step 1: Write the failing test** (`tests/lab/test_jobs.py`)

```python
from tradingagents.lab import jobs
from tradingagents.lab.store import Store

MODEL = {"provider": "openai_compatible", "base_url": "http://x/v1", "quick": "q", "deep": "q", "needs_key": False}


def make(tmp_path):
    return Store(tmp_path / "lab.sqlite")


def test_fifo_and_chain(tmp_path):
    s = make(tmp_path)
    a = jobs.enqueue(s, "analysis", ticker="NVDA", trade_date="2026-06-01", model=MODEL)
    b = jobs.enqueue(s, "analysis", ticker="AAPL", trade_date="2026-06-01", model=MODEL, after_id=a)
    assert jobs.next_runnable(s)["id"] == a
    jobs.mark(s, a, "running")
    assert jobs.next_runnable(s) is None          # b waits for a
    jobs.mark(s, a, "done")
    assert jobs.next_runnable(s)["id"] == b
    assert jobs.get(s, b)["model"] == MODEL


def test_failed_predecessor_blocks_chain(tmp_path):
    s = make(tmp_path)
    a = jobs.enqueue(s, "analysis", ticker="NVDA", trade_date="2026-06-01", model=MODEL)
    jobs.enqueue(s, "analysis", ticker="NVDA", trade_date="2026-06-08", model=MODEL, after_id=a)
    jobs.mark(s, a, "failed", error="boom")
    assert jobs.next_runnable(s) is None


def test_recover(tmp_path):
    s = make(tmp_path)
    keyless = jobs.enqueue(s, "analysis", ticker="A", trade_date="2026-06-01", model=MODEL)
    keyed = jobs.enqueue(s, "analysis", ticker="B", trade_date="2026-06-01", model={**MODEL, "needs_key": True})
    jobs.mark(s, keyless, "running")
    jobs.recover(s, has_key=lambda _id: False)
    assert jobs.get(s, keyless)["status"] == "queued"
    got = jobs.get(s, keyed)
    assert got["status"] == "failed" and "reenvie" in got["error"]


def test_events(tmp_path):
    s = make(tmp_path)
    a = jobs.enqueue(s, "analysis", ticker="A", trade_date="2026-06-01", model=MODEL)
    jobs.add_event(s, a, "Market Analyst", "end")
    assert [e["agent"] for e in jobs.events(s, a)] == ["Market Analyst"]
```

- [ ] **Step 2: Run** `.venv/bin/pytest tests/lab/test_jobs.py -q` → FAIL (`ModuleNotFoundError: tradingagents.lab`).

- [ ] **Step 3: Implement**

`tradingagents/lab/__init__.py`:
```python
"""Trading Lab: paper portfolios, a run queue and an HTTP API over the framework."""
```

`tradingagents/lab/store.py`:
```python
"""One SQLite file shared by the lab's API and its worker thread."""

from __future__ import annotations

import sqlite3
import threading
from datetime import UTC, datetime
from pathlib import Path

SCHEMA = """
create table if not exists jobs(
  id integer primary key, kind text not null, status text not null default 'queued',
  ticker text, trade_date text, analysts text, model_json text, portfolio_id integer,
  after_id integer, payload_json text, error text, rating text, report_dir text,
  created_at text not null, started_at text, finished_at text);
create table if not exists job_events(
  job_id integer not null, ts text not null, agent text, phase text, note text);
create table if not exists portfolios(
  id integer primary key, name text not null, currency text not null, initial_cash real not null,
  benchmark text not null, mode text not null default 'live', replay_json text, weights_json text,
  created_at text not null);
create table if not exists trades(
  id integer primary key, portfolio_id integer not null, job_id integer, ticker text not null,
  trade_date text not null, rating text not null, trader_plan text, side text, quantity real,
  price real, fill_date text, sizing_source text, status text not null default 'pending', reason text);
create table if not exists watchlists(
  id integer primary key, name text not null unique, tickers_json text not null);
create table if not exists backtests(
  id integer primary key, job_id integer, tickers_json text not null, start text not null,
  end text not null, every_days integer not null, summary_json text);
"""


def now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


class Store:
    def __init__(self, path: str | Path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._con = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        self._con.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        with self._lock:
            self._con.execute("pragma journal_mode=wal")
            self._con.executescript(SCHEMA)

    def run(self, sql: str, args=()) -> int:
        with self._lock:
            return self._con.execute(sql, args).lastrowid

    def all(self, sql: str, args=()) -> list[dict]:
        with self._lock:
            return [dict(r) for r in self._con.execute(sql, args).fetchall()]

    def one(self, sql: str, args=()) -> dict | None:
        rows = self.all(sql, args)
        return rows[0] if rows else None
```

`tradingagents/lab/jobs.py`:
```python
"""The run queue: one job at a time, in order, a chained job after its predecessor."""

from __future__ import annotations

import json
from collections.abc import Callable

from .store import Store, now

KEY_LOST = "chave perdida no reinício — reenvie"


def _decode(row: dict | None) -> dict | None:
    if row is None:
        return None
    row["model"] = json.loads(row.pop("model_json") or "null")
    row["payload"] = json.loads(row.pop("payload_json") or "null")
    row["analysts"] = json.loads(row["analysts"] or "[]")
    return row


def enqueue(store: Store, kind: str, *, ticker=None, trade_date=None, analysts=(), model=None,
            portfolio_id=None, after_id=None, payload=None) -> int:
    return store.run(
        "insert into jobs(kind, ticker, trade_date, analysts, model_json, portfolio_id, after_id,"
        " payload_json, created_at) values (?,?,?,?,?,?,?,?,?)",
        (kind, ticker, trade_date, json.dumps(list(analysts)), json.dumps(model),
         portfolio_id, after_id, json.dumps(payload), now()))


def get(store: Store, job_id: int) -> dict | None:
    return _decode(store.one("select * from jobs where id=?", (job_id,)))


def listing(store: Store, status: str | None = None, limit: int = 100) -> list[dict]:
    if status:
        rows = store.all("select * from jobs where status=? order by id desc limit ?", (status, limit))
    else:
        rows = store.all("select * from jobs order by id desc limit ?", (limit,))
    return [_decode(r) for r in rows]


def next_runnable(store: Store) -> dict | None:
    if store.one("select id from jobs where status='running'"):
        return None
    return _decode(store.one(
        "select * from jobs j where status='queued' and (after_id is null or exists("
        " select 1 from jobs a where a.id=j.after_id and a.status in ('done','review')))"
        " order by id limit 1"))


def mark(store: Store, job_id: int, status: str, **fields) -> None:
    if status == "running":
        fields.setdefault("started_at", now())
    if status in ("done", "failed", "review"):
        fields.setdefault("finished_at", now())
    if "payload" in fields:
        fields["payload_json"] = json.dumps(fields.pop("payload"))
    cols = ", ".join(f"{k}=?" for k in ("status", *fields))
    store.run(f"update jobs set {cols} where id=?", (status, *fields.values(), job_id))


def add_event(store: Store, job_id: int, agent: str, phase: str, note: str = "") -> None:
    store.run("insert into job_events values (?,?,?,?,?)", (job_id, now(), agent, phase, note))


def events(store: Store, job_id: int) -> list[dict]:
    return store.all("select ts, agent, phase, note from job_events where job_id=? order by rowid", (job_id,))


def recover(store: Store, has_key: Callable[[int], bool]) -> None:
    """After a restart: running jobs go back to the queue; keyed jobs whose key died fail."""
    for job in (_decode(r) for r in store.all("select * from jobs where status in ('queued','running')")):
        if (job["model"] or {}).get("needs_key") and not has_key(job["id"]):
            mark(store, job["id"], "failed", error=KEY_LOST)
        elif job["status"] == "running":
            store.run("update jobs set status='queued', started_at=null where id=?", (job["id"],))
```

- [ ] **Step 4: Run** `.venv/bin/pytest tests/lab/test_jobs.py -q && .venv/bin/ruff check tradingagents/lab tests/lab` → PASS.
- [ ] **Step 5: Commit** `git add tradingagents/lab tests/lab && git commit -m "feat(lab): store SQLite e fila de jobs"`

---

### Task 2: Regra de tamanho (`sizing.py`)

**Files:** Create `tradingagents/lab/sizing.py`; Test `tests/lab/test_sizing.py`

**Interfaces:**
- Produces: `DEFAULT_WEIGHTS: dict[str, float]`; `Target(kind, value=0.0, source="")` (kind ∈ `none|review|weight|quantity|amount|fraction`); `trader_sizing(plan) -> str | None`; `trader_action(plan) -> str | None`; `decide_target(rating, trader_plan, weights=None) -> Target`; `order_delta(target, held, cash, total_value, price, bullish) -> int`.

- [ ] **Step 1: Write the failing test**

```python
import pytest

from tradingagents.lab.sizing import Target, decide_target, order_delta, trader_sizing


def plan(action="Buy", sizing="not provided"):
    return (f"**Action**: {action}\n\n**Reasoning**: r\n\n**Entry Price**: not provided\n\n"
            f"**Stop Loss**: not provided\n\n**Position Sizing**: {sizing}\n\n"
            f"FINAL TRANSACTION PROPOSAL: **{action.upper()}**")


def test_reads_sizing_line():
    assert trader_sizing(plan(sizing="5% of portfolio")) == "5% of portfolio"
    assert trader_sizing(plan()) is None
    assert trader_sizing(None) is None


@pytest.mark.parametrize("rating,p,expected", [
    ("Buy", plan(sizing="5% of portfolio"), Target("weight", 0.05, "ai_percent")),
    ("Buy", plan(sizing="200 shares"), Target("quantity", 200, "ai_quantity")),
    ("Overweight", plan(sizing="$10,000"), Target("amount", 10000, "ai_amount")),
    ("Buy", plan(sizing="small starter position"), Target("weight", 0.10, "default_weight")),
    ("Buy", plan(), Target("weight", 0.10, "default_weight")),
    ("Underweight", plan("Sell", "trim 50% of the position"), Target("fraction", 0.5, "ai_percent")),
    ("Sell", plan("Sell"), Target("weight", 0.0, "default_weight")),
    ("Hold", plan("Hold"), Target("none", 0.0, "hold")),
    ("REVIEW", plan(), Target("review", 0.0, "review")),
    # trader disagrees with the PM: trader sizing ignored, PM's default weight
    ("Underweight", plan("Buy", "5% of portfolio"), Target("weight", 0.03, "default_weight")),
])
def test_decide_target(rating, p, expected):
    assert decide_target(rating, p) == expected


def test_custom_weights():
    assert decide_target("Buy", plan(), {"Buy": 0.2}).value == 0.2


def test_order_delta_weight_buy_limited_by_cash():
    t = Target("weight", 0.5, "ai_percent")
    assert order_delta(t, held=0, cash=1000, total_value=10000, price=100, bullish=True) == 10


def test_bullish_never_reduces():
    t = Target("weight", 0.05, "ai_percent")
    assert order_delta(t, held=100, cash=0, total_value=10000, price=10, bullish=True) == 0


def test_quantity_is_order_size():
    assert order_delta(Target("quantity", 20), held=5, cash=1e6, total_value=1e6, price=10, bullish=True) == 20
    assert order_delta(Target("quantity", 20), held=5, cash=0, total_value=50, price=10, bullish=False) == -5


def test_fraction_and_full_exit():
    assert order_delta(Target("fraction", 0.5), held=7, cash=0, total_value=70, price=10, bullish=False) == -3
    assert order_delta(Target("weight", 0.0), held=7, cash=0, total_value=70, price=10, bullish=False) == -7


def test_bearish_never_increases():
    assert order_delta(Target("weight", 0.03), held=0, cash=1000, total_value=1000, price=10, bullish=False) == 0
```

- [ ] **Step 2: Run** `.venv/bin/pytest tests/lab/test_sizing.py -q` → FAIL (import).

- [ ] **Step 3: Implement** `tradingagents/lab/sizing.py`

```python
"""Turn a decision into an order size for a paper portfolio.

Direction comes from the Portfolio Manager's rating; magnitude from the trader's
``Position Sizing`` line when the trader agrees with that direction, else from a
default weight per rating. Bullish ratings never shrink a position, bearish ones
never grow it, and a position never goes short.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass

DEFAULT_WEIGHTS = {"Buy": 0.10, "Overweight": 0.07, "Underweight": 0.03, "Sell": 0.0}
BULLISH = {"Buy", "Overweight"}
BEARISH = {"Underweight", "Sell"}

_FIELD = re.compile(r"\*\*Position Sizing\*\*:\s*(.+)")
_ACTION = re.compile(r"\*\*Action\*\*:\s*(\w+)")
_PCT = re.compile(r"(\d+(?:\.\d+)?)\s*%")
_QTY = re.compile(r"(\d[\d,]*(?:\.\d+)?)\s*(?:shares|units|ações|acoes)\b", re.I)
_AMT = re.compile(r"(?:R\$|US\$|\$|€|£|USD|BRL)\s*(\d[\d,]*(?:\.\d+)?)", re.I)


@dataclass(frozen=True)
class Target:
    kind: str           # none | review | weight | quantity | amount | fraction
    value: float = 0.0
    source: str = ""


def _number(text: str) -> float:
    return float(text.replace(",", ""))


def trader_sizing(plan: str | None) -> str | None:
    m = _FIELD.search(plan or "")
    text = m.group(1).strip() if m else ""
    return None if not text or text.lower() == "not provided" else text


def trader_action(plan: str | None) -> str | None:
    m = _ACTION.search(plan or "")
    return m.group(1).capitalize() if m else None


def _parse(text: str, bearish: bool) -> Target | None:
    low = text.lower()
    if m := _PCT.search(text):
        pct = _number(m.group(1)) / 100
        if bearish and "position" in low and "portfolio" not in low:
            return Target("fraction", pct, "ai_percent")
        return Target("weight", pct, "ai_percent")
    if m := _QTY.search(text):
        return Target("quantity", _number(m.group(1)), "ai_quantity")
    if m := _AMT.search(text):
        return Target("amount", _number(m.group(1)), "ai_amount")
    return None


def decide_target(rating: str, trader_plan: str | None, weights: dict | None = None) -> Target:
    if rating not in BULLISH | BEARISH | {"Hold"}:
        return Target("review", 0.0, "review")
    if rating == "Hold":
        return Target("none", 0.0, "hold")
    weights = {**DEFAULT_WEIGHTS, **(weights or {})}
    action = trader_action(trader_plan)
    agrees = action is None or (rating in BULLISH and action == "Buy") or (rating in BEARISH and action == "Sell")
    sizing = trader_sizing(trader_plan) if agrees else None
    return (sizing and _parse(sizing, rating in BEARISH)) or Target("weight", weights[rating], "default_weight")


def order_delta(target: Target, held: float, cash: float, total_value: float, price: float, bullish: bool) -> int:
    """Signed whole units to trade: positive buys, negative sells."""
    held = int(held)
    if target.kind == "weight":
        desired = math.floor(total_value * target.value / price)
    elif target.kind == "quantity":
        desired = held + int(target.value) * (1 if bullish else -1)
    elif target.kind == "amount":
        desired = held + math.floor(target.value / price) * (1 if bullish else -1)
    elif target.kind == "fraction":
        desired = held - math.floor(held * target.value)
    else:
        return 0
    if bullish:
        buy = max(desired - held, 0)
        return min(buy, math.floor(cash / price))
    return min(max(desired, 0), held) - held
```

- [ ] **Step 4: Run** `.venv/bin/pytest tests/lab/test_sizing.py -q && .venv/bin/ruff check tradingagents/lab tests/lab` → PASS.
- [ ] **Step 5: Commit** `git commit -am "feat(lab): regra de tamanho (direcao do PM, tamanho do trader, pesos padrao)"` (add new files first).

---

### Task 3: Preços e carteira (`fills.py`, `book.py`)

**Files:** Create `tradingagents/lab/fills.py`, `tradingagents/lab/book.py`; Test `tests/lab/fakes.py`, `tests/lab/test_book.py`

**Interfaces:**
- Consumes: `Store`, `sizing.decide_target/order_delta/BULLISH`.
- Produces (`fills`): `PriceSource` protocol (`closes(ticker, start, end) -> list[tuple[str, float]]`, `currency(ticker) -> str | None`); `YahooPrices()`; `next_close(prices, ticker, day) -> tuple[str, float] | None`; `close_on_or_before(prices, ticker, day) -> float | None`.
- Produces (`book`): `BENCHMARKS = {"BRL": "^BVSP", "USD": "SPY"}`; `create(store, name, currency, initial_cash, benchmark=None, mode="live", replay=None, weights=None) -> int`; `get(store, pid) -> dict | None`; `listing(store) -> list[dict]`; `holdings(store, pid) -> dict[str, dict]` (`{"quantity", "avg_price"}`); `cash(store, pid) -> float`; `context(store, pid) -> PortfolioContext`; `record_decision(store, pid, job_id, ticker, trade_date, rating, trader_plan) -> int | None`; `fill_pending(store, prices, pid=None) -> int`; `snapshot(store, pid, prices, today) -> dict`.

- [ ] **Step 1: Write fakes and the failing test**

`tests/lab/fakes.py`:
```python
class FakePrices:
    """closes: {ticker: {"YYYY-MM-DD": price}}"""

    def __init__(self, closes, currencies=None):
        self.data, self.currencies = closes, currencies or {}

    def closes(self, ticker, start, end):
        return sorted((d, p) for d, p in self.data.get(ticker, {}).items() if start <= d < end)

    def currency(self, ticker):
        return self.currencies.get(ticker)
```

`tests/lab/test_book.py`:
```python
from tests.lab.fakes import FakePrices
from tradingagents.lab import book
from tradingagents.lab.fills import next_close
from tradingagents.lab.store import Store

BUY5 = "**Action**: Buy\n\n**Position Sizing**: 50% of portfolio"
PRICES = FakePrices({
    "NVDA": {"2026-06-01": 100.0, "2026-06-02": 110.0, "2026-06-03": 120.0},
    "SPY": {"2026-06-01": 500.0, "2026-06-02": 505.0, "2026-06-03": 510.0},
})


def test_next_close_skips_trade_date():
    assert next_close(PRICES, "NVDA", "2026-06-01") == ("2026-06-02", 110.0)
    assert next_close(PRICES, "NVDA", "2026-06-03") is None


def test_buy_fills_next_close_and_values(tmp_path):
    s = Store(tmp_path / "l.sqlite")
    pid = book.create(s, "t", "USD", 10000)
    assert book.get(s, pid)["benchmark"] == "SPY"
    book.record_decision(s, pid, 1, "NVDA", "2026-06-01", "Buy", BUY5)
    assert book.fill_pending(s, PRICES, pid) == 1
    h = book.holdings(s, pid)
    assert h["NVDA"]["quantity"] == 45 and h["NVDA"]["avg_price"] == 110.0   # floor(5000/110)
    assert book.cash(s, pid) == 10000 - 45 * 110
    snap = book.snapshot(s, pid, PRICES, "2026-06-03")
    assert snap["value"] == book.cash(s, pid) + 45 * 120
    assert snap["curve"][-1]["date"] == "2026-06-03"
    assert snap["benchmark_curve"][-1]["value"] == 10000 * 510 / 500


def test_pending_until_close_exists(tmp_path):
    s = Store(tmp_path / "l.sqlite")
    pid = book.create(s, "t", "USD", 10000)
    book.record_decision(s, pid, 1, "NVDA", "2026-06-03", "Buy", BUY5)
    assert book.fill_pending(s, PRICES, pid) == 0
    assert s.one("select status from trades")["status"] == "pending"


def test_hold_and_review(tmp_path):
    s = Store(tmp_path / "l.sqlite")
    pid = book.create(s, "t", "USD", 10000)
    assert book.record_decision(s, pid, 1, "NVDA", "2026-06-01", "REVIEW", "") is None
    book.record_decision(s, pid, 2, "NVDA", "2026-06-01", "Hold", "")
    assert s.one("select status from trades")["status"] == "skipped"


def test_context_lists_positions(tmp_path):
    s = Store(tmp_path / "l.sqlite")
    pid = book.create(s, "t", "USD", 10000)
    book.record_decision(s, pid, 1, "NVDA", "2026-06-01", "Buy", BUY5)
    book.fill_pending(s, PRICES, pid)
    ctx = book.context(s, pid)
    assert ctx.currency == "USD" and ctx.position_in("NVDA").quantity == 45
```

- [ ] **Step 2: Run** `.venv/bin/pytest tests/lab/test_book.py -q` → FAIL (import).

- [ ] **Step 3: Implement**

`tradingagents/lab/fills.py`:
```python
"""Prices for paper fills: the first close after the analysis date."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Protocol


class PriceSource(Protocol):
    def closes(self, ticker: str, start: str, end: str) -> list[tuple[str, float]]: ...
    def currency(self, ticker: str) -> str | None: ...


def _shift(day: str, days: int) -> str:
    return (datetime.strptime(day, "%Y-%m-%d") + timedelta(days=days)).strftime("%Y-%m-%d")


class YahooPrices:
    def closes(self, ticker: str, start: str, end: str) -> list[tuple[str, float]]:
        import yfinance as yf

        df = yf.Ticker(ticker).history(start=start, end=end, auto_adjust=False)
        return [(idx.strftime("%Y-%m-%d"), float(v)) for idx, v in df["Close"].items()]

    def currency(self, ticker: str) -> str | None:
        import yfinance as yf

        try:
            return yf.Ticker(ticker).fast_info["currency"]
        except Exception:  # unknown ticker or offline: the caller decides
            return None


def next_close(prices: PriceSource, ticker: str, day: str) -> tuple[str, float] | None:
    for d, p in prices.closes(ticker, _shift(day, 1), _shift(day, 12)):
        if d > day:
            return d, p
    return None


def close_on_or_before(prices: PriceSource, ticker: str, day: str) -> float | None:
    rows = [p for d, p in prices.closes(ticker, _shift(day, -12), _shift(day, 1)) if d <= day]
    return rows[-1] if rows else None
```

`tradingagents/lab/book.py`:
```python
"""Paper portfolios: decisions become trades filled at the next close."""

from __future__ import annotations

import json

from tradingagents.portfolio import PortfolioContext, Position

from . import sizing
from .fills import PriceSource, close_on_or_before, next_close
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
    start = p["created_at"][:10] if p["mode"] == "live" else (p["replay"] or {}).get("start", p["created_at"][:10])
    bench = prices.closes(p["benchmark"], start, today + "~")
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
```

Note: `today + "~"` makes the end bound inclusive for string-compared fakes and is a valid date string for Yahoo only if trimmed — use `fills._shift(today, 1)` instead in the implementation (import `_shift`); the test is unaffected.

- [ ] **Step 4: Run** `.venv/bin/pytest tests/lab -q && .venv/bin/ruff check tradingagents/lab tests/lab` → PASS.
- [ ] **Step 5: Commit** `git add -A tradingagents/lab tests/lab && git commit -m "feat(lab): carteira ficticia -- fechamento seguinte, caixa, curva x indice"`

---

### Task 4: Modelo/chave e execução do grafo (`llm.py`, `runner.py`)

**Files:** Create `tradingagents/lab/llm.py`, `tradingagents/lab/runner.py`; Test `tests/lab/test_runner.py`

**Interfaces:**
- Produces (`llm`): `KeyVault` (`put(id, key)`, `get(id)`, `has(id)`, `drop(id)`); `parse_model(d: dict) -> dict` (normalizado: provider, base_url, quick, deep, needs_key); `build_config(base: dict, model: dict, data_dir: Path) -> dict`; `provider_key(provider, key)` context manager.
- Produces (`runner`): `REPORT_AGENTS: dict[str, str]`; `run_analysis(graph, ticker, trade_date, portfolio=None, on_event=None) -> tuple[dict, str]`.

- [ ] **Step 1: Write the failing test**

```python
import os
from contextlib import contextmanager

from tradingagents.lab.llm import build_config, parse_model, provider_key
from tradingagents.lab.runner import run_analysis


class FakePropagator:
    def get_graph_args(self):
        return {"config": {}}


class FakeGraph:
    def __init__(self):
        self.propagator, self.recorded, self.cleared, self.args = FakePropagator(), None, False, None

    @contextmanager
    def checkpoint_scope(self, ticker, date, asset_type, portfolio):
        yield "tid-1"

    def create_run_state(self, ticker, date, asset_type, portfolio):
        return {"company_of_interest": ticker}

    def checkpoint_input(self, init):
        return init

    def stream_run(self, graph_input, **args):
        self.args = args
        yield [], {"market_report": "m"}
        yield [], None
        yield [], {"market_report": "m", "final_trade_decision": "**Rating**: Buy", "trader_investment_plan": "p"}

    def record_decision(self, ticker, date, final):
        self.recorded = final

    def clear_checkpoint_on_success(self, *a):
        self.cleared = True


def test_run_analysis_streams_events_and_rating():
    g, seen = FakeGraph(), []
    final, rating = run_analysis(g, "NVDA", "2026-06-01", on_event=seen.append)
    assert seen == ["Market Analyst", "Portfolio Manager", "Trader"] or seen[:1] == ["Market Analyst"]
    assert rating == "Buy" and g.recorded is final and g.cleared
    assert g.args["config"]["configurable"]["thread_id"] == "tid-1"


def test_parse_model_defaults():
    m = parse_model({"base_url": "http://odysseus:8000/v1", "quick": "qwen"})
    assert m == {"provider": "openai_compatible", "base_url": "http://odysseus:8000/v1",
                 "quick": "qwen", "deep": "qwen", "needs_key": False}


def test_build_config(tmp_path):
    cfg = build_config({"llm_provider": "openai"}, parse_model({"quick": "a", "deep": "b", "base_url": "u"}), tmp_path)
    assert cfg["llm_provider"] == "openai_compatible" and cfg["deep_think_llm"] == "b"
    assert cfg["checkpoint_enabled"] is True and cfg["results_dir"] == str(tmp_path / "logs")


def test_provider_key_restores_env():
    os.environ.pop("OPENAI_COMPATIBLE_API_KEY", None)
    with provider_key("openai_compatible", "sk-x"):
        assert os.environ["OPENAI_COMPATIBLE_API_KEY"] == "sk-x"
    assert "OPENAI_COMPATIBLE_API_KEY" not in os.environ
```

- [ ] **Step 2: Run** → FAIL (import).

- [ ] **Step 3: Implement**

`tradingagents/lab/llm.py`:
```python
"""The model a job runs on, and its API key, which lives only in memory."""

from __future__ import annotations

import copy
import os
import threading
from contextlib import contextmanager
from pathlib import Path

from tradingagents.llm_clients.api_key_env import get_api_key_env


class KeyVault:
    def __init__(self):
        self._keys: dict[int, str] = {}
        self._lock = threading.Lock()

    def put(self, job_id: int, key: str | None) -> None:
        if key:
            with self._lock:
                self._keys[job_id] = key

    def get(self, job_id: int) -> str | None:
        with self._lock:
            return self._keys.get(job_id)

    def has(self, job_id: int) -> bool:
        return self.get(job_id) is not None

    def drop(self, job_id: int) -> None:
        with self._lock:
            self._keys.pop(job_id, None)


def parse_model(d: dict) -> dict:
    if not d or not d.get("quick"):
        raise ValueError("model.quick é obrigatório")
    return {"provider": d.get("provider") or "openai_compatible", "base_url": d.get("base_url"),
            "quick": d["quick"], "deep": d.get("deep") or d["quick"], "needs_key": bool(d.get("needs_key"))}


def build_config(base: dict, model: dict, data_dir: Path) -> dict:
    cfg = copy.deepcopy(base)
    cfg.update(llm_provider=model["provider"], backend_url=model["base_url"],
               quick_think_llm=model["quick"], deep_think_llm=model["deep"],
               results_dir=str(data_dir / "logs"), data_cache_dir=str(data_dir / "cache"),
               memory_log_path=str(data_dir / "memory" / "trading_memory.md"), checkpoint_enabled=True)
    return cfg


@contextmanager
def provider_key(provider: str, key: str | None):
    """Expose the key to the provider client for one job, then take it back."""
    name = get_api_key_env(provider)
    if not name or not key:
        yield
        return
    old = os.environ.get(name)
    os.environ[name] = key
    try:
        yield
    finally:
        if old is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = old
```

`tradingagents/lab/runner.py`:
```python
"""Run one analysis through the graph's stream, reporting each agent as it files."""

from __future__ import annotations

from collections.abc import Callable

from tradingagents.agents.rating import run_rating

REPORT_AGENTS = {
    "market_report": "Market Analyst", "sentiment_report": "Sentiment Analyst",
    "news_report": "News Analyst", "fundamentals_report": "Fundamentals Analyst",
    "investment_plan": "Research Manager", "trader_investment_plan": "Trader",
    "final_trade_decision": "Portfolio Manager",
}


def run_analysis(graph, ticker: str, trade_date: str, portfolio=None,
                 on_event: Callable[[str], None] | None = None) -> tuple[dict, str]:
    """Same steps as TradingAgentsGraph.propagate, through stream_run (mirrors _run_graph)."""
    final: dict = {}
    seen: set[str] = set()
    with graph.checkpoint_scope(ticker, trade_date, "stock", portfolio) as thread_id:
        init = graph.create_run_state(ticker, trade_date, "stock", portfolio)
        args = graph.propagator.get_graph_args()
        if thread_id is not None:
            args.setdefault("config", {}).setdefault("configurable", {})["thread_id"] = thread_id
        for _messages, state in graph.stream_run(graph.checkpoint_input(init), **args):
            if not state:
                continue
            final.update(state)
            for key, agent in REPORT_AGENTS.items():
                if state.get(key) and key not in seen:
                    seen.add(key)
                    if on_event:
                        on_event(agent)
        graph.record_decision(ticker, trade_date, final)
        graph.clear_checkpoint_on_success(ticker, trade_date, "stock", portfolio)
    return final, run_rating(final)
```

Before Step 4, check `run_rating`'s input with `grep -n "def run_rating" -A8 tradingagents/agents/rating.py`; if it reads a different key than `final_trade_decision`, adjust the fake's final state (the assertion stays `rating == "Buy"`). Tighten the `seen` assertion to the exact order the fake yields: `["Market Analyst", "Trader", "Portfolio Manager"]` (dict order of `REPORT_AGENTS`).

- [ ] **Step 4: Run** `.venv/bin/pytest tests/lab -q && .venv/bin/ruff check tradingagents/lab tests/lab` → PASS.
- [ ] **Step 5: Commit** `git add -A tradingagents/lab tests/lab && git commit -m "feat(lab): modelo por job, chave so em memoria, analise por stream com eventos"`

---

### Task 5: Trabalhador + reencenação (`worker.py`, `replay.py`)

**Files:** Create `tradingagents/lab/worker.py`, `tradingagents/lab/replay.py`; Test `tests/lab/test_worker.py`

**Interfaces:**
- Consumes: `jobs`, `book`, `llm`, `runner.run_analysis`, `fills.PriceSource`.
- Produces: `Worker(store, data_dir, *, prices, keys, graph_factory=None, backtest_fn=None, base_config=None)` com `.recover()`, `.run_once() -> bool`, `.start()`, `.stop()`; `replay.schedule(store, pid, tickers, start, end, every_days, analysts, model) -> list[int]`.
- Job `payload` ao terminar uma análise: `{"trader_plan": str}`; `report_dir` = `data_dir/reports/<id>`.

- [ ] **Step 1: Write the failing test**

```python
from tests.lab.fakes import FakePrices
from tradingagents.lab import book, jobs, replay
from tradingagents.lab.llm import KeyVault
from tradingagents.lab.store import Store
from tradingagents.lab.worker import Worker

MODEL = {"provider": "openai_compatible", "base_url": "u", "quick": "q", "deep": "q", "needs_key": True}
PRICES = FakePrices({"NVDA": {"2026-06-01": 100.0, "2026-06-02": 100.0, "2026-06-08": 100.0, "2026-06-09": 100.0},
                     "SPY": {"2026-06-02": 1.0}})


class FakeGraph:
    contexts = []

    def __init__(self, config, analysts, rating="Buy"):
        self.config, self.rating = config, rating

    def run(self, ticker, date, portfolio, on_event):
        FakeGraph.contexts.append(portfolio)
        on_event("Market Analyst")
        plan = "**Action**: Buy\n\n**Position Sizing**: 10% of portfolio"
        return {"trader_investment_plan": plan, "final_trade_decision": f"**Rating**: {self.rating}"}, self.rating

    def run_settings(self):
        return {}


def fake_run(graph, ticker, date, portfolio=None, on_event=None):
    return graph.run(ticker, date, portfolio, on_event)


def make(tmp_path, monkeypatch, rating="Buy"):
    import tradingagents.lab.worker as w
    monkeypatch.setattr(w, "run_analysis", fake_run)
    monkeypatch.setattr(w, "write_report_tree", lambda final, ticker, path, settings=None: path.mkdir(parents=True))
    s, keys = Store(tmp_path / "l.sqlite"), KeyVault()
    return s, keys, Worker(s, tmp_path, prices=PRICES, keys=keys,
                           graph_factory=lambda cfg, an: FakeGraph(cfg, an, rating))


def test_analysis_done_key_used_then_dropped(tmp_path, monkeypatch):
    s, keys, w = make(tmp_path, monkeypatch)
    jid = jobs.enqueue(s, "analysis", ticker="NVDA", trade_date="2026-06-01", analysts=["market"], model=MODEL)
    keys.put(jid, "sk-secret")
    assert w.run_once() is True
    job = jobs.get(s, jid)
    assert job["status"] == "done" and job["rating"] == "Buy" and not keys.has(jid)
    assert [e["agent"] for e in jobs.events(s, jid)][-1] == "Market Analyst"
    raw = (tmp_path / "l.sqlite").read_bytes() + (tmp_path / "l.sqlite-wal").read_bytes()
    assert b"sk-secret" not in raw


def test_review_waits(tmp_path, monkeypatch):
    s, keys, w = make(tmp_path, monkeypatch, rating="REVIEW")
    jid = jobs.enqueue(s, "analysis", ticker="NVDA", trade_date="2026-06-01", model={**MODEL, "needs_key": False})
    w.run_once()
    assert jobs.get(s, jid)["status"] == "review"


def test_failure_marks_failed(tmp_path, monkeypatch):
    s, keys, w = make(tmp_path, monkeypatch)
    import tradingagents.lab.worker as wm
    monkeypatch.setattr(wm, "run_analysis", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("yahoo down")))
    jid = jobs.enqueue(s, "analysis", ticker="NVDA", trade_date="2026-06-01", model={**MODEL, "needs_key": False})
    w.run_once()
    assert jobs.get(s, jid)["status"] == "failed" and "yahoo down" in jobs.get(s, jid)["error"]


def test_replay_chain_passes_portfolio_of_the_moment(tmp_path, monkeypatch):
    s, keys, w = make(tmp_path, monkeypatch)
    FakeGraph.contexts.clear()
    pid = book.create(s, "r", "USD", 10000, mode="replay", replay={"start": "2026-06-01"})
    ids = replay.schedule(s, pid, ["NVDA"], "2026-06-01", "2026-06-08", 7, ["market"], {**MODEL, "needs_key": False})
    assert len(ids) == 2
    while w.run_once():
        pass
    assert FakeGraph.contexts[0].positions == []
    assert FakeGraph.contexts[1].position_in("NVDA").quantity == 10      # 10% of 10000 at 100
    assert all(jobs.get(s, i)["status"] == "done" for i in ids)
```

- [ ] **Step 2: Run** → FAIL (import).

- [ ] **Step 3: Implement**

`tradingagents/lab/replay.py`:
```python
"""Replay a past period: one chained job per date and ticker, in time order."""

from __future__ import annotations

from tradingagents.backtest import iter_grid

from . import jobs
from .store import Store


def schedule(store: Store, pid: int, tickers: list[str], start: str, end: str, every_days: int,
             analysts: list[str], model: dict) -> list[int]:
    ids: list[int] = []
    prev = None
    for day in iter_grid(start, end, every_days):
        for ticker in tickers:
            prev = jobs.enqueue(store, "analysis", ticker=ticker, trade_date=day, analysts=analysts,
                                model=model, portfolio_id=pid, after_id=prev)
            ids.append(prev)
    return ids
```

`tradingagents/lab/worker.py`:
```python
"""One thread, one job at a time."""

from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path

from tradingagents.backtest import run_backtest, summarize
from tradingagents.default_config import DEFAULT_CONFIG
from tradingagents.reporting import write_report_tree

from . import book, jobs
from .fills import PriceSource
from .llm import KeyVault, build_config, provider_key
from .runner import run_analysis
from .store import Store

logger = logging.getLogger(__name__)
FILL_EVERY_S = 3600


def _default_graph(config: dict, analysts: list[str]):
    from tradingagents.graph.trading_graph import TradingAgentsGraph

    return TradingAgentsGraph(analysts or ["market", "social", "news", "fundamentals"], config=config)


class Worker:
    def __init__(self, store: Store, data_dir: Path, *, prices: PriceSource, keys: KeyVault,
                 graph_factory=None, backtest_fn=None, base_config: dict | None = None):
        self.store, self.data_dir, self.prices, self.keys = store, Path(data_dir), prices, keys
        self.graph_factory = graph_factory or _default_graph
        self.backtest_fn = backtest_fn or run_backtest
        self.base_config = base_config or DEFAULT_CONFIG
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.current: int | None = None

    def recover(self) -> None:
        jobs.recover(self.store, self.keys.has)

    def run_once(self) -> bool:
        job = jobs.next_runnable(self.store)
        if job is None:
            return False
        self.current = job["id"]
        jobs.mark(self.store, job["id"], "running")
        try:
            with provider_key(job["model"]["provider"], self.keys.get(job["id"])):
                if job["kind"] == "backtest":
                    self._backtest(job)
                else:
                    self._analysis(job)
            self.keys.drop(job["id"])
        except Exception as exc:  # the job fails, the worker goes on
            logger.exception("job %s failed", job["id"])
            jobs.mark(self.store, job["id"], "failed", error=str(exc)[:2000])
        finally:
            self.current = None
        return True

    def _config(self, job: dict) -> dict:
        return build_config(self.base_config, job["model"], self.data_dir)

    def _analysis(self, job: dict) -> None:
        pid = job["portfolio_id"]
        graph = self.graph_factory(self._config(job), job["analysts"])
        portfolio = book.context(self.store, pid) if pid else None
        final, rating = run_analysis(graph, job["ticker"], job["trade_date"], portfolio,
                                     on_event=lambda agent: jobs.add_event(self.store, job["id"], agent, "end"))
        report_dir = self.data_dir / "reports" / str(job["id"])
        write_report_tree(final, job["ticker"], report_dir, settings=graph.run_settings())
        plan = final.get("trader_investment_plan")
        if rating == "REVIEW":
            jobs.mark(self.store, job["id"], "review", rating=rating, report_dir=str(report_dir),
                      payload={"trader_plan": plan})
            return
        jobs.mark(self.store, job["id"], "done", rating=rating, report_dir=str(report_dir),
                  payload={"trader_plan": plan})
        if pid:
            book.record_decision(self.store, pid, job["id"], job["ticker"], job["trade_date"], rating, plan)
            book.fill_pending(self.store, self.prices, pid)

    def _backtest(self, job: dict) -> None:
        bt = self.store.one("select * from backtests where job_id=?", (job["id"],))
        tickers = json.loads(bt["tickers_json"])
        from tradingagents.backtest import iter_grid

        dates = iter_grid(bt["start"], bt["end"], bt["every_days"])
        result = self.backtest_fn(
            tickers, dates, self._config(job), selected_analysts=job["analysts"] or
            ("market", "social", "news", "fundamentals"), run_id=f"lab{bt['id']}",
            progress=lambda done, total, t, d: jobs.add_event(self.store, job["id"], f"{t} {d}", "start",
                                                             f"{done}/{total}"))
        summary = summarize(result)
        self.store.run("update backtests set summary_json=? where id=?", (json.dumps({
            "text": summary.render(), "resolved": summary.resolved, "pending": summary.pending,
            "by_rating": {k: vars(v) for k, v in summary.by_rating.items()},
            "failures": result.failures}), bt["id"]))
        jobs.mark(self.store, job["id"], "done")

    def _loop(self) -> None:
        last_fill = 0.0
        while not self._stop.is_set():
            try:
                worked = self.run_once()
                if time.monotonic() - last_fill > FILL_EVERY_S:
                    book.fill_pending(self.store, self.prices)
                    last_fill = time.monotonic()
            except Exception:  # never let the loop die
                logger.exception("worker loop")
                worked = False
            if not worked:
                self._stop.wait(2)

    def start(self) -> None:
        self.recover()
        self._thread = threading.Thread(target=self._loop, name="lab-worker", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
```

`write_report_tree` imported by name in `worker` so the test can monkeypatch `tradingagents.lab.worker.write_report_tree`. `summarize(result)` on a fake: the backtest test lives in Task 6 via the API with a `backtest_fn` fake returning a `BacktestResult` whose `log_path` does not exist — guard: if `summarize` raises `FileNotFoundError`, store `{"text": "nenhuma célula concluída", "failures": result.failures}`.

- [ ] **Step 4: Run** `.venv/bin/pytest tests/lab -q && .venv/bin/ruff check tradingagents/lab tests/lab` → PASS.
- [ ] **Step 5: Commit** `git add -A tradingagents/lab tests/lab && git commit -m "feat(lab): trabalhador (uma analise por vez) e reencenacao encadeada"`

---

### Task 6: API HTTP + `tradingagents lab serve` + empacotamento

**Files:** Create `tradingagents/lab/api.py`; Modify `cli/main.py` (sub-app `lab`), `pyproject.toml` (extra `lab`), `Dockerfile` (`pip install ".[lab]"`); Test `tests/lab/test_api.py`

**Interfaces:**
- Consumes: everything above.
- Produces: `create_app(store, worker, keys, prices) -> FastAPI`; HTTP contract (consumed by the Odysseus plan):
  - `GET /health` → `{"ok": true, "version", "queued", "running": id|null}`
  - `POST /jobs` body `{ticker, date, analysts?, model, api_key?, portfolio_id?}` → `{"id"}`; 400 if currency of ticker ≠ portfolio currency
  - `GET /jobs?status=&limit=` → `{"jobs": [...]}` (no keys); `GET /jobs/{id}` → job + `events`
  - `POST /jobs/{id}/retry` body `{api_key?}`; `POST /jobs/{id}/resolve` body `{rating: str|null}`; `DELETE /jobs/{id}` (only queued)
  - `GET /reports/{id}` → `text/html`; `GET /reports/{id}/md` → `text/markdown`
  - `GET /portfolios`; `POST /portfolios` body `{name, currency, initial_cash, benchmark?, weights?, mode, replay?: {tickers, start, end, every_days, analysts?, model, api_key?}}`; `GET /portfolios/{id}`; `POST /portfolios/{id}/run` body `{tickers? | watchlist_id?, analysts?, model, api_key?, date?}` → `{"ids": [...]}`
  - `GET/POST /watchlists` (`{name, tickers}`), `DELETE /watchlists/{id}`
  - `GET /backtests`, `POST /backtests` body `{tickers, start, end, every_days, analysts?, model, api_key?}` → `{"id", "job_id"}`, `GET /backtests/{id}`

- [ ] **Step 1: Write the failing test** (`tests/lab/test_api.py`)

```python
from fastapi.testclient import TestClient

from tests.lab.fakes import FakePrices
from tradingagents.lab import jobs
from tradingagents.lab.api import create_app
from tradingagents.lab.llm import KeyVault
from tradingagents.lab.store import Store

MODEL = {"base_url": "http://odysseus:8000/v1", "quick": "qwen"}


class NoWorker:
    current = None


def client(tmp_path):
    s, keys = Store(tmp_path / "l.sqlite"), KeyVault()
    prices = FakePrices({}, {"PETR4.SA": "BRL", "NVDA": "USD"})
    return TestClient(create_app(s, NoWorker(), keys, prices)), s, keys


def test_job_roundtrip_key_kept_in_memory_only(tmp_path):
    c, s, keys = client(tmp_path)
    r = c.post("/jobs", json={"ticker": "nvda", "date": "2026-06-01", "model": MODEL, "api_key": "sk-1"})
    jid = r.json()["id"]
    job = c.get(f"/jobs/{jid}").json()
    assert job["ticker"] == "NVDA" and job["model"]["needs_key"] is True and "api_key" not in str(job)
    assert keys.get(jid) == "sk-1"
    assert c.get("/health").json()["queued"] == 1


def test_currency_mismatch_rejected(tmp_path):
    c, s, _ = client(tmp_path)
    pid = c.post("/portfolios", json={"name": "br", "currency": "BRL", "initial_cash": 10000, "mode": "live"}).json()["id"]
    r = c.post("/jobs", json={"ticker": "NVDA", "date": "2026-06-01", "model": MODEL, "portfolio_id": pid})
    assert r.status_code == 400
    ok = c.post(f"/portfolios/{pid}/run", json={"tickers": ["PETR4.SA"], "model": MODEL, "date": "2026-06-01"})
    assert len(ok.json()["ids"]) == 1


def test_replay_portfolio_enqueues_chain(tmp_path):
    c, s, _ = client(tmp_path)
    r = c.post("/portfolios", json={"name": "r", "currency": "USD", "initial_cash": 1000, "mode": "replay",
                                    "replay": {"tickers": ["NVDA"], "start": "2026-06-01", "end": "2026-06-15",
                                               "every_days": 7, "model": MODEL}})
    assert r.status_code == 200
    rows = jobs.listing(s)
    assert len(rows) == 3 and rows[0]["after_id"] == rows[1]["id"]


def test_watchlist_and_backtest(tmp_path):
    c, s, _ = client(tmp_path)
    wid = c.post("/watchlists", json={"name": "br", "tickers": ["PETR4.SA", "VALE3.SA"]}).json()["id"]
    assert c.get("/watchlists").json()["watchlists"][0]["tickers"] == ["PETR4.SA", "VALE3.SA"]
    b = c.post("/backtests", json={"tickers": ["NVDA"], "start": "2026-06-01", "end": "2026-06-15",
                                   "every_days": 7, "model": MODEL}).json()
    assert jobs.get(s, b["job_id"])["kind"] == "backtest"
    assert c.delete(f"/watchlists/{wid}").status_code == 200


def test_resolve_review_applies_to_portfolio(tmp_path):
    c, s, _ = client(tmp_path)
    pid = c.post("/portfolios", json={"name": "p", "currency": "USD", "initial_cash": 1000, "mode": "live"}).json()["id"]
    jid = jobs.enqueue(s, "analysis", ticker="NVDA", trade_date="2026-06-01", model=MODEL, portfolio_id=pid)
    jobs.mark(s, jid, "review", payload={"trader_plan": "**Action**: Buy"})
    assert c.post(f"/jobs/{jid}/resolve", json={"rating": "Buy"}).status_code == 200
    assert jobs.get(s, jid)["status"] == "done"
    assert s.one("select rating, status from trades") == {"rating": "Buy", "status": "pending"}


def test_report_404_when_missing(tmp_path):
    c, s, _ = client(tmp_path)
    assert c.get("/reports/99").status_code == 404
```

- [ ] **Step 2: Run** → FAIL (import).

- [ ] **Step 3: Implement** `tradingagents/lab/api.py`

```python
"""HTTP API of the lab. No auth of its own: reachable only on the compose network."""

from __future__ import annotations

import json
from datetime import date as _date
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

from tradingagents import __version__

from . import book, jobs, replay
from .fills import PriceSource
from .llm import KeyVault, parse_model
from .store import Store


class JobIn(BaseModel):
    ticker: str
    date: str
    analysts: list[str] = []
    model: dict
    api_key: str | None = None
    portfolio_id: int | None = None


class RetryIn(BaseModel):
    api_key: str | None = None


class ResolveIn(BaseModel):
    rating: str | None = None


class ReplayIn(BaseModel):
    tickers: list[str]
    start: str
    end: str
    every_days: int = 7
    analysts: list[str] = []
    model: dict
    api_key: str | None = None


class PortfolioIn(BaseModel):
    name: str
    currency: str
    initial_cash: float
    benchmark: str | None = None
    weights: dict | None = None
    mode: str = "live"
    replay: ReplayIn | None = None


class RunIn(BaseModel):
    tickers: list[str] = []
    watchlist_id: int | None = None
    analysts: list[str] = []
    model: dict
    api_key: str | None = None
    date: str | None = None


class WatchlistIn(BaseModel):
    name: str
    tickers: list[str]


class BacktestIn(BaseModel):
    tickers: list[str]
    start: str
    end: str
    every_days: int = 7
    analysts: list[str] = []
    model: dict
    api_key: str | None = None


def _model(d: dict, api_key: str | None) -> dict:
    try:
        return {**parse_model(d), "needs_key": bool(api_key)}
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from None


def create_app(store: Store, worker, keys: KeyVault, prices: PriceSource) -> FastAPI:
    app = FastAPI(title="TradingAgents Lab")

    def check_currency(pid: int | None, ticker: str) -> None:
        if pid is None:
            return
        p = book.get(store, pid)
        if p is None:
            raise HTTPException(404, "carteira não existe")
        cur = prices.currency(ticker)
        if cur and cur.upper() != p["currency"]:
            raise HTTPException(400, f"{ticker} é cotado em {cur}; a carteira é em {p['currency']}")

    def add_job(ticker: str, day: str, analysts, model: dict, api_key, pid=None) -> int:
        ticker = ticker.strip().upper()
        check_currency(pid, ticker)
        jid = jobs.enqueue(store, "analysis", ticker=ticker, trade_date=day, analysts=analysts,
                           model=model, portfolio_id=pid)
        keys.put(jid, api_key)
        return jid

    @app.get("/health")
    def health():
        return {"ok": True, "version": __version__, "running": worker.current,
                "queued": len(jobs.listing(store, "queued", 10_000))}

    @app.post("/jobs")
    def create_job(body: JobIn):
        return {"id": add_job(body.ticker, body.date, body.analysts, _model(body.model, body.api_key),
                              body.api_key, body.portfolio_id)}

    @app.get("/jobs")
    def list_jobs(status: str | None = None, limit: int = 100):
        return {"jobs": jobs.listing(store, status, limit)}

    @app.get("/jobs/{jid}")
    def job_detail(jid: int):
        job = jobs.get(store, jid) or _404()
        return {**job, "events": jobs.events(store, jid)}

    @app.post("/jobs/{jid}/retry")
    def retry(jid: int, body: RetryIn):
        job = jobs.get(store, jid) or _404()
        if job["status"] != "failed":
            raise HTTPException(409, "só um job com falha pode ser retomado")
        keys.put(jid, body.api_key)
        jobs.mark(store, jid, "queued", error=None, started_at=None, finished_at=None)
        return {"ok": True}

    @app.post("/jobs/{jid}/resolve")
    def resolve(jid: int, body: ResolveIn):
        job = jobs.get(store, jid) or _404()
        if job["status"] != "review":
            raise HTTPException(409, "job não está em revisão")
        rating = body.rating or "REVIEW"
        jobs.mark(store, jid, "done", rating=rating)
        if body.rating and job["portfolio_id"]:
            book.record_decision(store, job["portfolio_id"], jid, job["ticker"], job["trade_date"], body.rating,
                                 (job["payload"] or {}).get("trader_plan"))
        return {"ok": True}

    @app.delete("/jobs/{jid}")
    def cancel(jid: int):
        job = jobs.get(store, jid) or _404()
        if job["status"] != "queued":
            raise HTTPException(409, "só job na fila pode ser cancelado")
        store.run("delete from jobs where id=?", (jid,))
        keys.drop(jid)
        return {"ok": True}

    def _report(jid: int, name: str) -> str:
        job = jobs.get(store, jid)
        path = Path(job["report_dir"]) / name if job and job["report_dir"] else None
        if not path or not path.is_file():
            raise HTTPException(404, "relatório não existe")
        return path.read_text(encoding="utf-8")

    @app.get("/reports/{jid}")
    def report_html(jid: int):
        return PlainTextResponse(_report(jid, "complete_report.html"), media_type="text/html")

    @app.get("/reports/{jid}/md")
    def report_md(jid: int):
        return PlainTextResponse(_report(jid, "complete_report.md"), media_type="text/markdown")

    @app.get("/portfolios")
    def portfolios():
        return {"portfolios": book.listing(store)}

    @app.post("/portfolios")
    def create_portfolio(body: PortfolioIn):
        if body.mode == "replay" and not body.replay:
            raise HTTPException(400, "reencenação precisa de período e ações")
        r = body.replay
        pid = book.create(store, body.name, body.currency, body.initial_cash, body.benchmark, body.mode,
                          r.model_dump(exclude={"api_key"}) if r else None, body.weights)
        if r:
            for t in r.tickers:
                check_currency(pid, t.upper())
            ids = replay.schedule(store, pid, [t.upper() for t in r.tickers], r.start, r.end, r.every_days,
                                  r.analysts, _model(r.model, r.api_key))
            for jid in ids:
                keys.put(jid, r.api_key)
        return {"id": pid}

    @app.get("/portfolios/{pid}")
    def portfolio(pid: int):
        p = book.get(store, pid) or _404()
        today = (p["replay"] or {}).get("end") if p["mode"] == "replay" else None
        return book.snapshot(store, pid, prices, today or _date.today().isoformat())

    @app.post("/portfolios/{pid}/run")
    def run_portfolio(pid: int, body: RunIn):
        tickers = body.tickers
        if body.watchlist_id:
            wl = store.one("select * from watchlists where id=?", (body.watchlist_id,)) or _404()
            tickers = json.loads(wl["tickers_json"])
        model = _model(body.model, body.api_key)
        day = body.date or _date.today().isoformat()
        return {"ids": [add_job(t, day, body.analysts, model, body.api_key, pid) for t in tickers]}

    @app.get("/watchlists")
    def watchlists():
        return {"watchlists": [{"id": w["id"], "name": w["name"], "tickers": json.loads(w["tickers_json"])}
                               for w in store.all("select * from watchlists order by name")]}

    @app.post("/watchlists")
    def add_watchlist(body: WatchlistIn):
        tickers = [t.strip().upper() for t in body.tickers if t.strip()]
        return {"id": store.run("insert or replace into watchlists(name, tickers_json) values (?,?)",
                                (body.name, json.dumps(tickers)))}

    @app.delete("/watchlists/{wid}")
    def delete_watchlist(wid: int):
        store.run("delete from watchlists where id=?", (wid,))
        return {"ok": True}

    @app.get("/backtests")
    def backtests():
        rows = store.all("select b.*, j.status from backtests b left join jobs j on j.id=b.job_id order by b.id desc")
        return {"backtests": [_bt(r) for r in rows]}

    @app.post("/backtests")
    def add_backtest(body: BacktestIn):
        tickers = [t.strip().upper() for t in body.tickers]
        jid = jobs.enqueue(store, "backtest", analysts=body.analysts, model=_model(body.model, body.api_key))
        keys.put(jid, body.api_key)
        bid = store.run("insert into backtests(job_id, tickers_json, start, end, every_days) values (?,?,?,?,?)",
                        (jid, json.dumps(tickers), body.start, body.end, body.every_days))
        return {"id": bid, "job_id": jid}

    @app.get("/backtests/{bid}")
    def backtest(bid: int):
        row = store.one("select b.*, j.status from backtests b left join jobs j on j.id=b.job_id where b.id=?",
                        (bid,)) or _404()
        return _bt(row)

    return app


def _bt(row: dict) -> dict:
    row["tickers"] = json.loads(row.pop("tickers_json"))
    row["summary"] = json.loads(row.pop("summary_json") or "null")
    return row


def _404():
    raise HTTPException(404, "não encontrado")
```

`cli/main.py` — before `if __name__ == "__main__":` add:
```python
lab_app = typer.Typer(help="Trading Lab: paper portfolios, a run queue and an HTTP API.")
app.add_typer(lab_app, name="lab")


@lab_app.command("serve")
def lab_serve(host: str = "127.0.0.1", port: int = 7100):
    """Serve the lab API and run its worker (one analysis at a time)."""
    import os
    from pathlib import Path

    import uvicorn

    from tradingagents.lab.api import create_app
    from tradingagents.lab.fills import YahooPrices
    from tradingagents.lab.llm import KeyVault
    from tradingagents.lab.store import Store
    from tradingagents.lab.worker import Worker

    data_dir = Path(os.getenv("TRADINGAGENTS_LAB_DIR") or Path.home() / ".tradingagents")
    store, keys, prices = Store(data_dir / "lab.sqlite"), KeyVault(), YahooPrices()
    worker = Worker(store, data_dir, prices=prices, keys=keys)
    worker.start()
    uvicorn.run(create_app(store, worker, keys, prices), host=host, port=port)
```

`pyproject.toml` `[project.optional-dependencies]` add:
```toml
# Trading Lab API (tradingagents lab serve).
lab = [
    "fastapi>=0.115",
    "uvicorn>=0.30",
]
```
and add `"httpx>=0.27"` to `dev` (FastAPI TestClient).

`Dockerfile` builder: `RUN pip install --no-cache-dir ".[lab]"`.

- [ ] **Step 4: Run** `.venv/bin/pip install -q -e ".[dev,lab]" && .venv/bin/pytest -q && .venv/bin/ruff check .` → whole suite PASS, ruff clean.
- [ ] **Step 5: Smoke** `TRADINGAGENTS_LAB_DIR=$(mktemp -d) timeout 5 .venv/bin/tradingagents lab serve --port 7199 & sleep 3; curl -s localhost:7199/health` → `{"ok":true,...}`.
- [ ] **Step 6: Commit** `git add -A && git commit -m "feat(lab): API HTTP, 'tradingagents lab serve', extra [lab] e imagem"`

---

### Task 7: Docs do fork

**Files:** Create `docs/lab.md`; Modify `README.md` (one line under Installation linking `docs/lab.md`).

- [ ] **Step 1:** `docs/lab.md` — o que é (dinheiro fictício, nunca ordem real), como subir (`tradingagents lab serve` / container no compose do Odysseus), variáveis (`TRADINGAGENTS_LAB_DIR`), contrato da API (copiar a lista da Task 6), regra de tamanho (copiar §4.3 da spec), onde ficam os dados.
- [ ] **Step 2:** Commit `git commit -am "docs(lab): como usar o Trading Lab"`

---

## Self-review

- Spec §4 módulos: store/jobs (T1), sizing (T2), fills/book (T3), llm/runner (T4), worker/replay (T5), api/serve (T6). `watchlists.py` da spec virou tabela + rotas na API (sem lógica própria). §4.2 chave: T4 KeyVault/provider_key, T5 teste varre o SQLite+WAL. §4.4 pendente + tique de 1 h: T3 + `_loop`. §4.6 backtest como job: T5/T6. §6 erros: failed/retry/review/requeue/moeda — T1, T5, T6. Odysseus (§5) é o plano B.
- Tipos: `decide_target(rating, trader_plan, weights)` e `order_delta(target, held, cash, total_value, price, bullish)` usados igual em T2/T3; `run_analysis(graph, ticker, trade_date, portfolio, on_event)` igual em T4/T5; `KeyVault.put/get/has/drop` igual em T4/T5/T6.
