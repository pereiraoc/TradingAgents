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
