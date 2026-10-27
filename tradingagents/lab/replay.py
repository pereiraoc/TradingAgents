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
