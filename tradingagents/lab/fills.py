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
    """Daily closes and quote currency through the data layer (vendor calls live there)."""

    def closes(self, ticker: str, start: str, end: str) -> list[tuple[str, float]]:
        from tradingagents.dataflows.vendors.yahoo.market import get_closes

        return [(idx.strftime("%Y-%m-%d"), float(v)) for idx, v in get_closes(ticker, start, end).items()]

    def currency(self, ticker: str) -> str | None:
        from tradingagents.dataflows.vendors.yahoo.market import get_currency

        return get_currency(ticker)


def next_close(prices: PriceSource, ticker: str, day: str) -> tuple[str, float] | None:
    for d, p in prices.closes(ticker, _shift(day, 1), _shift(day, 12)):
        if d > day:
            return d, p
    return None


def close_on_or_before(prices: PriceSource, ticker: str, day: str) -> float | None:
    rows = [p for d, p in prices.closes(ticker, _shift(day, -12), _shift(day, 1)) if d <= day]
    return rows[-1] if rows else None
