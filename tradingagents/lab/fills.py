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
