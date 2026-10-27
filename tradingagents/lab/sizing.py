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
