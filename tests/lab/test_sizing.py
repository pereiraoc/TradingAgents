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
