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
