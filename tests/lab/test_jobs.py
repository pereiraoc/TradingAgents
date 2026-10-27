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
