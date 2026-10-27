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
