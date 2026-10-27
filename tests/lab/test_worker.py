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
        on_event("Market Analyst", "excerpt")
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
                           graph_factory=lambda cfg, an: FakeGraph(cfg, an, rating), model_up=lambda m, k: True)


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


def test_failure_text_never_carries_the_key(tmp_path, monkeypatch):
    s, keys, w = make(tmp_path, monkeypatch)
    import tradingagents.lab.worker as wm
    monkeypatch.setattr(wm, "run_analysis",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("401 key=mykey123 hdr Bearer abc sk-zzzzzzzz")))
    jid = jobs.enqueue(s, "analysis", ticker="NVDA", trade_date="2026-06-01", model=MODEL)
    keys.put(jid, "mykey123")
    w.run_once()
    err = jobs.get(s, jid)["error"]
    assert "mykey123" not in err and "abc" not in err and "sk-zzzz" not in err


def test_orphan_running_job_does_not_block_queue(tmp_path, monkeypatch):
    s, keys, w = make(tmp_path, monkeypatch)
    stuck = jobs.enqueue(s, "analysis", ticker="NVDA", trade_date="2026-06-01", model={**MODEL, "needs_key": False})
    jobs.mark(s, stuck, "running")
    nxt = jobs.enqueue(s, "analysis", ticker="NVDA", trade_date="2026-06-08", model={**MODEL, "needs_key": False})
    assert w.run_once() is True
    assert jobs.get(s, stuck)["status"] == "failed" and jobs.get(s, nxt)["status"] == "done"


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


def test_waits_for_model_instead_of_failing(tmp_path, monkeypatch):
    import tradingagents.lab.worker as wm
    s, keys, w = make(tmp_path, monkeypatch)
    up = {"v": False}
    w.model_up = lambda m, k: up["v"]
    monkeypatch.setattr(wm, "MODEL_POLL_S", 0)
    jid = jobs.enqueue(s, "analysis", ticker="NVDA", trade_date="2026-06-01", model={**MODEL, "needs_key": False})
    assert w.run_once() is False and w.waiting == jid
    assert jobs.get(s, jid)["status"] == "queued"
    assert [e["agent"] for e in jobs.events(s, jid)] == ["Modelo"]
    up["v"] = True
    assert w.run_once() is True and jobs.get(s, jid)["status"] == "done" and w.waiting is None


def test_gives_up_after_max_wait(tmp_path, monkeypatch):
    import tradingagents.lab.worker as wm
    s, keys, w = make(tmp_path, monkeypatch)
    w.model_up = lambda m, k: False
    monkeypatch.setattr(wm, "MODEL_POLL_S", 0)
    monkeypatch.setattr(wm, "MODEL_WAIT_MAX_S", -1)
    jid = jobs.enqueue(s, "analysis", ticker="NVDA", trade_date="2026-06-01", model={**MODEL, "needs_key": False})
    w.run_once()
    assert jobs.get(s, jid)["status"] == "failed" and "Cookbook" in jobs.get(s, jid)["error"]
