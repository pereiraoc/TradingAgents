from fastapi.testclient import TestClient

from tests.lab.fakes import FakePrices
from tradingagents.lab import jobs
from tradingagents.lab.api import create_app
from tradingagents.lab.llm import KeyVault
from tradingagents.lab.store import Store

MODEL = {"base_url": "http://odysseus:8000/v1", "quick": "qwen"}


class NoWorker:
    current = None


def client(tmp_path):
    s, keys = Store(tmp_path / "l.sqlite"), KeyVault()
    prices = FakePrices({}, {"PETR4.SA": "BRL", "NVDA": "USD"})
    return TestClient(create_app(s, NoWorker(), keys, prices)), s, keys


def test_job_roundtrip_key_kept_in_memory_only(tmp_path):
    c, s, keys = client(tmp_path)
    r = c.post("/jobs", json={"ticker": "nvda", "date": "2026-06-01", "model": MODEL, "api_key": "sk-1"})
    jid = r.json()["id"]
    job = c.get(f"/jobs/{jid}").json()
    assert job["ticker"] == "NVDA" and job["model"]["needs_key"] is True and "api_key" not in str(job)
    assert keys.get(jid) == "sk-1"
    assert c.get("/health").json()["queued"] == 1


def test_currency_mismatch_rejected(tmp_path):
    c, s, _ = client(tmp_path)
    pid = c.post("/portfolios", json={"name": "br", "currency": "BRL", "initial_cash": 10000, "mode": "live"}).json()["id"]
    r = c.post("/jobs", json={"ticker": "NVDA", "date": "2026-06-01", "model": MODEL, "portfolio_id": pid})
    assert r.status_code == 400
    ok = c.post(f"/portfolios/{pid}/run", json={"tickers": ["PETR4.SA"], "model": MODEL, "date": "2026-06-01"})
    assert len(ok.json()["ids"]) == 1


def test_replay_portfolio_enqueues_chain(tmp_path):
    c, s, _ = client(tmp_path)
    r = c.post("/portfolios", json={"name": "r", "currency": "USD", "initial_cash": 1000, "mode": "replay",
                                    "replay": {"tickers": ["NVDA"], "start": "2026-06-01", "end": "2026-06-15",
                                               "every_days": 7, "model": MODEL}})
    assert r.status_code == 200
    rows = jobs.listing(s)
    assert len(rows) == 3 and rows[0]["after_id"] == rows[1]["id"]


def test_watchlist_and_backtest(tmp_path):
    c, s, _ = client(tmp_path)
    wid = c.post("/watchlists", json={"name": "br", "tickers": ["PETR4.SA", "VALE3.SA"]}).json()["id"]
    assert c.get("/watchlists").json()["watchlists"][0]["tickers"] == ["PETR4.SA", "VALE3.SA"]
    b = c.post("/backtests", json={"tickers": ["NVDA"], "start": "2026-06-01", "end": "2026-06-15",
                                   "every_days": 7, "model": MODEL}).json()
    assert jobs.get(s, b["job_id"])["kind"] == "backtest"
    assert c.delete(f"/watchlists/{wid}").status_code == 200


def test_resolve_review_applies_to_portfolio(tmp_path):
    c, s, _ = client(tmp_path)
    pid = c.post("/portfolios", json={"name": "p", "currency": "USD", "initial_cash": 1000, "mode": "live"}).json()["id"]
    jid = jobs.enqueue(s, "analysis", ticker="NVDA", trade_date="2026-06-01", model=MODEL, portfolio_id=pid)
    jobs.mark(s, jid, "review", payload={"trader_plan": "**Action**: Buy"})
    assert c.post(f"/jobs/{jid}/resolve", json={"rating": "Buy"}).status_code == 200
    assert jobs.get(s, jid)["status"] == "done"
    assert s.one("select rating, status from trades") == {"rating": "Buy", "status": "pending"}


def test_report_404_when_missing(tmp_path):
    c, s, _ = client(tmp_path)
    assert c.get("/reports/99").status_code == 404
