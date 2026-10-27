from datetime import datetime

from fastapi.testclient import TestClient

from tests.lab.fakes import FakePrices
from tradingagents.lab import autopilot, book, jobs, sizing
from tradingagents.lab.api import create_app
from tradingagents.lab.llm import KeyVault
from tradingagents.lab.store import Store
from tradingagents.lab.worker import Worker

MODEL = {"provider": "openai_compatible", "base_url": "http://odysseus:8000/v1", "quick": "q", "deep": "q",
         "needs_key": False}
PRICES = FakePrices({}, {"PETR4.SA": "BRL", "VALE3.SA": "BRL", "NVDA": "USD"})
TUE_10 = datetime(2026, 10, 27, 10, 0)
TUE_20 = datetime(2026, 10, 27, 20, 0)


def portfolio(s, **auto):
    pid = book.create(s, "auto", "BRL", 10_000)
    autopilot.save(s, pid, {"model": MODEL, "tickers": ["PETR4.SA", "VALE3.SA"], **auto})
    return pid


def test_slots_skip_weekends():
    assert autopilot.last_slot(TUE_10, 19) == "2026-10-26"
    assert autopilot.last_slot(TUE_20, 19) == "2026-10-27"
    assert autopilot.last_slot(datetime(2026, 11, 1, 12), 19) == "2026-10-30"   # Sunday -> Friday
    assert autopilot.next_slot(datetime(2026, 10, 30, 20), 19) == "2026-11-02T19:00"


def test_first_round_now_then_once_per_business_day(tmp_path):
    s = Store(tmp_path / "l.sqlite")
    pid = portfolio(s)
    assert autopilot.due(autopilot.get(s, pid), TUE_10)
    ids = autopilot.run_round(s, pid, PRICES.currency, TUE_10)
    assert [jobs.get(s, i)["ticker"] for i in ids] == ["PETR4.SA", "VALE3.SA"]
    assert not autopilot.due(autopilot.get(s, pid), TUE_10)
    assert autopilot.due(autopilot.get(s, pid), TUE_20)
    assert autopilot.run_round(s, pid, PRICES.currency, TUE_20) == []   # still queued: not twice


def test_round_includes_holdings_and_skips_failed_only(tmp_path):
    s = Store(tmp_path / "l.sqlite")
    pid = portfolio(s, tickers=["VALE3.SA"])
    s.run("insert into trades(portfolio_id, ticker, trade_date, rating, side, quantity, price, fill_date, status)"
          " values (?,?,?,?,?,?,?,?,?)", (pid, "ITUB4.SA", "2026-10-20", "Buy", "buy", 10, 30.0, "2026-10-21", "filled"))
    failed = jobs.enqueue(s, "analysis", ticker="VALE3.SA", trade_date="2026-10-27", model=MODEL, portfolio_id=pid)
    jobs.mark(s, failed, "failed", error="x")
    ids = autopilot.run_round(s, pid, PRICES.currency, TUE_10)
    assert [jobs.get(s, i)["ticker"] for i in ids] == ["ITUB4.SA", "VALE3.SA"]


def test_weights_spread_cash_and_ignore_trader_percent():
    w = autopilot.weights_for(8)
    plan = "**Action**: Buy\n\n**Position Sizing**: 3% of portfolio"
    t = sizing.decide_target("Buy", plan, w)
    assert t.kind == "weight" and t.value == 0.125 and t.source == "default_weight"
    assert sizing.decide_target("Buy", plan, {}).value == 0.03


def test_worker_waits_for_model(tmp_path):
    s = Store(tmp_path / "l.sqlite")
    pid = portfolio(s)
    up = {"v": False}
    w = Worker(s, tmp_path, prices=PRICES, keys=KeyVault(), model_up=lambda m, k: up["v"], clock=lambda: TUE_10)
    assert w._auto_rounds() is False and jobs.listing(s, "queued") == []
    up["v"], w._last_auto = True, -float("inf")
    assert w._auto_rounds() is True
    assert {j["ticker"] for j in jobs.listing(s, "queued")} == {"PETR4.SA", "VALE3.SA"}
    assert all(j["portfolio_id"] == pid for j in jobs.listing(s, "queued"))


class UpWorker:
    current = None

    @staticmethod
    def model_up(model, key):
        return True


def client(tmp_path):
    s = Store(tmp_path / "l.sqlite")
    return TestClient(create_app(s, UpWorker(), KeyVault(), PRICES)), s


def test_api_create_auto_refuses_key_and_wrong_currency(tmp_path):
    c, s = client(tmp_path)
    body = {"name": "a", "currency": "BRL", "initial_cash": 10000}
    r = c.post("/portfolios", json={**body, "auto": {"model": MODEL, "api_key": "sk-x", "tickers": ["PETR4"]}})
    assert r.status_code == 400 and "sk-x" not in r.text
    assert c.post("/portfolios", json={**body, "auto": {"model": MODEL, "tickers": ["NVDA"]}}).status_code == 400
    assert book.listing(s) == []
    pid = c.post("/portfolios", json={**body, "auto": {"model": MODEL, "tickers": ["petr4"], "max_positions": 4}}).json()["id"]
    snap = c.get(f"/portfolios/{pid}").json()
    assert snap["auto"]["tickers"] == ["PETR4.SA"] and snap["auto"]["next"] == "agora"
    assert snap["portfolio"]["weights"]["Buy"] == 0.25 and snap["jobs"] == []


def test_api_toggle_and_delete(tmp_path):
    c, s = client(tmp_path)
    pid = c.post("/portfolios", json={"name": "a", "currency": "BRL", "initial_cash": 10000,
                                      "auto": {"model": MODEL}}).json()["id"]
    assert c.post(f"/portfolios/{pid}/auto", json={"enabled": False}).json()["auto"]["enabled"] is False
    jobs.enqueue(s, "analysis", ticker="PETR4.SA", trade_date="2026-10-27", model=MODEL, portfolio_id=pid)
    assert c.delete(f"/portfolios/{pid}").json()["ok"]
    assert book.listing(s) == [] and jobs.listing(s) == []
    assert book.record_decision(s, pid, 99, "PETR4.SA", "2026-10-27", "Buy", None) is None


def test_old_database_gains_auto_column(tmp_path):
    import sqlite3
    path = tmp_path / "old.sqlite"
    con = sqlite3.connect(path)
    con.execute("create table portfolios(id integer primary key, name text not null, currency text not null,"
                " initial_cash real not null, benchmark text not null, mode text not null default 'live',"
                " replay_json text, weights_json text, created_at text not null)")
    con.commit()
    con.close()
    s = Store(path)
    pid = book.create(s, "x", "BRL", 1)
    assert book.get(s, pid)["auto"] is None
