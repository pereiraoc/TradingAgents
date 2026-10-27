import json
from datetime import datetime

from fastapi.testclient import TestClient

from tests.lab.fakes import FakePrices
from tradingagents.lab import book, jobs, radar
from tradingagents.lab.api import create_app
from tradingagents.lab.llm import KeyVault
from tradingagents.lab.radar import sources
from tradingagents.lab.store import Store
from tradingagents.lab.worker import Worker

RSS = b"""<?xml version="1.0"?><rss><channel>
<item><title>Tesla jumps after Musk post</title><link>https://x/1</link>
<pubDate>Tue, 27 Oct 2026 02:46:20 GMT</pubDate><description>EV maker rallies</description></item>
<item><title>Iron ore slides</title><link>https://x/2</link><pubDate>Tue, 27 Oct 2026 01:00:00 GMT</pubDate></item>
</channel></rss>"""
CONGRESS = json.dumps({"trades": [
    {"member": "Jane Doe", "member_slug": "jane-doe", "chamber": "house", "ticker": "NVDA", "type": "purchase",
     "amount_low": 1001, "amount_high": 15000, "amount_range": "$1,001 - $15,000",
     "transaction_date": "2026-10-16", "disclosure_date": "2026-10-25", "asset": "NVIDIA"},
    {"member": "Jane Doe", "member_slug": "jane-doe", "chamber": "house", "ticker": "", "type": "purchase"},
]}).encode()
ATOM = b"""<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom">
<entry><title>4 - Boss (Reporting)</title><link href="https://sec/Archives/1/2/0001-index.htm"/><updated>2026-10-26T18:00:00-04:00</updated></entry>
<entry><title>4 - ACME (Issuer)</title><link href="https://sec/Archives/9/2/0001-index.htm"/><updated>2026-10-26T18:00:00-04:00</updated></entry>
</feed>"""
INDEX = json.dumps({"directory": {"item": [{"name": "0001-index.htm"}, {"name": "form4.xml"}]}}).encode()
FORM4 = b"""<ownershipDocument><issuer><issuerTradingSymbol>ACME</issuerTradingSymbol></issuer>
<reportingOwner><reportingOwnerId><rptOwnerName>Big Boss</rptOwnerName></reportingOwnerId>
<reportingOwnerRelationship><officerTitle>CEO</officerTitle></reportingOwnerRelationship></reportingOwner>
<nonDerivativeTable><nonDerivativeTransaction><transactionCoding><transactionCode>P</transactionCode></transactionCoding>
<transactionAmounts><transactionShares><value>10000</value></transactionShares>
<transactionPricePerShare><value>50</value></transactionPricePerShare></transactionAmounts>
</nonDerivativeTransaction></nonDerivativeTable></ownershipDocument>"""


def fetch(url):
    if "bloomberg" in url or "news.google" in url:
        return RSS
    if "bargo" in url:
        return CONGRESS
    if "getcurrent" in url:
        return ATOM
    if url.endswith("index.json"):
        return INDEX
    if url.endswith("form4.xml"):
        return FORM4
    raise AssertionError(url)


def test_sources_parse():
    assert [s["title"] for s in sources.bloomberg(lambda u: RSS)][:2] == ["Tesla jumps after Musk post", "Iron ore slides"]
    c = sources.congress(fetch)
    assert len(c) == 1 and c[0]["hits"][0]["ticker"] == "NVDA" and c[0]["hits"][0]["direction"] == 1
    ins = sources.insiders(fetch, pause=0)
    assert len(ins) == 1 and ins[0]["hits"][0] == {"ticker": "ACME", "direction": 1, "strength": ins[0]["hits"][0]["strength"],
                                                   "reason": "executivo comprou no mercado"}
    assert ins[0]["title"] == "Big Boss (CEO) comprou US$ 500,000 de ACME"


def test_collect_is_idempotent_and_reports_failures(tmp_path):
    s = Store(tmp_path / "l.sqlite")
    radar.set_config(s, {"figures": [{"name": "Elon Musk", "lang": "en"}]})
    first = radar.collect(s, fetch)
    assert first["bloomberg"] == 2 and first["congress"] == 1 and first["insider"] == 1
    assert radar.collect(s, fetch)["bloomberg"] == 0
    bad = radar.collect(s, lambda u: (_ for _ in ()).throw(OSError("offline")))
    assert str(bad["bloomberg"]).startswith("falhou")


def test_parse_hits_tolerates_noise():
    text = '<think>hmm</think> Sure: {"hits":[{"ticker":"tsla","direction":1,"strength":0.8,"reason":"post"},' \
           '{"ticker":"bad ticker!","direction":1}]}'
    assert radar.parse_hits(text) == [{"ticker": "TSLA", "direction": 1, "strength": 0.8, "reason": "post"}]
    assert radar.parse_hits("no json here") == []


def test_process_ranking_and_pick(tmp_path):
    s = Store(tmp_path / "l.sqlite")
    radar.collect(s, fetch)
    asked = []

    def ask(prompt):
        asked.append(prompt)
        if "Tesla" in prompt:
            return '{"hits":[{"ticker":"TSLA","direction":1,"strength":0.9,"reason":"Musk"},{"ticker":"FAKE","direction":1}]}'
        return '{"hits":[]}'
    stored = radar.process(s, ask, symbol_ok=lambda t: t != "FAKE")
    assert stored >= 1 and asked
    assert s.one("select count(*) n from radar_signals where processed=0")["n"] == 0
    board = radar.ranking(s, at=datetime(2026, 10, 27, 12))
    tickers = [b["ticker"] for b in board]
    assert "TSLA" in tickers and "FAKE" not in tickers and "NVDA" in tickers
    pid = book.create(s, "radar", "USD", 10000)
    radar.set_config(s, {"portfolio_id": pid, "min_score": 0.1, "top_n": 2})
    picks = radar.pick(s, currency_of=lambda t: "USD", at=datetime(2026, 10, 27, 12))
    assert picks and picks[0] == "TSLA" and len(picks) <= 2


def test_worker_radar_job_queues_picks(tmp_path, monkeypatch):
    s, keys = Store(tmp_path / "l.sqlite"), KeyVault()
    radar.collect(s, fetch)
    pid = book.create(s, "radar", "USD", 10000)
    radar.set_config(s, {"portfolio_id": pid, "min_score": 0.1, "top_n": 1})
    w = Worker(s, tmp_path, prices=FakePrices({}, {"TSLA": "USD", "NVDA": "USD", "ACME": "USD"}), keys=keys,
               model_up=lambda m, k: True,
               ask_factory=lambda cfg: (lambda p: '{"hits":[{"ticker":"TSLA","direction":1,"strength":1}]}'))
    model = {"provider": "openai_compatible", "base_url": "u", "quick": "q", "deep": "q", "needs_key": True}
    jid = jobs.enqueue(s, "radar", model=model)
    keys.put(jid, "sk-1")
    assert w.run_once()
    job = jobs.get(s, jid)
    assert job["status"] == "done" and job["payload"]["picks"] == ["TSLA"]
    new = job["payload"]["jobs"][0]
    assert jobs.get(s, new)["portfolio_id"] == pid and keys.get(new) == "sk-1"


def test_api_radar_and_search(tmp_path, monkeypatch):
    s, keys = Store(tmp_path / "l.sqlite"), KeyVault()

    class W:
        current = None
        waiting = None
    c = TestClient(create_app(s, W(), keys, FakePrices({}, {"PETR4.SA": "BRL"})))
    monkeypatch.setattr(radar, "collect", lambda store, fetch_=None: {"bloomberg": 0})
    assert c.post("/radar/collect").json() == {"report": {"bloomberg": 0}}
    cfg = c.post("/radar/config", json={"top_n": 5, "nonsense": 1}).json()
    assert cfg["top_n"] == 5 and "nonsense" not in cfg
    view = c.get("/radar").json()
    assert view["ranking"] == [] and view["config"]["top_n"] == 5
    jid = c.post("/radar/run", json={"model": {"base_url": "u", "quick": "q"}}).json()["id"]
    assert jobs.get(s, jid)["kind"] == "radar"
    import tradingagents.dataflows.vendors.yahoo.market as m
    monkeypatch.setattr(m, "search_symbols", lambda q, limit=8: [])
    assert c.get("/search", params={"q": "petr4"}).json()["results"][0]["symbol"] == "PETR4.SA"
    r = c.post("/jobs", json={"ticker": "petr4", "date": "2026-06-01", "model": {"base_url": "u", "quick": "q"}})
    assert jobs.get(s, r.json()["id"])["ticker"] == "PETR4.SA"
