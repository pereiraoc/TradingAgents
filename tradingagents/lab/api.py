"""HTTP API of the lab. No auth of its own: reachable only on the compose network."""

from __future__ import annotations

import json
from datetime import date as _date
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

from tradingagents import __version__

from . import book, jobs, replay
from .fills import PriceSource
from .llm import KeyVault, parse_model
from .store import Store


class JobIn(BaseModel):
    ticker: str
    date: str
    analysts: list[str] = []
    model: dict
    api_key: str | None = None
    portfolio_id: int | None = None


class RetryIn(BaseModel):
    api_key: str | None = None


class ResolveIn(BaseModel):
    rating: str | None = None


class ReplayIn(BaseModel):
    tickers: list[str]
    start: str
    end: str
    every_days: int = 7
    analysts: list[str] = []
    model: dict
    api_key: str | None = None


class PortfolioIn(BaseModel):
    name: str
    currency: str
    initial_cash: float
    benchmark: str | None = None
    weights: dict | None = None
    mode: str = "live"
    replay: ReplayIn | None = None


class RunIn(BaseModel):
    tickers: list[str] = []
    watchlist_id: int | None = None
    analysts: list[str] = []
    model: dict
    api_key: str | None = None
    date: str | None = None


class WatchlistIn(BaseModel):
    name: str
    tickers: list[str]


class BacktestIn(BaseModel):
    tickers: list[str]
    start: str
    end: str
    every_days: int = 7
    analysts: list[str] = []
    model: dict
    api_key: str | None = None


def _model(d: dict, api_key: str | None) -> dict:
    try:
        return {**parse_model(d), "needs_key": bool(api_key)}
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from None


def create_app(store: Store, worker, keys: KeyVault, prices: PriceSource) -> FastAPI:
    app = FastAPI(title="TradingAgents Lab")

    def check_currency(pid: int | None, ticker: str) -> None:
        if pid is None:
            return
        p = book.get(store, pid)
        if p is None:
            raise HTTPException(404, "carteira não existe")
        cur = prices.currency(ticker)
        if cur and cur.upper() != p["currency"]:
            raise HTTPException(400, f"{ticker} é cotado em {cur}; a carteira é em {p['currency']}")

    def add_job(ticker: str, day: str, analysts, model: dict, api_key, pid=None) -> int:
        ticker = ticker.strip().upper()
        check_currency(pid, ticker)
        jid = jobs.enqueue(store, "analysis", ticker=ticker, trade_date=day, analysts=analysts,
                           model=model, portfolio_id=pid)
        keys.put(jid, api_key)
        return jid

    @app.get("/health")
    def health():
        return {"ok": True, "version": __version__, "running": worker.current,
                "queued": len(jobs.listing(store, "queued", 10_000))}

    @app.post("/jobs")
    def create_job(body: JobIn):
        return {"id": add_job(body.ticker, body.date, body.analysts, _model(body.model, body.api_key),
                              body.api_key, body.portfolio_id)}

    @app.get("/jobs")
    def list_jobs(status: str | None = None, limit: int = 100):
        return {"jobs": jobs.listing(store, status, limit)}

    @app.get("/jobs/{jid}")
    def job_detail(jid: int):
        job = jobs.get(store, jid) or _404()
        return {**job, "events": jobs.events(store, jid)}

    @app.post("/jobs/{jid}/retry")
    def retry(jid: int, body: RetryIn):
        job = jobs.get(store, jid) or _404()
        if job["status"] != "failed":
            raise HTTPException(409, "só um job com falha pode ser retomado")
        keys.put(jid, body.api_key)
        jobs.mark(store, jid, "queued", error=None, started_at=None, finished_at=None)
        return {"ok": True}

    @app.post("/jobs/{jid}/resolve")
    def resolve(jid: int, body: ResolveIn):
        job = jobs.get(store, jid) or _404()
        if job["status"] != "review":
            raise HTTPException(409, "job não está em revisão")
        rating = body.rating or "REVIEW"
        jobs.mark(store, jid, "done", rating=rating)
        if body.rating and job["portfolio_id"]:
            book.record_decision(store, job["portfolio_id"], jid, job["ticker"], job["trade_date"], body.rating,
                                 (job["payload"] or {}).get("trader_plan"))
        return {"ok": True}

    @app.delete("/jobs/{jid}")
    def cancel(jid: int):
        job = jobs.get(store, jid) or _404()
        if job["status"] != "queued":
            raise HTTPException(409, "só job na fila pode ser cancelado")
        store.run("delete from jobs where id=?", (jid,))
        keys.drop(jid)
        return {"ok": True}

    def _report(jid: int, name: str) -> str:
        job = jobs.get(store, jid)
        path = Path(job["report_dir"]) / name if job and job["report_dir"] else None
        if not path or not path.is_file():
            raise HTTPException(404, "relatório não existe")
        return path.read_text(encoding="utf-8")

    @app.get("/reports/{jid}")
    def report_html(jid: int):
        return PlainTextResponse(_report(jid, "complete_report.html"), media_type="text/html")

    @app.get("/reports/{jid}/md")
    def report_md(jid: int):
        return PlainTextResponse(_report(jid, "complete_report.md"), media_type="text/markdown")

    @app.get("/portfolios")
    def portfolios():
        return {"portfolios": book.listing(store)}

    @app.post("/portfolios")
    def create_portfolio(body: PortfolioIn):
        if body.mode == "replay" and not body.replay:
            raise HTTPException(400, "reencenação precisa de período e ações")
        r = body.replay
        pid = book.create(store, body.name, body.currency, body.initial_cash, body.benchmark, body.mode,
                          r.model_dump(exclude={"api_key"}) if r else None, body.weights)
        if r:
            for t in r.tickers:
                check_currency(pid, t.upper())
            ids = replay.schedule(store, pid, [t.upper() for t in r.tickers], r.start, r.end, r.every_days,
                                  r.analysts, _model(r.model, r.api_key))
            for jid in ids:
                keys.put(jid, r.api_key)
        return {"id": pid}

    @app.get("/portfolios/{pid}")
    def portfolio(pid: int):
        p = book.get(store, pid) or _404()
        today = (p["replay"] or {}).get("end") if p["mode"] == "replay" else None
        return book.snapshot(store, pid, prices, today or _date.today().isoformat())

    @app.post("/portfolios/{pid}/run")
    def run_portfolio(pid: int, body: RunIn):
        tickers = body.tickers
        if body.watchlist_id:
            wl = store.one("select * from watchlists where id=?", (body.watchlist_id,)) or _404()
            tickers = json.loads(wl["tickers_json"])
        model = _model(body.model, body.api_key)
        day = body.date or _date.today().isoformat()
        return {"ids": [add_job(t, day, body.analysts, model, body.api_key, pid) for t in tickers]}

    @app.get("/watchlists")
    def watchlists():
        return {"watchlists": [{"id": w["id"], "name": w["name"], "tickers": json.loads(w["tickers_json"])}
                               for w in store.all("select * from watchlists order by name")]}

    @app.post("/watchlists")
    def add_watchlist(body: WatchlistIn):
        tickers = [t.strip().upper() for t in body.tickers if t.strip()]
        return {"id": store.run("insert or replace into watchlists(name, tickers_json) values (?,?)",
                                (body.name, json.dumps(tickers)))}

    @app.delete("/watchlists/{wid}")
    def delete_watchlist(wid: int):
        store.run("delete from watchlists where id=?", (wid,))
        return {"ok": True}

    @app.get("/backtests")
    def backtests():
        rows = store.all("select b.*, j.status from backtests b left join jobs j on j.id=b.job_id order by b.id desc")
        return {"backtests": [_bt(r) for r in rows]}

    @app.post("/backtests")
    def add_backtest(body: BacktestIn):
        tickers = [t.strip().upper() for t in body.tickers]
        jid = jobs.enqueue(store, "backtest", analysts=body.analysts, model=_model(body.model, body.api_key))
        keys.put(jid, body.api_key)
        bid = store.run("insert into backtests(job_id, tickers_json, start, end, every_days) values (?,?,?,?,?)",
                        (jid, json.dumps(tickers), body.start, body.end, body.every_days))
        return {"id": bid, "job_id": jid}

    @app.get("/backtests/{bid}")
    def backtest(bid: int):
        row = store.one("select b.*, j.status from backtests b left join jobs j on j.id=b.job_id where b.id=?",
                        (bid,)) or _404()
        return _bt(row)

    return app


def _bt(row: dict) -> dict:
    row["tickers"] = json.loads(row.pop("tickers_json"))
    row["summary"] = json.loads(row.pop("summary_json") or "null")
    return row


def _404():
    raise HTTPException(404, "não encontrado")
