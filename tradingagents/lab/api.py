"""HTTP API of the lab. No auth of its own: reachable only on the compose network."""

from __future__ import annotations

import json
from datetime import date as _date
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field

from tradingagents import __version__

from . import autopilot, book, jobs, radar, replay
from .fills import PriceSource
from .llm import KeyVault, parse_model
from .store import Store
from .symbols import normalize


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


class AutoIn(BaseModel):
    enabled: bool | None = None
    tickers: list[str] | None = None
    radar_top: int | None = Field(None, ge=0, le=10)
    max_positions: int | None = Field(None, ge=1, le=30)
    hour: int | None = Field(None, ge=0, le=23)
    analysts: list[str] | None = None
    model: dict | None = None
    api_key: str | None = None
    run_now: bool = False


class PortfolioIn(BaseModel):
    name: str
    currency: str
    initial_cash: float
    benchmark: str | None = None
    weights: dict | None = None
    mode: str = "live"
    replay: ReplayIn | None = None
    auto: AutoIn | None = None


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


class RadarRunIn(BaseModel):
    model: dict
    api_key: str | None = None
    analysts: list[str] = []


class FigureIn(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    lang: Literal["en", "pt"] = "en"


class RadarConfigIn(BaseModel):
    figures: list[FigureIn] | None = None
    sources: dict[str, bool] | None = None
    portfolio_id: int | None = None
    top_n: int | None = Field(None, ge=1, le=20)
    min_score: float | None = Field(None, ge=0)
    cooldown_days: int | None = Field(None, ge=0, le=365)


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

    @app.exception_handler(RequestValidationError)
    def invalid(_: Request, exc: RequestValidationError):
        # FastAPI's default echoes the request body ("input"), which can carry api_key
        return JSONResponse({"detail": [{"loc": e.get("loc"), "msg": e.get("msg")} for e in exc.errors()]},
                            status_code=422)

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
        ticker = normalize(ticker)
        check_currency(pid, ticker)
        jid = jobs.enqueue(store, "analysis", ticker=ticker, trade_date=day, analysts=analysts,
                           model=model, portfolio_id=pid)
        keys.put(jid, api_key)
        return jid

    @app.get("/health")
    def health():
        return {"ok": True, "version": __version__, "running": worker.current,
                "waiting_model": getattr(worker, "waiting", None),
                "queued": len(jobs.listing(store, "queued", 10_000))}

    @app.post("/jobs")
    def create_job(body: JobIn):
        return {"id": add_job(body.ticker, body.date, body.analysts, _model(body.model, body.api_key),
                              body.api_key, body.portfolio_id)}

    @app.get("/jobs")
    def list_jobs(status: str | None = None, limit: int = Query(100, ge=1, le=1000)):
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
        if r:  # validate everything before the portfolio exists: a refusal leaves nothing behind
            tickers = [normalize(t) for t in r.tickers]
            model = _model(r.model, r.api_key)
            for t in tickers:
                cur = prices.currency(t)
                if cur and cur.upper() != body.currency.upper():
                    raise HTTPException(400, f"{t} é cotado em {cur}; a carteira é em {body.currency.upper()}")
        auto = None
        if body.auto:
            if body.mode != "live":
                raise HTTPException(400, "só carteira ao vivo pode ser automática")
            auto = auto_patch(body.auto, body.currency.upper())
            if not auto.get("model"):
                raise HTTPException(400, "escolha o modelo da carteira automática")
        pid = book.create(store, body.name, body.currency, body.initial_cash, body.benchmark, body.mode,
                          r.model_dump(exclude={"api_key"}) if r else None, body.weights)
        if auto:
            autopilot.save(store, pid, auto)
        if r:
            ids = replay.schedule(store, pid, tickers, r.start, r.end, r.every_days, r.analysts, model)
            for jid in ids:
                keys.put(jid, r.api_key)
        return {"id": pid}

    @app.get("/portfolios/{pid}")
    def portfolio(pid: int):
        p = book.get(store, pid) or _404()
        today = (p["replay"] or {}).get("end") if p["mode"] == "replay" else None
        snap = book.snapshot(store, pid, prices, today or _date.today().isoformat())
        if p["auto"]:
            auto = autopilot.get(store, pid)
            snap["auto"] = {**auto, "next": autopilot.next_slot(autopilot.now_local(), auto["hour"])
                            if auto["last_round"] else "agora", "model_up": worker.model_up(auto["model"], None)
                            if auto["enabled"] and auto["model"] else None}
        return snap

    def auto_patch(body: AutoIn, currency: str) -> dict:
        """Checked before anything is saved; the API key is refused, never kept."""
        if body.api_key:
            raise HTTPException(400, "carteira automática só com modelo local: a chave de API não fica guardada")
        patch = body.model_dump(exclude={"api_key", "run_now"}, exclude_none=True)
        if "model" in patch:
            patch["model"] = _model(patch["model"], None)
        if "tickers" in patch:
            patch["tickers"] = list(dict.fromkeys(normalize(t) for t in patch["tickers"] if t.strip()))
            for t in patch["tickers"]:
                cur = prices.currency(t)
                if cur and cur.upper() != currency:
                    raise HTTPException(400, f"{t} é cotado em {cur}; a carteira é em {currency}")
        if body.run_now:
            patch["last_round"] = None
        return patch

    @app.post("/portfolios/{pid}/auto")
    def set_auto(pid: int, body: AutoIn):
        p = book.get(store, pid) or _404()
        if p["mode"] != "live":
            raise HTTPException(400, "só carteira ao vivo pode ser automática")
        auto = autopilot.save(store, pid, auto_patch(body, p["currency"]))
        if auto["enabled"] and not auto["model"]:
            raise HTTPException(400, "escolha o modelo da carteira automática")
        return {"auto": auto}

    @app.delete("/portfolios/{pid}")
    def delete_portfolio(pid: int):
        book.get(store, pid) or _404()
        for j in store.all("select id from jobs where portfolio_id=? and status='queued'", (pid,)):
            keys.drop(j["id"])
        store.run("delete from jobs where portfolio_id=? and status='queued'", (pid,))
        store.run("update jobs set portfolio_id=null where portfolio_id=?", (pid,))   # reports stay
        store.run("delete from trades where portfolio_id=?", (pid,))
        store.run("delete from portfolios where id=?", (pid,))
        if radar.get_config(store).get("portfolio_id") == pid:
            radar.set_config(store, {"portfolio_id": None})
        return {"ok": True}

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
        tickers = [normalize(t) for t in body.tickers if t.strip()]
        store.run("insert into watchlists(name, tickers_json) values (?,?)"
                  " on conflict(name) do update set tickers_json=excluded.tickers_json", (body.name, json.dumps(tickers)))
        return {"id": store.one("select id from watchlists where name=?", (body.name,))["id"]}   # id kept on update

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
        tickers = [normalize(t) for t in body.tickers]
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

    @app.get("/search")
    def search(q: str):
        from tradingagents.dataflows.vendors.yahoo.market import search_symbols

        q = q.strip()
        if len(q) < 2:
            return {"results": []}
        results = search_symbols(q)
        guess = normalize(q)
        if guess != q.upper() and not any(r["symbol"] == guess for r in results):
            results.insert(0, {"symbol": guess, "name": "código da B3", "exchange": "São Paulo", "type": "EQUITY"})
        return {"results": results}

    @app.get("/radar")
    def radar_view(days: int = Query(7, ge=1, le=90)):
        signals = store.all("select * from radar_signals order by ts desc limit 80")
        for sig in signals:
            sig["hits"] = store.all("select ticker, direction, strength, reason from radar_hits where signal_id=?",
                                    (sig["id"],))
        pending = store.one("select count(*) n from radar_signals where processed=0")["n"]
        return {"ranking": radar.ranking(store, days=days), "signals": signals, "pending": pending,
                "config": radar.get_config(store)}

    @app.post("/radar/config")
    def radar_config(patch: RadarConfigIn):
        data = patch.model_dump(exclude_unset=True)
        if data.get("portfolio_id") is not None and not book.get(store, data["portfolio_id"]):
            raise HTTPException(404, "carteira não existe")
        for k in ("figures", "sources", "top_n", "min_score", "cooldown_days"):
            if k in data and data[k] is None:
                data.pop(k)
        return radar.set_config(store, data)

    @app.post("/radar/collect")
    def radar_collect():
        return {"report": radar.collect(store)}

    @app.post("/radar/run")
    def radar_run(body: RadarRunIn):
        jid = jobs.enqueue(store, "radar", analysts=body.analysts, model=_model(body.model, body.api_key))
        keys.put(jid, body.api_key)
        return {"id": jid}

    return app


def _bt(row: dict) -> dict:
    row["tickers"] = json.loads(row.pop("tickers_json"))
    row["summary"] = json.loads(row.pop("summary_json") or "null")
    return row


def _404():
    raise HTTPException(404, "não encontrado")
