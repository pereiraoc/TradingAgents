"""One thread, one job at a time."""

from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path

from tradingagents.backtest import iter_grid, run_backtest, summarize
from tradingagents.default_config import DEFAULT_CONFIG
from tradingagents.reporting import write_report_tree

from . import autopilot, book, jobs, radar
from .fills import PriceSource
from .llm import KeyVault, build_config, provider_key, scrub
from .runner import run_analysis
from .store import Store

logger = logging.getLogger(__name__)
FILL_EVERY_S = 3600
COLLECT_EVERY_S = 3600
AUTO_EVERY_S = 60
MODEL_POLL_S = 15
MODEL_WAIT_MAX_S = 2 * 3600


def _default_model_up(model: dict, key: str | None) -> bool:
    """An OpenAI-compatible server answers /models once it is serving."""
    base = (model or {}).get("base_url")
    if not base:
        return True
    import requests

    try:
        headers = {"Authorization": f"Bearer {key}"} if key else {}
        return requests.get(base.rstrip("/") + "/models", headers=headers, timeout=5).status_code < 500
    except requests.RequestException:
        return False


def _default_ask(config: dict):
    from tradingagents.llm_clients.factory import create_tier_client

    llm = create_tier_client(config, "quick").get_llm()
    return lambda prompt: llm.invoke(prompt).content


def _default_graph(config: dict, analysts: list[str]):
    from tradingagents.graph.trading_graph import TradingAgentsGraph

    return TradingAgentsGraph(analysts or ["market", "social", "news", "fundamentals"], config=config)


class Worker:
    def __init__(self, store: Store, data_dir: Path, *, prices: PriceSource, keys: KeyVault,
                 graph_factory=None, backtest_fn=None, base_config: dict | None = None, model_up=None,
                 ask_factory=None, collect_fn=None, clock=None):
        self.store, self.data_dir, self.prices, self.keys = store, Path(data_dir), prices, keys
        self.graph_factory = graph_factory or _default_graph
        self.backtest_fn = backtest_fn or run_backtest
        self.base_config = base_config or DEFAULT_CONFIG
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.current: int | None = None
        self.model_up = model_up or _default_model_up
        self.ask_factory = ask_factory or _default_ask
        self.collect_fn = collect_fn or radar.collect
        self._symbols: dict[str, bool] = {}
        self.waiting: int | None = None
        self._wait_since: dict[int, float] = {}
        self._last_probe: dict[int, float] = {}
        self._last_fill = self._last_collect = self._last_auto = -float("inf")
        self.clock = clock or autopilot.now_local

    def recover(self) -> None:
        jobs.recover(self.store, self.keys.has)

    def run_once(self) -> bool:
        stale = self.store.one("select id from jobs where status='running'")
        if stale and self.current is None:  # the failure handler itself died: don't block the queue
            jobs.mark(self.store, stale["id"], "failed", error="interrompido — use 'tentar de novo'")
        job = jobs.next_runnable(self.store)
        if job is None or not self._model_ready(job):
            return False
        self.current = job["id"]
        jobs.mark(self.store, job["id"], "running")
        try:
            with provider_key(job["model"]["provider"], self.keys.get(job["id"])):
                if job["kind"] == "backtest":
                    self._backtest(job)
                elif job["kind"] == "radar":
                    self._radar(job)
                else:
                    self._analysis(job)
            self.keys.drop(job["id"])
        except Exception as exc:  # the job fails, the worker goes on
            error = scrub(str(exc), self.keys.get(job["id"]))[:2000]
            logger.error("job %s failed: %s", job["id"], error)
            jobs.mark(self.store, job["id"], "failed", error=error)
        finally:
            self.current = None
        return True

    def _model_ready(self, job: dict) -> bool:
        """A job whose model is not serving yet waits in the queue (up to MODEL_WAIT_MAX_S)."""
        jid, now = job["id"], time.monotonic()
        if now - self._last_probe.get(jid, -MODEL_POLL_S) < MODEL_POLL_S:
            return False
        self._last_probe[jid] = now
        if self.model_up(job["model"], self.keys.get(jid)):
            self._wait_since.pop(jid, None)
            self._last_probe.pop(jid, None)
            self.waiting = None
            return True
        if jid not in self._wait_since:
            self._wait_since[jid] = now
            jobs.add_event(self.store, jid, "Modelo", "wait",
                           f"esperando {job['model'].get('quick')} responder (ligue no Cookbook)")
        self.waiting = jid
        if now - self._wait_since[jid] > MODEL_WAIT_MAX_S:
            jobs.mark(self.store, jid, "failed",
                      error="o modelo não respondeu em 2 h — ligue-o no Cookbook e use 'tentar de novo'")
            self._wait_since.pop(jid, None)
            self.waiting = None
        return False

    def _config(self, job: dict) -> dict:
        return build_config(self.base_config, job["model"], self.data_dir)

    def _analysis(self, job: dict) -> None:
        pid = job["portfolio_id"]
        graph = self.graph_factory(self._config(job), job["analysts"])
        portfolio = book.context(self.store, pid) if pid else None
        final, rating = run_analysis(graph, job["ticker"], job["trade_date"], portfolio,
                                     on_event=lambda agent, text: jobs.add_event(self.store, job["id"], agent, "end", text))
        report_dir = self.data_dir / "reports" / str(job["id"])
        write_report_tree(final, job["ticker"], report_dir, settings=graph.run_settings())
        plan = final.get("trader_investment_plan")
        if rating == "REVIEW":
            jobs.mark(self.store, job["id"], "review", rating=rating, report_dir=str(report_dir),
                      payload={"trader_plan": plan})
            return
        if pid:  # before 'done': a crash in between must not leave a finished job without its trade
            book.record_decision(self.store, pid, job["id"], job["ticker"], job["trade_date"], rating, plan)
        jobs.mark(self.store, job["id"], "done", rating=rating, report_dir=str(report_dir),
                  payload={"trader_plan": plan})
        if pid:
            book.fill_pending(self.store, self.prices, pid)   # per-trade guarded; the hourly sweep retries

    def _symbol_ok(self, ticker: str) -> bool:
        if ticker not in self._symbols:
            self._symbols[ticker] = self.prices.currency(ticker) is not None
        return self._symbols[ticker]

    def _radar(self, job: dict) -> None:
        """Read pending signals with the job's model, then queue the top picks in the radar portfolio."""
        stored = radar.process(self.store, self.ask_factory(self._config(job)), self._symbol_ok)
        jobs.add_event(self.store, job["id"], "Radar", "end", f"{stored} menções a ações encontradas")
        picks = radar.pick(self.store, self.prices.currency)
        ids = radar.enqueue_picks(self.store, picks, job["model"], job["analysts"], time.strftime("%Y-%m-%d"))
        for jid in ids:
            self.keys.put(jid, self.keys.get(job["id"]))
        jobs.mark(self.store, job["id"], "done", payload={"hits": stored, "picks": picks, "jobs": ids})

    def _backtest(self, job: dict) -> None:
        bt = self.store.one("select * from backtests where job_id=?", (job["id"],))
        tickers = json.loads(bt["tickers_json"])
        dates = iter_grid(bt["start"], bt["end"], bt["every_days"])
        result = self.backtest_fn(
            tickers, dates, self._config(job), selected_analysts=job["analysts"] or
            ("market", "social", "news", "fundamentals"), run_id=f"lab{bt['id']}",
            progress=lambda done, total, t, d: jobs.add_event(self.store, job["id"], f"{t} {d}", "start",
                                                             f"{done}/{total}"))
        try:
            summary = summarize(result)
            data = {"text": summary.render(), "resolved": summary.resolved, "pending": summary.pending,
                    "by_rating": {k: vars(v) for k, v in summary.by_rating.items()}}
        except FileNotFoundError:  # every cell failed: no log to score
            data = {"text": "nenhuma célula concluída"}
        data["failures"] = result.failures
        self.store.run("update backtests set summary_json=? where id=?", (json.dumps(data), bt["id"]))
        jobs.mark(self.store, job["id"], "done")

    def _loop(self) -> None:
        while not self._stop.is_set():
            worked = False
            for step in (self.run_once, self._hourly_fill, self._hourly_collect, self._auto_rounds):
                try:
                    worked = step() or worked
                except Exception:  # never let the loop die, nor one chore starve the others
                    logger.exception("worker loop: %s", step.__name__)
            if not worked:
                self._stop.wait(2)

    def _hourly_fill(self) -> bool:
        if time.monotonic() - self._last_fill > FILL_EVERY_S:
            self._last_fill = time.monotonic()   # set first: a failing feed waits an hour, not 2 s
            book.fill_pending(self.store, self.prices)
        return False

    def _hourly_collect(self) -> bool:
        if time.monotonic() - self._last_collect > COLLECT_EVERY_S:
            self._last_collect = time.monotonic()
            self.collect_fn(self.store)
        return False

    def _auto_rounds(self) -> bool:
        """Automatic portfolios whose round is due start it, but only while their model is serving."""
        if time.monotonic() - self._last_auto < AUTO_EVERY_S:
            return False
        self._last_auto = time.monotonic()
        at, queued = self.clock(), False
        for p in book.listing(self.store):
            auto = p["auto"] and autopilot.get(self.store, p["id"])
            if p["mode"] != "live" or not autopilot.due(auto, at) or not self.model_up(auto["model"], None):
                continue
            try:
                queued = bool(autopilot.run_round(self.store, p["id"], self.prices.currency, at)) or queued
            except Exception:  # one portfolio's round must not stop the others
                logger.exception("automatic round of portfolio %s failed", p["id"])
        return queued

    def start(self) -> None:
        self.recover()
        self._thread = threading.Thread(target=self._loop, name="lab-worker", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
