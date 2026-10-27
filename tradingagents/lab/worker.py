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

from . import book, jobs
from .fills import PriceSource
from .llm import KeyVault, build_config, provider_key
from .runner import run_analysis
from .store import Store

logger = logging.getLogger(__name__)
FILL_EVERY_S = 3600


def _default_graph(config: dict, analysts: list[str]):
    from tradingagents.graph.trading_graph import TradingAgentsGraph

    return TradingAgentsGraph(analysts or ["market", "social", "news", "fundamentals"], config=config)


class Worker:
    def __init__(self, store: Store, data_dir: Path, *, prices: PriceSource, keys: KeyVault,
                 graph_factory=None, backtest_fn=None, base_config: dict | None = None):
        self.store, self.data_dir, self.prices, self.keys = store, Path(data_dir), prices, keys
        self.graph_factory = graph_factory or _default_graph
        self.backtest_fn = backtest_fn or run_backtest
        self.base_config = base_config or DEFAULT_CONFIG
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.current: int | None = None

    def recover(self) -> None:
        jobs.recover(self.store, self.keys.has)

    def run_once(self) -> bool:
        job = jobs.next_runnable(self.store)
        if job is None:
            return False
        self.current = job["id"]
        jobs.mark(self.store, job["id"], "running")
        try:
            with provider_key(job["model"]["provider"], self.keys.get(job["id"])):
                if job["kind"] == "backtest":
                    self._backtest(job)
                else:
                    self._analysis(job)
            self.keys.drop(job["id"])
        except Exception as exc:  # the job fails, the worker goes on
            logger.exception("job %s failed", job["id"])
            jobs.mark(self.store, job["id"], "failed", error=str(exc)[:2000])
        finally:
            self.current = None
        return True

    def _config(self, job: dict) -> dict:
        return build_config(self.base_config, job["model"], self.data_dir)

    def _analysis(self, job: dict) -> None:
        pid = job["portfolio_id"]
        graph = self.graph_factory(self._config(job), job["analysts"])
        portfolio = book.context(self.store, pid) if pid else None
        final, rating = run_analysis(graph, job["ticker"], job["trade_date"], portfolio,
                                     on_event=lambda agent: jobs.add_event(self.store, job["id"], agent, "end"))
        report_dir = self.data_dir / "reports" / str(job["id"])
        write_report_tree(final, job["ticker"], report_dir, settings=graph.run_settings())
        plan = final.get("trader_investment_plan")
        if rating == "REVIEW":
            jobs.mark(self.store, job["id"], "review", rating=rating, report_dir=str(report_dir),
                      payload={"trader_plan": plan})
            return
        jobs.mark(self.store, job["id"], "done", rating=rating, report_dir=str(report_dir),
                  payload={"trader_plan": plan})
        if pid:
            book.record_decision(self.store, pid, job["id"], job["ticker"], job["trade_date"], rating, plan)
            book.fill_pending(self.store, self.prices, pid)

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
        last_fill = 0.0
        while not self._stop.is_set():
            try:
                worked = self.run_once()
                if time.monotonic() - last_fill > FILL_EVERY_S:
                    book.fill_pending(self.store, self.prices)
                    last_fill = time.monotonic()
            except Exception:  # never let the loop die
                logger.exception("worker loop")
                worked = False
            if not worked:
                self._stop.wait(2)

    def start(self) -> None:
        self.recover()
        self._thread = threading.Thread(target=self._loop, name="lab-worker", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
