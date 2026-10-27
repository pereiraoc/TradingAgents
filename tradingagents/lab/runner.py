"""Run one analysis through the graph's stream, reporting each agent as it files."""

from __future__ import annotations

from collections.abc import Callable

from tradingagents.agents.rating import run_rating

REPORT_AGENTS = {
    "market_report": "Market Analyst", "sentiment_report": "Sentiment Analyst",
    "news_report": "News Analyst", "fundamentals_report": "Fundamentals Analyst",
    "investment_plan": "Research Manager", "trader_investment_plan": "Trader",
    "final_trade_decision": "Portfolio Manager",
}


def run_analysis(graph, ticker: str, trade_date: str, portfolio=None,
                 on_event: Callable[[str], None] | None = None) -> tuple[dict, str]:
    """Same steps as TradingAgentsGraph.propagate, through stream_run (mirrors _run_graph)."""
    final: dict = {}
    seen: set[str] = set()
    with graph.checkpoint_scope(ticker, trade_date, "stock", portfolio) as thread_id:
        init = graph.create_run_state(ticker, trade_date, "stock", portfolio)
        args = graph.propagator.get_graph_args()
        if thread_id is not None:
            args.setdefault("config", {}).setdefault("configurable", {})["thread_id"] = thread_id
        for _messages, state in graph.stream_run(graph.checkpoint_input(init), **args):
            if not state:
                continue
            final.update(state)
            for key, agent in REPORT_AGENTS.items():
                if state.get(key) and key not in seen:
                    seen.add(key)
                    if on_event:
                        on_event(agent)
        graph.record_decision(ticker, trade_date, final)
        graph.clear_checkpoint_on_success(ticker, trade_date, "stock", portfolio)
    return final, run_rating(final)
