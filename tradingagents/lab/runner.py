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
# Debates live inside nested states: (state key, history key, label).
DEBATES = (
    ("investment_debate_state", "bull_history", "Bull Researcher"),
    ("investment_debate_state", "bear_history", "Bear Researcher"),
    ("risk_debate_state", "aggressive_history", "Aggressive Analyst"),
    ("risk_debate_state", "conservative_history", "Conservative Analyst"),
    ("risk_debate_state", "neutral_history", "Neutral Analyst"),
)
EXCERPT = 600


def _filed(state: dict):
    """(label, text) for every report or debate turn present in a state chunk."""
    for key, agent in REPORT_AGENTS.items():
        if state.get(key):
            yield agent, str(state[key])
    for outer, inner, agent in DEBATES:
        text = (state.get(outer) or {}).get(inner) if isinstance(state.get(outer), dict) else None
        if text:
            yield agent, str(text)


def run_analysis(graph, ticker: str, trade_date: str, portfolio=None,
                 on_event: Callable[[str, str], None] | None = None) -> tuple[dict, str]:
    """Same steps as TradingAgentsGraph.propagate, through stream_run (mirrors _run_graph).

    ``on_event(agent, excerpt)`` fires once per agent, the first time its output appears.
    """
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
            for agent, text in _filed(state):
                if agent not in seen:
                    seen.add(agent)
                    if on_event:
                        on_event(agent, text[:EXCERPT])
        graph.record_decision(ticker, trade_date, final)
        graph.clear_checkpoint_on_success(ticker, trade_date, "stock", portfolio)
    return final, run_rating(final)
