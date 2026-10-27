import os
from contextlib import contextmanager

from tradingagents.lab.llm import build_config, parse_model, provider_key
from tradingagents.lab.runner import run_analysis


class FakePropagator:
    def get_graph_args(self):
        return {"config": {}}


class FakeGraph:
    def __init__(self):
        self.propagator, self.recorded, self.cleared, self.args = FakePropagator(), None, False, None

    @contextmanager
    def checkpoint_scope(self, ticker, date, asset_type, portfolio):
        yield "tid-1"

    def create_run_state(self, ticker, date, asset_type, portfolio):
        return {"company_of_interest": ticker}

    def checkpoint_input(self, init):
        return init

    def stream_run(self, graph_input, **args):
        self.args = args
        yield [], {"market_report": "m"}
        yield [], None
        yield [], {"investment_debate_state": {"bull_history": "bull case"}}
        yield [], {"market_report": "m", "final_trade_decision": "**Rating**: Buy", "trader_investment_plan": "p"}

    def record_decision(self, ticker, date, final):
        self.recorded = final

    def clear_checkpoint_on_success(self, *a):
        self.cleared = True


def test_run_analysis_streams_events_and_rating():
    g, seen = FakeGraph(), []
    final, rating = run_analysis(g, "NVDA", "2026-06-01", on_event=lambda a, t: seen.append((a, t)))
    assert seen == [("Market Analyst", "m"), ("Bull Researcher", "bull case"), ("Trader", "p"),
                    ("Portfolio Manager", "**Rating**: Buy")]
    assert rating == "Buy" and g.recorded is final and g.cleared
    assert g.args["config"]["configurable"]["thread_id"] == "tid-1"


def test_parse_model_defaults():
    m = parse_model({"base_url": "http://odysseus:8000/v1", "quick": "qwen"})
    assert m == {"provider": "openai_compatible", "base_url": "http://odysseus:8000/v1",
                 "quick": "qwen", "deep": "qwen", "needs_key": False}


def test_build_config(tmp_path):
    cfg = build_config({"llm_provider": "openai"}, parse_model({"quick": "a", "deep": "b", "base_url": "u"}), tmp_path)
    assert cfg["llm_provider"] == "openai_compatible" and cfg["deep_think_llm"] == "b"
    assert cfg["checkpoint_enabled"] is True and cfg["results_dir"] == str(tmp_path / "logs")


def test_provider_key_restores_env():
    os.environ.pop("OPENAI_COMPATIBLE_API_KEY", None)
    with provider_key("openai_compatible", "sk-x"):
        assert os.environ["OPENAI_COMPATIBLE_API_KEY"] == "sk-x"
    assert "OPENAI_COMPATIBLE_API_KEY" not in os.environ
