"""The model a job runs on, and its API key, which lives only in memory."""

from __future__ import annotations

import copy
import os
import threading
from contextlib import contextmanager
from pathlib import Path

from tradingagents.llm_clients.api_key_env import get_api_key_env


class KeyVault:
    def __init__(self):
        self._keys: dict[int, str] = {}
        self._lock = threading.Lock()

    def put(self, job_id: int, key: str | None) -> None:
        if key:
            with self._lock:
                self._keys[job_id] = key

    def get(self, job_id: int) -> str | None:
        with self._lock:
            return self._keys.get(job_id)

    def has(self, job_id: int) -> bool:
        return self.get(job_id) is not None

    def drop(self, job_id: int) -> None:
        with self._lock:
            self._keys.pop(job_id, None)


def parse_model(d: dict) -> dict:
    if not d or not d.get("quick"):
        raise ValueError("model.quick é obrigatório")
    return {"provider": d.get("provider") or "openai_compatible", "base_url": d.get("base_url"),
            "quick": d["quick"], "deep": d.get("deep") or d["quick"], "needs_key": bool(d.get("needs_key"))}


def build_config(base: dict, model: dict, data_dir: Path) -> dict:
    cfg = copy.deepcopy(base)
    cfg.update(llm_provider=model["provider"], backend_url=model["base_url"],
               quick_think_llm=model["quick"], deep_think_llm=model["deep"],
               results_dir=str(data_dir / "logs"), data_cache_dir=str(data_dir / "cache"),
               memory_log_path=str(data_dir / "memory" / "trading_memory.md"), checkpoint_enabled=True)
    return cfg


@contextmanager
def provider_key(provider: str, key: str | None):
    """Expose the key to the provider client for one job, then take it back."""
    name = get_api_key_env(provider)
    if not name or not key:
        yield
        return
    old = os.environ.get(name)
    os.environ[name] = key
    try:
        yield
    finally:
        if old is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = old
