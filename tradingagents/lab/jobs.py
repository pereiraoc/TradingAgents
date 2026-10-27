"""The run queue: one job at a time, in order, a chained job after its predecessor."""

from __future__ import annotations

import json
from collections.abc import Callable

from .store import Store, now

KEY_LOST = "chave perdida no reinício — reenvie"


def _decode(row: dict | None) -> dict | None:
    if row is None:
        return None
    row["model"] = json.loads(row.pop("model_json") or "null")
    row["payload"] = json.loads(row.pop("payload_json") or "null")
    row["analysts"] = json.loads(row["analysts"] or "[]")
    return row


def enqueue(store: Store, kind: str, *, ticker=None, trade_date=None, analysts=(), model=None,
            portfolio_id=None, after_id=None, payload=None) -> int:
    return store.run(
        "insert into jobs(kind, ticker, trade_date, analysts, model_json, portfolio_id, after_id,"
        " payload_json, created_at) values (?,?,?,?,?,?,?,?,?)",
        (kind, ticker, trade_date, json.dumps(list(analysts)), json.dumps(model),
         portfolio_id, after_id, json.dumps(payload), now()))


def get(store: Store, job_id: int) -> dict | None:
    return _decode(store.one("select * from jobs where id=?", (job_id,)))


def listing(store: Store, status: str | None = None, limit: int = 100) -> list[dict]:
    if status:
        rows = store.all("select * from jobs where status=? order by id desc limit ?", (status, limit))
    else:
        rows = store.all("select * from jobs order by id desc limit ?", (limit,))
    return [_decode(r) for r in rows]


def next_runnable(store: Store) -> dict | None:
    if store.one("select id from jobs where status='running'"):
        return None
    return _decode(store.one(
        "select * from jobs j where status='queued' and (after_id is null or exists("
        " select 1 from jobs a where a.id=j.after_id and a.status in ('done','review')))"
        " order by id limit 1"))


def mark(store: Store, job_id: int, status: str, **fields) -> None:
    if status == "running":
        fields.setdefault("started_at", now())
    if status in ("done", "failed", "review"):
        fields.setdefault("finished_at", now())
    if "payload" in fields:
        fields["payload_json"] = json.dumps(fields.pop("payload"))
    cols = ", ".join(f"{k}=?" for k in ("status", *fields))
    store.run(f"update jobs set {cols} where id=?", (status, *fields.values(), job_id))


def add_event(store: Store, job_id: int, agent: str, phase: str, note: str = "") -> None:
    store.run("insert into job_events values (?,?,?,?,?)", (job_id, now(), agent, phase, note))


def events(store: Store, job_id: int) -> list[dict]:
    return store.all("select ts, agent, phase, note from job_events where job_id=? order by rowid", (job_id,))


def recover(store: Store, has_key: Callable[[int], bool]) -> None:
    """After a restart: running jobs go back to the queue; keyed jobs whose key died fail."""
    for job in (_decode(r) for r in store.all("select * from jobs where status in ('queued','running')")):
        if (job["model"] or {}).get("needs_key") and not has_key(job["id"]):
            mark(store, job["id"], "failed", error=KEY_LOST)
        elif job["status"] == "running":
            store.run("update jobs set status='queued', started_at=null where id=?", (job["id"],))
