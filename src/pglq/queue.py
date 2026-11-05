"""Producer and admin functions. They take a connection, so they join the caller's transaction.

That is the main reason to keep a queue in the same database as the data: ``enqueue`` inside the
transaction that creates the order means the job exists if and only if the order does. No
dual-write, no outbox table.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from pglq import sql


@dataclass(frozen=True)
class QueueStats:
    queue: str
    ready: int
    scheduled: int
    running: int
    done: int
    dead: int
    cancelled: int
    lag_seconds: float


def _params(
    task: str,
    args: Mapping[str, Any] | None,
    queue: str,
    priority: int,
    delay: float,
    max_attempts: int,
    key: str | None,
) -> dict[str, Any]:
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")
    if not -32768 <= priority <= 32767:
        raise ValueError("priority must fit in a smallint")
    return {
        "task": task,
        "args": Jsonb(dict(args or {})),
        "queue": queue,
        "priority": priority,
        "delay": max(delay, 0.0),
        "max_attempts": max_attempts,
        "key": key,
    }


def enqueue(
    conn: psycopg.Connection[Any],
    task: str,
    args: Mapping[str, Any] | None = None,
    *,
    queue: str = "default",
    priority: int = 0,
    delay: float = 0.0,
    max_attempts: int = 5,
    idempotency_key: str | None = None,
) -> int:
    """Add a job; return its id. With an ``idempotency_key``, a repeat returns the existing id.

    Runs in the connection's current transaction. Workers are woken on commit.
    """
    p = _params(task, args, queue, priority, delay, max_attempts, idempotency_key)
    row = conn.execute(sql.ENQUEUE, p).fetchone()
    if row is not None:
        conn.execute(sql.NOTIFY, (queue,))
        return int(row[0])
    existing = conn.execute(sql.EXISTING, p).fetchone()
    if existing is None:  # pragma: no cover - only if the winner rolled back between statements
        raise RuntimeError(f"idempotent enqueue of {idempotency_key!r} found no job; retry the call")
    return int(existing[0])


async def aenqueue(
    conn: psycopg.AsyncConnection[Any],
    task: str,
    args: Mapping[str, Any] | None = None,
    *,
    queue: str = "default",
    priority: int = 0,
    delay: float = 0.0,
    max_attempts: int = 5,
    idempotency_key: str | None = None,
) -> int:
    """``enqueue`` for asyncio code."""
    p = _params(task, args, queue, priority, delay, max_attempts, idempotency_key)
    row = await (await conn.execute(sql.ENQUEUE, p)).fetchone()
    if row is not None:
        await conn.execute(sql.NOTIFY, (queue,))
        return int(row[0])
    existing = await (await conn.execute(sql.EXISTING, p)).fetchone()
    if existing is None:  # pragma: no cover
        raise RuntimeError(f"idempotent enqueue of {idempotency_key!r} found no job; retry the call")
    return int(existing[0])


def enqueue_many(
    conn: psycopg.Connection[Any],
    jobs: Iterable[tuple[str, Mapping[str, Any]]],
    *,
    queue: str = "default",
    priority: int = 0,
    delay: float = 0.0,
    max_attempts: int = 5,
) -> list[int]:
    """Bulk insert in one statement: ``[(task, args), ...]``. Returns the new ids."""
    items = list(jobs)
    if not items:
        return []
    p = _params("", None, queue, priority, delay, max_attempts, None)
    p["tasks"] = [t for t, _ in items]
    p["args"] = [Jsonb(dict(a)) for _, a in items]
    ids = [int(r[0]) for r in conn.execute(sql.ENQUEUE_MANY, p).fetchall()]
    conn.execute(sql.NOTIFY, (queue,))
    return ids


def stats(conn: psycopg.Connection[Any]) -> list[QueueStats]:
    with conn.cursor(row_factory=dict_row) as cur:
        return [QueueStats(**r) for r in cur.execute(sql.STATS).fetchall()]


def get(conn: psycopg.Connection[Any], job_id: int) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        return cur.execute("select * from pglq_jobs where id = %s", (job_id,)).fetchone()


def retry_dead(conn: psycopg.Connection[Any], *, queue: str | None = None, task: str | None = None) -> int:
    """Give dead jobs a fresh set of attempts. Returns how many were revived."""
    rows = conn.execute(sql.RETRY_DEAD, {"queue": queue, "task": task}).fetchall()
    for q, _ in rows:
        conn.execute(sql.NOTIFY, (q,))
    return sum(int(n) for _, n in rows)


def cancel(conn: psycopg.Connection[Any], job_id: int) -> bool:
    """Cancel a job that has not started. Running jobs are left alone."""
    return conn.execute(sql.CANCEL, {"id": job_id}).rowcount == 1


def prune(conn: psycopg.Connection[Any], *, older_than: float, batch: int = 10_000) -> int:
    """Delete finished (done or cancelled) jobs older than ``older_than`` seconds, in batches.

    Dead jobs are kept: they are the record of what needs a human.
    """
    total = 0
    while True:
        n = conn.execute(sql.PRUNE, {"age": older_than, "limit": batch}).rowcount
        total += n
        if n < batch:
            return total
