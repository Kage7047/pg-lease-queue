from __future__ import annotations

import json
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID

import psycopg
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from pglq import sql


class LeaseLost(Exception):
    """The job's lease expired and someone else may now own it. Its result is thrown away."""

    def __init__(self, job_id: int) -> None:
        super().__init__(f"lost the lease on job {job_id}")
        self.job_id = job_id


class Retry(Exception):
    """Raise from a handler to try again after ``delay`` seconds (uses up an attempt)."""

    def __init__(self, delay: float, reason: str = "retry requested") -> None:
        super().__init__(reason)
        self.delay = delay


class Permanent(Exception):
    """Raise from a handler when retrying cannot help. The job goes straight to dead."""


def as_json(value: Any) -> Jsonb | None:
    if value is None:
        return None
    try:
        json.dumps(value)
    except (TypeError, ValueError):
        value = {"repr": repr(value)}
    return Jsonb(value)


@dataclass
class Job:
    id: int
    queue: str
    task: str
    args: dict[str, Any]
    priority: int
    attempts: int
    max_attempts: int
    lease_token: UUID
    run_at: datetime
    created_at: datetime
    pickup_delay: float = 0.0
    lost: bool = False
    completed: bool = False
    result: Any = None
    _pool: AsyncConnectionPool | None = field(default=None, repr=False)

    @property
    def last_attempt(self) -> bool:
        return self.attempts >= self.max_attempts

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[psycopg.AsyncConnection]:
        """Do the job's database writes and finish the job in one transaction.

        The job row is locked and its lease token checked first. If the lease has been lost,
        ``LeaseLost`` is raised and nothing is written. Otherwise the writes and ``status = 'done'``
        commit together, so a crash, a retry or a zombie worker can never apply them twice.
        Set ``job.result`` inside the block to store a result.
        """
        if self._pool is None:
            raise RuntimeError("job.transaction() needs a job handed out by a Worker")
        async with self._pool.connection() as conn, conn.transaction():
            ids = {"id": self.id, "token": self.lease_token}
            if await (await conn.execute(sql.LOCK_FOR_EFFECT, ids)).fetchone() is None:
                raise LeaseLost(self.id)
            yield conn
            cur = await conn.execute(sql.COMPLETE, {**ids, "result": as_json(self.result)})
            if cur.rowcount != 1:  # impossible while we hold the row lock; keep the invariant loud
                raise LeaseLost(self.id)
        self.completed = True


Handler = Callable[[Job], Awaitable[Any]] | Callable[[Job], Any]


@dataclass(frozen=True)
class TaskSpec:
    name: str
    fn: Handler
    timeout: float | None
    is_async: bool


class Tasks:
    """A registry of handlers, looked up by the job's ``task`` name."""

    def __init__(self) -> None:
        self._specs: dict[str, TaskSpec] = {}

    def task(self, name: str | None = None, *, timeout: float | None = None) -> Callable[[Handler], Handler]:
        def register(fn: Handler) -> Handler:
            import inspect

            key = name or fn.__name__
            if key in self._specs:
                raise ValueError(f"task {key!r} registered twice")
            self._specs[key] = TaskSpec(key, fn, timeout, inspect.iscoroutinefunction(fn))
            return fn

        return register

    def get(self, name: str) -> TaskSpec | None:
        return self._specs.get(name)

    def __contains__(self, name: object) -> bool:
        return name in self._specs

    @property
    def names(self) -> list[str]:
        return sorted(self._specs)
