"""Every test runs against a real Postgres: a queue's correctness lives in its SQL.

Set PGLQ_TEST_DSN to a database the tests may wipe, e.g.
    PGLQ_TEST_DSN=postgresql://postgres:postgres@localhost:5432/pglq_test
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from typing import Any

import psycopg
import pytest

from pglq import schema, sql
from pglq.worker import Worker

DSN = os.environ.get("PGLQ_TEST_DSN", "")


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if DSN:
        return
    skip = pytest.mark.skip(reason="set PGLQ_TEST_DSN to a scratch Postgres database")
    for item in items:
        if "no_db" not in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def dsn() -> str:
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("drop table if exists pglq_jobs, pglq_meta, effects")
        schema.migrate(conn)
        conn.execute("create table effects (job_id bigint not null, worker text, at timestamptz default now())")
    return DSN


@pytest.fixture
def conn(dsn: str) -> Iterator[psycopg.Connection[Any]]:
    with psycopg.connect(dsn, autocommit=True) as c:
        c.execute("truncate pglq_jobs, effects restart identity")
        yield c


def claim(conn: psycopg.Connection[Any], n: int = 1, *, lease: float = 30, worker: str = "test", queues=("default",)):
    """The worker's claim statement, run synchronously so tests can drive the protocol by hand."""
    from psycopg.rows import dict_row

    with conn.cursor(row_factory=dict_row) as cur:
        return cur.execute(sql.CLAIM, {"queues": list(queues), "n": n, "lease": lease, "worker": worker}).fetchall()


def row(conn: psycopg.Connection[Any], job_id: int) -> dict[str, Any]:
    from psycopg.rows import dict_row

    with conn.cursor(row_factory=dict_row) as cur:
        r = cur.execute("select * from pglq_jobs where id = %s", (job_id,)).fetchone()
    assert r is not None
    return r


@asynccontextmanager
async def running(worker: Worker) -> AsyncIterator[Worker]:
    task = asyncio.create_task(worker.run())
    await asyncio.wait_for(worker.wait_started(), 10)
    try:
        yield worker
    finally:
        worker.stop()
        await asyncio.wait_for(task, 30)


async def until(predicate, within: float = 10.0, every: float = 0.02) -> None:
    deadline = asyncio.get_running_loop().time() + within
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(every)
