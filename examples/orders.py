"""A shop that charges cards, end to end.

    export PGLQ_DSN=postgresql://postgres:postgres@localhost:5432/postgres
    python examples/orders.py setup        # tables + 50 orders, each with its charge job
    pglq worker examples.orders:tasks      # in one or more terminals; kill -9 them as you like
    python examples/orders.py check        # every order charged exactly once

Two ideas from the README in about forty lines:

* ``enqueue`` runs inside the order's transaction, so there is never an order without its charge
  job or a charge job without its order (no dual write, no outbox table).
* the charge is recorded with ``job.transaction()``, so a worker killed at any point cannot charge
  twice: the ledger row and "job done" commit together or not at all.
"""

from __future__ import annotations

import asyncio
import os
import random
import sys

import psycopg

import pglq

tasks = pglq.Tasks()


@tasks.task(timeout=30)
async def charge(job: pglq.Job) -> None:
    await asyncio.sleep(random.uniform(0.05, 0.3))  # the payment provider call
    if random.random() < 0.15:
        raise RuntimeError("provider timed out")  # retried with backoff
    async with job.transaction() as tx:
        await tx.execute(
            "insert into ledger (order_id, amount_cents) values (%s, %s)",
            (job.args["order_id"], job.args["amount_cents"]),
        )
        await tx.execute("update orders set paid = true where id = %s", (job.args["order_id"],))
        job.result = {"charged": job.args["amount_cents"]}


def setup(dsn: str) -> None:
    with psycopg.connect(dsn) as conn:
        pglq.migrate(conn)
        conn.execute(
            "create table if not exists orders (id serial primary key, amount_cents int, paid bool default false)"
        )
        conn.execute("create table if not exists ledger (order_id int, amount_cents int, at timestamptz default now())")
        for _ in range(50):
            amount = random.randint(500, 20_000)
            (order_id,) = conn.execute(
                "insert into orders (amount_cents) values (%s) returning id", (amount,)
            ).fetchone()
            pglq.enqueue(
                conn,
                "charge",
                {"order_id": order_id, "amount_cents": amount},
                idempotency_key=f"charge-order-{order_id}",
                max_attempts=10,
            )
        conn.commit()
    print("50 orders placed; start some workers: pglq worker examples.orders:tasks")


def check(dsn: str) -> None:
    with psycopg.connect(dsn) as conn:
        orders, paid = conn.execute("select count(*), count(*) filter (where paid) from orders").fetchone()
        twice = conn.execute("select count(*) from (select order_id from ledger group by 1 having count(*) > 1) t")
        print(f"{paid}/{orders} orders paid, {twice.fetchone()[0]} charged more than once")


if __name__ == "__main__":
    {"setup": setup, "check": check}[sys.argv[1]](os.environ["PGLQ_DSN"])
