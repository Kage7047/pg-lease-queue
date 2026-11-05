"""Handlers for the chaos test. Each job pays once into ``effects``; some fail and retry."""

from __future__ import annotations

import asyncio
import os
import random

import psycopg

from pglq import Job, Tasks

tasks = Tasks()


@tasks.task()
async def pay(job: Job) -> None:
    await asyncio.sleep(random.uniform(0, 0.05))  # work before the write
    if random.random() < 0.1:
        raise RuntimeError("transient downstream error")
    async with job.transaction() as tx:
        await tx.execute("insert into effects (job_id, worker) values (%s, %s)", (job.id, job.args["tag"]))
        await asyncio.sleep(random.uniform(0, 0.03))  # widen the window a kill can land in


@tasks.task()
async def pay_naive(job: Job) -> None:
    """The control: the same write, committed on its own before the job is marked done."""
    await asyncio.sleep(random.uniform(0, 0.05))
    async with await psycopg.AsyncConnection.connect(os.environ["PGLQ_DSN"], autocommit=True) as c:
        await c.execute("insert into effects (job_id, worker) values (%s, %s)", (job.id, job.args["tag"]))
    await asyncio.sleep(random.uniform(0, 0.03))  # a kill here re-runs a job whose effect already landed
