"""Benchmarks: drain throughput by worker count, and pickup latency with and without LISTEN.

Everything runs on a private queue name and is deleted afterwards, so it is safe to point at a
database that has real jobs (it will compete with them for CPU, of course).
"""

from __future__ import annotations

import argparse
import os
import platform
import statistics
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager

import psycopg

from pglq import queue, schema
from pglq.job import Job, Tasks

QUEUE = "pglq-bench"
tasks = Tasks()


@tasks.task("noop")
async def noop(job: Job) -> None:
    return None


@contextmanager
def workers(dsn: str, n: int, concurrency: int, *extra: str) -> Iterator[None]:
    # the DSN may hold a password: pass it in the environment, not in argv where `ps` shows it
    cmd = [sys.executable, "-m", "pglq", "worker", "pglq.bench:tasks", "--queues", QUEUE,
           "--concurrency", str(concurrency), *extra]  # fmt: skip
    env = {**os.environ, "PGLQ_DSN": dsn}
    procs = [subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env) for _ in range(n)]
    try:
        yield
    finally:
        for p in procs:
            p.terminate()
        for p in procs:
            p.wait(timeout=30)


def _clear(conn: psycopg.Connection) -> None:
    conn.execute("delete from pglq_jobs where queue = %s", (QUEUE,))


def _wait_done(conn: psycopg.Connection, n: int, timeout: float = 600) -> float:
    t0 = time.perf_counter()
    while time.perf_counter() - t0 < timeout:
        row = conn.execute("select count(*) from pglq_jobs where queue = %s and status = 'done'", (QUEUE,)).fetchone()
        if row and row[0] >= n:
            return time.perf_counter()
        time.sleep(0.02)
    raise TimeoutError(f"jobs not drained within {timeout}s")


def throughput(dsn: str, n_jobs: int, n_workers: int, concurrency: int) -> float:
    with psycopg.connect(dsn, autocommit=True) as conn, workers(dsn, n_workers, concurrency):
        _clear(conn)
        warm = n_workers * concurrency  # make sure every worker is up and connected
        queue.enqueue_many(conn, [("noop", {})] * warm, queue=QUEUE)
        _wait_done(conn, warm)
        _clear(conn)
        with conn.transaction():
            queue.enqueue_many(conn, [("noop", {})] * n_jobs, queue=QUEUE)
        t0 = time.perf_counter()
        t1 = _wait_done(conn, n_jobs)
        _clear(conn)
    return n_jobs / (t1 - t0)


def latency(dsn: str, samples: int, listen: bool) -> tuple[float, float, float]:
    extra = ("--poll", "1") if listen else ("--poll", "1", "--no-listen")
    with psycopg.connect(dsn, autocommit=True) as conn, workers(dsn, 1, 4, *extra):
        _clear(conn)
        queue.enqueue(conn, "noop", queue=QUEUE)
        _wait_done(conn, 1)
        _clear(conn)
        for _ in range(samples):
            queue.enqueue(conn, "noop", queue=QUEUE)
            time.sleep(0.02)
        _wait_done(conn, samples)
        row = conn.execute(
            """
            select percentile_cont(array[0.5, 0.95, 0.99]) within group
                   (order by extract(epoch from started_at - run_at))
            from pglq_jobs where queue = %s
            """,
            (QUEUE,),
        ).fetchone()
        _clear(conn)
    assert row is not None
    p50, p95, p99 = (float(x) * 1000 for x in row[0])
    return p50, p95, p99


def main(dsn: str, args: argparse.Namespace) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        schema.migrate(conn)
        version = conn.execute("show server_version").fetchone()
    print(f"# pglq bench: {args.jobs} no-op jobs, concurrency {args.concurrency} per worker process")
    print(f"Postgres {version[0] if version else '?'}, Python {platform.python_version()}, {os.cpu_count()} CPUs,"
          f" {platform.processor() or platform.machine()}\n")  # fmt: skip
    print(f"| worker processes | jobs/s (median of {args.repeat}) | runs |\n|---:|---:|---|")
    for n in (int(x) for x in args.workers.split(",")):
        runs = sorted(throughput(dsn, args.jobs, n, args.concurrency) for _ in range(args.repeat))
        spread = ", ".join(f"{r:,.0f}" for r in runs)
        print(f"| {n} | {statistics.median(runs):,.0f} | {spread} |", flush=True)
    print("\n| wake-up | p50 ms | p95 ms | p99 ms |\n|---|---:|---:|---:|")
    for listen in (True, False):
        p50, p95, p99 = latency(dsn, args.latency_samples, listen)
        label = "LISTEN/NOTIFY" if listen else "poll every 1s"
        print(f"| {label} | {p50:.1f} | {p95:.1f} | {p99:.1f} |", flush=True)
