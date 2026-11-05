from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import logging
import os
import re
import signal
import sys
from typing import Any

import psycopg

from pglq import __version__, queue, schema
from pglq.job import Tasks
from pglq.worker import Worker


def _dsn(args: argparse.Namespace) -> str:
    dsn: str | None = args.dsn or os.environ.get("PGLQ_DSN")
    if not dsn:
        sys.exit("pglq: set --dsn or PGLQ_DSN (e.g. postgresql://user:pass@localhost:5432/app)")
    return dsn


def _duration(text: str) -> float:
    """'90', '90s', '15m', '12h', '7d' -> seconds."""
    m = re.fullmatch(r"(\d+(?:\.\d+)?)([smhd]?)", text.strip())
    if not m:
        raise argparse.ArgumentTypeError(f"not a duration: {text!r} (try 30s, 15m, 12h, 7d)")
    return float(m.group(1)) * {"": 1, "s": 1, "m": 60, "h": 3600, "d": 86400}[m.group(2)]


def load_tasks(ref: str) -> Tasks:
    """'package.module:attribute' -> the Tasks registry it names."""
    module_name, _, attr = ref.partition(":")
    obj = getattr(importlib.import_module(module_name), attr or "tasks")
    if not isinstance(obj, Tasks):
        sys.exit(f"pglq: {ref} is a {type(obj).__name__}, not a pglq.Tasks registry")
    return obj


def cmd_migrate(args: argparse.Namespace) -> None:
    with psycopg.connect(_dsn(args), autocommit=True) as conn:
        applied = schema.migrate(conn)
    print(f"applied {applied}" if applied else f"up to date (v{schema.LATEST})")


def cmd_enqueue(args: argparse.Namespace) -> None:
    payload: dict[str, Any] = json.loads(args.args)
    with psycopg.connect(_dsn(args)) as conn:
        job_id = queue.enqueue(
            conn,
            args.task,
            payload,
            queue=args.queue,
            priority=args.priority,
            delay=args.delay,
            max_attempts=args.max_attempts,
            idempotency_key=args.key,
        )
    print(job_id)


def cmd_stats(args: argparse.Namespace) -> None:
    with psycopg.connect(_dsn(args), autocommit=True) as conn:
        rows = queue.stats(conn)
    if args.json:
        print(json.dumps([r.__dict__ for r in rows]))
        return
    cols = ("queue", "ready", "scheduled", "running", "done", "dead", "cancelled", "lag_seconds")
    print("  ".join(f"{c:>11}" for c in cols))
    for r in rows:
        print("  ".join(f"{getattr(r, c):>11.1f}" if c == "lag_seconds" else f"{getattr(r, c)!s:>11}" for c in cols))


def cmd_retry_dead(args: argparse.Namespace) -> None:
    with psycopg.connect(_dsn(args)) as conn:
        print(f"revived {queue.retry_dead(conn, queue=args.queue, task=args.task)} jobs")


def cmd_prune(args: argparse.Namespace) -> None:
    with psycopg.connect(_dsn(args), autocommit=True) as conn:
        print(f"deleted {queue.prune(conn, older_than=args.older_than)} finished jobs")


def cmd_worker(args: argparse.Namespace) -> None:
    sys.path.insert(0, os.getcwd())
    tasks = load_tasks(args.tasks)
    if args.metrics_port:
        from prometheus_client import start_http_server

        start_http_server(args.metrics_port)
    worker = Worker(
        _dsn(args),
        tasks,
        queues=args.queues.split(","),
        concurrency=args.concurrency,
        lease=args.lease,
        poll_interval=args.poll,
        grace=args.grace,
        listen=not args.no_listen,
        stats_interval=15.0 if args.metrics_port else None,
    )

    async def main() -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, worker.stop)
        await worker.run()

    asyncio.run(main())


def cmd_bench(args: argparse.Namespace) -> None:
    from pglq import bench

    bench.main(_dsn(args), args)


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pglq", description="A crash-safe job queue on plain Postgres.")
    p.add_argument("--version", action="version", version=f"pglq {__version__}")
    p.add_argument("--dsn", help="Postgres connection string (default: $PGLQ_DSN)")
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("migrate", help="create or upgrade the pglq_jobs table").set_defaults(fn=cmd_migrate)

    e = sub.add_parser("enqueue", help="add a job")
    e.add_argument("task")
    e.add_argument("--args", default="{}", help="JSON object passed to the handler as job.args")
    e.add_argument("--queue", default="default")
    e.add_argument("--priority", type=int, default=0)
    e.add_argument("--delay", type=_duration, default=0.0)
    e.add_argument("--max-attempts", type=int, default=5)
    e.add_argument("--key", help="idempotency key: a repeat with the same key returns the same job")
    e.set_defaults(fn=cmd_enqueue)

    s = sub.add_parser("stats", help="jobs per queue and state, and the age of the oldest ready job")
    s.add_argument("--json", action="store_true")
    s.set_defaults(fn=cmd_stats)

    r = sub.add_parser("retry-dead", help="give dead jobs a fresh set of attempts")
    r.add_argument("--queue")
    r.add_argument("--task")
    r.set_defaults(fn=cmd_retry_dead)

    pr = sub.add_parser("prune", help="delete done and cancelled jobs older than a cutoff")
    pr.add_argument("--older-than", type=_duration, default=_duration("7d"))
    pr.set_defaults(fn=cmd_prune)

    w = sub.add_parser("worker", help="run handlers from a module:attribute Tasks registry")
    w.add_argument("tasks", help="e.g. myapp.jobs:tasks")
    w.add_argument("--queues", default="default", help="comma-separated")
    w.add_argument("--concurrency", type=int, default=10)
    w.add_argument("--lease", type=_duration, default=30.0)
    w.add_argument("--poll", type=_duration, default=2.0)
    w.add_argument("--grace", type=_duration, default=30.0)
    w.add_argument("--no-listen", action="store_true", help="poll only, no LISTEN/NOTIFY")
    w.add_argument("--metrics-port", type=int, help="serve Prometheus metrics on this port")
    w.set_defaults(fn=cmd_worker)

    b = sub.add_parser("bench", help="throughput and pickup-latency benchmarks (uses a scratch queue)")
    b.add_argument("--jobs", type=int, default=20_000)
    b.add_argument("--workers", default="1,2,4,8", help="worker process counts to try")
    b.add_argument("--concurrency", type=int, default=16)
    b.add_argument("--latency-samples", type=int, default=200)
    b.add_argument("--repeat", type=int, default=3, help="runs per worker count; the median is reported")
    b.set_defaults(fn=cmd_bench)
    return p


def main(argv: list[str] | None = None) -> None:
    args = parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args.fn(args)
