from __future__ import annotations

import argparse
import json

import pytest

from pglq import cli
from tests.conftest import claim


def run(dsn: str, *argv: str) -> None:
    cli.main(["--dsn", dsn, *argv])


def test_enqueue_stats_retry_and_prune(dsn, conn, capsys):
    run(dsn, "migrate")
    assert "up to date" in capsys.readouterr().out
    run(dsn, "enqueue", "email", "--args", '{"to": "x@example.com"}', "--priority", "2", "--key", "k1")
    first = capsys.readouterr().out.strip()
    run(dsn, "enqueue", "email", "--key", "k1")
    assert capsys.readouterr().out.strip() == first  # same idempotency key, same job
    run(dsn, "enqueue", "report", "--delay", "15m", "--max-attempts", "1")
    capsys.readouterr()

    run(dsn, "stats", "--json")
    (s,) = json.loads(capsys.readouterr().out)
    assert (s["queue"], s["ready"], s["scheduled"]) == ("default", 1, 1)
    run(dsn, "stats")
    assert "lag_seconds" in capsys.readouterr().out

    (j,) = claim(conn)
    conn.execute("update pglq_jobs set status = 'dead', lease_token = null, lease_until = null where id = %s",
                 (j["id"],))  # fmt: skip
    run(dsn, "retry-dead", "--queue", "default")
    assert "revived 1" in capsys.readouterr().out
    run(dsn, "prune", "--older-than", "1d")
    assert "deleted 0" in capsys.readouterr().out


def test_durations():
    assert [cli._duration(x) for x in ("90", "90s", "1.5m", "2h", "7d")] == [90, 90, 90, 7200, 604800]
    with pytest.raises(argparse.ArgumentTypeError):
        cli._duration("soon")


def test_load_tasks():
    assert "pay" in cli.load_tasks("tests.chaos_tasks:tasks")
    with pytest.raises(SystemExit):
        cli.load_tasks("tests.chaos_tasks:random")  # a module attribute that is not a registry


def test_missing_dsn_is_a_clear_error(monkeypatch):
    monkeypatch.delenv("PGLQ_DSN", raising=False)
    with pytest.raises(SystemExit, match="PGLQ_DSN"):
        cli.main(["stats"])


def test_bench_runs_end_to_end(dsn, conn, capsys):
    run(
        dsn,
        "bench",
        "--jobs",
        "300",
        "--workers",
        "1",
        "--concurrency",
        "4",
        "--latency-samples",
        "10",
        "--repeat",
        "1",
    )
    out = capsys.readouterr().out
    assert "| 1 |" in out and "LISTEN/NOTIFY" in out and "poll every 1s" in out
    assert conn.execute("select count(*) from pglq_jobs where queue = 'pglq-bench'").fetchone() == (0,)
