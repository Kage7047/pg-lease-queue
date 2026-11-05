"""Kill -9 workers at random while they drain thousands of jobs, then check the books.

Each job's handler writes one row into ``effects``. SIGKILL can land anywhere: mid-handler,
mid-transaction, between commit and heartbeat. The kills keep coming until the queue is empty.

With ``job.transaction()`` (the write and the "done" commit together):
* every job ends up done (crashed jobs are reaped and re-run; transient errors are retried);
* every job has exactly one effect row, never zero and never two.

The control runs the same write committed on its own, then finishes the job. Kills between the
two re-run jobs whose effect already landed, so duplicates must appear. If they did not, the test
would not be able to see the bug it is guarding against.
"""

from __future__ import annotations

import os
import random
import signal
import subprocess
import sys
import time

import pytest

import pglq

pytestmark = pytest.mark.chaos

JOBS = int(os.environ.get("PGLQ_CHAOS_JOBS", "3000"))
WORKERS = 4


def start(dsn: str) -> subprocess.Popen[bytes]:
    cmd = [sys.executable, "-m", "pglq", "worker", "tests.chaos_tasks:tasks",
           "--concurrency", "8", "--lease", "2", "--poll", "0.5"]  # fmt: skip
    env = {**os.environ, "PGLQ_DSN": dsn}
    return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)


def chaos(dsn: str, conn, task: str) -> dict[str, int]:
    pglq.enqueue_many(conn, [(task, {"tag": f"job-{i}"}) for i in range(JOBS)], max_attempts=100)
    rng = random.Random(7)
    procs = [start(dsn) for _ in range(WORKERS)]
    kills = 0
    deadline = time.monotonic() + 240

    def done() -> int:
        return int(conn.execute("select count(*) from pglq_jobs where status = 'done'").fetchone()[0])

    try:
        while done() < JOBS:
            assert time.monotonic() < deadline, "jobs did not drain"
            time.sleep(rng.uniform(0.2, 0.8))
            if done() > JOBS * 0.95:
                continue  # let the tail drain so the test ends
            i = rng.randrange(len(procs))
            procs[i].send_signal(signal.SIGKILL)
            procs[i].wait()
            kills += 1
            procs[i] = start(dsn)
    finally:
        for p in procs:
            p.terminate()
        for p in procs:
            p.wait(timeout=30)

    def one(q: str) -> int:
        return int(conn.execute(q).fetchone()[0])

    return {
        "kills": kills,
        "done": done(),
        "effects": one("select count(*) from effects"),
        "doubled": one("select count(*) from (select job_id from effects group by job_id having count(*) > 1) d"),
        "missing": one(
            "select count(*) from pglq_jobs j where not exists (select from effects e where e.job_id = j.id)"
        ),
        "retried": one("select count(*) from pglq_jobs where attempts > 1"),
        "reaped": one("select count(*) from pglq_jobs where last_error like 'lease expired%'"),
    }


def test_kill_dash_nine_never_loses_or_doubles_an_effect(dsn, conn):
    r = chaos(dsn, conn, "pay")
    print(f"\njob.transaction(): {r}")
    assert r["done"] == JOBS and r["effects"] == JOBS and r["doubled"] == 0 and r["missing"] == 0
    assert r["kills"] >= 10 and r["retried"] > 0 and r["reaped"] > 0


def test_control_without_the_transaction_does_double_effects(dsn, conn):
    r = chaos(dsn, conn, "pay_naive")
    print(f"\nnaive (commit, then finish): {r}")
    assert r["done"] == JOBS and r["missing"] == 0
    assert r["doubled"] > 0, "the chaos never landed between commit and finish; the test has lost its teeth"
