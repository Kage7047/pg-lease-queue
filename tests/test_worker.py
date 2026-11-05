"""The asyncio worker end to end, against a real Postgres."""

from __future__ import annotations

import asyncio
import time

import pytest

import pglq
from pglq import Backoff, Permanent, Retry, Tasks, Worker
from tests.conftest import row, running, until

FAST = Backoff(base=0.05, cap=0.1)


def worker(dsn: str, tasks: Tasks, **kw) -> Worker:
    kw = {"poll_interval": 0.2, "lease": 5.0, "backoff": FAST, "grace": 5.0, **kw}
    return Worker(dsn, tasks, **kw)


def status(conn, job_id: int) -> str:
    return row(conn, job_id)["status"]


async def test_runs_a_job_and_stores_its_result(dsn, conn):
    tasks = Tasks()

    @tasks.task()
    async def add(job):
        return {"sum": job.args["a"] + job.args["b"]}

    job_id = pglq.enqueue(conn, "add", {"a": 2, "b": 3})
    async with running(worker(dsn, tasks)) as w:
        await until(lambda: status(conn, job_id) == "done")
    r = row(conn, job_id)
    assert r["result"] == {"sum": 5} and r["attempts"] == 1 and r["lease_token"] is None and w.handled == 1


async def test_notify_wakes_an_idle_worker_long_before_its_poll(dsn, conn):
    tasks = Tasks()
    tasks.task("noop")(lambda job: None)
    async with running(worker(dsn, tasks, poll_interval=30)):
        await asyncio.sleep(0.3)  # let it finish its first empty claim and go to sleep
        t0 = time.perf_counter()
        job_id = pglq.enqueue(conn, "noop")
        await until(lambda: status(conn, job_id) == "done", within=5)
        assert time.perf_counter() - t0 < 2


async def test_polling_alone_still_finds_scheduled_jobs(dsn, conn):
    tasks = Tasks()
    tasks.task("noop")(lambda job: None)
    job_id = pglq.enqueue(conn, "noop", delay=0.4)
    async with running(worker(dsn, tasks, listen=False, poll_interval=0.1)):
        await asyncio.sleep(0.2)
        assert status(conn, job_id) == "queued"
        await until(lambda: status(conn, job_id) == "done", within=5)


async def test_failures_retry_with_backoff_then_go_dead(dsn, conn):
    tasks = Tasks()
    calls = []

    @tasks.task()
    async def flaky(job):
        calls.append(job.attempts)
        raise RuntimeError(f"downstream 503 on attempt {job.attempts}")

    job_id = pglq.enqueue(conn, "flaky", max_attempts=3)
    async with running(worker(dsn, tasks)):
        await until(lambda: status(conn, job_id) == "dead")
    r = row(conn, job_id)
    assert calls == [1, 2, 3] and r["attempts"] == 3
    assert r["last_error"].startswith("RuntimeError: downstream 503 on attempt 3")


async def test_retry_and_permanent_signals(dsn, conn):
    tasks = Tasks()

    @tasks.task()
    async def later(job):
        if job.attempts == 1:
            raise Retry(0.3, "rate limited")
        return "ok"

    @tasks.task()
    async def bad(job):
        raise Permanent("invalid card number")

    a = pglq.enqueue(conn, "later")
    b = pglq.enqueue(conn, "bad", max_attempts=10)
    async with running(worker(dsn, tasks)):
        await until(lambda: status(conn, b) == "dead")
        await until(lambda: status(conn, a) == "done")
    assert row(conn, a)["attempts"] == 2 and row(conn, b)["attempts"] == 1
    assert "invalid card number" in row(conn, b)["last_error"]


async def test_unknown_task_goes_dead_with_a_clear_error(dsn, conn):
    job_id = pglq.enqueue(conn, "nobody-handles-this")
    async with running(worker(dsn, Tasks())):
        await until(lambda: status(conn, job_id) == "dead")
    assert "no handler registered" in row(conn, job_id)["last_error"]


async def test_timeouts_count_as_failures(dsn, conn):
    tasks = Tasks()

    @tasks.task(timeout=0.1)
    async def slow(job):
        await asyncio.sleep(10)

    job_id = pglq.enqueue(conn, "slow", max_attempts=2)
    async with running(worker(dsn, tasks)):
        await until(lambda: status(conn, job_id) == "dead")
    assert row(conn, job_id)["last_error"].startswith("TimeoutError")


async def test_sync_handlers_run_in_a_thread(dsn, conn):
    tasks = Tasks()

    @tasks.task()
    def blocking(job):
        time.sleep(0.05)
        return job.args["x"] * 2

    job_id = pglq.enqueue(conn, "blocking", {"x": 21})
    async with running(worker(dsn, tasks)):
        await until(lambda: status(conn, job_id) == "done")
    assert row(conn, job_id)["result"] == 42


async def test_heartbeats_keep_a_long_job_alive(dsn, conn):
    tasks = Tasks()

    @tasks.task()
    async def long(job):
        await asyncio.sleep(1.5)  # 2.5x the lease

    job_id = pglq.enqueue(conn, "long")
    async with running(worker(dsn, tasks, lease=0.6, reap_interval=0.1)):
        await until(lambda: status(conn, job_id) == "done", within=5)
    r = row(conn, job_id)
    assert r["attempts"] == 1 and r["last_error"] is None


async def test_losing_the_lease_cancels_the_handler(dsn, conn):
    tasks = Tasks()
    seen = []

    @tasks.task()
    async def long(job):
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            seen.append("cancelled")
            raise

    job_id = pglq.enqueue(conn, "long")
    async with running(worker(dsn, tasks, lease=0.6)):
        await until(lambda: status(conn, job_id) == "running")
        # another worker took over (say, after a network partition): the token changes under us
        conn.execute("update pglq_jobs set lease_token = gen_random_uuid(), worker = 'B' where id = %s", (job_id,))
        await until(lambda: seen == ["cancelled"], within=3)
    r = row(conn, job_id)
    assert r["status"] == "running" and r["worker"] == "B"  # untouched: it is B's job now


async def test_transaction_makes_effects_exactly_once(dsn, conn):
    tasks = Tasks()

    @tasks.task()
    async def pay(job):
        async with job.transaction() as tx:
            await tx.execute("insert into effects (job_id) values (%s)", (job.id,))
            job.result = {"paid": True}

    job_id = pglq.enqueue(conn, "pay")
    async with running(worker(dsn, tasks)):
        await until(lambda: status(conn, job_id) == "done")
    assert conn.execute("select count(*) from effects").fetchone() == (1,)
    assert row(conn, job_id)["result"] == {"paid": True}


async def test_transaction_writes_nothing_once_the_lease_is_gone(dsn, conn):
    tasks = Tasks()
    gate = asyncio.Event()
    outcome = []

    @tasks.task()
    async def pay(job):
        await gate.wait()
        try:
            async with job.transaction() as tx:
                await tx.execute("insert into effects (job_id) values (%s)", (job.id,))
        except pglq.LeaseLost:
            outcome.append("lease lost")
            raise

    job_id = pglq.enqueue(conn, "pay")
    async with running(worker(dsn, tasks)):
        await until(lambda: status(conn, job_id) == "running")
        conn.execute("update pglq_jobs set lease_token = gen_random_uuid() where id = %s", (job_id,))
        gate.set()
        await until(lambda: outcome == ["lease lost"])
    assert conn.execute("select count(*) from effects").fetchone() == (0,)


async def test_rollback_inside_the_transaction_retries_cleanly(dsn, conn):
    tasks = Tasks()

    @tasks.task()
    async def pay(job):
        async with job.transaction() as tx:
            await tx.execute("insert into effects (job_id) values (%s)", (job.id,))
            if job.attempts == 1:
                raise RuntimeError("card network timeout after the insert")

    job_id = pglq.enqueue(conn, "pay")
    async with running(worker(dsn, tasks)):
        await until(lambda: status(conn, job_id) == "done")
    assert conn.execute("select count(*) from effects").fetchone() == (1,)  # attempt 1's insert rolled back
    assert row(conn, job_id)["attempts"] == 2


async def test_concurrency_limit(dsn, conn):
    tasks = Tasks()
    now = peak = 0

    @tasks.task()
    async def work(job):
        nonlocal now, peak
        now += 1
        peak = max(peak, now)
        await asyncio.sleep(0.1)
        now -= 1

    pglq.enqueue_many(conn, [("work", {})] * 12)
    async with running(worker(dsn, tasks, concurrency=3)):
        await until(lambda: conn.execute("select count(*) from pglq_jobs where status = 'done'").fetchone() == (12,))
    assert peak == 3


async def test_shutdown_finishes_quick_jobs_and_hands_back_slow_ones(dsn, conn):
    tasks = Tasks()

    @tasks.task()
    async def quick(job):
        await asyncio.sleep(0.2)

    @tasks.task()
    async def slow(job):
        await asyncio.sleep(30)

    q = pglq.enqueue(conn, "quick")
    s = pglq.enqueue(conn, "slow")
    async with running(worker(dsn, tasks, grace=1.0)):
        await until(lambda: status(conn, q) == "running" and status(conn, s) == "running")
    assert status(conn, q) == "done"
    r = row(conn, s)
    assert r["status"] == "queued" and r["attempts"] == 0 and r["worker"] is None


async def test_a_crashed_workers_jobs_are_reaped_by_a_live_one(dsn, conn):
    job_id = pglq.enqueue(conn, "noop")
    conn.execute(  # a worker that claimed it and then vanished
        "update pglq_jobs set status = 'running', attempts = 1, lease_token = gen_random_uuid(),"
        " lease_until = now() - interval '1s', worker = 'ghost' where id = %s",
        (job_id,),
    )
    tasks = Tasks()
    tasks.task("noop")(lambda job: None)
    async with running(worker(dsn, tasks, reap_interval=0.1)):
        await until(lambda: status(conn, job_id) == "done")
    assert row(conn, job_id)["attempts"] == 2


async def test_worker_survives_a_database_restart_of_its_connections(dsn, conn):
    tasks = Tasks()
    tasks.task("noop")(lambda job: None)
    async with running(worker(dsn, tasks)):
        await asyncio.sleep(0.3)
        conn.execute(  # kill every pglq connection except our own, like a failover would
            "select pg_terminate_backend(pid) from pg_stat_activity"
            " where datname = current_database() and pid <> pg_backend_pid() and backend_type = 'client backend'"
        )
        job_id = pglq.enqueue(conn, "noop")
        await until(lambda: status(conn, job_id) == "done", within=15)


def test_job_transaction_needs_a_worker():
    job = pglq.Job(1, "q", "t", {}, 0, 1, 1, None, None, None)  # type: ignore[arg-type]

    async def go():
        async with job.transaction():
            pass

    with pytest.raises(RuntimeError):
        asyncio.run(go())


def test_task_names_are_unique():
    tasks = Tasks()
    tasks.task("a")(lambda job: None)
    with pytest.raises(ValueError):
        tasks.task("a")(lambda job: None)
    assert tasks.names == ["a"] and "a" in tasks


def test_worker_validates_settings(dsn):
    with pytest.raises(ValueError):
        Worker(dsn, Tasks(), concurrency=0)
    with pytest.raises(ValueError):
        Worker(dsn, Tasks(), lease=0)


@pytest.mark.no_db
def test_backoff_grows_with_jitter_and_caps():
    b = Backoff(base=1, cap=8)
    for attempt, (lo, hi) in {1: (0.5, 1), 2: (1, 2), 3: (2, 4), 4: (4, 8), 10: (4, 8)}.items():
        assert all(lo <= b.delay(attempt) <= hi for _ in range(50))


async def test_aenqueue_and_queue_gauges(dsn, conn):
    import psycopg
    from prometheus_client import REGISTRY

    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as ac:
        a = await pglq.aenqueue(ac, "noop", {"n": 1}, idempotency_key="once", queue="gauges")
        assert await pglq.aenqueue(ac, "noop", idempotency_key="once", queue="gauges") == a
    tasks = Tasks()
    tasks.task("noop")(lambda job: None)
    async with running(worker(dsn, tasks, queues=["gauges"], stats_interval=0.05)):
        await until(lambda: status(conn, a) == "done")
        await until(lambda: REGISTRY.get_sample_value("pglq_queue_jobs", {"queue": "gauges", "state": "done"}) == 1)
    assert REGISTRY.get_sample_value("pglq_jobs_total", {"queue": "gauges", "task": "noop", "outcome": "done"}) >= 1


async def test_unregistered_task_names_share_one_metric_label(dsn, conn):
    from prometheus_client import REGISTRY

    pglq.enqueue(conn, "typo-1", queue="labels")
    pglq.enqueue(conn, "typo-2", queue="labels")
    async with running(worker(dsn, Tasks(), queues=["labels"])):
        await until(lambda: conn.execute("select count(*) from pglq_jobs where status = 'dead'").fetchone() == (2,))
    labels = {"queue": "labels", "task": "unregistered", "outcome": "dead"}
    assert REGISTRY.get_sample_value("pglq_jobs_total", labels) == 2
    assert REGISTRY.get_sample_value("pglq_jobs_total", {**labels, "task": "typo-1"}) is None
