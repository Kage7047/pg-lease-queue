"""The protocol, driven by hand: enqueue, claim, extend, complete, fail, reap, release."""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import psycopg
import pytest

import pglq
from pglq import schema, sql
from tests.conftest import claim, row


def ids(job) -> dict:
    return {"id": job["id"], "token": job["lease_token"]}


# ---- schema -----------------------------------------------------------------------------------


def test_concurrent_migrations_apply_once(dsn):
    with psycopg.connect(dsn, autocommit=True) as c:
        c.execute("drop schema if exists mig cascade")
        c.execute("create schema mig")
    scoped = f"{dsn}{'&' if '?' in dsn else '?'}options=-csearch_path%3Dmig"
    barrier = threading.Barrier(8)

    def go() -> list[int]:
        with psycopg.connect(scoped, autocommit=True) as c:
            barrier.wait()
            return schema.migrate(c)

    with ThreadPoolExecutor(8) as pool:
        results = list(pool.map(lambda _: go(), range(8)))
    assert sorted(results) == [[]] * 7 + [[1]]
    with psycopg.connect(scoped, autocommit=True) as c:
        assert schema.current_version(c) == schema.LATEST
        c.execute("drop schema mig cascade")


def test_the_database_enforces_lease_iff_running(conn):
    job_id = pglq.enqueue(conn, "t")
    with pytest.raises(psycopg.errors.CheckViolation):
        conn.execute("update pglq_jobs set status = 'running' where id = %s", (job_id,))


# ---- enqueue ----------------------------------------------------------------------------------


def test_enqueue_defaults(conn):
    job_id = pglq.enqueue(conn, "send", {"to": "a@example.com"})
    r = row(conn, job_id)
    assert (r["task"], r["args"], r["status"], r["queue"], r["attempts"], r["max_attempts"]) == (
        "send", {"to": "a@example.com"}, "queued", "default", 0, 5,
    )  # fmt: skip


def test_idempotency_key_dedupes_per_queue(conn):
    a = pglq.enqueue(conn, "charge", {"order": 1}, idempotency_key="order-1")
    b = pglq.enqueue(conn, "charge", {"order": 1}, idempotency_key="order-1")
    c = pglq.enqueue(conn, "charge", {"order": 1}, idempotency_key="order-1", queue="other")
    assert a == b != c
    assert conn.execute("select count(*) from pglq_jobs").fetchone() == (2,)


def test_concurrent_idempotent_enqueues_create_one_job(dsn, conn):
    barrier = threading.Barrier(10)

    def go() -> int:
        with psycopg.connect(dsn, autocommit=True) as c:
            barrier.wait()
            return pglq.enqueue(c, "charge", idempotency_key="race")

    with ThreadPoolExecutor(10) as pool:
        got = set(pool.map(lambda _: go(), range(10)))
    assert len(got) == 1


def test_enqueue_joins_the_callers_transaction(dsn, conn):
    with psycopg.connect(dsn) as c:
        pglq.enqueue(c, "welcome-email")
        c.rollback()  # the signup failed, so the email job never existed
        pglq.enqueue(c, "welcome-email", {"n": 2})
        c.commit()
    assert [r[0] for r in conn.execute("select args->>'n' from pglq_jobs")] == ["2"]


def test_enqueue_many(conn):
    got = pglq.enqueue_many(conn, [("a", {"i": 1}), ("b", {"i": 2})], queue="bulk", priority=3)
    assert len(got) == 2
    assert pglq.enqueue_many(conn, []) == []
    assert row(conn, got[1])["task"] == "b" and row(conn, got[1])["priority"] == 3


def test_enqueue_validates(conn):
    with pytest.raises(ValueError):
        pglq.enqueue(conn, "t", max_attempts=0)
    with pytest.raises(ValueError):
        pglq.enqueue(conn, "t", priority=40_000)


# ---- claim ------------------------------------------------------------------------------------


def test_claim_order_is_priority_then_age(conn):
    low = pglq.enqueue(conn, "t")
    high = pglq.enqueue(conn, "t", priority=10)
    low2 = pglq.enqueue(conn, "t")
    assert [j["id"] for j in claim(conn, 3)] == [high, low, low2]


def test_claim_respects_queue_and_schedule(conn):
    later = pglq.enqueue(conn, "t", delay=60)
    other = pglq.enqueue(conn, "t", queue="other")
    assert claim(conn, 5) == []
    assert [j["id"] for j in claim(conn, 5, queues=("other",))] == [other]
    conn.execute("update pglq_jobs set run_at = now() - interval '1s' where id = %s", (later,))
    (j,) = claim(conn)
    assert j["id"] == later and j["attempts"] == 1 and j["pickup_delay"] >= 1


def test_concurrent_claimers_never_get_the_same_job(dsn, conn):
    pglq.enqueue_many(conn, [("t", {})] * 500)
    seen: list[int] = []
    lock = threading.Lock()

    def drain(_: int) -> None:
        with psycopg.connect(dsn, autocommit=True) as c:
            while got := claim(c, 7):
                with lock:
                    seen.extend(j["id"] for j in got)

    with ThreadPoolExecutor(16) as pool:
        list(pool.map(drain, range(16)))
    assert len(seen) == 500 and len(set(seen)) == 500


# ---- finishing, and fencing -------------------------------------------------------------------


def test_complete_needs_the_current_token(conn):
    pglq.enqueue(conn, "t")
    (j,) = claim(conn)
    bogus = {"id": j["id"], "token": "00000000-0000-0000-0000-000000000000", "result": None}
    assert conn.execute(sql.COMPLETE, bogus).rowcount == 0
    assert conn.execute(sql.COMPLETE, {**ids(j), "result": None}).rowcount == 1
    assert row(conn, j["id"])["status"] == "done"


def test_a_zombie_worker_cannot_finish_a_job_it_lost(conn):
    """The fencing scenario: A stalls past its lease, B takes over, A wakes up and tries to finish."""
    pglq.enqueue(conn, "t")
    (a,) = claim(conn, lease=0.2, worker="A")
    time.sleep(0.3)
    reaped = conn.execute(sql.REAP, {"limit": 10}).fetchall()
    assert reaped == [(a["id"], "queued")]
    (b,) = claim(conn, worker="B")
    assert b["id"] == a["id"] and b["lease_token"] != a["lease_token"] and b["attempts"] == 2
    assert conn.execute(sql.COMPLETE, {**ids(a), "result": None}).rowcount == 0  # A is fenced off
    assert conn.execute(sql.EXTEND, {**ids(a), "lease": 30}).rowcount == 0
    assert conn.execute(sql.COMPLETE, {**ids(b), "result": None}).rowcount == 1
    r = row(conn, a["id"])
    assert r["status"] == "done" and "stopped heartbeating" in r["last_error"]


def test_heartbeat_extends_the_lease(conn):
    pglq.enqueue(conn, "t")
    (j,) = claim(conn, lease=1)
    before = row(conn, j["id"])["lease_until"]
    assert conn.execute(sql.EXTEND, {**ids(j), "lease": 60}).rowcount == 1
    assert row(conn, j["id"])["lease_until"] - before > timedelta(seconds=50)


def test_fail_retries_with_delay_then_buries(conn):
    pglq.enqueue(conn, "t", max_attempts=2)
    (j,) = claim(conn)
    st = conn.execute(sql.FAIL, {**ids(j), "permanent": False, "delay": 60, "error": "boom"}).fetchone()
    assert st == ("queued",) and claim(conn) == []  # not due for a minute
    conn.execute("update pglq_jobs set run_at = now()")
    (j,) = claim(conn)
    st = conn.execute(sql.FAIL, {**ids(j), "permanent": False, "delay": 60, "error": "boom 2"}).fetchone()
    r = row(conn, j["id"])
    assert st == ("dead",) and r["attempts"] == 2 and r["last_error"] == "boom 2" and r["finished_at"]


def test_permanent_failure_skips_the_retries(conn):
    pglq.enqueue(conn, "t", max_attempts=10)
    (j,) = claim(conn)
    st = conn.execute(sql.FAIL, {**ids(j), "permanent": True, "delay": 1, "error": "bad input"}).fetchone()
    assert st == ("dead",)


def test_a_job_that_keeps_killing_its_worker_ends_up_dead(conn):
    pglq.enqueue(conn, "oom", max_attempts=2)
    for expected in ("queued", "dead"):
        claim(conn, lease=0.05)
        time.sleep(0.1)
        assert [s for _, s in conn.execute(sql.REAP, {"limit": 10})] == [expected]
    assert claim(conn) == []


def test_release_hands_back_without_using_an_attempt(conn):
    pglq.enqueue(conn, "t")
    (j,) = claim(conn)
    assert conn.execute(sql.RELEASE, ids(j)).rowcount == 1
    r = row(conn, j["id"])
    assert r["status"] == "queued" and r["attempts"] == 0 and r["lease_token"] is None


# ---- admin ------------------------------------------------------------------------------------


def test_retry_dead_cancel_prune_and_stats(conn):
    dead = pglq.enqueue(conn, "t", max_attempts=1)
    (j,) = claim(conn)
    conn.execute(sql.FAIL, {**ids(j), "permanent": False, "delay": 0, "error": "x"})
    waiting = pglq.enqueue(conn, "t", delay=3600)
    done = pglq.enqueue(conn, "t", queue="q2")
    (d,) = claim(conn, queues=("q2",))
    conn.execute(sql.COMPLETE, {**ids(d), "result": None})

    by_queue = {s.queue: s for s in pglq.stats(conn)}
    assert (by_queue["default"].dead, by_queue["default"].scheduled, by_queue["q2"].done) == (1, 1, 1)

    assert pglq.cancel(conn, waiting) is True
    assert pglq.cancel(conn, waiting) is False  # already cancelled
    assert pglq.retry_dead(conn, task="other") == 0
    assert pglq.retry_dead(conn) == 1
    assert row(conn, dead)["status"] == "queued" and row(conn, dead)["attempts"] == 0

    conn.execute("update pglq_jobs set finished_at = now() - interval '8 days' where id in (%s, %s)", (done, waiting))
    assert pglq.prune(conn, older_than=7 * 86400) == 2
    assert pglq.get(conn, done) is None and pglq.get(conn, dead) is not None


def test_lag_is_the_age_of_the_oldest_ready_job(conn):
    j = pglq.enqueue(conn, "t")
    conn.execute("update pglq_jobs set run_at = now() - interval '90 seconds' where id = %s", (j,))
    (s,) = pglq.stats(conn)
    assert 89 < s.lag_seconds < 120
