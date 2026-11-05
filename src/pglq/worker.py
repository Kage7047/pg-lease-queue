"""The asyncio worker: claim, run, heartbeat, finish; reap other workers' expired leases.

One worker process runs up to ``concurrency`` jobs at a time. It is woken by ``LISTEN pglq`` when a
job is enqueued and also polls every ``poll_interval`` seconds, so a lost notification (or a job
scheduled for later) only costs a poll interval, never a stuck job.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import secrets
import socket
import time
import traceback
from collections.abc import Sequence
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from pglq import metrics, sql
from pglq.backoff import Backoff
from pglq.job import Job, LeaseLost, Permanent, Retry, Tasks, TaskSpec, as_json

log = logging.getLogger("pglq.worker")


def default_name() -> str:
    return f"{socket.gethostname()}:{os.getpid()}:{secrets.token_hex(2)}"


class Completer:
    """Batches "mark done" updates without adding latency.

    A completion is written at once if nothing else is being written. Completions that arrive
    while a write is in flight wait for it and then go out together in one statement and one
    commit. Under light load every batch has one job; under heavy load batches grow by themselves.
    """

    def __init__(self, pool: AsyncConnectionPool, max_batch: int = 500) -> None:
        self.pool = pool
        self.max_batch = max_batch
        self._pending: list[tuple[Job, asyncio.Future[bool]]] = []
        self._kick = asyncio.Event()
        self.batches = 0

    async def complete(self, job: Job) -> bool:
        fut: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
        self._pending.append((job, fut))
        self._kick.set()
        return await fut

    async def run(self) -> None:
        try:
            while True:
                await self._kick.wait()
                self._kick.clear()
                while self._pending:
                    batch, self._pending = self._pending[: self.max_batch], self._pending[self.max_batch :]
                    await self._flush(batch)
        finally:
            for _, fut in self._pending:
                if not fut.done():
                    fut.set_exception(RuntimeError("worker stopped before the completion was written"))

    async def _flush(self, batch: list[tuple[Job, asyncio.Future[bool]]]) -> None:
        batch.sort(key=lambda b: b[0].id)  # a stable lock order
        params = {
            "ids": [j.id for j, _ in batch],
            "tokens": [j.lease_token for j, _ in batch],
            "results": [as_json(j.result) for j, _ in batch],
        }
        try:
            async with self.pool.connection() as conn:
                rows = await (await conn.execute(sql.COMPLETE_MANY, params)).fetchall()
        except Exception as e:
            for _, fut in batch:
                if not fut.done():
                    fut.set_exception(e)
            return
        self.batches += 1
        metrics.COMPLETE_BATCH.observe(len(batch))
        ours = {int(r[0]) for r in rows}
        for job, fut in batch:
            if not fut.done():
                fut.set_result(job.id in ours)


class Worker:
    def __init__(
        self,
        dsn: str,
        tasks: Tasks,
        *,
        queues: Sequence[str] = ("default",),
        concurrency: int = 10,
        claim_batch: int | None = None,
        lease: float = 30.0,
        poll_interval: float = 2.0,
        reap_interval: float | None = None,
        grace: float = 30.0,
        listen: bool = True,
        stats_interval: float | None = None,
        backoff: Backoff | None = None,
        name: str | None = None,
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be at least 1")
        if lease <= 0:
            raise ValueError("lease must be positive")
        self.dsn = dsn
        self.tasks = tasks
        self.queues = list(queues)
        self.concurrency = concurrency
        # Claim in batches, not one slot at a time: each claim is a locking index scan, so a busy
        # worker that re-claimed after every single completion would spend its time claiming.
        self.claim_batch = claim_batch or max(1, concurrency // 4)
        self.lease = lease
        self.poll_interval = poll_interval
        self.reap_interval = reap_interval if reap_interval is not None else min(max(lease / 2, 0.5), 10.0)
        self.grace = grace
        self.listen = listen
        self.stats_interval = stats_interval
        self.backoff = backoff or Backoff()
        self.name = name or default_name()
        self.pool = AsyncConnectionPool(
            dsn, min_size=1, max_size=concurrency + 3, kwargs={"autocommit": True}, open=False
        )
        self.completer = Completer(self.pool)
        self._inflight: set[asyncio.Task[None]] = set()
        self._wake = asyncio.Event()
        self._stop = asyncio.Event()
        self._started = asyncio.Event()
        self.handled = 0

    # ---- lifecycle -----------------------------------------------------------------------------

    def stop(self) -> None:
        """Stop claiming. Running jobs get ``grace`` seconds to finish, then are handed back."""
        self._stop.set()
        self._wake.set()

    async def wait_started(self) -> None:
        await self._started.wait()

    async def run(self) -> None:
        await self.pool.open(wait=True)
        helpers = [
            asyncio.create_task(self._reaper(), name="pglq-reaper"),
            asyncio.create_task(self.completer.run(), name="pglq-completer"),
        ]
        if self.listen:
            helpers.append(asyncio.create_task(self._listener(), name="pglq-listener"))
        if self.stats_interval:
            helpers.append(asyncio.create_task(self._stats(), name="pglq-stats"))
        log.info(
            "worker %s up: queues=%s concurrency=%d lease=%.1fs", self.name, self.queues, self.concurrency, self.lease
        )
        self._started.set()
        try:
            await self._loop()
        finally:
            await self._drain()
            for h in helpers:
                h.cancel()
            await asyncio.gather(*helpers, return_exceptions=True)
            await self.pool.close()
            log.info("worker %s stopped after %d jobs", self.name, self.handled)

    async def _loop(self) -> None:
        while not self._stop.is_set():
            self._wake.clear()  # before claiming, so a notify that lands mid-claim is not lost
            free = self.concurrency - len(self._inflight)
            if free >= self.claim_batch or (free > 0 and not self._inflight):
                try:
                    jobs = await self._claim(free)
                except Exception as e:  # database down or restarting: back off, keep trying
                    log.warning("claim failed: %s", e)
                    await self._sleep(self.poll_interval)
                    continue
                for job in jobs:
                    t = asyncio.create_task(self._execute(job), name=f"pglq-job-{job.id}")
                    self._inflight.add(t)
                metrics.INFLIGHT.set(len(self._inflight))
                if jobs and len(jobs) == free:
                    continue  # there may be more; claim again as soon as a batch of slots frees up
            await self._sleep(self.poll_interval)

    async def _sleep(self, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._wake.wait(), timeout=seconds)

    async def _drain(self) -> None:
        if not self._inflight:
            return
        log.info("waiting up to %.0fs for %d running jobs", self.grace, len(self._inflight))
        _, pending = await asyncio.wait(self._inflight, timeout=self.grace)
        for t in pending:
            t.cancel()  # _execute hands the job back on cancellation
        await asyncio.gather(*pending, return_exceptions=True)

    # ---- the protocol --------------------------------------------------------------------------

    async def _claim(self, n: int) -> list[Job]:
        t0 = time.perf_counter()
        async with self.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql.CLAIM, {"queues": self.queues, "n": n, "lease": self.lease, "worker": self.name})
            rows = await cur.fetchall()
        metrics.CLAIM_SECONDS.observe(time.perf_counter() - t0)
        jobs = [Job(**r, _pool=self.pool) for r in rows]
        for j in jobs:
            metrics.PICKUP_SECONDS.labels(j.queue).observe(max(j.pickup_delay, 0.0))
        return jobs

    async def _run(self, sql_text: str, params: dict[str, Any]) -> int:
        async with self.pool.connection() as conn:
            return (await conn.execute(sql_text, params)).rowcount

    def _ids(self, job: Job) -> dict[str, Any]:
        return {"id": job.id, "token": job.lease_token}

    async def _heartbeat(self, job: Job, handler: asyncio.Task[Any]) -> None:
        interval = self.lease / 3
        while not job.completed:
            await asyncio.sleep(interval)
            if job.completed:
                return
            try:
                ok = await self._run(sql.EXTEND, {**self._ids(job), "lease": self.lease})
            except Exception as e:  # a blip: try again next beat; the lease has two more beats of slack
                log.warning("heartbeat for job %d failed: %s", job.id, e)
                continue
            if not ok and not job.completed:
                if job.in_transaction:
                    continue  # blocked behind job.transaction()'s row lock; its commit decides
                log.warning("job %d: lease lost, cancelling the handler", job.id)
                job.lost = True
                handler.cancel()
                return

    async def _call(self, spec: TaskSpec, job: Job) -> Any:
        async with asyncio.timeout(spec.timeout):
            if spec.is_async:
                return await spec.fn(job)
            return await asyncio.to_thread(spec.fn, job)

    async def _execute(self, job: Job) -> None:
        spec = self.tasks.get(job.task)
        t0 = time.perf_counter()
        outcome = "done"
        try:
            if spec is None:
                raise Permanent(f"no handler registered for task {job.task!r}")
            handler = asyncio.create_task(self._call(spec, job))
            beat = asyncio.create_task(self._heartbeat(job, handler))
            try:
                try:
                    result = await handler
                except asyncio.CancelledError:
                    task = asyncio.current_task()
                    if job.lost and not (task and task.cancelling()):
                        raise LeaseLost(job.id) from None
                    raise
            finally:
                beat.cancel()
            if not job.completed:
                job.result = result if job.result is None else job.result
                if not await self.completer.complete(job):
                    raise LeaseLost(job.id)
                job.completed = True
        except LeaseLost:
            outcome = "lease_lost"
            log.warning("job %d (%s): lease lost, result discarded", job.id, job.task)
        except asyncio.CancelledError:
            outcome = "released"
            if not job.completed:
                await self._release(job)
            raise
        except Exception as e:
            outcome = "done" if job.completed else await self._fail(job, e)
            if job.completed:
                log.warning("job %d (%s) raised after its transaction committed: %r", job.id, job.task, e)
        finally:
            # label by registered task names only: a producer typo must not mint new time series
            task_label = job.task if spec is not None else "unregistered"
            metrics.JOB_SECONDS.labels(job.queue, task_label).observe(time.perf_counter() - t0)
            metrics.JOBS.labels(job.queue, task_label, outcome).inc()
            self.handled += 1
            current = asyncio.current_task()
            if current is not None:
                self._inflight.discard(current)
            metrics.INFLIGHT.set(len(self._inflight))
            self._wake.set()  # a slot is free

    async def _fail(self, job: Job, e: BaseException) -> str:
        permanent = isinstance(e, Permanent)
        delay = e.delay if isinstance(e, Retry) else self.backoff.delay(job.attempts)
        error = "".join(traceback.format_exception_only(e)).strip()
        if not isinstance(e, Retry | Permanent):
            error += "\n" + "".join(traceback.format_tb(e.__traceback__)[-3:])
        params = {**self._ids(job), "permanent": permanent, "delay": delay, "error": error[:4000]}
        try:
            async with self.pool.connection() as conn:
                row = await (await conn.execute(sql.FAIL, params)).fetchone()
        except Exception as db_error:  # the reaper will recover it when the lease runs out
            log.error("job %d: could not record failure (%s); the lease will expire", job.id, db_error)
            return "retry"
        if row is None:
            return "lease_lost"
        status = "dead" if row[0] == "dead" else "retry"
        level = logging.ERROR if status == "dead" else logging.INFO
        first_line = error.splitlines()[0]
        log.log(
            level,
            "job %d (%s) attempt %d/%d failed -> %s: %s",
            job.id,
            job.task,
            job.attempts,
            job.max_attempts,
            status,
            first_line,
        )
        return status

    async def _release(self, job: Job) -> None:
        try:
            await asyncio.shield(self._run(sql.RELEASE, self._ids(job)))
        except Exception as e:
            log.warning("job %d: could not hand back (%s); the lease will expire", job.id, e)

    # ---- helpers -------------------------------------------------------------------------------

    async def reap(self, limit: int = 500) -> list[tuple[int, str]]:
        async with self.pool.connection() as conn:
            rows = await (await conn.execute(sql.REAP, {"limit": limit})).fetchall()
        for job_id, status in rows:
            metrics.REAPED.labels(status).inc()
            log.warning("job %d: lease expired, now %s", job_id, status)
        if any(s == "queued" for _, s in rows):
            self._wake.set()
        return [(int(i), str(s)) for i, s in rows]

    async def _reaper(self) -> None:
        while True:
            try:
                await self.reap()
            except Exception as e:
                log.warning("reaper: %s", e)
            await asyncio.sleep(self.reap_interval)

    async def _listener(self) -> None:
        delay = 1.0
        wanted = set(self.queues)
        while True:
            try:
                async with await psycopg.AsyncConnection.connect(self.dsn, autocommit=True) as conn:
                    await conn.execute(f"listen {sql.CHANNEL}")
                    delay = 1.0
                    self._wake.set()  # anything enqueued while we were not listening
                    async for n in conn.notifies():
                        if n.payload in wanted:
                            self._wake.set()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("listener: %s (reconnecting in %.0fs; polling meanwhile)", e, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30.0)

    async def _stats(self) -> None:
        assert self.stats_interval
        while True:
            try:
                async with self.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                    for r in await (await cur.execute(sql.STATS)).fetchall():
                        if r["queue"] not in self.queues:
                            continue  # only report the queues this worker serves
                        for state in ("ready", "scheduled", "running", "done", "dead"):
                            metrics.DEPTH.labels(r["queue"], state).set(r[state])
                        metrics.LAG.labels(r["queue"]).set(r["lag_seconds"])
            except Exception as e:
                log.warning("stats: %s", e)
            await asyncio.sleep(self.stats_interval)
