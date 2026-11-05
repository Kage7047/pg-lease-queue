# 3. Exactly-once effects through `job.transaction()`

**Status:** accepted, 2026-11-05

**Context.** Delivery is at least once: any worker can die after doing the work and before marking
the job done, and then the job runs again. For a charge, a ledger entry or a counter, running twice
is a bug. Distributed exactly-once delivery is not achievable in general, but exactly-once *effects*
are achievable when the effect and the "done" mark live in the same database.

**Decision.** `job.transaction()` is an async context manager that:

1. opens a transaction on the worker's pool;
2. runs `select ... where id = $id and lease_token = $token and status = 'running' for update`.
   If no row comes back the lease is gone: raise `LeaseLost`, write nothing;
3. yields the connection to the handler for its writes;
4. sets `status = 'done'` (same token check) and commits.

While the job row is locked, the reaper (which skips locked rows) cannot take the job, even if
the lease runs out mid-transaction. So the commit either includes both the effect and "done", or
neither. A crash before the commit re-runs the job against a clean slate. After the commit the job
is finished and is never claimed again.

**Evidence.** `tests/test_chaos.py` runs 3,000 jobs through 4 worker processes while one is
`SIGKILL`ed every 0.2 to 0.8 s until the queue drains. With `job.transaction()` every job has exactly
one effect row. The control test does the same write committed on its own and then finishes the
job; it must produce duplicates (one local run: 43 out of 3,000), which shows the test can detect
the failure it guards against.

**Consequences.** This only covers effects in the queue's own Postgres database. An email, an HTTP
call or a write to another store is still at least once; pass a stable idempotency key downstream
(the job id, or the enqueue's idempotency key). Handlers that use `job.transaction()` hold a row lock
for the length of their transaction, so that transaction should be short: do slow I/O before it.
