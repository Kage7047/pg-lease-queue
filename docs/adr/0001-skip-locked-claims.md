# 1. Claims with `FOR UPDATE SKIP LOCKED`

**Status:** accepted, 2026-11-05

**Context.** Many workers pull jobs from one table. The claim must hand each ready job to exactly
one worker, must not make workers wait on each other, and must survive a worker dying with the job
in hand.

**Options.**
- *Plain `SELECT ... FOR UPDATE`*: correct, but every worker queues behind the first one's lock on
  the head of the queue. Throughput is one claim at a time.
- *Advisory locks* (`pg_try_advisory_lock(id)`, the original Que design): no row locks, but the lock
  lives with the session, not the row, so a pooled connection can carry it to the wrong place. It
  also has to be checked row by row while scanning, and the "claimed" state is invisible to SQL.
- *An external broker* (Redis, RabbitMQ): fast, but enqueue can no longer join the transaction that
  creates the data, which brings back the dual-write problem (an order without its job, or a job
  for an order that rolled back).
- *`FOR UPDATE SKIP LOCKED`* (Postgres 9.5+): each claimer locks the first N ready rows that no one
  else has locked and skips the rest without waiting.

**Decision.** `SKIP LOCKED` in a CTE, followed by an `UPDATE` in the same statement that marks the
rows `running` and attaches a lease (ADR 0002). The claim is one round trip and commits at once, so
row locks are held for microseconds, not for the job's run time. Running is a *state in the row*,
not a held lock, which makes it visible to `pglq stats`, to the reaper and to plain SQL.

A partial index, `(queue, priority desc, run_at, id) where status = 'queued'`, contains only ready
work, so the claim walks the head of the index rather than the whole table.

**Consequences.** Ordering is priority then `run_at`, best effort: two claimers can each take a
batch and finish in either order. Strict FIFO would need a single consumer. `UPDATE ... RETURNING`
has no order of its own, so the claim sorts the claimed rows before handing them out (a test caught
this). See ADR 0004 for what `SKIP LOCKED` costs under heavy churn.
