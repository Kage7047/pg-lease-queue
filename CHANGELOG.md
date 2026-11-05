# Changelog

## 0.1.0 (2026-11-05)

First release.

- `FOR UPDATE SKIP LOCKED` claims, leases with fencing tokens on the database clock, a reaper for
  expired leases, and a `CHECK` constraint tying `running` to a held lease.
- `job.transaction()` for exactly-once effects in the same database; `enqueue` joins the caller's
  transaction.
- Retries with exponential backoff and jitter, `Retry` and `Permanent` signals, a dead state and
  `retry-dead`, idempotency keys, priorities, delayed jobs, cancel and prune.
- asyncio worker with `LISTEN/NOTIFY` wake-ups and a polling backstop, batched claims and
  latency-free batched completions, per-task timeouts, sync handlers in threads, graceful shutdown.
- Prometheus metrics, a `pglq` CLI, and `pglq bench`.
- A `kill -9` chaos test with a control, run in CI on Postgres 14 to 17.
