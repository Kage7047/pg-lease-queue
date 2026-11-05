# pg-lease-queue

[![ci](https://github.com/Kage7047/pg-lease-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/Kage7047/pg-lease-queue/actions/workflows/ci.yml)
![python](https://img.shields.io/badge/python-3.12%E2%80%933.14-blue)
![postgres](https://img.shields.io/badge/postgres-14%E2%80%9317-336791)
![license](https://img.shields.io/badge/license-MIT-green)

A crash-safe job queue on plain Postgres, for Python asyncio. No Redis, no broker: one table, a
few carefully ordered SQL statements, and a worker that assumes it can be `kill -9`ed at any moment.

- **Claims with `FOR UPDATE SKIP LOCKED`**: any number of workers pull from one table without
  blocking each other or getting the same job twice.
- **Leases with fencing tokens**: a job is held for a lease the worker keeps renewing. A worker that
  crashes, hangs or is partitioned loses the job when the lease runs out, and its token stops working,
  so a "zombie" that wakes up later cannot finish or overwrite a job someone else now owns.
- **Exactly-once effects**: `job.transaction()` commits the handler's writes and "job done" in one
  transaction. A crash before the commit re-runs the job; after it, the job is finished. Never both.
- **Transactional enqueue**: `enqueue(conn, ...)` joins your transaction, so a job exists if and only
  if the order (or signup, or payment) that created it does.
- **Instant wake-ups**: `LISTEN/NOTIFY` wakes idle workers in ~3 ms. Polling stays on as a backstop.
- Retries with exponential backoff and jitter, a dead-letter state, idempotency keys, priorities,
  delayed jobs, graceful shutdown, Prometheus metrics and a CLI.

The proof is [`tests/test_chaos.py`](tests/test_chaos.py): four worker processes drain 3,000 jobs
while one of them is `SIGKILL`ed every 0.2 to 0.8 seconds until the queue is empty. One local run
(CI runs it on every push and prints its own numbers):

| run | kills | jobs done | effect rows | doubled | missing |
|---|---:|---:|---:|---:|---:|
| `job.transaction()` | 17 | 3,000 | 3,000 | **0** | 0 |
| control: commit the write, then finish the job | 23 | 3,000 | 3,043 | **43** | 0 |

The control shows the test can see the bug it guards against: committing the side effect and
finishing the job in two steps double-charges whenever a kill lands between them.

## Quickstart

```bash
pip install "pg-lease-queue @ git+https://github.com/Kage7047/pg-lease-queue"
export PGLQ_DSN=postgresql://postgres:postgres@localhost:5432/postgres
pglq migrate
```

```python
# app/jobs.py
import pglq

tasks = pglq.Tasks()


@tasks.task(timeout=30)
async def send_receipt(job: pglq.Job):
    await mailer.send(job.args["email"], template="receipt")  # at-least-once: make it idempotent


@tasks.task()
async def charge(job: pglq.Job):
    if job.args["amount_cents"] <= 0:
        raise pglq.Permanent("nothing to charge")  # straight to dead, no retries
    charge_id = await provider.charge(job.args["order_id"], idempotency_key=f"order-{job.args['order_id']}")
    async with job.transaction() as tx:  # exactly once in your database
        await tx.execute("insert into ledger (order_id, charge_id) values (%s, %s)", (job.args["order_id"], charge_id))
```

```python
# in a request handler: the job commits with the order, or not at all
with psycopg.connect(dsn) as conn:
    order_id = create_order(conn, cart)
    pglq.enqueue(
        conn,
        "charge",
        {"order_id": order_id, "amount_cents": total},
        idempotency_key=f"charge-{order_id}",
        max_attempts=10,
    )
```

```bash
pglq worker app.jobs:tasks --concurrency 20 --metrics-port 9100   # run as many as you like
pglq stats
```

[`examples/orders.py`](examples/orders.py) is a runnable version: place 50 orders, start workers,
`kill -9` some, and check that every order was charged exactly once. CI does exactly that on every
push ([`tests/test_example.py`](tests/test_example.py)).

## How it works

```mermaid
stateDiagram-v2
    [*] --> queued: enqueue
    queued --> running: claim (attempts + 1, new lease token)
    running --> running: heartbeat extends the lease
    running --> done: complete (token must match)
    running --> queued: failure with attempts left (backoff), lease expired, or shutdown hand-back
    running --> dead: Permanent, or attempts used up
    dead --> queued: pglq retry-dead
    queued --> cancelled: cancel
    done --> [*]: prune
```

Every state change is one statement, all of them in [`src/pglq/sql.py`](src/pglq/sql.py). The
interesting ones:

**Claim.** `SKIP LOCKED` lets every worker take the next ready rows nobody else has locked, without
waiting. Each claim mints a fresh `lease_token` and sets `lease_until` from the database clock, so
clock skew between worker machines cannot shorten or stretch a lease.

```sql
with ready as (
    select id from pglq_jobs
    where status = 'queued' and queue = any($1) and run_at <= now()
    order by priority desc, run_at, id
    limit $2
    for update skip locked
)
update pglq_jobs j
set status = 'running', attempts = j.attempts + 1, lease_token = gen_random_uuid(),
    lease_until = now() + make_interval(secs => $3), worker = $4
from ready where j.id = ready.id
returning ...
```

**Fencing.** Heartbeat, complete, fail and release all say `where id = $1 and lease_token = $2`.
If the lease expired and the reaper handed the job to someone else, the token no longer matches,
zero rows change, and the old worker learns it lost the job (its handler is cancelled and its
result thrown away).

```mermaid
sequenceDiagram
    participant A as worker A
    participant DB as pglq_jobs
    participant B as worker B
    A->>DB: claim job 7 (token a1)
    Note over A: GC pause / partition / stuck syscall
    DB->>DB: lease expires, reaper: status = queued
    B->>DB: claim job 7 (token b2)
    A->>DB: complete where token = a1
    DB-->>A: 0 rows: fenced off
    B->>DB: complete where token = b2
    DB-->>B: 1 row: done
```

**Exactly-once effects.** `job.transaction()` opens a transaction, locks the job row *and checks the
token*, runs your writes, then sets `status = 'done'` before committing. While the row is locked the
reaper (which also uses `SKIP LOCKED`) cannot take the job away. If the lease was already lost, the
check fails, `LeaseLost` is raised and nothing is written.

**The database enforces the invariant** `status = 'running'` ⇔ a lease is held (a `CHECK`
constraint), so a bug cannot leave a running job without an owner or a queued job with one.

## Guarantees, and what they are not

| | |
|---|---|
| Delivery | **At least once.** A job runs again if its worker dies before finishing it. |
| Effects in the same Postgres | **Exactly once** with `job.transaction()`. |
| Effects elsewhere (email, HTTP) | At least once: pass an idempotency key downstream (the job id works). |
| Crash recovery | Within one lease (default 30 s) plus a reap interval. |
| Poison jobs | A crash uses up an attempt, so a job that kills its worker every time ends up `dead`, not looping. |
| Ordering | Priority, then `run_at`. Best effort under concurrency, not a strict FIFO. |
| Shutdown | `SIGTERM`: stop claiming, give running jobs `--grace` seconds, hand the rest back without using an attempt. |

## Performance

On a 2016 laptop (Intel i5-7200U, 2 cores / 4 threads, Postgres 16 on the same machine), no-op jobs,
16 concurrent per worker process, median of 3 runs ([details](docs/BENCHMARKS.md)):

| worker processes | jobs/s |
|---:|---:|
| 1 | 1,428 |
| 2 | 2,144 |
| 4 | 2,747 |
| 8 | 2,152 |

| wake-up | p50 | p99 |
|---|---:|---:|
| `LISTEN/NOTIFY` | 2.3 ms | 4.8 ms |
| polling every 1 s | 554 ms | 998 ms |

The first version collapsed to **306 jobs/s at 8 workers**. Sampling `pg_stat_activity` showed
claims burning CPU on dead tuples and "mark done" updates queued behind them. Batching claims,
batching completions, and per-table autovacuum settings fixed it. The diagnosis is in
[ADR 0004](docs/adr/0004-batching-claims-and-completions.md). Throughput at this size is bound by
commits and by CPU on two cores, not by locking.

## Metrics

`--metrics-port` serves Prometheus metrics:

| metric | what |
|---|---|
| `pglq_jobs_total{queue,task,outcome}` | done, retry, dead, lease_lost, released |
| `pglq_job_seconds{queue,task}` | handler run time |
| `pglq_pickup_delay_seconds{queue}` | ready → claimed |
| `pglq_queue_jobs{queue,state}`, `pglq_queue_lag_seconds{queue}` | depth, and age of the oldest ready job |
| `pglq_reaped_total{status}` | leases recovered from dead workers |
| `pglq_claim_seconds`, `pglq_complete_batch_size`, `pglq_inflight` | the worker's own loop |

Alert on `pglq_queue_lag_seconds` (work is waiting) and on `increase(pglq_jobs_total{outcome="dead"}[1h])`.

## CLI

```text
pglq migrate                                   create or upgrade the table (safe to run concurrently)
pglq enqueue TASK --args '{...}' [--queue q] [--priority n] [--delay 15m] [--max-attempts n] [--key k]
pglq worker module:tasks [--queues a,b] [--concurrency n] [--lease 30s] [--grace 30s] [--metrics-port p]
pglq stats [--json]                            ready / scheduled / running / done / dead per queue, and lag
pglq retry-dead [--queue q] [--task t]         give dead jobs a fresh set of attempts
pglq prune --older-than 7d                     delete finished jobs (dead ones are kept)
pglq bench                                     the benchmarks above, on a scratch queue
```

## When not to use it

- Throughput in the tens of thousands of jobs per second sustained: every job is several row
  versions in Postgres, and `NOTIFY` takes a global lock at commit. Use a log (Kafka, Redpanda) or a
  broker built for it.
- You need strict FIFO per key, fan-out, or streaming consumers: that is a log, not a job queue.
- Your jobs' data does not live in Postgres: you lose the main benefit (transactional enqueue and
  exactly-once effects) and keep the costs.

Mature options in this space, with more features: [Procrastinate](https://github.com/procrastinate-org/procrastinate)
(Python), [River](https://github.com/riverqueue/river) (Go), [graphile-worker](https://github.com/graphile/worker)
(Node), [pgmq](https://github.com/tembo-io/pgmq) (an extension). This project is smaller on purpose:
the whole protocol fits in one file and is tested by killing it.

## Design records

- [0001](docs/adr/0001-skip-locked-claims.md): claims with `SKIP LOCKED` instead of advisory locks or a broker
- [0002](docs/adr/0002-leases-and-fencing-tokens.md): leases with fencing tokens, on the database clock
- [0003](docs/adr/0003-exactly-once-effects.md): exactly-once effects through `job.transaction()`
- [0004](docs/adr/0004-batching-claims-and-completions.md): the 8-worker collapse and how batching fixed it

## Development

```bash
make install
make db          # a throwaway Postgres in Docker on :54329 (or point PGLQ_TEST_DSN anywhere disposable)
make test        # 49 tests against Postgres
make chaos       # the kill -9 test, its control, and the example under kill -9 (~1 minute)
make lint        # ruff + mypy --strict
```

CI runs the whole suite, chaos included, against Postgres 14, 15, 16 and 17, the unit suite on
Python 3.13 and 3.14, builds the wheel and installs it in a clean environment, and scans for secrets.

## License

MIT
