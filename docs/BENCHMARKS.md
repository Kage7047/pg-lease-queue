# Benchmarks

`pglq bench` runs everything on a private queue name (`pglq-bench`) and deletes its rows afterwards.

**Throughput**: start N worker processes (`--concurrency 16` each), wait until they are all
connected (a warm-up batch), insert 20,000 no-op jobs in one transaction, and time until the last
one is `done`. Each setting runs three times; the median is reported along with all three runs.

**Pickup latency**: one worker, 300 jobs enqueued 20 ms apart, measuring `started_at - run_at`
(ready to claimed) in the database. Once with `LISTEN/NOTIFY`, once polling only (every 1 s).

## Results, 2026-11-05

Machine: Intel i5-7200U (2 cores / 4 threads, 2016), 8 GB RAM, SATA SSD, Ubuntu 24.04. Python 3.12, Postgres 16.2
on the same machine with default settings (`synchronous_commit = on`), plus the per-table
autovacuum settings from the migration. Other software was running (a desktop session and a
small polling service), so expect a few percent of noise.

`pglq bench --jobs 20000 --workers 1,2,4,8 --concurrency 16 --latency-samples 300`

| worker processes | jobs/s (median of 3) | runs |
|---:|---:|---|
| 1 | 1,428 | 888, 1,428, 1,502 |
| 2 | 2,144 | 2,058, 2,144, 2,273 |
| 4 | 2,747 | 2,630, 2,747, 2,979 |
| 8 | 2,152 | 2,083, 2,152, 2,485 |

| wake-up | p50 ms | p95 ms | p99 ms |
|---|---:|---:|---:|
| LISTEN/NOTIFY | 2.3 | 2.8 | 4.8 |
| poll every 1s | 553.6 | 957.1 | 997.6 |

### Before the batching fixes (ADR 0004)

One claim per free slot and one commit per completion, single run each:

| worker processes | jobs/s |
|---:|---:|
| 1 | 922 |
| 2 | 1,378 |
| 4 | 1,638 |
| 8 | 306 |

## Reading the numbers

- These are no-op jobs, so this measures the queue's own overhead: a claim (amortised over the
  batch), a completion (amortised over its batch) and the commits. Real handlers that take 50 ms
  need `concurrency` × workers ≈ jobs/s × 0.05 to keep up, which is where the concurrency setting
  comes from.
- At this scale the limit is commits and CPU, not locks. Postgres runs on the same two cores as the
  workers. On a separate database server with more cores, the 8-worker figure would keep climbing.
- `LISTEN/NOTIFY` matters more than throughput for most apps. Polling every second averages about
  500 ms before an idle worker notices new work; a notification takes about 3 ms.

Run it yourself:

```bash
pglq bench --jobs 20000 --workers 1,2,4,8 --concurrency 16
```
