# 4. Batching claims and completions (the 8-worker collapse)

**Status:** accepted, 2026-11-05

**Context.** The first benchmark (20,000 no-op jobs, 16 concurrent per worker process, a 2-core
laptop) scaled from 922 jobs/s with one worker to 1,638 with four, then **fell to 306 with eight**.
More workers made the queue five times slower.

**Diagnosis.** Sampling `pg_stat_activity` during the 8-worker run (15 samples, 0.5 s apart):

| state | samples |
|---|---:|
| claim query on CPU | 71 |
| "mark done" update waiting on `Lock:transactionid` | 35 |
| "mark done" waiting on `LWLock:WALWrite` / `IO:WALSync` | 33 |

and `pg_stat_user_tables` showed 91,185 dead tuples against 0 live, with zero HOT updates.

Three things compound:

1. Each job leaves three row versions (queued, running, done). None can be a HOT update, because
   every transition changes an indexed column or a partial-index predicate. Until vacuum runs, the
   ready index is full of entries for rows that are no longer ready, and every claim pays to look at
   them. The default autovacuum threshold (20% of the table) is far too lazy for a queue.
2. The worker re-claimed after every single completion, so with 16 slots it ran about one claim
   per job: many expensive scans, each fetching one row.
3. Under `READ COMMITTED`, when `SELECT ... FOR UPDATE SKIP LOCKED` meets a row that was updated
   since its snapshot, Postgres follows the update chain and locks the newest version before
   re-checking the `WHERE` clause. A claim can therefore hold a lock on a row it then skips (one that
   is already `running`), and the owner's "mark done" waits for that claim to finish. Slow claims
   (1, 2) make these stray waits long.

**Decision.**
- **Batch claims.** A worker claims only when at least `claim_batch` slots are free (default
  `concurrency / 4`), or when it is idle. Fewer claims, each taking more rows.
- **Batch completions without adding latency.** A `Completer` writes a completion at once if no
  write is in flight. Completions that arrive while one is in flight go out together next, as a
  single `update ... from unnest(ids, tokens, results)` in one commit. Each row is still fenced by
  its own token. Under light load every batch is one job; under load batches grow by themselves.
- **Per-table autovacuum settings** in the migration: `autovacuum_vacuum_scale_factor = 0.01` (vacuum
  after 1% churn instead of 20%) and `autovacuum_vacuum_cost_delay = 1`.

**Result** (same machine and workload, median of 3):

| worker processes | before | after |
|---:|---:|---:|
| 1 | 922 | 1,428 |
| 2 | 1,378 | 2,144 |
| 4 | 1,638 | 2,747 |
| 8 | **306** | 2,152 |

The remaining dip at 8 is CPU: 8 Python processes plus their Postgres backends on 2 physical cores.

**Consequences.** A worker with fewer than `claim_batch` free slots will not take new work until
more finish. Raise `claim_batch` toward 1 for latency-sensitive queues with long jobs. A completion
batch that fails (database down) fails every job in it; those jobs are retried through the normal
failure path or recovered by the reaper, so nothing is lost, only re-run. `prune` old jobs regularly:
a small table keeps claims cheap.
