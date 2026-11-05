# 2. Leases with fencing tokens, on the database clock

**Status:** accepted, 2026-11-05

**Context.** A worker can die with a job in hand (OOM kill, deploy, power loss), or worse, stop
for a while and come back (GC pause, stuck syscall, network partition). The queue must recover the
job from the first kind, and must stop the second kind from finishing a job that has meanwhile been
given to someone else.

**Decision.**
- A claim sets `lease_until = now() + lease`. The worker extends it every `lease / 3` (so two missed
  heartbeats are tolerated). Each worker runs a reaper that moves `running` jobs with an expired
  lease back to `queued`, or to `dead` once their attempts are used up. The reaper uses `SKIP LOCKED`
  too, so many reapers never contend.
- Each claim mints a new random `lease_token` (UUID). Every later statement on the job (extend,
  complete, fail, release, and the lock in `job.transaction()`) includes `and lease_token = $token`.
  This is a fencing token in Kleppmann's sense: the database rejects a write from a lease holder
  that is no longer the current one. A worker whose heartbeat matches zero rows cancels the handler.
- All times are `now()` in the database. Worker clocks are never compared, so skew between machines
  cannot shorten or stretch a lease.
- A `CHECK` constraint ties `status = 'running'` to "a token and an expiry are set", so no code path
  can leave a running job without an owner.
- Attempts are counted at claim time. A crash therefore uses up an attempt, and a job that kills its
  worker every time ends up `dead` instead of taking the fleet down in a loop. A graceful shutdown
  hands the job back with `attempts - 1`, so deploys do not burn attempts.

**Consequences.** Recovery time is at most one lease plus one reap interval (30 s + 10 s by
default). Shorter leases recover faster but cost more heartbeat writes and risk expiring during a
long database stall. A sync handler cannot be cancelled mid-call (it runs in a thread); its result
is still discarded by the token check, but its external side effects are not undone, which is why
delivery is at least once and external calls need their own idempotency key (ADR 0003).
