"""Every statement the queue runs. Keeping them in one place makes the protocol reviewable.

Times always come from the database clock (``now()``), never from the worker, so clock skew
between worker machines cannot shorten or stretch a lease.
"""

CHANNEL = "pglq"

ENQUEUE = """
with ins as (
    insert into pglq_jobs (queue, task, args, priority, run_at, max_attempts, idempotency_key)
    values (%(queue)s, %(task)s, %(args)s, %(priority)s, now() + make_interval(secs => %(delay)s),
            %(max_attempts)s, %(key)s)
    on conflict (queue, idempotency_key) where idempotency_key is not null do nothing
    returning id
)
select id from ins
"""

# an idempotent enqueue that lost the race: the row exists, find it
EXISTING = "select id from pglq_jobs where queue = %(queue)s and idempotency_key = %(key)s"

ENQUEUE_MANY = """
with ins as (
    insert into pglq_jobs (queue, task, args, priority, run_at, max_attempts)
    select %(queue)s, t.task, t.args, %(priority)s, now() + make_interval(secs => %(delay)s), %(max_attempts)s
    from unnest(%(tasks)s::text[], %(args)s::jsonb[]) as t(task, args)
    returning id
)
select id from ins
"""

NOTIFY = "select pg_notify('pglq', %s)"

# The heart of it. SKIP LOCKED lets any number of workers run this at once: each one takes the
# next ready rows that nobody else has locked, without waiting. Every claim mints a new lease
# token; only the holder of the current token can finish the job (see COMPLETE / FAIL).
CLAIM = """
with ready as (
    select id from pglq_jobs
    where status = 'queued' and queue = any(%(queues)s) and run_at <= now()
    order by priority desc, run_at, id
    limit %(n)s
    for update skip locked
), claimed as (
    update pglq_jobs j
    set status = 'running',
        attempts = j.attempts + 1,
        lease_token = gen_random_uuid(),
        lease_until = now() + make_interval(secs => %(lease)s),
        worker = %(worker)s,
        started_at = now(),
        updated_at = now()
    from ready
    where j.id = ready.id
    returning j.id, j.queue, j.task, j.args, j.priority, j.attempts, j.max_attempts, j.lease_token,
              j.run_at, j.created_at, extract(epoch from now() - j.run_at)::float8 as pickup_delay
)
-- UPDATE ... RETURNING has no order of its own; hand jobs out in priority order
select * from claimed order by priority desc, run_at, id
"""

EXTEND = """
update pglq_jobs set lease_until = now() + make_interval(secs => %(lease)s), updated_at = now()
where id = %(id)s and lease_token = %(token)s and status = 'running'
"""

COMPLETE = """
update pglq_jobs
set status = 'done', result = %(result)s, finished_at = now(), updated_at = now(),
    lease_token = null, lease_until = null
where id = %(id)s and lease_token = %(token)s and status = 'running'
"""

# Many completions in one statement and one commit. Each row is still fenced by its own token;
# the ids that come back are the ones that were still ours.
COMPLETE_MANY = """
update pglq_jobs j
set status = 'done', result = v.result, finished_at = now(), updated_at = now(),
    lease_token = null, lease_until = null
from unnest(%(ids)s::bigint[], %(tokens)s::uuid[], %(results)s::jsonb[]) as v(id, token, result)
where j.id = v.id and j.lease_token = v.token and j.status = 'running'
returning j.id
"""

# retry later, or bury it once attempts are used up
FAIL = """
update pglq_jobs
set status = case when attempts >= max_attempts or %(permanent)s then 'dead' else 'queued' end,
    run_at = case when attempts >= max_attempts or %(permanent)s
                  then run_at else now() + make_interval(secs => %(delay)s) end,
    finished_at = case when attempts >= max_attempts or %(permanent)s then now() end,
    last_error = %(error)s, updated_at = now(), lease_token = null, lease_until = null, worker = null
where id = %(id)s and lease_token = %(token)s and status = 'running'
returning status
"""

# graceful shutdown: hand the job back without using up an attempt
RELEASE = """
update pglq_jobs
set status = 'queued', attempts = greatest(attempts - 1, 0), run_at = now(), updated_at = now(),
    lease_token = null, lease_until = null, worker = null
where id = %(id)s and lease_token = %(token)s and status = 'running'
"""

# Lock the job row for a handler's own transaction. If the lease is gone, nothing comes back
# and the handler's writes never commit.
LOCK_FOR_EFFECT = """
select id from pglq_jobs
where id = %(id)s and lease_token = %(token)s and status = 'running'
for update
"""

# A worker that stopped heartbeating (crashed, killed, partitioned) loses its jobs here. A crash
# still uses up the attempt, so a job that kills its worker every time ends up dead, not looping.
REAP = """
with expired as (
    select id from pglq_jobs
    where status = 'running' and lease_until < now()
    order by lease_until
    limit %(limit)s
    for update skip locked
)
update pglq_jobs j
set status = case when j.attempts >= j.max_attempts then 'dead' else 'queued' end,
    run_at = now(),
    finished_at = case when j.attempts >= j.max_attempts then now() end,
    last_error = 'lease expired: worker ' || coalesce(j.worker, '?') || ' stopped heartbeating',
    updated_at = now(), lease_token = null, lease_until = null, worker = null
from expired
where j.id = expired.id
returning j.id, j.status
"""

STATS = """
select queue,
       count(*) filter (where status = 'queued' and run_at <= now()) as ready,
       count(*) filter (where status = 'queued' and run_at > now())  as scheduled,
       count(*) filter (where status = 'running')                    as running,
       count(*) filter (where status = 'done')                       as done,
       count(*) filter (where status = 'dead')                       as dead,
       count(*) filter (where status = 'cancelled')                  as cancelled,
       coalesce(extract(epoch from now() - min(run_at) filter (where status = 'queued' and run_at <= now())), 0)
           ::float8 as lag_seconds
from pglq_jobs
group by queue
order by queue
"""

RETRY_DEAD = """
with revived as (
    update pglq_jobs
    set status = 'queued', attempts = 0, run_at = now(), finished_at = null, updated_at = now()
    where status = 'dead' and (%(queue)s::text is null or queue = %(queue)s)
                          and (%(task)s::text is null or task = %(task)s)
    returning queue
)
select queue, count(*) as n from revived group by queue
"""

CANCEL = """
update pglq_jobs set status = 'cancelled', finished_at = now(), updated_at = now()
where id = %(id)s and status = 'queued'
"""

PRUNE = """
with old as (
    select id from pglq_jobs
    where status in ('done', 'cancelled') and finished_at < now() - make_interval(secs => %(age)s)
    limit %(limit)s
)
delete from pglq_jobs j using old where j.id = old.id
"""
