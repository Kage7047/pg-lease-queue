"""The table, and a tiny forward-only migrator.

Migrations run under a transaction-scoped advisory lock, so ten workers starting at once apply
each one exactly once. The schema version lives in ``pglq_meta``.
"""

from __future__ import annotations

import psycopg

MIGRATIONS: list[str] = [
    # v1
    """
    create table pglq_jobs (
        id              bigint generated always as identity primary key,
        queue           text        not null default 'default',
        task            text        not null,
        args            jsonb       not null default '{}',
        status          text        not null default 'queued'
                        check (status in ('queued', 'running', 'done', 'dead', 'cancelled')),
        priority        smallint    not null default 0,
        run_at          timestamptz not null default now(),
        attempts        integer     not null default 0,
        max_attempts    integer     not null default 5 check (max_attempts > 0),
        lease_token     uuid,
        lease_until     timestamptz,
        worker          text,
        idempotency_key text,
        result          jsonb,
        last_error      text,
        created_at      timestamptz not null default now(),
        updated_at      timestamptz not null default now(),
        started_at      timestamptz,
        finished_at     timestamptz,
        -- a job holds a lease exactly while it is running: the database enforces the invariant
        constraint pglq_lease_iff_running
            check ((status = 'running') = (lease_token is not null and lease_until is not null))
    ) with (
        -- every job writes three row versions (queued, running, done); a queue table needs vacuum
        -- far more often than the 20%-of-the-table default, or claims wade through dead tuples
        autovacuum_vacuum_scale_factor = 0.01,
        autovacuum_vacuum_insert_scale_factor = 0.05,
        autovacuum_analyze_scale_factor = 0.05,
        autovacuum_vacuum_cost_delay = 1
    );
    -- the claim query walks this index in order; only ready rows are in it
    create index pglq_jobs_ready on pglq_jobs (queue, priority desc, run_at, id) where status = 'queued';
    -- the reaper looks for expired leases
    create index pglq_jobs_leases on pglq_jobs (lease_until) where status = 'running';
    create unique index pglq_jobs_idempotency on pglq_jobs (queue, idempotency_key)
        where idempotency_key is not null;
    """,
]

LATEST = len(MIGRATIONS)


def current_version(conn: psycopg.Connection) -> int:
    conn.execute("create table if not exists pglq_meta (version integer not null)")
    row = conn.execute("select version from pglq_meta").fetchone()
    return int(row[0]) if row else 0


def migrate(conn: psycopg.Connection) -> list[int]:
    """Apply pending migrations; return the versions applied. Safe to call concurrently."""
    applied: list[int] = []
    with conn.transaction():
        conn.execute("select pg_advisory_xact_lock(hashtext('pglq.migrate'))")
        version = current_version(conn)
        for v in range(version + 1, LATEST + 1):
            conn.execute(MIGRATIONS[v - 1])
            applied.append(v)
        if applied:
            conn.execute("delete from pglq_meta")
            conn.execute("insert into pglq_meta (version) values (%s)", (LATEST,))
    return applied
