from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

JOBS = Counter(
    "pglq_jobs_total",
    "Jobs handled by this worker, by outcome (done, retry, dead, lease_lost, released)",
    ["queue", "task", "outcome"],
)
JOB_SECONDS = Histogram(
    "pglq_job_seconds",
    "Handler run time",
    ["queue", "task"],
    buckets=(0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300),
)
PICKUP_SECONDS = Histogram(
    "pglq_pickup_delay_seconds",
    "Time from a job becoming ready to a worker claiming it",
    ["queue"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
)
CLAIM_SECONDS = Histogram("pglq_claim_seconds", "Claim query round trip")
INFLIGHT = Gauge("pglq_inflight", "Jobs this worker is running now")
REAPED = Counter("pglq_reaped_total", "Expired leases recovered by this worker's reaper", ["status"])
DEPTH = Gauge("pglq_queue_jobs", "Jobs in the table by queue and state", ["queue", "state"])
LAG = Gauge("pglq_queue_lag_seconds", "Age of the oldest ready job", ["queue"])
COMPLETE_BATCH = Histogram(
    "pglq_complete_batch_size", "Jobs marked done per statement", buckets=(1, 2, 5, 10, 25, 50, 100, 250, 500)
)
