# Security

Please report vulnerabilities privately through
[GitHub's private vulnerability reporting](https://github.com/Kage7047/pg-lease-queue/security/advisories/new),
not in a public issue. I will reply within a week.

Things that count: a way to make a job run twice under `job.transaction()`, to finish or change a
job without holding its lease, to read or write outside `pglq_*` tables through the library's SQL,
or to leak connection strings (the CLI reads them from `PGLQ_DSN`, never from logs).
