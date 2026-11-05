.PHONY: install db test chaos lint bench

DB_URL ?= postgresql://postgres:postgres@127.0.0.1:54329/postgres
export PGLQ_TEST_DSN ?= $(DB_URL)

install:            ## venv + package + dev tools
	uv venv && uv pip install -e ".[dev]"

db:                 ## a throwaway Postgres 17 in Docker on :54329
	docker run -d --rm --name pglq-db -p 54329:5432 -e POSTGRES_PASSWORD=postgres postgres:17-alpine \
		-c max_connections=300
	until docker exec pglq-db pg_isready -U postgres >/dev/null 2>&1; do sleep 0.5; done

test:               ## everything except the chaos test
	.venv/bin/pytest -q -m "not chaos" --cov=pglq

chaos:              ## kill -9 workers at random while they drain jobs (~1 minute)
	.venv/bin/pytest -q -s -m chaos

lint:
	.venv/bin/ruff check . && .venv/bin/ruff format --check . && .venv/bin/mypy

bench:
	.venv/bin/pglq --dsn $(DB_URL) bench
