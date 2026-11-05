"""The README's example, run for real: 50 orders, a worker killed halfway, every order charged once."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time

import pytest

pytestmark = pytest.mark.chaos


def test_orders_example_charges_every_order_exactly_once(dsn, conn):
    conn.execute("drop table if exists orders, ledger")
    env = {**os.environ, "PGLQ_DSN": dsn}
    script = ["examples/orders.py"]
    subprocess.run([sys.executable, *script, "setup"], env=env, check=True, capture_output=True)

    def worker() -> subprocess.Popen[bytes]:
        cmd = [sys.executable, "-m", "pglq", "worker", "examples.orders:tasks", "--lease", "2", "--poll", "0.5"]
        return subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def paid() -> int:
        return int(conn.execute("select count(*) from orders where paid").fetchone()[0])

    victim, survivor = worker(), worker()
    try:
        deadline = time.monotonic() + 120
        while paid() < 15 and time.monotonic() < deadline:
            time.sleep(0.05)
        victim.send_signal(signal.SIGKILL)  # mid-run, with charges in flight
        while paid() < 50 and time.monotonic() < deadline:
            time.sleep(0.1)
    finally:
        for p in (victim, survivor):
            p.kill()
            p.wait()

    out = subprocess.run([sys.executable, *script, "check"], env=env, check=True, capture_output=True, text=True)
    assert out.stdout.strip() == "50/50 orders paid, 0 charged more than once"
