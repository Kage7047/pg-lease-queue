from __future__ import annotations

import random
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Backoff:
    """Exponential backoff with "equal jitter": half the delay is fixed, half is random.

    attempt 1 waits ~base, attempt 2 ~2*base, ... capped at ``cap``. The jitter spreads out a
    burst of failures (say, a downstream outage) so the retries do not arrive as a second burst.
    """

    base: float = 2.0
    cap: float = 600.0
    rng: random.Random = field(default_factory=random.Random, compare=False)

    def delay(self, attempt: int) -> float:
        d = min(self.cap, self.base * 2 ** max(attempt - 1, 0))
        return float(d / 2 + self.rng.uniform(0, d / 2))
