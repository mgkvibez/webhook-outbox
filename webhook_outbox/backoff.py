"""When to try again, and when retrying is just being stubborn.

Two decisions live here, and they are separate:

* how long to wait before attempt N (exponential from a base, capped, jittered)
* whether an answer is even worth retrying (a 404 will be a 404 in an hour)
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Callable

RETRY = "retry"
DEAD = "dead"
DELIVERED = "delivered"

#: Answers that mean "try later": the server is unwell, or it is telling us to slow down.
RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504, 507, 509})


@dataclass(frozen=True)
class RetryPolicy:
    """Exponential backoff with jitter, capped so a long outage does not push a delivery to
    next year. Defaults give roughly 1s, 2s, 4s, 8s, 16s, 32s, 60s, 60s ... for eight tries."""

    max_attempts: int = 8
    base: float = 1.0
    factor: float = 2.0
    cap: float = 3600.0
    jitter: float = 0.2

    def delay(self, attempt: int, rand: Callable[[], float] | None = None) -> float:
        """Seconds to wait after ``attempt`` (1-based) failed.

        Jitter spreads retries out so a thousand deliveries that all failed at once do not
        return in one synchronised wave.
        """
        if attempt < 1:
            raise ValueError("attempt is 1-based")
        raw = min(self.base * (self.factor ** (attempt - 1)), self.cap)
        if self.jitter <= 0:
            return raw
        picker = rand or random.random
        spread = raw * self.jitter
        return max(0.0, raw + (picker() * 2 - 1) * spread)

    def exhausted(self, attempts: int) -> bool:
        return attempts >= self.max_attempts


def classify(status: int | None, error: str | None = None) -> str:
    """What an attempt's outcome means.

    ``status`` is the HTTP status, or None when no response arrived at all (connection refused,
    timeout, DNS). No response is always worth retrying: the far end may simply be restarting.
    """
    if error and status is None:
        return RETRY
    if status is None:
        return RETRY
    if 200 <= status < 300:
        return DELIVERED
    if status in RETRYABLE_STATUS:
        return RETRY
    if 400 <= status < 500:
        # 400/401/403/404/410 say the request or the endpoint is wrong. Retrying eight times
        # only delays the dead-letter, so send it there now.
        return DEAD
    if 300 <= status < 400:
        return DEAD          # a redirect on a webhook endpoint is a misconfiguration
    return RETRY
