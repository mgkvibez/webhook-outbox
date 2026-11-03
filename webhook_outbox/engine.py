"""The loop that makes the outbox useful: claim, sign, send, record, back off or bury."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Union

from .backoff import DEAD, DELIVERED, RetryPolicy, classify
from .sender import HttpSender, ThreadedAsyncSender
from .sign import HEADER as SIGNATURE_HEADER
from .sign import sign
from .store import Delivery, SQLiteStore

SecretSource = Union[str, Callable[[str], Optional[str]], None]


@dataclass
class RunSummary:
    """What one pass of the worker did. ``clean`` means nothing is waiting on a human."""

    claimed: int = 0
    delivered: int = 0
    retried: int = 0
    dead: int = 0
    notes: list[str] = field(default_factory=list)

    @property
    def clean(self) -> bool:
        return self.dead == 0 and self.retried == 0


class Outbox:
    """Queue, deliver and audit outgoing webhooks.

    .. code-block:: python

        outbox = Outbox(SQLiteStore("outbox.db"), secret="whsec_...")
        outbox.enqueue("https://partner.example/hooks", "charge.succeeded",
                       {"id": "ch_1"}, dedupe_key="charge.succeeded:ch_1")
        outbox.deliver_due()

    ``secret`` may be a string used for every endpoint, or a callable that maps an endpoint to
    its own secret. Deliveries are signed only when a secret resolves; an unsigned delivery is
    still attempted, because local testing should not require a key.
    """

    def __init__(self, store: SQLiteStore, sender: HttpSender | None = None,
                 policy: RetryPolicy | None = None, secret: SecretSource = None,
                 lease_seconds: float = 60.0, clock: Callable[[], float] = time.time,
                 async_sender: Any = None,
                 on_event: Optional[Callable[[dict[str, Any]], None]] = None) -> None:
        self.store = store
        self.sender = sender if sender is not None else HttpSender()
        self.policy = policy if policy is not None else RetryPolicy()
        self.secret = secret
        self.lease_seconds = lease_seconds
        self.clock = clock
        self.async_sender = async_sender if async_sender is not None else ThreadedAsyncSender(self.sender)
        self.on_event = on_event
        #: In-process totals since this object was built; durable history lives in the store.
        self.counters: dict[str, int] = {"attempts": 0, "delivered": 0, "retried": 0, "dead": 0}

    # -- writing ---------------------------------------------------------------

    def enqueue(self, endpoint: str, event: str, payload: Any,
                dedupe_key: str | None = None):
        """Record intent. Same dedupe key twice = one delivery, which is what makes a retried
        business operation safe to hand to this queue."""
        return self.store.enqueue(endpoint, event, payload, dedupe_key, now=self.clock())

    def replay(self, delivery_id: int) -> Delivery:
        """Send a dead or delivered webhook again, on purpose."""
        return self.store.replay(delivery_id, now=self.clock())

    # -- delivering ------------------------------------------------------------

    def deliver_due(self, limit: int = 10) -> RunSummary:
        """One pass, synchronous. Claims what is due, sends it, records the outcome of each."""
        summary = RunSummary()
        for delivery in self.store.claim_due(limit, self.lease_seconds, now=self.clock()):
            summary.claimed += 1
            self._emit("attempt", delivery)
            body = delivery.body
            result = self.sender.send(delivery.endpoint, body, self._headers(delivery, body))
            self._record(delivery, result, summary)
        return summary

    async def deliver_due_async(self, limit: int = 10) -> RunSummary:
        """One pass, awaiting an async sender. Same claim and same recording path as the
        synchronous version, so a delivery cannot behave differently depending on which one
        a worker happens to use."""
        summary = RunSummary()
        for delivery in self.store.claim_due(limit, self.lease_seconds, now=self.clock()):
            summary.claimed += 1
            self._emit("attempt", delivery)
            body = delivery.body
            result = await self.async_sender.send(
                delivery.endpoint, body, self._headers(delivery, body))
            self._record(delivery, result, summary)
        return summary

    def _record(self, delivery: Delivery, result: Any, summary: RunSummary) -> None:
        verdict = self._tally(delivery, result, summary)
        if verdict == DELIVERED:
            summary.delivered += 1
            self.counters["delivered"] += 1
            self._emit("delivered", delivery, status=result.status)
        elif verdict == DEAD:
            summary.dead += 1
            self.counters["dead"] += 1
            self._emit("dead", delivery, status=result.status, error=result.error)
        else:
            summary.retried += 1
            self.counters["retried"] += 1
            self._emit("retried", delivery, status=result.status, error=result.error)

    def _tally(self, delivery: Delivery, result: Any, summary: RunSummary) -> str:
        verdict = classify(result.status, result.error)
        stamp = self.clock()

        if verdict == DELIVERED:
            self.store.mark_delivered(delivery.id, result.status, now=stamp)
            return DELIVERED

        problem = result.error or f"HTTP {result.status}"
        if verdict == DEAD:
            self.store.mark_dead(delivery.id, result.status,
                                 f"{problem} (not retried: the endpoint refused it)", now=stamp)
            summary.notes.append(f"{delivery.id} -> {problem}")
            return DEAD

        if self.policy.exhausted(delivery.attempts):
            self.store.mark_dead(delivery.id, result.status,
                                 "%s (gave up after %d attempts)"
                                 % (problem, delivery.attempts), now=stamp)
            summary.notes.append("%s gave up after %d attempts" % (delivery.id,
                                                                   delivery.attempts))
            return DEAD

        wait = self.policy.delay(delivery.attempts)
        self.store.mark_retry(delivery.id, result.status, problem, stamp + wait, now=stamp)
        summary.notes.append(f"{delivery.id} -> {problem}, retry in {wait:.1f}s")
        return "retry"

    def _headers(self, delivery: Delivery, body: bytes) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "X-Event": delivery.event,
            "X-Delivery-Id": str(delivery.id),
            "X-Delivery-Attempt": str(max(1, delivery.attempts)),
        }
        secret = self._secret_for(delivery.endpoint)
        if secret:
            headers[SIGNATURE_HEADER] = sign(secret, body)
        return headers

    def _secret_for(self, endpoint: str) -> str | None:
        if self.secret is None:
            return None
        if callable(self.secret):
            return self.secret(endpoint)
        return self.secret

    def _emit(self, kind: str, delivery: Delivery, **fields: Any) -> None:
        """Report one step to whoever is watching. A hook that raises must never stop a
        delivery: observability is not allowed to become a failure mode."""
        if kind == "attempt":
            self.counters["attempts"] += 1
        if self.on_event is None:
            return
        payload = {"event": kind, "delivery_id": delivery.id, "endpoint": delivery.endpoint,
                   "name": delivery.event, "attempt": delivery.attempts}
        payload.update(fields)
        try:
            self.on_event(payload)
        except Exception:                 # noqa: BLE001 - a broken hook is not a broken send
            pass

    # -- reading ---------------------------------------------------------------

    def deliveries(self, status: str | None = None, endpoint: str | None = None,
                   event: str | None = None, since: float | None = None,
                   limit: int = 50) -> list[Delivery]:
        """The delivery log, newest first, filtered however you need it."""
        return self.store.query(status=status, endpoint=endpoint, event=event,
                                since=since, limit=limit)

    def metrics(self) -> dict[str, Any]:
        """Counters for this worker plus the durable depth of the queue."""
        snapshot: dict[str, Any] = dict(self.counters)
        snapshot.update(self.store.stats(now=self.clock()))
        return snapshot

    def stats(self) -> dict[str, Any]:
        return self.store.stats(now=self.clock())

    def dead_letters(self, limit: int = 50) -> list[Delivery]:
        return self.store.dead_letters(limit)

    def get(self, delivery_id: int) -> Delivery:
        return self.store.get(delivery_id)
