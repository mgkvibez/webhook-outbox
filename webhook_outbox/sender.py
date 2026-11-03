"""The only part that touches the network: one HTTP POST, and a result instead of an exception.

A sender never raises. An unreachable host is an ordinary outcome here, not an error the caller
has to catch, because the whole point is to record it and try again later.
"""

from __future__ import annotations

import asyncio

import socket
import time
import urllib.error
import urllib.request
from dataclasses import dataclass

EXCERPT = 200


@dataclass
class DeliveryResult:
    """What one attempt produced."""

    status: int | None = None
    error: str | None = None
    duration_ms: int = 0
    excerpt: str = ""

    @property
    def ok(self) -> bool:
        return self.status is not None and 200 <= self.status < 300


class HttpSender:
    """POST JSON with a timeout. Redirects are not followed on purpose: a webhook endpoint that
    redirects is misconfigured, and quietly delivering somewhere else hides that."""

    def __init__(self, timeout: float = 10.0, user_agent: str = "webhook-outbox/0.2.0") -> None:
        self.timeout = timeout
        self.user_agent = user_agent

    def send(self, endpoint: str, body: bytes, headers: dict[str, str]) -> DeliveryResult:
        started = time.time()
        request = urllib.request.Request(endpoint, data=body, method="POST")
        for name, value in headers.items():
            request.add_header(name, value)
        request.add_header("User-Agent", self.user_agent)

        class _NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, req, fp, code, msg, hdrs, newurl):
                return None                 # surface the 3xx instead of following it

        opener = urllib.request.build_opener(_NoRedirect)
        try:
            with opener.open(request, timeout=self.timeout) as response:
                text = response.read(EXCERPT).decode("utf-8", "replace")
                return DeliveryResult(status=response.status, excerpt=text,
                                      duration_ms=int((time.time() - started) * 1000))
        except urllib.error.HTTPError as exc:
            try:
                text = exc.read(EXCERPT).decode("utf-8", "replace")
            except Exception:
                text = ""
            return DeliveryResult(status=exc.code, excerpt=text,
                                  duration_ms=int((time.time() - started) * 1000))
        except (urllib.error.URLError, socket.timeout, TimeoutError) as exc:
            reason = getattr(exc, "reason", exc)
            return DeliveryResult(error=f"{type(exc).__name__}: {reason}",
                                  duration_ms=int((time.time() - started) * 1000))
        except Exception as exc:                     # a sender must never raise
            return DeliveryResult(error=f"{type(exc).__name__}: {exc}",
                                  duration_ms=int((time.time() - started) * 1000))


class ThreadedAsyncSender:
    """An async facade over any synchronous sender, for frameworks that are async-first.

    Deliberately does not add an async HTTP client: requiring aiohttp or httpx to install this
    package would make the dependency decision for every user. ``asyncio.to_thread`` keeps the
    event loop free during the socket wait, which is the only thing that actually needed fixing.

    .. code-block:: python

        outbox = Outbox(store, sender=HttpSender(), async_sender=ThreadedAsyncSender())
        await outbox.deliver_due_async()
    """

    def __init__(self, sender: HttpSender | None = None) -> None:
        self.sender = sender if sender is not None else HttpSender()

    async def send(self, endpoint: str, body: bytes, headers: dict[str, str]) -> DeliveryResult:
        return await asyncio.to_thread(self.sender.send, endpoint, body, headers)
