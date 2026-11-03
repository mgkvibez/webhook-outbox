"""Deliver outgoing webhooks exactly once, or fail loudly trying."""

from .backoff import RetryPolicy, classify
from .engine import Outbox, RunSummary
from .sender import DeliveryResult, HttpSender, ThreadedAsyncSender
from .sign import SignatureError, sign, verify
from .store import Delivery, SQLiteStore

__version__ = "0.2.0"

__all__ = [
    "Delivery",
    "DeliveryResult",
    "HttpSender",
    "Outbox",
    "RetryPolicy",
    "RunSummary",
    "SQLiteStore",
    "SignatureError",
    "ThreadedAsyncSender",
    "classify",
    "sign",
    "verify",
    "__version__",
]
