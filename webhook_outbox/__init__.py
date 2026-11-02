"""Deliver outgoing webhooks exactly once, or fail loudly trying."""

from .backoff import RetryPolicy, classify
from .engine import Outbox, RunSummary
from .sign import SignatureError, sign, verify
from .store import Delivery, SQLiteStore

__version__ = "0.1.0"

__all__ = [
    "Delivery",
    "Outbox",
    "RetryPolicy",
    "RunSummary",
    "SQLiteStore",
    "SignatureError",
    "classify",
    "sign",
    "verify",
    "__version__",
]
