"""Sign what you send, so the receiver can prove it came from you and was not replayed.

The header shape is the one receivers already have verifiers for:
``X-Signature: t=1730000000,v1=<hex hmac of "t.body">``. Including the timestamp inside the
signed content is what makes a captured request useless after the tolerance window: an attacker
who replays the body cannot change the timestamp without invalidating the signature.
"""

from __future__ import annotations

import hashlib
import hmac
import time
from typing import Union

HEADER = "X-Signature"
DEFAULT_TOLERANCE = 300      # five minutes

Secret = Union[str, bytes]


class SignatureError(Exception):
    """The signature was missing, malformed, stale or wrong."""


def _as_bytes(secret: Secret) -> bytes:
    return secret.encode("utf-8") if isinstance(secret, str) else secret


def _signed_payload(timestamp: int, body: bytes) -> bytes:
    return b"%d." % timestamp + body


def sign(secret: Secret, body: bytes, timestamp: int | None = None) -> str:
    """Return the header value for this body."""
    ts = int(timestamp if timestamp is not None else time.time())
    digest = hmac.new(_as_bytes(secret), _signed_payload(ts, body), hashlib.sha256)
    return "t=%d,v1=%s" % (ts, digest.hexdigest())


def parse(header: str) -> tuple[int, list[str]]:
    """Split a header into its timestamp and every v1 signature (rotation means several)."""
    if not header or not header.strip():
        raise SignatureError("signature header is empty")
    timestamp = None
    signatures: list[str] = []
    for part in header.split(","):
        part = part.strip()
        if not part:
            continue
        if "=" not in part:
            raise SignatureError(f"malformed signature segment: {part!r}")
        name, _, value = part.partition("=")
        name = name.strip()
        if name == "t":
            try:
                timestamp = int(value)
            except ValueError as exc:
                raise SignatureError(
                    f"timestamp is not an integer: {value!r}") from exc
        elif name == "v1":
            signatures.append(value.strip())
    if timestamp is None:
        raise SignatureError("signature header has no timestamp")
    if not signatures:
        raise SignatureError("signature header has no v1 signature")
    return timestamp, signatures


def verify(secret: Secret, body: bytes, header: str, tolerance: int = DEFAULT_TOLERANCE,
           now: int | None = None) -> None:
    """Raise SignatureError unless this header is ours and fresh.

    Accepts any v1 signature in the header, so a secret can be rotated by signing with two keys
    during the changeover.
    """
    timestamp, signatures = parse(header)
    current = int(now if now is not None else time.time())
    if tolerance and abs(current - timestamp) > tolerance:
        raise SignatureError(
            "signature timestamp is %ds away from now, outside the %ds tolerance; the request "
            "was replayed or a clock is wrong" % (abs(current - timestamp), tolerance))
    expected = hmac.new(_as_bytes(secret), _signed_payload(timestamp, body),
                        hashlib.sha256).hexdigest()
    for candidate in signatures:
        if hmac.compare_digest(expected, candidate):
            return
    raise SignatureError("signature does not match the body")
