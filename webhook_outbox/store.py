"""The outbox itself: intent written down before anything is sent, and attempt state after.

Two guarantees are enforced by SQLite rather than by hope:

* **dedupe** - a ``dedupe_key`` is UNIQUE, so enqueuing the same business event twice writes one
  row. Enqueue is an INSERT ... ON CONFLICT DO NOTHING followed by a read of the winner.
* **one worker per delivery** - claiming is a BEGIN IMMEDIATE transaction that flips the row to
  ``in_flight`` under a lease. A worker that dies mid-send does not strand the delivery: once the
  lease lapses the row is claimable again, which is why the lease exists.
"""

from __future__ import annotations

import builtins
import json
import sqlite3
import threading
import time
from dataclasses import dataclass
from typing import Any

PENDING = "pending"
IN_FLIGHT = "in_flight"
DELIVERED = "delivered"
DEAD = "dead"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS deliveries (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    dedupe_key      TEXT UNIQUE,
    endpoint        TEXT NOT NULL,
    event           TEXT NOT NULL,
    payload         TEXT NOT NULL,
    status          TEXT NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    replays         INTEGER NOT NULL DEFAULT 0,
    next_attempt_at REAL NOT NULL,
    lease_until     REAL,
    last_status     INTEGER,
    last_error      TEXT,
    created         REAL NOT NULL,
    updated         REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_due ON deliveries (status, next_attempt_at);
"""


@dataclass
class Delivery:
    """One intended delivery and what has happened to it."""

    id: int
    endpoint: str
    event: str
    payload: Any
    status: str
    attempts: int
    next_attempt_at: float
    lease_until: float | None = None
    last_status: int | None = None
    last_error: str | None = None
    dedupe_key: str | None = None
    replays: int = 0
    created: float = 0.0
    updated: float = 0.0

    @property
    def body(self) -> bytes:
        """The exact bytes to sign and send. Stable across attempts, which signing requires."""
        return json.dumps({"event": self.event, "data": self.payload},
                          sort_keys=True, separators=(",", ":")).encode("utf-8")


class SQLiteStore:
    """A durable outbox in one file, safe for several worker processes on the same host."""

    def __init__(self, path: str = "outbox.db", busy_timeout: float = 10.0) -> None:
        self.path = path
        self._lock = threading.RLock()   # enqueue/replay read back under the same lock
        self._conn = sqlite3.connect(path, timeout=busy_timeout, check_same_thread=False,
                                     isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(_SCHEMA)

    # -- writing ---------------------------------------------------------------

    def enqueue(self, endpoint: str, event: str, payload: Any,
                dedupe_key: str | None = None, now: float | None = None
                ) -> tuple[Delivery, bool]:
        """Record the intent. Returns (delivery, created).

        ``created`` is False when the dedupe key was already present, so a caller can tell "this
        is new work" from "you already asked for this".
        """
        stamp = time.time() if now is None else now
        body = json.dumps(payload, sort_keys=True)
        with self._lock:
            cursor = self._conn.execute(
                "INSERT INTO deliveries"
                " (dedupe_key, endpoint, event, payload, status, next_attempt_at, created, updated)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
                " ON CONFLICT(dedupe_key) DO NOTHING",
                (dedupe_key, endpoint, event, body, PENDING, stamp, stamp, stamp))
            created = cursor.rowcount == 1
            if created:
                delivery_id = cursor.lastrowid
            else:
                row = self._conn.execute(
                    "SELECT id FROM deliveries WHERE dedupe_key = ?", (dedupe_key,)).fetchone()
                if row is None:                      # no key given: INSERT cannot have conflicted
                    raise RuntimeError("enqueue lost its row")
                delivery_id = row[0]
            row = self._row(delivery_id)
        return row, created

    def claim_due(self, limit: int = 10, lease_seconds: float = 60.0,
                  now: float | None = None) -> builtins.list[Delivery]:
        """Take up to ``limit`` due deliveries for this worker.

        Due means pending and past its next attempt, or in flight with a lapsed lease (a worker
        died holding it). Attempts are incremented here, because an attempt is beginning: a crash
        mid-send has genuinely consumed a try, and pretending otherwise would retry forever.
        """
        stamp = time.time() if now is None else now
        claimed: list[int] = []
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                rows = self._conn.execute(
                    "SELECT id FROM deliveries"
                    " WHERE (status = ? AND next_attempt_at <= ?)"
                    "    OR (status = ? AND lease_until IS NOT NULL AND lease_until <= ?)"
                    " ORDER BY next_attempt_at ASC LIMIT ?",
                    (PENDING, stamp, IN_FLIGHT, stamp, limit)).fetchall()
                for (delivery_id,) in rows:
                    self._conn.execute(
                        "UPDATE deliveries SET status = ?, attempts = attempts + 1,"
                        " lease_until = ?, updated = ? WHERE id = ?",
                        (IN_FLIGHT, stamp + lease_seconds, stamp, delivery_id))
                    claimed.append(delivery_id)
                self._conn.execute("COMMIT")
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
        return [self._row(i) for i in claimed]

    def mark_delivered(self, delivery_id: int, status_code: int | None = None,
                       now: float | None = None) -> None:
        self._finish(delivery_id, DELIVERED, status_code, None, now)

    def mark_retry(self, delivery_id: int, status_code: int | None, error: str | None,
                   next_attempt_at: float, now: float | None = None) -> None:
        stamp = time.time() if now is None else now
        with self._lock:
            self._conn.execute(
                "UPDATE deliveries SET status = ?, last_status = ?, last_error = ?,"
                " next_attempt_at = ?, lease_until = NULL, updated = ? WHERE id = ?",
                (PENDING, status_code, error, next_attempt_at, stamp, delivery_id))

    def mark_dead(self, delivery_id: int, status_code: int | None = None,
                  error: str | None = None, now: float | None = None) -> None:
        self._finish(delivery_id, DEAD, status_code, error, now)

    def _finish(self, delivery_id: int, status: str, status_code: int | None,
                error: str | None, now: float | None) -> None:
        stamp = time.time() if now is None else now
        with self._lock:
            self._conn.execute(
                "UPDATE deliveries SET status = ?, last_status = ?, last_error = ?,"
                " lease_until = NULL, updated = ? WHERE id = ?",
                (status, status_code, error, stamp, delivery_id))

    def replay(self, delivery_id: int, now: float | None = None) -> Delivery:
        """Put a delivery back in the queue, keeping the attempt history visible.

        Attempts reset, because a replay is a deliberate new run rather than a continuation, but
        ``replays`` counts up so the log shows this delivery has been round before.
        """
        stamp = time.time() if now is None else now
        with self._lock:
            cursor = self._conn.execute(
                "UPDATE deliveries SET status = ?, attempts = 0, replays = replays + 1,"
                " next_attempt_at = ?, lease_until = NULL, updated = ? WHERE id = ?",
                (PENDING, stamp, stamp, delivery_id))
            if cursor.rowcount != 1:
                raise KeyError(f"no delivery with id {delivery_id!r}")
            row = self._row(delivery_id)
        return row

    # -- reading ---------------------------------------------------------------

    def get(self, delivery_id: int) -> Delivery:
        row = self._row(delivery_id)
        if row is None:
            raise KeyError(f"no delivery with id {delivery_id!r}")
        return row

    def list(self, status: str | None = None, limit: int = 50) -> builtins.list[Delivery]:
        with self._lock:
            if status:
                rows = self._conn.execute(
                    "SELECT id FROM deliveries WHERE status = ? ORDER BY id DESC LIMIT ?",
                    (status, limit)).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT id FROM deliveries ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [d for d in (self._row(r[0]) for r in rows) if d is not None]

    def query(self, status: str | None = None, endpoint: str | None = None,
              event: str | None = None, since: float | None = None,
              limit: int = 50) -> builtins.list[Delivery]:
        """Newest first. Every filter is optional, so this answers "what happened" questions
        without a second schema."""
        sql = ["SELECT id FROM deliveries"]
        where: list[str] = []
        params: list[Any] = []
        for column, value in (("status", status), ("endpoint", endpoint), ("event", event)):
            if value is not None:
                where.append("%s = ?" % column)
                params.append(value)
        if since is not None:
            where.append("created >= ?")
            params.append(since)
        if where:
            sql.append("WHERE " + " AND ".join(where))
        sql.append("ORDER BY id DESC LIMIT ?")
        params.append(int(limit))
        with self._lock:
            rows = self._conn.execute(" ".join(sql), params).fetchall()
        return [d for d in (self._row(r[0]) for r in rows) if d is not None]

    def dead_letters(self, limit: int = 50) -> builtins.list[Delivery]:
        return self.list(DEAD, limit)

    def stats(self, now: float | None = None) -> dict[str, Any]:
        stamp = time.time() if now is None else now
        with self._lock:
            counts = dict(self._conn.execute(
                "SELECT status, COUNT(*) FROM deliveries GROUP BY status").fetchall())
            oldest = self._conn.execute(
                "SELECT MIN(created) FROM deliveries WHERE status IN (?, ?)",
                (PENDING, IN_FLIGHT)).fetchone()[0]
        return {
            "pending": counts.get(PENDING, 0),
            "in_flight": counts.get(IN_FLIGHT, 0),
            "delivered": counts.get(DELIVERED, 0),
            "dead": counts.get(DEAD, 0),
            "oldest_waiting_seconds": round(stamp - oldest, 1) if oldest else None,
        }

    def _row(self, delivery_id: int) -> Delivery | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT id, endpoint, event, payload, status, attempts, next_attempt_at,"
                " lease_until, last_status, last_error, dedupe_key, replays, created, updated"
                " FROM deliveries WHERE id = ?", (delivery_id,)).fetchone()
        if row is None:
            return None
        return Delivery(
            id=row[0], endpoint=row[1], event=row[2], payload=json.loads(row[3]), status=row[4],
            attempts=row[5], next_attempt_at=row[6], lease_until=row[7], last_status=row[8],
            last_error=row[9], dedupe_key=row[10], replays=row[11], created=row[12],
            updated=row[13])

    def close(self) -> None:
        self._conn.close()
