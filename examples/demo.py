"""A whole delivery lifecycle against a real local HTTP server, offline.

The receiver verifies every signature with this package's own verifier, so "it is signed" is
proven rather than asserted. One endpoint works, one fails twice then recovers, one is gone.
"""

import json
import os
import pathlib
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))   # run from anywhere

from webhook_outbox import Outbox, RetryPolicy, SQLiteStore, verify

SECRET = "whsec_demo"
state = {"valid": 0, "invalid": 0, "flaky": 0}


class Receiver(BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("content-length") or 0))
        try:
            verify(SECRET, body, self.headers.get("X-Signature", ""))
            state["valid"] += 1
        except Exception:
            state["invalid"] += 1
        if self.path == "/gone":
            code = 404                      # never coming back: bury it
        elif self.path == "/flaky":
            state["flaky"] += 1
            code = 500 if state["flaky"] < 3 else 200
        else:
            code = 200
        self.send_response(code)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    server = ThreadingHTTPServer(("127.0.0.1", 0), Receiver)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = "http://127.0.0.1:%d" % server.server_address[1]

    db = str(pathlib.Path(tempfile.mkdtemp()) / "outbox.db")
    store = SQLiteStore(db)
    outbox = Outbox(store, secret=SECRET,
                    policy=RetryPolicy(max_attempts=5, base=0.15, jitter=0.0),
                    lease_seconds=5)

    first, created = outbox.enqueue(base + "/ok", "charge.succeeded", {"id": "ch_1"},
                                    dedupe_key="charge.succeeded:ch_1")
    again, created_again = outbox.enqueue(base + "/ok", "charge.succeeded", {"id": "ch_1"},
                                          dedupe_key="charge.succeeded:ch_1")
    print("enqueue twice with one key -> delivery %d created=%s then created=%s"
          % (first.id, created, created_again))
    outbox.enqueue(base + "/flaky", "invoice.paid", {"id": "in_9"})
    outbox.enqueue(base + "/gone", "customer.deleted", {"id": "cus_3"})

    for attempt in (1, 2, 3, 4):
        summary = outbox.deliver_due()
        print("pass %d: claimed=%d delivered=%d waiting=%d dead=%d"
              % (attempt, summary.claimed, summary.delivered, summary.retried, summary.dead))
        time.sleep(0.4)

    print("\nlog:")
    for d in store.list(limit=10):
        print("  #%d %-9s attempt %d  %-18s %s"
              % (d.id, d.status, d.attempts, d.event, d.last_error or ""))

    dead = outbox.dead_letters()
    print("\ndead letter: #%d %s -> %s" % (dead[0].id, dead[0].event, dead[0].last_error))
    outbox.replay(dead[0].id)
    print("replayed #%d, next pass:" % dead[0].id)
    summary = outbox.deliver_due()
    print("  claimed=%d dead=%d (%s)"
          % (summary.claimed, summary.dead, store.get(dead[0].id).last_error))

    print("\nreceiver: %d signature(s) verified, %d rejected"
          % (state["valid"], state["invalid"]))
    print("stats:", json.dumps(outbox.stats()))
    print("outbox file:", os.path.basename(db))
    store.close()
    server.shutdown()
