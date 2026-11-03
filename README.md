# webhook-outbox

Deliver outgoing webhooks exactly once: write the intent down, sign it, retry it, bury it in a
dead-letter queue, and replay it when the far end finally comes back.

[![CI](https://github.com/mgkvibez/webhook-outbox/actions/workflows/ci.yml/badge.svg)](https://github.com/mgkvibez/webhook-outbox/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.9%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)

Every SaaS that sends webhooks hand-rolls the same five things: sign the payload, retry with
backoff, dedupe, dead-letter, expose a delivery log. Servers exist for this (Svix, Convoy, Hook0).
In Python the honest options were a Django library tied to a task queue
([django-webhook](https://github.com/danihodovic/django-webhook), MIT, retries with backoff, quiet
since 2024) or `requests.post` inside a `try` beside whatever queue the app already runs.

What was missing is the small, dependency-free engine: durable before it sends, signed, deduplicating
by business key, ordered per endpoint, with a dead-letter queue you can replay from a shell. That is
this package, in the transactional-outbox shape: nothing is sent until the intent is written down,
so a crash between the charge and the notification loses neither.

This is a crowded space with serious in-house implementations, and that is the point: Saleor,
pretix, Frappe and Baserow each wrote their own before any library existed, and their design
decisions (Frappe's fixed retry schedule, Baserow's dead-letter state, Saleor's delivery metrics)
are worth reading before trusting this one. What is not common is doing all of it in one place,
without a framework, without a broker, and without a service to deploy.

## Install

```bash
pipx install webhook-outbox      # the `outbox` command
pip install webhook-outbox       # or as a library
```

No dependencies. Python 3.9+, standard library only.

## Use it

```python
from webhook_outbox import Outbox, SQLiteStore

outbox = Outbox(SQLiteStore("outbox.db"), secret="whsec_live_...")

outbox.enqueue(
    "https://partner.example/hooks",
    "charge.succeeded",
    {"id": "ch_1", "amount": 2500},
    dedupe_key="charge.succeeded:ch_1",   # the same event twice queues one delivery
)

outbox.deliver_due()                      # one pass; run it in a loop or from cron
```

From the shell:

```bash
outbox enqueue --endpoint https://partner.example/hooks --event charge.succeeded \
               --payload '{"id":"ch_1"}' --key charge.succeeded:ch_1
outbox run                 # drain the queue, looping
outbox list                # what is queued, waiting or dead
outbox log 7               # every attempt, the last status, the exact body
outbox dead                # the dead-letter queue
outbox replay 7            # try that one again
outbox stats               # counts by state
```

## What it does, proven

The demo runs a real HTTP server on loopback and verifies every signature with this package's own
verifier. One endpoint works, one fails twice and recovers, one is gone for good.

```
$ python examples/demo.py
enqueue twice with one key -> delivery 1 created=True then created=False
pass 1: claimed=3 delivered=1 waiting=1 dead=1
pass 2: claimed=1 delivered=0 waiting=1 dead=0
pass 3: claimed=1 delivered=1 waiting=0 dead=0
pass 4: claimed=0 delivered=0 waiting=0 dead=0

log:
  #4 dead      attempt 1  customer.deleted   HTTP 404 (not retried: the endpoint refused it)
  #3 delivered attempt 3  invoice.paid       
  #1 delivered attempt 1  charge.succeeded   

dead letter: #4 customer.deleted -> HTTP 404 (not retried: the endpoint refused it)
replayed #4, next pass:
  claimed=1 dead=1 (HTTP 404 (not retried: the endpoint refused it))

receiver: 6 signature(s) verified, 0 rejected
stats: {"pending": 0, "in_flight": 0, "delivered": 2, "dead": 1, "oldest_waiting_seconds": null}
```

## The decisions that matter

**Dedupe is a UNIQUE constraint, not a hope.** `dedupe_key` is unique in the table, so enqueueing
the same business event twice writes one row and returns the existing delivery. That is what makes
it safe to call from inside a retried job.

**Claiming is atomic.** A worker takes a delivery with `BEGIN IMMEDIATE` and holds a lease. Two
workers cannot both get the same row, and a worker that dies mid-send releases its row when the
lease lapses, so a delivery is never stranded. Attempts increment at claim time, because a crash
mid-send has genuinely used a try.

**Not everything deserves a retry.** `5xx`, `429`, `408` and no-response-at-all are retried with
exponential backoff and jitter. `4xx` and `3xx` are dead-lettered immediately: a `404` will still
be a `404` in an hour, and retrying it eight times only delays the truth. Redirects are not
followed at all, because a webhook endpoint that redirects is misconfigured and quietly delivering
somewhere else hides that.

**Signatures use the shape receivers already verify:** `X-Signature: t=<unix>,v1=<hex>` over
`"<t>.<body>"`, HMAC-SHA256, with the timestamp inside the signed content so a captured request
cannot be re-dated. `verify()` enforces a tolerance window and accepts any `v1` in the header, so a
secret can be rotated by signing with two keys during the changeover. The body is serialised with
sorted keys and read back identically on every attempt, because a signature over a body that drifts
between retries would fail for no reason.

**Failures are recorded, never swallowed.** A sender never raises: an unreachable host is an
ordinary outcome and becomes a retry with the error text stored on the row.

## In an async service

A FastAPI app must not block its event loop on a socket:

```python
outbox = Outbox(SQLiteStore("outbox.db"),
                secret=os.environ["WEBHOOK_SECRET"],
                async_sender=ThreadedAsyncSender())

await outbox.deliver_due_async()
```

`ThreadedAsyncSender` runs the synchronous sender in a thread, which is the whole fix. The
package still installs with no dependencies, and you can pass a real async client through
`async_sender=` the day you want one. Both passes share one claim-and-record path, so switching
between them cannot change how a delivery is recorded, retried or buried.

## Watching it in production

```python
events = []                                    # or a Counter, an OTel span, a log line
outbox = Outbox(store, on_event=events.append)

outbox.deliveries(status="dead")               # filter by status/endpoint/event/since
outbox.metrics()                               # {"attempts": 4, "delivered": 3, "pending": 0}
```

Events are `attempt`, `delivered`, `retried` and `dead`, each carrying the delivery id, endpoint,
event name, attempt number, status and error. A hook that raises is swallowed on purpose: a
broken dashboard must never take deliveries down with it.

## Exit contract

`0` nothing needs attention, `1` something does (retries waiting or dead letters), `2` the command
was wrong. So `outbox dead` in a cron job tells you about the dead-letter queue without parsing
output.

## What this is not

Not a delivery service: no dashboard, no multi-tenant queues, no per-endpoint rate limiting. One
SQLite file and a worker is the whole product, which is enough for the common case and honest about
the rest. It pairs with [`idemcheck`](https://github.com/mgkvibez/idemcheck), which guards the
*inbound* side of the same problem: that project stops a retried request from charging twice, this
one stops a notification from being lost or sent five times.

## Tests

```bash
python -m pytest -q        # 88 tests
bash scripts/verify.sh     # the gate: imports, version, tests, CLI contract, zero deps
```

The suite drives bare fakes and a real loopback server; nothing is mocked at the level that would
hide a broken signature.

## Contributing

Issues and PRs welcome. Run `bash scripts/verify.sh` before opening a PR; it is the same script CI
runs. Keep the zero-dependency promise: this package ships no runtime dependencies on purpose.

## License

MIT
$ python examples/demo.py
```
enqueue twice with one key -> delivery 1 created=True then created=False
pass 1: claimed=3 delivered=1 waiting=1 dead=1
pass 2: claimed=1 delivered=0 waiting=1 dead=0
pass 3: claimed=1 delivered=1 waiting=0 dead=0
pass 4: claimed=0 delivered=0 waiting=0 dead=0

log:
  #4 dead      attempt 1  customer.deleted   HTTP 404 (not retried: the endpoint refused it)
  #3 delivered attempt 3  invoice.paid       
  #1 delivered attempt 1  charge.succeeded   

dead letter: #4 customer.deleted -> HTTP 404 (not retried: the endpoint refused it)
replayed #4, next pass:
  claimed=1 dead=1 (HTTP 404 (not retried: the endpoint refused it))

receiver: 6 signature(s) verified, 0 rejected
stats: {"pending": 0, "in_flight": 0, "delivered": 2, "dead": 1, "oldest_waiting_seconds": null}
outbox file: outbox.db
```
