## v0.1.0 - 2026-11-02

- First release. `webhook-outbox` is the outbound half of the webhook problem: the transactional
  outbox pattern for outgoing webhooks, in one SQLite file, with no dependencies.
- `SQLiteStore` writes the intent before anything is sent. `dedupe_key` is UNIQUE, so enqueueing
  the same business event twice queues one delivery and returns the existing row.
- Claiming is a `BEGIN IMMEDIATE` transaction under a lease, so two workers cannot take the same
  delivery and a worker that dies mid-send does not strand it: the row is claimable once the lease
  lapses. Attempts increment at claim time, because a crash genuinely used a try.
- `RetryPolicy` is exponential backoff with jitter and a cap. `classify()` decides whether an answer
  is worth retrying at all: 5xx/429/408/no-response retry, 4xx dead-letters immediately, 3xx
  dead-letters because a redirecting webhook endpoint is misconfigured. Redirects are never followed.
- Signing is `X-Signature: t=<unix>,v1=<hex>` over `"<t>.<body>"`, HMAC-SHA256. `verify()` enforces
  a tolerance window, accepts any `v1` for key rotation, and the body is serialised with sorted keys
  so the signature stays valid across attempts.
- `replay()` requeues a dead or delivered row, resetting attempts and counting itself in `replays`.
- CLI: `enqueue`, `run` (`--once` or looping), `list`, `log`, `replay`, `dead`, `stats`, with a
  documented exit contract of 0 clean / 1 needs attention / 2 usage.
- `HttpSender` never raises: an unreachable host becomes a recorded error and a retry.
- 72 tests, including a real loopback server that verifies signatures with this package's own
  verifier. Gate has five checks and was run three times before the tag.
