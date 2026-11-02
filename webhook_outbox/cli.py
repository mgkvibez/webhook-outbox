"""Command line for the outbox: queue a webhook, drain the queue, inspect what happened.

Exit contract (documented, and asserted by the gate): 0 nothing needs attention, 1 something
does (retries waiting, or dead letters), 2 the command was wrong.
"""

from __future__ import annotations

import argparse
import json
import sys
import time

from . import __version__
from .engine import Outbox
from .store import DEAD, PENDING, SQLiteStore

USAGE_ERROR = 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="outbox",
        description="Deliver outgoing webhooks exactly once: sign, retry, dead-letter, replay.")
    parser.add_argument("--version", action="version", version=f"outbox {__version__}")
    parser.add_argument("--db", default="outbox.db", help="outbox file (default: outbox.db)")
    sub = parser.add_subparsers(dest="command", required=True)

    enqueue = sub.add_parser("enqueue", help="record an outgoing webhook")
    enqueue.add_argument("--endpoint", required=True, help="URL to POST to")
    enqueue.add_argument("--event", required=True, help="event name, sent as X-Event")
    enqueue.add_argument("--payload", default=None, help="JSON payload")
    enqueue.add_argument("--payload-file", default=None, help="read the JSON payload from a file")
    enqueue.add_argument("--key", default=None,
                         help="dedupe key: the same key twice enqueues one delivery")
    enqueue.add_argument("--secret", default=None, help="sign with this secret")

    run = sub.add_parser("run", help="deliver everything that is due")
    run.add_argument("--once", action="store_true", help="one pass, then exit")
    run.add_argument("--limit", type=int, default=10, help="deliveries per pass (default: 10)")
    run.add_argument("--interval", type=float, default=5.0, help="seconds between passes")
    run.add_argument("--secret", default=None, help="sign with this secret")

    listing = sub.add_parser("list", help="show recent deliveries")
    listing.add_argument("--status", default=None, choices=[PENDING, "in_flight", "delivered", DEAD])
    listing.add_argument("--limit", type=int, default=20)

    log = sub.add_parser("log", help="everything about one delivery")
    log.add_argument("id", type=int)

    replay = sub.add_parser("replay", help="queue a delivery again")
    replay.add_argument("id", type=int)

    dead = sub.add_parser("dead", help="list the dead-letter queue")
    dead.add_argument("--limit", type=int, default=20)

    sub.add_parser("stats", help="counts by state")
    return parser


def _load_payload(args) -> dict:
    if args.payload_file:
        with open(args.payload_file, encoding="utf-8") as handle:
            return json.load(handle)
    if args.payload:
        return json.loads(args.payload)
    return {}


def cmd_enqueue(store, args) -> int:
    try:
        payload = _load_payload(args)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"outbox: could not read the payload: {exc}", file=sys.stderr)
        return USAGE_ERROR
    outbox = Outbox(store, secret=args.secret)
    delivery, created = outbox.enqueue(args.endpoint, args.event, payload, dedupe_key=args.key)
    print("%s delivery %d: %s -> %s" % ("queued" if created else "already queued",
                                        delivery.id, delivery.event, delivery.endpoint))
    return 0


def cmd_run(store, args) -> int:
    outbox = Outbox(store, secret=args.secret)
    problems = 0

    def once() -> int:
        summary = outbox.deliver_due(args.limit)
        if summary.claimed:
            print("pass: %d claimed, %d delivered, %d waiting, %d dead"
                  % (summary.claimed, summary.delivered, summary.retried, summary.dead))
            for note in summary.notes:
                print(f"  {note}")
        return 0 if summary.clean else 1

    if args.once:
        return once()
    try:
        while True:
            problems = max(problems, once())
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("stopped")
        return problems
    return problems


def cmd_list(store, args) -> int:
    deliveries = store.list(args.status, args.limit)
    if not deliveries:
        print("no deliveries" + (f" with status {args.status}" if args.status else ""))
        return 0
    for d in deliveries:
        print("#%-4d %-9s attempt %-2d %-22s %s%s"
              % (d.id, d.status, d.attempts, d.event[:22], d.endpoint[:48],
                 "" if d.last_error is None else f"  [{d.last_error[:40]}]"))
    return 0


def cmd_log(store, args) -> int:
    try:
        d = store.get(args.id)
    except KeyError as exc:
        print(f"outbox: {exc}", file=sys.stderr)
        return USAGE_ERROR
    print("delivery   #%d" % d.id)
    print(f"event      {d.event}")
    print(f"endpoint   {d.endpoint}")
    print("status     %s after %d attempt(s), %d replay(s)" % (d.status, d.attempts, d.replays))
    print("last       %s" % (d.last_status if d.last_status is not None else "-"))
    print("error      %s" % (d.last_error or "-"))
    print("dedupe key %s" % (d.dedupe_key or "-"))
    print("created    {}".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(d.created))))
    print("updated    {}".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(d.updated))))
    print("body       {}".format(d.body.decode("utf-8")))
    return 0


def cmd_replay(store, args) -> int:
    try:
        d = Outbox(store).replay(args.id)
    except KeyError as exc:
        print(f"outbox: {exc}", file=sys.stderr)
        return USAGE_ERROR
    print("replay queued for delivery #%d (%s) - it will be sent on the next run" % (d.id, d.event))
    return 0


def cmd_dead(store, args) -> int:
    dead = store.dead_letters(args.limit)
    if not dead:
        print("dead-letter queue is empty")
        return 0
    print("%d dead letter(s):" % len(dead))
    for d in dead:
        print("  #%-4d %-22s %-40s %s" % (d.id, d.event[:22], d.endpoint[:40],
                                          (d.last_error or "")[:40]))
    print("replay one with: outbox replay <id>")
    return 1


def cmd_stats(store, args) -> int:
    stats = store.stats()
    print("pending    %d" % stats["pending"])
    print("in flight  %d" % stats["in_flight"])
    print("delivered  %d" % stats["delivered"])
    print("dead       %d" % stats["dead"])
    if stats["oldest_waiting_seconds"] is not None:
        print("oldest waiting {:.0f}s".format(stats["oldest_waiting_seconds"]))
    return 0 if stats["dead"] == 0 else 1


HANDLERS = {
    "enqueue": cmd_enqueue, "run": cmd_run, "list": cmd_list, "log": cmd_log,
    "replay": cmd_replay, "dead": cmd_dead, "stats": cmd_stats,
}


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    store = SQLiteStore(args.db)
    try:
        return HANDLERS[args.command](store, args)
    finally:
        store.close()


if __name__ == "__main__":
    raise SystemExit(main())
