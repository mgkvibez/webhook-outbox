"""What a dashboard or an on-call engineer needs: the log, the counters, and the hook."""

import pytest

from webhook_outbox.engine import Outbox
from webhook_outbox.sender import DeliveryResult
from webhook_outbox.store import SQLiteStore


class FakeSender:
    def __init__(self, results):
        self.results = list(results)

    def send(self, endpoint, body, headers):
        return self.results.pop(0) if self.results else DeliveryResult(status=200)


@pytest.fixture
def store(tmp_path):
    return SQLiteStore(str(tmp_path / "o.db"))


def test_a_delivery_reports_attempt_then_delivered(store):
    seen = []
    outbox = Outbox(store, sender=FakeSender([DeliveryResult(status=200)]),
                    on_event=seen.append)
    outbox.enqueue("https://p/h", "charge.succeeded", {})
    outbox.deliver_due()
    assert [e["event"] for e in seen] == ["attempt", "delivered"]
    assert seen[1]["status"] == 200
    assert seen[0]["name"] == "charge.succeeded"


def test_a_retry_reports_why(store):
    seen = []
    outbox = Outbox(store, sender=FakeSender([DeliveryResult(status=502, error="bad gateway")]),
                    on_event=seen.append)
    outbox.enqueue("https://p/h", "e", {})
    outbox.deliver_due()
    assert [e["event"] for e in seen] == ["attempt", "retried"]
    assert seen[1]["status"] == 502 and "bad gateway" in seen[1]["error"]


def test_a_dead_letter_is_reported_as_dead(store):
    seen = []
    outbox = Outbox(store, sender=FakeSender([DeliveryResult(status=410)]), on_event=seen.append)
    outbox.enqueue("https://p/h", "e", {})
    outbox.deliver_due()
    assert seen[-1]["event"] == "dead"


def test_counters_track_each_attempt(store):
    outbox = Outbox(store, sender=FakeSender([DeliveryResult(status=200),
                                              DeliveryResult(status=500)]))
    outbox.enqueue("https://a/h", "e", {})
    outbox.enqueue("https://b/h", "e", {})
    outbox.deliver_due()
    assert outbox.counters == {"attempts": 2, "delivered": 1, "retried": 1, "dead": 0}


def test_a_hook_that_raises_does_not_stop_the_delivery(store):
    def broken(_event):
        raise RuntimeError("dashboard is down")

    outbox = Outbox(store, sender=FakeSender([DeliveryResult(status=200)]), on_event=broken)
    delivery, _ = outbox.enqueue("https://p/h", "e", {})
    summary = outbox.deliver_due()
    assert summary.delivered == 1
    assert store.get(delivery.id).status == "delivered"


def test_metrics_merge_worker_counters_with_queue_depth(store):
    outbox = Outbox(store, sender=FakeSender([DeliveryResult(status=200)]))
    outbox.enqueue("https://p/h", "e", {})
    outbox.enqueue("https://later/h", "e", {})
    outbox.clock = lambda: 100.0
    outbox.enqueue("https://later/h", "f", {})
    outbox.deliver_due(limit=1)
    m = outbox.metrics()
    assert m["delivered"] == 1 and m["attempts"] == 1
    assert m["pending"] + m["delivered"] == 3


def test_the_delivery_log_filters_by_status_endpoint_and_event(store):
    outbox = Outbox(store, sender=FakeSender([DeliveryResult(status=200),
                                              DeliveryResult(status=404)]))
    a, _ = outbox.enqueue("https://a/h", "charge.succeeded", {})
    b, _ = outbox.enqueue("https://b/h", "invoice.paid", {})
    outbox.deliver_due()

    assert [d.id for d in outbox.deliveries(endpoint="https://b/h")] == [b.id]
    assert [d.id for d in outbox.deliveries(event="charge.succeeded")] == [a.id]
    assert [d.id for d in outbox.deliveries(status="dead")] == [b.id]
    assert [d.id for d in outbox.deliveries(limit=1)] == [b.id]      # newest first
    assert outbox.deliveries(since=1e12) == []


def test_the_log_reports_replays(store):
    outbox = Outbox(store, sender=FakeSender([DeliveryResult(status=410),
                                              DeliveryResult(status=200)]))
    delivery, _ = outbox.enqueue("https://p/h", "e", {})
    outbox.deliver_due()
    outbox.replay(delivery.id)
    outbox.deliver_due()
    row = store.get(delivery.id)
    assert row.status == "delivered" and row.replays == 1
