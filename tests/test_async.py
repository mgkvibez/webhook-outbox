"""The async path must not behave differently from the synchronous one."""

import asyncio

import pytest

from webhook_outbox.engine import Outbox
from webhook_outbox.sender import DeliveryResult, HttpSender, ThreadedAsyncSender
from webhook_outbox.store import DEAD, DELIVERED, SQLiteStore


class FakeAsyncSender:
    def __init__(self, results):
        self.results = list(results)
        self.calls = []

    async def send(self, endpoint, body, headers):
        self.calls.append((endpoint, body, dict(headers)))
        return self.results.pop(0) if self.results else DeliveryResult(status=200)


class FakeSyncSender:
    def __init__(self, results=()):
        self.results = list(results)
        self.calls = []

    def send(self, endpoint, body, headers):
        self.calls.append(endpoint)
        return self.results.pop(0) if self.results else DeliveryResult(status=200)


@pytest.fixture
def store(tmp_path):
    return SQLiteStore(str(tmp_path / "a.db"))


def test_an_async_pass_delivers_and_records(store):
    outbox = Outbox(store, async_sender=FakeAsyncSender([DeliveryResult(status=201)]))
    delivery, _ = outbox.enqueue("https://p/h", "e", {"n": 1})
    summary = asyncio.run(outbox.deliver_due_async())
    assert summary.delivered == 1 and summary.clean
    assert store.get(delivery.id).status == DELIVERED
    assert store.get(delivery.id).last_status == 201


def test_an_async_server_error_is_retried_with_a_future_attempt(store):
    sender = FakeAsyncSender([DeliveryResult(status=503)])
    outbox = Outbox(store, async_sender=sender)
    outbox.clock = lambda: 5000.0
    delivery, _ = outbox.enqueue("https://p/h", "e", {})
    summary = asyncio.run(outbox.deliver_due_async())
    assert summary.retried == 1
    row = store.get(delivery.id)
    assert row.status != DEAD and row.next_attempt_at > 5000.0


def test_an_async_refusal_buries_it_instead_of_retrying(store):
    outbox = Outbox(store, async_sender=FakeAsyncSender([DeliveryResult(status=404)]))
    delivery, _ = outbox.enqueue("https://p/h", "e", {})
    summary = asyncio.run(outbox.deliver_due_async())
    assert summary.dead == 1 and not summary.clean
    assert store.get(delivery.id).status == DEAD


def test_an_async_delivery_is_signed_the_same_way(store):
    sender = FakeAsyncSender([DeliveryResult(status=200)])
    outbox = Outbox(store, secret="whsec_async", async_sender=sender)
    outbox.enqueue("https://p/h", "charge.succeeded", {"id": "ch_1"})
    asyncio.run(outbox.deliver_due_async())
    headers = sender.calls[0][2]
    assert headers["X-Signature"].startswith("t=")
    assert headers["X-Event"] == "charge.succeeded"


def test_the_async_sender_defaults_to_a_thread_over_the_sync_one(store):
    sync = FakeSyncSender()
    outbox = Outbox(store, sender=sync)
    assert isinstance(outbox.async_sender, ThreadedAsyncSender)
    assert outbox.async_sender.sender is sync


def test_the_default_threaded_sender_actually_sends_through_the_sync_sender(store):
    sync = FakeSyncSender([DeliveryResult(status=200)])
    outbox = Outbox(store, sender=sync)
    delivery, _ = outbox.enqueue("https://p/h", "e", {})
    asyncio.run(outbox.deliver_due_async())
    assert sync.calls == ["https://p/h"]
    assert store.get(delivery.id).status == DELIVERED


def test_two_concurrent_passes_do_not_send_the_same_delivery_twice(store):
    sender = FakeAsyncSender([DeliveryResult(status=200), DeliveryResult(status=200)])
    outbox = Outbox(store, async_sender=sender)
    first, _ = outbox.enqueue("https://p/h", "e", {"n": 1})
    second, _ = outbox.enqueue("https://p/h", "e", {"n": 2})

    async def both():
        return await asyncio.gather(outbox.deliver_due_async(limit=1),
                                    outbox.deliver_due_async(limit=1))

    a, b = asyncio.run(both())
    assert a.claimed + b.claimed == 2
    assert len(sender.calls) == 2
    assert store.get(first.id).status == DELIVERED
    assert store.get(second.id).status == DELIVERED


def test_a_sync_sender_with_a_real_http_sender_is_wrapped_not_replaced(store):
    outbox = Outbox(store)
    assert isinstance(outbox.sender, HttpSender)
    assert outbox.async_sender.sender is outbox.sender
