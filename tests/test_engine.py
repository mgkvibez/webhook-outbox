import pytest

from webhook_outbox.backoff import RetryPolicy
from webhook_outbox.engine import Outbox
from webhook_outbox.sender import DeliveryResult
from webhook_outbox.sign import verify
from webhook_outbox.store import DEAD, DELIVERED, PENDING, SQLiteStore

SECRET = "whsec_test"


class FakeSender:
    """Records what would have been sent and returns scripted answers."""

    def __init__(self, results=None):
        self.results = list(results or [DeliveryResult(status=200)])
        self.calls = []

    def send(self, endpoint, body, headers):
        self.calls.append({"endpoint": endpoint, "body": body, "headers": dict(headers)})
        if len(self.results) > 1:
            return self.results.pop(0)
        return self.results[0]


@pytest.fixture
def store(tmp_path):
    s = SQLiteStore(str(tmp_path / "outbox.db"))
    yield s
    s.close()


def make_outbox(store, results=None, **kwargs):
    sender = FakeSender(results)
    return Outbox(store, sender=sender, secret=kwargs.pop("secret", SECRET),
                  policy=kwargs.pop("policy", RetryPolicy(max_attempts=3, base=1.0, jitter=0.0)),
                  **kwargs), sender


def test_a_200_marks_the_delivery_done(store):
    outbox, sender = make_outbox(store)
    delivery, _ = outbox.enqueue("https://partner/hook", "charge.succeeded", {"id": "ch_1"})
    summary = outbox.deliver_due()
    assert summary.claimed == 1 and summary.delivered == 1 and summary.clean
    assert store.get(delivery.id).status == DELIVERED
    assert sender.calls[0]["endpoint"] == "https://partner/hook"


def test_the_receiver_gets_the_event_and_the_delivery_id(store):
    outbox, sender = make_outbox(store)
    delivery, _ = outbox.enqueue("https://partner/hook", "charge.succeeded", {"id": 1})
    outbox.deliver_due()
    headers = sender.calls[0]["headers"]
    assert headers["X-Event"] == "charge.succeeded"
    assert headers["X-Delivery-Id"] == str(delivery.id)
    assert headers["X-Delivery-Attempt"] == "1"


def test_the_body_is_signed_with_the_secret(store):
    outbox, sender = make_outbox(store)
    outbox.enqueue("https://partner/hook", "charge.succeeded", {"id": "ch_1"})
    outbox.deliver_due()
    call = sender.calls[0]
    verify(SECRET, call["body"], call["headers"]["X-Signature"])


def test_without_a_secret_nothing_is_signed(store):
    outbox, sender = make_outbox(store, secret=None)
    outbox.enqueue("https://partner/hook", "e", {})
    outbox.deliver_due()
    assert "X-Signature" not in sender.calls[0]["headers"]


def test_a_secret_lookup_can_differ_per_endpoint(store):
    outbox, sender = make_outbox(store, secret=lambda url: "s1" if "one" in url else "s2")
    outbox.enqueue("https://one/hook", "e", {})
    outbox.deliver_due()
    verify("s1", sender.calls[0]["body"], sender.calls[0]["headers"]["X-Signature"])


def test_a_503_is_retried_with_a_future_attempt(store):
    outbox, sender = make_outbox(store, [DeliveryResult(status=503)])
    delivery, _ = outbox.enqueue("https://partner/hook", "e", {})
    summary = outbox.deliver_due()
    assert summary.retried == 1 and not summary.clean
    waiting = store.get(delivery.id)
    assert waiting.status == PENDING and waiting.attempts == 1
    assert waiting.next_attempt_at > waiting.updated


def test_a_timeout_is_retried(store):
    outbox, _ = make_outbox(store, [DeliveryResult(error="URLError: Connection refused")])
    delivery, _ = outbox.enqueue("https://partner/hook", "e", {})
    outbox.deliver_due()
    assert store.get(delivery.id).status == PENDING


def test_a_404_is_dead_immediately_rather_than_retried(store):
    outbox, sender = make_outbox(store, [DeliveryResult(status=404)])
    delivery, _ = outbox.enqueue("https://partner/hook", "e", {})
    summary = outbox.deliver_due()
    assert summary.dead == 1
    buried = store.get(delivery.id)
    assert buried.status == DEAD and "not retried" in buried.last_error
    assert len(sender.calls) == 1


def test_attempts_are_bounded_then_it_is_buried(store):
    outbox, sender = make_outbox(store, [DeliveryResult(status=500)])
    clock = {"now": 1000.0}
    outbox.clock = lambda: clock["now"]
    delivery, _ = outbox.enqueue("https://partner/hook", "e", {})
    for _ in range(3):
        outbox.deliver_due()
        clock["now"] += 100.0                     # jump past every backoff
    buried = store.get(delivery.id)
    assert buried.status == DEAD and buried.attempts == 3
    assert "gave up after 3 attempts" in buried.last_error
    assert len(sender.calls) == 3


def test_a_flaky_endpoint_that_recovers_is_delivered(store):
    outbox, sender = make_outbox(store, [DeliveryResult(status=500), DeliveryResult(status=200)])
    clock = {"now": 1000.0}
    outbox.clock = lambda: clock["now"]
    delivery, _ = outbox.enqueue("https://partner/hook", "e", {})
    outbox.deliver_due()
    clock["now"] += 100.0
    summary = outbox.deliver_due()
    assert summary.delivered == 1 and summary.clean
    assert store.get(delivery.id).status == DELIVERED


def test_the_same_business_event_queues_one_delivery(store):
    outbox, _ = make_outbox(store)
    first, created = outbox.enqueue("https://partner/hook", "e", {"id": 1}, dedupe_key="e:1")
    second, again = outbox.enqueue("https://partner/hook", "e", {"id": 1}, dedupe_key="e:1")
    assert created and not again and first.id == second.id
    assert len(store.list()) == 1


def test_one_failing_endpoint_does_not_stop_the_rest(store):
    outbox, sender = make_outbox(store, [DeliveryResult(status=503), DeliveryResult(status=200)])
    store.enqueue("https://a/hook", "e", {})
    store.enqueue("https://b/hook", "e", {})
    summary = outbox.deliver_due()
    assert summary.claimed == 2 and summary.delivered == 1 and summary.retried == 1


def test_replay_sends_a_dead_delivery_again(store):
    outbox, sender = make_outbox(store, [DeliveryResult(status=404), DeliveryResult(status=200)])
    delivery, _ = outbox.enqueue("https://partner/hook", "e", {})
    outbox.deliver_due()
    assert store.get(delivery.id).status == DEAD
    outbox.replay(delivery.id)
    assert outbox.deliver_due().delivered == 1
    assert store.get(delivery.id).status == DELIVERED


def test_the_summary_reports_what_happened(store):
    outbox, _ = make_outbox(store)
    store.enqueue("https://a/hook", "e", {})
    summary = outbox.deliver_due()
    assert summary.claimed == 1 and summary.delivered == 1
    assert summary.clean
