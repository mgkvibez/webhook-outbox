import pytest

from webhook_outbox.store import DEAD, DELIVERED, IN_FLIGHT, PENDING, SQLiteStore


@pytest.fixture
def store(tmp_path):
    s = SQLiteStore(str(tmp_path / "outbox.db"))
    yield s
    s.close()


def test_enqueue_records_intent(store):
    delivery, created = store.enqueue("https://x/hook", "charge.succeeded", {"id": 1})
    assert created and delivery.id == 1
    assert delivery.status == PENDING and delivery.attempts == 0


def test_the_same_dedupe_key_enqueues_once(store):
    first, created_first = store.enqueue("https://x", "e", {"n": 1}, dedupe_key="k")
    second, created_second = store.enqueue("https://x", "e", {"n": 1}, dedupe_key="k")
    assert created_first and not created_second
    assert first.id == second.id
    assert len(store.list()) == 1


def test_without_a_dedupe_key_every_enqueue_is_new(store):
    store.enqueue("https://x", "e", {"n": 1})
    store.enqueue("https://x", "e", {"n": 1})
    assert len(store.list()) == 2


def test_claiming_takes_a_delivery_and_counts_the_attempt(store):
    store.enqueue("https://x", "e", {}, now=100.0)
    claimed = store.claim_due(limit=5, lease_seconds=30, now=100.0)
    assert len(claimed) == 1
    assert claimed[0].attempts == 1 and claimed[0].lease_until == 130.0


def test_a_second_worker_does_not_get_the_same_delivery(store):
    store.enqueue("https://x", "e", {}, now=100.0)
    first = store.claim_due(limit=5, lease_seconds=30, now=100.0)
    second = store.claim_due(limit=5, lease_seconds=30, now=101.0)
    assert len(first) == 1 and second == []


def test_a_lapsed_lease_returns_the_delivery_to_the_queue(store):
    """A worker that dies mid-send must not strand the delivery forever."""
    store.enqueue("https://x", "e", {}, now=100.0)
    store.claim_due(limit=5, lease_seconds=30, now=100.0)
    recovered = store.claim_due(limit=5, lease_seconds=30, now=131.0)
    assert len(recovered) == 1 and recovered[0].attempts == 2


def test_a_future_attempt_is_not_claimed_early(store):
    delivery, _ = store.enqueue("https://x", "e", {}, now=100.0)
    store.claim_due(limit=5, now=100.0)
    store.mark_retry(delivery.id, 503, "boom", next_attempt_at=200.0, now=100.0)
    assert store.claim_due(limit=5, now=150.0) == []
    assert len(store.claim_due(limit=5, now=200.0)) == 1


def test_outcomes_are_recorded(store):
    delivery, _ = store.enqueue("https://x", "e", {})
    store.claim_due(limit=5)
    store.mark_delivered(delivery.id, 204)
    done = store.get(delivery.id)
    assert done.status == DELIVERED and done.last_status == 204 and done.lease_until is None

    store.replay(delivery.id)
    store.claim_due(limit=5)
    store.mark_dead(delivery.id, 404, "HTTP 404")
    buried = store.get(delivery.id)
    assert buried.status == DEAD and buried.last_error == "HTTP 404"


def test_replay_requeues_and_counts_itself(store):
    delivery, _ = store.enqueue("https://x", "e", {}, now=100.0)
    store.claim_due(limit=5, now=100.0)
    store.mark_dead(delivery.id, 400, "HTTP 400", now=100.0)
    replayed = store.replay(delivery.id, now=200.0)
    assert replayed.status == PENDING and replayed.attempts == 0 and replayed.replays == 1
    assert replayed.next_attempt_at == 200.0


def test_replaying_a_missing_delivery_raises(store):
    with pytest.raises(KeyError):
        store.replay(999)


def test_stats_count_by_state(store):
    a, _ = store.enqueue("https://x", "e", {}, now=100.0)
    store.enqueue("https://x", "e", {}, now=100.0)
    store.claim_due(limit=1, now=100.0)
    store.mark_delivered(a.id, 200, now=100.0)
    stats = store.stats(now=160.0)
    assert stats["delivered"] == 1 and stats["pending"] == 1
    assert stats["oldest_waiting_seconds"] == 60.0


def test_the_outbox_survives_a_reopen(tmp_path):
    path = str(tmp_path / "outbox.db")
    first = SQLiteStore(path)
    delivery, _ = first.enqueue("https://x", "e", {"n": 1})
    first.claim_due(limit=5)
    first.mark_delivered(delivery.id, 200)
    first.close()

    reopened = SQLiteStore(path)
    recovered = reopened.get(delivery.id)
    assert recovered.status == DELIVERED and recovered.payload == {"n": 1}
    reopened.close()


def test_the_signed_body_is_stable_across_reads(store):
    """Signing is computed from the body at send time, so it must not drift between attempts."""
    delivery, _ = store.enqueue("https://x", "e", {"b": 2, "a": 1})
    assert delivery.body == store.get(delivery.id).body
    assert delivery.body == b'{"data":{"a":1,"b":2},"event":"e"}'


def test_a_dead_delivery_is_listed_as_a_dead_letter(store):
    delivery, _ = store.enqueue("https://x", "e", {})
    store.mark_dead(delivery.id, 404, "HTTP 404")
    assert [d.id for d in store.dead_letters()] == [delivery.id]
    assert store.list(IN_FLIGHT) == []
