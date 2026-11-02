import pytest

from webhook_outbox.backoff import DEAD, DELIVERED, RETRY, RetryPolicy, classify


def test_backoff_doubles_from_the_base():
    policy = RetryPolicy(base=1.0, factor=2.0, cap=3600.0, jitter=0.0)
    assert [policy.delay(n) for n in range(1, 6)] == [1.0, 2.0, 4.0, 8.0, 16.0]


def test_backoff_stops_at_the_cap():
    policy = RetryPolicy(base=1.0, factor=10.0, cap=60.0, jitter=0.0)
    assert policy.delay(6) == 60.0


def test_jitter_stays_within_its_band():
    policy = RetryPolicy(base=10.0, jitter=0.2)
    for pick in (0.0, 0.25, 0.5, 0.75, 1.0):
        assert 8.0 <= policy.delay(1, rand=lambda p=pick: p) <= 12.0


def test_jitter_never_goes_negative():
    policy = RetryPolicy(base=1.0, jitter=2.0)
    assert policy.delay(1, rand=lambda: 0.0) >= 0.0


def test_attempt_is_one_based():
    with pytest.raises(ValueError):
        RetryPolicy().delay(0)


def test_exhaustion_is_reached_at_max_attempts():
    policy = RetryPolicy(max_attempts=3)
    assert not policy.exhausted(2)
    assert policy.exhausted(3)


@pytest.mark.parametrize("status,expected", [
    (200, DELIVERED), (204, DELIVERED),
    (500, RETRY), (502, RETRY), (503, RETRY), (429, RETRY), (408, RETRY),
    (400, DEAD), (401, DEAD), (403, DEAD), (404, DEAD), (410, DEAD),
    (301, DEAD),
])
def test_statuses_are_classified(status, expected):
    assert classify(status) == expected


def test_no_response_is_worth_retrying():
    """Connection refused and timeouts mean the far end may simply be restarting."""
    assert classify(None, "URLError: Connection refused") == RETRY
    assert classify(None) == RETRY
