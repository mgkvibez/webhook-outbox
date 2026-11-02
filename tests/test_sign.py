import pytest

from webhook_outbox.sign import SignatureError, parse, sign, verify

SECRET = "whsec_test"


def test_the_same_body_and_timestamp_sign_identically():
    assert sign(SECRET, b'{"a":1}', timestamp=1000) == sign(SECRET, b'{"a":1}', timestamp=1000)


def test_a_different_body_does_not_verify():
    header = sign(SECRET, b'{"a":1}', timestamp=1000)
    with pytest.raises(SignatureError, match="does not match"):
        verify(SECRET, b'{"a":2}', header, now=1000)


def test_a_different_secret_does_not_verify():
    header = sign("other", b'{"a":1}', timestamp=1000)
    with pytest.raises(SignatureError):
        verify(SECRET, b'{"a":1}', header, now=1000)


def test_a_stale_signature_is_refused():
    header = sign(SECRET, b'{}', timestamp=1000)
    with pytest.raises(SignatureError, match="outside the"):
        verify(SECRET, b'{}', header, tolerance=300, now=1000 + 301)


def test_a_fresh_signature_inside_the_window_is_accepted():
    header = sign(SECRET, b'{}', timestamp=1000)
    verify(SECRET, b'{}', header, tolerance=300, now=1000 + 299)


def test_the_timestamp_is_part_of_what_is_signed():
    """An attacker cannot re-date a captured request without breaking the signature."""
    header = sign(SECRET, b'{}', timestamp=1000)
    retimed = header.replace("t=1000", "t=2000")
    with pytest.raises(SignatureError, match="does not match"):
        verify(SECRET, b'{}', retimed, tolerance=0, now=2000)


def test_any_matching_v1_is_accepted_so_keys_can_rotate():
    old = sign("old", b'{}', timestamp=1000).split("v1=")[1]
    new = sign(SECRET, b'{}', timestamp=1000).split("v1=")[1]
    verify(SECRET, b'{}', f"t=1000,v1={old},v1={new}", now=1000)


def test_a_malformed_header_is_rejected_clearly():
    for header, message in [("", "empty"), ("v1=abc", "no timestamp"),
                            ("t=1000", "no v1"), ("t=abc,v1=1", "not an integer")]:
        with pytest.raises(SignatureError, match=message):
            verify(SECRET, b'{}', header)


def test_parse_returns_every_signature():
    stamp, signatures = parse("t=5,v1=aa,v1=bb")
    assert stamp == 5 and signatures == ["aa", "bb"]
