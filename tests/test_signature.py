"""HMAC webhook signature verification."""

from __future__ import annotations

import hashlib
import hmac
import json

from app.security import compute_signature, verify_signature
from tests.conftest import TEST_API_KEY, make_event, post_webhook


def test_signature_matches_the_documented_formula():
    body = b'{"event_id":"evt_1"}'
    expected = hmac.new(TEST_API_KEY.encode(), body, hashlib.sha256).hexdigest()

    assert compute_signature(TEST_API_KEY, body) == expected


def test_verify_accepts_prefixed_and_bare_hex():
    body = b'{"a":1}'
    digest = compute_signature(TEST_API_KEY, body)

    assert verify_signature(TEST_API_KEY, body, f"sha256={digest}")
    assert verify_signature(TEST_API_KEY, body, digest)
    assert verify_signature(TEST_API_KEY, body, f"SHA256={digest.upper()}")


def test_verify_rejects_wrong_secret_and_tampered_body():
    body = b'{"a":1}'
    digest = compute_signature(TEST_API_KEY, body)

    assert not verify_signature("other-key", body, f"sha256={digest}")
    assert not verify_signature(TEST_API_KEY, b'{"a":2}', f"sha256={digest}")
    assert not verify_signature(TEST_API_KEY, body, None)
    assert not verify_signature("", body, f"sha256={digest}")


def test_valid_signature_is_accepted(client_factory):
    client = client_factory(verify_webhook_signature="true")
    with client:
        response = post_webhook(client, make_event("evt_signed"), secret=TEST_API_KEY)
    assert response.status_code == 200


def test_invalid_signature_is_rejected(client_factory):
    client = client_factory(verify_webhook_signature="true")
    with client:
        body = json.dumps(make_event("evt_bad")).encode()
        response = client.post(
            "/webhook",
            content=body,
            headers={"X-PseudoGram-Signature": "sha256=" + "0" * 64},
        )
    assert response.status_code == 401


def test_missing_signature_is_rejected_when_verification_is_on(client_factory):
    client = client_factory(verify_webhook_signature="true")
    with client:
        response = post_webhook(client, make_event("evt_unsigned"))
    assert response.status_code == 401


def test_signature_is_computed_over_the_raw_body(client_factory):
    """Re-serialised JSON (different spacing) must not validate."""
    client = client_factory(verify_webhook_signature="true")
    payload = make_event("evt_raw")
    reserialised = json.dumps(payload, separators=(", ", ": ")).encode()
    sent_body = json.dumps(payload, separators=(",", ":")).encode()
    digest = compute_signature(TEST_API_KEY, reserialised)

    with client:
        response = client.post(
            "/webhook",
            content=sent_body,
            headers={"X-PseudoGram-Signature": f"sha256={digest}"},
        )
    assert response.status_code == 401


def test_unsigned_requests_pass_when_verification_is_disabled(client):
    assert post_webhook(client, make_event("evt_open")).status_code == 200
