"""Webhook signature verification.

    X-PseudoGram-Signature: sha256=<hex>
    hex = HMAC-SHA256(raw_request_body, API_KEY)

The *raw* body bytes are hashed. Parsing JSON and re-serialising it would
change whitespace/key order and break every signature.
"""

from __future__ import annotations

import hashlib
import hmac

SIGNATURE_HEADER = "X-PseudoGram-Signature"
_PREFIX = "sha256="


def compute_signature(secret: str, raw_body: bytes) -> str:
    return hmac.new(secret.encode(), raw_body, hashlib.sha256).hexdigest()


def signature_header_value(secret: str, raw_body: bytes) -> str:
    return _PREFIX + compute_signature(secret, raw_body)


def verify_signature(secret: str, raw_body: bytes, header_value: str | None) -> bool:
    if not secret or not header_value:
        return False

    provided = header_value.strip()
    if provided.lower().startswith(_PREFIX):
        provided = provided[len(_PREFIX) :]

    expected = compute_signature(secret, raw_body)
    # Constant-time comparison: a plain `==` leaks how much of the digest matched.
    return hmac.compare_digest(expected, provided.lower())
