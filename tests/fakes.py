"""In-memory stand-ins for the PseudoGram API.

Unit tests never touch the network: the real client is replaced by these fakes
so every failure mode (202, 400, 429, 500, timeouts, delivery failures) can be
reproduced deterministically.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable

from app.services.pseudogram import (
    SendOutcome,
    SendResult,
    StatusOutcome,
    StatusResult,
)


def accepted(dm_id: str = "dm_test") -> SendResult:
    return SendResult(outcome=SendOutcome.ACCEPTED, dm_id=dm_id, status_code=202)


def server_error() -> SendResult:
    return SendResult(
        outcome=SendOutcome.SERVER_ERROR, status_code=500, error="internal_error"
    )


def invalid_request(detail: str = "bad recipient") -> SendResult:
    return SendResult(outcome=SendOutcome.INVALID, status_code=400, error=detail)


def rate_limited(retry_after: float = 7.0) -> SendResult:
    return SendResult(
        outcome=SendOutcome.RATE_LIMITED,
        status_code=429,
        retry_after=retry_after,
        error="rate_limited",
    )


def auth_error() -> SendResult:
    return SendResult(
        outcome=SendOutcome.AUTH_ERROR, status_code=401, error="Malformed API key"
    )


def transport_error() -> SendResult:
    return SendResult(outcome=SendOutcome.TRANSPORT_ERROR, error="ConnectTimeout")


class FakePseudoGramClient:
    """Scripted client. `send_results` is consumed in order; the final entry is
    reused once exhausted."""

    def __init__(
        self,
        send_results: Iterable[SendResult] | Callable[[dict], SendResult] | None = None,
        statuses: dict[str, list[str]] | None = None,
    ) -> None:
        self._send_results = (
            list(send_results) if isinstance(send_results, Iterable) else send_results
        )
        self._statuses = statuses or {}
        self.sends: list[dict] = []
        self.status_calls: list[str] = []
        self.configured = True

    async def send_dm(
        self,
        *,
        recipient_user_id: str,
        message: str,
        comment_id: str | None,
        idempotency_key: str,
    ) -> SendResult:
        call = {
            "recipient_user_id": recipient_user_id,
            "message": message,
            "comment_id": comment_id,
            "idempotency_key": idempotency_key,
        }
        self.sends.append(call)

        if self._send_results is None:
            return accepted(f"dm_{len(self.sends)}")
        if callable(self._send_results):
            return self._send_results(call)
        if not self._send_results:
            return accepted(f"dm_{len(self.sends)}")
        if len(self._send_results) == 1:
            return self._send_results[0]
        return self._send_results.pop(0)

    async def get_dm_status(self, dm_id: str) -> StatusResult:
        self.status_calls.append(dm_id)
        sequence = self._statuses.get(dm_id)
        if sequence is None:
            return StatusResult(
                outcome=StatusOutcome.OK, status="queued", status_code=200
            )
        value = sequence.pop(0) if len(sequence) > 1 else sequence[0]
        if value == "__error__":
            return StatusResult(outcome=StatusOutcome.ERROR, error="boom")
        if value == "__missing__":
            return StatusResult(outcome=StatusOutcome.NOT_FOUND, status_code=404)
        return StatusResult(outcome=StatusOutcome.OK, status=value, status_code=200)

    async def aclose(self) -> None:
        return None
