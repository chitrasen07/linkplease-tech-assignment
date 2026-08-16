"""HTTP client for the PseudoGram API.

The client only translates HTTP into small result objects - it makes no policy
decisions about retrying, rate limiting or state transitions. That keeps it
trivially replaceable by a fake in tests.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import httpx

from app.config import Settings, get_settings

logger = logging.getLogger(__name__)


class SendOutcome(StrEnum):
    ACCEPTED = "accepted"  # 202/200: PseudoGram queued the DM
    RATE_LIMITED = "rate_limited"  # 429: back off for Retry-After seconds
    SERVER_ERROR = "server_error"  # 5xx: safe to retry
    INVALID = "invalid"  # 4xx we caused: never retry
    AUTH_ERROR = "auth_error"  # 401/403: bad key, retry once it is fixed
    TRANSPORT_ERROR = "transport_error"  # timeout/connection: retry, may have landed
    UNEXPECTED = "unexpected"


class StatusOutcome(StrEnum):
    OK = "ok"
    NOT_FOUND = "not_found"
    ERROR = "error"


@dataclass(slots=True)
class SendResult:
    outcome: SendOutcome
    dm_id: str | None = None
    status_code: int | None = None
    retry_after: float | None = None
    error: str | None = None


@dataclass(slots=True)
class StatusResult:
    outcome: StatusOutcome
    status: str | None = None  # queued | delivered | failed
    status_code: int | None = None
    error: str | None = None


def _parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None


def _safe_json(response: httpx.Response) -> dict[str, Any]:
    try:
        payload = response.json()
    except ValueError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _truncate(text: str, limit: int = 300) -> str:
    return text if len(text) <= limit else text[:limit] + "..."


class PseudoGramClient:
    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._client: httpx.AsyncClient | None = None

    @property
    def configured(self) -> bool:
        return bool(self._settings.pseudogram_api_key)

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self._settings.pseudogram_base_url.rstrip("/"),
                timeout=self._settings.pseudogram_timeout_seconds,
                headers={"X-API-Key": self._settings.pseudogram_api_key},
            )
        return self._client

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def send_dm(
        self,
        *,
        recipient_user_id: str,
        message: str,
        comment_id: str | None,
        idempotency_key: str,
    ) -> SendResult:
        if not self.configured:
            return SendResult(
                outcome=SendOutcome.INVALID,
                error="PSEUDOGRAM_API_KEY is not configured",
            )

        payload = {
            "recipient_user_id": recipient_user_id,
            "message": message,
            "comment_id": comment_id,
        }
        try:
            response = await self._get_client().post(
                "/v1/dm/send",
                json=payload,
                headers={"Idempotency-Key": idempotency_key},
            )
        except httpx.HTTPError as exc:
            # The request may or may not have reached PseudoGram. Retrying with
            # the same Idempotency-Key is what makes that ambiguity harmless.
            return SendResult(
                outcome=SendOutcome.TRANSPORT_ERROR, error=f"{type(exc).__name__}: {exc}"
            )

        code = response.status_code
        body = _safe_json(response)

        if code in (200, 201, 202):
            return SendResult(
                outcome=SendOutcome.ACCEPTED,
                dm_id=body.get("dm_id"),
                status_code=code,
            )
        if code == 429:
            return SendResult(
                outcome=SendOutcome.RATE_LIMITED,
                status_code=code,
                retry_after=_parse_retry_after(response.headers.get("Retry-After")),
                error=body.get("error", "rate_limited"),
            )
        if code >= 500:
            return SendResult(
                outcome=SendOutcome.SERVER_ERROR,
                status_code=code,
                error=body.get("error") or _truncate(response.text),
            )
        if code in (401, 403):
            # A bad or missing API key is a configuration problem: the job is
            # fine and must not be spent on attempts while we are locked out.
            return SendResult(
                outcome=SendOutcome.AUTH_ERROR,
                status_code=code,
                error=body.get("detail") or body.get("error") or "unauthorised",
            )
        if code in (408, 409):
            return SendResult(
                outcome=SendOutcome.SERVER_ERROR,
                status_code=code,
                error=body.get("error") or _truncate(response.text),
            )
        if 400 <= code < 500:
            return SendResult(
                outcome=SendOutcome.INVALID,
                status_code=code,
                error=body.get("detail") or body.get("error") or _truncate(response.text),
            )
        return SendResult(
            outcome=SendOutcome.UNEXPECTED,
            status_code=code,
            error=_truncate(response.text),
        )

    async def get_dm_status(self, dm_id: str) -> StatusResult:
        """GET requests do not count against the send rate limit."""
        if not self.configured:
            return StatusResult(
                outcome=StatusOutcome.ERROR, error="PSEUDOGRAM_API_KEY is not configured"
            )
        try:
            response = await self._get_client().get(f"/v1/dm/{dm_id}")
        except httpx.HTTPError as exc:
            return StatusResult(
                outcome=StatusOutcome.ERROR, error=f"{type(exc).__name__}: {exc}"
            )

        if response.status_code == 404:
            return StatusResult(outcome=StatusOutcome.NOT_FOUND, status_code=404)
        if response.status_code >= 400:
            return StatusResult(
                outcome=StatusOutcome.ERROR,
                status_code=response.status_code,
                error=_truncate(response.text),
            )
        body = _safe_json(response)
        return StatusResult(
            outcome=StatusOutcome.OK,
            status=body.get("status"),
            status_code=response.status_code,
        )
