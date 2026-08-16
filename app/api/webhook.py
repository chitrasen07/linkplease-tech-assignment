"""Webhook receiver.

Contract: answer HTTP 200 fast (target < 200 ms) and never do network I/O here.
The endpoint validates, verifies the signature and writes one row; matching and
DM sending happen in background workers.
"""

from __future__ import annotations

import json
import logging

from fastapi import APIRouter, HTTPException, Request, Response, status
from pydantic import ValidationError
from starlette.concurrency import run_in_threadpool

from app.config import get_settings
from app.database import session_scope
from app.schemas import WebhookAck, WebhookEvent
from app.security import SIGNATURE_HEADER, verify_signature
from app.services.events import ingest_event

logger = logging.getLogger(__name__)

router = APIRouter(tags=["webhook"])


def _store(payload: WebhookEvent) -> bool:
    """Persist the event; returns True when it was a duplicate delivery."""
    with session_scope() as session:
        result = ingest_event(session, payload)
    return result.duplicate


@router.post("/webhook", response_model=WebhookAck)
async def receive_webhook(request: Request, response: Response) -> WebhookAck:
    settings = get_settings()
    raw_body = await request.body()

    if len(raw_body) > settings.max_webhook_body_bytes:
        raise HTTPException(status_code=413, detail="payload too large")

    if settings.verify_webhook_signature:
        provided = request.headers.get(SIGNATURE_HEADER)
        if not verify_signature(settings.signing_secret, raw_body, provided):
            logger.warning(
                "rejected webhook: %s signature (set VERIFY_WEBHOOK_SIGNATURE=false "
                "only if the sender does not sign requests)",
                "missing" if not provided else "invalid",
            )
            raise HTTPException(status_code=401, detail="invalid signature")

    try:
        parsed = json.loads(raw_body)
    except (ValueError, UnicodeDecodeError):
        raise HTTPException(status_code=400, detail="body must be valid JSON") from None

    if not isinstance(parsed, dict):
        raise HTTPException(status_code=400, detail="body must be a JSON object")

    try:
        payload = WebhookEvent.model_validate(parsed)
    except ValidationError as exc:
        logger.warning("rejected webhook: invalid payload (%d errors)", exc.error_count())
        raise HTTPException(status_code=400, detail="invalid webhook payload") from None

    try:
        duplicate = await run_in_threadpool(_store, payload)
    except Exception:
        # Returning 5xx keeps the event alive: the sender will retry, and
        # duplicate delivery is something we already handle.
        logger.exception("failed to persist event_id=%s", payload.event_id)
        raise HTTPException(status_code=503, detail="storage unavailable") from None

    logger.info(
        "webhook received event_id=%s type=%s duplicate=%s",
        payload.event_id,
        payload.event_type,
        duplicate,
    )
    response.status_code = status.HTTP_200_OK
    return WebhookAck(
        status="duplicate" if duplicate else "accepted",
        event_id=payload.event_id,
        duplicate=duplicate,
    )
