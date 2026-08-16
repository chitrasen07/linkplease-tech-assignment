"""Delivery reconciliation.

HTTP 202 only means PseudoGram accepted the DM; roughly 15% of accepted DMs
eventually fail. Nothing is counted as `sent` until GET /v1/dm/{dm_id} says
`delivered`. These GETs do not count against the send rate limit.
"""

from __future__ import annotations

import asyncio
import logging

from sqlalchemy import select

from app.config import Settings, get_settings
from app.database import session_scope
from app.models import DMJob, JobStatus
from app.services import jobs
from app.services.pseudogram import PseudoGramClient, StatusOutcome

logger = logging.getLogger(__name__)


class ReconciliationService:
    def __init__(
        self,
        client: PseudoGramClient,
        settings: Settings | None = None,
        *,
        batch_size: int = 50,
    ) -> None:
        self._client = client
        self._settings = settings or get_settings()
        self._batch_size = batch_size

    @staticmethod
    def _load_pending(batch_size: int) -> list[tuple[int, str]]:
        with session_scope() as session:
            rows = session.execute(
                select(DMJob.id, DMJob.dm_id)
                .where(DMJob.status == JobStatus.ACCEPTED, DMJob.dm_id.is_not(None))
                .order_by(DMJob.updated_at)
                .limit(batch_size)
            ).all()
        return [(int(job_id), str(dm_id)) for job_id, dm_id in rows]

    def _apply(self, job_id: int, status: str | None) -> None:
        with session_scope() as session:
            job = session.get(DMJob, job_id)
            if job is None or job.status != JobStatus.ACCEPTED:
                return
            jobs.apply_delivery_status(session, job, status, self._settings)

    async def run_once(self) -> int:
        """Poll every accepted job once. Returns the number checked."""
        pending = await asyncio.to_thread(self._load_pending, self._batch_size)
        checked = 0
        for job_id, dm_id in pending:
            result = await self._client.get_dm_status(dm_id)
            if result.outcome is StatusOutcome.OK:
                await asyncio.to_thread(self._apply, job_id, result.status)
            elif result.outcome is StatusOutcome.NOT_FOUND:
                logger.warning(
                    "dm_id unknown to PseudoGram job_id=%s dm_id=%s", job_id, dm_id
                )
            else:
                # Transient problem reading status: try again next cycle. The
                # job stays `accepted`, so nothing is lost.
                logger.warning(
                    "delivery status check failed job_id=%s dm_id=%s error=%s",
                    job_id,
                    dm_id,
                    result.error,
                )
            checked += 1
        return checked
