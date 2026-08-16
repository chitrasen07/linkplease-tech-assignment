"""The DM sending worker.

One coroutine claims one job at a time and sends it, which is also how the
10-sends-per-60-seconds limit is honoured: sends are serialised through a
single rate-limited path instead of 500 concurrent tasks.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

from app.config import Settings, get_settings
from app.database import session_scope
from app.models import DMJob, JobStatus
from app.services import jobs
from app.services.pseudogram import PseudoGramClient, SendOutcome, SendResult
from app.services.rate_limiter import RollingWindowRateLimiter

logger = logging.getLogger(__name__)


class DMWorker:
    def __init__(
        self,
        client: PseudoGramClient,
        rate_limiter: RollingWindowRateLimiter,
        settings: Settings | None = None,
        *,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._client = client
        self._limiter = rate_limiter
        self._settings = settings or get_settings()
        self._sleep = sleep

    # --- database helpers (run in a thread) -------------------------------
    @staticmethod
    def _claim() -> dict | None:
        with session_scope() as session:
            job = jobs.claim_next_job(session)
            if job is None:
                return None
            return {
                "id": job.id,
                "user_id": job.user_id,
                "message": job.message,
                "comment_id": job.comment_id,
                "idempotency_key": job.idempotency_key,
            }

    def _apply(self, job_id: int, result: SendResult) -> None:
        with session_scope() as session:
            job = session.get(DMJob, job_id)
            if job is None:
                logger.error("job disappeared while sending job_id=%s", job_id)
                return
            jobs.apply_send_result(session, job, result, self._settings)

    def _release_claim(self, job_id: int) -> None:
        """Unexpected crash while sending: put the job back so it is not lost."""
        with session_scope() as session:
            job = session.get(DMJob, job_id)
            if job is not None and job.status == JobStatus.SENDING:
                job.status = JobStatus.QUEUED

    # --- main loop --------------------------------------------------------
    async def run_once(self) -> bool:
        """Send at most one DM. Returns True if a job was processed."""
        if not self._client.configured:
            # Without a key every send would fail permanently, so leave the
            # jobs queued until one is configured instead of burning attempts.
            return False

        claimed = await asyncio.to_thread(self._claim)
        if claimed is None:
            return False

        job_id = claimed["id"]
        try:
            await self._limiter.acquire()
            result = await self._client.send_dm(
                recipient_user_id=claimed["user_id"],
                message=claimed["message"],
                comment_id=claimed["comment_id"],
                idempotency_key=claimed["idempotency_key"],
            )
            logger.info(
                "dm send job_id=%s outcome=%s status_code=%s",
                job_id,
                result.outcome,
                result.status_code,
            )
            await asyncio.to_thread(self._apply, job_id, result)

            if result.outcome is SendOutcome.AUTH_ERROR:
                # Every other queued job would hit the same wall.
                await self._sleep(self._settings.auth_error_pause_seconds)

            elif result.outcome is SendOutcome.RATE_LIMITED:
                # Defensive: the limiter should have prevented this, so trust
                # the server and stop sending for as long as it asked.
                delay = result.retry_after
                if delay is None:
                    delay = self._settings.send_rate_limit_window_seconds
                await self._limiter.penalise(delay)
        except asyncio.CancelledError:
            await asyncio.to_thread(self._release_claim, job_id)
            raise
        except Exception:
            logger.exception("unexpected error while sending job_id=%s", job_id)
            await asyncio.to_thread(self._release_claim, job_id)
        return True

    async def run_forever(self, stop_event: asyncio.Event) -> None:
        logger.info("dm worker started")
        try:
            while not stop_event.is_set():
                try:
                    did_work = await self.run_once()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # A single bad iteration must never kill the worker.
                    logger.exception("dm worker iteration failed")
                    did_work = False

                if not did_work:
                    await _wait(stop_event, self._settings.worker_poll_interval_seconds)
        finally:
            logger.info("dm worker stopped")


async def _wait(stop_event: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
    except TimeoutError:
        pass
