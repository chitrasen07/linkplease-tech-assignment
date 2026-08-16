"""Owns the background tasks and their clean shutdown."""

from __future__ import annotations

import asyncio
import logging

from app.config import Settings, get_settings
from app.database import session_scope
from app.services import counters, events, jobs
from app.services.pseudogram import PseudoGramClient
from app.services.rate_limiter import RollingWindowRateLimiter, SendAttemptStore
from app.services.reconciliation import ReconciliationService
from app.workers.dm_worker import DMWorker
from app.workers.event_worker import EventWorker

logger = logging.getLogger(__name__)


class WorkerManager:
    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self.client = PseudoGramClient(self._settings)
        self.rate_limiter = RollingWindowRateLimiter(
            max_calls=self._settings.send_rate_limit_max_calls,
            window_seconds=self._settings.send_rate_limit_window_seconds,
            safety_seconds=self._settings.send_rate_limit_safety_seconds,
            store=SendAttemptStore(),
        )
        self.dm_worker = DMWorker(self.client, self.rate_limiter, self._settings)
        self.event_worker = EventWorker(self._settings)
        self.reconciler = ReconciliationService(self.client, self._settings)

    def recover_state(self) -> None:
        """Undo the effects of an unclean shutdown before starting workers."""
        with session_scope() as session:
            counters.seed_counters(session)
            events.requeue_stuck_events(session)
            jobs.requeue_stale_sending(session, stale_seconds=0)

    async def start(self) -> None:
        await asyncio.to_thread(self.recover_state)
        await self.rate_limiter.warm_start()
        self._stop.clear()
        self._tasks = [
            asyncio.create_task(
                self.event_worker.run_forever(self._stop), name="event-worker"
            ),
            asyncio.create_task(self.dm_worker.run_forever(self._stop), name="dm-worker"),
            asyncio.create_task(self._reconcile_forever(), name="reconciler"),
            asyncio.create_task(self._maintenance_forever(), name="maintenance"),
        ]
        logger.info("background workers started")

    async def stop(self) -> None:
        self._stop.set()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []
        await self.client.aclose()
        logger.info("background workers stopped")

    async def _reconcile_forever(self) -> None:
        interval = self._settings.delivery_poll_interval_seconds
        logger.info("reconciliation worker started (every %.1fs)", interval)
        try:
            while not self._stop.is_set():
                try:
                    await self.reconciler.run_once()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("reconciliation iteration failed")
                await self._sleep_or_stop(interval)
        finally:
            logger.info("reconciliation worker stopped")

    async def _maintenance_forever(self) -> None:
        """Requeue jobs abandoned by a crashed/cancelled send."""
        interval = max(5.0, self._settings.sending_stale_seconds / 2)
        try:
            while not self._stop.is_set():
                try:
                    await asyncio.to_thread(self._requeue_stale)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("maintenance iteration failed")
                await self._sleep_or_stop(interval)
        finally:
            logger.info("maintenance worker stopped")

    def _requeue_stale(self) -> None:
        # Only `sending` jobs are reaped on a timer; in-flight `processing`
        # events belong to the running event worker and are recovered at
        # startup instead.
        with session_scope() as session:
            jobs.requeue_stale_sending(
                session, stale_seconds=self._settings.sending_stale_seconds
            )

    async def _sleep_or_stop(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except TimeoutError:
            pass
