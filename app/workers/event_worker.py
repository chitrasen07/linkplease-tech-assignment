"""Background matching worker.

The webhook endpoint only persists events; this loop turns them into DM jobs.
Keeping the two apart is what lets /webhook answer in milliseconds and what
makes a crash mid-processing recoverable (the event row is still `received`).
"""

from __future__ import annotations

import asyncio
import logging

from app.config import Settings, get_settings
from app.database import session_scope
from app.services import events

logger = logging.getLogger(__name__)


class EventWorker:
    def __init__(self, settings: Settings | None = None, *, batch_size: int = 100):
        self._settings = settings or get_settings()
        self._batch_size = batch_size

    @staticmethod
    def _process(batch_size: int) -> int:
        with session_scope() as session:
            return events.process_pending_events(session, limit=batch_size)

    async def run_once(self) -> int:
        return await asyncio.to_thread(self._process, self._batch_size)

    async def run_forever(self, stop_event: asyncio.Event) -> None:
        logger.info("event worker started")
        try:
            while not stop_event.is_set():
                try:
                    handled = await self.run_once()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("event worker iteration failed")
                    handled = 0

                if handled < self._batch_size:
                    await _wait(stop_event, self._settings.event_poll_interval_seconds)
        finally:
            logger.info("event worker stopped")


async def _wait(stop_event: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
    except TimeoutError:
        pass
