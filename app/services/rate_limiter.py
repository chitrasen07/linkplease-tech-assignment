"""Rolling-window rate limiter for POST /v1/dm/send.

PseudoGram allows 10 send requests per rolling 60 seconds. Every send in the
process goes through one limiter instance, and each grant is written to the
`send_attempt_log` table so a restart cannot "forget" the last minute of
traffic and burst straight through the limit.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from collections.abc import Awaitable, Callable, Iterable

from sqlalchemy import delete, select

from app.database import session_scope
from app.models import SendAttemptLog

logger = logging.getLogger(__name__)


class SendAttemptStore:
    """Database-backed history of send attempts."""

    def load_recent(self, since_ts: float) -> list[float]:
        with session_scope() as session:
            rows = session.scalars(
                select(SendAttemptLog.ts).where(SendAttemptLog.ts >= since_ts)
            ).all()
        return sorted(float(ts) for ts in rows)

    def record(self, ts: float) -> None:
        with session_scope() as session:
            session.add(SendAttemptLog(ts=ts))

    def prune(self, before_ts: float) -> None:
        with session_scope() as session:
            session.execute(delete(SendAttemptLog).where(SendAttemptLog.ts < before_ts))


class RollingWindowRateLimiter:
    def __init__(
        self,
        max_calls: int,
        window_seconds: float,
        *,
        safety_seconds: float = 0.0,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        store: SendAttemptStore | None = None,
    ) -> None:
        self.max_calls = max_calls
        self.window_seconds = window_seconds
        self.safety_seconds = safety_seconds
        self._clock = clock
        self._sleep = sleep
        self._store = store
        self._timestamps: deque[float] = deque()
        self._lock = asyncio.Lock()

    def seed(self, timestamps: Iterable[float]) -> None:
        self._timestamps = deque(sorted(timestamps))
        self._trim(self._clock())

    async def warm_start(self) -> None:
        """Reload the current window from the database after a restart."""
        if self._store is None:
            return
        cutoff = self._clock() - self.window_seconds
        try:
            recent = await asyncio.to_thread(self._store.load_recent, cutoff)
            await asyncio.to_thread(self._store.prune, cutoff)
        except Exception:  # pragma: no cover - never block startup on this
            logger.exception("rate limiter warm start failed")
            return
        self.seed(recent)
        if recent:
            logger.info("rate limiter warm start: %d recent sends", len(recent))

    def _trim(self, now: float) -> None:
        horizon = now - self.window_seconds
        while self._timestamps and self._timestamps[0] <= horizon:
            self._timestamps.popleft()

    def available_now(self) -> int:
        self._trim(self._clock())
        return max(0, self.max_calls - len(self._timestamps))

    async def acquire(self) -> None:
        """Block until a send slot is free, then consume it."""
        async with self._lock:
            while True:
                now = self._clock()
                self._trim(now)
                if len(self._timestamps) < self.max_calls:
                    self._timestamps.append(now)
                    await self._persist(now)
                    return
                wait_for = (
                    self._timestamps[0] + self.window_seconds + self.safety_seconds - now
                )
                logger.debug("rate limit reached, waiting %.2fs", wait_for)
                await self._sleep(max(wait_for, 0.01))

    async def penalise(self, seconds: float) -> None:
        """Apply a server-instructed pause (429 Retry-After).

        The caller is the single send worker, so blocking here stops all
        outbound sends for the duration the server asked for.
        """
        seconds = max(0.0, seconds)
        logger.warning("rate limited by PseudoGram, pausing sends for %.1fs", seconds)
        await self._sleep(seconds)

    async def _persist(self, ts: float) -> None:
        if self._store is None:
            return
        try:
            await asyncio.to_thread(self._store.record, ts)
        except Exception:  # pragma: no cover - logging only
            logger.exception("failed to persist send attempt timestamp")
