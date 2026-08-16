"""The 10-sends-per-rolling-60-seconds limit."""

from __future__ import annotations

import asyncio

from sqlalchemy import func, select

from app.database import session_scope
from app.models import SendAttemptLog
from app.services.rate_limiter import RollingWindowRateLimiter, SendAttemptStore


class FakeClock:
    """Virtual time: `sleep` advances the clock instead of waiting."""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start
        self.sleeps: list[float] = []

    def time(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def make_limiter(clock: FakeClock, **kwargs) -> RollingWindowRateLimiter:
    return RollingWindowRateLimiter(
        max_calls=kwargs.pop("max_calls", 10),
        window_seconds=kwargs.pop("window_seconds", 60.0),
        clock=clock.time,
        sleep=clock.sleep,
        **kwargs,
    )


async def test_first_ten_calls_do_not_wait():
    clock = FakeClock()
    limiter = make_limiter(clock)

    for _ in range(10):
        await limiter.acquire()

    assert clock.sleeps == []
    assert limiter.available_now() == 0


async def test_eleventh_call_waits_for_the_window_to_roll():
    clock = FakeClock()
    limiter = make_limiter(clock)

    for _ in range(10):
        await limiter.acquire()
        clock.now += 1  # ten sends spread over ten seconds

    start = clock.now
    await limiter.acquire()

    # First send was at t=1000, so the 11th can only go at t=1060.
    assert clock.now >= 1060
    assert clock.now - start > 0


async def test_never_more_than_ten_sends_in_any_60_second_window():
    clock = FakeClock()
    limiter = make_limiter(clock)
    granted: list[float] = []

    for _ in range(35):
        await limiter.acquire()
        granted.append(clock.now)

    for index, start in enumerate(granted):
        in_window = [t for t in granted[index:] if t < start + 60]
        assert len(in_window) <= 10, f"window at {start} had {len(in_window)} sends"


async def test_safety_margin_is_applied():
    clock = FakeClock()
    limiter = make_limiter(clock, max_calls=1, safety_seconds=5)

    await limiter.acquire()
    await limiter.acquire()

    assert clock.now >= 1065


async def test_concurrent_acquirers_are_serialised():
    clock = FakeClock()
    limiter = make_limiter(clock, max_calls=2)

    await asyncio.gather(*(limiter.acquire() for _ in range(6)))

    # 6 sends, 2 per window -> two extra windows had to elapse.
    assert clock.now >= 1120


async def test_send_attempts_are_persisted_and_reloaded():
    """A restart must not forget the last minute of traffic."""
    store = SendAttemptStore()
    clock = FakeClock()
    limiter = make_limiter(clock, store=store)

    for _ in range(10):
        await limiter.acquire()

    with session_scope() as session:
        assert session.scalar(select(func.count(SendAttemptLog.id))) == 10

    fresh = make_limiter(clock, store=store)
    await fresh.warm_start()
    assert fresh.available_now() == 0

    await fresh.acquire()
    assert clock.now >= 1060


async def test_retries_consume_rate_limit_slots_too():
    """A retried send is still a send: it must go through the limiter."""
    from app.clock import utcnow
    from app.config import get_settings
    from app.database import session_scope
    from app.models import DMJob, JobStatus, Rule
    from app.services.matching import normalize_keyword
    from app.workers.dm_worker import DMWorker
    from tests import fakes

    with session_scope() as session:
        rule = Rule(
            keyword="PRICE", keyword_normalized=normalize_keyword("PRICE"), dm_message="m"
        )
        session.add(rule)
        session.flush()
        job = DMJob(
            rule_id=rule.id, user_id="usr_1", message="m", status=JobStatus.QUEUED
        )
        session.add(job)
        session.flush()
        job_id = job.id

    clock = FakeClock()
    limiter = make_limiter(clock, max_calls=2)
    client = fakes.FakePseudoGramClient([fakes.server_error()])
    worker = DMWorker(client, limiter, get_settings(), sleep=clock.sleep)

    for _ in range(3):
        with session_scope() as session:
            session.get(DMJob, job_id).next_retry_at = utcnow()
        await worker.run_once()

    assert len(client.sends) == 3
    assert clock.now >= 1060, "the third send should have waited for the window"


async def test_warm_start_ignores_timestamps_outside_the_window():
    store = SendAttemptStore()
    clock = FakeClock()
    limiter = make_limiter(clock, store=store)
    for _ in range(10):
        await limiter.acquire()

    clock.now += 120  # two minutes later everything has expired
    fresh = make_limiter(clock, store=store)
    await fresh.warm_start()

    assert fresh.available_now() == 10
