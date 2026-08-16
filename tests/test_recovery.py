"""Crash and restart recovery.

Each test kills the process at a different point by resetting the engine (all
in-memory state is gone) and then running the startup recovery that
`WorkerManager` performs, exactly as a redeploy would.
"""

from __future__ import annotations

from datetime import timedelta

from sqlalchemy import func, select

from app.clock import utcnow
from app.config import get_settings
from app.database import reset_engine, session_scope
from app.models import DMJob, Event, EventStatus, JobStatus, SendAttemptLog
from app.services import events as events_service
from app.services import jobs as jobs_service
from app.services.rate_limiter import RollingWindowRateLimiter, SendAttemptStore
from app.workers.dm_worker import DMWorker
from tests import fakes
from tests.conftest import create_rule, drain_events, make_event, post_webhook


def restart() -> None:
    """Simulate a process restart: drop all state, then recover."""
    reset_engine()
    with session_scope() as session:
        events_service.requeue_stuck_events(session)
        jobs_service.requeue_stale_sending(session, stale_seconds=0)


def make_job(status: JobStatus, **overrides) -> int:
    rule = create_rule()
    with session_scope() as session:
        job = DMJob(
            rule_id=rule.id,
            user_id=overrides.pop("user_id", "usr_1"),
            comment_id="cmt_1",
            message="hello",
            status=status,
            **overrides,
        )
        session.add(job)
        session.flush()
        return job.id


def read(job_id: int) -> DMJob:
    with session_scope() as session:
        job = session.get(DMJob, job_id)
        session.expunge(job)
        return job


def unlimited() -> RollingWindowRateLimiter:
    return RollingWindowRateLimiter(max_calls=10_000, window_seconds=60)


def test_crash_before_send_keeps_the_job(client):
    create_rule()
    post_webhook(client, make_event("evt_1"))
    drain_events()

    restart()

    with session_scope() as session:
        assert session.scalar(select(func.count(DMJob.id))) == 1
        assert session.scalar(select(DMJob)).status == JobStatus.QUEUED


def test_crash_between_receiving_and_matching_reprocesses_the_event(client):
    """The event was stored but never matched: it must still become a job."""
    post_webhook(client, make_event("evt_1"))
    create_rule()

    with session_scope() as session:  # claimed, then the worker died
        events_service.claim_pending_events(session)
        assert session.scalar(select(Event)).status == EventStatus.PROCESSING

    restart()
    with session_scope() as session:
        assert session.scalar(select(Event)).status == EventStatus.RECEIVED
    drain_events()

    with session_scope() as session:
        assert session.scalar(select(func.count(DMJob.id))) == 1


async def test_crash_while_sending_requeues_and_reuses_the_idempotency_key():
    job_id = make_job(JobStatus.QUEUED, user_id="usr_crash")
    client = fakes.FakePseudoGramClient([fakes.accepted("dm_1")])
    worker = DMWorker(client, unlimited(), get_settings())

    with session_scope() as session:  # claimed, request in flight, then crash
        jobs_service.claim_next_job(session)
    assert read(job_id).status == JobStatus.SENDING

    restart()
    assert read(job_id).status == JobStatus.QUEUED

    await worker.run_once()
    assert client.sends[0]["idempotency_key"] == f"dm-job-{job_id}"


async def test_crash_after_202_before_commit_does_not_send_a_second_dm():
    """PseudoGram accepted the DM but we never stored the dm_id.

    The retry reuses the same key, so an idempotent provider replays the
    original dm_id instead of delivering twice.
    """
    job_id = make_job(JobStatus.SENDING, user_id="usr_lost")
    replayed: dict[str, str] = {}

    def respond(call: dict):
        key = call["idempotency_key"]
        replayed.setdefault(key, f"dm_{len(replayed) + 1}")
        return fakes.accepted(replayed[key])

    client = fakes.FakePseudoGramClient(respond)

    restart()
    await DMWorker(client, unlimited(), get_settings()).run_once()

    assert len(replayed) == 1, "a new key would have produced a second DM"
    assert read(job_id).dm_id == "dm_1"


def test_crash_while_waiting_for_retry_keeps_the_schedule():
    due = utcnow() + timedelta(minutes=5)
    job_id = make_job(JobStatus.RETRY_WAIT, attempts=2, transient_failures=2)
    with session_scope() as session:
        session.get(DMJob, job_id).next_retry_at = due

    restart()

    job = read(job_id)
    assert job.status == JobStatus.RETRY_WAIT
    assert job.attempts == 2
    assert abs((job.next_retry_at - due).total_seconds()) < 1


def test_crash_while_awaiting_reconciliation_keeps_the_job_pending(client):
    job_id = make_job(JobStatus.ACCEPTED, dm_id="dm_1", attempts=1)

    restart()

    job = read(job_id)
    assert job.status == JobStatus.ACCEPTED
    assert job.dm_id == "dm_1"
    assert client.get("/stats").json()["queued"] == 1


def test_delivered_and_failed_jobs_are_untouched_by_recovery(client):
    make_job(JobStatus.DELIVERED, user_id="usr_ok")
    make_job(JobStatus.FAILED, user_id="usr_bad")

    restart()

    body = client.get("/stats").json()
    assert body["sent"] == 1
    assert body["failed"] == 1
    assert body["queued"] == 0


async def test_rate_limiter_does_not_burst_after_a_restart():
    store = SendAttemptStore()
    clock = _Clock()
    before = RollingWindowRateLimiter(
        max_calls=10, window_seconds=60, clock=clock.time, sleep=clock.sleep, store=store
    )
    for _ in range(10):
        await before.acquire()

    reset_engine()  # restart

    after = RollingWindowRateLimiter(
        max_calls=10, window_seconds=60, clock=clock.time, sleep=clock.sleep, store=store
    )
    await after.warm_start()
    assert after.available_now() == 0

    await after.acquire()
    assert clock.now >= 1060, "the 11th send must wait for the window to roll"

    with session_scope() as session:
        assert session.scalar(select(func.count(SendAttemptLog.id))) == 11


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def time(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += seconds
