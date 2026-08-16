"""Retry policy, delivery reconciliation and idempotency."""

from __future__ import annotations

import asyncio

from sqlalchemy import func, select

from app.clock import utcnow
from app.config import get_settings
from app.database import session_scope
from app.models import DMJob, JobStatus, SendAttemptLog
from app.services.jobs import (
    apply_delivery_status,
    apply_send_result,
    backoff_delay,
    claim_next_job,
    requeue_stale_sending,
)
from app.services.rate_limiter import RollingWindowRateLimiter
from app.services.reconciliation import ReconciliationService
from app.workers.dm_worker import DMWorker
from tests import fakes
from tests.conftest import create_rule


def make_job(**overrides) -> int:
    rule = create_rule()
    with session_scope() as session:
        job = DMJob(
            rule_id=rule.id,
            user_id=overrides.pop("user_id", "usr_1"),
            comment_id="cmt_1",
            message="hello",
            status=overrides.pop("status", JobStatus.SENDING),
            **overrides,
        )
        session.add(job)
        session.flush()
        return job.id


def get_job(job_id: int) -> DMJob:
    with session_scope() as session:
        job = session.get(DMJob, job_id)
        session.expunge(job)
        return job


def apply(job_id: int, result) -> DMJob:
    with session_scope() as session:
        job = session.get(DMJob, job_id)
        apply_send_result(session, job, result, get_settings())
    return get_job(job_id)


def unlimited_limiter() -> RollingWindowRateLimiter:
    return RollingWindowRateLimiter(max_calls=1000, window_seconds=60)


# --- send responses -------------------------------------------------------


def test_500_schedules_a_retry_rather_than_failing():
    job_id = make_job()
    job = apply(job_id, fakes.server_error())

    assert job.status == JobStatus.RETRY_WAIT
    assert job.attempts == 1
    assert job.next_retry_at > utcnow()
    assert job.last_error == "internal_error"


def test_400_fails_immediately_without_retry():
    job_id = make_job()
    job = apply(job_id, fakes.invalid_request("bad recipient"))

    assert job.status == JobStatus.FAILED
    assert job.next_retry_at is None
    assert job.last_error == "bad recipient"


def test_429_waits_for_retry_after_and_does_not_spend_an_attempt():
    job_id = make_job()
    before = utcnow()
    job = apply(job_id, fakes.rate_limited(retry_after=30))

    assert job.status == JobStatus.RETRY_WAIT
    assert job.attempts == 0
    assert (job.next_retry_at - before).total_seconds() >= 29


def test_429_without_retry_after_falls_back_to_the_window():
    job_id = make_job()
    before = utcnow()
    job = apply(job_id, fakes.rate_limited(retry_after=None))

    assert (job.next_retry_at - before).total_seconds() >= 59


def test_401_does_not_burn_attempts_or_fail_the_job():
    """A misconfigured key must not permanently fail every queued DM."""
    job_id = make_job()
    job = apply(job_id, fakes.auth_error())

    assert job.status == JobStatus.RETRY_WAIT
    assert job.attempts == 0
    assert "auth_error" in job.last_error


async def test_worker_pauses_after_an_auth_error():
    make_job(status=JobStatus.QUEUED, user_id="usr_401")
    slept: list[float] = []
    limiter = RollingWindowRateLimiter(
        max_calls=1000, window_seconds=60, sleep=lambda s: _record(slept, s)
    )
    worker = DMWorker(
        fakes.FakePseudoGramClient([fakes.auth_error()]),
        limiter,
        get_settings(),
        sleep=lambda s: _record(slept, s),
    )

    await worker.run_once()

    assert slept == [get_settings().auth_error_pause_seconds]


def test_transport_error_retries_because_the_request_may_have_landed():
    job_id = make_job()
    job = apply(job_id, fakes.transport_error())

    assert job.status == JobStatus.RETRY_WAIT
    assert job.attempts == 1


def test_202_is_accepted_not_sent():
    job_id = make_job()
    job = apply(job_id, fakes.accepted("dm_123"))

    assert job.status == JobStatus.ACCEPTED
    assert job.dm_id == "dm_123"
    assert job.status != JobStatus.DELIVERED


def test_exhausted_attempts_mark_the_job_failed():
    job_id = make_job()
    max_attempts = get_settings().max_dm_attempts

    for _ in range(max_attempts):
        job = apply(job_id, fakes.server_error())
        if job.status == JobStatus.RETRY_WAIT:
            with session_scope() as session:
                session.get(DMJob, job_id).status = JobStatus.SENDING

    assert job.status == JobStatus.FAILED
    assert job.attempts == max_attempts


def test_backoff_grows_exponentially():
    settings = get_settings()
    for attempt, expected in [(1, 1), (2, 2), (3, 4), (4, 8), (5, 16)]:
        delay = backoff_delay(attempt, settings)
        assert expected * 0.7 <= delay <= expected * 1.3


def test_backoff_is_capped():
    settings = get_settings()
    assert backoff_delay(30, settings) <= settings.retry_max_backoff_seconds * 1.3


# --- delivery reconciliation ---------------------------------------------


def test_delivered_status_marks_the_job_delivered():
    job_id = make_job(status=JobStatus.ACCEPTED, dm_id="dm_1", attempts=1)
    with session_scope() as session:
        apply_delivery_status(session, session.get(DMJob, job_id), "delivered")

    assert get_job(job_id).status == JobStatus.DELIVERED


def test_delivery_failure_schedules_a_retry():
    job_id = make_job(status=JobStatus.ACCEPTED, dm_id="dm_1", attempts=1)
    with session_scope() as session:
        apply_delivery_status(session, session.get(DMJob, job_id), "failed")

    job = get_job(job_id)
    assert job.status == JobStatus.RETRY_WAIT
    assert job.attempts == 2
    assert job.dm_id is None
    assert job.send_cycle == 1


def test_delivery_failure_on_the_last_delivery_attempt_fails_the_job():
    """The delivery budget - not the total attempt count - decides this."""
    job_id = make_job(
        status=JobStatus.ACCEPTED,
        dm_id="dm_1",
        attempts=99,  # plenty of transient noise earlier: irrelevant here
        send_cycle=get_settings().max_delivery_attempts - 1,
    )
    with session_scope() as session:
        apply_delivery_status(session, session.get(DMJob, job_id), "failed")

    job = get_job(job_id)
    assert job.status == JobStatus.FAILED
    assert job.last_error == "delivery_failed"
    assert job.send_cycle == get_settings().max_delivery_attempts


def test_a_high_attempt_count_alone_does_not_fail_a_delivery_retry():
    """Regression: transient noise must not shorten the delivery budget."""
    job_id = make_job(status=JobStatus.ACCEPTED, dm_id="dm_1", attempts=99, send_cycle=0)
    with session_scope() as session:
        apply_delivery_status(session, session.get(DMJob, job_id), "failed")

    assert get_job(job_id).status == JobStatus.RETRY_WAIT


def test_still_queued_delivery_leaves_the_job_pending():
    job_id = make_job(status=JobStatus.ACCEPTED, dm_id="dm_1", attempts=1)
    with session_scope() as session:
        apply_delivery_status(session, session.get(DMJob, job_id), "queued")

    job = get_job(job_id)
    assert job.status == JobStatus.ACCEPTED
    assert job.last_status_check_at is not None


async def test_reconciliation_loop_marks_delivered_and_retries_failed():
    delivered_id = make_job(status=JobStatus.ACCEPTED, dm_id="dm_ok", user_id="usr_ok")
    failed_id = make_job(status=JobStatus.ACCEPTED, dm_id="dm_bad", user_id="usr_bad")

    client = fakes.FakePseudoGramClient(
        statuses={"dm_ok": ["delivered"], "dm_bad": ["failed"]}
    )
    checked = await ReconciliationService(client, get_settings()).run_once()

    assert checked == 2
    assert get_job(delivered_id).status == JobStatus.DELIVERED
    assert get_job(failed_id).status == JobStatus.RETRY_WAIT


async def test_reconciliation_survives_status_errors():
    job_id = make_job(status=JobStatus.ACCEPTED, dm_id="dm_err")
    client = fakes.FakePseudoGramClient(statuses={"dm_err": ["__error__"]})

    await ReconciliationService(client, get_settings()).run_once()

    assert get_job(job_id).status == JobStatus.ACCEPTED


async def test_reconciliation_does_not_consume_send_rate_limit_budget():
    """GET status calls are free, so they must not be recorded as sends."""
    make_job(status=JobStatus.ACCEPTED, dm_id="dm_ok")
    client = fakes.FakePseudoGramClient(statuses={"dm_ok": ["delivered"]})

    await asyncio.wait_for(
        ReconciliationService(client, get_settings()).run_once(), timeout=5
    )

    with session_scope() as session:
        recorded = session.scalar(select(func.count(SendAttemptLog.id)))
    assert recorded == 0


# --- worker level ---------------------------------------------------------


async def test_worker_sends_claimed_job_and_records_dm_id():
    make_job(status=JobStatus.QUEUED, user_id="usr_w")

    client = fakes.FakePseudoGramClient([fakes.accepted("dm_worker")])
    worker = DMWorker(client, unlimited_limiter(), get_settings())

    assert await worker.run_once() is True
    assert await worker.run_once() is False  # nothing left to claim

    with session_scope() as session:
        job = session.scalar(select(DMJob).where(DMJob.user_id == "usr_w"))
    assert job.status == JobStatus.ACCEPTED
    assert job.dm_id == "dm_worker"
    assert client.sends[0]["recipient_user_id"] == "usr_w"


async def test_idempotency_key_is_stable_across_transport_retries():
    job_id = make_job(status=JobStatus.QUEUED, user_id="usr_idem")
    client = fakes.FakePseudoGramClient([fakes.server_error(), fakes.accepted("dm_ok")])
    worker = DMWorker(client, unlimited_limiter(), get_settings())

    await worker.run_once()
    # Make the retry immediately runnable instead of sleeping through backoff.
    with session_scope() as session:
        session.get(DMJob, job_id).next_retry_at = utcnow()
    await worker.run_once()

    assert len(client.sends) == 2
    assert client.sends[0]["idempotency_key"] == client.sends[1]["idempotency_key"]
    assert client.sends[0]["idempotency_key"] == f"dm-job-{job_id}"


async def test_idempotency_key_advances_only_after_a_confirmed_delivery_failure():
    job_id = make_job(status=JobStatus.ACCEPTED, dm_id="dm_1", user_id="usr_cycle")
    with session_scope() as session:
        apply_delivery_status(session, session.get(DMJob, job_id), "failed")
    with session_scope() as session:
        session.get(DMJob, job_id).next_retry_at = utcnow()

    client = fakes.FakePseudoGramClient([fakes.accepted("dm_2")])
    await DMWorker(client, unlimited_limiter(), get_settings()).run_once()

    assert client.sends[0]["idempotency_key"] == f"dm-job-{job_id}-r1"


async def test_worker_honours_retry_after_on_429():
    make_job(status=JobStatus.QUEUED, user_id="usr_429")
    slept: list[float] = []

    limiter = RollingWindowRateLimiter(
        max_calls=1000, window_seconds=60, sleep=lambda s: _record(slept, s)
    )
    client = fakes.FakePseudoGramClient([fakes.rate_limited(retry_after=12)])
    await DMWorker(client, limiter, get_settings()).run_once()

    assert slept == [12.0]


async def _record(bucket: list[float], seconds: float) -> None:
    bucket.append(seconds)


async def test_worker_recovers_a_job_when_sending_raises():
    job_id = make_job(status=JobStatus.QUEUED, user_id="usr_boom")

    class ExplodingClient(fakes.FakePseudoGramClient):
        async def send_dm(self, **kwargs):
            raise RuntimeError("boom")

    await DMWorker(ExplodingClient(), unlimited_limiter(), get_settings()).run_once()

    assert get_job(job_id).status == JobStatus.QUEUED


def test_stale_sending_jobs_are_requeued():
    job_id = make_job(status=JobStatus.SENDING)

    with session_scope() as session:
        requeue_stale_sending(session, stale_seconds=0)

    assert get_job(job_id).status == JobStatus.QUEUED


def test_claiming_is_exclusive():
    make_job(status=JobStatus.QUEUED)

    with session_scope() as session:
        first = claim_next_job(session)
    with session_scope() as session:
        second = claim_next_job(session)

    assert first is not None
    assert second is None


def test_retry_wait_job_is_not_claimed_before_its_time():
    from datetime import timedelta

    job_id = make_job(status=JobStatus.RETRY_WAIT)
    with session_scope() as session:
        session.get(DMJob, job_id).next_retry_at = utcnow() + timedelta(minutes=5)

    with session_scope() as session:
        assert claim_next_job(session) is None


def test_accepted_without_dm_id_is_retried():
    job_id = make_job()
    job = apply(job_id, fakes.SendResult(outcome=fakes.SendOutcome.ACCEPTED, dm_id=None))

    assert job.status == JobStatus.RETRY_WAIT
    assert job.last_error == "accepted_without_dm_id"
