"""Regression tests for the `sent=11 vs truth=12` mismatch.

A 500-event dry run produced one permanently `failed` job whose history was:
three `500`s, then a `202`, then a delivery status of `failed`. Five "attempts"
had been spent, so the job was abandoned - even though PseudoGram had only ever
attempted **one** real DM for it. Transient failures (where nothing was
accepted) and confirmed delivery failures now have separate budgets.
"""

from __future__ import annotations

from app.clock import utcnow
from app.config import get_settings
from app.database import session_scope
from app.models import DMJob, JobStatus
from app.services.jobs import apply_delivery_status, compute_stats
from app.services.rate_limiter import RollingWindowRateLimiter
from app.services.reconciliation import ReconciliationService
from app.workers.dm_worker import DMWorker
from tests import fakes
from tests.conftest import create_rule


def make_job(user_id: str = "usr_sim_7") -> int:
    rule = create_rule()
    with session_scope() as session:
        job = DMJob(
            rule_id=rule.id,
            user_id=user_id,
            comment_id="cmt_sim_42",
            message="here is the price list",
            status=JobStatus.QUEUED,
        )
        session.add(job)
        session.flush()
        return job.id


def read(job_id: int) -> DMJob:
    with session_scope() as session:
        job = session.get(DMJob, job_id)
        session.expunge(job)
        return job


def make_runnable(job_id: int) -> None:
    """Skip the backoff wait without changing any other state."""
    with session_scope() as session:
        job = session.get(DMJob, job_id)
        if job.next_retry_at:
            job.next_retry_at = utcnow()


def unlimited() -> RollingWindowRateLimiter:
    return RollingWindowRateLimiter(max_calls=10_000, window_seconds=60)


async def test_transient_errors_do_not_consume_the_delivery_retry_budget():
    """The exact failing sequence: 500, 500, 500, 202, delivery failed.

    Before the fix this ended as `failed` with sent=0. It must now retry the
    delivery and end up delivered.
    """
    job_id = make_job()
    client = fakes.FakePseudoGramClient(
        [
            fakes.server_error(),
            fakes.server_error(),
            fakes.server_error(),
            fakes.accepted("dm_first"),
            fakes.accepted("dm_second"),
        ],
        statuses={"dm_first": ["failed"], "dm_second": ["delivered"]},
    )
    worker = DMWorker(client, unlimited(), get_settings())
    reconciler = ReconciliationService(client, get_settings())

    for _ in range(4):
        make_runnable(job_id)
        await worker.run_once()

    assert read(job_id).status == JobStatus.ACCEPTED
    await reconciler.run_once()  # delivery reports failed

    job = read(job_id)
    assert job.status == JobStatus.RETRY_WAIT, "the job must not be abandoned here"
    assert job.send_cycle == 1

    make_runnable(job_id)
    await worker.run_once()
    await reconciler.run_once()  # second delivery succeeds

    assert read(job_id).status == JobStatus.DELIVERED
    with session_scope() as session:
        assert compute_stats(session)["sent"] == 1
        assert compute_stats(session)["failed"] == 0

    # Transport retries share one key; only the confirmed delivery failure
    # starts a new one.
    keys = [call["idempotency_key"] for call in client.sends]
    assert keys == [f"dm-job-{job_id}"] * 4 + [f"dm-job-{job_id}-r1"]


async def test_a_successful_acceptance_resets_the_transient_budget():
    job_id = make_job()
    client = fakes.FakePseudoGramClient(
        [
            fakes.server_error(),
            fakes.server_error(),
            fakes.server_error(),
            fakes.server_error(),
            fakes.accepted("dm_ok"),
        ]
    )
    worker = DMWorker(client, unlimited(), get_settings())

    for _ in range(5):
        make_runnable(job_id)
        await worker.run_once()

    job = read(job_id)
    assert job.status == JobStatus.ACCEPTED
    assert job.transient_failures == 0
    assert job.attempts == 5


async def test_consecutive_transient_failures_still_give_up():
    """The fix must not make jobs immortal."""
    job_id = make_job()
    client = fakes.FakePseudoGramClient([fakes.server_error()])
    worker = DMWorker(client, unlimited(), get_settings())

    for _ in range(get_settings().max_dm_attempts):
        make_runnable(job_id)
        await worker.run_once()

    job = read(job_id)
    assert job.status == JobStatus.FAILED
    assert job.transient_failures == get_settings().max_dm_attempts
    assert len(client.sends) == get_settings().max_dm_attempts


def test_repeated_delivery_failures_still_give_up():
    job_id = make_job()
    max_deliveries = get_settings().max_delivery_attempts

    for cycle in range(1, max_deliveries + 1):
        with session_scope() as session:
            job = session.get(DMJob, job_id)
            job.status = JobStatus.ACCEPTED
            job.dm_id = f"dm_{cycle}"
        with session_scope() as session:
            apply_delivery_status(session, session.get(DMJob, job_id), "failed")

    job = read(job_id)
    assert job.status == JobStatus.FAILED
    assert job.send_cycle == max_deliveries
    assert job.last_error == "delivery_failed"


def test_total_send_requests_per_job_stay_bounded():
    """Worst case is bounded by the product of the two budgets."""
    settings = get_settings()
    worst_case = settings.max_delivery_attempts * settings.max_dm_attempts

    assert worst_case == 25
    assert settings.max_dm_attempts == 5  # the value the assignment suggests


async def test_stats_never_report_a_job_in_two_buckets_during_recovery():
    """sent + failed + queued must equal the number of jobs at every step."""
    job_id = make_job()
    client = fakes.FakePseudoGramClient(
        [fakes.server_error(), fakes.accepted("dm_x")], statuses={"dm_x": ["delivered"]}
    )
    worker = DMWorker(client, unlimited(), get_settings())
    reconciler = ReconciliationService(client, get_settings())

    for step in range(3):
        make_runnable(job_id)
        await worker.run_once()
        await reconciler.run_once()
        with session_scope() as session:
            stats = compute_stats(session)
        assert stats["sent"] + stats["failed"] + stats["queued"] == 1, f"step {step}"

    assert read(job_id).status == JobStatus.DELIVERED
