"""DM job lifecycle: creation, claiming, state transitions and statistics.

All transition logic lives here (rather than inside the worker loop) so it can
be unit tested without any timers or HTTP.
"""

from __future__ import annotations

import logging
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum

from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.clock import utcnow
from app.config import Settings, get_settings
from app.models import (
    CANCELLABLE_JOB_STATUSES,
    PENDING_JOB_STATUSES,
    CounterKey,
    DMJob,
    JobStatus,
    Rule,
)
from app.services import counters
from app.services.pseudogram import SendOutcome, SendResult

logger = logging.getLogger(__name__)

MAX_ERROR_LENGTH = 500


class CreateOutcome(StrEnum):
    CREATED = "created"
    DUPLICATE = "duplicate"


@dataclass(slots=True)
class CreateResult:
    outcome: CreateOutcome
    job_id: int | None = None


def create_job(
    session: Session,
    *,
    rule: Rule,
    user_id: str,
    comment_id: str | None,
) -> CreateResult:
    """Insert a DM job, letting UNIQUE(rule_id, user_id) reject duplicates.

    The insert is attempted first and the failure interpreted afterwards: a
    check-then-insert would still race with a concurrent webhook.
    """
    job = DMJob(
        rule_id=rule.id,
        user_id=user_id,
        comment_id=comment_id,
        message=rule.dm_message,
        status=JobStatus.QUEUED,
    )
    try:
        with session.begin_nested():
            session.add(job)
        session.flush()
    except IntegrityError:
        counters.increment(session, CounterKey.DUPLICATES_BLOCKED)
        logger.info(
            "duplicate DM blocked rule_id=%s user_id=%s comment_id=%s",
            rule.id,
            user_id,
            comment_id,
        )
        return CreateResult(outcome=CreateOutcome.DUPLICATE)

    logger.info(
        "dm job created job_id=%s rule_id=%s user_id=%s comment_id=%s",
        job.id,
        rule.id,
        user_id,
        comment_id,
    )
    return CreateResult(outcome=CreateOutcome.CREATED, job_id=job.id)


def cancel_jobs_for_comment(session: Session, comment_id: str) -> int:
    """Cancel not-yet-sent jobs created by a comment that was deleted.

    Jobs already handed to PseudoGram (`accepted`) or delivered are left alone:
    a DM cannot be un-sent.
    """
    if not comment_id:
        return 0
    result = session.execute(
        update(DMJob)
        .where(
            DMJob.comment_id == comment_id,
            DMJob.status.in_([s.value for s in CANCELLABLE_JOB_STATUSES]),
        )
        .values(status=JobStatus.CANCELLED, updated_at=utcnow())
        .execution_options(synchronize_session=False)
    )
    if result.rowcount:
        logger.info(
            "cancelled %d queued job(s) for deleted comment_id=%s",
            result.rowcount,
            comment_id,
        )
    return int(result.rowcount or 0)


def claim_next_job(session: Session, *, now: datetime | None = None) -> DMJob | None:
    """Atomically move one runnable job to `sending` and return it.

    The conditional UPDATE is the claim: two workers racing on the same row
    means exactly one of them sees rowcount == 1.
    """
    now = now or utcnow()
    candidate_ids = session.scalars(
        select(DMJob.id)
        .where(
            (DMJob.status == JobStatus.QUEUED)
            | ((DMJob.status == JobStatus.RETRY_WAIT) & (DMJob.next_retry_at <= now))
        )
        .order_by(DMJob.next_retry_at.is_(None).desc(), DMJob.next_retry_at, DMJob.id)
        .limit(10)
    ).all()

    for job_id in candidate_ids:
        result = session.execute(
            update(DMJob)
            .where(
                DMJob.id == job_id,
                DMJob.status.in_([JobStatus.QUEUED.value, JobStatus.RETRY_WAIT.value]),
            )
            .values(status=JobStatus.SENDING, updated_at=utcnow())
            .execution_options(synchronize_session=False)
        )
        if result.rowcount:
            session.commit()
            return session.get(DMJob, job_id)
    return None


def requeue_stale_sending(session: Session, *, stale_seconds: float) -> int:
    """Recover jobs whose worker died mid-send.

    Resending is safe because the retry reuses the same Idempotency-Key.
    """
    cutoff = utcnow() - timedelta(seconds=stale_seconds)
    result = session.execute(
        update(DMJob)
        .where(DMJob.status == JobStatus.SENDING, DMJob.updated_at < cutoff)
        .values(status=JobStatus.QUEUED, updated_at=utcnow())
        .execution_options(synchronize_session=False)
    )
    if result.rowcount:
        logger.warning("requeued %d stale sending job(s)", result.rowcount)
    return int(result.rowcount or 0)


def backoff_delay(attempts: int, settings: Settings | None = None) -> float:
    """Exponential backoff with jitter: ~1s, 2s, 4s, 8s, 16s ..."""
    settings = settings or get_settings()
    exponent = max(0, attempts - 1)
    base = settings.retry_base_seconds * (2**exponent)
    base = min(base, settings.retry_max_backoff_seconds)
    jitter = base * settings.retry_jitter_ratio
    return max(0.1, base + random.uniform(-jitter, jitter))


def _schedule_retry(
    job: DMJob, *, delay_seconds: float, error: str | None, now: datetime
) -> None:
    job.status = JobStatus.RETRY_WAIT
    job.next_retry_at = now + timedelta(seconds=delay_seconds)
    job.last_error = (error or "")[:MAX_ERROR_LENGTH] or None
    job.updated_at = now


def _give_up(job: DMJob, *, error: str | None, now: datetime) -> None:
    job.status = JobStatus.FAILED
    job.next_retry_at = None
    job.last_error = (error or "")[:MAX_ERROR_LENGTH] or None
    job.updated_at = now
    logger.error(
        "dm job failed permanently job_id=%s attempts=%s error=%s",
        job.id,
        job.attempts,
        job.last_error,
    )


def apply_send_result(
    session: Session,
    job: DMJob,
    result: SendResult,
    settings: Settings | None = None,
    *,
    now: datetime | None = None,
) -> None:
    """Translate one send response into the job's next state."""
    settings = settings or get_settings()
    now = now or utcnow()

    if result.outcome is SendOutcome.ACCEPTED and not result.dm_id:
        # Accepted without a dm_id leaves nothing to reconcile against, so treat
        # it as a transient failure and retry (same idempotency key).
        _handle_transient_failure(
            job, error="accepted_without_dm_id", settings=settings, now=now
        )

    elif result.outcome is SendOutcome.ACCEPTED:
        # 202 means *accepted*, not delivered: the job stays pending until
        # reconciliation confirms it.
        job.attempts += 1
        # Progress: PseudoGram now owns a DM for us, so the transient-failure
        # budget starts fresh.
        job.transient_failures = 0
        job.status = JobStatus.ACCEPTED
        job.dm_id = result.dm_id
        job.last_error = None
        job.next_retry_at = None
        job.updated_at = now
        logger.info(
            "dm accepted job_id=%s dm_id=%s attempts=%s", job.id, job.dm_id, job.attempts
        )

    elif result.outcome is SendOutcome.RATE_LIMITED:
        # Our own throttling problem, not the job's fault: do not spend an
        # attempt on it, just wait for Retry-After.
        delay = result.retry_after
        if delay is None:
            delay = settings.send_rate_limit_window_seconds
        _schedule_retry(job, delay_seconds=delay, error="rate_limited", now=now)
        logger.warning("dm rate limited job_id=%s retry_in=%.1fs", job.id, delay)

    elif result.outcome is SendOutcome.AUTH_ERROR:
        # Nothing about this job is wrong, so keep it whole and wait for the
        # key to be fixed rather than failing every queued DM.
        _schedule_retry(
            job,
            delay_seconds=settings.auth_error_pause_seconds,
            error=f"auth_error: {result.error}",
            now=now,
        )
        logger.error(
            "PseudoGram rejected our API key (job_id=%s): %s", job.id, result.error
        )

    elif result.outcome is SendOutcome.INVALID:
        # Malformed request: retrying cannot help.
        job.attempts += 1
        _give_up(job, error=result.error or "invalid_request", now=now)

    else:  # SERVER_ERROR, TRANSPORT_ERROR, UNEXPECTED
        _handle_transient_failure(
            job,
            error=result.error or str(result.outcome),
            settings=settings,
            now=now,
        )

    session.flush()


def _handle_transient_failure(
    job: DMJob, *, error: str, settings: Settings, now: datetime
) -> None:
    """PseudoGram never accepted anything, so no DM was attempted.

    These failures get their own budget: they must not eat into the retries
    that protect against a DM that was really sent and really failed.
    """
    job.attempts += 1
    job.transient_failures += 1
    if job.transient_failures >= settings.max_dm_attempts:
        _give_up(job, error=error, now=now)
        return

    delay = backoff_delay(job.transient_failures, settings)
    _schedule_retry(job, delay_seconds=delay, error=error, now=now)
    logger.warning(
        "dm send retry job_id=%s attempts=%s consecutive=%s retry_in=%.1fs error=%s",
        job.id,
        job.attempts,
        job.transient_failures,
        delay,
        job.last_error,
    )


def apply_delivery_status(
    session: Session,
    job: DMJob,
    status: str | None,
    settings: Settings | None = None,
    *,
    now: datetime | None = None,
) -> None:
    """Apply the result of GET /v1/dm/{dm_id}."""
    settings = settings or get_settings()
    now = now or utcnow()
    job.last_status_check_at = now

    if status == "delivered":
        job.status = JobStatus.DELIVERED
        job.last_error = None
        job.next_retry_at = None
        job.updated_at = now
        logger.info("dm delivered job_id=%s dm_id=%s", job.id, job.dm_id)

    elif status == "failed":
        job.attempts += 1
        # A confirmed delivery failure needs a genuinely new DM, so the
        # idempotency key advances with the send cycle. `send_cycle` therefore
        # counts real delivery attempts that failed, and it is that count -
        # not the transient errors along the way - that decides when to stop.
        job.send_cycle += 1
        job.dm_id = None
        if job.send_cycle >= settings.max_delivery_attempts:
            _give_up(job, error="delivery_failed", now=now)
        else:
            job.transient_failures = 0
            delay = backoff_delay(job.send_cycle, settings)
            _schedule_retry(job, delay_seconds=delay, error="delivery_failed", now=now)
            logger.warning(
                "dm delivery failed, retrying job_id=%s delivery_attempt=%s "
                "attempts=%s retry_in=%.1fs",
                job.id,
                job.send_cycle,
                job.attempts,
                delay,
            )
    else:
        # Still queued at PseudoGram: leave it pending.
        job.updated_at = now

    session.flush()


def compute_stats(session: Session) -> dict[str, int]:
    rows = session.execute(
        select(DMJob.status, func.count(DMJob.id)).group_by(DMJob.status)
    ).all()
    by_status = {status: int(count) for status, count in rows}
    pending = sum(by_status.get(s.value, 0) for s in PENDING_JOB_STATUSES)
    return {
        "sent": by_status.get(JobStatus.DELIVERED.value, 0),
        "failed": by_status.get(JobStatus.FAILED.value, 0),
        "queued": pending,
        "duplicates_blocked": counters.get(session, CounterKey.DUPLICATES_BLOCKED),
    }
