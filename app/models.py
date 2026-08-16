"""SQLAlchemy models.

The database - not process memory - is the source of truth for every piece of
state that matters: which events were seen, which DMs are owed, how many
duplicates were blocked, and how many sends were made in the last minute.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import StrEnum

from sqlalchemy import (
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from app.clock import utcnow


class Base(DeclarativeBase):
    pass


class EventStatus(StrEnum):
    RECEIVED = "received"  # persisted, not matched yet
    PROCESSING = "processing"  # claimed by the event worker
    PROCESSED = "processed"  # matching finished
    IGNORED = "ignored"  # nothing to do (unknown type, deleted comment, ...)


class JobStatus(StrEnum):
    QUEUED = "queued"  # waiting for the first send
    SENDING = "sending"  # claimed by the DM worker, request in flight
    ACCEPTED = "accepted"  # PseudoGram returned 202, delivery unknown
    DELIVERED = "delivered"  # confirmed delivered  -> counts as `sent`
    RETRY_WAIT = "retry_wait"  # transient failure, waiting for next_retry_at
    FAILED = "failed"  # gave up after retries      -> counts as `failed`
    CANCELLED = "cancelled"  # comment deleted before we sent


#: Job states that still represent outstanding work (the `queued` stat).
PENDING_JOB_STATUSES = (
    JobStatus.QUEUED,
    JobStatus.SENDING,
    JobStatus.ACCEPTED,
    JobStatus.RETRY_WAIT,
)

#: Job states that can still be cancelled when a comment is deleted.
CANCELLABLE_JOB_STATUSES = (JobStatus.QUEUED, JobStatus.RETRY_WAIT)


class CounterKey(StrEnum):
    DUPLICATES_BLOCKED = "duplicates_blocked"
    DUPLICATE_EVENTS = "duplicate_events"
    EVENTS_RECEIVED = "events_received"


def _new_id() -> str:
    return uuid.uuid4().hex


class Rule(Base):
    __tablename__ = "rules"

    id: Mapped[str] = mapped_column(String(64), primary_key=True, default=_new_id)
    keyword: Mapped[str] = mapped_column(String(255), nullable=False)
    # Lower-cased copy so matching never depends on how the rule was typed.
    keyword_normalized: Mapped[str] = mapped_column(
        String(255), nullable=False, index=True
    )
    dm_message: Mapped[str] = mapped_column(Text, nullable=False)
    is_active: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class Event(Base):
    __tablename__ = "events"
    __table_args__ = (
        # The single most important constraint for at-least-once webhooks.
        UniqueConstraint("event_id", name="uq_events_event_id"),
        Index("ix_events_status", "status"),
        Index("ix_events_comment_id", "comment_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(128), nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    comment_id: Mapped[str | None] = mapped_column(String(128))
    post_id: Mapped[str | None] = mapped_column(String(128))
    user_id: Mapped[str | None] = mapped_column(String(128))
    username: Mapped[str | None] = mapped_column(String(255))
    text: Mapped[str | None] = mapped_column(Text)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime)
    received_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime)
    status: Mapped[str] = mapped_column(
        String(32), nullable=False, default=EventStatus.RECEIVED
    )


class DMJob(Base):
    __tablename__ = "dm_jobs"
    __table_args__ = (
        # "The same user never gets DMed twice for the same rule" - enforced by
        # the database, not by application-level check-then-insert.
        #
        # Cancelled jobs are excluded: a job cancelled because its comment was
        # deleted never sent anything, so it must not block the user forever if
        # they comment the keyword again. Every job that was sent or is still
        # sendable still holds the slot.
        Index(
            "uq_dm_jobs_rule_user",
            "rule_id",
            "user_id",
            unique=True,
            sqlite_where=text("status != 'cancelled'"),
            postgresql_where=text("status != 'cancelled'"),
        ),
        Index("ix_dm_jobs_status_retry", "status", "next_retry_at"),
        Index("ix_dm_jobs_comment_id", "comment_id"),
        Index("ix_dm_jobs_dm_id", "dm_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    rule_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("rules.id"), nullable=False
    )
    user_id: Mapped[str] = mapped_column(String(128), nullable=False)
    comment_id: Mapped[str | None] = mapped_column(String(128))
    message: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(
        String(32), nullable=False, default=JobStatus.QUEUED
    )
    # Total send requests made for this job (all outcomes).
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # Consecutive failures where PseudoGram never accepted anything (500s,
    # timeouts). Reset to 0 on a 202, because that is progress.
    transient_failures: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # Bumped only when PseudoGram reports a *delivery* failure and we decide to
    # send a genuinely new DM; see `idempotency_key`. Equals the number of real
    # delivery attempts that failed.
    send_cycle: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    dm_id: Mapped[str | None] = mapped_column(String(128))
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime)
    last_error: Mapped[str | None] = mapped_column(Text)
    last_status_check_at: Mapped[datetime | None] = mapped_column(DateTime)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow
    )

    @property
    def idempotency_key(self) -> str:
        """Stable per job (and per deliberate re-send cycle).

        Transport level retries - timeouts, 500s, 429s - reuse the exact same
        key, so a request that reached PseudoGram but whose response we lost
        cannot turn into a second DM.
        """
        if self.send_cycle:
            return f"dm-job-{self.id}-r{self.send_cycle}"
        return f"dm-job-{self.id}"


class DeletedComment(Base):
    """Remembers `comment.deleted` so an out-of-order `comment.created` for the
    same comment does not create work we would immediately have to cancel."""

    __tablename__ = "deleted_comments"

    comment_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    deleted_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class Counter(Base):
    """Persistent counters incremented with an atomic UPDATE."""

    __tablename__ = "counters"

    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class SendAttemptLog(Base):
    """One row per outbound POST /v1/dm/send, used by the rate limiter so the
    rolling window survives a process restart."""

    __tablename__ = "send_attempt_log"
    __table_args__ = (Index("ix_send_attempt_log_ts", "ts"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts: Mapped[float] = mapped_column(Float, nullable=False)
