"""Event ingestion and processing.

Ingestion (called from the webhook request) does the minimum: persist the event
and return. Processing (matching + job creation) happens in the background
worker, so a crash between the two leaves a `received` row that is simply
picked up again after restart.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.clock import parse_iso8601, utcnow
from app.config import Settings, get_settings
from app.models import CounterKey, DeletedComment, Event, EventStatus
from app.schemas import WebhookEvent
from app.services import counters, jobs, matching

logger = logging.getLogger(__name__)

EVENT_COMMENT_CREATED = "comment.created"
EVENT_COMMENT_DELETED = "comment.deleted"


@dataclass(slots=True)
class IngestResult:
    duplicate: bool
    event_pk: int | None


def ingest_event(
    session: Session, payload: WebhookEvent, settings: Settings | None = None
) -> IngestResult:
    """Persist an incoming webhook exactly once.

    Deduplication is the UNIQUE constraint on events.event_id, not a Python
    set: the set would vanish on restart and would not survive concurrency.
    """
    settings = settings or get_settings()
    data = payload.data
    sender = data.from_

    event = Event(
        event_id=payload.event_id,
        event_type=payload.event_type,
        comment_id=data.comment_id,
        post_id=data.post_id,
        user_id=sender.user_id if sender else None,
        username=sender.username if sender else None,
        text=data.text,
        sent_at=parse_iso8601(payload.sent_at),
        received_at=utcnow(),
        status=EventStatus.RECEIVED,
    )
    try:
        with session.begin_nested():
            session.add(event)
        session.flush()
    except IntegrityError:
        _handle_duplicate_event(session, payload, settings)
        return IngestResult(duplicate=True, event_pk=None)

    counters.increment(session, CounterKey.EVENTS_RECEIVED)
    return IngestResult(duplicate=False, event_pk=event.id)


def _handle_duplicate_event(
    session: Session, payload: WebhookEvent, settings: Settings
) -> None:
    """A webhook we have already stored arrived again.

    By default this is *not* counted as a blocked duplicate: the assignment
    ties `duplicates_blocked` to blocked DM-job insertions, and a redelivery is
    a transport artifact of the same logical event. `COUNT_DUPLICATE_EVENTS_AS_
    BLOCKED=true` switches to counting every matching delivery instead.
    """
    counters.increment(session, CounterKey.DUPLICATE_EVENTS)
    logger.info("duplicate event ignored event_id=%s", payload.event_id)

    if not settings.count_duplicate_events_as_blocked:
        return
    if payload.event_type != EVENT_COMMENT_CREATED:
        return
    sender = payload.data.from_
    if not sender or not sender.user_id:
        return
    if payload.data.comment_id and _comment_is_deleted(session, payload.data.comment_id):
        return

    blocked = len(matching.find_matching_rules(session, payload.data.text))
    if blocked:
        counters.increment(session, CounterKey.DUPLICATES_BLOCKED, blocked)


def _comment_is_deleted(session: Session, comment_id: str) -> bool:
    return (
        session.scalar(
            select(DeletedComment.comment_id).where(
                DeletedComment.comment_id == comment_id
            )
        )
        is not None
    )


def claim_pending_events(session: Session, limit: int = 50) -> list[Event]:
    """Move up to `limit` freshly received events into `processing`."""
    ids = session.scalars(
        select(Event.id)
        .where(Event.status == EventStatus.RECEIVED)
        .order_by(Event.id)
        .limit(limit)
    ).all()

    claimed: list[Event] = []
    for event_id in ids:
        result = session.execute(
            update(Event)
            .where(Event.id == event_id, Event.status == EventStatus.RECEIVED)
            .values(status=EventStatus.PROCESSING)
            .execution_options(synchronize_session=False)
        )
        if result.rowcount:
            claimed.append(session.get(Event, event_id))
    if claimed:
        session.commit()
    return claimed


def process_event(session: Session, event: Event) -> None:
    """Match one stored event against the rules and create/cancel jobs."""
    if event.event_type == EVENT_COMMENT_CREATED:
        _process_comment_created(session, event)
    elif event.event_type == EVENT_COMMENT_DELETED:
        _process_comment_deleted(session, event)
    else:
        logger.info(
            "ignoring unsupported event_type=%s event_id=%s",
            event.event_type,
            event.event_id,
        )
        event.status = EventStatus.IGNORED

    event.processed_at = utcnow()
    session.flush()


def _process_comment_created(session: Session, event: Event) -> None:
    if not event.user_id:
        logger.warning(
            "comment.created without data.from.user_id event_id=%s", event.event_id
        )
        event.status = EventStatus.IGNORED
        return

    # Out-of-order safety: the deletion may have arrived before the creation.
    if event.comment_id and _comment_is_deleted(session, event.comment_id):
        logger.info(
            "skipping event for already deleted comment_id=%s event_id=%s",
            event.comment_id,
            event.event_id,
        )
        event.status = EventStatus.IGNORED
        return

    for rule in matching.find_matching_rules(session, event.text):
        jobs.create_job(
            session,
            rule=rule,
            user_id=event.user_id,
            comment_id=event.comment_id,
        )
    event.status = EventStatus.PROCESSED


def _process_comment_deleted(session: Session, event: Event) -> None:
    comment_id = event.comment_id
    if not comment_id:
        logger.warning("comment.deleted without comment_id event_id=%s", event.event_id)
        event.status = EventStatus.IGNORED
        return

    if not _comment_is_deleted(session, comment_id):
        try:
            with session.begin_nested():
                session.add(DeletedComment(comment_id=comment_id))
        except IntegrityError:
            pass

    jobs.cancel_jobs_for_comment(session, comment_id)
    event.status = EventStatus.PROCESSED


def process_pending_events(session: Session, limit: int = 50) -> int:
    """Process one batch of events. Returns how many were handled."""
    events = claim_pending_events(session, limit=limit)
    handled = 0
    for event in events:
        try:
            process_event(session, event)
            session.commit()
            handled += 1
        except Exception:
            session.rollback()
            logger.exception("failed to process event_id=%s", event.event_id)
            # Leave it retryable rather than losing it.
            session.execute(
                update(Event)
                .where(Event.id == event.id)
                .values(status=EventStatus.RECEIVED)
                .execution_options(synchronize_session=False)
            )
            session.commit()
    return handled


def requeue_stuck_events(session: Session) -> int:
    """Events claimed by a worker that died never left `processing`."""
    result = session.execute(
        update(Event)
        .where(Event.status == EventStatus.PROCESSING)
        .values(status=EventStatus.RECEIVED)
        .execution_options(synchronize_session=False)
    )
    if result.rowcount:
        logger.warning("requeued %d stuck event(s)", result.rowcount)
    return int(result.rowcount or 0)
