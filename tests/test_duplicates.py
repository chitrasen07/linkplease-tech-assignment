"""Duplicate events and - more importantly - duplicate DMs."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import func, select

from app.database import session_scope
from app.models import CounterKey, DMJob, Event
from app.services import counters
from app.services.jobs import CreateOutcome, create_job
from tests.conftest import create_rule, drain_events, make_event, post_webhook


def _job_count() -> int:
    with session_scope() as session:
        return session.scalar(select(func.count(DMJob.id)))


def _duplicates_blocked() -> int:
    with session_scope() as session:
        return counters.get(session, CounterKey.DUPLICATES_BLOCKED)


def test_duplicate_event_id_is_ignored(client):
    create_rule()
    payload = make_event("evt_dup")

    first = post_webhook(client, payload)
    second = post_webhook(client, payload)
    third = post_webhook(client, payload)

    assert [r.status_code for r in (first, second, third)] == [200, 200, 200]
    assert first.json()["duplicate"] is False
    assert second.json()["duplicate"] is True

    with session_scope() as session:
        stored = session.scalar(
            select(func.count(Event.id)).where(Event.event_id == "evt_dup")
        )
    assert stored == 1

    drain_events()
    assert _job_count() == 1


def test_same_user_same_rule_creates_only_one_job(client):
    create_rule()
    post_webhook(client, make_event("evt_1", comment_id="cmt_1"))
    post_webhook(client, make_event("evt_2", comment_id="cmt_2"))
    drain_events()

    assert _job_count() == 1
    assert _duplicates_blocked() == 1


def test_database_constraint_blocks_duplicate_even_without_prior_check():
    rule = create_rule()

    with session_scope() as session:
        first = create_job(session, rule=rule, user_id="usr_1", comment_id="c1")
        second = create_job(session, rule=rule, user_id="usr_1", comment_id="c2")

    assert first.outcome is CreateOutcome.CREATED
    assert second.outcome is CreateOutcome.DUPLICATE
    assert _job_count() == 1
    assert _duplicates_blocked() == 1


def test_concurrent_duplicate_inserts_produce_one_job():
    """Simulates two webhooks for the same user racing in separate sessions."""
    rule = create_rule()
    rule_id = rule.id

    def attempt(index: int) -> str:
        from app.models import Rule

        with session_scope() as session:
            local_rule = session.get(Rule, rule_id)
            return create_job(
                session, rule=local_rule, user_id="usr_race", comment_id=f"c{index}"
            ).outcome.value

    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(pool.map(attempt, range(8)))

    assert outcomes.count("created") == 1
    assert outcomes.count("duplicate") == 7
    assert _job_count() == 1
    assert _duplicates_blocked() == 7


def test_different_users_each_get_a_job(client):
    create_rule()
    post_webhook(client, make_event("evt_1", user_id="usr_1", comment_id="c1"))
    post_webhook(client, make_event("evt_2", user_id="usr_2", comment_id="c2"))
    drain_events()

    assert _job_count() == 2
    assert _duplicates_blocked() == 0


def test_same_user_gets_one_job_per_rule(client):
    create_rule("PRICE", "price list")
    create_rule("LINK", "here is the link")

    post_webhook(client, make_event("evt_1", text="PRICE and LINK please"))
    drain_events()

    with session_scope() as session:
        jobs = session.scalars(select(DMJob)).all()
        messages = sorted(job.message for job in jobs)
    assert messages == ["here is the link", "price list"]
    assert _duplicates_blocked() == 0


def test_redelivered_event_is_not_counted_as_a_blocked_duplicate_by_default(client):
    """A redelivery is the same logical event, not a second DM decision."""
    create_rule()
    payload = make_event("evt_redelivered")

    post_webhook(client, payload)
    drain_events()
    post_webhook(client, payload)  # exact redelivery

    assert _job_count() == 1
    assert _duplicates_blocked() == 0


def test_redelivered_non_matching_event_is_not_counted(client):
    create_rule()
    payload = make_event("evt_nomatch", text="looks great!")

    post_webhook(client, payload)
    post_webhook(client, payload)
    drain_events()

    assert _job_count() == 0
    assert _duplicates_blocked() == 0


def test_counting_redeliveries_can_be_enabled(client_factory):
    client = client_factory(count_duplicate_events_as_blocked="true")
    with client:
        create_rule()
        payload = make_event("evt_cfg")
        post_webhook(client, payload)
        post_webhook(client, payload)
        drain_events()

    assert _job_count() == 1
    assert _duplicates_blocked() == 1


def test_duplicate_events_do_not_create_extra_events_after_restart(client):
    """Dedup lives in the database, so it survives losing all process state."""
    create_rule()
    payload = make_event("evt_persist")
    post_webhook(client, payload)

    from app.database import reset_engine

    reset_engine()  # everything in memory is gone

    assert post_webhook(client, payload).json()["duplicate"] is True
    drain_events()
    assert _job_count() == 1
