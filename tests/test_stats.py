"""GET /stats accounting."""

from __future__ import annotations

from app.database import session_scope
from app.models import CounterKey, DMJob, JobStatus
from app.services import counters
from tests.conftest import create_rule, drain_events, make_event, post_webhook


def add_job(status: JobStatus, user_id: str, *, keyword: str | None = None) -> int:
    rule = create_rule(keyword or f"kw_{user_id}", "msg")
    with session_scope() as session:
        job = DMJob(
            rule_id=rule.id,
            user_id=user_id,
            comment_id="cmt",
            message="msg",
            status=status,
        )
        session.add(job)
        session.flush()
        return job.id


def test_empty_stats(client):
    assert client.get("/stats").json() == {
        "sent": 0,
        "failed": 0,
        "queued": 0,
        "duplicates_blocked": 0,
    }


def test_stats_counts_each_status_once(client):
    add_job(JobStatus.DELIVERED, "u1")
    add_job(JobStatus.DELIVERED, "u2")
    add_job(JobStatus.FAILED, "u3")
    add_job(JobStatus.QUEUED, "u4")
    add_job(JobStatus.SENDING, "u5")
    add_job(JobStatus.ACCEPTED, "u6")
    add_job(JobStatus.RETRY_WAIT, "u7")
    with session_scope() as session:
        counters.increment(session, CounterKey.DUPLICATES_BLOCKED, 3)

    assert client.get("/stats").json() == {
        "sent": 2,
        "failed": 1,
        "queued": 4,
        "duplicates_blocked": 3,
    }


def test_cancelled_jobs_are_counted_nowhere(client):
    add_job(JobStatus.CANCELLED, "u1")

    body = client.get("/stats").json()
    assert body == {"sent": 0, "failed": 0, "queued": 0, "duplicates_blocked": 0}


def test_accepted_job_counts_as_queued_not_sent(client):
    add_job(JobStatus.ACCEPTED, "u1")

    body = client.get("/stats").json()
    assert body["queued"] == 1
    assert body["sent"] == 0


def test_retry_wait_counts_as_queued_not_failed(client):
    add_job(JobStatus.RETRY_WAIT, "u1")

    body = client.get("/stats").json()
    assert body["queued"] == 1
    assert body["failed"] == 0


def test_every_job_appears_in_exactly_one_bucket(client):
    for index, status in enumerate(
        [
            JobStatus.QUEUED,
            JobStatus.SENDING,
            JobStatus.ACCEPTED,
            JobStatus.RETRY_WAIT,
            JobStatus.DELIVERED,
            JobStatus.FAILED,
        ]
    ):
        add_job(status, f"u{index}")

    body = client.get("/stats").json()
    assert body["sent"] + body["failed"] + body["queued"] == 6


def test_redeliveries_inflate_nothing(client):
    create_rule()
    payload = make_event("evt_1")
    for _ in range(5):
        post_webhook(client, payload)
    drain_events()

    body = client.get("/stats").json()
    assert body["queued"] == 1
    assert body["duplicates_blocked"] == 0


def test_repeat_comments_from_one_user_count_as_blocked_duplicates(client):
    """Distinct events, same user and rule: four DMs correctly not sent."""
    create_rule()
    for index in range(5):
        post_webhook(client, make_event(f"evt_{index}", comment_id=f"c{index}"))
    drain_events()

    body = client.get("/stats").json()
    assert body["queued"] == 1
    assert body["duplicates_blocked"] == 4


def test_stats_survive_engine_reset(client):
    add_job(JobStatus.DELIVERED, "u1")

    from app.database import reset_engine

    reset_engine()

    assert client.get("/stats").json()["sent"] == 1
