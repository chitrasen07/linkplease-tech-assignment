"""comment.deleted handling (including out-of-order deliveries)."""

from __future__ import annotations

from sqlalchemy import select

from app.database import session_scope
from app.models import DeletedComment, DMJob, JobStatus
from tests.conftest import (
    create_rule,
    drain_events,
    make_deleted_event,
    make_event,
    post_webhook,
)


def jobs_by_user(user_id: str) -> list[DMJob]:
    with session_scope() as session:
        found = list(session.scalars(select(DMJob).where(DMJob.user_id == user_id)))
        for job in found:
            session.expunge(job)
        return found


def test_deletion_cancels_a_still_queued_job(client):
    create_rule()
    post_webhook(client, make_event("evt_1", comment_id="cmt_x", user_id="usr_1"))
    drain_events()
    assert jobs_by_user("usr_1")[0].status == JobStatus.QUEUED

    post_webhook(client, make_deleted_event("evt_2", "cmt_x"))
    drain_events()

    assert jobs_by_user("usr_1")[0].status == JobStatus.CANCELLED


def test_deletion_cancels_a_job_waiting_to_retry(client):
    create_rule()
    post_webhook(client, make_event("evt_1", comment_id="cmt_x", user_id="usr_1"))
    drain_events()
    with session_scope() as session:
        session.scalar(select(DMJob)).status = JobStatus.RETRY_WAIT

    post_webhook(client, make_deleted_event("evt_2", "cmt_x"))
    drain_events()

    assert jobs_by_user("usr_1")[0].status == JobStatus.CANCELLED


def test_delivered_dm_is_never_cancelled(client):
    create_rule()
    post_webhook(client, make_event("evt_1", comment_id="cmt_x", user_id="usr_1"))
    drain_events()
    with session_scope() as session:
        session.scalar(select(DMJob)).status = JobStatus.DELIVERED

    post_webhook(client, make_deleted_event("evt_2", "cmt_x"))
    drain_events()

    assert jobs_by_user("usr_1")[0].status == JobStatus.DELIVERED
    assert client.get("/stats").json()["sent"] == 1


def test_accepted_dm_is_not_cancelled(client):
    """Once PseudoGram has the DM it cannot be un-sent."""
    create_rule()
    post_webhook(client, make_event("evt_1", comment_id="cmt_x", user_id="usr_1"))
    drain_events()
    with session_scope() as session:
        job = session.scalar(select(DMJob))
        job.status = JobStatus.ACCEPTED
        job.dm_id = "dm_1"

    post_webhook(client, make_deleted_event("evt_2", "cmt_x"))
    drain_events()

    assert jobs_by_user("usr_1")[0].status == JobStatus.ACCEPTED


def test_deletion_does_not_touch_unrelated_jobs(client):
    create_rule()
    post_webhook(client, make_event("evt_1", comment_id="cmt_a", user_id="usr_1"))
    post_webhook(client, make_event("evt_2", comment_id="cmt_b", user_id="usr_2"))
    drain_events()

    post_webhook(client, make_deleted_event("evt_3", "cmt_a"))
    drain_events()

    assert jobs_by_user("usr_1")[0].status == JobStatus.CANCELLED
    assert jobs_by_user("usr_2")[0].status == JobStatus.QUEUED


def test_deletion_arriving_before_creation_prevents_the_job(client):
    """Out-of-order delivery: the delete lands first."""
    create_rule()
    post_webhook(client, make_deleted_event("evt_del", "cmt_late"))
    drain_events()

    post_webhook(client, make_event("evt_new", comment_id="cmt_late", user_id="usr_1"))
    drain_events()

    assert jobs_by_user("usr_1") == []
    with session_scope() as session:
        assert session.get(DeletedComment, "cmt_late") is not None


def test_deleted_event_without_comment_id_is_ignored(client):
    payload = make_deleted_event("evt_del", "cmt_x")
    payload["data"] = {}
    assert post_webhook(client, payload).status_code == 200
    drain_events()

    with session_scope() as session:
        assert session.scalars(select(DeletedComment)).all() == []


def test_a_later_comment_from_the_same_user_can_still_trigger_the_rule(client):
    """A cancelled job never DMed anyone, so it must not block the user."""
    create_rule()
    post_webhook(client, make_event("evt_1", comment_id="cmt_x", user_id="usr_1"))
    drain_events()
    post_webhook(client, make_deleted_event("evt_2", "cmt_x"))
    drain_events()

    post_webhook(client, make_event("evt_3", comment_id="cmt_y", user_id="usr_1"))
    drain_events()

    statuses = sorted(job.status for job in jobs_by_user("usr_1"))
    assert statuses == [JobStatus.CANCELLED, JobStatus.QUEUED]
    assert client.get("/stats").json()["queued"] == 1
    assert client.get("/stats").json()["duplicates_blocked"] == 0


def test_a_sent_job_still_blocks_a_later_comment(client):
    """The carve-out is only for cancelled jobs; delivered ones still hold."""
    create_rule()
    post_webhook(client, make_event("evt_1", comment_id="cmt_x", user_id="usr_1"))
    drain_events()
    with session_scope() as session:
        session.scalar(select(DMJob)).status = JobStatus.DELIVERED

    post_webhook(client, make_event("evt_2", comment_id="cmt_y", user_id="usr_1"))
    drain_events()

    assert len(jobs_by_user("usr_1")) == 1
    assert client.get("/stats").json()["duplicates_blocked"] == 1


def test_cancelled_job_is_not_counted_in_stats(client):
    create_rule()
    post_webhook(client, make_event("evt_1", comment_id="cmt_x", user_id="usr_1"))
    drain_events()
    post_webhook(client, make_deleted_event("evt_2", "cmt_x"))
    drain_events()

    assert client.get("/stats").json() == {
        "sent": 0,
        "failed": 0,
        "queued": 0,
        "duplicates_blocked": 0,
    }
