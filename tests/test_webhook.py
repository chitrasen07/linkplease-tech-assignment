"""POST /webhook contract: fast, validating, persistent."""

from __future__ import annotations

import time

from sqlalchemy import select

from app.database import session_scope
from app.models import Event, EventStatus
from tests.conftest import make_deleted_event, make_event, post_webhook


def test_valid_event_is_accepted_and_persisted(client):
    response = post_webhook(client, make_event("evt_a"))

    assert response.status_code == 200
    assert response.json()["duplicate"] is False

    with session_scope() as session:
        event = session.scalar(select(Event).where(Event.event_id == "evt_a"))
        assert event is not None
        assert event.user_id == "usr_1"
        assert event.username == "arjun.shoots"
        assert event.text == "PRICE please"
        assert event.status == EventStatus.RECEIVED
        assert event.sent_at is not None


def test_webhook_responds_quickly(client):
    started = time.perf_counter()
    post_webhook(client, make_event("evt_fast"))
    elapsed = time.perf_counter() - started

    assert elapsed < 1.0, f"webhook took {elapsed:.3f}s (contract allows 5s)"


def test_invalid_json_is_rejected(client):
    response = client.post(
        "/webhook", content=b"{not json", headers={"Content-Type": "application/json"}
    )
    assert response.status_code == 400


def test_missing_event_id_is_rejected(client):
    payload = make_event("evt_x")
    del payload["event_id"]

    assert post_webhook(client, payload).status_code == 400


def test_oversized_body_is_rejected(client):
    payload = make_event("evt_big", text="x" * 70000)
    assert post_webhook(client, payload).status_code == 413


def test_comment_deleted_event_is_accepted(client):
    response = post_webhook(client, make_deleted_event("evt_del", "cmt_1"))

    assert response.status_code == 200
    with session_scope() as session:
        event = session.scalar(select(Event).where(Event.event_id == "evt_del"))
        assert event.event_type == "comment.deleted"
        assert event.comment_id == "cmt_1"
        assert event.user_id is None


def test_unknown_event_type_is_stored_but_ignored(client):
    post_webhook(client, make_event("evt_other", event_type="comment.liked"))

    from tests.conftest import drain_events

    drain_events()
    with session_scope() as session:
        event = session.scalar(select(Event).where(Event.event_id == "evt_other"))
        assert event.status == EventStatus.IGNORED


def test_out_of_order_sent_at_is_still_accepted(client):
    newer = post_webhook(client, make_event("evt_new", sent_at="2026-08-10T10:00:00Z"))
    older = post_webhook(client, make_event("evt_old", sent_at="2026-08-10T09:00:00Z"))

    assert newer.status_code == 200
    assert older.status_code == 200
    with session_scope() as session:
        assert session.scalar(select(Event).where(Event.event_id == "evt_old"))


def test_unparseable_sent_at_does_not_break_ingestion(client):
    response = post_webhook(client, make_event("evt_ts", sent_at="not-a-timestamp"))

    assert response.status_code == 200
    with session_scope() as session:
        assert (
            session.scalar(select(Event).where(Event.event_id == "evt_ts")).sent_at
            is None
        )


def test_health_endpoint(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_openapi_docs_are_available(client):
    assert client.get("/docs").status_code == 200
    assert client.get("/redoc").status_code == 200
    schema = client.get("/openapi.json").json()
    assert "/webhook" in schema["paths"]
    assert "/rules" in schema["paths"]
    assert "/stats" in schema["paths"]
