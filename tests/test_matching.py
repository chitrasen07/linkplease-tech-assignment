"""Keyword matching rules."""

from __future__ import annotations

import pytest
from sqlalchemy import select

from app.database import session_scope
from app.models import DMJob, Event, EventStatus
from app.services.matching import find_matching_rules, text_matches
from tests.conftest import create_rule, drain_events, make_event, post_webhook


@pytest.mark.parametrize(
    "text",
    [
        "PRICE",
        "price please",
        "Can you send me the PRICE?",
        "what is the price?",
        "PrIcE 🙏",
        "no space:price!",
    ],
)
def test_keyword_matches_case_insensitively_anywhere(text):
    assert text_matches("price", text)


@pytest.mark.parametrize("text", ["", "how much?", "pri ce", None])
def test_non_matching_text(text):
    assert not text_matches("price", text)


def test_rule_keyword_is_normalised_before_matching(client):
    client.post("/rules", json={"keyword": "  PrIcE  ", "dm_message": "hi"})

    with session_scope() as session:
        assert [r.keyword for r in find_matching_rules(session, "the price is?")] == [
            "PrIcE"
        ]


def test_job_uses_user_id_not_username(client):
    create_rule()
    post_webhook(client, make_event("evt_1", user_id="usr_abc", username="changed.name"))
    drain_events()

    with session_scope() as session:
        job = session.scalar(select(DMJob))
    assert job.user_id == "usr_abc"


def test_comment_matching_two_rules_creates_two_jobs(client):
    create_rule("PRICE", "price list")
    create_rule("LINK", "link list")
    post_webhook(client, make_event("evt_1", text="PRICE and LINK please"))
    drain_events()

    with session_scope() as session:
        assert session.scalars(select(DMJob)).all().__len__() == 2


def test_no_rules_means_no_jobs(client):
    post_webhook(client, make_event("evt_1"))
    drain_events()

    with session_scope() as session:
        assert session.scalars(select(DMJob)).all() == []
        assert session.scalar(select(Event)).status == EventStatus.PROCESSED


def test_event_without_user_id_is_ignored(client):
    create_rule()
    payload = make_event("evt_nouser")
    payload["data"]["from"] = {}
    post_webhook(client, payload)
    drain_events()

    with session_scope() as session:
        assert session.scalars(select(DMJob)).all() == []
        assert session.scalar(select(Event)).status == EventStatus.IGNORED


def test_rules_created_after_the_event_do_not_match_it(client):
    """Matching happens when the event is processed, not retroactively."""
    post_webhook(client, make_event("evt_early"))
    drain_events()
    create_rule()

    with session_scope() as session:
        assert session.scalars(select(DMJob)).all() == []
