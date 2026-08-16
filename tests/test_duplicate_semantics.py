"""The five duplicate scenarios, spelled out under both counting modes.

`duplicates_blocked` is the one statistic the assignment defines ambiguously.
Section 7 says to increment it "when a duplicate job insertion is blocked",
while section 6 describes duplicate `event_id` handling without mentioning the
counter at all. These tests pin down exactly what this implementation does in
every case, so the behaviour is a documented decision rather than an accident.

Only scenario A/E - an identical webhook delivered twice - differs between the
two modes. Default (`COUNT_DUPLICATE_EVENTS_AS_BLOCKED=false`) counts blocked
job insertions only.
"""

from __future__ import annotations

import pytest
from sqlalchemy import func, select

from app.database import session_scope
from app.models import CounterKey, DMJob
from app.services import counters
from tests.conftest import create_rule, drain_events, make_event, post_webhook


def outcome() -> tuple[int, int]:
    with session_scope() as session:
        jobs = session.scalar(select(func.count(DMJob.id)))
        blocked = counters.get(session, CounterKey.DUPLICATES_BLOCKED)
    return int(jobs), blocked


@pytest.fixture
def strict_client(client_factory):
    """Default mode: only blocked job insertions count."""
    return client_factory(count_duplicate_events_as_blocked="false")


@pytest.fixture
def delivery_client(client_factory):
    """Alternative mode: every matching delivery that makes no job counts."""
    return client_factory(count_duplicate_events_as_blocked="true")


# --- A / E: the same event_id delivered twice -----------------------------


def test_a_same_event_twice_default_counts_no_duplicate(strict_client):
    with strict_client as client:
        create_rule()
        payload = make_event("evt_1")
        post_webhook(client, payload)
        post_webhook(client, payload)
        drain_events()

    assert outcome() == (1, 0)


def test_a_same_event_twice_counts_a_duplicate_in_delivery_mode(delivery_client):
    with delivery_client as client:
        create_rule()
        payload = make_event("evt_1")
        post_webhook(client, payload)
        post_webhook(client, payload)
        drain_events()

    assert outcome() == (1, 1)


def test_e_same_event_same_user_same_rule_many_times(strict_client):
    with strict_client as client:
        create_rule()
        payload = make_event("evt_1")
        for _ in range(10):
            post_webhook(client, payload)
        drain_events()

    assert outcome() == (1, 0)


# --- B: different events, same user, same rule ----------------------------


def test_b_different_events_same_user_same_rule(strict_client):
    """Both modes agree: the second event is a genuinely blocked DM."""
    with strict_client as client:
        create_rule()
        post_webhook(client, make_event("evt_1", comment_id="c1"))
        post_webhook(client, make_event("evt_2", comment_id="c2"))
        drain_events()

    assert outcome() == (1, 1)


def test_b_also_counts_one_in_delivery_mode(delivery_client):
    with delivery_client as client:
        create_rule()
        post_webhook(client, make_event("evt_1", comment_id="c1"))
        post_webhook(client, make_event("evt_2", comment_id="c2"))
        drain_events()

    assert outcome() == (1, 1)


# --- C: same user, different rules ----------------------------------------


def test_c_same_user_different_rules_gets_both_dms(strict_client):
    with strict_client as client:
        create_rule("PRICE", "price list")
        create_rule("LINK", "the link")
        post_webhook(client, make_event("evt_1", text="PRICE and LINK please"))
        drain_events()

    assert outcome() == (2, 0)


# --- D: different users, same rule ----------------------------------------


def test_d_different_users_same_rule_each_get_a_dm(strict_client):
    with strict_client as client:
        create_rule()
        post_webhook(client, make_event("evt_1", user_id="usr_1", comment_id="c1"))
        post_webhook(client, make_event("evt_2", user_id="usr_2", comment_id="c2"))
        drain_events()

    assert outcome() == (2, 0)


# --- the invariant each mode maintains ------------------------------------


def test_default_mode_invariant_is_per_unique_event(strict_client):
    """jobs + duplicates_blocked == unique matching events."""
    with strict_client as client:
        create_rule()
        unique_events = [
            make_event(f"evt_{i}", user_id=f"usr_{i % 3}", comment_id=f"c{i}")
            for i in range(9)
        ]
        for payload in unique_events:
            post_webhook(client, payload)
            post_webhook(client, payload)  # redelivered
        drain_events()

    jobs, blocked = outcome()
    assert jobs == 3
    assert jobs + blocked == len(unique_events)


def test_delivery_mode_invariant_is_per_delivery(delivery_client):
    """jobs + duplicates_blocked == matching deliveries."""
    with delivery_client as client:
        create_rule()
        unique_events = [
            make_event(f"evt_{i}", user_id=f"usr_{i % 3}", comment_id=f"c{i}")
            for i in range(9)
        ]
        deliveries = 0
        for payload in unique_events:
            post_webhook(client, payload)
            post_webhook(client, payload)
            deliveries += 2
        drain_events()

    jobs, blocked = outcome()
    assert jobs == 3
    assert jobs + blocked == deliveries
