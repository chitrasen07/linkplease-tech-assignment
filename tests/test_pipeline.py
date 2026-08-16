"""End-to-end flow with a fake PseudoGram: webhook in, `sent` out."""

from __future__ import annotations

import asyncio

from app.config import get_settings
from app.services.rate_limiter import RollingWindowRateLimiter
from app.services.reconciliation import ReconciliationService
from app.workers.dm_worker import DMWorker
from app.workers.event_worker import EventWorker
from tests import fakes
from tests.conftest import create_rule, make_event, post_webhook


def unlimited() -> RollingWindowRateLimiter:
    return RollingWindowRateLimiter(max_calls=10_000, window_seconds=60)


async def drain_all(client_fake) -> None:
    settings = get_settings()
    await EventWorker(settings).run_once()
    worker = DMWorker(client_fake, unlimited(), settings)
    while await worker.run_once():
        pass
    await ReconciliationService(client_fake, settings).run_once()


async def test_full_happy_path_ends_in_sent(client):
    create_rule()
    post_webhook(client, make_event("evt_1", user_id="usr_1"))

    fake = fakes.FakePseudoGramClient(
        [fakes.accepted("dm_1")], statuses={"dm_1": ["delivered"]}
    )
    await drain_all(fake)

    assert client.get("/stats").json() == {
        "sent": 1,
        "failed": 0,
        "queued": 0,
        "duplicates_blocked": 0,
    }


async def test_accepted_but_not_yet_delivered_is_queued(client):
    create_rule()
    post_webhook(client, make_event("evt_1", user_id="usr_1"))

    fake = fakes.FakePseudoGramClient(
        [fakes.accepted("dm_1")], statuses={"dm_1": ["queued"]}
    )
    await drain_all(fake)

    body = client.get("/stats").json()
    assert body["sent"] == 0
    assert body["queued"] == 1


async def test_permanent_failure_is_counted_once(client):
    create_rule()
    post_webhook(client, make_event("evt_1", user_id="usr_1"))

    fake = fakes.FakePseudoGramClient([fakes.invalid_request()])
    await drain_all(fake)

    body = client.get("/stats").json()
    assert body == {"sent": 0, "failed": 1, "queued": 0, "duplicates_blocked": 0}
    assert len(fake.sends) == 1  # 400 is never retried


async def test_burst_of_duplicate_events_produces_one_dm_per_user(client):
    """200 deliveries of 20 logical events, 3 users, 1 rule -> 3 DMs."""
    create_rule()
    users = ["usr_a", "usr_b", "usr_c"]
    logical_events = [(f"evt_{i}", users[i % 3], f"cmt_{i}") for i in range(20)]

    delivered = 0
    for _ in range(10):  # PseudoGram redelivers everything ten times
        for event_id, user, comment_id in logical_events:
            response = post_webhook(
                client, make_event(event_id, user_id=user, comment_id=comment_id)
            )
            assert response.status_code == 200
            delivered += 1

    fake = fakes.FakePseudoGramClient(
        None, statuses={f"dm_{i}": ["delivered"] for i in range(1, 10)}
    )
    await drain_all(fake)

    body = client.get("/stats").json()
    assert body["sent"] == 3
    assert body["failed"] == 0
    assert body["queued"] == 0
    # Every *logical* event either became a DM or was a blocked duplicate;
    # the 180 redeliveries are absorbed by event deduplication.
    assert delivered == 200
    assert body["sent"] + body["duplicates_blocked"] == len(logical_events)
    assert len(fake.sends) == 3


async def test_workers_start_and_stop_cleanly(client_factory):
    """Lifespan starts the loops and shutdown cancels them."""
    from app.workers.manager import WorkerManager

    manager = WorkerManager(get_settings())
    manager.client = fakes.FakePseudoGramClient()
    manager.dm_worker._client = manager.client
    manager.reconciler._client = manager.client

    await manager.start()
    await asyncio.sleep(0.2)
    assert all(not task.done() for task in manager._tasks)

    await manager.stop()
    assert manager._tasks == []
