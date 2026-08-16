"""Shared fixtures: an isolated SQLite database and a configured TestClient."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator

import pytest
from fastapi.testclient import TestClient

from app.config import Settings, get_settings, reset_settings_cache
from app.database import init_db, reset_engine, session_scope
from app.main import app
from app.models import Rule
from app.schemas import WebhookEvent
from app.security import signature_header_value
from app.services import counters, events
from app.services.matching import normalize_keyword

TEST_API_KEY = "test-api-key-do-not-use-in-prod"


@pytest.fixture(scope="session", autouse=True)
def _ignore_local_dotenv() -> None:
    """A developer's real .env must not leak into the test run."""
    Settings.model_config["env_file"] = None


@pytest.fixture(autouse=True)
def isolated_env(tmp_path, monkeypatch) -> Iterator[None]:
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path.as_posix()}")
    monkeypatch.setenv("PSEUDOGRAM_API_KEY", TEST_API_KEY)
    monkeypatch.setenv("VERIFY_WEBHOOK_SIGNATURE", "false")
    monkeypatch.setenv("ENABLE_WORKERS", "false")
    monkeypatch.setenv("MAX_DM_ATTEMPTS", "5")
    monkeypatch.setenv("RETRY_BASE_SECONDS", "1.0")
    monkeypatch.setenv("LOG_LEVEL", "WARNING")

    reset_settings_cache()
    reset_engine()
    init_db()
    with session_scope() as session:
        counters.seed_counters(session)

    yield

    reset_engine()
    reset_settings_cache()


@pytest.fixture
def client() -> Iterator[TestClient]:
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def client_factory(monkeypatch) -> Callable[..., TestClient]:
    """Build a client after overriding environment variables."""

    def factory(**env: str) -> TestClient:
        for key, value in env.items():
            monkeypatch.setenv(key.upper(), value)
        reset_settings_cache()
        reset_engine()
        init_db()
        with session_scope() as session:
            counters.seed_counters(session)
        return TestClient(app)

    return factory


@pytest.fixture
def settings():
    return get_settings()


# --- helpers --------------------------------------------------------------


def make_event(
    event_id: str = "evt_1",
    *,
    event_type: str = "comment.created",
    text: str = "PRICE please",
    user_id: str = "usr_1",
    username: str = "arjun.shoots",
    comment_id: str = "cmt_1",
    post_id: str = "post_1",
    sent_at: str = "2026-08-10T09:14:22.481Z",
) -> dict:
    return {
        "event_id": event_id,
        "event_type": event_type,
        "sent_at": sent_at,
        "data": {
            "comment_id": comment_id,
            "post_id": post_id,
            "text": text,
            "created_at": "2026-08-10T09:14:21.900Z",
            "from": {"user_id": user_id, "username": username},
        },
    }


def make_deleted_event(event_id: str, comment_id: str) -> dict:
    return {
        "event_id": event_id,
        "event_type": "comment.deleted",
        "sent_at": "2026-08-10T09:20:00.000Z",
        "data": {"comment_id": comment_id},
    }


def post_webhook(client: TestClient, payload: dict, *, secret: str | None = None):
    body = json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"}
    if secret is not None:
        headers["X-PseudoGram-Signature"] = signature_header_value(secret, body)
    return client.post("/webhook", content=body, headers=headers)


def create_rule(keyword: str = "PRICE", message: str = "Here is the price list") -> Rule:
    with session_scope() as session:
        rule = Rule(
            keyword=keyword,
            keyword_normalized=normalize_keyword(keyword),
            dm_message=message,
        )
        session.add(rule)
        session.flush()
        session.expunge(rule)
    return rule


def drain_events(limit: int = 500) -> int:
    """Run the matching step that the background worker would normally run."""
    with session_scope() as session:
        return events.process_pending_events(session, limit=limit)


def ingest(payload: dict) -> bool:
    """Persist an event directly (bypassing HTTP). Returns True if duplicate."""
    with session_scope() as session:
        return events.ingest_event(
            session, WebhookEvent.model_validate(payload)
        ).duplicate
