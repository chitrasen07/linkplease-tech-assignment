"""Configuration loading."""

from __future__ import annotations

from app.config import Settings, get_settings, reset_settings_cache


def test_api_key_whitespace_is_stripped(monkeypatch):
    monkeypatch.setenv("PSEUDOGRAM_API_KEY", "  pk_live_abc\n")
    reset_settings_cache()

    assert get_settings().pseudogram_api_key == "pk_live_abc"


def test_signing_secret_defaults_to_the_api_key(monkeypatch):
    monkeypatch.setenv("PSEUDOGRAM_API_KEY", "key-a")
    monkeypatch.delenv("WEBHOOK_SIGNING_SECRET", raising=False)
    reset_settings_cache()

    assert get_settings().signing_secret == "key-a"


def test_signing_secret_can_be_overridden(monkeypatch):
    monkeypatch.setenv("PSEUDOGRAM_API_KEY", "key-a")
    monkeypatch.setenv("WEBHOOK_SIGNING_SECRET", "key-b")
    reset_settings_cache()

    assert get_settings().signing_secret == "key-b"


def test_defaults_are_safe(monkeypatch):
    for name in (
        "VERIFY_WEBHOOK_SIGNATURE",
        "PSEUDOGRAM_API_KEY",
        "MAX_DM_ATTEMPTS",
    ):
        monkeypatch.delenv(name, raising=False)

    defaults = Settings(_env_file=None)

    assert defaults.verify_webhook_signature is True
    assert defaults.pseudogram_api_key == ""
    assert defaults.send_rate_limit_max_calls == 10
    assert defaults.send_rate_limit_window_seconds == 60
    assert defaults.max_dm_attempts == 5


def test_settings_never_expose_the_key_in_health(client):
    body = client.get("/health").json()

    assert body == {"status": "ok"}
