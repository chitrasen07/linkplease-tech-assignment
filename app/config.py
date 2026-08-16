"""Application configuration, loaded from environment variables / .env."""

from functools import lru_cache

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- PseudoGram -------------------------------------------------------
    pseudogram_base_url: str = "https://pseudogram-api.onrender.com"
    pseudogram_api_key: str = ""
    pseudogram_timeout_seconds: float = 15.0

    # --- Storage ----------------------------------------------------------
    database_url: str = "sqlite:///./linkplease.db"

    # --- Webhook security -------------------------------------------------
    verify_webhook_signature: bool = True
    # Signatures are HMAC-SHA256(raw_body, key). The key defaults to the
    # PseudoGram API key; override only if the sender signs with something else.
    webhook_signing_secret: str = ""
    max_webhook_body_bytes: int = 65536

    # --- Delivery policy --------------------------------------------------
    # Consecutive send failures (500/timeout) tolerated before giving up.
    max_dm_attempts: int = 5
    # Confirmed delivery failures tolerated before giving up. Separate budget:
    # a flaky API must not consume the retries owed to a real failed delivery.
    max_delivery_attempts: int = 5
    retry_base_seconds: float = 1.0
    retry_jitter_ratio: float = 0.25
    retry_max_backoff_seconds: float = 300.0
    # How long to hold off after PseudoGram rejects our API key.
    auth_error_pause_seconds: float = 60.0

    # --- Workers ----------------------------------------------------------
    enable_workers: bool = True
    worker_poll_interval_seconds: float = 1.0
    event_poll_interval_seconds: float = 0.2
    delivery_poll_interval_seconds: float = 5.0
    # A job stuck in `sending` for longer than this is assumed to belong to a
    # crashed worker and is requeued (the idempotency key makes that safe).
    sending_stale_seconds: float = 120.0

    # --- Rate limiting (POST /v1/dm/send only) ----------------------------
    send_rate_limit_max_calls: int = 10
    send_rate_limit_window_seconds: float = 60.0
    # Small safety margin so clock skew against PseudoGram cannot push us over.
    send_rate_limit_safety_seconds: float = 0.5

    # --- Stats semantics --------------------------------------------------
    # Default (false) counts only blocked DM-job insertions, which is how the
    # assignment defines the counter ("when a duplicate job insertion is
    # blocked"). Set true to also count redelivered webhooks that would have
    # produced the same DM. See README "duplicates_blocked".
    count_duplicate_events_as_blocked: bool = False

    # --- Logging ----------------------------------------------------------
    log_level: str = "INFO"

    @field_validator(
        "pseudogram_api_key",
        "webhook_signing_secret",
        "pseudogram_base_url",
        "database_url",
        mode="after",
    )
    @classmethod
    def _strip(cls, value: str) -> str:
        # A key pasted into a hosting dashboard often carries a stray newline,
        # which would silently break every HMAC comparison.
        return value.strip()

    @property
    def signing_secret(self) -> str:
        return self.webhook_signing_secret or self.pseudogram_api_key


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def reset_settings_cache() -> None:
    """Used by tests after mutating the environment."""
    get_settings.cache_clear()
