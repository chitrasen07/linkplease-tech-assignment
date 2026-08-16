"""Pydantic request/response models (the public API contract)."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class RuleCreate(BaseModel):
    model_config = ConfigDict(extra="ignore")

    keyword: str = Field(max_length=255)
    dm_message: str = Field(max_length=4000)


class RuleOut(BaseModel):
    rule_id: str
    keyword: str
    dm_message: str


class WebhookUser(BaseModel):
    model_config = ConfigDict(extra="ignore")

    user_id: str | None = None
    username: str | None = None


class WebhookData(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    comment_id: str | None = None
    post_id: str | None = None
    text: str | None = None
    created_at: str | None = None
    from_: WebhookUser | None = Field(default=None, alias="from")


class WebhookEvent(BaseModel):
    model_config = ConfigDict(extra="ignore")

    event_id: str = Field(min_length=1, max_length=128)
    event_type: str = Field(min_length=1, max_length=64)
    sent_at: str | None = None
    data: WebhookData = Field(default_factory=WebhookData)


class WebhookAck(BaseModel):
    status: str
    event_id: str
    duplicate: bool = False


class StatsOut(BaseModel):
    sent: int
    failed: int
    queued: int
    duplicates_blocked: int


class HealthOut(BaseModel):
    status: str
