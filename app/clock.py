"""Time helpers.

Every datetime stored in the database is a *naive UTC* datetime. SQLite drops
timezone information, so mixing aware and naive values would raise at
comparison time; normalising on the way in removes that whole class of bug.
"""

from __future__ import annotations

from datetime import UTC, datetime


def utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def to_naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def parse_iso8601(value: str | None) -> datetime | None:
    """Parse an ISO-8601 timestamp (including the trailing `Z` form)."""
    if not value:
        return None
    text = value.strip()
    if text.endswith(("z", "Z")):
        text = text[:-1] + "+00:00"
    try:
        return to_naive_utc(datetime.fromisoformat(text))
    except ValueError:
        return None
