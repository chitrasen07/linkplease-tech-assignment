"""Keyword matching.

Rules match case-insensitively, anywhere inside the comment text, so
"PRICE", "price please" and "what is the price?" all match keyword PRICE.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Rule


def normalize_keyword(keyword: str) -> str:
    return keyword.strip().lower()


def text_matches(keyword_normalized: str, text: str | None) -> bool:
    if not keyword_normalized or not text:
        return False
    return keyword_normalized in text.lower()


def active_rules(session: Session) -> list[Rule]:
    return list(session.scalars(select(Rule).where(Rule.is_active == 1)).all())


def find_matching_rules(session: Session, text: str | None) -> list[Rule]:
    if not text:
        return []
    lowered = text.lower()
    return [r for r in active_rules(session) if r.keyword_normalized in lowered]
