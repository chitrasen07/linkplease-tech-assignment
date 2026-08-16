"""Rule management endpoints."""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Rule
from app.schemas import RuleCreate, RuleOut
from app.services.matching import normalize_keyword

logger = logging.getLogger(__name__)

router = APIRouter(tags=["rules"])

DbSession = Annotated[Session, Depends(get_db)]


@router.post("/rules", response_model=RuleOut, status_code=status.HTTP_201_CREATED)
def create_rule(payload: RuleCreate, db: DbSession) -> RuleOut:
    keyword = payload.keyword.strip()
    message = payload.dm_message.strip()

    if not keyword:
        raise HTTPException(status_code=400, detail="keyword must not be empty")
    if not message:
        raise HTTPException(status_code=400, detail="dm_message must not be empty")

    rule = Rule(
        keyword=keyword,
        keyword_normalized=normalize_keyword(keyword),
        dm_message=message,
    )
    db.add(rule)
    db.flush()
    logger.info("rule created rule_id=%s keyword=%r", rule.id, rule.keyword)
    return RuleOut(rule_id=rule.id, keyword=rule.keyword, dm_message=rule.dm_message)


@router.get("/rules", response_model=list[RuleOut])
def list_rules(db: DbSession) -> list[RuleOut]:
    rules = db.scalars(select(Rule).where(Rule.is_active == 1).order_by(Rule.created_at))
    return [
        RuleOut(rule_id=r.id, keyword=r.keyword, dm_message=r.dm_message) for r in rules
    ]
