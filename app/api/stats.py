"""Statistics endpoint.

Every number is derived from persistent state at read time, so retries and
redeliveries cannot double count anything.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.database import get_db
from app.schemas import StatsOut
from app.services.jobs import compute_stats

router = APIRouter(tags=["stats"])

DbSession = Annotated[Session, Depends(get_db)]


@router.get("/stats", response_model=StatsOut)
def get_stats(db: DbSession) -> StatsOut:
    return StatsOut(**compute_stats(db))
