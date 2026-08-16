"""Persistent counters.

`duplicates_blocked` cannot be derived from job rows (a blocked duplicate never
becomes a row), so it lives in a counters table and is incremented with a single
atomic UPDATE statement rather than read-modify-write in Python.
"""

from __future__ import annotations

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models import Counter, CounterKey


def seed_counters(session: Session) -> None:
    existing = set(session.scalars(select(Counter.key)).all())
    for key in CounterKey:
        if key.value not in existing:
            session.add(Counter(key=key.value, value=0))
    session.flush()


def increment(session: Session, key: CounterKey | str, amount: int = 1) -> None:
    if amount == 0:
        return
    key = str(key)
    result = session.execute(
        update(Counter)
        .where(Counter.key == key)
        .values(value=Counter.value + amount)
        .execution_options(synchronize_session=False)
    )
    if result.rowcount:
        return
    # First use of this counter: create the row, tolerating a concurrent create.
    try:
        with session.begin_nested():
            session.add(Counter(key=key, value=amount))
    except IntegrityError:
        session.execute(
            update(Counter)
            .where(Counter.key == key)
            .values(value=Counter.value + amount)
            .execution_options(synchronize_session=False)
        )


def get(session: Session, key: CounterKey | str) -> int:
    value = session.scalar(select(Counter.value).where(Counter.key == str(key)))
    return int(value or 0)
