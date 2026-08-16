"""FastAPI application factory and lifecycle."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app import __version__
from app.api import rules, stats, webhook
from app.config import get_settings
from app.database import init_db, session_scope
from app.schemas import HealthOut
from app.services import counters
from app.workers.manager import WorkerManager

logger = logging.getLogger(__name__)


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    configure_logging(settings.log_level)

    init_db()
    with session_scope() as session:
        counters.seed_counters(session)

    if not settings.pseudogram_api_key:
        logger.warning(
            "PSEUDOGRAM_API_KEY is not set: DM sending will fail until it is "
            "configured (webhooks are still accepted and queued)"
        )

    manager: WorkerManager | None = None
    if settings.enable_workers:
        manager = WorkerManager(settings)
        await manager.start()
    else:
        logger.info("background workers disabled (ENABLE_WORKERS=false)")
    app.state.worker_manager = manager

    try:
        yield
    finally:
        if manager is not None:
            await manager.stop()


def create_app() -> FastAPI:
    app = FastAPI(
        title="LinkPlease Mini",
        version=__version__,
        description=(
            "Comment webhooks in, reliably delivered DMs out: deduplicated, "
            "rate limited, retried and reconciled."
        ),
        lifespan=lifespan,
    )

    app.include_router(webhook.router)
    app.include_router(rules.router)
    app.include_router(stats.router)

    @app.get("/health", response_model=HealthOut, tags=["ops"])
    def health() -> HealthOut:
        return HealthOut(status="ok")

    return app


app = create_app()
