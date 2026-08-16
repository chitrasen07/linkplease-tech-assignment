#!/usr/bin/env python
"""A local mock of the PseudoGram API - for development only.

    python -m uvicorn scripts.mock_pseudogram:app --port 9000

It reproduces the behaviour the real API documents (10 sends per rolling 60s,
occasional 500s, 202-then-maybe-fail delivery, Idempotency-Key dedup) so the
worker, rate limiter and reconciliation loop can be exercised end to end
without an API key. It is never used by the application or by pytest, and it
proves nothing about the real service.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import json
import os
import random
import time
import uuid

import httpx
from fastapi import FastAPI, Header, Request, Response

app = FastAPI(title="Mock PseudoGram")

WINDOW_SECONDS = 60.0
MAX_SENDS = 10
FAILURE_RATE = float(os.getenv("MOCK_DELIVERY_FAILURE_RATE", "0.15"))
ERROR_RATE = float(os.getenv("MOCK_500_RATE", "0.2"))

_send_times: list[float] = []
_dms: dict[str, dict] = {}
_by_idempotency: dict[str, str] = {}
_violations: list[str] = []


@app.post("/v1/dm/send")
async def send_dm(
    request: Request,
    response: Response,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
):
    body = await request.json()
    now = time.time()

    if idempotency_key and idempotency_key in _by_idempotency:
        dm_id = _by_idempotency[idempotency_key]
        response.status_code = 202
        return {"dm_id": dm_id, "status": "queued", "replayed": True}

    _send_times[:] = [t for t in _send_times if t > now - WINDOW_SECONDS]
    if len(_send_times) >= MAX_SENDS:
        _violations.append(f"rate limit exceeded at {now:.1f}")
        response.status_code = 429
        response.headers["Retry-After"] = str(
            int(WINDOW_SECONDS - (now - _send_times[0])) + 1
        )
        return {"error": "rate_limited"}
    _send_times.append(now)

    if not body.get("recipient_user_id"):
        response.status_code = 400
        return {"error": "invalid_request", "detail": "recipient_user_id is required"}

    if random.random() < ERROR_RATE:
        response.status_code = 500
        return {"error": "internal_error"}

    dm_id = f"dm_{uuid.uuid4().hex[:6]}"
    _dms[dm_id] = {
        "dm_id": dm_id,
        "status": "queued",
        "recipient_user_id": body["recipient_user_id"],
        "created": now,
        "will_fail": random.random() < FAILURE_RATE,
    }
    if idempotency_key:
        _by_idempotency[idempotency_key] = dm_id
    response.status_code = 202
    return {"dm_id": dm_id, "status": "queued"}


@app.get("/v1/dm/{dm_id}")
async def get_dm(dm_id: str, response: Response):
    record = _dms.get(dm_id)
    if record is None:
        response.status_code = 404
        return {"error": "not_found"}

    if record["status"] == "queued" and time.time() - record["created"] > 3:
        record["status"] = "failed" if record["will_fail"] else "delivered"
    return {
        "dm_id": dm_id,
        "status": record["status"],
        "recipient_user_id": record["recipient_user_id"],
    }


_runs: dict[str, dict] = {}

MATCH_RATE = 0.8  # share of comments containing the keyword
REDELIVERY_RATE = 0.25  # share of deliveries that repeat an earlier event_id
USER_POOL = int(os.getenv("MOCK_USER_POOL", "12"))


async def _run_simulation(
    run_id: str, webhook_url: str, count: int, duration: int, api_key: str, keyword: str
) -> None:
    """Fire `count` comment webhooks, deliberately including redeliveries."""
    run = _runs[run_id]
    interval = duration / max(1, count)
    history: list[dict] = []
    matching_users: set[str] = set()
    matching_event_ids: set[str] = set()
    matching_deliveries = 0

    async with httpx.AsyncClient(timeout=30) as client:
        for index in range(count):
            if history and random.random() < REDELIVERY_RATE:
                payload = random.choice(history)
            else:
                matches = random.random() < MATCH_RATE
                user_id = f"usr_sim_{random.randrange(USER_POOL)}"
                payload = {
                    "event_id": f"evt_sim_{run_id[:6]}_{index}",
                    "event_type": "comment.created",
                    "sent_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "data": {
                        "comment_id": f"cmt_sim_{index}",
                        "post_id": "post_sim",
                        "text": f"{keyword} please" if matches else "nice shot",
                        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                        "from": {"user_id": user_id, "username": f"u_{user_id}"},
                    },
                }
                history.append(payload)

            if keyword.lower() in payload["data"]["text"].lower():
                matching_deliveries += 1
                matching_users.add(payload["data"]["from"]["user_id"])
                matching_event_ids.add(payload["event_id"])

            body = json.dumps(payload).encode()
            headers = {
                "Content-Type": "application/json",
                "X-PseudoGram-Signature": "sha256="
                + hmac.new(api_key.encode(), body, hashlib.sha256).hexdigest(),
            }
            try:
                response = await client.post(webhook_url, content=body, headers=headers)
                run["delivered"] += 1
                if response.status_code != 200:
                    run["rejected"] += 1
            except httpx.HTTPError:
                run["errors"] += 1
            await asyncio.sleep(interval)

    run["finished"] = True
    run["matching_deliveries"] = matching_deliveries
    run["unique_matching_events"] = len(matching_event_ids)
    run["unique_matching_users"] = len(matching_users)


@app.post("/v1/simulate/start")
async def simulate_start(request: Request, x_api_key: str = Header(default="")):
    body = await request.json()
    run_id = uuid.uuid4().hex[:12]
    _runs[run_id] = {
        "run_id": run_id,
        "delivered": 0,
        "rejected": 0,
        "errors": 0,
        "finished": False,
    }
    asyncio.create_task(
        _run_simulation(
            run_id,
            body["webhook_url"],
            int(body.get("count", 500)),
            int(body.get("duration_seconds", 10)),
            x_api_key,
            os.getenv("MOCK_KEYWORD", "PRICE"),
        )
    )
    return {"run_id": run_id, "status": "started"}


@app.get("/v1/simulate/{run_id}/truth")
async def simulate_truth(run_id: str, response: Response):
    run = _runs.get(run_id)
    if run is None:
        response.status_code = 404
        return {"error": "not_found"}
    users = run.get("unique_matching_users", 0)
    return {
        "run_id": run_id,
        "finished_sending": run["finished"],
        "webhooks_delivered": run["delivered"],
        "webhooks_rejected": run["rejected"],
        "webhook_errors": run["errors"],
        "matching_deliveries": run.get("matching_deliveries", 0),
        "unique_matching_events": run.get("unique_matching_events", 0),
        # Counting blocked DM-job insertions only (the app's default).
        "expected": {
            "sent": users,
            "failed": 0,
            "queued": 0,
            "duplicates_blocked": run.get("unique_matching_events", 0) - users,
        },
        # The alternative reading, where redelivered webhooks also count.
        "expected_counting_redeliveries": {
            "sent": users,
            "failed": 0,
            "queued": 0,
            "duplicates_blocked": run.get("matching_deliveries", 0) - users,
        },
    }


@app.get("/mock/report")
async def report():
    """Everything the mock observed, including rate-limit violations."""
    statuses: dict[str, int] = {}
    for record in _dms.values():
        statuses[record["status"]] = statuses.get(record["status"], 0) + 1
    return {
        "total_sends": len(_dms),
        "statuses": statuses,
        "rate_limit_violations": _violations,
        "idempotency_keys": len(_by_idempotency),
    }


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=9000)
    args = parser.parse_args()
    uvicorn.run(app, host="127.0.0.1", port=args.port)
