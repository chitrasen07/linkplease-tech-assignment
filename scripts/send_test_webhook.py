#!/usr/bin/env python
"""Send a correctly signed test webhook to a running instance.

    python scripts/send_test_webhook.py --url http://localhost:8000 --text "PRICE?"

Signs the exact bytes it sends, which is the same thing the server verifies.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.security import SIGNATURE_HEADER, signature_header_value  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--event-id", default=None, help="reuse one to test dedup")
    parser.add_argument("--event-type", default="comment.created")
    parser.add_argument("--text", default="PRICE please 🙏")
    parser.add_argument("--user-id", default="usr_3b91fe")
    parser.add_argument("--username", default="arjun.shoots")
    parser.add_argument("--comment-id", default="cmt_9f2a7c")
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--unsigned", action="store_true")
    args = parser.parse_args()

    secret = os.getenv("WEBHOOK_SIGNING_SECRET") or os.getenv("PSEUDOGRAM_API_KEY", "")
    if not secret and not args.unsigned:
        print("[WARN] no signing secret in the environment; sending unsigned")

    for index in range(args.repeat):
        event_id = args.event_id or f"evt_{uuid.uuid4().hex[:12]}"
        payload = {
            "event_id": event_id,
            "event_type": args.event_type,
            "sent_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "data": {
                "comment_id": args.comment_id,
                "post_id": "post_44de1b",
                "text": args.text,
                "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                "from": {"user_id": args.user_id, "username": args.username},
            },
        }
        body = json.dumps(payload).encode()
        headers = {"Content-Type": "application/json"}
        if secret and not args.unsigned:
            headers[SIGNATURE_HEADER] = signature_header_value(secret, body)

        response = httpx.post(
            f"{args.url.rstrip('/')}/webhook", content=body, headers=headers, timeout=30
        )
        print(f"[{index + 1}/{args.repeat}] {response.status_code} {response.text}")

    print("\nstats:", httpx.get(f"{args.url.rstrip('/')}/stats", timeout=30).text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
