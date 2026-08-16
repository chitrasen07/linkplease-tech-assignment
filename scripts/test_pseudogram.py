#!/usr/bin/env python
"""Run the official 500-event simulation against a deployed instance.

    python scripts/test_pseudogram.py --app-url https://my-app.onrender.com

It checks the environment, creates a rule, starts the simulation, polls /stats
while it drains, then prints our numbers next to PseudoGram's truth. Nothing
here runs automatically - the application never calls /v1/simulate/start by
itself.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

import httpx

DEFAULT_BASE_URL = "https://pseudogram-api.onrender.com"


def fail(message: str) -> None:
    print(f"[FAIL] {message}")
    sys.exit(1)


def info(message: str) -> None:
    print(f"[INFO] {message}")


def masked(value: str) -> str:
    if not value:
        return "<empty>"
    if len(value) < 12:
        return f"<set, {len(value)} chars>"
    return f"{value[:4]}...{value[-2:]} ({len(value)} chars)"


def check_environment(app_url: str) -> tuple[str, str]:
    api_key = os.getenv("PSEUDOGRAM_API_KEY", "").strip()
    base_url = os.getenv("PSEUDOGRAM_BASE_URL", DEFAULT_BASE_URL).rstrip("/")

    if not api_key:
        fail(
            "PSEUDOGRAM_API_KEY is not set. Get one with:\n"
            "  python scripts/keygen.py --email you@example.com\n"
            "then export it (PowerShell: $env:PSEUDOGRAM_API_KEY='...')."
        )
    info(f"PSEUDOGRAM_API_KEY = {masked(api_key)}")
    info(f"PSEUDOGRAM_BASE_URL = {base_url}")
    info(f"app URL = {app_url}")

    try:
        health = httpx.get(f"{app_url}/health", timeout=30)
    except httpx.HTTPError as exc:
        fail(f"cannot reach {app_url}/health: {exc}")
    if health.status_code != 200:
        fail(f"{app_url}/health returned {health.status_code}")
    info("app health check: ok")
    return api_key, base_url


def create_rule(app_url: str, keyword: str, message: str) -> None:
    response = httpx.post(
        f"{app_url}/rules",
        json={"keyword": keyword, "dm_message": message},
        timeout=30,
    )
    if response.status_code != 201:
        fail(f"POST /rules returned {response.status_code}: {response.text}")
    info(f"rule created: {response.json()}")


def get_stats(app_url: str) -> dict:
    response = httpx.get(f"{app_url}/stats", timeout=30)
    response.raise_for_status()
    return response.json()


def start_simulation(
    base_url: str, api_key: str, webhook_url: str, count: int, duration: int
) -> str:
    response = httpx.post(
        f"{base_url}/v1/simulate/start",
        json={
            "webhook_url": webhook_url,
            "count": count,
            "duration_seconds": duration,
        },
        headers={"X-API-Key": api_key},
        timeout=60,
    )
    if response.status_code >= 400:
        fail(f"simulate/start returned {response.status_code}: {response.text}")
    body = response.json()
    run_id = body.get("run_id")
    if not run_id:
        fail(f"simulate/start did not return a run_id: {body}")
    info(f"simulation started run_id={run_id}")
    return run_id


def get_truth(base_url: str, api_key: str, run_id: str) -> dict:
    response = httpx.get(
        f"{base_url}/v1/simulate/{run_id}/truth",
        headers={"X-API-Key": api_key},
        timeout=60,
    )
    if response.status_code >= 400:
        fail(f"truth endpoint returned {response.status_code}: {response.text}")
    return response.json()


def compare(stats: dict, truth: dict) -> bool:
    """Compare every key the truth payload shares with /stats."""
    print("\n--- comparison ------------------------------------------------")
    print("truth payload:")
    print(json.dumps(truth, indent=2, sort_keys=True))
    print("\n/stats payload:")
    print(json.dumps(stats, indent=2, sort_keys=True))

    expected = truth.get("expected")
    if isinstance(expected, dict):
        flat_truth: dict[str, object] = dict(expected)
    else:
        flat_truth = {k: v for k, v in truth.items() if not isinstance(v, dict)}
        for value in truth.values():
            if isinstance(value, dict):
                flat_truth.update(value)

    shared = [key for key in stats if key in flat_truth]
    if not shared:
        print(
            "\n[WARN] the truth payload has no keys in common with /stats; "
            "compare the fields above manually."
        )
        return True

    ok = True
    print(f"\n{'field':22} {'ours':>10} {'truth':>10}  result")
    for key in shared:
        ours, theirs = stats[key], flat_truth[key]
        match = ours == theirs
        ok = ok and match
        print(f"{key:22} {ours:>10} {theirs!s:>10}  {'MATCH' if match else 'MISMATCH'}")

    alternative = truth.get("expected_counting_redeliveries")
    if isinstance(alternative, dict):
        print(f"\nalternative duplicates_blocked reading: {alternative}")
    return ok


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--app-url", required=True, help="public base URL of this app")
    parser.add_argument("--keyword", default="PRICE")
    parser.add_argument("--message", default="Here's the price list: https://example.com")
    parser.add_argument("--count", type=int, default=500)
    parser.add_argument("--duration", type=int, default=10)
    parser.add_argument(
        "--wait",
        type=int,
        default=180,
        help=(
            "seconds to wait before comparing. PseudoGram allows only 10 sends "
            "per minute, so N unique recipients need roughly N*6 seconds."
        ),
    )
    parser.add_argument("--poll-interval", type=int, default=15)
    parser.add_argument("--skip-rule", action="store_true")
    args = parser.parse_args()

    app_url = args.app_url.rstrip("/")
    api_key, base_url = check_environment(app_url)

    if not args.skip_rule:
        create_rule(app_url, args.keyword, args.message)

    before = get_stats(app_url)
    info(f"stats before: {before}")

    run_id = start_simulation(
        base_url, api_key, f"{app_url}/webhook", args.count, args.duration
    )

    deadline = time.time() + args.wait
    while time.time() < deadline:
        time.sleep(min(args.poll_interval, max(1, deadline - time.time())))
        stats = get_stats(app_url)
        remaining = int(deadline - time.time())
        print(f"[{remaining:>4}s left] {stats}")
        if stats["queued"] == 0 and stats["sent"] + stats["failed"] > 0:
            info("no queued work left")
            break

    stats = get_stats(app_url)
    truth = get_truth(base_url, api_key, run_id)
    matched = compare(stats, truth)

    if stats["queued"]:
        print(
            f"\n[WARN] {stats['queued']} job(s) still queued - the 10 sends/minute "
            "limit means large runs need more time. Re-run with a longer --wait "
            "or just poll /stats again later."
        )
    print("\nRESULT:", "all shared fields match" if matched else "MISMATCH - see above")
    return 0 if matched else 1


if __name__ == "__main__":
    raise SystemExit(main())
