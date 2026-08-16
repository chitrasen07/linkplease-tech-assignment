#!/usr/bin/env python
"""Request a PseudoGram API key.

    python scripts/keygen.py --email you@example.com

Prints the key once. Put it in .env (which is git-ignored) or export it; it is
never written to the repository by this script.
"""

from __future__ import annotations

import argparse
import os
import sys

import httpx

DEFAULT_BASE_URL = "https://pseudogram-api.onrender.com"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--email", required=True, help="the email you applied with")
    parser.add_argument(
        "--base-url", default=os.getenv("PSEUDOGRAM_BASE_URL", DEFAULT_BASE_URL)
    )
    args = parser.parse_args()

    response = httpx.post(
        f"{args.base_url.rstrip('/')}/v1/keygen",
        json={"email": args.email},
        timeout=60,
    )
    if response.status_code >= 400:
        print(f"[FAIL] {response.status_code}: {response.text}", file=sys.stderr)
        return 1

    body = response.json()
    api_key = body.get("api_key")
    if not api_key:
        print(f"[FAIL] unexpected response: {body}", file=sys.stderr)
        return 1

    print("API key received. Store it in .env (never commit it):\n")
    print(f"PSEUDOGRAM_API_KEY={api_key}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
