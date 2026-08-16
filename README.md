# LinkPlease Tech Intern Assignment

A miniature version of LinkPlease: comment webhooks come in, matching rules turn
them into DM jobs, and a background worker delivers those DMs through the
deliberately unreliable PseudoGram API without ever losing, duplicating or
miscounting one.

**Stack:** Python 3.12+ · FastAPI · SQLAlchemy 2.x · SQLite (PostgreSQL-ready) ·
httpx · pytest

**Parts completed:** A (webhook + rules + matching), B (persistent queue,
retries, rate limiting, duplicate protection), C (delivery reconciliation:
`202 != delivered`). All three are implemented and covered by the test suite;
see [Test results](#test-results).

---

## Table of contents

1. [What it does](#what-it-does)
2. [Architecture](#architecture)
3. [Why FastAPI](#why-fastapi)
4. [Database design](#database-design)
5. [Duplicate event handling](#duplicate-event-handling)
6. [Duplicate DM protection](#duplicate-dm-protection)
7. [What `duplicates_blocked` counts](#what-duplicates_blocked-counts)
8. [Idempotency-Key strategy](#idempotency-key-strategy)
9. [Retry strategy](#retry-strategy)
10. [Rate limiting](#rate-limiting)
11. [Delivery reconciliation](#delivery-reconciliation)
12. [Webhook signature verification](#webhook-signature-verification)
13. [`comment.deleted` handling](#commentdeleted-handling)
14. [Out-of-order events](#out-of-order-events)
15. [API endpoints](#api-endpoints)
16. [Local setup](#local-setup)
17. [Environment variables](#environment-variables)
18. [Running the application](#running-the-application)
19. [Running the tests](#running-the-tests)
20. [Manual test commands](#manual-test-commands)
21. [Applying for a PseudoGram API key](#applying-for-a-pseudogram-api-key)
22. [Getting a PseudoGram API key](#getting-a-pseudogram-api-key)
23. [Running the 500-event simulation](#running-the-500-event-simulation)
24. [Deployment](#deployment)
25. [Known limitations](#known-limitations)

---

## What it does

A creator writes a rule: *"when a comment contains PRICE, DM the commenter this
message."* PseudoGram then posts comment webhooks to `/webhook`. For each new
comment the application:

1. verifies the HMAC signature over the raw body,
2. stores the event (unique on `event_id`, so redeliveries are free),
3. matches the text against every active rule (case-insensitive substring),
4. creates one DM job per matching rule - at most one per `(rule, user)` ever,
5. sends the DM through a rate-limited worker with retries and a stable
   idempotency key,
6. polls the delivery status until PseudoGram confirms `delivered`,
7. reports honest numbers on `/stats`.

## Architecture

```
                        ┌──────────── FastAPI process ─────────────┐
 PseudoGram             │                                          │
   webhook  ── POST ───►│  /webhook   verify HMAC → INSERT event   │
                        │                 │  (returns 200 in ~4 ms)│
                        │                 ▼                        │
                        │           events table                   │
                        │                 │                        │
                        │        ┌────────▼────────┐               │
                        │        │  event worker   │ match rules   │
                        │        └────────┬────────┘               │
                        │                 ▼                        │
                        │           dm_jobs table  ◄── UNIQUE(rule_id, user_id)
                        │                 │                        │
                        │        ┌────────▼────────┐  10 sends /   │
   POST /v1/dm/send ◄───┼────────│    DM worker    │  60 s limiter │
                        │        └────────┬────────┘               │
                        │                 ▼ 202 + dm_id            │
                        │        ┌─────────────────┐               │
   GET /v1/dm/{id} ◄────┼────────│  reconciliation │ every 5 s     │
                        │        └────────┬────────┘  (free calls) │
                        │                 ▼                        │
                        │      delivered / failed / retry          │
                        │                                          │
                        │  /stats  ← counted from dm_jobs + counters│
                        └──────────────────────────────────────────┘
```

Four background loops run inside the FastAPI process, started by the lifespan
handler and cancelled cleanly on shutdown:

| Loop | File | Job |
|---|---|---|
| event worker | `app/workers/event_worker.py` | turn stored events into DM jobs |
| DM worker | `app/workers/dm_worker.py` | claim one job, rate-limit, send |
| reconciliation | `app/services/reconciliation.py` | poll `GET /v1/dm/{dm_id}` |
| maintenance | `app/workers/manager.py` | requeue jobs abandoned mid-send |

Every loop reads and writes the database; nothing important lives only in
memory. Restarting the process loses no work.

## Why FastAPI

* The webhook endpoint has a hard latency budget, and ASGI lets the request
  handler do nothing but validate and insert while long-running work happens in
  `asyncio` tasks in the same process - no extra broker to deploy or explain.
* Pydantic gives request validation and the `/docs` + `/redoc` pages for free,
  which the assignment asks for.
* `httpx.AsyncClient` shares the event loop with the workers, so the send loop
  and the reconciliation loop interleave without threads.

Database access is deliberately *synchronous* SQLAlchemy: SQLite is a local
file, so async drivers buy nothing. Endpoints are plain `def` (FastAPI runs them
in a thread pool) and worker coroutines wrap DB calls in `asyncio.to_thread`.

## Database design

`app/models.py`. Timestamps are naive UTC everywhere (SQLite drops timezones, so
normalising on the way in avoids aware/naive comparison bugs).

**rules** — `id` (uuid hex), `keyword`, `keyword_normalized` (lower-cased copy
used for matching), `dm_message`, `is_active`, `created_at`.

**events** — `id`, `event_id` **UNIQUE**, `event_type`, `comment_id`, `post_id`,
`user_id`, `username`, `text`, `sent_at`, `received_at`, `processed_at`,
`status` (`received` → `processing` → `processed`/`ignored`).

**dm_jobs** — `id`, `rule_id`, `user_id`, `comment_id`, `message`, `status`,
`attempts`, `transient_failures`, `send_cycle`, `dm_id`, `next_retry_at`,
`last_error`, `last_status_check_at`, `created_at`, `updated_at`, with
**UNIQUE(rule_id, user_id) WHERE status != 'cancelled'**.

Job status machine:

```
queued ──claim──► sending ──202──► accepted ──delivered──► delivered   (sent)
   ▲                 │                  │
   │                 ├─500/timeout──►  retry_wait ──(next_retry_at)──► queued
   │                 ├─400──────────►  failed                          (failed)
   │                 └─429──────────►  retry_wait (Retry-After)
   └── comment.deleted ─────────────►  cancelled  (counted nowhere)
```

**deleted_comments** — comment ids seen in `comment.deleted`, so a late
`comment.created` for the same comment never creates work.

**counters** — persistent `duplicates_blocked` (and diagnostics), incremented
with a single atomic `UPDATE`.

**send_attempt_log** — one row per outbound send, so the rate-limit window
survives a restart.

Two supporting models beyond the required three exist because both hold state
that must outlive the process: a blocked duplicate never becomes a row you could
count later, and neither does last minute's API traffic.

### Moving to PostgreSQL

Only `DATABASE_URL` changes (`postgresql+psycopg://...`) plus installing
`psycopg`. No SQLite-specific SQL is used; the unique constraints, savepoints
and conditional `UPDATE` claims all behave the same, and PostgreSQL additionally
allows several application instances to share one queue.

## Duplicate event handling

`event_id` carries a UNIQUE constraint. Ingestion inserts first and interprets
the failure afterwards:

```python
try:
    with session.begin_nested():      # SAVEPOINT
        session.add(event)
    session.flush()
except IntegrityError:
    ...                                # duplicate delivery, still answer 200
```

A Python `set` of seen ids was rejected on purpose: it disappears on restart and
two concurrent requests can both miss it. Redelivery is normal for
at-least-once webhooks, so a duplicate is answered `200 {"duplicate": true}`,
never an error.

## Duplicate DM protection

The rule *"the same user never gets DMed twice for the same rule"* is enforced
by a unique index on `dm_jobs (rule_id, user_id)`, and job creation is an insert
whose `IntegrityError` is the duplicate signal (`app/services/jobs.py`). Check-
then-insert would let two concurrent webhooks both see "no existing job" and
both insert; the constraint is the final arbiter, and a test drives eight
threads at one `(rule, user)` pair to prove exactly one job survives. Raw SQL
that bypasses the ORM is rejected too - the guarantee lives in the schema.

The index is **partial**: `WHERE status != 'cancelled'`. A job cancelled because
its comment was deleted never sent anything, so keeping it in the index would
bar that user from ever triggering the rule again, even with a later, perfectly
valid comment. Every job that was sent or can still be sent holds its slot, so
nobody is DMed twice. SQLite and PostgreSQL both enforce partial unique indexes.

## What `duplicates_blocked` counts

**Default: a blocked DM-job insertion, and nothing else.** For every *logical*
`comment.created` event that matches a rule, either a job is created or
`duplicates_blocked` increments once per matching rule:

> `dms_created + duplicates_blocked == unique matching events`

This is the reading the assignment supports most directly: it says to increment
the counter "when a duplicate job insertion is blocked", and its section on
duplicate `event_id`s says only to return 200 and create no job - the counter is
never mentioned there. A redelivered webhook is the same logical event arriving
twice over an at-least-once transport, not a second decision about a DM.

The alternative reading - count every matching *delivery* that produces no job,
including redeliveries - is one environment variable away:
`COUNT_DUPLICATE_EVENTS_AS_BLOCKED=true`, giving
`dms_created + duplicates_blocked == matching deliveries`.

`tests/test_duplicate_semantics.py` pins down all five scenarios under both
modes; only an identical redelivered `event_id` differs between them:

| Scenario | DMs | `duplicates_blocked` (default) | (`=true`) |
|---|---|---|---|
| A/E same `event_id` delivered twice | 1 | **0** | **1** |
| B different events, same user + rule | 1 | 1 | 1 |
| C same user, two different rules | 2 | 0 | 0 |
| D different users, same rule | 2 | 0 | 0 |

## Idempotency-Key strategy

Every send carries `Idempotency-Key: dm-job-{job_id}`. The key is a pure
function of the job, so **transport-level retries reuse it exactly**: if a
request reached PseudoGram but the response was lost to a timeout, the retry
cannot produce a second DM.

The one case where the key advances is a *confirmed delivery failure*: when
`GET /v1/dm/{dm_id}` returns `failed`, we intentionally want a new DM, so
`send_cycle` increments and the key becomes `dm-job-{job_id}-r1`, `-r2`, ...
Reusing the original key there would just replay the same already-failed
`dm_id` forever. Within each cycle the key is still stable.

## Retry strategy

Two different things can go wrong, and they get **two separate budgets**:

* **Transient failures** — `500`, timeouts, connection errors. PseudoGram never
  accepted anything, so no DM was ever attempted. Counted in
  `transient_failures`, bounded by `MAX_DM_ATTEMPTS` (5), and **reset to zero on
  a `202`**, because acceptance is progress.
* **Delivery failures** — `GET /v1/dm/{dm_id}` reported `failed`. A real DM was
  attempted and really failed. Counted in `send_cycle`, bounded by
  `MAX_DELIVERY_ATTEMPTS` (5).

Sharing one budget was a genuine bug: a job that saw three `500`s and then one
failed delivery was abandoned after a *single* real delivery attempt. See
[Retry budgets](#why-two-budgets) below.

| Response | Budget consumed | Action |
|---|---|---|
| `202` | none (resets transient) | `accepted`, wait for reconciliation |
| `500` / other 5xx | transient | `retry_wait`, exponential backoff |
| timeout / connection error | transient | `retry_wait`, backoff, same idempotency key |
| `429` | **none** | `retry_wait` for `Retry-After`, worker pauses too |
| `400` | – | `failed` immediately, `last_error` stored, never retried |
| `401` / `403` | **none** | `retry_wait`, worker pauses; a bad key must not fail every job |
| delivery `failed` | delivery | new send cycle, backoff, retry |
| either budget exhausted | – | `failed`, counted once |

Backoff is `RETRY_BASE_SECONDS * 2^(n-1)` with ±25% jitter, capped at
`RETRY_MAX_BACKOFF_SECONDS`: roughly 1 s, 2 s, 4 s, 8 s, 16 s. A `429` consumes
nothing because being throttled is our scheduling problem, not a defect in the
job. `attempts` still records the total number of send requests, and the worst
case per job is bounded at `MAX_DM_ATTEMPTS × MAX_DELIVERY_ATTEMPTS` sends.

### Why two budgets

A DM job's whole purpose is to get one message delivered. Retrying a `500`
costs nothing but a slot, and it tells you nothing about whether the message is
deliverable; a `failed` delivery status is the only evidence that the attempt
itself did not work. Spending the same counter on both means an unlucky burst of
server errors silently shortens the retries that actually matter - which is how
a deliverable DM ended up in `failed` and made `/stats.sent` disagree with the
truth by one. Each budget now bounds the failure mode it is about, and both are
still bounded, so `failed` keeps its meaning: *we gave up after retries*.

## Rate limiting

`POST /v1/dm/send` is limited to 10 requests per rolling 60 seconds, so:

* exactly one DM worker sends at a time (no fan-out of 500 tasks),
* it must `acquire()` a slot from `RollingWindowRateLimiter` before every send,
* the limiter keeps the timestamps of granted sends and, when the window is
  full, sleeps until the oldest one ages out (plus a small safety margin),
* every grant is also written to `send_attempt_log`, and `warm_start()` reloads
  the current window after a restart - otherwise a redeploy would immediately
  burst 10 more sends on top of the 10 just made,
* a `429` is still handled defensively: the job waits for `Retry-After` and the
  worker itself pauses for the same duration.

`GET /v1/dm/{dm_id}` is free and never goes through the limiter.

**Consequence worth stating plainly:** 500 events with *N* distinct recipients
need about *N × 6* seconds of sending. That is the API's limit, not a bug, and
`/stats` will honestly report the rest as `queued` while the queue drains.

## Delivery reconciliation

`202` means accepted. Nothing is counted as `sent` until the reconciliation loop
(every `DELIVERY_POLL_INTERVAL_SECONDS`, default 5 s) sees `delivered`:

* `delivered` → job `delivered`, `sent += 1`,
* `failed` → attempt counted, new send cycle scheduled with backoff, or `failed`
  if attempts are exhausted,
* `queued` → left pending, checked again next cycle,
* status call errors → left pending; a temporarily unreadable status never
  loses a job.

## Webhook signature verification

```
X-PseudoGram-Signature: sha256=<hex>
hex = HMAC-SHA256(raw_request_body, PSEUDOGRAM_API_KEY)
```

The handler reads `await request.body()` and hashes those exact bytes - the JSON
is parsed only afterwards, because re-serialising it would change the digest.
Comparison uses `hmac.compare_digest`. Missing or invalid signature → `401`.

Controlled by `VERIFY_WEBHOOK_SIGNATURE` (default `true`). For local testing
with hand-written `curl` payloads set it to `false`, or use
`scripts/send_test_webhook.py`, which signs what it sends.

> If a real simulation run shows zero events arriving, check the logs for
> `rejected webhook: missing signature`. That means the sender does not sign
> requests, and `VERIFY_WEBHOOK_SIGNATURE=false` is the correct setting for that
> run.

## `comment.deleted` handling

* Jobs for that `comment_id` in `queued` or `retry_wait` → `cancelled`
  (counted in no statistic).
* Jobs already `accepted` (handed to PseudoGram) or `delivered` are left alone -
  a DM cannot be un-sent.
* The `comment_id` is recorded in `deleted_comments`, so a `comment.created` for
  the same comment arriving *later* creates no job at all.
* Only jobs from that exact `comment_id` are touched.

* A cancelled job **releases** its `(rule_id, user_id)` slot, so a later valid
  comment from that user can still trigger the rule.

That last point is a deliberate reading of "the same user never gets DMed twice
for the same rule". A cancelled job DMed nobody, so allowing a fresh one cannot
produce a second DM - the user still receives at most one. Permanently burning
the slot would instead mean a deleted comment silently disqualifies a user
forever, which punishes them for editing their comment. The carve-out is
narrow: it applies only to `cancelled`, and it is enforced in the schema rather
than in Python (see [Duplicate DM protection](#duplicate-dm-protection)).

## Out-of-order events

Nothing depends on arrival order or on `sent_at`. Events are stored as they
arrive, an unparseable `sent_at` is stored as `NULL` rather than rejected, and
older timestamps are never dropped. The one ordering case that matters -
deletion before creation - is handled by the `deleted_comments` table.

## API endpoints

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/webhook` | receive comment events (200 fast, HMAC verified) |
| `POST` | `/rules` | create a keyword rule (201) |
| `GET` | `/rules` | list active rules (convenience) |
| `GET` | `/stats` | `sent` / `failed` / `queued` / `duplicates_blocked` |
| `GET` | `/health` | `{"status": "ok"}` |
| `GET` | `/docs`, `/redoc`, `/openapi.json` | generated API documentation |

`sent` = jobs confirmed `delivered`. `failed` = jobs that gave up after retries.
`queued` = jobs in `queued`, `sending`, `accepted` or `retry_wait`.
`duplicates_blocked` = as described [above](#what-duplicates_blocked-counts).
Cancelled jobs appear in none of them, and every other job appears in exactly
one.

## Local setup

```bash
git clone https://github.com/<your-username>/linkplease-tech-assignment.git
cd linkplease-tech-assignment

python -m venv .venv
source .venv/bin/activate          # Windows: .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

cp .env.example .env               # Windows: copy .env.example .env
# put your PseudoGram key in .env
```

## Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `PSEUDOGRAM_BASE_URL` | `https://pseudogram-api.onrender.com` | API base URL |
| `PSEUDOGRAM_API_KEY` | *(empty)* | **required to send DMs**; also the HMAC secret |
| `PSEUDOGRAM_TIMEOUT_SECONDS` | `15` | HTTP timeout |
| `DATABASE_URL` | `sqlite:///./linkplease.db` | SQLAlchemy URL |
| `VERIFY_WEBHOOK_SIGNATURE` | `true` | reject unsigned/invalid webhooks |
| `WEBHOOK_SIGNING_SECRET` | *(empty)* | override the HMAC secret |
| `MAX_WEBHOOK_BODY_BYTES` | `65536` | body size limit (413 above it) |
| `MAX_DM_ATTEMPTS` | `5` | consecutive transient send failures before giving up |
| `MAX_DELIVERY_ATTEMPTS` | `5` | confirmed delivery failures before giving up |
| `RETRY_BASE_SECONDS` | `1.0` | first backoff step |
| `RETRY_JITTER_RATIO` | `0.25` | ± jitter applied to backoff |
| `RETRY_MAX_BACKOFF_SECONDS` | `300` | backoff cap |
| `AUTH_ERROR_PAUSE_SECONDS` | `60` | pause after PseudoGram rejects the key |
| `ENABLE_WORKERS` | `true` | run background loops (tests set `false`) |
| `WORKER_POLL_INTERVAL_SECONDS` | `1` | idle poll for DM jobs |
| `EVENT_POLL_INTERVAL_SECONDS` | `0.2` | idle poll for new events |
| `DELIVERY_POLL_INTERVAL_SECONDS` | `5` | reconciliation interval |
| `SENDING_STALE_SECONDS` | `120` | requeue jobs stuck in `sending` |
| `SEND_RATE_LIMIT_MAX_CALLS` | `10` | sends per window |
| `SEND_RATE_LIMIT_WINDOW_SECONDS` | `60` | window length |
| `SEND_RATE_LIMIT_SAFETY_SECONDS` | `0.5` | margin against clock skew |
| `COUNT_DUPLICATE_EVENTS_AS_BLOCKED` | `false` | see `duplicates_blocked` |
| `LOG_LEVEL` | `INFO` | logging level |

`.env` is git-ignored; only `.env.example` (with empty values) is committed.

## Running the application

```bash
uvicorn app.main:app --reload --port 8000          # development
uvicorn app.main:app --host 0.0.0.0 --port $PORT   # production
docker compose up --build                          # container + persistent volume
```

Then open <http://localhost:8000/docs>.

## Running the tests

```bash
pytest -q
```

Tests never touch the network: `tests/fakes.py` provides a scripted PseudoGram
client that can return 202/400/429/500/timeouts and `queued`/`delivered`/
`failed` statuses on demand, and each test gets its own temporary SQLite file.

### Test results

```
$ python -m pytest -q
141 passed in 5.03s
```

Covered, among others: rule creation and validation, case-insensitive substring
matching, `user_id` (not username) as identity, duplicate `event_id`, eight
concurrent inserts for one `(rule, user)`, different users, one job per rule,
500 → retry, 400 → no retry, 429 → honours `Retry-After`, 202 ≠ sent, delivered
→ `sent`, delivery failure → retry, exhausted attempts → `failed`, `queued`
arithmetic, valid/invalid/missing signatures, raw-body hashing,
`comment.deleted` cancelling only queued work, delivered DMs never cancelled,
idempotency key stability, and "never more than 10 sends in any 60-second
window" checked over 35 grants of virtual time.

Three files exist specifically because of the pre-submission audit:

* `tests/test_regression.py` - the `sent=11 vs truth=12` bug: transient errors
  must not consume the delivery retry budget, and both budgets must still stop.
* `tests/test_duplicate_semantics.py` - scenarios A-E under both counting modes.
* `tests/test_recovery.py` - the process dies before sending, mid-send, after a
  `202` but before the commit, during backoff, and during reconciliation; plus
  the rate limiter refusing to burst after a restart.

## Manual test commands

```bash
# health
curl http://localhost:8000/health

# create a rule
curl -X POST http://localhost:8000/rules \
  -H "Content-Type: application/json" \
  -d '{"keyword":"PRICE","dm_message":"Here is the price list"}'

# stats
curl http://localhost:8000/stats

# a webhook (works when VERIFY_WEBHOOK_SIGNATURE=false)
curl -X POST http://localhost:8000/webhook \
  -H "Content-Type: application/json" \
  -d '{"event_id":"evt_01J8ZQ4K2N7RXA","event_type":"comment.created",
       "sent_at":"2026-08-10T09:14:22.481Z",
       "data":{"comment_id":"cmt_9f2a7c","post_id":"post_44de1b",
               "text":"PRICE please","created_at":"2026-08-10T09:14:21.900Z",
               "from":{"user_id":"usr_3b91fe","username":"arjun.shoots"}}}'

# send the same event again - answered 200, no second DM
# (repeat the command above verbatim, then check duplicates_blocked)
```

With signature verification on, use the helper which signs the exact bytes it
sends:

```bash
python scripts/send_test_webhook.py --url http://localhost:8000 --text "PRICE?"
python scripts/send_test_webhook.py --url http://localhost:8000 --event-id evt_fixed --repeat 3
```

### Exercising the full pipeline without an API key

`scripts/mock_pseudogram.py` is a local mock of PseudoGram (rate limit, random
500s, 202-then-maybe-fail delivery, idempotency replay). It is a development
aid only - it says nothing about the real service:

```bash
python -m uvicorn scripts.mock_pseudogram:app --port 9000
PSEUDOGRAM_BASE_URL=http://127.0.0.1:9000 PSEUDOGRAM_API_KEY=local-dev-key \
  uvicorn app.main:app --port 8000
curl http://127.0.0.1:9000/mock/report   # includes any rate-limit violations
```

## Applying for a PseudoGram API key

Do this yourself; the application never calls the apply endpoint.

```bash
curl -X POST https://pseudogram-api.onrender.com/v1/apply \
  -H "Content-Type: application/json" \
  -d '{"name":"Your Name","email":"you@example.com","phone":"+91...",
       "whatsapp":"+91...","linkedin_url":"https://linkedin.com/in/you"}'
```

## Getting a PseudoGram API key

```bash
python scripts/keygen.py --email you@example.com
# or
curl -X POST https://pseudogram-api.onrender.com/v1/keygen \
  -H "Content-Type: application/json" -d '{"email":"you@example.com"}'
```

Put the key in `.env` (git-ignored) or in your host's environment settings.
Never commit it; the same key is also the HMAC secret for webhook signatures.

## Running the 500-event simulation

Deploy first - PseudoGram must be able to reach your `/webhook` publicly.

```bash
export PSEUDOGRAM_API_KEY=...        # PowerShell: $env:PSEUDOGRAM_API_KEY="..."
python scripts/test_pseudogram.py --app-url https://<your-app>.onrender.com --wait 600
```

The script checks the environment, creates the rule, starts the run, polls
`/stats` while the queue drains and prints our numbers next to the truth
payload, flagging mismatches. Raw equivalents:

```bash
curl -X POST https://pseudogram-api.onrender.com/v1/simulate/start \
  -H "X-API-Key: $PSEUDOGRAM_API_KEY" -H "Content-Type: application/json" \
  -d '{"webhook_url":"https://<your-app>.onrender.com/webhook","count":500,"duration_seconds":10}'

curl -H "X-API-Key: $PSEUDOGRAM_API_KEY" \
  https://pseudogram-api.onrender.com/v1/simulate/<run_id>/truth
curl https://<your-app>.onrender.com/stats
```

Give it time: at 10 sends per minute, *N* distinct recipients take about
*N × 6* seconds. Comparing `/stats` before the queue drains will legitimately
show a large `queued` number.

### What local dry runs look like

Against `scripts/mock_pseudogram.py` (**a local mock, not the real API**): 500
events in 10 seconds, 20 distinct users, ~25% redelivery, 20% `500`s and 15%
delivery failures. Three independent runs, each with a fresh database and a
fresh process:

| Run | delivered/rejected | app `sent/failed/queued/dupes` | truth | rate-limit violations |
|---|---|---|---|---|
| 1 | 500 / 0 | 20 / 0 / 0 / 275 | 20 / 0 / 0 / 275 | 0 |
| 2 | 500 / 0 | 20 / 0 / 0 / 269 | 20 / 0 / 0 / 269 | 0 |
| 3 | 500 / 0 | 20 / 0 / 0 / 283 | 20 / 0 / 0 / 283 | 0 |

Every field matched on every run. The mock computes its truth independently
from the events it generated, and reports both `duplicates_blocked` readings
(the alternative, delivery-based number was 391/375/367) so the choice of
semantics is visible rather than assumed.

An earlier run of this same setup produced `sent=11` against a truth of `12`.
That was a real defect - transient `500`s were consuming the retry budget owed
to failed deliveries - and it is fixed and pinned by `tests/test_regression.py`.
Numbers from the real PseudoGram API will differ; run the script yourself after
deploying.

## Deployment

### Render (Docker)

1. Push the repository to GitHub.
2. Render → **New** → **Web Service** → connect the repo, runtime **Docker**.
3. Environment variables: `PSEUDOGRAM_API_KEY`, `VERIFY_WEBHOOK_SIGNATURE=true`,
   `DATABASE_URL=sqlite:////data/linkplease.db`.
4. **Add a persistent disk** mounted at `/data` (1 GB is plenty).
5. Keep the instance count at **1** (see below).
6. Deploy, then check `https://<service>.onrender.com/health`.

The container binds `0.0.0.0` and uses `$PORT` when the platform provides it.

**SQLite on an ephemeral filesystem:** without a mounted disk, Render's
filesystem is wiped on every deploy and on some restarts - queued jobs, stored
`event_id`s and the `duplicates_blocked` counter would all disappear, and
redelivered events would be processed again. Either mount a disk or use
PostgreSQL:

```
DATABASE_URL=postgresql+psycopg://user:password@host:5432/linkplease
```

(add `psycopg[binary]` to `requirements.txt`; the schema is created on startup).

### Any other host

```bash
docker build -t linkplease .
docker run -p 8000:8000 --env-file .env -v linkplease-data:/data linkplease
```

## Known limitations

Full detail, with the reasoning behind each, is in [FAILURES.md](FAILURES.md).
The short list:

* **Single instance only.** SQLite plus the startup recovery step assume one
  process owns the queue. Scale-out needs PostgreSQL first.
* **Throughput is capped by PseudoGram** at 10 DMs/minute; large bursts sit in
  `queued` for a long time, by design.
* **A cancelled job keeps its `(rule, user)` slot,** so a user whose comment was
  deleted will not be DMed if they comment again.
* **`duplicates_blocked` is a judgement call** where the spec is ambiguous; the
  chosen definition is documented above and switchable.
* **Rules are matched at processing time,** so a rule created after an event was
  processed does not apply retroactively.
* **No authentication on `/rules`** - it is open, as the assignment's contract
  describes it.
