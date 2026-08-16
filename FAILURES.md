# FAILURES.md

Honest failure modes of *this* implementation. Everything below is derived from
the code in `app/`, not from a generic list, and each entry says what actually
happens, what the damage is, and what would fix it.

This system does **not** handle all edge cases. The ones it does handle are
described in the README; the ones below are real gaps or accepted trade-offs.

---

## 1. The database is unavailable while a webhook is being processed

**What happens.** `POST /webhook` wraps the insert in a `try`; on any exception
it logs and returns `503`, so the event is *not* acknowledged. PseudoGram is
free to redeliver, and a redelivery is something we already handle.

**Damage.** If the sender does not retry (see #5), that comment is lost
permanently - no DM, no record, and our `/stats` will legitimately disagree with
a truth that counted it.

**Why it is built this way.** Returning `200` for an event we failed to store
would be worse: it converts a retryable error into silent data loss.

**Would fix it.** A durable write-ahead buffer in front of the database, or a
managed PostgreSQL instance with connection retry.

---

## 2. The process crashes between claiming a job and getting a send response

**What happens.** The job row sits in `sending`. Three mechanisms recover it:
`WorkerManager.recover_state()` requeues every `sending` job at startup, the
maintenance loop requeues jobs stuck in `sending` for more than
`SENDING_STALE_SECONDS`, and a cancelled/failed send releases its own claim.

**Damage.** If PseudoGram had already accepted the DM before the crash, we
re-send it. `Idempotency-Key: dm-job-{job_id}` is unchanged across that retry, so
PseudoGram should return the original `dm_id` rather than sending twice - *if*
its idempotency window is still open. If the crash-to-retry gap exceeds that
window (unknown to us, plausibly minutes), the recipient can receive the DM
twice.

**Would fix it.** Recording the outbound request before sending it and querying
by idempotency key on recovery instead of blind re-sending.

---

## 3. The process restarts while events are being matched

**What happens.** Events claimed by the event worker are in `processing`;
`requeue_stuck_events()` puts them back to `received` at startup, so matching
runs again.

**Damage.** Matching is re-run, and the second run may find that a job already
exists for that `(rule, user)` - which increments `duplicates_blocked`. A crash
at exactly that point therefore *inflates* `duplicates_blocked` by up to one per
event in flight (at most ~100, the batch size, and in practice a handful).

**Would fix it.** Marking the event `processed` and creating its jobs in one
transaction is already done; the residual risk is that the transaction committed
but the process died before the next event. Making duplicate counting idempotent
per `(event_id, rule_id)` - a table instead of a counter - would remove it
entirely at the cost of one row per suppression.

---

## 4. PseudoGram is down for longer than the retry budget

**What happens.** Each attempt gets a `500`/timeout and backs off ~1, 2, 4, 8,
16 s. After `MAX_DM_ATTEMPTS` (default 5) *consecutive* transient failures -
roughly 30 s of continuous outage - the job is marked `failed` with
`last_error`, and `/stats.failed` increases. Independently, `MAX_DELIVERY_
ATTEMPTS` (default 5) confirmed delivery failures also end a job.

**Damage.** A 60-second outage permanently fails DMs that a more patient system
would have delivered. Nothing retries them afterwards; there is no
"resurrect failed jobs" endpoint.

**Note on the counters.** These two budgets used to be one, which was a real
defect: a local 500-event dry run produced a `failed` job that had drawn three
`500`s and then a single failed delivery, so it was abandoned after one real
delivery attempt and `/stats.sent` disagreed with the truth by one. Transient
errors now have their own budget and are reset by a `202`. Three subsequent
500-event runs matched truth exactly, but the underlying trade-off stands: any
finite budget can abandon a DM that a more patient system would have delivered,
and 5 consecutive `500`s at a 20% error rate is roughly a 1-in-3000 event per
job.

**Would fix it.** A circuit breaker that pauses the worker instead of spending
attempts while the API is globally down (which is what we already do for `429`
and `401`, but not for `500`), or a manual replay endpoint.

---

## 5. The webhook sender gives up before we receive the event

**What happens.** Nothing - we never learn the event existed.

**Damage.** Missing DMs and a `/stats` that under-counts against the evaluator's
truth. This is the failure mode of #1 and of any deploy window: while the
service restarts (a Render deploy takes tens of seconds), inbound webhooks get
connection errors.

**Would fix it.** Zero-downtime deploys, and a reconciliation endpoint that
pulls missed events from the source rather than waiting to be pushed.

---

## 6. Two application instances share one SQLite file

**What happens.** SQLite serialises writers with WAL and a 30 s busy timeout, so
correctness of the *constraints* holds. What breaks is the recovery logic:
`recover_state()` at startup requeues every `sending` job, including jobs the
*other* instance is actively sending. That instance then applies its result to a
job another worker may already have re-sent.

**Damage.** Duplicate DMs (mitigated but not guaranteed by the idempotency key),
and doubled send throughput - two workers each honour 10/60 s locally, so the
account can exceed the real limit and start collecting `429`s.

**Would fix it.** PostgreSQL with `SELECT ... FOR UPDATE SKIP LOCKED` for
claiming, an owner/lease column instead of blanket startup recovery, and a
shared (database-backed, already the case) rate-limit window that is *checked*
rather than cached in memory. Until then: run exactly one instance.

---

## 7. The reconciliation loop is down (or the API's status endpoint is broken)

**What happens.** Jobs stay in `accepted` forever. They are counted as `queued`,
never as `sent`.

**Damage.** `/stats.sent` stays at zero while DMs are in fact being delivered -
the numbers under-report reality. Nothing times out an `accepted` job, on
purpose: guessing "it was probably delivered" would fabricate the one number the
evaluator checks most closely.

**Would fix it.** Alerting on the age of the oldest `accepted` job. A cap that
force-fails very old accepted jobs would trade one wrong number for another, so
it was deliberately not implemented.

---

## 8. A network failure right after PseudoGram accepted a send

**What happens.** `httpx` raises, the client returns `TRANSPORT_ERROR`, the job
retries with the same idempotency key.

**Damage.** Bounded by PseudoGram's idempotency behaviour, which we cannot
verify from outside. If it dedupes, we get the original `dm_id` back and
everything reconciles. If it does not, the user receives two DMs and we track
only the second `dm_id`; the first delivery is invisible to us.

**Would fix it.** Nothing on our side alone - this needs a lookup-by-
idempotency-key endpoint on the provider.

---

## 9. `duplicates_blocked` may not match the evaluator's definition

**What happens.** The spec defines it as "DMs the system correctly decided not
to send because the same user had already received/been scheduled for that
rule", which does not say whether a *redelivered webhook* counts. We do **not**
count redeliveries, because the spec increments the counter "when a duplicate
job insertion is blocked" and a redelivery never reaches job insertion. So
`duplicates_blocked` here is `unique_matching_events − dms_created`.

**Damage.** If the evaluator's truth is computed per *delivery* rather than per
logical event, our number is lower than theirs by the number of matching
redeliveries - on a 500-event run with heavy redelivery that was 391 vs 275,
i.e. a large, obvious difference rather than a subtle one.

**Would fix it.** `COUNT_DUPLICATE_EVENTS_AS_BLOCKED=true` switches to the
delivery-based definition without a code change; both modes are covered by
`tests/test_duplicate_semantics.py`, and the local mock reports both numbers so
the two can be compared directly.

---

## 10. A cancelled job releases its `(rule_id, user_id)` slot

**What happens.** `comment.deleted` marks the job `cancelled`, and the unique
index is partial (`WHERE status != 'cancelled'`), so a later valid comment from
that user can create a new job for the same rule.

**Damage.** A user who deletes and reposts a matching comment gets one DM, which
is the intent - but if the evaluator reads "never DM the same user twice for the
same rule" as "never create a second job for the pair, ever", this counts one DM
where their truth counts a blocked duplicate. The narrower alternative was worse:
a deleted comment would silently disqualify that user for the rest of the run.

**Bounded by.** Only `cancelled` is excluded. A job that was sent, is being sent,
or is waiting to be sent still holds its slot, so no user can be DMed twice.

---

## 11. Throughput is far below the inbound event rate

**What happens.** 500 events can arrive in 10 seconds; sends leave at 10 per
minute. With *N* distinct recipients the queue takes about *N × 6* seconds to
drain (500 unique users ≈ 50 minutes).

**Damage.** Anyone comparing `/stats` shortly after a run sees a large `queued`
and a small `sent`. That is accurate, not a bug - but it *will* look like a
mismatch if the comparison is made too early.

**Would fix it.** Nothing, without violating the documented rate limit.

---

## 12. Unbounded growth and no back-pressure

**What happens.** Events, jobs and send-log rows are never pruned (the rate
limiter prunes only its own window at startup). A long-running deployment
accumulates rows, and `/stats` does a full `GROUP BY` over `dm_jobs` on every
request.

**Damage.** Gradual slowdown and disk growth. At assignment scale (thousands of
rows) this is invisible; at production scale it needs retention and indexes on
the stats query.

---

## 13. There are no schema migrations

**What happens.** `init_db()` calls `create_all()`, which creates missing tables
but never alters existing ones. Deploying a version that adds a column (as this
audit did, with `dm_jobs.transient_failures`) on top of a persistent disk that
already holds an older database will fail at query time, not at startup.

**Damage.** A redeploy onto an existing volume breaks until the file is deleted
or patched by hand. It does not affect a first deployment, which is the case
here.

**Would fix it.** Alembic. It was left out deliberately - one migration tool for
a six-table schema that has never shipped is more machinery than the assignment
needs - but it is the first thing to add before a second release.

---

## 14. In-process workers share the web server's fate

**What happens.** The workers run as `asyncio` tasks inside the API process, so
a crash, an OOM kill or a deploy stops both at once. Loop bodies catch and log
exceptions so one bad job cannot kill a worker, but the process itself is a
single point of failure.

**Damage.** During any restart no DMs are sent (they queue safely) and no
webhooks are accepted (those can be lost - see #5).

**Would fix it.** A separate worker process against a shared PostgreSQL queue.
