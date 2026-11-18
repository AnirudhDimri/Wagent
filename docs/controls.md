# Batching, limits, opt-out, and erasure (M4)

M4 controls when the bot runs, how many messages it attempts, and which data it
keeps. Apply the migration before serving:

```sh
uv run alembic upgrade head
```

Use `./ve/bin/uv` instead of `uv` if uv is installed in the earlier ve environment.
The migration preserves existing application rows and reserves daily capacity
for existing accepted/unknown/in-flight outbound messages using their timestamps.

## Try the offline tests

```sh
uv run pytest -q tests/integration/test_controls.py
uv run pytest -q tests/integration/test_erasure.py tests/unit/test_logging.py
```

These use temporary databases and fake Graph API requests, not real WhatsApp
messages. The erasure tests use a real SQLite LangGraph checkpointer.

## Batching

With DEBOUNCE_MS=1500, the worker waits until no new message has arrived for
1.5 seconds before starting a turn. MAX_DEBOUNCE_MS=5000 caps the wait from the
oldest pending message, so continuous messages cannot postpone work indefinitely.
Both decisions use persisted receipt timestamps and survive restarts.

Three rapid messages such as "Hi", "I need an appointment", and "Tomorrow"
become one ordered turn and usually one reply. Ordering still uses WhatsApp's
message timestamp, then receipt timestamp. Already claimed/retrying turns retain
their membership; a newer ordinary message cannot bypass an older retry.
DEBOUNCE_MS=0 disables the idle wait for tests or applications that need it.

## Limits and pause

| Setting | Default | Behavior |
| --- | --- | --- |
| RATE_LIMIT_TURNS_PER_MIN | 6 | Rolling 60-second agent-turn admissions per conversation. |
| DAILY_OUTBOUND_CAP_PER_USER | 50 | Outbound message reservations per conversation per UTC day. |
| PAUSE_ALL | false | Skip inbound bot work and drop queued ordinary replies. |

A conversation is one sender at one business phone number. Rate-limited inbound
batches are stored as skipped. At most one slow-down notice is queued per rolling
hour. Retry attempts of an already admitted agent turn are not new admissions.
A notice is subject to the normal window and daily send cap.

The daily cap counts logical outgoing messages reserved before HTTP. This is a
conservative safety budget, not a delivered-message count or billing estimate:
unknown outcomes and failed attempts can retain a slot. A safe retry of the same
outbound row reuses its slot within that UTC day. Another day's attempt reserves
capacity on that day. Each text chunk consumes one slot; a reply can be partially
sent if the remaining chunks exceed the cap. Set the cap to 0 to block ordinary
outgoing messages. Workers serialize reservations through the conversation row.

Changing PAUSE_ALL in .env takes effect on application restart. It is not an
admin toggle yet. Paused/human conversations skip bot turns; human notifications
and takeover endpoints remain M5. The opt-out check always happens first.

## Opt-out

OPT_OUT_KEYWORDS defaults to `STOP,UNSUBSCRIBE`, a comma-separated list. Matching
uses a whole message, ignoring surrounding whitespace and case. A sentence that
merely mentions "stop" does not opt someone out.

On the first matching batch, the worker atomically:

1. Marks the conversation opted_out and records an opt_out audit.
2. Cancels queued ordinary replies.
3. Saves one free-form unsubscription confirmation with a unique key.
4. Marks the batch done without calling the agent.

The check runs in bot, human, and paused modes, including PAUSE_ALL. Repeat STOP
messages do not create another confirmation. Subsequent ordinary messages are
stored but skipped; no templates are sent to opted-out users. There is no
automatic opt-in keyword in M4.

The single opt-out confirmation is exempt from mode/global pause and the daily
cap, so those controls cannot suppress it. It must still fit the 24-hour window;
it never becomes a template. Delivery remains subject to network errors and the
outbox's conservative ambiguous-send rule. A request already written to Meta
cannot be recalled when STOP arrives.

## Raw payload retention and logging

The retention job runs during application lifespan, including ingestion-only
mode. It clears raw inbound JSON older than RETENTION_DAYS (default 30) on startup
and every RETENTION_INTERVAL_SECONDS (default 3600). Text, media references,
checkpoint memory, and usage are retained; this is not full conversation erasure.

When LOG_SALT is configured, worker logs use a stable HMAC sender label. Without
it, the worker does not log sender identifiers. Keep the salt private and stable
if you want to correlate labels across restarts. Exception messages, payloads,
and tokens are not included in our worker/CLI logs; pywa's request DEBUG bodies
remain suppressed during our template sends. User-supplied agent/tool logging
is the operator's responsibility.

## Erase a conversation

With ADMIN_API_KEY configured, an authorized request can call:

```text
DELETE /admin/conversations/<conversation-uuid>
Authorization: Bearer <admin-key>
```

Success returns 204, missing conversations 404, and unauthorized calls 401. The
route is absent when the admin key is unset. Active worker leases or live sends
return 409: retry after the work finishes rather than deleting an active thread.

The CLI can erase every conversation with a supplied sender ID across business
phone numbers in this database:

```sh
uv run wagent purge --wa-id <sender-id>
```

It reads DATABASE_URL from the environment/.env and needs no Meta credentials.
It prints only the number of erased conversations. A busy or incomplete erasure
returns a nonzero exit code. This command deletes data without another prompt.

Erasure removes inbound/outbound messages, turns, usage, linked status receipts,
audit rows, and the LangGraph thread's checkpoints/writes/blobs. It preserves
other people's conversations and checkpoint schema tables. It retains no
identifying purge audit row after deletion.

Checkpoint erasure and application-row deletion use separate transactions. A
durable erasing marker blocks worker/sender claims and makes inbound requests
retry with 503 until the operation completes. If checkpoint deletion fails,
DELETE returns 503 and the marker remains; retry the same erasure to finish.
Concurrent erasure requests are fenced by a lease. After a process crash or a failure in the final database
transaction, retry once its lease expires. Built-in async runners must honor timeouts/cancellation.

M4 erases the built-in LangGraph checkpoint store selected by DATABASE_URL. It
cannot erase custom remote-agent stores, third-party traces, backups, Meta's own
records, or messages already delivered to a phone. New inbound messages can
create a new conversation after deletion; late Meta status callbacks can create
new unmatched minimal receipts. Operators control webhook access, backup
retention, legal basis for processing, and compliance with Meta's messaging policies.
