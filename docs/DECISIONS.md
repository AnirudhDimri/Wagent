# Decisions

## M1: own the durable ingress transaction

Use pywa's public raw-byte signature utility inside our FastAPI route, with a
strict header check. Do not register pywa's auto-routes or callback dispatcher:
its in-memory deduplication, first-message parsing, and swallowed callback errors
would allow acknowledgment without durable ingestion. Traverse the complete
Cloud API envelope ourselves and commit before 200. Unknown and malformed event
items are ignored independently; invalid JSON returns 400, storage failure 503.
No raw payloads or identifiers are logged.

M1 exposes `Wagent(settings=Settings()).fastapi_app`; the runner parameter arrives
in M2. Only conversations and inbound_messages are migrated in M1. Status events
are acknowledged and ignored until M3 adds the outbox and usage tables.

Run Alembic migrations explicitly before serving. Startup never creates or
changes schema; readiness checks both database access and required tables.
SQLite timestamps are stored as naive UTC and restored as aware UTC through a
SQLAlchemy type, since SQLite does not preserve timezone information itself.
Both settings and migrations read DATABASE_URL from the environment or `.env`.

## M2: worker and outbox

`Wagent(settings=..., runner=...)` enables background processing during ASGI
lifespan. Omitting runner retains an ingestion-only gateway. Custom runners only
need `run(ctx)`; the built-in LangGraphRunner additionally opens/closes its
checkpointer during lifespan. graph_factory is invoked once per startup.
`graph_client=` allows injecting an httpx client for offline tests; injected
clients remain caller-owned. Production clients have no transport retries.

Turn rows are inserted before agent work. A nullable outcome denotes an active
turn. `inbound_messages.next_attempt_at` makes exponential runner backoff durable.
Retrying/crashed turns retain the same inbound membership and deterministic ID,
even if newer messages arrive meanwhile. Completion and failure use a conditional
write to fence stale lease owners on SQLite as well as PostgreSQL. Outcome,
inbound completion, outbox insertion, and lease release commit atomically.
Failures store exception class names, not exception messages or raw data. Audit
events and handoff transitions remain for later milestones.

SQLite permits one active Wagent worker process per file, enforced with an OS
file lock. Within that process, bounded async slots let separate senders proceed
concurrently; each conversation has one lease. PostgreSQL claims use SKIP LOCKED
and conditional updates. LEASE_SECONDS must exceed runner and send timeouts.
Cancellation leaves a processing turn for recovery after lease expiry.

Outbox rows become sending in a committed transaction before any HTTP call.
`outbound_messages.sending_at` permits stale-send recovery after LEASE_SECONDS;
such rows become unknown and never get requeued. A live sender has a shorter
HTTP timeout, so another process cannot prematurely recover its in-flight send.
Ambiguous transport/server responses are unknown; definite failures are failed.
M2 does not auto-retry sends (including 429). M3 adds safe send retry scheduling,
status reconciliation, templates, and chunking. A basic free-form window guard
rejects closed-window sends rather than violating Meta's rules.

The runner contract includes Button/handoff fields for API continuity, but M2
rejects those outputs. escalation_tool=True raises a clear unsupported error;
its actual tool and mode transitions arrive in M5. The example intentionally
uses a deterministic fake model and performs no real clinic bookings.

## M3: receipts, pricing, templates, and retries

Add message_status_events to durably retain unmatched receipts and handle the
race where a status arrives before a send's returned ID is committed. Receipts
are deduplicated by a hash of the canonical raw status plus business phone ID.
Every new send carries the outbox UUID in biz_opaque_callback_data. Matching is
scoped to the business phone, and a conflicting known wamid is never overwritten.
Status updates and pricing commit in the same ingress transaction as any inbound
messages. Atomic conditional writes fence concurrent send/status updates.

Usage has a unique outbound_id: delivery/read retries cannot multiply message
counts. Preserve absent billable as null and retain raw pricing, even when pywa
would infer a value. pricing_at orders pricing updates; recorded_at is the
message's earliest pricing receipt timestamp for UTC daily aggregates. The
public Meta pricing page conflicts with the brief's 2026-10-01 claim; no rates or
free-message allowances are encoded. Exact source findings are in VERIFIED.md.

Add audit_events now because the M3 drop policy requires an audit. type is a
string so outbound_drop can be recorded in addition to future handoff/opt-out
transitions. No other admin workflows are added. Minimal Bearer auth is needed
for the M3 usage endpoint and will be reused by later admin routes.

Use pywa.send_template for parameterless fallback templates. A separate SDK
client forwards through the caller's HTTP client without sharing mutable headers
or client ownership. A task-scoped filter suppresses pywa.api DEBUG bodies during
our template calls only. Require WA_WABA_ID at configuration time, even though
SDK sending by known name does not perform a WABA lookup.

Chunk queued text at 4096 characters, with stable indexed dedupe keys and ordered
creation timestamps. A closed-window long reply coalesces remaining queued chunks
into one template. Dropped/coalesced rows use failed plus an explanatory code,
preserving the existing outbound status vocabulary.

Send retries have their own SEND_MAX_ATTEMPTS budget. Honor Retry-After on a
narrow explicit rejection allowlist, recheck policy/window for each attempt, and
never auto-retry ambiguous sends or failed-delivery webhooks. Status evidence can
resolve unknown even when the HTTP response was lost. New retry schedules live
in outbound_messages.next_attempt_at; queued older replies block younger sends
within the same conversation. Template sends and text sends use identical rules.

## M4: control admission and erasure

Debounce uses correlated min/max pending receipt timestamps, with defaults
1500/5000 ms. Retry turns bypass debounce but preserve membership and backoff;
an exact STOP batch can preempt backoff. Claims include all modes and opted-out
conversations so controls are checked before deciding whether to run an agent.
Turn.agent_attempted separates real admissions from skipped/control turns.

Cap reservations are serialized by a conversation write before outbox mutation,
the same lock order used by turn completion and erasure. cap_reserved_at tracks
the current UTC-day reservation for each logical outbound. Safe same-day retries
reuse capacity. Ambiguous/failed attempts retain capacity conservatively. This
is a send safety budget, not Meta's billing count. Migration backfills known
accepted/unknown/in-flight rows. The one opt-out confirmation is explicitly
exempt from the cap, mode, and global pause, but never from the window/signature/
unknown-send rules. All templates are blocked for opted-out users.

The first opt-out atomically audits, cancels queued normal replies, queues one
uniquely keyed confirmation, and finishes the inbound batch. Later STOP messages
cannot queue another confirmation. There is no opt-in/reset control yet. Normal
human-mode messages are skipped without agent calls; notification hooks stay M5.

Erasure uses a durable erasing flag plus exclusive lease: reject active agent/
send work, fence ingress/claims, delete checkpoints using the verified saver API,
then delete linked status receipts and cascade application rows. A checkpoint
error leaves the flag and releases this eraser's lease for an immediate retry;
a crashed eraser is reclaimable after expiry. No success is reported until both
stores are cleared. Erasure retains no identifying audit record. Later inbound
or unmatched status events are new data, not retroactively prevented by deletion.
The CLI scopes by sender across all business phones and needs database settings
only. The API scopes by conversation UUID and existing Bearer authentication.

Retention only clears old inbound raw JSON, not text/checkpoint/usage history.
Worker identifiers use optional LOG_SALT HMAC labels; absent salt means no sender
logging. No UI, handoff notifications, broadcasts, or hosted services are added.

## M4: controls and erasure

Debounce uses min/max pending receipt timestamps in the database. Retry turns
bypass debounce without changing membership. Whole-message opt-out matching runs
before pause/mode/rate admission; default opt-out keywords can preempt a retry
backoff. Non-bot modes still claim and store/skips batches so STOP is checked.
Human notifications remain M5 scope.

turns.agent_attempted distinguishes admitted turns from skipped control batches;
legacy turns default true. A persisted last_rate_notice_at limits notices to one
per rolling hour. Outbound purpose distinguishes the single deduped opt-out
confirmation from ordinary agent/slow-down text. Only that confirmation bypasses
mode/pause and daily cap, and it can never fall back to a template.

Daily caps are conservative per-conversation UTC-day send reservations stored in
cap_reserved_at, not a delivered-message or monetary ledger. Unknown outcomes and
safe failures retain reservations; a safe retry reuses its same-day slot. The
migration backfills accepted/unknown/in-flight rows using existing timestamps.
Sender admissions lock the conversation before outbox rows, matching worker and
erasure lock order. This serializes capacity admission across PostgreSQL workers.

Erasure first locks a conversation, then checks live sends in a fresh statement
so PostgreSQL snapshots cannot miss a just-committed sending row. Live work
returns 409. A durable erasing flag plus an exclusive expiring erasure lease
fences workers/senders/ingestion while the separate checkpointer transaction
runs. Checkpoint deletion is bounded below the lease duration. Failed checkpoint deletion
leaves the flag and releases the owner for retry; a crash or failure in the final
application transaction leaves an expiring lease.
Ingestion returns 503 instead of acknowledging new messages into an erasing row.
The final transaction removes matching status receipts and cascades all linked
application rows. No identifying purge audit is retained after erasure. The CLI
erases all matching wa_id conversations in the configured database and does not
require WhatsApp credentials.

Retention clears only raw inbound JSON to SQL NULL. It runs at startup and on a
configurable interval in ingestion-only and worker-enabled applications. Stable
HMAC labels require explicit LOG_SALT; without it, no sender identifiers are
logged by the worker. Application database connections already hide SQL params.

## PostgreSQL concurrency correction

An ON CONFLICT update targeting only (phone_number_id, wa_id) can still fail on
its separate thread_id uniqueness constraint during concurrent first deliveries.
Insert conversations with untargeted ON CONFLICT DO NOTHING, then atomically
update the exact sender row if it already exists. This handles either unique
index without dropping constraints or changing the transaction/dedupe guarantees.
The live PostgreSQL regression now submits ten concurrent duplicate deliveries.
