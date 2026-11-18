# Agents, memory, and replies (M2)

M2 extends the gateway: receive a message, store it, let a background worker call
an agent, save its reply in an outbox, and send that reply through the Graph API.
The webhook only verifies and stores. It never waits for the agent or Graph API.

## Try the offline tests first

No Meta account or LLM key is needed to see the complete pipeline working:

```sh
uv run pytest -q tests/e2e/test_pipeline.py
uv run pytest -q tests/integration/test_langgraph.py
uv run pytest -q tests/e2e/test_crash.py
```

If uv is installed in the earlier `ve` environment, use `./ve/bin/uv` instead of
`uv`. These tests use temporary databases and a fake Graph API. They send nothing
to real WhatsApp users. The crash test kills a child sender process after it has
committed sending and written a fake request, then verifies no resend occurs.

## Enable a runner in your application

First update your application schema. This adds turns, outbound_messages, and
retry timestamps; it preserves the M1 inbox tables and data:

```sh
uv sync --all-extras
uv run alembic upgrade head
```

In `.env`, configure WA_PHONE_ID, WA_APP_SECRET, WA_VERIFY_TOKEN, and WA_TOKEN.
For local SQLite use `DATABASE_URL=sqlite+aiosqlite:///./wagent.db`.
PostgreSQL uses `postgresql+asyncpg://...` and the postgres extra.

A simple `app.py` that echoes incoming text:

```python
from wagent import Settings, Wagent
from wagent.runners import EchoRunner

app = Wagent(settings=Settings(), runner=EchoRunner()).fastapi_app
```

Run it with `uv run uvicorn app:app`. After you configure Meta's callback to this
application's public webhook URL, incoming messages will be echoed back. This
uses your real WA_TOKEN and can send real messages. Endpoint configuration and
signature behavior are described in [the gateway guide](gateway.md).
Omitting `runner=` keeps the application ingestion-only.

For the included LangGraph example, run from the repository root:

```sh
uv run uvicorn examples.clinic_booking.app:app
```

The example uses a fake model: it repeats the user text with a count of prior
user messages. It does not contact an LLM, create appointments, or need an LLM
API key. It demonstrates persistent memory and a graph factory you can replace.
It still uses real Graph API delivery when started with your WhatsApp credentials.

## What the new files do

| File | Job |
| --- | --- |
| runners/base.py | Describes the messages given to your agent and the reply it returns. |
| runners/echo.py | Returns the incoming text, useful for testing. |
| runners/langgraph_runner.py | Opens persisted memory and calls your graph with a stable thread ID. |
| worker/queue.py | Claims a conversation, tracks retries, and saves a completed turn and its reply. |
| worker/loop.py | Runs agent calls in the background with timeouts and bounded concurrency. |
| outbound/sender.py | Claims a saved reply, commits sending, and makes one HTTP send attempt. |
| outbound/window.py | Checks whether free-form replies are still allowed. |
| store/migrations/versions/0002_worker_outbox.py | Adds the M2 database structure. |
| examples/clinic_booking/agent.py | Small graph with a fake model and persisted message history. |

## Database workflow

1. Webhook inserts an inbound message with status pending.
2. Worker leases that conversation and changes its messages to processing.
3. Worker records a deterministic turn ID based on the inbound message IDs.
4. Agent receives messages sorted by WhatsApp timestamp, then receipt time.
5. On success, one transaction marks inbound messages done, finishes the turn,
   inserts a queued reply, and releases the lease.
6. Sender commits the reply as sending, then calls Meta.
7. A successful API response records sent and its WhatsApp message ID. This means
   API acceptance, not delivered/read confirmation; M3 tracks those status receipts.

A runner exception or timeout increments attempts and schedules durable backoff.
After MAX_ATTEMPTS the inbound messages become failed. Newer messages cannot
bypass an older turn waiting for retry. A crash leaves the original turn to be
reclaimed after its lease expires. There is no rollback of arbitrary agent tool
side effects: tool authors must handle repeated invocation themselves.

If a send's outcome is unclear, the row is unknown. A crash leaving sending is
also changed to unknown once LEASE_SECONDS has passed. Such rows are not resent.
M3 adds safe send retries and status resolution; see [sending and usage](sending.md).
The operator resolution endpoint remains for a later milestone.

## Memory and configuration

LangGraphRunner accepts `graph_factory(checkpointer)`, optional
`input_mapper(ctx)` and `output_mapper(state)`. Its default input is a
HumanMessage whose ID is the turn ID; its output is the last AIMessage text.
It supplies `thread_id=wa:{business_phone_id}:{sender_id}` on every graph call.

SQLite checkpoint tables live in the same file as the inbox and survive restarts.
PostgreSQL uses AsyncPostgresSaver. The runner initializes checkpoint tables at
startup; run only one initial startup before adding PostgreSQL worker processes
on a newly provisioned database. Application tables still require Alembic.

| Environment variable | Default | Purpose |
| --- | --- | --- |
| LEASE_SECONDS | 120 | How long a worker owns a conversation / stale-send grace period. |
| RUNNER_TIMEOUT_SECONDS | 60 | Maximum time allowed for an agent call. |
| SEND_TIMEOUT_SECONDS | 20 | Maximum time allowed for an HTTP send. |
| MAX_ATTEMPTS | 3 | Maximum agent attempts before failing a turn. |
| RETRY_BACKOFF_SECONDS | 1 | Initial exponential delay for agent retries, capped at 300 seconds. |
| WORKER_POLL_SECONDS | 0.1 | Idle polling interval. |
| WORKER_CONCURRENCY | 8 | Async slots for turns and for outbound sends. |
| WA_API_VERSION | v26.0 | Explicit Graph API version. |

LEASE_SECONDS must exceed both timeouts. Custom async runners should honor
cancellation and avoid blocking the event loop. Shutdown cancels background work
before closing the checkpointer or HTTP client.

SQLite permits one active worker process per database file, with concurrent
conversations inside that process. PostgreSQL supports multiple processes.
A separate sender serializes queued sends within each conversation.

M3 now adds [templates, delivery statuses, usage, chunking, and safe send retries](sending.md).
M4 now adds [debounce, limits, opt-out, retention, and erasure](controls.md).
M5 adds human takeover and the remaining admin APIs.
In M2, buttons, escalation_tool=True, and AgentReply.handoff are unsupported.
