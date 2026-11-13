# wagent: build brief

Put this file at `docs/BRIEF.md` in the repo. `AGENTS.md` goes in the repo root.

---

## 0. What we are building

A Python library (MIT licence) that lets a developer put a **LangGraph** agent on **WhatsApp** (Cloud API) in about ten lines,
with the tricky production parts already solved:

| # | Problem every WhatsApp bot hits | What wagent does |
|---|---|---|
| 1 | Webhook handshake and signature check | Verifies both; acknowledges fast; never does agent work in the request |
| 2 | Duplicate and out-of-order deliveries | Stores each message id once; processes per conversation, in order |
| 3 | No memory between messages | One LangGraph thread per sender, persisted in SQLite or Postgres |
| 4 | The 24-hour customer service window | Tracks it; free-form inside, template fallback outside |
| 5 | Human takeover | Per-conversation `bot / human / paused` mode, admin API, notification hook |
| 6 | Failed or ambiguous sends | Outbox with idempotency keys; no blind retries that could double-send |
| 7 | Cost surprises (replies became billable on 1 Oct 2026, see section 1) | Batches rapid messages into one reply, rate limits, records Meta's pricing info per message |
| 8 | Testing without Meta | Local simulator, signed fake webhooks, fake Graph API |

**Non-goals for v0.1:** no UI, no multi-tenant hosting, no billing, no WhatsApp Flows, no calling, no marketing campaigns,
no support for other messaging providers. A hosted version is phase 2 (section 12) and must not be started early.

**Built on:** `pywa` (WhatsApp client and webhook handling), `langgraph` / `langchain`, FastAPI, SQLAlchemy 2 (async), Alembic.

---

## 1. Facts this design relies on

These came from research on 12 Nov 2026. Items marked **VERIFY** may be stale or imprecise: confirm them against official
docs or installed source before coding against them, and record the result in `docs/VERIFIED.md`.

**WhatsApp Cloud API**
- Webhook setup: Meta sends a `GET` with `hub.mode=subscribe`, `hub.verify_token`, `hub.challenge`. Reply `200` with the challenge only if the token matches.
- Every `POST` carries `X-Hub-Signature-256` = `sha256=` + HMAC-SHA256 of the **raw request body** keyed with the app secret. Verify on raw bytes, before JSON parsing, with constant-time comparison.
- Delivery is at-least-once, with no ordering guarantee. Meta retries when it does not get a fast `200`. Aim to acknowledge in well under a second (guides cite a 5 second limit). **VERIFY** the exact timeout.
- Deduplicate on the WhatsApp message `id`. Order by the message `timestamp`, not arrival order.
- The customer service window opens when the user messages you and lasts 24 hours from their **most recent** message. Inside it you may send free-form messages. Outside it you may send only approved **template** messages.
- Pricing changed on **1 Oct 2026** (several 2026 sources agree): free-form replies inside the window became billable per delivered message after the first 1,000 free service messages per business phone number per month; utility templates inside the window also became billable; inbound messages stay free; messages inside a 72-hour free-entry-point window stay free. **VERIFY** on Meta's pricing page.
- Message status webhooks (`sent`, `delivered`, `read`, `failed`) carry a `pricing` object with fields including `billable`, `pricing_model`, `type`, `category`. **VERIFY** the current field names.
- A failed send request may still have reached Meta, so blind retries can duplicate messages. Retry only when it is provably safe.
- **VERIFY:** maximum text body length (believed 4096 characters), limits on reply buttons and lists, and the limits of Meta's free test phone number.

**pywa** (latest seen: 4.3.1; Python 3.10+)
- Install with `pip install -U "pywa[server]"`. Async API is `pywa_async` (same interface, `async`/`await`).
- Constructor takes `phone_id`, `token`, `app_id`, `app_secret`, `callback_url`, `verify_token`, and `server=<FastAPI app>`; `webhook_endpoint="/webhook"` sets the path.
- Handlers: `@wa.on_message(filters.text)`; `msg.reply(...)`; templates via `wa.send_template(...)` (needs `waba_id`).
- **VERIFY:** the status-update handler name and the status object fields; whether pywa validates `X-Hub-Signature-256` itself when `app_secret` is set (if you cannot confirm, add our own raw-body check in front, with a test); how to read the message id, timestamp and sender; how to mark a message as read.

**LangGraph / LangChain** (LangGraph 1.0 went GA in Oct 2025)
- `langgraph.prebuilt.create_react_agent` is deprecated; the replacement is `langchain.agents.create_agent`. **VERIFY** the current signature.
- Persistence: pass `{"configurable": {"thread_id": "..."}}` on every call. Production checkpointer: `AsyncPostgresSaver` (`langgraph-checkpoint-postgres`), used as `async with AsyncPostgresSaver.from_conn_string(url) as cp: await cp.setup()`. Dev: `AsyncSqliteSaver` (`langgraph-checkpoint-sqlite`). **VERIFY** exact import paths.
- Human-in-the-loop pattern in LangGraph: `interrupt()` and `Command(resume=...)`, which requires a checkpointer. wagent's handoff does **not** depend on it (section 8.5).
- The `add_messages` reducer replaces a message that has the same `id`. We rely on this to avoid duplicate history on retry. **VERIFY** the behaviour.
- **VERIFY:** how to delete a thread's checkpoints (needed for data erasure) and how to append a message to a thread's state from outside the graph (needed for handoff context).

---

## 2. Architecture

```text
 WhatsApp user
      |
      v
 Meta Cloud API --(POST /webhook, signed)--> [Gateway: FastAPI + pywa]
                                                |  1. verify signature (raw body)
                                                |  2. upsert conversation
                                                |  3. INSERT inbound message (dedupe on wa_message_id)
                                                |  4. record status events + pricing info
                                                |  5. return 200 immediately
                                                v
                                        [Database: SQLite or Postgres]
                                           conversations, inbound_messages, turns,
                                           outbound_messages, usage, audit_events
                                                ^
                                                | claim conversation (lease), debounce, batch
                                                |
                                         [Worker loop (in-process asyncio)]
                                                |  mode check (bot/human/paused), opt-out, rate limits
                                                v
                                         [AgentRunner]  --> LangGraph graph (user code)
                                                |              thread_id per sender, checkpointer
                                                v
                                         [Outbox / Sender] --window check--> Graph API (free-form or template)
```

Design choices: **no Redis, no broker.** The database is the queue. Single process by default. On Postgres, several workers may run
(claim with `FOR UPDATE SKIP LOCKED` and per-conversation leases). SQLite supports one worker only.

---

## 3. Delivery guarantees (state these in the docs)

- **Inbound:** at-least-once from Meta; we deduplicate on `wa_message_id`, so each message is processed **at most once per successful turn**.
- **Agent turn:** at-least-once. A crash mid-turn causes a re-run after the lease expires. The turn id is deterministic (hash of the sorted inbound message ids), and the human message sent to LangGraph uses that turn id as its message `id`, so history is not duplicated.
- **Outbound:** effectively once. Each outbound row has a unique `dedupe_key` (`{turn_id}:{index}`). If a row with that key exists in state `sending`, `sent`, `delivered`, `read` or `unknown`, we do not send again.
- **Ambiguous send** (timeout or connection reset after the request was written): mark `unknown`, do **not** auto-retry free-form text. Resolve through the status webhook, or surface it in the admin API. Only retry automatically on: connection failure before the request was sent, HTTP 429 (honour `Retry-After`), and errors Meta documents as safe to retry (**VERIFY** the list).

---

## 4. Data model (SQLAlchemy 2, async; Alembic migrations)

All timestamps are timezone-aware UTC.

**conversations**
`id` (uuid pk), `phone_number_id`, `wa_id`, `thread_id` (unique, `wa:{phone_number_id}:{wa_id}`), `mode` (`bot|human|paused`, default `bot`),
`last_inbound_at`, `opted_out` (bool), `locale` (nullable), `lease_owner`, `lease_until`, `created_at`, `updated_at`.
Unique `(phone_number_id, wa_id)`.

**inbound_messages**
`wa_message_id` (pk), `conversation_id` (fk), `received_at`, `wa_timestamp`, `kind` (`text|audio|image|document|location|interactive|other`),
`text` (nullable), `media_id` (nullable), `raw` (JSON, purged after `RETENTION_DAYS`), `status` (`pending|processing|done|failed|skipped`),
`attempts`, `last_error`, `turn_id` (nullable).

**turns**
`id` (deterministic hash), `conversation_id`, `started_at`, `finished_at`, `inbound_ids` (JSON list), `outcome` (`replied|handoff|skipped|error`), `latency_ms`.

**outbound_messages**
`id` (uuid pk), `conversation_id`, `turn_id` (nullable for human/admin sends), `dedupe_key` (unique), `kind` (`text|template|interactive|media`), `body` (JSON),
`status` (`queued|sending|sent|delivered|read|failed|unknown`), `wa_message_id` (nullable, unique), `error_code`, `attempts`, `created_at`, `sent_at`.

**usage**
`id`, `outbound_id` (fk), `billable` (bool, nullable), `pricing_model`, `category`, `type`, `recorded_at`. Filled from status webhooks.

**audit_events**
`id`, `conversation_id`, `type` (`handoff|resume|pause|opt_out|purge|admin_reply|runner_error`), `actor`, `at`, `meta` (JSON).

---

## 5. Target developer experience

This is the design target for the public API. Names are ours, so adjust the details if reality demands, but keep the shape and document the change in `docs/DECISIONS.md`.

```python
# app.py
from wagent import Wagent, Settings
from wagent.runners import LangGraphRunner
from my_agent import build_graph  # (checkpointer) -> compiled LangGraph graph with a "messages" state key

wagent = Wagent(
    settings=Settings(),                     # reads WA_* and DATABASE_URL from the environment
    runner=LangGraphRunner(build_graph, escalation_tool=True),
)
app = wagent.fastapi_app                     # run: uvicorn app:app
```

Custom runner contract (any agent, not only LangGraph):

```python
class AgentRunner(Protocol):
    async def run(self, ctx: TurnContext) -> AgentReply: ...

@dataclass
class TurnContext:
    thread_id: str
    wa_id: str                    # raw id; do not log
    messages: list[InboundMessage]  # the batched user messages for this turn, in order
    window_open: bool
    now: datetime

@dataclass
class AgentReply:
    text: str | None = None
    buttons: list[Button] | None = None     # quick-reply buttons
    handoff: bool = False                   # switch conversation to human mode
    meta: dict[str, Any] = field(default_factory=dict)
```

`LangGraphRunner(graph_factory, input_mapper=None, output_mapper=None, escalation_tool=False)`:
- builds the checkpointer from `DATABASE_URL` (SQLite in dev, Postgres in prod), calls `graph_factory(checkpointer)` once at startup;
- per turn: invokes with `{"configurable": {"thread_id": ctx.thread_id}}` and input `{"messages": [HumanMessage(content=<joined text>, id=<turn_id>)]}` by default;
- reads the last AI message as the reply; `output_mapper` overrides this;
- `escalation_tool=True` adds a tool `escalate_to_human(reason: str)` that sets a handoff flag the runner reads after the turn.

Other runners to ship: `EchoRunner` (testing) and `HttpRunner(url, secret)` (calls a remote agent over signed HTTP; the contract phase 2 reuses, see section 12).

---

## 6. HTTP endpoints

Public (webhook path configurable, default `/webhook`): `GET /webhook` (handshake), `POST /webhook` (events), `GET /healthz`, `GET /readyz` (checks DB).

Admin (Bearer `ADMIN_API_KEY`, disabled if the key is unset):
- `GET /admin/conversations?mode=human&limit=50`
- `GET /admin/conversations/{id}/messages?limit=100` (merged inbound and outbound, time ordered)
- `POST /admin/conversations/{id}/reply` body `{ "text": "...", "idempotency_key": "..." }`
- `POST /admin/conversations/{id}/handoff` and `POST /admin/conversations/{id}/resume`
- `DELETE /admin/conversations/{id}` (erase: rows, plus the LangGraph thread)
- `GET /admin/usage?from=...&to=...` (counts by day, category, billable)

---

## 7. Configuration (pydantic-settings; env var names are ours)

```text
WA_PHONE_ID, WA_TOKEN, WA_APP_ID, WA_APP_SECRET, WA_VERIFY_TOKEN, WA_WABA_ID (needed for templates), WA_CALLBACK_URL (optional)
DATABASE_URL                    sqlite+aiosqlite:///./wagent.db  or  postgresql+asyncpg://...
ADMIN_API_KEY                   optional
LOG_SALT                        for hashing phone numbers in logs
DEBOUNCE_MS=1500                wait for the user to stop typing; MAX_DEBOUNCE_MS=5000
LEASE_SECONDS=120               RUNNER_TIMEOUT_SECONDS=60       MAX_ATTEMPTS=3
RATE_LIMIT_TURNS_PER_MIN=6      DAILY_OUTBOUND_CAP_PER_USER=50
OUT_OF_WINDOW_POLICY=drop|template   FALLBACK_TEMPLATE_NAME, FALLBACK_TEMPLATE_LANG
OPT_OUT_KEYWORDS=STOP,UNSUBSCRIBE    HANDOFF_KEYWORDS=            (empty by default)
HANDOFF_WEBHOOK_URL             optional; JSON POST when a conversation enters human mode
MARK_AS_READ=true               RETENTION_DAYS=30               PAUSE_ALL=false
```

---

## 8. Key algorithms

**8.1 Ingest (`POST /webhook`)**
Read raw body; verify signature (401 on failure). For each message event: upsert the conversation; insert the inbound row with `ON CONFLICT DO NOTHING` on `wa_message_id`; set `last_inbound_at = max(existing, wa_timestamp)`. For each status event: update the outbound row and insert a `usage` row from the `pricing` object. Return `200` without calling the agent. Unknown event types are logged at DEBUG and ignored, never a 5xx.

**8.2 Worker loop**
Pick conversations with `pending` inbound rows, no live lease, and a newest-pending message older than `DEBOUNCE_MS` (or an oldest-pending message older than `MAX_DEBOUNCE_MS`). Take the lease. Load all pending rows ordered by `(wa_timestamp, received_at)`.
Then, in order:
1. Opt-out keyword in the batch (checked in **every** mode, so a user in human mode can still stop messages) → set `opted_out`, add an audit event, send one confirmation (free-form; the window is open because the user just wrote), mark rows `done`, stop.
2. `PAUSE_ALL` or mode `paused` → mark rows `skipped`.
3. Mode `human` → mark rows `skipped`, fire the handoff notification once per new message batch, stop.
4. Rate limit exceeded → mark `skipped`, send at most one "please slow down" per hour.
5. Optionally mark the last inbound as read (`MARK_AS_READ`).
6. Build `TurnContext`; call the runner under `RUNNER_TIMEOUT_SECONDS`.
7. On success: write the turn, enqueue outbound rows (`dedupe_key = {turn_id}:{i}`), mark inbound `done`, release the lease. If `reply.handoff`: set mode `human`, audit event, notify.
8. On failure: `attempts += 1`, back off, release the lease. After `MAX_ATTEMPTS`: mark `failed`, audit event, optionally send a configured apology (off by default).

**8.3 Sender (outbox)**
Default is **one outbound message per turn** (the reply is consolidated; no filler messages). Long text is split at the platform limit on paragraph boundaries.
Before sending, compute `window_open = now < last_inbound_at + 24h`.
- Window open: send free-form.
- Window closed and policy `template`: send `FALLBACK_TEMPLATE_NAME`. Never send marketing-category templates to an opted-out user.
- Window closed and policy `drop`: do not send; write an audit event.
Apply `DAILY_OUTBOUND_CAP_PER_USER`. Write the row as `sending` **before** the HTTP call; set `sent` with the returned `wa_message_id` after. See section 3 for retry rules.

**8.4 Debounce and batching**
Users often send three short messages in a row. Joining them into one turn gives better answers and one billable reply instead of three.

**8.5 Human handoff**
Handoff is a **database mode flag**, not a LangGraph interrupt, so it survives restarts and deploys.
Entry: the agent's `escalate_to_human` tool, `HANDOFF_KEYWORDS`, or `POST .../handoff`. While in `human` mode the bot never replies; inbound messages are stored and the notification hook fires. A human replies through `POST .../reply` (the window check still applies). `POST .../resume` returns to `bot`.
Optional (M5 stretch, **VERIFY** the API): on resume, append the human's messages to the LangGraph thread state so the bot has context.

**8.6 Cost visibility**
Store the `pricing` object per outbound message. `GET /admin/usage` aggregates by day, `category`, `billable`. No money amounts are computed by default, because rates differ by country and change; allow an optional user-supplied rate table to estimate spend.

**8.7 Data protection**
Hash phone numbers in logs. `DELETE /admin/conversations/{id}` and `wagent purge --wa-id <id>` remove all rows and the LangGraph thread. A retention job deletes `raw` payloads older than `RETENTION_DAYS`. Document that operators are responsible for their own legal basis for processing and for Meta's messaging policies.

---

## 9. Repository layout

```text
wagent/
  AGENTS.md
  pyproject.toml            # uv; Python >= 3.11; extras: postgres, dev, docs
  README.md  CHANGELOG.md  LICENSE (MIT)  .env.example
  docs/  BRIEF.md  VERIFIED.md  DECISIONS.md  REAL_TEST.md  index.md (mkdocs-material)
  src/wagent/
    __init__.py  app.py  settings.py  logging.py
    gateway/      webhook.py  signature.py  events.py
    store/        models.py  repo.py  migrations/ (alembic)
    worker/       loop.py  debounce.py  limits.py  handoff.py
    runners/      base.py  langgraph_runner.py  echo.py  http_runner.py
    outbound/     sender.py  window.py  templates.py  chunking.py
    admin/        api.py  auth.py
    testing/      fake_graph_api.py  payloads.py  simulator.py
    cli.py        # typer: init, dev, simulate, doctor, purge
  tests/  unit/  integration/  e2e/
  examples/  clinic_booking/  order_status/
  .github/workflows/ci.yml  release.yml
```

---

## 10. Test plan (must exist by the milestone that introduces the behaviour)

- Signature: valid, invalid, missing header, body with emoji and accented characters (verify on raw bytes), tampered body.
- Handshake: right and wrong verify token.
- Duplicate delivery: the same message id posted 3 times → exactly one agent run, one reply.
- Out-of-order: two messages arriving in reverse timestamp order are processed in timestamp order.
- Slow agent: the webhook returns `200` fast while the runner takes several seconds.
- Crash safety: kill the worker after the outbound row is `sending`, restart → no second send; row ends `unknown` or `sent` per the status webhook.
- Memory: same sender, two turns, app restart in between → the second turn sees the first (real SQLite checkpointer).
- Window boundary (frozen time): 23h59m free-form; 24h01m falls to template or drop.
- Debounce: three messages within 500 ms → one turn, one reply.
- Rate limit and daily cap; opt-out; handoff in both directions; purge removes everything including the thread.
- Status webhooks: `pricing` object stored; `failed` status recorded.
- Fake Graph API contract tests: request shapes for text, template, buttons.

---

## 11. Milestones: paste one prompt at a time into Codex

Estimate: with Codex writing the code, plan 4 to 6 weeks of evenings and weekends. Most of the time goes on reviewing and on real-WhatsApp testing, not typing.
Do not move on until the previous milestone's acceptance criteria pass.

### M0: scaffold

```text
Read AGENTS.md and docs/BRIEF.md. Implement ONLY milestone M0.
Create the repo scaffold from section 9: pyproject.toml (uv, Python >=3.11, src layout, extras postgres/dev/docs),
ruff + pyright config, pytest + pytest-asyncio config, pre-commit, GitHub Actions CI running lint, format check, types and tests
on Python 3.11 and 3.12, MIT LICENSE, README stub, CHANGELOG, .env.example (placeholders only), empty docs/VERIFIED.md and docs/DECISIONS.md.
Create the package skeleton with importable empty modules and a `wagent --version` CLI command.
Acceptance: `uv sync --all-extras`, `uv run ruff check .`, `uv run ruff format --check .`, `uv run pyright`, `uv run pytest -q` all pass,
and CI is green. Pin dependency lower bounds after checking the latest releases of pywa, langgraph, langchain, fastapi and sqlalchemy.
```

### M1: gateway, signature, dedupe, durable inbox

```text
Read AGENTS.md and docs/BRIEF.md. Implement ONLY milestone M1 (sections 3, 4, 6, 8.1).
Before coding, resolve the VERIFY items for pywa in section 1 and write the findings to docs/VERIFIED.md.
Build: settings, SQLAlchemy models + Alembic migration for conversations and inbound_messages, the FastAPI gateway using pywa,
GET handshake, POST signature verification on the raw body (add our own check if pywa does not provide it),
upsert conversation, insert inbound with ON CONFLICT DO NOTHING, fast 200, /healthz and /readyz.
Build testing/payloads.py (generate realistic signed Cloud API webhook payloads) and a fake Graph API in testing/fake_graph_api.py.
Acceptance: tests for valid/invalid/missing signature, emoji and accented text signature, tampered body, handshake right/wrong token,
same message id posted 3 times yields one inbound row, out-of-order timestamps are stored correctly, unknown event types never cause a 5xx,
and the webhook responds in under 200 ms in tests. Do not implement the worker or sending yet.
```

### M2: sessions, runner, memory, outbound text

```text
Read AGENTS.md and docs/BRIEF.md. Implement ONLY milestone M2 (sections 5, 8.2 steps 6-8, 8.3 free-form path).
Resolve the LangGraph/LangChain VERIFY items first and record them in docs/VERIFIED.md.
Build: the AgentRunner protocol, EchoRunner, LangGraphRunner (checkpointer from DATABASE_URL, thread_id per sender,
HumanMessage id = turn id, default input/output mapping), the worker loop with leases, turns table, outbound_messages table,
the sender with sending-before-HTTP-call semantics and dedupe_key, and the Graph API text send (via pywa or httpx, whichever
VERIFY shows is safer for idempotent control). Provide examples/clinic_booking as a tiny LangGraph agent using a fake LLM for tests.
Acceptance: end-to-end test with signed webhook -> worker -> EchoRunner -> fake Graph API receives exactly one message;
memory test across an app restart with a real SQLite checkpointer; crash test (kill after status `sending`) causes no second send;
runner timeout and failure paths set attempts/backoff; messages from one sender are processed in order; two senders do not block each other.
```

### M3: 24-hour window, templates, status webhooks, usage

```text
Read AGENTS.md and docs/BRIEF.md. Implement ONLY milestone M3 (sections 8.3, 8.6, window rules in section 1).
Build: window computation, OUT_OF_WINDOW_POLICY drop|template with FALLBACK_TEMPLATE_NAME/LANG, template sending via pywa (needs WA_WABA_ID),
status webhook handling (sent/delivered/read/failed) updating outbound rows, the usage table filled from the pricing object,
GET /admin/usage, long-text chunking at the verified platform limit, and the ambiguous-send `unknown` state with the retry rules from section 3.
Resolve the VERIFY items about status handlers, the pricing object and body limits first.
Acceptance: frozen-time tests at 23h59m and 24h01m; template path and drop path both tested; statuses update rows idempotently
(the same status posted twice changes nothing); pricing object stored; a `failed` status is recorded with the error code; chunking tested with a 10,000-character reply.
```

### M4: debounce, limits, opt-out, erasure

```text
Read AGENTS.md and docs/BRIEF.md. Implement ONLY milestone M4 (section 8.2 steps 1, 2 and 4, plus 8.4 and 8.7).
Build: debounce/batching (DEBOUNCE_MS, MAX_DEBOUNCE_MS), per-user turn rate limit and daily outbound cap, PAUSE_ALL, opt-out keywords with a
single confirmation message and a block on template sends for opted-out users, retention job for raw payloads, DELETE /admin/conversations/{id}
and `wagent purge`, hashing of phone numbers in logs. Resolve the VERIFY item on deleting a LangGraph thread first.
Acceptance: three messages within 500 ms produce one turn and one reply; rate limit and cap behave as specified; opt-out and purge tests pass
(including opt-out while the conversation is in human mode, and that no rows and no checkpoints remain after purge);
logs in tests contain no raw phone numbers or tokens.
```

### M5: human handoff and admin API

```text
Read AGENTS.md and docs/BRIEF.md. Implement ONLY milestone M5 (sections 6 and 8.5).
Build: conversation modes, the escalate_to_human tool for LangGraphRunner, HANDOFF_KEYWORDS, HANDOFF_WEBHOOK_URL notification (signed JSON, with retries),
the admin API with Bearer auth (constant-time comparison; disabled when ADMIN_API_KEY is unset), admin reply with idempotency_key and the window check,
resume. Stretch (only if the VERIFY result shows a clean API): append human messages to the LangGraph thread on resume.
Acceptance: bot never replies in human mode; inbound is still stored and the hook fires once per batch; admin reply is delivered and is idempotent
on repeated idempotency_key; handoff survives an app restart; unauthorised admin calls get 401; every transition writes an audit event.
```

### M6: developer experience, docs, release

```text
Read AGENTS.md and docs/BRIEF.md. Implement ONLY milestone M6.
Build: `wagent dev` (SQLite, local simulator chat REPL so no Meta account is needed), `wagent simulate`, `wagent doctor`
(checks env vars, token validity, webhook reachability, template existence, DB connectivity), `wagent init` (scaffolds an app.py and .env),
examples/clinic_booking and examples/order_status, HttpRunner (signed HTTP agent contract), a mkdocs-material site (quickstart in under 10 lines,
concepts, delivery guarantees from section 3, configuration reference, "what wagent cannot fix" from section 14), docs/REAL_TEST.md
(step-by-step: create a Meta developer app, use Meta's test number, tunnel with cloudflared or ngrok, register the callback URL, send a message),
README with a 10-line quickstart that is executed in CI, and a release workflow using PyPI trusted publishing for tag v0.1.0.
Acceptance: a fresh virtualenv can `pip install` the built wheel, run `wagent dev`, chat with the example agent; docs build without warnings;
the README quickstart test passes; CI is green on 3.11 and 3.12.
```

After M6, **you** (not Codex) run the real-WhatsApp test from `docs/REAL_TEST.md` end to end before tagging v0.1.0.

---

## 12. Phase 2: hosted version (do not start until section 13's check passes)

Goal: developers who do not want to run a webhook server pay to have it hosted.

- **Separate private repo** (`wagent-cloud`). The OSS library stays fully usable on its own.
- **Tenant model:** each customer creates their own Meta app, so Meta calls *our* URL `https://hook.<domain>/t/{tenant_id}` and the customer gives us their token and app secret. Tokens are stored encrypted (envelope encryption). **VERIFY** Meta's Tech Provider / Solution Partner requirements before storing other businesses' credentials.
- **Agent hosting:** do **not** run customer code at first. The cloud gateway forwards each turn to the customer's HTTPS endpoint using the same signed request/response contract as `HttpRunner`. This keeps hosting cost and security risk low.
- **Inbox:** small FastAPI + Jinja + HTMX app for human takeover (list conversations, read, reply, hand back to the bot). Magic-link login.
- **Billing:** subscription per WhatsApp number, via Razorpay or Stripe. Price hypotheses to test, not facts: free self-host; a low monthly price per number for hosted webhook + inbox.
- **Do not build:** a no-code flow builder, a template manager, or broadcast campaigns. Those are what existing WhatsApp platforms sell, and they are the crowded part.

---

## 13. Launch checklist and kill rule

1. Check the name on PyPI and GitHub; rename if taken.
2. README: one GIF of `wagent dev`, the 10-line quickstart, an honest "what it cannot fix" section.
3. Tag `v0.1.0` and publish to PyPI.
4. One launch post (Show HN or the LangChain forum), written plainly: the problem, the 10 lines, the limits.
5. Open 5 "good first issue" tickets (more runners, more message types such as voice-note transcription).
6. **Kill rule:** six weeks after launch, if fewer than 3 people have asked for a hosted version or opened substantive issues, do not build phase 2. Keep the library as a portfolio piece and for your own client work.

---

## 14. What wagent cannot fix (put this in the docs)

- Meta business verification, display-name approval and template approval are done in Meta's dashboard, and approval can be slow or rejected.
- Per-message charges are set by Meta and now apply to free-form replies in the window as well; wagent only helps you send fewer messages and see what was billed.
- LLM quality, prompts and tools are yours. wagent does not make an agent smart; it makes it reliable on WhatsApp.
- Compliance (consent, privacy law, Meta's messaging policies) remains the operator's responsibility.