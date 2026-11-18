# Verified dependencies and APIs

## M1: pywa 4.5.0 (installed source, 2026-11-13)

Inspected `.venv/lib/python3.11/site-packages/pywa/` and `pywa_async/`.
Upstream source: https://github.com/david-lev/pywa
Documentation: https://pywa.readthedocs.io/en/latest/

- `pywa_async.WhatsApp` accepts phone_id, token, app_id, app_secret, waba_id,
  callback_url, verify_token, server, and webhook_endpoint. Its default endpoint
  is `/`, so our `/webhook` must be explicit. Python requirement is >=3.10.
- `WhatsApp.webhook_update_validator(update: bytes, hmac_header: str | None)`
  checks signatures when validate_updates is enabled and app_secret is set.
  In `pywa/server.py` it uses `pywa.utils.webhook_updates_validator`, which
  computes HMAC-SHA256 over the original bytes and uses `hmac.compare_digest`.
  Missing signatures return 401; mismatches return 403. The utility accepts an
  unprefixed digest, so wagent adds strict `sha256=`/64-hex validation and maps
  every invalid signature to 401 before JSON parsing.
- `pywa_async/server.py:webhook_update_handler` awaits callbacks, deduplicates
  whole payload hashes in memory by default, and catches/logs callback errors.
  `Message.from_update` in `pywa/types/message.py` reads only entry[0], change[0],
  messages[0]. Those defaults are unsuitable for a durable multi-event inbox:
  wagent owns FastAPI routes and transactions, uses pywa's public signature
  utility, and explicitly traverses every entry/change/message. Persistence errors
  propagate as retryable HTTP 503; no in-memory dedupe or callbacks hide them.
- Message fields are `id`, UTC `timestamp`, `from_user.wa_id` (not necessarily
  available for username users), `metadata.phone_number_id`, `text`, and media
  objects' `id`. Raw Cloud API messages expose `id`, `timestamp`, `from`, `type`.
  M1 requires a nonempty raw `from`; business-scoped user IDs without a phone ID
  are outside the current sender contract and are ignored without logging data.
- Handler name: `on_message_status`. MessageStatus fields include id, status,
  timestamp, metadata, from_user, pricing, conversation, tracker, error.
  Pricing exposes billable, model, type, category; raw keys include pricing_model
  and type (legacy pricing_type is accepted). Status persistence belongs to M3.
- Mark-as-read: `await wa.mark_message_as_read(message_id, sender=phone_id)`.
  No mark-as-read calls are made in M1.
- pywa's GET handler validates token/challenge but does not take hub.mode;
  wagent also requires hub.mode=subscribe and returns the challenge as plain text.

Meta's exact webhook timeout is not confirmed here. M1 tests the project's own
<200 ms local acknowledgment target; it does not claim a platform timeout.
Other Meta pricing/limits and LangGraph VERIFY items remain for later milestones.

## M2: LangGraph/LangChain (installed source, 2026-11-13)

Inspected langgraph 1.2.21, langchain 1.5.0, langgraph-checkpoint-sqlite 3.1.3,
and langgraph-checkpoint-postgres 3.2.0 using inspect.signature and their installed
source. Upstream: https://github.com/langchain-ai/langgraph and
https://github.com/langchain-ai/langchain.

- `langchain.agents.create_agent(model, tools=None, *, system_prompt=None,
  middleware=(), response_format=None, state_schema=None, context_schema=None,
  checkpointer=None, store=None, interrupt_before=None, interrupt_after=None,
  debug=False, name=None, cache=None, transformers=None)` is available. The
  example uses StateGraph directly with a deterministic fake model; no paid LLM
  provider is required. `create_react_agent` is not used.
- SQLite: `from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver`.
  `async with AsyncSqliteSaver.from_conn_string(path) as saver: await saver.setup()`.
  It takes a SQLite filename, not a SQLAlchemy URL. For SQLite it uses the same
  database file as the inbox (its own checkpoints tables).
- PostgreSQL: `from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver`.
  Same context-manager/setup shape; use a PostgreSQL URI without SQLAlchemy's
  +asyncpg driver suffix. It uses psycopg internally. Its setup is operationally
  separate from Alembic's application tables. Run one setup at deployment/startup
  before adding concurrent worker processes for a newly provisioned database.
- `graph.ainvoke(input, {"configurable": {"thread_id": thread_id}})` supplies
  the conversation identifier on each invocation.
- `langgraph/graph/message.py:add_messages` replaces existing messages by id
  (merged_by_id lookup), preserving other messages. Retrying a HumanMessage with
  the deterministic turn id does not append another copy of that user message.
  This does not make arbitrary agent tools exactly-once; graph work may repeat.
- Both savers expose `await saver.adelete_thread(thread_id)`; compiled graphs
  expose `await graph.aupdate_state(config, values, as_node=None, task_id=None)`.
  Thread erasure and handoff context behaviors remain M4/M5 scope.
- pywa's Graph API defaults to v26.0 in the installed 4.5.0 source. M2 uses an
  explicitly configured v26.0 endpoint and a plain httpx AsyncClient (no automatic
  transport retries). This gives the outbox ownership of sending-before-HTTP and
  error classification. Text envelope matches Meta's maintained example:
  https://www.postman.com/meta/whatsapp-business-platform/request/8gvd47s/send-text-message
  `messaging_product=whatsapp`, `to`, `type=text`, `text.body`, Bearer token.
  Uncertain HTTP/network outcomes are never retried automatically in M2.

## M3: sending, statuses, pricing (2026-11-14)

- Text maximum: 4096 characters, confirmed by pywa 4.5.0's official client
  reference, send_message.text:
  https://pywa.readthedocs.io/en/latest/content/client/client_reference.html
  Meta's text-reference pages returned HTTP 429 during this check, so the
  current maintained pywa reference is the usable primary source for this limit.
  pywa's installed callback.py also documents three reply buttons and ten total
  list rows; interactive sending is not part of M3.
- Customer service window: Meta's public pricing page says each inbound user
  message opens/resets a 24-hour service window:
  https://whatsappbusiness.com/products/platform-pricing/
  This page currently says service messages and responding utility messages are
  free. The brief's claimed 2026-10-01 change / 1000-message allowance was NOT
  confirmed and conflicts with this public primary source. Do not encode it as
  fact. Usage stores reported facts only; absent billable remains unknown.
- Installed pywa 4.5.0: on_message_status dispatches MessageStatus. Its pricing
  object reads raw billable, pricing_model, category, and type (legacy
  pricing_type accepted). Its Python model calls pricing_model 'model' and
  infers billable if absent; wagent deliberately preserves absent billable as
  null. Raw pricing JSON is retained without discarding unfamiliar fields.
- `await pywa_async.WhatsApp.send_template(to, name, language, params=None,
  tracker=...)` sends a single Graph POST, no hidden lookup or retries. The
  constructor supports waba_id; sending by known name/language does not itself
  require a WABA lookup, but wagent requires WA_WABA_ID for template policy as
  specified in the brief. M3 fallback supports approved templates with no params.
  `tracker` maps to biz_opaque_callback_data; text sends include this too, so an
  unknown send can be matched to the outbox UUID even without its returned wamid.
  https://pywa.readthedocs.io/en/latest/content/templates/overview.html
- pywa.errors.WhatsAppError exposes code and raw_response (httpx.Response), so
  Retry-After survives SDK errors. pywa's API mutates the supplied client's
  headers/base_url; wagent creates a short-lived separate SDK client sharing only
  a non-owning transport adapter to the configured HTTP client.
- Safe retry list: pre-write ConnectError/ConnectTimeout/PoolTimeout; HTTP 429;
  and explicit synchronous rejection codes 4, 80007, 130429, 131056. Meta's
  throttling section recommends waiting/retrying those operations:
  https://developers.facebook.com/documentation/business-messaging/whatsapp/support/error-codes
  Generic 1/2/131000/131016 server/transient errors are not proof of nonacceptance;
  treat them conservatively as unknown, with no retry. Do not auto-retry failed
  delivery webhooks. Honor numeric or HTTP-date Retry-After on allowed retries.

## M4: checkpoint erasure (installed source, 2026-11-14)

Inspected AsyncSqliteSaver.adelete_thread and AsyncPostgresSaver.adelete_thread.
SQLite deletes checkpoints and writes by thread_id, then commits. PostgreSQL
removes checkpoints, checkpoint_blobs, and checkpoint_writes by thread_id.
Both are public async methods and are idempotent for an absent thread. Source:
https://github.com/langchain-ai/langgraph/tree/main/libs/checkpoint-sqlite
https://github.com/langchain-ai/langgraph/tree/main/libs/checkpoint-postgres

Checkpoint deletion uses a separate connection/transaction from application rows.
M4 therefore fences erasure with a durable conversation flag, rejects active work,
and leaves that flag set if checkpoint erasure fails so a retry can finish safely.
