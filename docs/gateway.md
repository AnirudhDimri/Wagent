# Gateway and durable inbox (M1)

## Start locally

Install dependencies with `uv sync --all-extras`. Copy `.env.example` to `.env`
and set `WA_PHONE_ID`, `WA_APP_SECRET`, and `WA_VERIFY_TOKEN` to your own values.
Tokens and app secrets must stay outside source control. Only those three
settings are required for receiving messages in M1. `WA_TOKEN`, `WA_APP_ID`,
`WA_WABA_ID`, and `WA_CALLBACK_URL` are reserved for later integration steps;
M1 makes no Graph API calls or automatic webhook registration requests.

Create `app.py` in your application directory:

```python
from wagent import Settings, Wagent

app = Wagent(settings=Settings()).fastapi_app
```

From the repository root, apply schema migrations and start the server:

```sh
uv run alembic upgrade head
uv run uvicorn app:app
```

Both Settings and Alembic read `DATABASE_URL` from the environment, falling back
to `.env`. Alembic otherwise uses the default URL in `alembic.ini`. Supported
URLs are `sqlite+aiosqlite:///./wagent.db` and `postgresql+asyncpg://...`.
Use the `postgres` extra for PostgreSQL. Run migrations before serving, once
per deployment; startup never modifies schema. Alembic can also emit SQL with
`uv run alembic upgrade head --sql` or undo M1 with
`uv run alembic downgrade base` (which deletes the inbox tables and their data).

## HTTP routes

- `GET /webhook`: requires `hub.mode=subscribe`, matching `hub.verify_token`,
  and `hub.challenge`. Returns only the challenge as plain text; bad token/mode
  returns 403, missing challenge 400.
- `POST /webhook`: verifies `X-Hub-Signature-256` against the raw body before
  JSON parsing. Missing, malformed, or mismatched signatures return 401.
  Invalid JSON returns 400. Known messages are committed before returning 200;
  database failures return 503 so Meta can retry.
- `GET /healthz`: process liveness, independent of the database.
- `GET /readyz`: verifies database connectivity and both inbox tables; returns
  503 if the schema has not been migrated or the database is unavailable.

Set `WEBHOOK_PATH` to change the webhook route. This instance only accepts
messages whose metadata matches `WA_PHONE_ID`. Unknown event types and malformed
individual messages are ignored with constant DEBUG messages containing no
payloads. Future message kinds with valid IDs/senders/timestamps are stored as
`other`. Status events are acknowledged but not persisted until M3.

## Local signed request

With the server running and test configuration values in `.env`, use the payload
helper from another Python process. This does not contact Meta:

```python
import httpx

from wagent import Settings
from wagent.testing.payloads import message_payload, signed_payload

settings = Settings()
body, headers = signed_payload(
    message_payload("Hello café 🌍", phone_number_id=settings.wa_phone_id),
    settings.wa_app_secret.get_secret_value(),
)
response = httpx.post("http://127.0.0.1:8000/webhook", content=body, headers=headers)
response.raise_for_status()
```

`wagent.testing.fake_graph_api.FakeGraphAPI` supplies an offline ASGI app for
later send tests and captures authenticated requests in `requests`. Its contract
is intentionally a minimal stand-in, not a complete emulation of Meta's API.

## Delivery guarantees

M1 persists incoming messages once by WhatsApp message ID, with all messages in
a request committed atomically. Acknowledgment means durable ingestion, not that
an agent ran or replied. Database uniqueness handles retries and concurrent
duplicate deliveries across restarts. Messages retain their WhatsApp timestamps
and receipt timestamps; `last_inbound_at` can only advance.

The planned worker (M2+) will process each conversation in timestamp order.
Late arrivals cannot retroactively precede already completed turns. Agent turns
will be at-least-once after crashes, using deterministic turn/message IDs to
avoid duplicate history. The planned outbox will suppress resends of rows in
sending/sent/delivered/read/unknown states; ambiguous sends will require status
reconciliation or operator review. These later guarantees are not implemented
in M1. No global exactly-once delivery guarantee is claimed.

No worker, agent execution, outbound sender, or admin endpoints exist in M1.
Operators remain responsible for consent, data access, and Meta account setup.
Retention and erasure workflows arrive in M4.

## Validation

Run `uv run pytest -q` for the offline SQLite suite. The live PostgreSQL test
requires `WAGENT_TEST_POSTGRES_URL` pointing to a dedicated, initially empty test
database. It migrates that database and leaves test rows there. Without this
variable the PostgreSQL test is skipped. CI provisions a disposable PostgreSQL
service for it on Python 3.11 and 3.12.
