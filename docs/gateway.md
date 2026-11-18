# Gateway and durable inbox (M1)

## Start locally

Install dependencies with `uv sync --all-extras`. Copy `.env.example` to `.env`
and set `WA_PHONE_ID`, `WA_APP_SECRET`, and `WA_VERIFY_TOKEN` to your own values.
Tokens and app secrets must stay outside source control. Only those three
settings are required for an ingestion-only gateway. `WA_TOKEN` is additionally
required when enabling a runner in M2. This ingestion-only application makes no
Graph API calls or automatic webhook registration requests.

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
per deployment; application tables are never created by startup. Alembic can also emit SQL with
`uv run alembic upgrade head --sql` or remove the application tables with
`uv run alembic downgrade base` (which deletes application tables and their data).

## HTTP routes

- `GET /webhook`: requires `hub.mode=subscribe`, matching `hub.verify_token`,
  and `hub.challenge`. Returns only the challenge as plain text; bad token/mode
  returns 403, missing challenge 400.
- `POST /webhook`: verifies `X-Hub-Signature-256` against the raw body before
  JSON parsing. Missing, malformed, or mismatched signatures return 401.
  Invalid JSON returns 400. Known messages are committed before returning 200;
  database failures return 503 so Meta can retry.
- `GET /healthz`: process liveness, independent of the database.
- `GET /readyz`: verifies database connectivity and all required application tables; returns
  503 if the schema has not been migrated or the database is unavailable.

Set `WEBHOOK_PATH` to change the webhook route. This instance only accepts
messages whose metadata matches `WA_PHONE_ID`. Unknown event types and malformed
individual messages are ignored with constant DEBUG messages containing no
payloads. Future message kinds with valid IDs/senders/timestamps are stored as
`other`. M3 also persists status receipts and pricing; see [sending and usage](sending.md).

## Local signed request

With the server running and test configuration values in `.env`, use the payload
helper from another Python process. With the ingestion-only app above this
does not contact Meta. An app configured with a runner can send a real reply;
use the [offline tests](worker.md) to test the whole pipeline without Meta:

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
offline send tests and captures authenticated requests in `requests`. Its contract
is intentionally a minimal stand-in, not a complete emulation of Meta's API.

## Delivery guarantees

M1 persists incoming messages once by WhatsApp message ID, with all messages in
a request committed atomically. Acknowledgment means durable ingestion, not that
an agent ran or replied. Database uniqueness handles retries and concurrent
duplicate deliveries across restarts. Messages retain their WhatsApp timestamps
and receipt timestamps; `last_inbound_at` can only advance.

The M2 worker processes each conversation in timestamp order. Late arrivals
cannot retroactively precede already completed turns. Agent turns are
at-least-once after crashes; deterministic user-message IDs prevent duplicate
user history. Agent tools may still run again. The outbox suppresses resends of
sending/sent/delivered/read/unknown rows. M3 resolves ambiguous sends through matching status receipts; manual admin
resolution arrives in a later milestone.
No global exactly-once delivery guarantee is claimed.

The ingestion-only mode described here does not start a worker. M2 now adds
agent execution and outbound text: see [the worker guide](worker.md). Admin
management remains M5 scope; M3 adds protected usage and M4 adds conversation erasure.
Operators remain responsible for consent, data access, and Meta account setup.
M4 adds [retention and erasure workflows](controls.md).

## Validation

Run `uv run pytest -q` for the offline SQLite suite. The live PostgreSQL test
requires `WAGENT_TEST_POSTGRES_URL` pointing to a dedicated, initially empty test
database. It migrates that database and leaves test rows there. Without this
variable the PostgreSQL test is skipped. CI provisions a disposable PostgreSQL
service for it on Python 3.11 and 3.12.
