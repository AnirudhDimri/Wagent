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
