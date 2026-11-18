# Changelog

## Unreleased

- Add M4 durable debounce, rolling turn admission limits, daily send reservations,
  global pause, one-time opt-out confirmation, raw retention, salted sender logs,
  and retryable conversation/checkpoint erasure through admin DELETE and CLI purge.

- Add M3 send-time window/template policy, ordered long-text chunks, safe durable
  send retries, status receipt reconciliation, pricing storage, and protected usage API.

- Add M2 background turns with leases and durable retries, EchoRunner and
  LangGraphRunner, SQLite/PostgreSQL checkpoints, and a conservative text outbox.
- Add a fake-model clinic example and end-to-end, memory, and process-crash tests.

- Add M1 environment settings, signed webhook gateway, transactional inbox,
  Alembic migration, health/readiness routes, and offline testing helpers.

- Add the M0 package scaffold, version CLI, development tooling, and CI.
