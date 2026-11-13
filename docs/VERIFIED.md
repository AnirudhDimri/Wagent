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
