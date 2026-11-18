# Sending rules, delivery status, and usage (M3)

M3 adds decisions around sending a reply: whether ordinary text is allowed,
which approved template to use otherwise, whether a failed request is safe to
retry, and what Meta later reports about delivery and pricing.

## Update and test

Apply the new migration before starting the application:

```sh
uv run alembic upgrade head
```

Use `./ve/bin/uv` in place of `uv` if that is where uv is installed on your machine.
The migration adds usage, message_status_events, audit_events, and the outbound
retry timestamp. Existing conversations, messages, turns, and replies are kept.
LangGraph checkpoint tables are managed separately by the runner as before.

Try M3 offline, without a Meta account or real sends:

```sh
uv run pytest -q tests/integration/test_window_templates.py
uv run pytest -q tests/integration/test_statuses.py
uv run pytest -q tests/integration/test_send_retries.py tests/integration/test_usage.py
```

## The 24-hour window

The most recent inbound user message opens the window for 24 hours. The sender
checks it again immediately before preparing each attempt, including retries.
At exactly 24 hours the window is closed. A delivery/status receipt does not
extend it. Out-of-order inbound messages cannot move the latest inbound time
backwards.

Inside the window, an agent's text reply is sent as ordinary text. Outside:

- `OUT_OF_WINDOW_POLICY=drop` (default): no API call. The outbound row becomes
  failed with error_code=window_closed, and an outbound_drop audit records why.
- `OUT_OF_WINDOW_POLICY=template`: send the configured approved template through
  pywa. The AI reply is not inserted into the template. M3 supports templates
  with no required parameters.

For template fallback, set these alongside WA_TOKEN and WA_PHONE_ID:

```dotenv
OUT_OF_WINDOW_POLICY=template
WA_WABA_ID=your-business-account-id
FALLBACK_TEMPLATE_NAME=your_approved_template_name
FALLBACK_TEMPLATE_LANG=en_US
```

wagent does not create or approve templates. The template must already exist in
Meta with that name/language. WA_WABA_ID is required by our configuration, though
pywa sends a known template name directly without a WABA lookup. Templates are
blocked for opted-out conversations; M4 adds [keyword-driven opt-out processing](controls.md).

The outbox records template conversion before the network request. If a long
reply has several pending chunks when the window closes, one fallback template
replaces those pending chunks; the rest become failed/template_coalesced. It
does not send one fallback template per chunk.

## Long replies

Text is split into chunks no longer than 4096 Unicode characters, preferring
paragraph boundaries and preserving all text. Each chunk has its own stable
`{turn_id}:{index}` dedupe key. Chunks send in order, with no filler messages.
The original reply still uses one agent invocation.

## Safe retries and uncertain sends

Every attempt first commits the outbox row as sending. M3 retries only:

- Connection/pool failures known to occur before a request was written.
- HTTP 429 rejection responses.
- Explicit synchronous 4xx rejection codes 4, 80007, 130429, and 131056.

Retries are stored in outbound_messages.next_attempt_at, survive restarts, and
use exponential backoff. Retry-After can be seconds or an HTTP date; the sender
never retries before the later of that deadline and its normal backoff. A
backed-off reply blocks newer queued sends in the same conversation, while
other conversations can proceed.

`SEND_MAX_ATTEMPTS=3` limits send attempts separately from MAX_ATTEMPTS, which
limits agent attempts. RETRY_BACKOFF_SECONDS controls the initial delay for both;
normal backoff caps at 300 seconds, but a longer Retry-After is honored.

Read/write timeouts, connection resets after writing, generic server/transient
errors, and successful responses without a usable message ID become unknown.
They are never automatically resent. Generic codes 1, 2, 131000, and 131016 are
not treated as proof of nonacceptance. A crashed sending row becomes unknown
after LEASE_SECONDS. Failed delivery webhooks also never trigger resend.

Text and template calls share these rules. The pywa adapter does not mutate or
close the application's HTTP client, does not follow redirects, and suppresses
SDK request/response DEBUG bodies only during our template calls so recipient
numbers are not logged. Injected clients must not implement automatic retries.

## Delivery statuses

Meta posts sent, delivered, read, and failed receipts to the same signed webhook.
wagent stores each distinct receipt durably before acknowledging it.

- Duplicate receipts do not create additional usage rows.
- Late sent/delivered receipts cannot downgrade read.
- Failure codes are recorded; delivered/read evidence takes precedence over
  contradictory failure/sent receipts. The receipt history retains both events.
- New sends include the outbox UUID as biz_opaque_callback_data. Meta can echo it
  back, allowing a status to identify an unknown send whose HTTP response was lost.
- Receipts without a matching outbox row are retained unapplied. If a receipt
  races ahead of the HTTP response, binding the returned message ID replays it.
  A later tracked receipt can also reconcile earlier unmatched pricing receipts.

A returned send ID means API acceptance. Delivered/read require status receipts;
they are not inferred from a successful send request. Operators can inspect
unknown rows directly in the database until later admin endpoints are added.

## Usage endpoint

Set a nonempty ADMIN_API_KEY to enable GET /admin/usage. Without a key the route
is absent (404); enabled routes require the matching Bearer token (401 otherwise).
M4 also adds authenticated DELETE /admin/conversations/{id}; other admin
workflows remain for M5.

Example request header: `Authorization: Bearer <your-admin-key>`.
Example path: `/admin/usage?from=2026-11-14&to=2026-11-15`.

The response shape is:

```json
{
  "items": [
    {"day": "2026-11-14", "category": "utility", "billable": false, "count": 2}
  ]
}
```

Bounds accept YYYY-MM-DD (UTC midnight) or timezone-aware ISO datetimes. `from`
is inclusive; `to` is exclusive. With neither bound, all recorded usage is
returned. Days use UTC regardless of the PostgreSQL session timezone.

Usage counts one row per outbound message with reported pricing, grouped by day,
category, and billable. The day is the earliest reported pricing-receipt timestamp
for that message. It is not a delivery ledger or invoice. Missing billable or
category stays null, rather than being inferred from message type or window.
The usage table retains raw pricing fields; receipt history retains individual
pricing reports. No currency amounts are calculated.

The brief's claimed October 2026 pricing change was not confirmed: Meta's current
public pricing page says service replies are free. See [VERIFIED.md](VERIFIED.md)
for the primary source and discrepancy. The library follows reported pricing
facts rather than encoding that claim or a fixed rate table.
