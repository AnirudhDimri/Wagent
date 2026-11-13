# Repository instructions

Read `docs/BRIEF.md` before changing this repository. It defines the product,
architecture, delivery guarantees, milestone scope, and acceptance criteria.

- Implement only the milestone requested by the user. Do not start the next
  milestone until the current milestone's acceptance criteria pass.
- Do not begin phase 2 or publish a release without the required checks and
  explicit user instruction. The real WhatsApp release test is operator-run.
- Verify relevant items marked VERIFY against official documentation or installed
  source before coding against them. Record findings and sources in
  `docs/VERIFIED.md`; record API or design deviations in `docs/DECISIONS.md`.
- Use Python 3.11+, a src layout, uv, async SQLAlchemy 2, and the stack specified
  in the brief. Keep the database as the queue; do not introduce Redis or a broker.
- Preserve raw-body webhook signature verification, durable inbound deduplication,
  per-conversation ordering, and the outbox's conservative ambiguous-send policy.
  Never run agent work in the webhook request.
- Keep credentials out of source and examples. Never log raw phone numbers or
  tokens. Use placeholders in `.env.example`.
- Add the behavior tests required for the milestone and run its applicable lint,
  formatting, type, and test checks. Report checks that could not be run honestly.
- Keep v0.1 within the brief's non-goals. Do not add hosted services or a UI.

This file is derived from the supplied build brief; no separate AGENTS.md text
was included with it.
