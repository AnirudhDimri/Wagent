# wagent

A Python library for connecting LangGraph agents to WhatsApp Cloud API.
MIT licensed; Python 3.11+.

**Status:** M4. Webhooks, persistent agent turns, safe text/template sends,
status/usage tracking, batching, limits, pause, opt-out, retention, and erasure
are implemented. Start with [the worker guide](docs/worker.md), then
[sending and usage](docs/sending.md) and [controls and erasure](docs/controls.md).
Human takeover and the local simulator remain later milestones.

## Development

Install [uv](https://docs.astral.sh/uv/getting-started/installation/), then run:

```sh
uv sync --all-extras
uv run wagent --version
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest -q
```

uv uses `.venv`; the existing `ve` directory is not required or modified by sync.
The `postgres`, `dev`, and `docs` extras install optional dependencies.

Install local Git hooks in a Git checkout with:

```sh
uv run pre-commit install
```

Configuration placeholders are in [.env.example](.env.example). Tests run
offline with local secrets and temporary databases. The complete agent
quickstart will arrive in M6.
