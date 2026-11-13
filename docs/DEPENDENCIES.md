# M0 dependency baseline

Checked against PyPI JSON metadata on 2026-11-13, with TLS verification using
macOS's certificate bundle. Lower bounds in pyproject.toml reflect the current
releases of the five libraries required by M0:

| Package | Release | Source |
| --- | --- | --- |
| pywa | 4.5.0 | https://pypi.org/pypi/pywa/json |
| langgraph | 1.2.21 | https://pypi.org/pypi/langgraph/json |
| langchain | 1.5.0 | https://pypi.org/pypi/langchain/json |
| fastapi | 0.162.5 | https://pypi.org/pypi/fastapi/json |
| sqlalchemy | 2.1.8 | https://pypi.org/pypi/sqlalchemy/json |

Other dependency lower bounds are initial compatibility requirements, not claims
about their latest releases. uv.lock records the resolved development versions.
API behavior VERIFY items remain for the milestone that introduces each behavior.
VERIFIED.md and DECISIONS.md are intentionally empty for M0.
Release publishing configuration is deferred to M6.
