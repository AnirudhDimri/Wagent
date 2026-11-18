"""Run from the repo root: uvicorn examples.clinic_booking.app:app."""

from wagent import Settings, Wagent
from wagent.runners import LangGraphRunner

from .agent import build_graph

app = Wagent(
    settings=Settings(),  # pyright: ignore[reportCallIssue]
    runner=LangGraphRunner(build_graph),
).fastapi_app
