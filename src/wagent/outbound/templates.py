"""A pywa template send without mutating or owning the caller's HTTP client."""

import logging
from contextvars import ContextVar

import httpx
from pywa.types.templates import TemplateLanguage
from pywa_async.client import WhatsApp

from wagent.settings import Settings

_inside_template_call: ContextVar[bool] = ContextVar("wagent_template_call", default=False)


class _SDKPrivacyFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        # The SDK logs full request/response bodies at DEBUG, including recipients.
        return not (_inside_template_call.get() and record.levelno <= logging.DEBUG)


logging.getLogger("pywa.api").addFilter(_SDKPrivacyFilter())


class ForwardTransport(httpx.AsyncBaseTransport):
    def __init__(self, client: httpx.AsyncClient) -> None:
        self.client = client

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        response = await self.client.send(request, stream=True, follow_redirects=False, auth=None)
        if not response.is_success:
            await response.aread()
            await response.aclose()
            raise httpx.HTTPStatusError(
                "Template HTTP rejection", request=request, response=response
            )
        return response


async def send_template(
    client: httpx.AsyncClient,
    settings: Settings,
    phone_id: str,
    recipient: str,
    name: str,
    language: str,
    tracker: str,
) -> str:
    token = settings.wa_token
    if token is None:
        raise ValueError("WA_TOKEN is required")
    context = _inside_template_call.set(True)
    try:
        async with httpx.AsyncClient(
            transport=ForwardTransport(client), timeout=settings.send_timeout_seconds
        ) as sdk_client:
            wa = WhatsApp(
                phone_id=phone_id,
                token=token.get_secret_value(),
                waba_id=settings.wa_waba_id,
                session=sdk_client,
                api_version=settings.wa_api_version.removeprefix("v"),
            )
            result = await wa.send_template(  # pyright: ignore[reportUnknownMemberType]
                to=recipient, name=name, language=TemplateLanguage(language), tracker=tracker
            )
            return result.id
    finally:
        _inside_template_call.reset(context)
