"""Environment loading and safe configuration validation."""

import pytest
from pydantic import ValidationError

from wagent import Settings


def test_environment_loading(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WA_PHONE_ID", "123")
    monkeypatch.setenv("WA_APP_SECRET", "secret-do-not-log")
    monkeypatch.setenv("WA_VERIFY_TOKEN", "verify-do-not-log")
    settings = Settings()  # pyright: ignore[reportCallIssue]
    assert settings.wa_phone_id == "123"
    assert settings.wa_app_secret.get_secret_value() == "secret-do-not-log"
    assert "secret-do-not-log" not in repr(settings)
    assert "verify-do-not-log" not in repr(settings)


@pytest.mark.parametrize(
    "updates",
    [
        {"wa_app_secret": ""},
        {"wa_verify_token": " "},
        {"wa_phone_id": ""},
        {"database_url": "sqlite:///sync.db"},
        {"webhook_path": "relative"},
        {"webhook_path": "/healthz"},
        {"webhook_path": "/readyz/"},
    ],
)
def test_invalid_settings(settings: Settings, updates: dict[str, str]) -> None:
    values = settings.model_dump()
    values.update(updates)
    with pytest.raises(ValidationError):
        Settings.model_validate(values)
