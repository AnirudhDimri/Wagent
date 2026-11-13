"""Environment-backed gateway configuration."""

from typing import Self

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError


class Settings(BaseSettings):
    """Read WA_* variables and DATABASE_URL from the environment or .env."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore", hide_input_in_errors=True)

    wa_phone_id: str = Field(min_length=1)
    wa_app_secret: SecretStr
    wa_verify_token: SecretStr
    wa_token: SecretStr | None = None
    wa_app_id: str | None = None
    wa_waba_id: str | None = None
    wa_callback_url: str | None = None
    database_url: str = Field(default="sqlite+aiosqlite:///./wagent.db", repr=False)
    webhook_path: str = "/webhook"
    admin_api_key: SecretStr | None = None
    log_salt: SecretStr | None = None

    @field_validator("wa_app_secret", "wa_verify_token")
    @classmethod
    def nonempty_secret(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value().strip():
            raise ValueError("must not be empty")
        return value

    @field_validator("database_url")
    @classmethod
    def async_database(cls, value: str) -> str:
        try:
            driver = make_url(value).drivername
        except ArgumentError:
            raise ValueError("invalid database URL") from None
        if driver not in {"sqlite+aiosqlite", "postgresql+asyncpg"}:
            raise ValueError("use sqlite+aiosqlite or postgresql+asyncpg")
        return value

    @model_validator(mode="after")
    def valid_path(self) -> Self:
        if (
            not self.webhook_path.startswith("/")
            or "?" in self.webhook_path
            or "#" in self.webhook_path
            or "{" in self.webhook_path
            or "}" in self.webhook_path
            or self.webhook_path.rstrip("/") in {"/healthz", "/readyz"}
        ):
            raise ValueError(
                "WEBHOOK_PATH must be a literal absolute path distinct from health routes"
            )
        return self
