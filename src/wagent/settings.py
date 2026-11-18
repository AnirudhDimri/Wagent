"""Environment-backed gateway configuration."""

from typing import Literal, Self

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

    lease_seconds: float = Field(default=120, gt=0)
    runner_timeout_seconds: float = Field(default=60, gt=0)
    send_timeout_seconds: float = Field(default=20, gt=0)
    max_attempts: int = Field(default=3, ge=1)
    retry_backoff_seconds: float = Field(default=1, gt=0)
    worker_poll_seconds: float = Field(default=0.1, gt=0)
    worker_concurrency: int = Field(default=8, ge=1)
    wa_api_version: str = "v26.0"
    out_of_window_policy: Literal["drop", "template"] = "drop"
    fallback_template_name: str | None = None
    fallback_template_lang: str = "en_US"
    debounce_ms: int = Field(default=1500, ge=0)
    max_debounce_ms: int = Field(default=5000, ge=0)
    rate_limit_turns_per_min: int = Field(default=6, ge=1)
    daily_outbound_cap_per_user: int = Field(default=50, ge=0)
    pause_all: bool = False
    opt_out_keywords: str = "STOP,UNSUBSCRIBE"
    retention_days: int = Field(default=30, ge=0)
    retention_interval_seconds: float = Field(default=3600, gt=0)
    send_max_attempts: int = Field(default=3, ge=1)

    @model_validator(mode="after")
    def valid_debounce(self) -> Self:
        if self.max_debounce_ms < self.debounce_ms:
            raise ValueError("MAX_DEBOUNCE_MS must be at least DEBOUNCE_MS")
        return self

    @model_validator(mode="after")
    def template_configuration(self) -> Self:
        if self.out_of_window_policy == "template" and not (
            self.wa_waba_id
            and self.wa_waba_id.strip()
            and self.fallback_template_name
            and self.fallback_template_name.strip()
            and self.fallback_template_lang.strip()
        ):
            raise ValueError("template policy requires WA_WABA_ID and FALLBACK_TEMPLATE_NAME/LANG")
        return self

    @field_validator("wa_api_version")
    @classmethod
    def api_version(cls, value: str) -> str:
        import re

        if not re.fullmatch(r"v[0-9]+\.[0-9]+", value):
            raise ValueError("WA_API_VERSION must look like v26.0")
        return value

    @model_validator(mode="after")
    def lease_covers_work(self) -> Self:
        if self.lease_seconds <= max(self.runner_timeout_seconds, self.send_timeout_seconds):
            raise ValueError("LEASE_SECONDS must exceed runner and send timeouts")
        return self

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
