from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", hide_input_in_errors=True)

    database_url: SecretStr = (
        "postgresql+asyncpg://payments:payments-local@localhost:55432/payments"
    )
    rabbitmq_url: SecretStr = "amqp://payments:payments-local@localhost:55672/"
    api_key: SecretStr = Field(min_length=1, max_length=256)
    allowed_webhook_origins: list[str] = Field(default_factory=list)
    max_request_bytes: int = Field(default=65536, ge=1024, le=1048576)
    retry_base_seconds: float = Field(default=2, gt=0, le=3600)
    outbox_poll_seconds: float = Field(default=0.5, gt=0, le=60)
    webhook_timeout_seconds: float = Field(default=5, gt=0, le=30)
