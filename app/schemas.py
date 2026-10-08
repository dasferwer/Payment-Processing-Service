import json
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, JsonValue, model_validator


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)
    amount: Annotated[
        Decimal, Field(ge=Decimal("0.01"), max_digits=18, decimal_places=2, allow_inf_nan=False)
    ]
    currency: Literal["RUB", "USD", "EUR"]
    description: str = Field(max_length=1000)
    metadata: dict[str, JsonValue]
    webhook_url: Annotated[HttpUrl, Field(max_length=2048)]

    @model_validator(mode="before")
    @classmethod
    def bounded_json(cls, data):
        stack = [(data, 0)]
        nodes = 0
        while stack:
            value, depth = stack.pop()
            nodes += 1
            if depth > 32 or nodes > 4096:
                raise ValueError("JSON слишком сложный")
            if isinstance(value, str):
                if "\x00" in value:
                    raise ValueError("Нулевой символ запрещён")
                try:
                    value.encode("utf-8")
                except UnicodeEncodeError as exc:
                    raise ValueError("Некорректный Unicode") from exc
            elif isinstance(value, dict):
                stack.extend((item, depth + 1) for pair in value.items() for item in pair)
            elif isinstance(value, list):
                stack.extend((item, depth + 1) for item in value)
        if isinstance(data, dict) and isinstance(data.get("webhook_url"), str):
            raw_url = data["webhook_url"]
            if "\\" in raw_url or any(ord(char) < 32 for char in raw_url):
                raise ValueError("Недопустимые символы webhook URL")
        return data

    @model_validator(mode="after")
    def validate_postgres_text(self):
        # PostgreSQL не принимает NUL и некорректные Unicode-последовательности.
        data = self.model_dump(mode="json")

        def check_text(value):
            if isinstance(value, str):
                if "\x00" in value:
                    raise ValueError("Нулевой символ запрещён")
                value.encode("utf-8")
            elif isinstance(value, dict):
                for key, item in value.items():
                    check_text(key)
                    check_text(item)
            elif isinstance(value, list):
                for item in value:
                    check_text(item)

        try:
            check_text(data)
            json.dumps(data, ensure_ascii=False, allow_nan=False)
        except UnicodeEncodeError as exc:
            raise ValueError("Некорректный Unicode") from exc
        self.amount = self.amount.quantize(Decimal("0.01"))
        return self


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: Literal["pending", "succeeded", "failed"]
    created_at: datetime


class PaymentDetails(PaymentAccepted):
    idempotency_key: str
    amount: Decimal
    currency: str
    description: str
    metadata: dict[str, JsonValue]
    webhook_url: str
    processed_at: datetime | None
    webhook_attempts: int
    webhook_delivered_at: datetime | None
