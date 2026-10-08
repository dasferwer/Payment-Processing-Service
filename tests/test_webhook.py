from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest

from app.models import Payment
from app.processing import WebhookError, WebhookSender
from app.security import WebhookPolicy


async def public_resolver(host, port):
    return ["8.8.8.8"]


@pytest.mark.parametrize("status", [200, 201, 204, 299, 301, 400, 500])
async def test_webhook_contract_and_success_criterion(status):
    payment = Payment(
        id=uuid4(),
        status="failed",
        amount=Decimal("12.30"),
        currency="EUR",
        processed_at=datetime(2026, 10, 8, tzinfo=UTC),
        extra_metadata={"order_id": 1},
        webhook_url="https://example.org/webhook",
    )
    received = []

    def receiver(request):
        import json

        received.append(json.loads(request.content))
        assert request.method == "POST"
        assert request.headers["X-Webhook-Id"] == str(payment.id)
        return httpx.Response(status, headers={"Location": "https://other.example.org"})

    sender = WebhookSender(
        transport=httpx.MockTransport(receiver), policy=WebhookPolicy(resolver=public_resolver)
    )
    if status < 300:
        await sender(payment)
    else:
        with pytest.raises(WebhookError):
            await sender(payment)
    assert received == [
        {
            "payment_id": str(payment.id),
            "status": "failed",
            "amount": "12.30",
            "currency": "EUR",
            "processed_at": "2026-10-08T00:00:00+00:00",
            "metadata": {"order_id": 1},
        }
    ]


async def test_webhook_timeout_does_not_expose_url():
    payment = Payment(
        id=uuid4(),
        status="succeeded",
        amount=Decimal("1.00"),
        currency="RUB",
        processed_at=datetime.now(UTC),
        extra_metadata={},
        webhook_url="https://example.org/secret-token",
    )

    def timeout(request):
        raise httpx.ReadTimeout("secret-token", request=request)

    with pytest.raises(WebhookError, match="^ReadTimeout$"):
        await WebhookSender(
            transport=httpx.MockTransport(timeout),
            policy=WebhookPolicy(resolver=public_resolver),
        )(payment)
