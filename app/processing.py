import asyncio
import random
from collections.abc import Awaitable, Callable
from datetime import timedelta
from uuid import UUID

import httpx
from sqlalchemy import select

from app.models import Outbox, Payment, utcnow
from app.security import WebhookPolicy, WebhookPolicyError


class GatewayError(Exception):
    """Техническая ошибка шлюза, допускающая повтор."""


class WebhookError(Exception):
    """Уведомление не подтверждено получателем."""


class InvalidEvent(Exception):
    """Сообщение не соответствует сохранённому событию."""


async def emulate_gateway(payment: Payment) -> bool:
    await asyncio.sleep(random.uniform(2, 5))
    return random.random() >= 0.1


def webhook_payload(payment: Payment) -> dict:
    return {
        "payment_id": str(payment.id),
        "status": payment.status,
        "amount": str(payment.amount),
        "currency": payment.currency,
        "processed_at": payment.processed_at.isoformat(),
        "metadata": payment.extra_metadata,
    }


class WebhookSender:
    def __init__(self, transport=None, policy: WebhookPolicy | None = None, timeout_seconds=5):
        self.transport = transport
        self.policy = policy or WebhookPolicy()
        self.timeout_seconds = timeout_seconds

    async def __call__(self, payment: Payment):
        try:
            original, pinned = await self.policy.pinned_url(payment.webhook_url)
            # Отдельный клиент не переносит cookies между платежами и адресатами.
            async with httpx.AsyncClient(
                transport=self.transport,
                timeout=self.timeout_seconds,
                trust_env=False,
                limits=httpx.Limits(max_connections=1, max_keepalive_connections=0),
            ) as client:
                request = client.build_request(
                    "POST",
                    pinned,
                    json=webhook_payload(payment),
                    headers={
                        "X-Webhook-Id": str(payment.id),
                        "Host": original.netloc.decode("ascii"),
                    },
                )
                request.extensions["sni_hostname"] = original.host
                response = await client.send(request, stream=True, follow_redirects=False)
                try:
                    if not 200 <= response.status_code < 300:
                        raise WebhookError(f"HTTP {response.status_code}")
                finally:
                    # Успех определяется статусом: чужое тело ответа не загружаем.
                    await response.aclose()
        except WebhookPolicyError as exc:
            raise WebhookError(str(exc)) from exc
        except httpx.HTTPError as exc:
            raise WebhookError(type(exc).__name__) from exc


class Processor:
    def __init__(
        self,
        sessions,
        gateway: Callable[[Payment], Awaitable[bool]],
        webhook: Callable[[Payment], Awaitable[None]],
        retry_base: float = 2,
    ):
        self.sessions = sessions
        self.gateway = gateway
        self.webhook = webhook
        self.retry_base = retry_base

    async def _lock(self, session, event_id: UUID):
        # Во всех этапах порядок одинаков: событие, затем платёж.
        event = await session.scalar(select(Outbox).where(Outbox.id == event_id).with_for_update())
        if event is None or event.topic != "payments.new":
            raise InvalidEvent("Неизвестное событие")
        if event.completed_at is not None:
            return event, None
        payment = await session.scalar(
            select(Payment).where(Payment.id == event.payment_id).with_for_update()
        )
        if payment is None:
            raise InvalidEvent("Платёж не найден")
        return event, payment

    def _complete(self, session, event, payment, stage=None):
        attempts = event.payload.get("attempt", 1)
        now = utcnow()
        event.completed_at = now
        if stage is not None and attempts < 3:
            session.add(
                Outbox(
                    payment_id=payment.id,
                    payload={"attempt": attempts + 1},
                    available_at=now + timedelta(seconds=self.retry_base * 2 ** (attempts - 1)),
                )
            )
        elif stage is not None:
            # DLQ тоже проходит через outbox: недоступный RabbitMQ не теряет диагностику.
            session.add(
                Outbox(
                    payment_id=payment.id,
                    topic="payments.dlq",
                    payload={
                        "stage": stage,
                        "attempt": attempts,
                        "status": payment.status,
                        "processing_attempts": payment.processing_attempts,
                        "webhook_attempts": payment.webhook_attempts,
                        "processing_error": payment.processing_error,
                        "webhook_error": payment.webhook_error,
                    },
                )
            )

    async def handle(self, event_id: UUID):
        async with self.sessions.begin() as session:
            event, payment = await self._lock(session, event_id)
            if payment is None:
                return
            if payment.status == "pending":
                payment.processing_attempts += 1
                try:
                    async with asyncio.timeout(10):
                        succeeded = await self.gateway(payment)
                except (GatewayError, TimeoutError) as exc:
                    payment.processing_error = str(exc)[:1000] or type(exc).__name__
                    if payment.processing_attempts < 3:
                        self._complete(session, event, payment, "gateway")
                        return
                    payment.status = "failed"
                else:
                    payment.status = "succeeded" if succeeded else "failed"
                    payment.processing_error = None
                payment.processed_at = utcnow()
        # Здесь результат уже зафиксирован. Ошибка или отмена webhook его не откатывает.
        async with self.sessions.begin() as session:
            event, payment = await self._lock(session, event_id)
            if payment is None:
                return
            if payment.webhook_delivered_at is None and payment.webhook_attempts < 3:
                payment.webhook_attempts += 1
                try:
                    async with asyncio.timeout(35):
                        await self.webhook(payment)
                except (WebhookError, TimeoutError) as exc:
                    payment.webhook_error = str(exc)[:1000] or type(exc).__name__
                    self._complete(session, event, payment, "webhook")
                    return
                payment.webhook_delivered_at = utcnow()
                payment.webhook_error = None
            if payment.processing_error is not None:
                self._complete(session, event, payment, "gateway")
            else:
                self._complete(session, event, payment)
