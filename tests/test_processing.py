import asyncio
from datetime import timedelta

import pytest
from sqlalchemy import select

from app.models import Outbox, Payment, utcnow
from app.processing import GatewayError, Processor, WebhookError
from app.publisher import Publisher


async def seed(client, body, sessions):
    response = await client.post(
        "/api/v1/payments", json=body, headers={"Idempotency-Key": "process"}
    )
    async with sessions() as session:
        event = await session.scalar(select(Outbox))
    return response.json()["payment_id"], event.id


async def success(payment):
    return True


async def notify(payment):
    return None


async def snapshot(sessions):
    async with sessions() as session:
        payment = await session.scalar(select(Payment))
        events = list((await session.scalars(select(Outbox).order_by(Outbox.created_at))).all())
    return payment, events


async def test_success_duplicate_message_does_not_process_or_notify(client, body, sessions):
    _, event_id = await seed(client, body, sessions)
    processor = Processor(sessions, success, notify, retry_base=2)
    await processor.handle(event_id)

    async def forbidden(payment):
        raise AssertionError("Завершённое событие повторно обработано")

    await Processor(sessions, forbidden, forbidden).handle(event_id)
    payment, events = await snapshot(sessions)
    assert payment.status == "succeeded"
    assert payment.processing_attempts == 1
    assert payment.webhook_attempts == 1
    assert payment.webhook_delivered_at is not None
    assert events[0].completed_at is not None
    assert len(events) == 1


async def test_decline_is_final_result_and_delivered(client, body, sessions):
    _, event_id = await seed(client, body, sessions)

    async def decline(payment):
        return False

    await Processor(sessions, decline, notify).handle(event_id)
    payment, events = await snapshot(sessions)
    assert payment.status == "failed"
    assert payment.processing_attempts == 1
    assert payment.webhook_delivered_at is not None
    assert len(events) == 1


async def test_webhook_retry_keeps_result_and_schedules_backoff(client, body, sessions):
    _, event_id = await seed(client, body, sessions)

    async def fail(payment):
        raise WebhookError("HTTP 503")

    processor = Processor(sessions, success, fail, retry_base=2)
    for attempt in range(1, 4):
        before = utcnow()
        await processor.handle(event_id)
        payment, events = await snapshot(sessions)
        assert payment.status == "succeeded"
        assert payment.processing_attempts == 1
        assert payment.webhook_attempts == attempt
        assert events[-2].completed_at is not None
        if attempt < 3:
            assert events[-1].topic == "payments.new"
            assert events[-1].available_at >= before + timedelta(seconds=2**attempt)
        else:
            assert events[-1].topic == "payments.dlq"
            assert events[-1].payload["stage"] == "webhook"
        event_id = events[-1].id
    assert len(events) == 4


async def test_webhook_recovery_and_duplicate_retry(client, body, sessions):
    _, event_id = await seed(client, body, sessions)

    async def fail(payment):
        raise WebhookError("timeout")

    await Processor(sessions, success, fail).handle(event_id)
    _, events = await snapshot(sessions)
    retry_id = events[-1].id

    async def forbidden(payment):
        raise AssertionError("Повторная обработка шлюзом")

    recovered = Processor(sessions, forbidden, notify)
    await asyncio.gather(recovered.handle(retry_id), recovered.handle(retry_id))
    await recovered.handle(event_id)
    payment, events = await snapshot(sessions)
    assert payment.status == "succeeded"
    assert payment.processing_attempts == 1
    assert payment.webhook_attempts == 2
    assert payment.webhook_delivered_at is not None
    assert len(events) == 2


async def test_gateway_technical_failure_three_attempts_then_dlq(client, body, sessions):
    _, event_id = await seed(client, body, sessions)

    async def unavailable(payment):
        raise GatewayError("Шлюз недоступен")

    processor = Processor(sessions, unavailable, notify, retry_base=2)
    for attempt in range(1, 4):
        await processor.handle(event_id)
        payment, events = await snapshot(sessions)
        assert payment.processing_attempts == attempt
        if attempt < 3:
            assert payment.status == "pending"
            assert payment.webhook_attempts == 0
            assert events[-1].topic == "payments.new"
        else:
            assert payment.status == "failed"
            assert payment.processed_at is not None
            assert payment.webhook_delivered_at is not None
            assert events[-1].topic == "payments.dlq"
            assert events[-1].payload["stage"] == "gateway"
        event_id = events[-1].id


async def test_crash_after_payment_commit_recovers_without_gateway(client, body, sessions):
    _, event_id = await seed(client, body, sessions)

    async def crash(payment):
        raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await Processor(sessions, success, crash).handle(event_id)
    payment, events = await snapshot(sessions)
    assert payment.status == "succeeded"
    assert payment.processed_at is not None
    assert events[0].completed_at is None

    async def forbidden(payment):
        raise AssertionError("Повторная обработка после восстановления")

    await Processor(sessions, forbidden, notify).handle(event_id)
    payment, _ = await snapshot(sessions)
    assert payment.webhook_delivered_at is not None
    assert payment.processing_attempts == 1


async def test_crash_during_gateway_rolls_back_and_recovers(client, body, sessions):
    _, event_id = await seed(client, body, sessions)

    async def crash(payment):
        raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await Processor(sessions, crash, notify).handle(event_id)
    payment, _ = await snapshot(sessions)
    assert payment.status == "pending"
    assert payment.processing_attempts == 0
    await Processor(sessions, success, notify).handle(event_id)
    payment, _ = await snapshot(sessions)
    assert payment.status == "succeeded"


async def test_lost_publish_confirm_leaves_outbox_and_republishes(client, body, sessions):
    _, event_id = await seed(client, body, sessions)
    delivered = []

    class Broker:
        async def publish(self, message, **kwargs):
            delivered.append(message)
            if len(delivered) == 1:
                raise TimeoutError("Подтверждение потеряно")
            from pamqp.commands import Basic

            return Basic.Ack()

    publisher = Publisher(sessions, Broker())
    with pytest.raises(TimeoutError):
        await publisher.publish_batch()
    _, events = await snapshot(sessions)
    assert events[0].published_at is None
    assert await publisher.publish_batch() == 1
    assert [x["event_id"] for x in delivered] == [str(event_id), str(event_id)]
    _, events = await snapshot(sessions)
    assert events[0].published_at is not None


async def test_publisher_does_not_publish_future_retry(client, body, sessions):
    _, event_id = await seed(client, body, sessions)
    async with sessions.begin() as session:
        event = await session.get(Outbox, event_id)
        event.available_at = utcnow() + timedelta(hours=1)

    class Broker:
        async def publish(self, *args, **kwargs):
            raise AssertionError("Задержка не соблюдена")

    assert await Publisher(sessions, Broker()).publish_batch() == 0


async def test_gateway_recovers_after_technical_error(client, body, sessions):
    _, event_id = await seed(client, body, sessions)

    async def unavailable(payment):
        raise GatewayError("timeout")

    await Processor(sessions, unavailable, notify).handle(event_id)
    _, events = await snapshot(sessions)
    await Processor(sessions, success, notify).handle(events[-1].id)
    payment, events = await snapshot(sessions)
    assert payment.status == "succeeded"
    assert payment.processing_attempts == 2
    assert payment.processing_error is None
    assert payment.webhook_delivered_at is not None
    assert all(event.topic == "payments.new" for event in events)


async def test_gateway_and_webhook_share_three_total_attempts(client, body, sessions):
    _, event_id = await seed(client, body, sessions)

    async def unavailable(payment):
        raise GatewayError("timeout")

    async def failed_webhook(payment):
        raise WebhookError("HTTP 503")

    await Processor(sessions, unavailable, notify).handle(event_id)
    _, events = await snapshot(sessions)
    await Processor(sessions, success, failed_webhook).handle(events[-1].id)
    _, events = await snapshot(sessions)
    await Processor(sessions, success, failed_webhook).handle(events[-1].id)
    payment, events = await snapshot(sessions)
    assert payment.status == "succeeded"
    assert payment.processing_attempts == 2
    assert payment.webhook_attempts == 2
    assert len(events) == 4
    assert events[-1].topic == "payments.dlq"
