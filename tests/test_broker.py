import os
from uuid import uuid4

import pytest
from sqlalchemy import select

from app.models import Outbox
from app.publisher import Publisher
from app.worker import declare_topology, make_broker


@pytest.fixture
async def broker():
    # Отдельный vhost: проверки не читают очереди работающего приложения.
    instance = make_broker(
        os.getenv(
            "TEST_RABBITMQ_URL",
            "amqp://payments:payments-local@127.0.0.1:55672/payments_test",
        )
    )
    await instance.connect()
    await declare_topology(instance)
    for name in ["payments.new", "payments.dlq"]:
        from faststream.rabbit import RabbitQueue

        queue = await instance.declare_queue(RabbitQueue(name, declare=False))
        await queue.purge()
    yield instance
    await instance.stop()


async def test_real_publish_confirm_and_persistent_roundtrip(client, body, sessions, broker):
    await client.post("/api/v1/payments", json=body, headers={"Idempotency-Key": "broker"})
    assert await Publisher(sessions, broker).publish_batch() == 1
    from faststream.rabbit import RabbitQueue

    queue = await broker.declare_queue(RabbitQueue("payments.new", declare=False))
    message = await queue.get(timeout=5)
    import json

    content = json.loads(message.body)
    assert message.delivery_mode == 2
    async with sessions() as session:
        event = await session.scalar(select(Outbox))
        assert event.published_at is not None
        assert content["event_id"] == str(event.id)
    await message.ack()


async def test_real_nack_redelivery_and_dead_letter(broker):
    await broker.publish({"event_id": str(uuid4())}, queue="payments.new", persist=True)
    from faststream.rabbit import RabbitQueue

    queue = await broker.declare_queue(RabbitQueue("payments.new", declare=False))
    message = await queue.get(timeout=5)
    await message.nack(requeue=True)
    redelivered = await queue.get(timeout=5)
    assert redelivered.redelivered
    await redelivered.reject(requeue=False)
    dlq = await broker.declare_queue(RabbitQueue("payments.dlq", declare=False))
    import asyncio

    for _ in range(50):
        dead = await dlq.get(fail=False)
        if dead is not None:
            break
        await asyncio.sleep(0.1)
    assert dead is not None
    assert dead.headers["x-death"][0]["reason"] == "rejected"
    await dead.ack()


async def test_worker_rejects_poison_messages_without_blocking(broker, sessions):
    import asyncio

    from faststream import TestApp
    from faststream.rabbit import RabbitQueue

    from app.config import Settings
    from app.worker import create_worker

    application = create_worker(
        Settings(
            api_key="test",
            rabbitmq_url=os.getenv(
                "TEST_RABBITMQ_URL",
                "amqp://payments:payments-local@127.0.0.1:55672/payments_test",
            ),
        ),
        sessions=sessions,
    )
    queue = await broker.declare_queue(RabbitQueue("payments.dlq", declare=False))
    async with TestApp(application):
        for payload in [
            [],
            {"event_id": 1},
            b"not-json",
            b"[" * 2000 + b"0" + b"]" * 2000,
            b"x" * 10000,
            {"event_id": str(uuid4())},
        ]:
            await broker.publish(payload, queue="payments.new", content_type="application/json")
            dead = None
            for _ in range(30):
                dead = await queue.get(fail=False)
                if dead is not None:
                    break
                await asyncio.sleep(0.1)
            assert dead is not None, "Некорректное сообщение заблокировало consumer"
            await dead.ack()


async def test_unroutable_publish_keeps_outbox(client, body, sessions, broker):
    await client.post("/api/v1/payments", json=body, headers={"Idempotency-Key": "unroutable"})

    class MissingRoute:
        async def publish(self, message, **kwargs):
            kwargs["queue"] = "missing-" + uuid4().hex
            return await broker.publish(message, **kwargs)

    from aio_pika.exceptions import PublishError

    with pytest.raises(PublishError):
        await Publisher(sessions, MissingRoute()).publish_batch()
    async with sessions() as session:
        event = await session.scalar(select(Outbox))
        assert event.published_at is None


async def test_concurrent_publishers_publish_one_event(client, body, sessions, broker):
    import asyncio

    await client.post("/api/v1/payments", json=body, headers={"Idempotency-Key": "publishers"})
    counts = await asyncio.gather(
        Publisher(sessions, broker).publish_batch(),
        Publisher(sessions, broker).publish_batch(),
    )
    assert sum(counts) == 1
    from faststream.rabbit import RabbitQueue

    queue = await broker.declare_queue(RabbitQueue("payments.new", declare=False))
    message = await queue.get(timeout=5)
    await message.ack()
    assert await queue.get(fail=False) is None
