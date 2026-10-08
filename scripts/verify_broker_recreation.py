"""Проверить сохранение подтверждённого сообщения при пересоздании RabbitMQ."""

import asyncio
import json
import subprocess
from pathlib import Path
from uuid import uuid4

from faststream.rabbit import RabbitQueue

from app.config import Settings
from app.worker import declare_topology, make_broker

ROOT = Path(__file__).resolve().parents[1]


def compose(*args):
    subprocess.run(
        ["docker", "compose", *args],
        cwd=ROOT,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


async def main():
    settings = Settings()
    event_id = str(uuid4())
    await asyncio.to_thread(compose, "stop", "consumer")
    try:
        broker = make_broker(settings.rabbitmq_url.get_secret_value())
        await broker.connect()
        await declare_topology(broker)
        await broker.publish(
            {"event_id": event_id},
            queue="payments.new",
            exchange="payments.events",
            persist=True,
            timeout=5,
        )
        await broker.stop()
        await asyncio.to_thread(
            compose,
            "up",
            "-d",
            "--force-recreate",
            "--no-deps",
            "--wait",
            "--wait-timeout",
            "90",
            "rabbitmq",
        )
        broker = make_broker(settings.rabbitmq_url.get_secret_value())
        await broker.connect()
        try:
            queue = await broker.declare_queue(RabbitQueue("payments.new", declare=False))
            message = await queue.get(timeout=5)
            if json.loads(message.body).get("event_id") != event_id:
                await message.nack(requeue=True)
                raise AssertionError("В очереди есть другая работа; повторите после её завершения")
            await message.ack()
        finally:
            await broker.stop()
        print("PASS: persistent-сообщение сохранилось после force-recreate RabbitMQ")
    finally:
        await asyncio.to_thread(compose, "start", "consumer")


if __name__ == "__main__":
    asyncio.run(main())
