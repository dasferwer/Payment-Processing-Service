import asyncio
import logging

from pamqp.commands import Basic
from sqlalchemy import select

from app.models import Outbox, utcnow

logger = logging.getLogger(__name__)


class Publisher:
    def __init__(self, sessions, broker):
        self.sessions = sessions
        self.broker = broker

    async def publish_batch(self, limit: int = 100) -> int:
        count = 0
        for _ in range(limit):
            async with self.sessions.begin() as session:
                event = await session.scalar(
                    select(Outbox)
                    .where(
                        Outbox.published_at.is_(None),
                        Outbox.available_at <= utcnow(),
                    )
                    .order_by(Outbox.available_at)
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
                if event is None:
                    break
                result = await self.broker.publish(
                    {
                        **event.payload,
                        "event_id": str(event.id),
                        "payment_id": str(event.payment_id),
                    },
                    queue=event.topic,
                    exchange="payments.events",
                    persist=True,
                    mandatory=True,
                    timeout=5,
                    message_id=str(event.id),
                )
                if not isinstance(result, Basic.Ack):
                    raise RuntimeError("RabbitMQ не подтвердил публикацию")
                event.published_at = utcnow()
            count += 1
        return count

    async def run(self, poll_seconds: float):
        while True:
            try:
                await self.publish_batch()
            except Exception as exc:
                logger.error("Ошибка публикации outbox; повтор сохранён: %s", type(exc).__name__)
            await asyncio.sleep(poll_seconds)
