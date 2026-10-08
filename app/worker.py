import asyncio
import json
import logging
from contextlib import asynccontextmanager, suppress
from uuid import UUID

from faststream import AckPolicy, FastStream
from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitMessage, RabbitQueue
from faststream.rabbit.schemas import Channel

from app.config import Settings
from app.db import make_database
from app.logging_config import configure_logging
from app.processing import InvalidEvent, Processor, WebhookSender, emulate_gateway
from app.publisher import Publisher
from app.security import WebhookPolicy

logger = logging.getLogger(__name__)


def make_broker(url: str) -> RabbitBroker:
    return RabbitBroker(
        url,
        default_channel=Channel(
            prefetch_count=1,
            publisher_confirms=True,
            on_return_raises=True,
        ),
        graceful_timeout=15,
    )


EVENTS = RabbitExchange("payments.events", durable=True)
DLX = RabbitExchange("payments.dead", durable=True)
DLQ = RabbitQueue("payments.dlq", durable=True)
NEW = RabbitQueue(
    "payments.new",
    durable=True,
    arguments={
        "x-dead-letter-exchange": "payments.dead",
        "x-dead-letter-routing-key": "payments.dlq",
        "x-single-active-consumer": True,
    },
)


async def declare_topology(broker: RabbitBroker):
    exchange = await broker.declare_exchange(DLX)
    queue = await broker.declare_queue(DLQ)
    await queue.bind(exchange, routing_key="payments.dlq")
    events_exchange = await broker.declare_exchange(EVENTS)
    await queue.bind(events_exchange, routing_key="payments.dlq")
    new_queue = await broker.declare_queue(NEW)
    await new_queue.bind(events_exchange, routing_key="payments.new")


def create_worker(settings: Settings, sessions=None) -> FastStream:
    configure_logging()
    engine = None
    if sessions is None:
        engine, sessions = make_database(settings)
    broker = make_broker(settings.rabbitmq_url.get_secret_value())
    processor = Processor(
        sessions,
        emulate_gateway,
        WebhookSender(
            policy=WebhookPolicy(settings.allowed_webhook_origins),
            timeout_seconds=settings.webhook_timeout_seconds,
        ),
        settings.retry_base_seconds,
    )
    publisher = Publisher(sessions, broker)

    @asynccontextmanager
    async def lifespan():
        await broker.connect()
        await declare_topology(broker)
        task = asyncio.create_task(publisher.run(settings.outbox_poll_seconds))
        try:
            yield
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
            if engine is not None:
                await engine.dispose()

    application = FastStream(broker, lifespan=lifespan)

    def raw_decoder(message):
        # Декодируем внутри обработчика: ошибка JSON тоже должна получить reject.
        return message.body

    @broker.subscriber(
        NEW, exchange=EVENTS, ack_policy=AckPolicy.MANUAL, decoder=raw_decoder, no_reply=True
    )
    async def consume(body: bytes, message: RabbitMessage):
        try:
            if len(body) > 8192:
                raise ValueError("Сообщение слишком большое")
            decoded = json.loads(body)
            if not isinstance(decoded, dict) or not isinstance(decoded.get("event_id"), str):
                raise ValueError("Ожидался строковый event_id")
            event_id = UUID(decoded["event_id"])
        except (KeyError, TypeError, ValueError, UnicodeDecodeError, RecursionError):
            logger.error("Некорректное событие направлено в DLQ")
            await message.reject(requeue=False)
            return
        try:
            await processor.handle(event_id)
        except InvalidEvent:
            logger.error("Неизвестное событие %s направлено в DLQ", event_id)
            await message.reject(requeue=False)
        except Exception as exc:
            logger.error("Событие %s не завершено; ошибка %s", event_id, type(exc).__name__)
            await asyncio.sleep(1)
            await message.nack(requeue=True)
        else:
            await message.ack()

    return application


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(create_worker(Settings()).run())
