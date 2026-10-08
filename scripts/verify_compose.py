"""Проверить живой Compose: webhook, повторы, DLQ и восстановление consumer."""

import asyncio
import json
import os
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from uuid import UUID, uuid4

import httpx
from sqlalchemy import select

from app.config import Settings
from app.db import make_database
from app.models import Outbox, Payment

ROOT = Path(__file__).resolve().parents[1]
COUNTS = {}
PAYLOADS = {}
CRASH_SEEN = threading.Event()
RELEASE = threading.Event()


class Receiver(BaseHTTPRequestHandler):
    def do_POST(self):
        payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        payment_id = payload["payment_id"]
        COUNTS[payment_id] = COUNTS.get(payment_id, 0) + 1
        PAYLOADS[payment_id] = payload
        if self.path == "/crash" and COUNTS[payment_id] == 1:
            CRASH_SEEN.set()
            RELEASE.wait(30)
        status = 204
        if self.path == "/fail" or (self.path == "/flaky" and COUNTS[payment_id] == 1):
            status = 503
        try:
            self.send_response(status)
            self.end_headers()
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, *args):
        pass


def compose(*args):
    subprocess.run(["docker", "compose", *args], cwd=ROOT, check=True, stdout=subprocess.DEVNULL)


async def main():
    settings = Settings()
    engine, sessions = make_database(settings)
    hook_port = int(os.getenv("VERIFY_WEBHOOK_PORT", "59001"))
    hook_host = os.getenv("VERIFY_WEBHOOK_HOST", "host.docker.internal")
    server = ThreadingHTTPServer(("0.0.0.0", hook_port), Receiver)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base_url = os.getenv("VERIFY_API_URL", "http://127.0.0.1:58000")
    async with httpx.AsyncClient(
        base_url=base_url,
        headers={
            "X-API-Key": settings.api_key.get_secret_value(),
        },
        timeout=10,
    ) as client:

        async def create(path):
            result = await client.post(
                "/api/v1/payments",
                headers={"Idempotency-Key": str(uuid4())},
                json={
                    "amount": "10.20",
                    "currency": "RUB",
                    "description": "Проверка Compose",
                    "metadata": {},
                    "webhook_url": f"http://{hook_host}:{hook_port}{path}",
                },
            )
            assert result.status_code == 202, result.text
            return UUID(result.json()["payment_id"])

        async def payment(payment_id):
            async with sessions() as session:
                return await session.get(Payment, payment_id)

        async def wait_for(predicate, deadline_seconds=45):
            deadline = time.monotonic() + deadline_seconds
            while time.monotonic() < deadline:
                result = await predicate()
                if result:
                    return result
                await asyncio.sleep(0.1)
            raise AssertionError("Истёк срок ожидания сценария")

        async def delivered(payment_id):
            result = await payment(payment_id)
            return result if result.webhook_delivered_at else None

        try:
            for path, attempts in [("/ok", 1), ("/flaky", 2)]:
                payment_id = await create(path)
                result = await wait_for(lambda pid=payment_id: delivered(pid))
                assert result.processing_attempts == 1
                assert result.webhook_attempts == attempts
                assert COUNTS[str(payment_id)] == attempts
                assert PAYLOADS[str(payment_id)]["status"] == result.status
                print(f"PASS: {path}, webhook attempts={attempts}")

            payment_id = await create("/fail")

            async def dead_letter():
                async with sessions() as session:
                    return await session.scalar(
                        select(Outbox).where(
                            Outbox.payment_id == payment_id,
                            Outbox.topic == "payments.dlq",
                            Outbox.published_at.is_not(None),
                        )
                    )

            event = await wait_for(dead_letter)
            result = await payment(payment_id)
            assert result.processing_attempts == 1 and result.webhook_attempts == 3
            assert result.status in {"succeeded", "failed"}
            assert result.webhook_delivered_at is None
            async with httpx.AsyncClient() as management:
                response = await management.post(
                    "http://127.0.0.1:55673/api/queues/%2F/payments.dlq/get",
                    auth=("payments", os.getenv("RABBITMQ_PASSWORD", "payments-local")),
                    json={"count": 100, "ackmode": "ack_requeue_true", "encoding": "auto"},
                )
                response.raise_for_status()
                assert any(
                    json.loads(x["payload"])["event_id"] == str(event.id) for x in response.json()
                )
            print("PASS: три ошибки webhook, сохранённый результат, реальное сообщение в DLQ")

            payment_id = await create("/crash")
            assert await asyncio.to_thread(CRASH_SEEN.wait, 15)
            before = await payment(payment_id)
            assert before.status in {"succeeded", "failed"} and before.processing_attempts == 1
            compose("kill", "-s", "SIGKILL", "consumer")
            RELEASE.set()
            compose("start", "consumer")
            result = await wait_for(lambda pid=payment_id: delivered(pid))
            assert result.processing_attempts == 1
            assert COUNTS[str(payment_id)] >= 2
            print("PASS: SIGKILL после фиксации платежа, восстановление только webhook")

            compose("stop", "consumer")
            payment_id = await create("/ok")
            assert (await payment(payment_id)).status == "pending"
            compose("start", "consumer")

            # Ищем транзакцию, удерживающую блокировку платежа во время эмуляции.
            async def gateway_started():
                from sqlalchemy import text

                async with sessions() as session:
                    return await session.scalar(
                        text(
                            "SELECT count(*) FROM pg_stat_activity "
                            "WHERE state='idle in transaction' "
                            "AND query LIKE '%payments.id%FOR UPDATE%'"
                        )
                    )

            await wait_for(gateway_started, deadline_seconds=15)
            compose("kill", "-s", "SIGKILL", "consumer")
            compose("start", "consumer")
            result = await wait_for(lambda pid=payment_id: delivered(pid))
            assert result.processing_attempts == 1
            print("PASS: SIGKILL во время шлюза, откат и восстановление сообщения")

            compose("stop", "rabbitmq")
            payment_id = await create("/ok")
            async with sessions() as session:
                event = await session.scalar(select(Outbox).where(Outbox.payment_id == payment_id))
                assert event.published_at is None
            compose("start", "rabbitmq")
            result = await wait_for(lambda pid=payment_id: delivered(pid), deadline_seconds=60)
            assert result.processing_attempts == 1
            print("PASS: недоступность RabbitMQ, сохранение outbox и доставка после восстановления")
        finally:
            RELEASE.set()
            compose("start", "rabbitmq", "consumer")
            server.shutdown()
            server.server_close()
            await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
