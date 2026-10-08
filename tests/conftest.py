import os
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api import create_app
from app.config import Settings
from app.models import Base


@pytest.fixture
async def sessions():
    url = os.getenv(
        "TEST_DATABASE_URL",
        "postgresql+asyncpg://payments:payments-local@127.0.0.1:55432/payments",
    )
    schema = "test_" + uuid4().hex
    admin = create_async_engine(url)
    async with admin.begin() as conn:
        await conn.execute(text(f"CREATE SCHEMA {schema}"))
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}})
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()
    async with admin.begin() as conn:
        await conn.execute(text(f"DROP SCHEMA {schema} CASCADE"))
    await admin.dispose()


@pytest.fixture
async def client(sessions):
    app = create_app(
        Settings(api_key="test-key", allowed_webhook_origins=["http://127.0.0.1:9000"]), sessions
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"X-API-Key": "test-key"},
    ) as client:
        yield client


@pytest.fixture
def body():
    return {
        "amount": "123.45",
        "currency": "RUB",
        "description": "Оплата заказа",
        "metadata": {"order_id": 42},
        "webhook_url": "http://127.0.0.1:9000/hook",
    }
