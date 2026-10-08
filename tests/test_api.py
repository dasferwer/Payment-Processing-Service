import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from app.models import Outbox, Payment


async def test_create_and_get_decimal(client, body, sessions):
    response = await client.post(
        "/api/v1/payments", json=body, headers={"Idempotency-Key": "order-1"}
    )
    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "pending"
    result = await client.get("/api/v1/payments/" + data["payment_id"])
    assert result.status_code == 200
    assert result.json()["amount"] == "123.45"
    assert result.json()["metadata"] == {"order_id": 42}
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_concurrent_idempotency(client, body, sessions):
    responses = await asyncio.gather(
        *[
            client.post("/api/v1/payments", json=body, headers={"Idempotency-Key": "same"})
            for _ in range(20)
        ]
    )
    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 1
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_key_conflict_and_equivalent_body(client, body):
    headers = {"Idempotency-Key": "same"}
    first = await client.post("/api/v1/payments", json=body, headers=headers)
    equivalent = dict(body, amount="123.450")
    second = await client.post("/api/v1/payments", json=equivalent, headers=headers)
    assert second.status_code == 202
    assert second.json()["payment_id"] == first.json()["payment_id"]
    conflict = await client.post(
        "/api/v1/payments", json=dict(body, amount="124.00"), headers=headers
    )
    assert conflict.status_code == 409


@pytest.mark.parametrize(
    "field,value",
    [
        ("amount", "0"),
        ("amount", "-1"),
        ("amount", "1.001"),
        ("amount", "NaN"),
        ("amount", "10000000000000000"),
        ("currency", "BTC"),
        ("webhook_url", "ftp://host"),
        ("description", "\x00"),
        ("metadata", {"bad": "\x00"}),
    ],
)
async def test_validation(client, body, field, value):
    result = await client.post(
        "/api/v1/payments", json=dict(body, **{field: value}), headers={"Idempotency-Key": "x"}
    )
    assert result.status_code == 422


async def test_required_headers_and_unknown_payment(client, body):
    assert (await client.post("/api/v1/payments", json=body)).status_code == 422
    assert (
        await client.post("/api/v1/payments", json=body, headers={"Idempotency-Key": " "})
    ).status_code == 422
    for key in ["", "wrong"]:
        assert (
            await client.post(
                "/api/v1/payments", json=body, headers={"X-API-Key": key, "Idempotency-Key": "x"}
            )
        ).status_code == 401
        assert (
            await client.get("/api/v1/payments/" + str(uuid4()), headers={"X-API-Key": key})
        ).status_code == 401
    assert (await client.get("/api/v1/payments/" + str(uuid4()))).status_code == 404
    assert (await client.get("/docs")).status_code == 404


async def test_payment_rollback_if_outbox_insert_fails(client, body, sessions):
    from sqlalchemy import text

    async with sessions.begin() as session:
        await session.execute(
            text("ALTER TABLE outbox ADD CONSTRAINT reject_all CHECK (topic = 'impossible')")
        )
    result = await client.post("/api/v1/payments", json=body, headers={"Idempotency-Key": "atomic"})
    assert result.status_code == 500
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0


async def test_metadata_rejects_nonfinite_json_number(client, body):
    import json

    content = json.dumps(dict(body, metadata={"value": float("inf")})).replace("Infinity", "1e309")
    response = await client.post(
        "/api/v1/payments",
        content=content,
        headers={"Idempotency-Key": "nonfinite", "Content-Type": "application/json"},
    )
    assert response.status_code == 422


async def test_metadata_allows_literal_backslash_unicode_escape(client, body):
    response = await client.post(
        "/api/v1/payments",
        json=dict(body, metadata={"text": r"\u0000"}),
        headers={"Idempotency-Key": "literal"},
    )
    assert response.status_code == 202


async def test_get_contains_idempotency_key(client, body):
    response = await client.post(
        "/api/v1/payments", json=body, headers={"Idempotency-Key": "visible-key"}
    )
    details = await client.get("/api/v1/payments/" + response.json()["payment_id"])
    assert details.json()["idempotency_key"] == "visible-key"


async def test_idempotency_distinguishes_boolean_from_number(client, body):
    first = await client.post(
        "/api/v1/payments",
        json=dict(body, metadata={"value": 1}),
        headers={"Idempotency-Key": "json-types"},
    )
    assert first.status_code == 202
    second = await client.post(
        "/api/v1/payments",
        json=dict(body, metadata={"value": True}),
        headers={"Idempotency-Key": "json-types"},
    )
    assert second.status_code == 409
