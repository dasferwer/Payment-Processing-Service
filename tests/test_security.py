import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select

from app.api import create_app
from app.config import Settings
from app.models import Outbox, Payment
from app.processing import WebhookError, WebhookSender


def payment(url):
    return Payment(
        id=uuid4(),
        status="succeeded",
        amount=Decimal("1.00"),
        currency="RUB",
        processed_at=datetime.now(UTC),
        extra_metadata={},
        webhook_url=url,
    )


@pytest.mark.parametrize("amount", ["1e-999999999", "1e-999999", "0.001", "1e999999", "NaN"])
async def test_extreme_amount_rejected_before_database(client, body, amount):
    response = await client.post(
        "/api/v1/payments", json=dict(body, amount=amount), headers={"Idempotency-Key": "extreme"}
    )
    assert response.status_code == 422


async def test_oversized_body_rejected(client, body):
    response = await client.post(
        "/api/v1/payments",
        json=dict(body, metadata={"data": "x" * 100_000}),
        headers={"Idempotency-Key": "large"},
    )
    assert response.status_code == 413


async def test_oversized_chunked_body_rejected(client):
    async def content():
        for _ in range(10):
            yield b"x" * 8192

    response = await client.post(
        "/api/v1/payments",
        content=content(),
        headers={"Idempotency-Key": "chunked", "Content-Type": "application/json"},
    )
    assert response.status_code == 413


async def test_duplicate_api_key_rejected(client, body):
    response = await client.post(
        "/api/v1/payments",
        json=body,
        headers=[
            ("X-API-Key", "test-key"),
            ("X-API-Key", "wrong"),
            ("Idempotency-Key", "duplicate"),
        ],
    )
    assert response.status_code == 401


async def test_deep_json_is_client_error(client):
    content = b'{"metadata":' + b"[" * 2000 + b"0" + b"]" * 2000 + b"}"
    response = await client.post(
        "/api/v1/payments",
        content=content,
        headers={"Content-Type": "application/json", "Idempotency-Key": "deep"},
    )
    assert response.status_code == 422


async def test_auth_before_reading_body(sessions):
    calls = []
    app = create_app(Settings(api_key="test-key"), sessions)
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/api/v1/payments",
        "raw_path": b"/api/v1/payments",
        "query_string": b"",
        "headers": [],
        "client": ("127.0.0.1", 1),
        "server": ("test", 80),
    }

    async def receive():
        raise AssertionError("Тело прочитано до авторизации")

    async def send(message):
        calls.append(message)

    await app(scope, receive, send)
    assert calls[0]["status"] == 401


async def test_server_error_does_not_expose_sql_or_payload(client, body, sessions, caplog):
    from sqlalchemy import text

    async with sessions.begin() as session:
        await session.execute(
            text("ALTER TABLE outbox ADD CONSTRAINT reject_security CHECK (topic = 'impossible')")
        )
    secret = "SECURITY_TEST_SENTINEL"
    response = await client.post(
        "/api/v1/payments",
        json=dict(body, metadata={"secret": secret}),
        headers={"Idempotency-Key": "safe-error"},
    )
    assert response.status_code == 500
    assert secret not in response.text + caplog.text
    assert "INSERT INTO" not in response.text + caplog.text
    async with sessions() as session:
        assert await session.scalar(select(Payment)) is None
        assert await session.scalar(select(Outbox)) is None


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1/hook",
        "http://169.254.169.254/latest/meta-data",
        "http://10.0.0.1/hook",
        "http://localhost/hook",
        "http://[::1]/hook",
        "http://[::ffff:127.0.0.1]/hook",
        "http://2130706433/hook",
        "http://0177.0.0.1/hook",
        "http://127.1/hook",
        "http://0x7f000001/hook",
        "http://224.0.0.1/hook",
        "http://[ff02::1]/hook",
        "http://user:password@8.8.8.8/hook",
    ],
)
async def test_webhook_blocks_ssrf_without_network_request(url):
    sent = []

    def receiver(request):
        sent.append(request)
        return httpx.Response(204)

    with pytest.raises(WebhookError):
        await WebhookSender(transport=httpx.MockTransport(receiver))(payment(url))
    assert sent == []


async def test_webhook_never_reads_response_body():
    class Unbounded(httpx.AsyncByteStream):
        async def __aiter__(self):
            raise AssertionError("Тело ответа webhook не нужно читать")
            yield b""

        async def aclose(self):
            pass

    await WebhookSender(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(204, stream=Unbounded()),
        )
    )(payment("https://8.8.8.8/hook"))


async def test_webhook_no_cross_payment_cookies():
    requests = []

    def receiver(request):
        requests.append(request)
        return httpx.Response(204, headers={"Set-Cookie": "token=secret; Path=/; Secure"})

    sender = WebhookSender(transport=httpx.MockTransport(receiver))
    await sender(payment("https://8.8.8.8/one"))
    await sender(payment("https://8.8.8.8/two"))
    assert all("cookie" not in request.headers for request in requests)


async def test_dns_rebinding_and_mixed_dns_are_blocked():
    from app.security import WebhookPolicy

    calls = 0
    sent = []

    async def resolver(host, port):
        nonlocal calls
        calls += 1
        return ["8.8.8.8"] if calls == 1 else ["8.8.8.8", "127.0.0.1"]

    def receiver(request):
        sent.append(request)
        assert request.url.host == "8.8.8.8"
        assert request.headers["host"] == "public.example"
        assert request.extensions["sni_hostname"] == "public.example"
        assert "X-API-Key" not in request.headers
        return httpx.Response(204)

    sender = WebhookSender(
        transport=httpx.MockTransport(receiver), policy=WebhookPolicy(resolver=resolver)
    )
    await sender(payment("https://public.example/one"))
    with pytest.raises(WebhookError):
        await sender(payment("https://public.example/two"))
    assert len(sent) == 1


async def test_allowlist_is_exact_and_never_allows_metadata_server():
    from app.security import WebhookPolicy, WebhookPolicyError

    policy = WebhookPolicy(["http://localhost:9000"])
    assert policy.validate_url("http://localhost:9000/hook").port == 9000
    for url in [
        "http://localhost:9001/hook",
        "http://localhost.evil.example:9000/hook",
        "https://localhost:9000/hook",
    ]:
        with pytest.raises(WebhookPolicyError):
            policy.validate_url(url)

    async def metadata(host, port):
        return ["169.254.169.254"]

    with pytest.raises(WebhookPolicyError):
        await WebhookPolicy(["http://localhost:9000"], resolver=metadata).pinned_url(
            "http://localhost:9000/hook",
        )


async def test_redirect_does_not_send_second_request():
    sent = []

    def redirect(request):
        sent.append(request)
        return httpx.Response(302, headers={"Location": "http://169.254.169.254/secret"})

    with pytest.raises(WebhookError):
        await WebhookSender(transport=httpx.MockTransport(redirect))(
            payment("https://8.8.8.8/hook")
        )
    assert len(sent) == 1


@pytest.mark.parametrize("value", [None, [], "x", {"a": "\ud800"}, {"\ud800": "x"}])
async def test_invalid_metadata_never_becomes_server_error(client, body, value):
    response = await client.post(
        "/api/v1/payments",
        content=json.dumps(dict(body, metadata=value)),
        headers={"Content-Type": "application/json", "Idempotency-Key": "invalid-meta"},
    )
    assert response.status_code == 422


async def test_duplicate_json_key_rejected(client, body):
    content = json.dumps(body)[:-1] + ',"amount":"1.00"}'
    result = await client.post(
        "/api/v1/payments",
        content=content,
        headers={"Content-Type": "application/json", "Idempotency-Key": "duplicate-json"},
    )
    assert result.status_code == 422


async def test_sql_metacharacters_are_data(client, body, sessions):
    value = "'; DROP TABLE payments; --"
    result = await client.post(
        "/api/v1/payments",
        json=dict(body, description=value, metadata={"query": value}),
        headers={"Idempotency-Key": value},
    )
    assert result.status_code == 202
    details = await client.get("/api/v1/payments/" + result.json()["payment_id"])
    assert details.json()["description"] == value
    async with sessions() as session:
        assert await session.scalar(select(Outbox)) is not None


async def test_webhook_tokens_not_logged(caplog):
    import logging

    from app.logging_config import configure_logging

    configure_logging()
    with caplog.at_level(logging.INFO):
        await WebhookSender(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(204),
            )
        )(payment("https://8.8.8.8/hook?token=SECURITY_LOG_SENTINEL"))
    assert "SECURITY_LOG_SENTINEL" not in caplog.text


async def test_settings_repr_hides_connection_credentials():
    settings = Settings(
        api_key="SECURITY_KEY_SENTINEL",
        database_url="postgresql+asyncpg://user:SECURITY_DB_SENTINEL@localhost/db",
        rabbitmq_url="amqp://user:SECURITY_BROKER_SENTINEL@localhost/",
    )
    assert all(
        secret not in repr(settings)
        for secret in [
            "SECURITY_KEY_SENTINEL",
            "SECURITY_DB_SENTINEL",
            "SECURITY_BROKER_SENTINEL",
        ]
    )


async def test_nested_metadata_limit(client, body):
    metadata = {}
    for _ in range(40):
        metadata = {"next": metadata}
    result = await client.post(
        "/api/v1/payments",
        json=dict(body, metadata=metadata),
        headers={"Idempotency-Key": "complex"},
    )
    assert result.status_code == 422


async def test_body_boundary_and_response_cache_policy(client, body):
    result = await client.post("/api/v1/payments", json=body, headers={"Idempotency-Key": "cache"})
    assert result.status_code == 202
    assert result.headers["cache-control"] == "no-store"
    assert result.headers["x-content-type-options"] == "nosniff"


async def test_real_https_preserves_sni_and_validates_certificate(tmp_path):
    import ssl
    import subprocess

    from app.security import WebhookPolicy

    cert, key = tmp_path / "cert.pem", tmp_path / "key.pem"
    await asyncio.to_thread(
        subprocess.run,
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost",
        ],
        check=True,
        capture_output=True,
    )
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    sni, requests = [], []
    context.set_servername_callback(lambda sock, name, ctx: sni.append(name))

    async def receiver(reader, writer):
        try:
            async with asyncio.timeout(5):
                headers = await reader.readuntil(b"\r\n\r\n")
                requests.append(headers)
                writer.write(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
                await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(receiver, "127.0.0.1", 0, ssl=context)
    port = server.sockets[0].getsockname()[1]

    async def local_resolver(host, destination_port):
        return ["127.0.0.1"]

    trusted = ssl.create_default_context(cafile=str(cert))
    try:
        sender = WebhookSender(
            transport=httpx.AsyncHTTPTransport(verify=trusted, trust_env=False),
            policy=WebhookPolicy([f"https://localhost:{port}"], resolver=local_resolver),
        )
        await sender(payment(f"https://localhost:{port}/hook"))
        assert sni == ["localhost"]
        assert f"Host: localhost:{port}".encode() in requests[0]
        with pytest.raises(WebhookError):
            await WebhookSender(
                transport=httpx.AsyncHTTPTransport(verify=trusted, trust_env=False),
                policy=WebhookPolicy([f"https://wrong.test:{port}"], resolver=local_resolver),
            )(payment(f"https://wrong.test:{port}/hook"))
    finally:
        server.close()
        await server.wait_closed()


async def test_giant_content_length_is_client_error(client, body):
    response = await client.post(
        "/api/v1/payments",
        json=body,
        headers={"Idempotency-Key": "giant-length", "Content-Length": "9" * 5000},
    )
    assert response.status_code in {400, 413, 431}


@pytest.mark.parametrize(
    "url",
    [
        "http://169.254.169.254/hook",
        "http://user:secret@8.8.8.8/hook",
        "http://8.8.8.8:1234/hook",
        "http://example.org\\@127.0.0.1/hook",
    ],
)
async def test_api_rejects_forbidden_webhook_urls(client, body, url):
    result = await client.post(
        "/api/v1/payments",
        json=dict(body, webhook_url=url),
        headers={"Idempotency-Key": "blocked-url"},
    )
    assert result.status_code == 422


@pytest.mark.parametrize("address", ["64:ff9b::7f00:1", "2002:7f00:1::", "::ffff:169.254.169.254"])
async def test_ipv6_transition_addresses_do_not_bypass_private_filter(address):
    from app.security import WebhookPolicy, WebhookPolicyError

    async def resolver(host, port):
        return [address]

    with pytest.raises(WebhookPolicyError):
        await WebhookPolicy(resolver=resolver).pinned_url("https://public.example/hook")


async def test_concurrent_conflicting_bodies_are_atomic(client, body, sessions):
    from sqlalchemy import func

    results = await asyncio.gather(
        *[
            client.post(
                "/api/v1/payments",
                json=dict(body, amount=amount),
                headers={"Idempotency-Key": "competing-bodies"},
            )
            for amount in ["1.00", "2.00"] * 10
        ]
    )
    assert sum(response.status_code == 202 for response in results) == 10
    assert sum(response.status_code == 409 for response in results) == 10
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


@pytest.mark.parametrize(
    "field,value,expected",
    [
        ("amount", "0.01", 202),
        ("amount", "9999999999999999.99", 202),
        ("amount", None, 422),
        ("amount", False, 422),
        ("amount", {}, 422),
        ("amount", "1.0001", 422),
        ("amount", "1" * 200, 422),
        ("currency", "rub", 422),
        ("currency", None, 422),
        ("currency", 1, 422),
        ("description", None, 422),
        ("description", "x" * 1001, 422),
        ("description", "я" * 1000, 202),
        ("metadata", {"value": []}, 202),
        ("webhook_url", "file:///etc/passwd", 422),
        ("webhook_url", "https://8.8.8.8/hook#fragment", 422),
    ],
)
async def test_input_boundary_matrix(client, body, field, value, expected):
    result = await client.post(
        "/api/v1/payments", json=dict(body, **{field: value}), headers={"Idempotency-Key": "matrix"}
    )
    assert result.status_code == expected


async def test_runtime_database_role_cannot_read_passwords(sessions):
    from sqlalchemy import text
    from sqlalchemy.exc import ProgrammingError

    async with sessions() as session:
        role = (
            await session.execute(
                text(
                    "SELECT rolsuper, rolcreatedb, rolcreaterole "
                    "FROM pg_roles WHERE rolname=current_user"
                )
            )
        ).one()
        assert tuple(role) == (False, False, False)
        with pytest.raises(ProgrammingError):
            await session.execute(text("SELECT rolpassword FROM pg_authid"))


async def test_public_http_requires_explicit_origin_permission():
    from app.security import WebhookPolicy, WebhookPolicyError

    with pytest.raises(WebhookPolicyError):
        WebhookPolicy().validate_url("http://8.8.8.8/hook")
    assert WebhookPolicy(["http://8.8.8.8"]).validate_url("http://8.8.8.8/hook").scheme == "http"


async def test_explicit_origin_cannot_bypass_mapped_metadata_filter():
    from app.security import WebhookPolicy, WebhookPolicyError

    async def resolver(host, port):
        return ["::ffff:169.254.169.254"]

    with pytest.raises(WebhookPolicyError):
        await WebhookPolicy(["https://trusted.example"], resolver=resolver).pinned_url(
            "https://trusted.example/hook",
        )
