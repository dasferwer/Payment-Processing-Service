import asyncio
import ipaddress
import json
import logging
import socket
from collections.abc import Awaitable, Callable

import httpx

logger = logging.getLogger(__name__)


class WebhookPolicyError(ValueError):
    """Адрес webhook нарушает правила исходящих соединений."""


def public_address(value: str) -> bool:
    address = ipaddress.ip_address(value)
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
        address = address.ipv4_mapped
    if isinstance(address, ipaddress.IPv6Address) and (
        address.sixtofour is not None
        or address.teredo is not None
        or address in ipaddress.ip_network("64:ff9b::/96")
        or address in ipaddress.ip_network("64:ff9b:1::/48")
    ):
        return False
    return address.is_global and not (
        address.is_multicast
        or address.is_reserved
        or address.is_unspecified
        or address.is_loopback
        or address.is_link_local
        or address.is_private
    )


async def resolve_addresses(host: str, port: int) -> list[str]:
    try:
        return [str(ipaddress.ip_address(host))]
    except ValueError:
        pass
    async with asyncio.timeout(2):
        records = await asyncio.get_running_loop().getaddrinfo(
            host,
            port,
            type=socket.SOCK_STREAM,
        )
    return list(dict.fromkeys(record[4][0] for record in records))


class WebhookPolicy:
    def __init__(
        self,
        allowed_origins: list[str] | None = None,
        resolver: Callable[[str, int], Awaitable[list[str]]] = resolve_addresses,
    ):
        self.allowed_origins = set()
        self.resolver = resolver
        for value in allowed_origins or []:
            url = self._parse(value)
            if url.path != "/" or url.query or url.fragment:
                raise WebhookPolicyError("Разрешение должно содержать только схему, хост и порт")
            self.allowed_origins.add(self._origin(url))

    @staticmethod
    def _parse(value: str) -> httpx.URL:
        if "\\" in value or any(ord(char) < 32 for char in value):
            raise WebhookPolicyError("Недопустимые символы адреса webhook")
        try:
            url = httpx.URL(value)
        except httpx.InvalidURL as exc:
            raise WebhookPolicyError("Некорректный адрес webhook") from exc
        if url.scheme not in {"http", "https"} or not url.host or url.userinfo or url.fragment:
            raise WebhookPolicyError("Адрес webhook должен быть HTTP(S), без userinfo и fragment")
        return url

    @staticmethod
    def _origin(url: httpx.URL):
        return url.scheme, url.host.lower(), url.port or (443 if url.scheme == "https" else 80)

    def validate_url(self, value: str) -> httpx.URL:
        url = self._parse(value)
        if self._origin(url) in self.allowed_origins:
            return url
        if url.scheme != "https":
            raise WebhookPolicyError("Незашифрованный webhook требует явного разрешения origin")
        if (url.port or (443 if url.scheme == "https" else 80)) not in {80, 443}:
            raise WebhookPolicyError("Нестандартный порт требует явного разрешения origin")
        host = url.host.lower().rstrip(".")
        if host in {"localhost", "postgres", "rabbitmq", "api", "consumer"} or host.endswith(
            (".localhost", ".local", ".internal")
        ):
            raise WebhookPolicyError("Внутренний адрес webhook требует явного разрешения")
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            return url
        if not public_address(str(address)):
            raise WebhookPolicyError("Непубличный IP-адрес webhook запрещён")
        return url

    async def pinned_url(self, value: str) -> tuple[httpx.URL, httpx.URL]:
        original = self.validate_url(value)
        origin_allowed = self._origin(original) in self.allowed_origins
        try:
            addresses = await self.resolver(original.host, self._origin(original)[2])
        except (OSError, TimeoutError, ValueError) as exc:
            raise WebhookPolicyError("Не удалось безопасно разрешить адрес webhook") from exc
        if not addresses:
            raise WebhookPolicyError("DNS не вернул адрес webhook")
        for value in addresses:
            address = ipaddress.ip_address(value)
            if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
                address = address.ipv4_mapped
            # Даже локальное разрешение не открывает multicast и cloud metadata.
            if address.is_multicast or address.is_unspecified or address.is_link_local:
                raise WebhookPolicyError("Служебный IP-адрес webhook запрещён")
            if not origin_allowed and not public_address(value):
                raise WebhookPolicyError("DNS webhook вернул непубличный IP-адрес")
        # Соединяемся с проверенным IP, сохраняя исходные Host и TLS SNI.
        # Повторного DNS lookup исходного имени на этапе подключения нет.
        return original, original.copy_with(host=addresses[0])


def reject_constant(value):
    raise ValueError("Неконечное число в JSON")


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Повтор ключа JSON")
        result[key] = value
    return result


class SecurityMiddleware:
    def __init__(self, app, api_key: bytes, max_body_bytes: int = 65536):
        self.app = app
        self.api_key = api_key
        self.max_body_bytes = max_body_bytes

    async def __call__(self, scope, receive, send):
        import hmac

        from fastapi.responses import JSONResponse

        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        headers = scope.get("headers", [])
        keys = [value for name, value in headers if name.lower() == b"x-api-key"]
        if len(keys) != 1 or len(keys[0]) > 256 or not hmac.compare_digest(keys[0], self.api_key):
            return await JSONResponse({"detail": "Неверный API-ключ"}, 401)(scope, receive, send)
        if sum(len(name) + len(value) for name, value in headers) > 16384:
            return await JSONResponse({"detail": "Слишком большие заголовки"}, 431)(
                scope, receive, send
            )
        for name in [b"idempotency-key", b"content-length", b"content-type"]:
            if sum(key.lower() == name for key, _ in headers) > 1:
                return await JSONResponse({"detail": "Повтор заголовка запрещён"}, 400)(
                    scope,
                    receive,
                    send,
                )
        prepared_receive = receive
        if scope["method"] == "POST":
            header_map = {key.lower(): value for key, value in headers}
            content_type = header_map.get(b"content-type", b"").split(b";")[0].strip().lower()
            if content_type != b"application/json":
                return await JSONResponse({"detail": "Ожидался application/json"}, 415)(
                    scope, receive, send
                )
            declared = header_map.get(b"content-length")
            if declared is not None:
                if not declared.isdigit() or len(declared) > 16:
                    return await JSONResponse({"detail": "Некорректный Content-Length"}, 400)(
                        scope, receive, send
                    )
                if int(declared) > self.max_body_bytes:
                    return await JSONResponse({"detail": "Слишком большой запрос"}, 413)(
                        scope, receive, send
                    )
            body = bytearray()
            try:
                async with asyncio.timeout(10):
                    while True:
                        message = await receive()
                        if message["type"] == "http.disconnect":
                            return
                        chunk = message.get("body", b"")
                        if len(body) + len(chunk) > self.max_body_bytes:
                            return await JSONResponse({"detail": "Слишком большой запрос"}, 413)(
                                scope, receive, send
                            )
                        body.extend(chunk)
                        if not message.get("more_body", False):
                            break
            except TimeoutError:
                return await JSONResponse({"detail": "Истёк срок чтения запроса"}, 408)(
                    scope, receive, send
                )
            try:
                json.loads(body, parse_constant=reject_constant, object_pairs_hook=unique_object)
            except (ValueError, UnicodeError, RecursionError):
                return await JSONResponse({"detail": "Некорректный JSON"}, 422)(
                    scope, receive, send
                )
            emitted = False

            async def prepared_receive():
                nonlocal emitted
                if emitted:
                    return {"type": "http.disconnect"}
                emitted = True
                return {"type": "http.request", "body": bytes(body), "more_body": False}

        started = False

        async def guarded_send(message):
            nonlocal started
            if message["type"] == "http.response.start":
                started = True
                message["headers"].extend(
                    [
                        (b"cache-control", b"no-store"),
                        (b"x-content-type-options", b"nosniff"),
                    ]
                )
            await send(message)

        try:
            await self.app(scope, prepared_receive, guarded_send)
        except Exception as exc:
            # В исключении БД могут быть metadata и полный webhook URL. Не печатаем их.
            logger.error("Запрос завершился внутренней ошибкой: %s", type(exc).__name__)
            if not started:
                await JSONResponse({"detail": "Внутренняя ошибка сервиса"}, 500)(
                    scope,
                    prepared_receive,
                    guarded_send,
                )
