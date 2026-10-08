from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import FastAPI, Header, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.config import Settings
from app.db import make_database
from app.logging_config import configure_logging
from app.models import Outbox, Payment, utcnow
from app.schemas import PaymentAccepted, PaymentCreate, PaymentDetails
from app.security import SecurityMiddleware, WebhookPolicy, WebhookPolicyError


def create_app(settings: Settings, sessions=None) -> FastAPI:
    configure_logging()
    engine = None
    if sessions is None:
        engine, sessions = make_database(settings)

    @asynccontextmanager
    async def lifespan(app):
        yield
        if engine is not None:
            await engine.dispose()

    application = FastAPI(
        title="Процессинг платежей",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    application.add_middleware(
        SecurityMiddleware,
        api_key=settings.api_key.get_secret_value().encode(),
        max_body_bytes=settings.max_request_bytes,
    )
    policy = WebhookPolicy(settings.allowed_webhook_origins)

    @application.exception_handler(RequestValidationError)
    async def validation_error(request, exc):
        # Не возвращаем входные значения: они могут содержать секреты или NaN/Infinity.
        return JSONResponse(
            status_code=422,
            content={
                "detail": [
                    {"loc": error["loc"], "msg": error["msg"], "type": error["type"]}
                    for error in exc.errors()
                ]
            },
        )

    @application.post("/api/v1/payments", status_code=202, response_model=PaymentAccepted)
    async def create_payment(
        body: PaymentCreate,
        idempotency_key: Annotated[str, Header(min_length=1, max_length=255)],
    ):
        if not idempotency_key.strip() or "\x00" in idempotency_key:
            raise HTTPException(422, "Idempotency-Key не должен быть пустым или содержать NUL")
        try:
            policy.validate_url(str(body.webhook_url))
        except WebhookPolicyError as exc:
            raise HTTPException(422, str(exc)) from exc
        payload = body.model_dump(mode="json")
        async with sessions.begin() as session:
            payment_id = await session.scalar(
                insert(Payment)
                .values(
                    id=uuid4(),
                    amount=body.amount,
                    currency=body.currency,
                    description=body.description,
                    extra_metadata=body.metadata,
                    webhook_url=str(body.webhook_url),
                    idempotency_key=idempotency_key,
                    request_payload=payload,
                    created_at=utcnow(),
                    status="pending",
                    processing_attempts=0,
                    webhook_attempts=0,
                )
                .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
                .returning(Payment.id)
            )
            # Уникальный индекс дожидается конкурирующей транзакции; следующий SELECT
            # при READ COMMITTED видит её результат, включая завершённый платёж.
            # JSONB различает boolean и number; Python считает True == 1.
            payment, same_payload = (
                await session.execute(
                    select(Payment, Payment.request_payload == payload).where(
                        Payment.idempotency_key == idempotency_key
                    )
                )
            ).one()
            if not same_payload:
                raise HTTPException(409, "Ключ уже использован с другим телом запроса")
            if payment_id is not None:
                session.add(Outbox(payment_id=payment_id))
            return PaymentAccepted(
                payment_id=payment.id,
                status=payment.status,
                created_at=payment.created_at,
            )

    @application.get("/api/v1/payments/{payment_id}", response_model=PaymentDetails)
    async def get_payment(payment_id: UUID):
        async with sessions() as session:
            payment = await session.get(Payment, payment_id)
            if payment is None:
                raise HTTPException(404, "Платёж не найден")
            return PaymentDetails(
                payment_id=payment.id,
                status=payment.status,
                created_at=payment.created_at,
                amount=payment.amount,
                currency=payment.currency,
                description=payment.description,
                metadata=payment.extra_metadata,
                idempotency_key=payment.idempotency_key,
                webhook_url=payment.webhook_url,
                processed_at=payment.processed_at,
                webhook_attempts=payment.webhook_attempts,
                webhook_delivered_at=payment.webhook_delivered_at,
            )

    return application


app = create_app(Settings())
