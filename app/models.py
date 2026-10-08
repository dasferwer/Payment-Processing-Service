from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Numeric, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utcnow() -> datetime:
    return datetime.now(UTC)


class Base(DeclarativeBase):
    pass


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (
        CheckConstraint("amount > 0", name="payment_positive_amount"),
        CheckConstraint("status IN ('pending', 'succeeded', 'failed')", name="payment_status"),
        CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="payment_currency"),
        CheckConstraint("processing_attempts BETWEEN 0 AND 3", name="processing_attempts_limit"),
        CheckConstraint("webhook_attempts BETWEEN 0 AND 3", name="webhook_attempts_limit"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[str] = mapped_column(String(3))
    description: Mapped[str] = mapped_column(String(1000))
    extra_metadata: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB)
    status: Mapped[str] = mapped_column(String(16), default="pending")
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True)
    request_payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    webhook_url: Mapped[str] = mapped_column(String(2048))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    processing_attempts: Mapped[int] = mapped_column(default=0)
    webhook_attempts: Mapped[int] = mapped_column(default=0)
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    processing_error: Mapped[str | None] = mapped_column(String(1000))
    webhook_error: Mapped[str | None] = mapped_column(String(1000))


class Outbox(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        CheckConstraint("topic IN ('payments.new', 'payments.dlq')", name="outbox_topic"),
        Index("outbox_ready", "available_at", postgresql_where="published_at IS NULL"),
    )
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    payment_id: Mapped[UUID] = mapped_column(ForeignKey("payments.id"))
    topic: Mapped[str] = mapped_column(String(32), default="payments.new")
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
