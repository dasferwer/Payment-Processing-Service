"""Создать таблицы платежей и outbox"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "4997ef7cd0c5"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "payments",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("amount", sa.Numeric(precision=18, scale=2), nullable=False),
        sa.Column("currency", sa.String(length=3), nullable=False),
        sa.Column("description", sa.String(length=1000), nullable=False),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("request_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("webhook_url", sa.String(length=2048), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("processing_attempts", sa.Integer(), nullable=False),
        sa.Column("webhook_attempts", sa.Integer(), nullable=False),
        sa.Column("webhook_delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("processing_error", sa.String(length=1000), nullable=True),
        sa.Column("webhook_error", sa.String(length=1000), nullable=True),
        sa.CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="payment_currency"),
        sa.CheckConstraint("status IN ('pending', 'succeeded', 'failed')", name="payment_status"),
        sa.CheckConstraint("amount > 0", name="payment_positive_amount"),
        sa.CheckConstraint("processing_attempts BETWEEN 0 AND 3", name="processing_attempts_limit"),
        sa.CheckConstraint("webhook_attempts BETWEEN 0 AND 3", name="webhook_attempts_limit"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key"),
    )
    op.create_table(
        "outbox",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("payment_id", sa.Uuid(), nullable=False),
        sa.Column("topic", sa.String(length=32), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("topic IN ('payments.new', 'payments.dlq')", name="outbox_topic"),
        sa.ForeignKeyConstraint(
            ["payment_id"],
            ["payments.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "outbox_ready",
        "outbox",
        ["available_at"],
        unique=False,
        postgresql_where="published_at IS NULL",
    )


def downgrade():
    op.drop_index("outbox_ready", table_name="outbox", postgresql_where="published_at IS NULL")
    op.drop_table("outbox")
    op.drop_table("payments")
