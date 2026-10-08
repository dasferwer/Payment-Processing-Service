FROM ghcr.io/astral-sh/uv:0.11.17 AS uv
FROM python:3.12-alpine AS builder
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
WORKDIR /service
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

FROM python:3.12-alpine
RUN apk upgrade --no-cache \
    && python -m pip uninstall -y pip setuptools wheel
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PATH="/service/.venv/bin:$PATH"
WORKDIR /service
COPY --from=builder /service/.venv ./.venv
COPY app ./app
COPY migrations ./migrations
COPY alembic.ini ./
RUN adduser -D -u 10001 service
USER service
CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log", "--limit-concurrency", "64"]
