FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim AS builder

WORKDIR /app
COPY pyproject.toml ./
RUN uv sync --no-dev --no-install-project

FROM python:3.14-slim-bookworm

WORKDIR /app
RUN groupadd -g 1001 appgroup && useradd -u 1001 -g appgroup -s /bin/false appuser
COPY --from=builder --chown=appuser:appgroup /app/.venv /app/.venv
COPY --chown=appuser:appgroup app ./app

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8010

USER 1001
EXPOSE 8010
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:${PORT}/health')" || exit 1

CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8010}"]
