# Shared base for CPU-only workers (ingest, fusion, alerts, evidence, api).
FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg libsm6 libxext6 libgl1 curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml README.md ./
RUN pip install -e . --no-build-isolation

COPY packages ./packages
COPY scripts ./scripts

ENV PROMETHEUS_PORT=9100
EXPOSE 9100
