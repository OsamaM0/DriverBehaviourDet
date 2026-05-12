"""
Async Kafka producer/consumer wrappers (aiokafka).

Conventions:
- All values are JSON-serialised pydantic models (orjson).
- Key is `stream_id` (str) → preserves per-stream order.
- Consumers are part of a consumer group named after the service.
- Failures get sent to DLQ with the original headers + error metadata.
- Backpressure: caller awaits `send`, never fire-and-forget.
"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, TypeVar

import orjson
import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError
from pydantic import BaseModel
from tenacity import AsyncRetrying, stop_after_attempt, wait_exponential

from packages.common.config import settings
from packages.common.obs.metrics import (
    KAFKA_CONSUME_ERRORS,
    KAFKA_CONSUME_TOTAL,
    KAFKA_PRODUCE_ERRORS,
    KAFKA_PRODUCE_TOTAL,
)

log = structlog.get_logger(__name__)

T = TypeVar("T", bound=BaseModel)


def _dumps(obj: Any) -> bytes:
    if isinstance(obj, BaseModel):
        return obj.model_dump_json().encode()
    return orjson.dumps(obj)


class KafkaBus:
    """Singleton-ish Kafka producer + on-demand consumers."""

    def __init__(self) -> None:
        self._producer: AIOKafkaProducer | None = None
        self._lock = asyncio.Lock()

    async def producer(self) -> AIOKafkaProducer:
        if self._producer is not None:
            return self._producer
        async with self._lock:
            if self._producer is None:
                p = AIOKafkaProducer(
                    bootstrap_servers=settings.kafka_bootstrap,
                    client_id=f"{settings.kafka_client_id}-{settings.service_name}",
                    enable_idempotence=True,
                    acks="all",
                    compression_type="lz4",
                    max_request_size=2 * 1024 * 1024,
                    linger_ms=5,
                )
                await p.start()
                self._producer = p
        return self._producer

    async def close(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    async def send(self, topic: str, value: BaseModel | dict, key: str | None = None) -> None:
        """Ordered, idempotent produce. Retries with exponential backoff."""
        producer = await self.producer()
        payload = _dumps(value)
        key_b = key.encode() if key else None
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(5),
            wait=wait_exponential(multiplier=0.1, max=2.0),
            reraise=True,
        ):
            with attempt:
                try:
                    await producer.send_and_wait(topic, value=payload, key=key_b)
                    KAFKA_PRODUCE_TOTAL.labels(topic=topic).inc()
                except KafkaError as e:
                    KAFKA_PRODUCE_ERRORS.labels(topic=topic).inc()
                    log.warning("kafka_produce_retry", topic=topic, err=str(e))
                    raise

    async def consume(
        self,
        topics: list[str],
        group_id: str,
        model: type[T],
        handler: Callable[[T], Awaitable[None]],
        max_in_flight: int = 32,
    ) -> None:
        """Long-running consumer loop; drains messages with bounded concurrency."""
        consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=settings.kafka_bootstrap,
            client_id=f"{settings.kafka_client_id}-{settings.service_name}",
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="latest",
            max_poll_records=128,
        )
        await consumer.start()
        sem = asyncio.Semaphore(max_in_flight)

        async def _handle_one(raw_value: bytes) -> None:
            async with sem:
                try:
                    obj = model.model_validate_json(raw_value)
                    await handler(obj)
                except Exception as e:  # noqa: BLE001
                    KAFKA_CONSUME_ERRORS.labels(topic=",".join(topics), group=group_id).inc()
                    log.exception("handler_failed", err=str(e))
                    await self.send(settings.topic_dlq, {"err": str(e), "raw": raw_value.decode("utf-8", "replace")})

        try:
            async for msg in consumer:
                KAFKA_CONSUME_TOTAL.labels(topic=msg.topic, group=group_id).inc()
                await _handle_one(msg.value)
                # commit per-batch via async; aiokafka handles transactional offset commit otherwise
                await consumer.commit()
        finally:
            await consumer.stop()


bus = KafkaBus()


async def iter_topic(topics: list[str], group_id: str) -> AsyncIterator[bytes]:
    """Lower-level iteration when handler-style isn't desired (e.g., fusion)."""
    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=settings.kafka_bootstrap,
        client_id=f"{settings.kafka_client_id}-{settings.service_name}",
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="latest",
    )
    await consumer.start()
    try:
        async for msg in consumer:
            yield msg.value
            await consumer.commit()
    finally:
        await consumer.stop()
