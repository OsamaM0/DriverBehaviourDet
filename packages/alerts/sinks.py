"""Pluggable alert sinks. Loaded by `alert_service` based on tenant config."""
from __future__ import annotations

from abc import ABC, abstractmethod

import httpx

from packages.common.obs import get_logger
from packages.common.schemas import Alert

log = get_logger(__name__)


class AlertSink(ABC):
    @abstractmethod
    async def emit(self, alert: Alert) -> None: ...


class WebhookSink(AlertSink):
    def __init__(self, url: str, timeout: float = 5.0) -> None:
        self._url = url
        self._client = httpx.AsyncClient(timeout=timeout)

    async def emit(self, alert: Alert) -> None:
        try:
            r = await self._client.post(self._url, content=alert.model_dump_json(), headers={
                "content-type": "application/json",
            })
            r.raise_for_status()
        except Exception as e:  # noqa: BLE001
            log.warning("webhook_sink_failed", url=self._url, err=str(e))


class StdoutSink(AlertSink):
    async def emit(self, alert: Alert) -> None:
        log.info("alert", **alert.model_dump(mode="json"))
