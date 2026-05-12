"""
Alert service.

Consumes `events.alert`, persists each alert in Postgres, and fans out to
configured sinks for the tenant. Dedupe is mostly enforced upstream in fusion;
here we only do an idempotency check on `dedupe_key` to handle re-deliveries.
"""
from __future__ import annotations

import asyncio
import os

from packages.alerts.sinks import AlertSink, StdoutSink, WebhookSink
from packages.common.config import settings
from packages.common.kafka import bus
from packages.common.obs import bootstrap
from packages.common.schemas import Alert
from packages.storage.postgres.dao import insert_alert_idempotent

log = bootstrap("alerts")

GROUP_ID = "alerts"


def _build_sinks_for_tenant(tenant_id: str) -> list[AlertSink]:
    # TODO: load from configs/tenants/<tenant_id>.yaml. Static for now.
    sinks: list[AlertSink] = [StdoutSink()]
    webhook = os.environ.get("TENANT_WEBHOOK_URL")
    if webhook:
        sinks.append(WebhookSink(webhook))
    return sinks


async def main() -> None:
    sinks_by_tenant: dict[str, list[AlertSink]] = {}

    async def handler(alert: Alert) -> None:
        inserted = await insert_alert_idempotent(alert)
        if not inserted:
            log.debug("alert_duplicate", dedupe_key=alert.dedupe_key)
            return
        sinks = sinks_by_tenant.setdefault(alert.tenant_id, _build_sinks_for_tenant(alert.tenant_id))
        await asyncio.gather(*(s.emit(alert) for s in sinks), return_exceptions=True)

    log.info("alerts_starting")
    await bus.consume(
        topics=[settings.topic_events_alert],
        group_id=GROUP_ID,
        model=Alert,
        handler=handler,
        max_in_flight=32,
    )


if __name__ == "__main__":
    asyncio.run(main())
