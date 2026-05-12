"""WebSocket: live alert stream filtered per tenant."""
from __future__ import annotations

import asyncio

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect

from packages.common.config import settings
from packages.common.kafka import iter_topic
from packages.common.schemas import Alert

router = APIRouter()


@router.websocket("/v1/ws/alerts")
async def ws_alerts(ws: WebSocket, tenant_id: str = Query(...)):
    await ws.accept()
    try:
        async for raw in iter_topic([settings.topic_events_alert], group_id=f"ws-{tenant_id}-{id(ws)}"):
            try:
                a = Alert.model_validate_json(raw)
            except Exception:
                continue
            if a.tenant_id != tenant_id:
                continue
            await ws.send_text(a.model_dump_json())
    except WebSocketDisconnect:
        return
    except asyncio.CancelledError:
        return
