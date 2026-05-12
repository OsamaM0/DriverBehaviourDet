from fastapi import APIRouter, Query

from packages.storage.postgres.dao import list_recent_alerts

router = APIRouter()


@router.get("")
async def get_events(tenant_id: str = Query(...), limit: int = 100) -> list[dict]:
    rows = await list_recent_alerts(tenant_id, limit=limit)
    return [
        {
            "alert_id": r.id,
            "tenant_id": r.tenant_id,
            "stream_id": r.stream_id,
            "type": r.type,
            "severity": r.severity,
            "state": r.state,
            "ts_capture_ns": r.ts_capture_ns,
            "scores": r.scores,
            "evidence_uri": r.evidence_uri,
        }
        for r in rows
    ]
