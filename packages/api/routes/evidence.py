from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from packages.common.config import settings
from packages.storage.postgres.dao import SessionLocal
from packages.storage.postgres.models import AlertRow
from packages.storage.s3 import stream_get

router = APIRouter()


@router.get("/{alert_id}")
async def get_evidence(alert_id: str):
    async with SessionLocal() as s:
        row = await s.get(AlertRow, alert_id)
        if row is None or not row.evidence_uri:
            raise HTTPException(404, "no evidence")
    # s3://bucket/key
    if not row.evidence_uri.startswith("s3://"):
        raise HTTPException(500, "unsupported uri")
    _, _, rest = row.evidence_uri.partition("s3://")
    bucket, _, key = rest.partition("/")

    async def _iter():
        async with stream_get(bucket, key) as body:
            while True:
                chunk = body.read(64 * 1024)
                if not chunk:
                    break
                yield chunk

    return StreamingResponse(_iter(), media_type="video/mp4")


@router.get("")
async def evidence_meta() -> dict:
    return {"bucket": settings.s3_bucket_evidence}
