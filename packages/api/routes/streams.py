from fastapi import APIRouter
from pydantic import BaseModel
from sqlalchemy import select

from packages.storage.postgres.dao import SessionLocal
from packages.storage.postgres.models import Stream

router = APIRouter()


class StreamIn(BaseModel):
    id: str
    tenant_id: str
    url: str
    protocol: str = "rtsp"
    driver_id: str | None = None


@router.post("")
async def create_stream(s_in: StreamIn) -> dict:
    async with SessionLocal() as s:
        s.add(Stream(**s_in.model_dump()))
        await s.commit()
    return {"ok": True}


@router.get("")
async def list_streams(tenant_id: str | None = None) -> list[dict]:
    async with SessionLocal() as s:
        stmt = select(Stream)
        if tenant_id:
            stmt = stmt.where(Stream.tenant_id == tenant_id)
        rows = (await s.execute(stmt)).scalars().all()
        return [
            {"id": r.id, "tenant_id": r.tenant_id, "url": r.url,
             "protocol": r.protocol, "driver_id": r.driver_id, "enabled": r.enabled}
            for r in rows
        ]
