from fastapi import APIRouter
from pydantic import BaseModel
from sqlalchemy import select

from packages.storage.postgres.dao import SessionLocal
from packages.storage.postgres.models import Tenant

router = APIRouter()


class TenantIn(BaseModel):
    id: str
    name: str


@router.post("")
async def create_tenant(t: TenantIn) -> dict:
    async with SessionLocal() as s:
        s.add(Tenant(id=t.id, name=t.name))
        await s.commit()
    return {"ok": True}


@router.get("")
async def list_tenants() -> list[dict]:
    async with SessionLocal() as s:
        rows = (await s.execute(select(Tenant))).scalars().all()
        return [{"id": r.id, "name": r.name} for r in rows]
