from fastapi import APIRouter
from sqlalchemy import select

from packages.storage.postgres.dao import SessionLocal
from packages.storage.postgres.models import ModelVersion

router = APIRouter()


@router.get("")
async def list_models() -> list[dict]:
    async with SessionLocal() as s:
        rows = (await s.execute(select(ModelVersion))).scalars().all()
        return [
            {"name": r.name, "version": r.version, "backend": r.backend,
             "active": r.is_active, "canary_pct": r.canary_pct, "metrics": r.metrics}
            for r in rows
        ]
