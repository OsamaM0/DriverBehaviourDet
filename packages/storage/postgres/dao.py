"""Async session factory + minimal DAO functions used by services."""
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from packages.common.config import settings
from packages.common.schemas import Alert, BehaviorState
from packages.storage.postgres.models import AlertRow, BehaviorEventRow

_engine = create_async_engine(settings.database_url, pool_pre_ping=True, pool_size=10, max_overflow=20)
SessionLocal = async_sessionmaker(_engine, class_=AsyncSession, expire_on_commit=False)


async def insert_alert_idempotent(alert: Alert) -> bool:
    """Insert with ON CONFLICT(dedupe_key) DO NOTHING. Returns True if new."""
    async with SessionLocal() as s:
        stmt = pg_insert(AlertRow).values(
            id=alert.alert_id,
            tenant_id=alert.tenant_id,
            stream_id=alert.stream_id,
            type=alert.type.value,
            severity=alert.severity.value,
            state=alert.state.value,
            dedupe_key=alert.dedupe_key,
            ts_capture_ns=alert.ts_capture_ns,
            scores=alert.scores.model_dump(),
        ).on_conflict_do_nothing(index_elements=["dedupe_key"])
        result = await s.execute(stmt)
        await s.commit()
        return result.rowcount == 1


async def insert_behavior_event(b: BehaviorState) -> None:
    async with SessionLocal() as s:
        s.add(BehaviorEventRow(
            tenant_id=b.tenant_id,
            stream_id=b.stream_id,
            state=b.state.value,
            prev_state=b.prev_state.value,
            scores=b.scores.model_dump(),
            ts_capture_ns=b.ts_capture_ns,
            dwell_ms=b.dwell_ms,
            window_ms=b.window_ms,
        ))
        await s.commit()


async def update_alert_evidence(alert_id: str, s3_uri: str) -> None:
    async with SessionLocal() as s:
        row = await s.get(AlertRow, alert_id)
        if row is None:
            return
        row.evidence_uri = s3_uri
        await s.commit()


async def list_recent_alerts(tenant_id: str, limit: int = 100) -> list[AlertRow]:
    async with SessionLocal() as s:
        res = await s.execute(
            select(AlertRow).where(AlertRow.tenant_id == tenant_id)
            .order_by(AlertRow.ts_capture_ns.desc()).limit(limit)
        )
        return list(res.scalars().all())
