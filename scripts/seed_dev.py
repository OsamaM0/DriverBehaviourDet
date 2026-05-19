"""Seed dev tenant + stream rows.

Usage: python -m scripts.seed_dev
"""
import asyncio

from packages.storage.postgres.dao import SessionLocal, _engine
from packages.storage.postgres.models import Base, Stream, Tenant


async def main() -> None:
    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Insert tenant first (stream has FK to tenant)
    async with SessionLocal() as s:
        s.add(Tenant(id="acme", name="Acme Logistics"))
        try:
            await s.commit()
        except Exception:
            await s.rollback()

    # Then insert stream
    async with SessionLocal() as s:
        s.add(Stream(
            id="acme-cab-001", tenant_id="acme",
            url="rtsp://mediamtx:8554/driver_synth",
            protocol="rtsp", driver_id="driver-A",
        ))
        try:
            await s.commit()
        except Exception:
            await s.rollback()
            print("seed already present")
            return

    print("seed complete")


if __name__ == "__main__":
    asyncio.run(main())
