"""
Cooperative stream lease via Redis.

Stream controller writes the desired stream → ingest-worker assignment as a
hash. Each ingest worker also holds a Redis lease (`stream:lease:{id}`) to
prevent two workers from owning the same stream during deploys / scale events.
"""
from __future__ import annotations

import asyncio
import contextlib
import socket
import uuid

from packages.common.obs import get_logger
from packages.common.redis import acquire_lease, renew_lease

log = get_logger(__name__)

LEASE_TTL_SEC = 30
RENEW_INTERVAL_SEC = 10


def lease_key(stream_id: str) -> str:
    return f"stream:lease:{stream_id}"


def worker_id() -> str:
    return f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"


async def hold_lease(stream_id: str, owner: str, stop: asyncio.Event) -> None:
    """Background task: renew the lease until `stop` is set."""
    key = lease_key(stream_id)
    while not stop.is_set():
        ok = await renew_lease(key, owner, LEASE_TTL_SEC)
        if not ok:
            log.warning("lease_lost", stream_id=stream_id)
            stop.set()
            return
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=RENEW_INTERVAL_SEC)


async def claim(stream_id: str, owner: str) -> bool:
    return await acquire_lease(lease_key(stream_id), owner, LEASE_TTL_SEC)
