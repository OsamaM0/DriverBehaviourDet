"""Async Redis client + small helpers (rolling window, lease)."""
from __future__ import annotations

import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import redis.asyncio as aioredis

from packages.common.config import settings

_pool: aioredis.Redis | None = None


def get_redis() -> aioredis.Redis:
    global _pool
    if _pool is None:
        _pool = aioredis.from_url(
            settings.redis_url,
            encoding=None,           # we do binary I/O for frame bytes
            decode_responses=False,
            max_connections=64,
        )
    return _pool


async def acquire_lease(key: str, owner: str, ttl_sec: int) -> bool:
    """Atomic 'SET key owner NX EX ttl'. Returns True on acquisition."""
    r = get_redis()
    return bool(await r.set(key, owner.encode(), nx=True, ex=ttl_sec))


async def renew_lease(key: str, owner: str, ttl_sec: int) -> bool:
    """Renew only if we still own the lease (CAS via Lua)."""
    r = get_redis()
    script = (
        "if redis.call('GET', KEYS[1]) == ARGV[1] then "
        "return redis.call('EXPIRE', KEYS[1], ARGV[2]) else return 0 end"
    )
    result = await r.eval(script, 1, key, owner.encode(), str(ttl_sec))
    return bool(result)


@asynccontextmanager
async def lease(key: str, owner: str, ttl_sec: int = 30) -> AsyncIterator[bool]:
    got = await acquire_lease(key, owner, ttl_sec)
    try:
        yield got
    finally:
        if got:
            r = get_redis()
            # release only if still owner
            script = (
                "if redis.call('GET', KEYS[1]) == ARGV[1] then "
                "return redis.call('DEL', KEYS[1]) else return 0 end"
            )
            await r.eval(script, 1, key, owner.encode())


# ── Rolling window over fusion signals (sorted set per stream) ──────────────
async def push_sample(stream_key: str, score: float, payload: bytes, window_ms: int) -> None:
    r = get_redis()
    now_ms = int(time.time() * 1000)
    # store {payload}|{score}|{seq} at ts=now_ms; prune older than window
    pipe = r.pipeline()
    pipe.zadd(stream_key, {payload: now_ms})
    pipe.zremrangebyscore(stream_key, 0, now_ms - window_ms)
    pipe.expire(stream_key, max(60, window_ms // 1000 + 30))
    await pipe.execute()


async def get_window(stream_key: str, window_ms: int) -> list[bytes]:
    r = get_redis()
    now_ms = int(time.time() * 1000)
    return await r.zrangebyscore(stream_key, now_ms - window_ms, now_ms)
