"""
Frame cache: store decoded frame bytes off the Kafka path.

Default backend is Redis with a short TTL (≤ frame_cache_ttl_sec). For larger
frames or longer TTLs we'd swap in MinIO/S3 with the same interface.
"""
from __future__ import annotations

from packages.common.config import settings
from packages.common.redis.client import get_redis


def make_frame_key(stream_id: str, frame_id: int) -> str:
    return f"frame:{stream_id}:{frame_id}"


def frame_ref(stream_id: str, frame_id: int) -> str:
    return f"redis://{make_frame_key(stream_id, frame_id)}"


async def put_frame(stream_id: str, frame_id: int, data: bytes) -> str:
    """Store frame bytes; return ref string for FrameRef.frame_ref."""
    r = get_redis()
    key = make_frame_key(stream_id, frame_id)
    await r.set(key, data, ex=settings.frame_cache_ttl_sec)
    return f"redis://{key}"


async def get_frame(ref: str) -> bytes | None:
    """Resolve a frame ref. Supports `redis://...` only for now."""
    if ref.startswith("redis://"):
        key = ref[len("redis://") :]
        r = get_redis()
        return await r.get(key)
    raise ValueError(f"Unsupported frame ref scheme: {ref!r}")


async def delete_frame(stream_id: str, frame_id: int) -> None:
    r = get_redis()
    await r.delete(make_frame_key(stream_id, frame_id))
