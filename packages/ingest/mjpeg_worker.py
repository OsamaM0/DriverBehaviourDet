"""
HTTP MJPEG ingest worker.

Reads `multipart/x-mixed-replace; boundary=...` MJPEG streams (common on
cheap IP cameras and dashcam HTTP endpoints), splits frames at boundaries,
and feeds the same downstream pipeline as the RTSP worker.
"""
from __future__ import annotations

import asyncio
import os
import time

import cv2
import httpx
import numpy as np

from packages.common.config import settings
from packages.common.frame_cache import put_frame
from packages.common.kafka import bus
from packages.common.obs import bootstrap
from packages.common.obs.metrics import INGEST_DECODE_LATENCY, INGEST_FRAMES_TOTAL
from packages.common.schemas import FrameRef
from packages.ingest.stream_lease import claim, hold_lease, worker_id

log = bootstrap("ingest-mjpeg")

JPEG_SOI = b"\xff\xd8"
JPEG_EOI = b"\xff\xd9"


async def _frames_from_mjpeg(url: str):
    async with httpx.AsyncClient(timeout=None) as client:
        async with client.stream("GET", url) as resp:
            resp.raise_for_status()
            buf = bytearray()
            async for chunk in resp.aiter_bytes():
                buf.extend(chunk)
                while True:
                    soi = buf.find(JPEG_SOI)
                    if soi < 0:
                        buf.clear()
                        break
                    eoi = buf.find(JPEG_EOI, soi + 2)
                    if eoi < 0:
                        if soi > 0:
                            del buf[:soi]
                        break
                    yield bytes(buf[soi : eoi + 2])
                    del buf[: eoi + 2]


async def ingest_one(stream_id: str, tenant_id: str, url: str) -> None:
    owner = worker_id()
    if not await claim(stream_id, owner):
        log.warning("stream_already_owned", stream_id=stream_id)
        return
    stop = asyncio.Event()
    lease_task = asyncio.create_task(hold_lease(stream_id, owner, stop))
    period = 1.0 / max(1, settings.ingest_base_fps)
    next_emit_t = 0.0
    frame_id = 0

    try:
        async for jpeg in _frames_from_mjpeg(url):
            now = time.monotonic()
            if now < next_emit_t:
                continue
            next_emit_t = now + period

            t0 = time.perf_counter()
            arr = np.frombuffer(jpeg, dtype=np.uint8)
            bgr = cv2.imdecode(arr, cv2.IMREAD_COLOR)
            if bgr is None:
                continue
            h, w = bgr.shape[:2]
            ts_capture_ns = time.time_ns()

            ref = await put_frame(stream_id, frame_id, jpeg)
            msg = FrameRef(
                tenant_id=tenant_id, stream_id=stream_id, frame_id=frame_id,
                ts_capture_ns=ts_capture_ns, frame_ref=ref,
                width=w, height=h, pts_ms=0, fps_target=float(settings.ingest_base_fps),
                encoding="jpeg",
            )
            await bus.send(settings.topic_frames_raw, msg, key=stream_id)
            INGEST_FRAMES_TOTAL.labels(stream_id=stream_id).inc()
            INGEST_DECODE_LATENCY.labels(stream_id=stream_id).observe(time.perf_counter() - t0)
            frame_id += 1
    finally:
        stop.set()
        lease_task.cancel()
        await bus.close()


if __name__ == "__main__":
    asyncio.run(ingest_one(
        os.environ.get("STREAM_ID", "dev-stream"),
        os.environ.get("TENANT_ID", "dev-tenant"),
        os.environ["STREAM_URL"],
    ))
