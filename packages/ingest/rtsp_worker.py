"""
RTSP ingest worker.

Responsibilities:
  - Pull one or more RTSP streams via PyAV (FFmpeg under the hood).
  - Decode at adaptive FPS (base / escalated based on Redis hint).
  - JPEG-encode each sampled frame, store bytes in Redis frame cache,
    publish a `FrameRef` to Kafka `frames.raw` keyed by stream_id.
  - Maintain a per-stream evidence ring buffer for the clipper.
  - Hold a Redis lease per stream to prevent duplicate ownership.

Run:
  STREAM_ID=cam1 STREAM_URL=rtsp://localhost:8554/test \\
      python -m packages.ingest.rtsp_worker
"""
from __future__ import annotations

import asyncio
import os
import time

import av
import cv2
import numpy as np

from packages.common.config import settings
from packages.common.frame_cache import put_frame
from packages.common.kafka import bus
from packages.common.obs import bootstrap
from packages.common.obs.metrics import (
    INGEST_DECODE_LATENCY,
    INGEST_DROPS_TOTAL,
    INGEST_FPS,
    INGEST_FRAMES_TOTAL,
)
from packages.common.redis import get_redis
from packages.common.schemas import FrameRef
from packages.ingest.ring_buffer import FrameSlot, RingBuffer
from packages.ingest.stream_lease import claim, hold_lease, worker_id

log = bootstrap("ingest", metrics_port=9101)

JPEG_QUALITY = 80
MOTION_DOWNSCALE = 8                    # for cheap motion gate
MOTION_THRESHOLD = 4.0                  # mean-abs-diff threshold


def _encode_jpeg(bgr: np.ndarray) -> bytes:
    ok, buf = cv2.imencode(".jpg", bgr, [int(cv2.IMWRITE_JPEG_QUALITY), JPEG_QUALITY])
    if not ok:
        raise RuntimeError("JPEG encode failed")
    return buf.tobytes()


class _MotionGate:
    """Cheap motion detector to skip publishing identical frames (parked car)."""

    def __init__(self) -> None:
        self._prev: np.ndarray | None = None

    def has_motion(self, bgr: np.ndarray) -> bool:
        small = cv2.resize(
            bgr, (bgr.shape[1] // MOTION_DOWNSCALE, bgr.shape[0] // MOTION_DOWNSCALE),
            interpolation=cv2.INTER_AREA,
        )
        gray = cv2.cvtColor(small, cv2.COLOR_BGR2GRAY)
        if self._prev is None:
            self._prev = gray
            return True
        diff = float(np.abs(gray.astype(np.int16) - self._prev.astype(np.int16)).mean())
        self._prev = gray
        return diff >= MOTION_THRESHOLD


async def _fps_for_stream(stream_id: str) -> int:
    """Read escalation hint from Redis (set by fusion when state != NORMAL)."""
    r = get_redis()
    val = await r.get(f"stream:fps_hint:{stream_id}")
    if val:
        try:
            return int(val.decode())
        except Exception:  # noqa: BLE001
            pass
    return settings.ingest_base_fps


async def ingest_one(stream_id: str, tenant_id: str, url: str) -> None:
    owner = worker_id()
    if not await claim(stream_id, owner):
        log.warning("stream_already_owned", stream_id=stream_id)
        return

    stop = asyncio.Event()
    lease_task = asyncio.create_task(hold_lease(stream_id, owner, stop))
    ring = RingBuffer(seconds=12)

    log.info("rtsp_open", stream_id=stream_id, url=url)
    container = av.open(url, options={
        "rtsp_transport": "tcp",
        "stimeout": "5000000",       # socket timeout in microseconds
        "fflags": "nobuffer",
        "flags": "low_delay",
    })

    try:
        video = container.streams.video[0]
        # Hardware decode if available (NVDEC). Falls back to software.
        if os.environ.get("INGEST_HWACCEL", "0") == "1":
            video.thread_type = "AUTO"
            try:
                container.streams.video[0].codec_context.options = {"hwaccel": "cuda"}
            except Exception:  # noqa: BLE001
                pass

        gate = _MotionGate() if settings.ingest_motion_gate else None
        frame_id = 0
        t_window_start = time.monotonic()
        n_in_window = 0
        next_emit_t = 0.0

        for packet in container.demux(video):
            if stop.is_set():
                break
            for frame in packet.decode():
                now = time.monotonic()
                target_fps = await _fps_for_stream(stream_id)
                INGEST_FPS.labels(stream_id=stream_id).set(target_fps)
                period = 1.0 / max(1, target_fps)

                if now < next_emit_t:
                    INGEST_DROPS_TOTAL.labels(stream_id=stream_id, reason="sampling").inc()
                    continue
                next_emit_t = now + period

                t0 = time.perf_counter()
                bgr = frame.to_ndarray(format="bgr24")

                if gate is not None and not gate.has_motion(bgr):
                    INGEST_DROPS_TOTAL.labels(stream_id=stream_id, reason="motion_gate").inc()
                    continue

                jpeg = _encode_jpeg(bgr)
                h, w = bgr.shape[:2]

                ts_capture_ns = time.time_ns()
                ring.add(FrameSlot(
                    pts_ms=int((frame.pts or 0) * (frame.time_base or 0) * 1000),
                    ts_wall_ns=ts_capture_ns, jpeg=jpeg, width=w, height=h,
                ))

                ref = await put_frame(stream_id, frame_id, jpeg)
                msg = FrameRef(
                    tenant_id=tenant_id,
                    stream_id=stream_id,
                    frame_id=frame_id,
                    ts_capture_ns=ts_capture_ns,
                    frame_ref=ref,
                    width=w, height=h,
                    pts_ms=int((frame.pts or 0) * (frame.time_base or 0) * 1000) if frame.time_base else 0,
                    fps_target=float(target_fps),
                    encoding="jpeg",
                )
                await bus.send(settings.topic_frames_raw, msg, key=stream_id)

                INGEST_FRAMES_TOTAL.labels(stream_id=stream_id).inc()
                INGEST_DECODE_LATENCY.labels(stream_id=stream_id).observe(time.perf_counter() - t0)
                frame_id += 1
                n_in_window += 1

                if now - t_window_start >= 1.0:
                    log.debug("ingest_tick", stream_id=stream_id, fps_actual=n_in_window, fps_target=target_fps)
                    t_window_start = now
                    n_in_window = 0
    finally:
        stop.set()
        lease_task.cancel()
        container.close()
        await bus.close()
        log.info("rtsp_closed", stream_id=stream_id)


async def main() -> None:
    stream_id = os.environ.get("STREAM_ID", "dev-stream")
    tenant_id = os.environ.get("TENANT_ID", "dev-tenant")
    url = os.environ.get("STREAM_URL", "rtsp://localhost:8554/test")
    max_attempts = 30
    retry_delay = 5
    for attempt in range(1, max_attempts + 1):
        try:
            await ingest_one(stream_id, tenant_id, url)
            break  # completed normally
        except (OSError, Exception) as exc:  # noqa: BLE001
            if "Connection refused" not in str(exc) and "Connection timed out" not in str(exc):
                raise
            if attempt >= max_attempts:
                log.error("rtsp_connect_failed", url=url, attempts=attempt)
                raise
            log.warning("rtsp_connect_retry", url=url, attempt=attempt,
                        retry_in=retry_delay, error=str(exc))
            await asyncio.sleep(retry_delay)


if __name__ == "__main__":
    asyncio.run(main())
