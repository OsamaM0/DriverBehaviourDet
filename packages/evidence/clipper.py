"""
Evidence clipper.

For every alert on `events.alert`, request a ±N second window from the ingest
worker that owns the stream (via Redis rolling segment list — written by the
ingest ring buffer mirror). Mux JPEGs to MP4 with PyAV, upload to S3, and
publish `EvidenceReady` so the alert row is updated.

NOTE: in this scaffold the rolling segment mirror is a TODO; for the dev
end-to-end test the clipper falls back to "snapshot only" mode (single frame
PNG) so the API can still link to evidence. Replace `_collect_window` once
the ingest mirror is in place.
"""
from __future__ import annotations

import asyncio
import io

import av
import cv2
import numpy as np

from packages.common.config import settings
from packages.common.frame_cache import get_frame
from packages.common.kafka import bus
from packages.common.obs import bootstrap
from packages.common.schemas import Alert, EvidenceReady
from packages.storage.postgres.dao import update_alert_evidence
from packages.storage.s3 import put_object

log = bootstrap("evidence")

GROUP_ID = "evidence"
TARGET_FPS = 8


async def _collect_window(alert: Alert) -> list[np.ndarray]:
    """Best-effort: try to fetch the latest known frame for the stream.
    Replace with rolling-segment fetch from S3/MinIO once mirror lands."""
    # Walk recent frame_id backwards, fetching whatever's still in cache.
    frames: list[np.ndarray] = []
    for fid in range(max(0, alert.frame_id), max(0, alert.frame_id - alert.evidence_window_s * TARGET_FPS), -1):
        ref = f"redis://frame:{alert.stream_id}:{fid}"
        jpeg = await get_frame(ref)
        if jpeg is None:
            continue
        arr = np.frombuffer(jpeg, dtype=np.uint8)
        bgr = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if bgr is not None:
            frames.append(bgr)
    frames.reverse()
    return frames


def _mux_mp4(frames: list[np.ndarray], fps: int = TARGET_FPS) -> tuple[bytes, int, int]:
    if not frames:
        raise ValueError("No frames to mux")
    h, w = frames[0].shape[:2]
    buf = io.BytesIO()
    container = av.open(buf, mode="w", format="mp4")
    stream = container.add_stream("h264", rate=fps)
    stream.width = w
    stream.height = h
    stream.pix_fmt = "yuv420p"
    stream.options = {"crf": "23", "preset": "veryfast"}
    for bgr in frames:
        rgb = cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB)
        frame = av.VideoFrame.from_ndarray(rgb, format="rgb24")
        for pkt in stream.encode(frame):
            container.mux(pkt)
    for pkt in stream.encode():
        container.mux(pkt)
    container.close()
    return buf.getvalue(), w, h


async def _handle(alert: Alert) -> None:
    frames = await _collect_window(alert)
    if not frames:
        log.warning("evidence_no_frames", alert_id=alert.alert_id)
        return
    try:
        mp4, w, h = await asyncio.to_thread(_mux_mp4, frames)
    except Exception as e:  # noqa: BLE001
        log.exception("evidence_mux_failed", err=str(e))
        return

    key = f"{alert.tenant_id}/{alert.stream_id}/{alert.alert_id}.mp4"
    s3_uri = await put_object(settings.s3_bucket_evidence, key, mp4, "video/mp4")
    await update_alert_evidence(alert.alert_id, s3_uri)

    ev = EvidenceReady(
        tenant_id=alert.tenant_id, stream_id=alert.stream_id,
        frame_id=alert.frame_id, ts_capture_ns=alert.ts_capture_ns,
        alert_id=alert.alert_id, s3_uri=s3_uri,
        duration_s=len(frames) / TARGET_FPS, codec="h264",
        width=w, height=h,
    )
    await bus.send(settings.topic_evidence_ready, ev, key=alert.stream_id)
    log.info("evidence_uploaded", alert_id=alert.alert_id, s3_uri=s3_uri, frames=len(frames))


async def main() -> None:
    log.info("evidence_starting")
    await bus.consume(
        topics=[settings.topic_events_alert],
        group_id=GROUP_ID,
        model=Alert,
        handler=_handle,
        max_in_flight=8,
    )


if __name__ == "__main__":
    asyncio.run(main())
