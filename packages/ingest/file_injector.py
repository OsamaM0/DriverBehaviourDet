from __future__ import annotations

import asyncio
import os
import re
import time
from pathlib import Path

import av
import cv2
import numpy as np
import structlog

from packages.common.config import settings
from packages.common.frame_cache import put_frame
from packages.common.kafka import bus
from packages.common.schemas import FrameRef

log = structlog.get_logger(__name__)

JPEG_QUALITY = 80
_STREAM_SLUG_RE = re.compile(r"[^a-z0-9]+")


def make_upload_stream_id(filename: str | None, prefix: str = "upload") -> str:
    stem = Path(filename or "").stem.lower()
    slug = _STREAM_SLUG_RE.sub("-", stem).strip("-") or "video"
    return f"{prefix}-{slug}-{int(time.time())}"


def _encode_jpeg(bgr: np.ndarray) -> bytes:
    ok, buf = cv2.imencode(
        ".jpg",
        bgr,
        [int(cv2.IMWRITE_JPEG_QUALITY), JPEG_QUALITY],
    )
    if not ok:
        raise RuntimeError("JPEG encode failed")
    return buf.tobytes()


async def inject_video_file(
    video_path: str,
    tenant_id: str,
    stream_id: str,
    fps: int,
    loops: int = 1,
) -> int:
    if fps < 1:
        raise ValueError("fps must be >= 1")
    if loops < 1:
        raise ValueError("loops must be >= 1")

    interval = 1.0 / fps
    frame_id = 0
    total_sent = 0

    for loop_num in range(loops):
        container = av.open(video_path)
        try:
            video_stream = container.streams.video[0]
            prev_time = time.monotonic()
            loop_frames = 0

            for packet in container.demux(video_stream):
                for av_frame in packet.decode():
                    if av_frame is None:
                        continue

                    bgr = av_frame.to_ndarray(format="bgr24")
                    height, width = bgr.shape[:2]
                    jpeg = _encode_jpeg(bgr)
                    pts_ms = int((av_frame.pts or 0) * float(video_stream.time_base) * 1000)

                    ref = await put_frame(stream_id, frame_id, jpeg)
                    msg = FrameRef(
                        tenant_id=tenant_id,
                        stream_id=stream_id,
                        frame_id=frame_id,
                        ts_capture_ns=time.time_ns(),
                        frame_ref=ref,
                        width=width,
                        height=height,
                        pts_ms=pts_ms,
                        fps_target=float(fps),
                        encoding="jpeg",
                    )
                    await bus.send(settings.topic_frames_raw, msg, key=stream_id)

                    frame_id += 1
                    loop_frames += 1
                    total_sent += 1

                    elapsed = time.monotonic() - prev_time
                    wait = interval - elapsed
                    if wait > 0:
                        await asyncio.sleep(wait)
                    prev_time = time.monotonic()

            log.info(
                "uploaded_video_loop_complete",
                video_path=video_path,
                stream_id=stream_id,
                loop=loop_num + 1,
                frames=loop_frames,
            )
        finally:
            container.close()

    log.info(
        "uploaded_video_ingested",
        video_path=video_path,
        tenant_id=tenant_id,
        stream_id=stream_id,
        total_frames=total_sent,
    )
    return total_sent


async def ingest_uploaded_video(
    video_path: str,
    tenant_id: str,
    stream_id: str,
    fps: int,
    loops: int,
    cleanup: bool = True,
) -> None:
    try:
        await inject_video_file(video_path, tenant_id, stream_id, fps=fps, loops=loops)
    except Exception as exc:  # noqa: BLE001
        log.exception(
            "uploaded_video_ingest_failed",
            video_path=video_path,
            tenant_id=tenant_id,
            stream_id=stream_id,
            err=str(exc),
        )
    finally:
        if cleanup:
            try:
                os.unlink(video_path)
            except FileNotFoundError:
                pass
            except OSError as exc:
                log.warning("uploaded_video_cleanup_failed", video_path=video_path, err=str(exc))