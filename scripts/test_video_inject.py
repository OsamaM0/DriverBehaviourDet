"""
scripts/test_video_inject.py
────────────────────────────
End-to-end pipeline test using a local MP4 file.

Bypasses the RTSP ingest worker and directly injects frames from the video
into the same Kafka / Redis path that rtsp_worker would produce, so the rest
of the pipeline (detector → fusion → alerts → API) runs exactly as in production.

Usage
─────
  # from the project root (with .env loaded):
  python3 scripts/test_video_inject.py --video train/test.mp4

  # custom tenant / stream / speed:
  python3 scripts/test_video_inject.py \
      --video  train/test.mp4          \
      --tenant acme                    \
      --stream acme-cab-001            \
      --fps    8                       \
      --loop   1

  # then poll alerts:
  curl "http://localhost:8080/events?tenant_id=acme"
"""
from __future__ import annotations

import argparse
import asyncio
import sys
import time

import av
import cv2
import numpy as np

# ── make sure we can import the packages module from the project root ─────────
import os
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ".")

from packages.common.config import settings
from packages.common.frame_cache import put_frame
from packages.common.kafka import bus
from packages.common.obs import bootstrap
from packages.common.schemas import FrameRef

log = bootstrap("video-inject", metrics_port=9106)

JPEG_QUALITY = 80


def _encode_jpeg(bgr: np.ndarray) -> bytes:
    ok, buf = cv2.imencode(
        ".jpg", bgr,
        [int(cv2.IMWRITE_JPEG_QUALITY), JPEG_QUALITY],
    )
    if not ok:
        raise RuntimeError("JPEG encode failed")
    return buf.tobytes()


async def inject(
    video_path: str,
    tenant_id: str,
    stream_id: str,
    fps: int,
    loops: int,
) -> None:
    interval = 1.0 / fps
    frame_id = 0
    total_sent = 0

    print(f"\n{'─'*60}")
    print(f"  Video injector starting")
    print(f"  File    : {video_path}")
    print(f"  Tenant  : {tenant_id}")
    print(f"  Stream  : {stream_id}")
    print(f"  FPS     : {fps}  (interval {interval*1000:.0f} ms)")
    print(f"  Loops   : {loops}")
    print(f"  Kafka   : {settings.kafka_bootstrap}")
    print(f"  Redis   : {settings.redis_url}")
    print(f"{'─'*60}\n")

    producer = await bus.producer()
    try:
        for loop_num in range(loops):
            print(f"[loop {loop_num+1}/{loops}] Opening {video_path}...")
            container = av.open(video_path)
            video_stream = container.streams.video[0]

            frame_pts = 0
            prev_time = time.monotonic()
            loop_frames = 0

            for packet in container.demux(video_stream):
                for av_frame in packet.decode():
                    if av_frame is None:
                        continue

                    # Convert to numpy BGR (OpenCV format)
                    bgr = av_frame.to_ndarray(format="bgr24")
                    h, w = bgr.shape[:2]
                    jpeg = _encode_jpeg(bgr)
                    pts_ms = int((av_frame.pts or 0) * float(video_stream.time_base) * 1000)

                    # Store JPEG in Redis frame cache (2s TTL)
                    ref = await put_frame(stream_id, frame_id, jpeg)

                    # Build FrameRef and publish to Kafka
                    msg = FrameRef(
                        tenant_id=tenant_id,
                        stream_id=stream_id,
                        frame_id=frame_id,
                        ts_capture_ns=time.time_ns(),
                        frame_ref=ref,
                        width=w,
                        height=h,
                        pts_ms=pts_ms,
                        fps_target=float(fps),
                        encoding="jpeg",
                    )
                    await producer.send_and_wait(
                        settings.topic_frames_raw,
                        msg.model_dump_json().encode(),
                        key=stream_id.encode(),
                    )

                    frame_id += 1
                    loop_frames += 1
                    total_sent += 1

                    # Print progress every 30 frames
                    if loop_frames % 30 == 0:
                        print(
                            f"  [frame {frame_id:>5}]  {w}×{h}  "
                            f"{len(jpeg)/1024:.1f} KB  pts={pts_ms} ms"
                        )

                    # Rate-limit to simulate the target FPS
                    elapsed = time.monotonic() - prev_time
                    wait = interval - elapsed
                    if wait > 0:
                        await asyncio.sleep(wait)
                    prev_time = time.monotonic()

            container.close()
            print(f"[loop {loop_num+1}] Done — {loop_frames} frames sent\n")
    finally:
        await bus.close()

    print(f"{'─'*60}")
    print(f"  Injection complete — {total_sent} frames published to '{settings.topic_frames_raw}'")
    print(f"  Poll alerts: curl \"http://localhost:8080/events?tenant_id={tenant_id}\"")
    print(f"  Watch live:  curl \"http://localhost:8080/events?tenant_id={tenant_id}&limit=5\"")
    print(f"{'─'*60}\n")


async def wait_for_alerts(tenant_id: str, timeout: int = 60) -> None:
    """Poll the REST API until at least one alert appears or timeout."""
    import httpx

    url = f"http://localhost:8080/v1/events?tenant_id={tenant_id}&limit=20"
    print(f"\nPolling for alerts on {url}")
    print("(waiting up to %ds for the pipeline to process frames)\n" % timeout)

    deadline = time.monotonic() + timeout
    last_count = 0

    while time.monotonic() < deadline:
        try:
            resp = httpx.get(url, timeout=5)
            if resp.status_code == 200:
                alerts = resp.json()
                if len(alerts) > last_count:
                    for a in alerts[last_count:]:
                        print(
                            f"  🚨 ALERT  type={a['type']}  severity={a['severity']}"
                            f"  state={a['state']}  stream={a['stream_id']}"
                        )
                    last_count = len(alerts)
                else:
                    print(f"  ... waiting ({int(deadline - time.monotonic())}s left, {last_count} alerts so far)")
            else:
                print(f"  API returned {resp.status_code}")
        except Exception as exc:
            print(f"  API unreachable: {exc}")
        await asyncio.sleep(5)

    if last_count == 0:
        print("\n  No alerts detected in timeout window.")
        print("  Check worker logs:  tail -50 /workspace/logs/detector.log")
        print("  Check fusion log :  tail -50 /workspace/logs/fusion.log")
    else:
        print(f"\n  Total alerts received: {last_count}")


def main() -> None:
    p = argparse.ArgumentParser(description="Inject local MP4 frames into the pipeline")
    p.add_argument("--video",  default="train/test.mp4",  help="Path to MP4 file")
    p.add_argument("--tenant", default="acme",            help="Tenant ID")
    p.add_argument("--stream", default="acme-cab-001",    help="Stream ID")
    p.add_argument("--fps",    default=8,  type=int,      help="Injection rate (frames/sec)")
    p.add_argument("--loop",   default=1,  type=int,      help="Number of times to loop video")
    p.add_argument("--no-poll", action="store_true",      help="Skip polling alerts after inject")
    args = p.parse_args()

    async def _run() -> None:
        await inject(args.video, args.tenant, args.stream, args.fps, args.loop)
        if not args.no_poll:
            await wait_for_alerts(args.tenant, timeout=90)

    asyncio.run(_run())


if __name__ == "__main__":
    main()
