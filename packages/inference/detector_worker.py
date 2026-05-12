"""
RF-DETR detector worker.

Consumes `frames.raw`, fetches the JPEG from the frame cache, preprocesses,
calls Triton (server-side dynamic batching), postprocesses to `Box`es, and
publishes a `DetectionResult` wrapped in `InferenceEnvelope` on
`infer.results`.

Per-worker concurrency: many in-flight requests, all multiplexed over one
gRPC connection. Triton coalesces them into batches.
"""
from __future__ import annotations

import asyncio
import time

from packages.common.config import settings
from packages.common.frame_cache import get_frame
from packages.common.kafka import bus
from packages.common.obs import bootstrap
from packages.common.schemas import DetectionResult, FrameRef, InferenceEnvelope
from packages.inference.postprocess import postprocess_single
from packages.inference.preprocess import preprocess_jpeg
from packages.inference.triton_client import get_triton

log = bootstrap("detector")

CONCURRENCY = 32
GROUP_ID = "detector"


async def _handle(msg: FrameRef, sem: asyncio.Semaphore) -> None:
    async with sem:
        try:
            jpeg = await get_frame(msg.frame_ref)
            if jpeg is None:
                log.warning("frame_cache_miss", frame_ref=msg.frame_ref, frame_id=msg.frame_id)
                return
            nchw, ow, oh = preprocess_jpeg(jpeg)
            t0 = time.perf_counter()
            triton = get_triton()
            meta = await triton.metadata(settings.triton_model_detector, settings.triton_model_detector_version)
            input_name = meta.inputs[0][0]
            outputs = await triton.infer(
                settings.triton_model_detector,
                {input_name: nchw},
                version=settings.triton_model_detector_version,
            )
            boxes = postprocess_single(outputs, ow, oh)
            latency_ms = (time.perf_counter() - t0) * 1000.0

            det = DetectionResult(
                tenant_id=msg.tenant_id,
                stream_id=msg.stream_id,
                frame_id=msg.frame_id,
                ts_capture_ns=msg.ts_capture_ns,
                model_version=settings.triton_model_detector_version or "latest",
                boxes=boxes,
                latency_ms=latency_ms,
            )
            env = InferenceEnvelope(
                tenant_id=msg.tenant_id,
                stream_id=msg.stream_id,
                frame_id=msg.frame_id,
                ts_capture_ns=msg.ts_capture_ns,
                kind="detection",
                detection=det,
            )
            await bus.send(settings.topic_infer_results, env, key=msg.stream_id)
        except Exception as e:  # noqa: BLE001
            log.exception("detector_handler_failed", err=str(e))


async def main() -> None:
    sem = asyncio.Semaphore(CONCURRENCY)

    async def handler(msg: FrameRef) -> None:
        await _handle(msg, sem)

    log.info("detector_starting", model=settings.triton_model_detector)
    await bus.consume(
        topics=[settings.topic_frames_raw],
        group_id=GROUP_ID,
        model=FrameRef,
        handler=handler,
        max_in_flight=CONCURRENCY * 2,
    )


if __name__ == "__main__":
    asyncio.run(main())
