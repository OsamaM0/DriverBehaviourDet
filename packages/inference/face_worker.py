"""
Face/landmark worker (CPU, MediaPipe Face Mesh).

Strategy:
  - Subscribe to `infer.results` (we want detector output) AND `frames.raw`,
    or simpler: subscribe to a derived `infer.face` topic populated by a tiny
    router. For v1 we keep things simple: subscribe to `frames.raw` directly
    and run face mesh on the *full frame*. ROI cropping using the detector's
    DriverBehaviour box is a TODO (next phase) — the interface is already
    there.
  - Emit `FaceLandmarks` on `infer.results` with EAR + head pose.

EAR (Eye Aspect Ratio) is a cheap drowsiness signal; the eye-state CNN
(separate worker) provides confirmation.
"""
from __future__ import annotations

import asyncio
import math

import cv2
import mediapipe as mp
import numpy as np
from mediapipe.tasks.python import vision as _mp_vision
from mediapipe.tasks.python.core import base_options as _mp_base

_FACE_MODEL = str(
    __import__("pathlib").Path(__file__).parent.parent.parent
    / "models" / "face_landmarker.task"
)

from packages.common.config import settings
from packages.common.frame_cache import get_frame
from packages.common.kafka import bus
from packages.common.obs import bootstrap
from packages.common.schemas import FaceLandmarks, FrameRef, HeadPose, InferenceEnvelope

log = bootstrap("face", metrics_port=9103)

# Mediapipe Face Mesh eye landmark indices (468-mesh). EAR uses 6 points/eye.
LEFT_EYE = (33, 160, 158, 133, 153, 144)
RIGHT_EYE = (362, 385, 387, 263, 373, 380)
NOSE_TIP = 1
CHIN = 152
LEFT_EYE_OUTER = 33
RIGHT_EYE_OUTER = 263
LEFT_MOUTH = 61
RIGHT_MOUTH = 291

CONCURRENCY = 8     # MediaPipe is CPU-bound; keep modest
GROUP_ID = "face"


def _ear(pts: np.ndarray, idx: tuple[int, ...]) -> float:
    p1, p2, p3, p4, p5, p6 = pts[list(idx)]
    a = np.linalg.norm(p2 - p6)
    b = np.linalg.norm(p3 - p5)
    c = np.linalg.norm(p1 - p4)
    return float((a + b) / (2.0 * c + 1e-6))


def _head_pose(pts: np.ndarray, w: int, h: int) -> HeadPose:
    """Tiny PnP head pose. Returns yaw/pitch/roll in degrees."""
    image_pts = np.array([
        pts[NOSE_TIP], pts[CHIN], pts[LEFT_EYE_OUTER],
        pts[RIGHT_EYE_OUTER], pts[LEFT_MOUTH], pts[RIGHT_MOUTH],
    ], dtype=np.float64)
    model_pts = np.array([
        (0.0, 0.0, 0.0),
        (0.0, -63.6, -12.5),
        (-43.3, 32.7, -26.0),
        (43.3, 32.7, -26.0),
        (-28.9, -28.9, -24.1),
        (28.9, -28.9, -24.1),
    ], dtype=np.float64)
    f = float(w)
    cam = np.array([[f, 0, w / 2], [0, f, h / 2], [0, 0, 1]], dtype=np.float64)
    ok, rvec, _ = cv2.solvePnP(model_pts, image_pts, cam, np.zeros((4, 1)))
    if not ok:
        return HeadPose(yaw=0, pitch=0, roll=0)
    rmat, _ = cv2.Rodrigues(rvec)
    sy = math.sqrt(rmat[0, 0] ** 2 + rmat[1, 0] ** 2)
    pitch = math.degrees(math.atan2(-rmat[2, 0], sy))
    yaw = math.degrees(math.atan2(rmat[1, 0], rmat[0, 0]))
    roll = math.degrees(math.atan2(rmat[2, 1], rmat[2, 2]))
    return HeadPose(yaw=yaw, pitch=pitch, roll=roll)


class _FaceEngine:
    def __init__(self) -> None:
        opts = _mp_vision.FaceLandmarkerOptions(
            base_options=_mp_base.BaseOptions(model_asset_path=_FACE_MODEL),
            num_faces=1,
            min_face_detection_confidence=0.5,
            min_face_presence_confidence=0.5,
            min_tracking_confidence=0.5,
        )
        self._mp = _mp_vision.FaceLandmarker.create_from_options(opts)

    def process(self, bgr: np.ndarray) -> FaceLandmarks | None:
        h, w = bgr.shape[:2]
        rgb = cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB)
        mp_img = mp.Image(image_format=mp.ImageFormat.SRGB, data=rgb)
        res = self._mp.detect(mp_img)
        if not res.face_landmarks:
            return None
        lms = res.face_landmarks[0]
        pts = np.array([(lm.x * w, lm.y * h) for lm in lms], dtype=np.float64)
        return FaceLandmarks(
            tenant_id="", stream_id="", frame_id=0, ts_capture_ns=0,  # filled in by caller
            landmarks_count=len(lms),
            ear_left=_ear(pts, LEFT_EYE),
            ear_right=_ear(pts, RIGHT_EYE),
            head_pose=_head_pose(pts, w, h),
        )


_engine: _FaceEngine | None = None


def engine() -> _FaceEngine:
    global _engine
    if _engine is None:
        _engine = _FaceEngine()
    return _engine


async def _handle(msg: FrameRef, sem: asyncio.Semaphore) -> None:
    async with sem:
        jpeg = await get_frame(msg.frame_ref)
        if jpeg is None:
            return
        arr = np.frombuffer(jpeg, dtype=np.uint8)
        bgr = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if bgr is None:
            return
        face = await asyncio.to_thread(engine().process, bgr)
        if face is None:
            return
        face = face.model_copy(update={
            "tenant_id": msg.tenant_id, "stream_id": msg.stream_id,
            "frame_id": msg.frame_id, "ts_capture_ns": msg.ts_capture_ns,
        })
        env = InferenceEnvelope(
            tenant_id=msg.tenant_id, stream_id=msg.stream_id,
            frame_id=msg.frame_id, ts_capture_ns=msg.ts_capture_ns,
            kind="face", face=face,
        )
        await bus.send(settings.topic_infer_results, env, key=msg.stream_id)


async def main() -> None:
    sem = asyncio.Semaphore(CONCURRENCY)

    async def handler(msg: FrameRef) -> None:
        await _handle(msg, sem)

    log.info("face_worker_starting")
    await bus.consume(
        topics=[settings.topic_frames_raw],
        group_id=GROUP_ID,
        model=FrameRef,
        handler=handler,
        max_in_flight=CONCURRENCY * 2,
    )


if __name__ == "__main__":
    asyncio.run(main())
