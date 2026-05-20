"""
Versioned event schemas (Pydantic v2).

All events shared across pipeline stages live here. Bump `SCHEMA_VERSION` on any
breaking change and add a migration note. We mirror these to .proto when we add
a schema registry — keep field names/order stable.
"""
from __future__ import annotations

import time
import uuid
from enum import Enum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

SCHEMA_VERSION = 1


def _now_ns() -> int:
    return time.time_ns()


def _new_id() -> str:
    return str(uuid.uuid4())


class _Base(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=False)
    schema_version: int = SCHEMA_VERSION
    tenant_id: str
    stream_id: str
    frame_id: int                       # monotonic per stream (set by ingest)
    ts_capture_ns: int                  # camera-side capture time (best effort)
    ts_emit_ns: int = Field(default_factory=_now_ns)
    trace_id: str | None = None         # OTel trace_id hex


# ── Stage 1: ingest → router ────────────────────────────────────────────────
class FrameRef(_Base):
    """Reference to a decoded frame (bytes live in Redis/MinIO)."""
    frame_ref: str                      # e.g. "redis://frame:{stream_id}:{frame_id}" or "s3://..."
    width: int
    height: int
    pts_ms: int                         # presentation timestamp (ms from stream start)
    fps_target: float
    encoding: Literal["jpeg", "raw_bgr", "raw_rgb"] = "jpeg"
    hints: dict[str, str] = Field(default_factory=dict)


# ── Stage 2: per-model inference results ────────────────────────────────────
class Box(BaseModel):
    model_config = ConfigDict(extra="forbid")
    cls: int
    cls_name: str
    conf: float
    xyxy: tuple[float, float, float, float]   # absolute pixel coords on original frame


class DetectionResult(_Base):
    source: Literal["rfdetr"] = "rfdetr"
    model_version: str
    frame_width: int | None = None
    frame_height: int | None = None
    boxes: list[Box]
    latency_ms: float


class HeadPose(BaseModel):
    yaw: float
    pitch: float
    roll: float


class FaceLandmarks(_Base):
    source: Literal["mediapipe_facemesh"] = "mediapipe_facemesh"
    landmarks_count: int                 # we don't push 478 floats over Kafka — kept in Redis if needed
    ear_left: float | None
    ear_right: float | None
    head_pose: HeadPose | None
    gaze_xy: tuple[float, float] | None = None


class HandResult(_Base):
    source: Literal["hand_detector"] = "hand_detector"
    hands: list[Box]
    hand_on_wheel: bool
    iou_with_wheel: float


class EyeStateResult(_Base):
    source: Literal["eye_state_cnn"] = "eye_state_cnn"
    eyes_closed_prob: float


class InferenceEnvelope(_Base):
    """Generic envelope on `infer.results` so fusion has one consumer."""
    kind: Literal["detection", "face", "hand", "eye"]
    detection: DetectionResult | None = None
    face: FaceLandmarks | None = None
    hand: HandResult | None = None
    eye: EyeStateResult | None = None


# ── Stage 3: fusion → behaviour state ───────────────────────────────────────
class BehaviorStateName(str, Enum):
    NORMAL = "NORMAL"
    DISTRACTED = "DISTRACTED"
    DROWSY = "DROWSY"
    UNSAFE = "UNSAFE"
    CRITICAL = "CRITICAL"
    RECOVERED = "RECOVERED"


class BehaviorScores(BaseModel):
    phone: float = 0.0
    seatbelt: float = 0.0     # probability seatbelt PRESENT
    smoking: float = 0.0
    eating: float = 0.0
    drowsy: float = 0.0
    distracted: float = 0.0
    hand_off_wheel: float = 0.0


class BehaviorState(_Base):
    state: BehaviorStateName
    prev_state: BehaviorStateName
    scores: BehaviorScores
    dwell_ms: int                        # how long we've been in `state`
    window_ms: int                       # size of fusion window used


# ── Stage 4: alerts + evidence ──────────────────────────────────────────────
class AlertSeverity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AlertType(str, Enum):
    NO_SEATBELT = "no_seatbelt"
    PHONE_USE = "phone_use"
    SMOKING = "smoking"
    EATING = "eating"
    DROWSINESS = "drowsiness"
    DISTRACTION = "distraction"
    HANDS_OFF_WHEEL = "hands_off_wheel"


class Alert(_Base):
    alert_id: str = Field(default_factory=_new_id)
    type: AlertType
    severity: AlertSeverity
    state: BehaviorStateName
    dedupe_key: str                      # `${stream_id}:${type}:${minute_bucket}`
    evidence_window_s: int = 2          # ±N seconds (configurable per tenant)
    scores: BehaviorScores
    boxes: list[Box] = Field(default_factory=list)
    frame_width: int | None = None
    frame_height: int | None = None
    note: str | None = None


class EvidenceReady(_Base):
    alert_id: str
    s3_uri: str
    duration_s: float
    codec: str = "h264"
    width: int
    height: int


__all__ = [
    "SCHEMA_VERSION",
    "FrameRef",
    "Box",
    "DetectionResult",
    "FaceLandmarks",
    "HeadPose",
    "HandResult",
    "EyeStateResult",
    "InferenceEnvelope",
    "BehaviorStateName",
    "BehaviorScores",
    "BehaviorState",
    "AlertSeverity",
    "AlertType",
    "Alert",
    "EvidenceReady",
]
