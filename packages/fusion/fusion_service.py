"""
Fusion service.

Subscribes to `infer.results`, maintains per-stream rolling windows, runs the
behaviour state machine, and publishes:
  - `events.behavior` for every transition
  - `events.alert` per generated alert (with dedupe key + cooldown)

Also writes back a Redis hint `stream:fps_hint:{stream_id}` so ingest can
escalate sampling rate when state ≠ NORMAL.
"""
from __future__ import annotations

import asyncio
import time
from collections import defaultdict

from packages.common.config import settings
from packages.common.kafka import bus
from packages.common.obs import bootstrap
from packages.common.obs.metrics import ALERTS_DEDUPED, ALERTS_EMITTED, STATE_TRANSITIONS
from packages.common.redis import get_redis
from packages.common.schemas import (
    Alert,
    BehaviorScores,
    BehaviorState,
    BehaviorStateName,
    InferenceEnvelope,
)
from packages.fusion.state_machine import StateRecord, Thresholds, commit, evaluate
from packages.fusion.temporal_window import WindowStore

log = bootstrap("fusion", metrics_port=9104)

GROUP_ID = "fusion"
WINDOW_MS = 5000
ALERT_COOLDOWN_MS = 30_000
EAR_DROWSY_THRESHOLD = 0.21          # eyes closed below this
DISTRACT_YAW_DEG = 30.0
DISTRACT_PITCH_DEG = 20.0


def _signals_from_envelope(env: InferenceEnvelope) -> dict[str, float]:
    """Translate any inference message into named per-frame signals."""
    out: dict[str, float] = {}
    if env.kind == "detection" and env.detection is not None:
        # take max conf per class
        per_cls: dict[str, float] = defaultdict(float)
        for b in env.detection.boxes:
            per_cls[b.cls_name] = max(per_cls[b.cls_name], b.conf)
        if "phone" in per_cls:        out["phone"] = per_cls["phone"]
        if "cigarette" in per_cls:    out["smoking"] = per_cls["cigarette"]
        if "foodItem" in per_cls:     out["eating"] = per_cls["foodItem"]
        if "seatbelt" in per_cls:     out["seatbelt"] = per_cls["seatbelt"]
        if "wheel" in per_cls:        out["wheel_visible"] = per_cls["wheel"]
    elif env.kind == "face" and env.face is not None:
        f = env.face
        if f.ear_left is not None and f.ear_right is not None:
            ear = (f.ear_left + f.ear_right) / 2.0
            # convert: lower EAR ⇒ higher drowsy probability (clamped)
            d = max(0.0, min(1.0, (EAR_DROWSY_THRESHOLD - ear) / EAR_DROWSY_THRESHOLD))
            out["drowsy_ear"] = d
        if f.head_pose is not None:
            yaw_off = max(0.0, abs(f.head_pose.yaw) - DISTRACT_YAW_DEG) / 30.0
            pitch_off = max(0.0, abs(f.head_pose.pitch) - DISTRACT_PITCH_DEG) / 20.0
            out["distracted"] = min(1.0, max(yaw_off, pitch_off))
    elif env.kind == "hand" and env.hand is not None:
        out["hand_on_wheel"] = 1.0 if env.hand.hand_on_wheel else 0.0
    elif env.kind == "eye" and env.eye is not None:
        out["drowsy_eye"] = env.eye.eyes_closed_prob
    return out


def _scores_from_window(win) -> BehaviorScores:
    # treat seatbelt as latest (probability seatbelt is present); default 1.0 (assume present)
    seatbelt = win.latest("seatbelt") if win.samples else 1.0
    drowsy = max(win.ewma.get("drowsy_ear", 0.0), win.ewma.get("drowsy_eye", 0.0))
    hand_on_wheel = win.fraction_above("hand_on_wheel", 0.5)
    return BehaviorScores(
        phone=win.fraction_above("phone", 0.5),
        seatbelt=seatbelt or 1.0,
        smoking=win.fraction_above("smoking", 0.5),
        eating=win.fraction_above("eating", 0.5),
        drowsy=drowsy,
        distracted=win.ewma.get("distracted", 0.0),
        hand_off_wheel=1.0 - hand_on_wheel,
    )


async def _set_fps_hint(stream_id: str, state: BehaviorStateName) -> None:
    fps = settings.ingest_escalated_fps if state != BehaviorStateName.NORMAL else settings.ingest_base_fps
    r = get_redis()
    await r.set(f"stream:fps_hint:{stream_id}", str(fps).encode(), ex=120)


def _dedupe_key(stream_id: str, alert_type: str, cooldown_ms: int) -> str:
    bucket = (time.time_ns() // 1_000_000) // cooldown_ms
    return f"{stream_id}:{alert_type}:{bucket}"


async def _maybe_emit_alert(
    state_rec: StateRecord,
    env: InferenceEnvelope,
    decision_state: BehaviorStateName,
    scores: BehaviorScores,
    alerts: list,
    cooldown_ms: int = ALERT_COOLDOWN_MS,
) -> None:
    now_ns = time.time_ns()
    for alert_type, severity in alerts:
        last = state_rec.last_alerts.get(alert_type, 0)
        if now_ns - last < cooldown_ms * 1_000_000:
            ALERTS_DEDUPED.labels(type=alert_type.value).inc()
            continue
        state_rec.last_alerts[alert_type] = now_ns
        a = Alert(
            tenant_id=env.tenant_id,
            stream_id=env.stream_id,
            frame_id=env.frame_id,
            ts_capture_ns=env.ts_capture_ns,
            type=alert_type,
            severity=severity,
            state=decision_state,
            dedupe_key=_dedupe_key(env.stream_id, alert_type.value, cooldown_ms),
            scores=scores,
        )
        await bus.send(settings.topic_events_alert, a, key=env.stream_id)
        ALERTS_EMITTED.labels(type=alert_type.value, severity=severity.value).inc()
        log.info("alert_emitted", stream_id=env.stream_id, type=alert_type.value, severity=severity.value)


async def main() -> None:
    store = WindowStore()
    states: dict[str, StateRecord] = defaultdict(StateRecord)
    th = Thresholds()  # TODO: per-tenant override

    async def handler(env: InferenceEnvelope) -> None:
        signals = _signals_from_envelope(env)
        if not signals:
            return
        win = store.get(env.stream_id)
        win.add(signals, ts_ns=env.ts_capture_ns or time.time_ns(), window_ms=WINDOW_MS)

        scores = _scores_from_window(win)
        rec = states[env.stream_id]
        decision = evaluate(rec, scores, th)

        if decision.transitioned:
            STATE_TRANSITIONS.labels(from_state=rec.state.value, to_state=decision.new_state.value).inc()
            commit(rec, decision)
            bs = BehaviorState(
                tenant_id=env.tenant_id,
                stream_id=env.stream_id,
                frame_id=env.frame_id,
                ts_capture_ns=env.ts_capture_ns,
                state=rec.state,
                prev_state=rec.prev,
                scores=scores,
                dwell_ms=0,
                window_ms=WINDOW_MS,
            )
            await bus.send(settings.topic_events_behavior, bs, key=env.stream_id)
            await _set_fps_hint(env.stream_id, rec.state)

        if decision.alerts:
            await _maybe_emit_alert(rec, env, decision.new_state, scores, decision.alerts)

    log.info("fusion_starting", window_ms=WINDOW_MS)
    await bus.consume(
        topics=[settings.topic_infer_results],
        group_id=GROUP_ID,
        model=InferenceEnvelope,
        handler=handler,
        max_in_flight=64,
    )


if __name__ == "__main__":
    asyncio.run(main())
