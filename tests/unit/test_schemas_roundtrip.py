from packages.common.schemas import (
    Alert,
    AlertSeverity,
    AlertType,
    BehaviorScores,
    BehaviorState,
    BehaviorStateName,
    Box,
    DetectionResult,
    EvidenceReady,
    FrameRef,
    InferenceEnvelope,
)


def _frame_ref() -> FrameRef:
    return FrameRef(
        tenant_id="t1", stream_id="s1", frame_id=1,
        ts_capture_ns=1_000_000_000, ts_emit_ns=1_000_001_000,
        frame_ref="redis://frame:s1:1",
        width=576, height=576, encoding="jpeg",
        pts_ms=125,
        fps_target=8.0,
    )


def test_frameref_roundtrip() -> None:
    f = _frame_ref()
    assert FrameRef.model_validate_json(f.model_dump_json()) == f


def test_inference_envelope_detection() -> None:
    env = InferenceEnvelope(
        tenant_id="t1", stream_id="s1", frame_id=1,
        ts_capture_ns=1, ts_emit_ns=2,
        kind="detection",
        detection=DetectionResult(
            tenant_id="t1",
            stream_id="s1",
            frame_id=1,
            ts_capture_ns=1,
            model_version="rfdetr-onnx",
            latency_ms=12.5,
            boxes=[
                Box(cls=3, cls_name="phone", conf=0.9, xyxy=(10.0, 20.0, 30.0, 40.0))
            ],
        ),
    )
    j = env.model_dump_json()
    assert InferenceEnvelope.model_validate_json(j).detection.boxes[0].cls_name == "phone"


def test_alert_minimal() -> None:
    a = Alert(
        tenant_id="t1", stream_id="s1", frame_id=1,
        ts_capture_ns=1, ts_emit_ns=2,
        alert_id="a1",
        type=AlertType.PHONE_USE,
        severity=AlertSeverity.MEDIUM,
        state=BehaviorStateName.UNSAFE,
        dedupe_key="k",
        scores=BehaviorScores(phone=0.9, seatbelt=1.0),
    )
    assert Alert.model_validate_json(a.model_dump_json()).alert_id == "a1"


def test_state_payload() -> None:
    st = BehaviorState(
        tenant_id="t1", stream_id="s1", frame_id=1,
        ts_capture_ns=1, ts_emit_ns=2,
        state=BehaviorStateName.DROWSY, prev_state=BehaviorStateName.NORMAL,
        scores=BehaviorScores(drowsy=0.8, seatbelt=1.0),
        dwell_ms=2500,
        window_ms=5000,
    )
    assert BehaviorState.model_validate_json(st.model_dump_json()).state == BehaviorStateName.DROWSY


def test_evidence_ready_roundtrip() -> None:
    e = EvidenceReady(
        tenant_id="t1", stream_id="s1", frame_id=1,
        ts_capture_ns=1, ts_emit_ns=2,
        alert_id="a1",
        s3_uri="s3://driver-evidence/t1/s1/a1.mp4",
        duration_s=20.0,
        width=576,
        height=576,
    )
    assert EvidenceReady.model_validate_json(e.model_dump_json()).alert_id == "a1"
