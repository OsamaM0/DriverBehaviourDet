import json

from fastapi.testclient import TestClient

import packages.api.main as api_main
import packages.api.ws.live_events as live_events
from packages.common.schemas import Box, DetectionResult, InferenceEnvelope


def test_ws_inference_streams_detection_envelope(monkeypatch):
    monkeypatch.setattr(api_main, "bootstrap", lambda *args, **kwargs: None)

    async def fake_close() -> None:
        return None

    monkeypatch.setattr(api_main.bus, "close", fake_close)

    async def fake_iter_topic(topics, group_id):
        env = InferenceEnvelope(
            tenant_id="acme",
            stream_id="demo-stream",
            frame_id=7,
            ts_capture_ns=123,
            kind="detection",
            detection=DetectionResult(
                tenant_id="acme",
                stream_id="demo-stream",
                frame_id=7,
                ts_capture_ns=123,
                model_version="test",
                frame_width=640,
                frame_height=360,
                boxes=[Box(cls=3, cls_name="phone", conf=0.88, xyxy=(10, 20, 30, 40))],
                latency_ms=5.0,
            ),
        )
        yield env.model_dump_json().encode()

    monkeypatch.setattr(live_events, "iter_topic", fake_iter_topic)

    with TestClient(api_main.app) as client:
        with client.websocket_connect("/v1/ws/inference?tenant_id=acme&stream_id=demo-stream") as ws:
            payload = json.loads(ws.receive_text())

    assert payload["kind"] == "detection"
    assert payload["stream_id"] == "demo-stream"
    assert payload["detection"]["boxes"][0]["cls_name"] == "phone"