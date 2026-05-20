from pathlib import Path

from fastapi.testclient import TestClient

import packages.api.main as api_main
from packages.api.routes import events
from packages.api.routes import streams


def test_upload_stream_video_queues_background_ingest(monkeypatch):
    captured: dict[str, object] = {}

    monkeypatch.setattr(api_main, "bootstrap", lambda *args, **kwargs: None)

    async def fake_close() -> None:
        return None

    monkeypatch.setattr(api_main.bus, "close", fake_close)

    async def fake_ingest(video_path: str, tenant_id: str, stream_id: str, fps: int, loops: int) -> None:
        captured["video_path"] = video_path
        captured["tenant_id"] = tenant_id
        captured["stream_id"] = stream_id
        captured["fps"] = fps
        captured["loops"] = loops
        captured["exists_during_task"] = Path(video_path).exists()
        Path(video_path).unlink(missing_ok=True)

    monkeypatch.setattr(streams, "ingest_uploaded_video", fake_ingest)

    with TestClient(api_main.app) as client:
        response = client.post(
            "/v1/streams/upload?tenant_id=acme&fps=8&loops=1",
            content=b"fake-video-bytes",
            headers={
                "content-type": "video/mp4",
                "x-filename": "test.mp4",
            },
        )

    assert response.status_code == 202
    payload = response.json()
    assert payload["ok"] is True
    assert payload["tenant_id"] == "acme"
    assert payload["stream_id"].startswith("upload-test-")
    assert payload["bytes_received"] == len(b"fake-video-bytes")
    assert captured == {
        "video_path": captured["video_path"],
        "tenant_id": "acme",
        "stream_id": payload["stream_id"],
        "fps": 8,
        "loops": 1,
        "exists_during_task": True,
    }


def test_upload_stream_video_rejects_invalid_fps(monkeypatch):
    monkeypatch.setattr(api_main, "bootstrap", lambda *args, **kwargs: None)

    async def fake_close() -> None:
        return None

    monkeypatch.setattr(api_main.bus, "close", fake_close)

    with TestClient(api_main.app) as client:
        response = client.post(
            "/v1/streams/upload?tenant_id=acme&fps=0",
            content=b"fake-video-bytes",
            headers={
                "content-type": "video/mp4",
                "x-filename": "test.mp4",
            },
        )

    assert response.status_code == 400
    assert response.json()["detail"] == "fps must be between 1 and 60"


def test_get_events_passes_stream_filter(monkeypatch):
    monkeypatch.setattr(api_main, "bootstrap", lambda *args, **kwargs: None)

    async def fake_close() -> None:
        return None

    monkeypatch.setattr(api_main.bus, "close", fake_close)

    captured: dict[str, object] = {}

    class Row:
        id = "alert-1"
        tenant_id = "acme"
        stream_id = "upload-test-1"
        type = "distraction"
        severity = "medium"
        state = "UNSAFE"
        ts_capture_ns = 123
        scores = {"distracted": 0.8}
        evidence_uri = None

    async def fake_list_recent_alerts(tenant_id: str, limit: int = 100, stream_id: str | None = None):
        captured["tenant_id"] = tenant_id
        captured["limit"] = limit
        captured["stream_id"] = stream_id
        return [Row()]

    monkeypatch.setattr(events, "list_recent_alerts", fake_list_recent_alerts)

    with TestClient(api_main.app) as client:
        response = client.get("/v1/events?tenant_id=acme&stream_id=upload-test-1&limit=5")

    assert response.status_code == 200
    assert captured == {"tenant_id": "acme", "limit": 5, "stream_id": "upload-test-1"}
    assert response.json() == [{
        "alert_id": "alert-1",
        "tenant_id": "acme",
        "stream_id": "upload-test-1",
        "type": "distraction",
        "severity": "medium",
        "state": "UNSAFE",
        "ts_capture_ns": 123,
        "scores": {"distracted": 0.8},
        "evidence_uri": None,
    }]