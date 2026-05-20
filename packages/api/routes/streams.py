import os
import tempfile
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, Header, HTTPException, Request, status
from pydantic import BaseModel
from sqlalchemy import select

from packages.common.config import settings
from packages.ingest.file_injector import ingest_uploaded_video, make_upload_stream_id
from packages.storage.postgres.dao import SessionLocal
from packages.storage.postgres.models import Stream

router = APIRouter()


class StreamIn(BaseModel):
    id: str
    tenant_id: str
    url: str
    protocol: str = "rtsp"
    driver_id: str | None = None


@router.post("")
async def create_stream(s_in: StreamIn) -> dict:
    async with SessionLocal() as s:
        s.add(Stream(**s_in.model_dump()))
        await s.commit()
    return {"ok": True}


@router.post("/upload", status_code=status.HTTP_202_ACCEPTED)
async def upload_stream_video(
    request: Request,
    background_tasks: BackgroundTasks,
    tenant_id: str,
    stream_id: str | None = None,
    fps: int = settings.ingest_base_fps,
    loops: int = 1,
    x_filename: str | None = Header(default=None),
) -> dict:
    if fps < 1 or fps > 60:
        raise HTTPException(400, "fps must be between 1 and 60")
    if loops < 1 or loops > 10:
        raise HTTPException(400, "loops must be between 1 and 10")

    content_type = (request.headers.get("content-type") or "").split(";", 1)[0].strip().lower()
    if content_type and not content_type.startswith("video/") and content_type != "application/octet-stream":
        raise HTTPException(415, "upload must be a video file")

    filename = Path(x_filename or "upload.mp4").name or "upload.mp4"
    resolved_stream_id = stream_id.strip() if stream_id and stream_id.strip() else make_upload_stream_id(filename)

    fd, temp_path = tempfile.mkstemp(prefix="upload-", suffix=Path(filename).suffix or ".mp4")
    bytes_written = 0
    try:
        with os.fdopen(fd, "wb") as temp_file:
            async for chunk in request.stream():
                if not chunk:
                    continue
                temp_file.write(chunk)
                bytes_written += len(chunk)
    except Exception:
        try:
            os.unlink(temp_path)
        except FileNotFoundError:
            pass
        raise

    if bytes_written == 0:
        os.unlink(temp_path)
        raise HTTPException(400, "empty upload")

    background_tasks.add_task(
        ingest_uploaded_video,
        temp_path,
        tenant_id,
        resolved_stream_id,
        fps,
        loops,
    )
    return {
        "ok": True,
        "tenant_id": tenant_id,
        "stream_id": resolved_stream_id,
        "filename": filename,
        "bytes_received": bytes_written,
        "fps": fps,
        "loops": loops,
        "status": "queued",
    }


@router.get("")
async def list_streams(tenant_id: str | None = None) -> list[dict]:
    async with SessionLocal() as s:
        stmt = select(Stream)
        if tenant_id:
            stmt = stmt.where(Stream.tenant_id == tenant_id)
        rows = (await s.execute(stmt)).scalars().all()
        return [
            {"id": r.id, "tenant_id": r.tenant_id, "url": r.url,
             "protocol": r.protocol, "driver_id": r.driver_id, "enabled": r.enabled}
            for r in rows
        ]
