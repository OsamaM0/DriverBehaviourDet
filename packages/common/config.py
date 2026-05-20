"""
Centralized settings (pydantic-settings).

All env vars in `.env.example` map here. Each service imports `settings` and
reads only what it needs. Never hard-code endpoints in workers.
"""
from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Identity
    service_name: str = "driver-analytics"
    # Use APP_ENV to avoid clashing with the system ENV variable set by
    # the DeepStream container (ENV=/etc/shinit_v2).
    env: Literal["dev", "staging", "prod"] = Field(default="dev", validation_alias="APP_ENV")
    log_level: str = "INFO"
    log_json: bool = True

    # Kafka
    kafka_bootstrap: str = "localhost:19092"
    kafka_client_id: str = "driver-analytics"
    kafka_schema_registry: str | None = "http://localhost:18081"
    topic_frames_raw: str = "frames.raw"
    topic_infer_detector: str = "infer.detector"
    topic_infer_face: str = "infer.face"
    topic_infer_eye: str = "infer.eye"
    topic_infer_hand: str = "infer.hand"
    topic_infer_results: str = "infer.results"
    topic_events_behavior: str = "events.behavior"
    topic_events_alert: str = "events.alert"
    topic_evidence_ready: str = "events.evidence_ready"
    topic_dlq: str = "dlq"

    # Redis
    redis_url: str = "redis://localhost:6379/0"
    frame_cache_ttl_sec: int = 180

    # Postgres
    database_url: str = (
        "postgresql+asyncpg://postgres:postgres@localhost:5432/driver_analytics"
    )

    # S3 / MinIO
    s3_endpoint: str | None = "http://localhost:9000"
    s3_region: str = "us-east-1"
    s3_access_key: str | None = "minio"
    s3_secret_key: str | None = "minio12345"
    s3_bucket_evidence: str = "driver-evidence"
    s3_bucket_models: str = "driver-models"

    # Triton
    triton_url: str = "localhost:8001"
    triton_http_url: str = "http://localhost:8000"
    triton_model_detector: str = "rfdetr_driver_behaviour_onnx"
    triton_model_detector_version: str = ""
    triton_model_eye: str = "eye_state"
    triton_model_hand: str = "hand_detector"

    # Pipeline
    detector_input_size: int = 576
    detector_conf_threshold: float = 0.35
    ingest_base_fps: int = 8
    ingest_escalated_fps: int = 15
    ingest_motion_gate: bool = True

    # Observability
    otel_exporter_otlp_endpoint: str = "http://localhost:4317"
    otel_service_name: str = "driver-analytics"
    prometheus_port: int = Field(default=9100)

    # API
    api_jwt_secret: str = "change-me"
    api_cors_origins: str = "*"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
