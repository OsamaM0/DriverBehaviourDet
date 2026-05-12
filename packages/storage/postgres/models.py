"""SQLAlchemy ORM models. Migrations via Alembic (see alembic/ — TODO)."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    JSON,
    BigInteger,
    DateTime,
    Float,
    ForeignKey,
    Index,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Tenant(Base):
    __tablename__ = "tenants"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(255))
    config: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class Stream(Base):
    __tablename__ = "streams"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenants.id"), index=True)
    url: Mapped[str] = mapped_column(String(1024))
    protocol: Mapped[str] = mapped_column(String(16))            # rtsp | mjpeg
    enabled: Mapped[bool] = mapped_column(default=True)
    driver_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class AlertRow(Base):
    __tablename__ = "alerts"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)         # alert_id
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    stream_id: Mapped[str] = mapped_column(String(64), index=True)
    type: Mapped[str] = mapped_column(String(32))
    severity: Mapped[str] = mapped_column(String(16))
    state: Mapped[str] = mapped_column(String(16))
    dedupe_key: Mapped[str] = mapped_column(String(255))
    ts_capture_ns: Mapped[int] = mapped_column(BigInteger)
    scores: Mapped[dict] = mapped_column(JSON)
    evidence_uri: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("dedupe_key", name="uq_alerts_dedupe"),
        Index("ix_alerts_tenant_ts", "tenant_id", "ts_capture_ns"),
    )


class BehaviorEventRow(Base):
    __tablename__ = "behavior_events"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    stream_id: Mapped[str] = mapped_column(String(64), index=True)
    state: Mapped[str] = mapped_column(String(16))
    prev_state: Mapped[str] = mapped_column(String(16))
    scores: Mapped[dict] = mapped_column(JSON)
    ts_capture_ns: Mapped[int] = mapped_column(BigInteger)
    dwell_ms: Mapped[int] = mapped_column(BigInteger)
    window_ms: Mapped[int] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class ModelVersion(Base):
    __tablename__ = "model_versions"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(128), index=True)
    version: Mapped[str] = mapped_column(String(64))
    backend: Mapped[str] = mapped_column(String(32))                 # onnx | tensorrt
    s3_uri: Mapped[str] = mapped_column(String(1024))
    metrics: Mapped[dict] = mapped_column(JSON, default=dict)
    is_active: Mapped[bool] = mapped_column(default=False)
    canary_pct: Mapped[float] = mapped_column(Float, default=0.0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
