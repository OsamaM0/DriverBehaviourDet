"""Prometheus metrics — defined once, imported everywhere."""
from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from packages.common.config import settings

# ── Pipeline / ingest ──────────────────────────────────────────────────────────
INGEST_FRAMES_TOTAL = Counter(
    "ingest_frames_total", "Decoded frames published to frames.raw", ["stream_id"]
)
INGEST_DROPS_TOTAL = Counter(
    "ingest_drops_total", "Frames dropped (sampling/backpressure)", ["stream_id", "reason"]
)
INGEST_DECODE_LATENCY = Histogram(
    "ingest_decode_latency_seconds", "Decode + publish latency", ["stream_id"],
    buckets=(0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0),
)
INGEST_FPS = Gauge("ingest_fps", "Effective FPS per stream", ["stream_id"])

# ── Inference ──────────────────────────────────────────────────────────────────
INFER_LATENCY = Histogram(
    "infer_latency_seconds", "Per-call inference latency (client-side, incl. queue)",
    ["model"],
    buckets=(0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0),
)
INFER_REQUESTS = Counter("infer_requests_total", "Inference requests", ["model", "result"])
INFER_BATCH_SIZE = Histogram(
    "infer_batch_size", "Client batch size submitted to Triton", ["model"],
    buckets=(1, 2, 4, 8, 16, 32, 64),
)

# ── Kafka ──────────────────────────────────────────────────────────────────────
KAFKA_PRODUCE_TOTAL = Counter("kafka_produce_total", "Kafka produce ok", ["topic"])
KAFKA_PRODUCE_ERRORS = Counter("kafka_produce_errors_total", "Kafka produce errors", ["topic"])
KAFKA_CONSUME_TOTAL = Counter("kafka_consume_total", "Kafka consume ok", ["topic", "group"])
KAFKA_CONSUME_ERRORS = Counter("kafka_consume_errors_total", "Kafka consume errors", ["topic", "group"])

# ── Fusion / state machine ─────────────────────────────────────────────────────
STATE_TRANSITIONS = Counter(
    "fusion_state_transitions_total", "State machine transitions", ["from_state", "to_state"]
)
ALERTS_EMITTED = Counter("alerts_emitted_total", "Alerts emitted", ["type", "severity"])
ALERTS_DEDUPED = Counter("alerts_deduped_total", "Alerts suppressed by dedupe/cooldown", ["type"])


def start_metrics_server(port: int | None = None) -> None:
    """Expose Prometheus metrics on :PROMETHEUS_PORT (per-process)."""
    start_http_server(port or settings.prometheus_port)
