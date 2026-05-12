# Driver Behaviour Analytics Platform

Cloud-native, event-driven, multi-tenant video analytics platform for driver
monitoring. Ingests many RTSP/MJPEG streams, decodes once per stream, runs a
shared multi-model GPU inference pool (NVIDIA Triton with dynamic batching),
fuses signals over time via a per-driver state machine, and emits alerts with
±10 s evidence clips to S3.

See `/memories/session/plan.md` (architect plan) for the full design.

## Quick start (dev)

```bash
make dev-up          # docker compose: Redpanda, Redis, Postgres, MinIO, MediaMTX, Triton, Prom/Grafana
make seed            # create dev tenant + stream
make run-ingest      # run one ingest worker locally
make run-detector    # run one detector worker locally
make run-fusion      # run fusion service
make run-api         # run FastAPI gateway
```

## Stages

```
RTSP/MJPEG → Ingest → frames.raw → Detector (Triton) ─┐
                                       Face (MediaPipe)├─► infer.results → Fusion ─► Alerts/Evidence ─► API
                                       Eye / Hand     ─┘
```

Frame *bytes* live in Redis (short TTL); only *refs* travel on Kafka.
Partition by `stream_id` to preserve per-stream order end-to-end.

## Layout

- `packages/common`     — schemas, kafka/redis/obs/config helpers, frame cache
- `packages/ingest`     — RTSP/MJPEG workers, ring buffer
- `packages/inference`  — Triton client + per-model workers + pre/post
- `packages/fusion`     — temporal window + state machine
- `packages/alerts`     — dedupe, cooldown, sinks
- `packages/evidence`   — clipper to S3
- `packages/api`        — FastAPI REST + WS
- `packages/control_plane` — stream lease + model registry
- `triton/`             — model repository + TRT conversion + parity tests
- `deploy/`             — Dockerfiles + Helm chart (on-prem + RunPod values)
- `observability/`      — OTel collector, Prom rules, Grafana dashboards
- `tests/`              — unit / integration / e2e / load
