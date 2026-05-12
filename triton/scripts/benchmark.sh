#!/usr/bin/env bash
# Triton perf_analyzer wrapper to validate batching + throughput.
set -euo pipefail
MODEL="${MODEL:-rfdetr_driver_behaviour}"
URL="${URL:-localhost:8001}"
CONCURRENCY="${CONCURRENCY:-1:32:4}"      # start:end:step
SHAPE="${SHAPE:-input:3,576,576}"

perf_analyzer \
  -m "$MODEL" \
  -u "$URL" -i grpc \
  --shape "$SHAPE" \
  --concurrency-range "$CONCURRENCY" \
  --measurement-mode count_windows \
  --collect-metrics \
  --percentile=95
