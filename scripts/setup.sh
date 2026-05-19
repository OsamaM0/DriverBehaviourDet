#!/usr/bin/env bash
# =============================================================================
# setup.sh — One-time environment bootstrap for DriverBehaviourDet on RunPod
#
# Usage:
#   bash scripts/setup.sh          # standard setup (ONNX backend)
#   bash scripts/setup.sh --trt    # also build TensorRT FP16 plan (~10 min on L4)
#
# What this installs (actual services, not just Python connectors):
#   • PostgreSQL 16      — apt package  → port 5432
#   • Redis              — apt package  → port 6379
#   • Redpanda           — apt package  → Kafka:19092 · SchemaReg:18081
#   • MinIO server       — binary       → S3:9000 · Console:9001
#   • Python project     — pip install -e ".[dev]"
#   • gdown              — pip install (for model download)
#
# Run once. Idempotent — skips steps that already completed.
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
MODELS_DIR="$PROJECT_DIR/models"
LOGS_DIR="$PROJECT_DIR/logs"
PID_DIR="$LOGS_DIR/pids"
DATA_DIR="$PROJECT_DIR/data"
GDRIVE_FILE_ID="1NmZdondieMuqWeKOkWYPSvxBWdnUI56C"

# ── Colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'
log()  { echo -e "${BLUE}[SETUP]${NC} $*"; }
ok()   { echo -e "${GREEN}[  OK ]${NC} $*"; }
warn() { echo -e "${YELLOW}[ WARN]${NC} $*"; }
die()  { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }
step() { echo -e "\n${BOLD}${CYAN}━━━ $* ━━━${NC}"; }

# ── Flags ─────────────────────────────────────────────────────────────────────
BUILD_TRT=false
[[ "${1:-}" == "--trt" ]] && BUILD_TRT=true

# ── GPU detection ─────────────────────────────────────────────────────────────
_GPU_NAME=$(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null | head -1 | xargs || echo "No GPU detected")
_GPU_VRAM_MB=$(nvidia-smi --query-gpu=memory.total --format=csv,noheader,nounits 2>/dev/null | head -1 | tr -d ' ' || echo "0")

echo -e "${BOLD}${GREEN}"
echo "  ╔══════════════════════════════════════════════════════╗"
echo "  ║   DriverBehaviourDet — RunPod Setup                  ║"
echo "  ║   GPU: ${_GPU_NAME}  (${_GPU_VRAM_MB} MB VRAM)"
echo "  ╚══════════════════════════════════════════════════════╝"
echo -e "${NC}"

# ── Workspace directories ─────────────────────────────────────────────────────
mkdir -p "$MODELS_DIR" "$LOGS_DIR" "$PID_DIR" "$DATA_DIR/minio"
# Ensure log dir exists before anything tries to write to it
mkdir -p "$LOGS_DIR" "$PID_DIR"

# ─────────────────────────────────────────────────────────────────────────────
step "1 · System packages (PostgreSQL 16, Redis, ffmpeg)"
# ─────────────────────────────────────────────────────────────────────────────
apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
  postgresql-16 postgresql-client-16 \
  redis-server \
  netcat-openbsd \
  procps \
  ffmpeg 2>&1 | grep -E "^(Setting up|Unpacking|Get:|Reading)" | tail -10 || true

# Docker overlay quirk: reinstall codec libs to ensure .so files are present
DEBIAN_FRONTEND=noninteractive apt-get --reinstall install -y --no-install-recommends \
  libvpx9 libx264-164 libx265-199 libmp3lame0 libmpg123-0t64 libflac12t64 \
  2>&1 | tail -3 || true

ok "System packages ready"

# ─────────────────────────────────────────────────────────────────────────────
step "2 · Redpanda (Kafka + Schema Registry)"
# ─────────────────────────────────────────────────────────────────────────────
if ! command -v redpanda &>/dev/null; then
  log "Adding Redpanda apt repository..."
  curl -1sLf 'https://dl.redpanda.com/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.deb.sh' | bash
  DEBIAN_FRONTEND=noninteractive apt-get install -y redpanda 2>&1 | tail -5
  ok "Redpanda installed"
else
  ok "Redpanda already installed: $(redpanda --version 2>/dev/null | head -1 || echo 'unknown')"
fi

log "Writing Redpanda config → /etc/redpanda/redpanda.yaml"
mkdir -p /etc/redpanda /var/lib/redpanda/data
cat > /etc/redpanda/redpanda.yaml << 'RPEOF'
redpanda:
  developer_mode: true
  # smp=1 limits Redpanda to one CPU shard — dramatically speeds up first-start
  smp: 1
  data_directory: /var/lib/redpanda/data
  seed_servers: []
  kafka_api:
    - address: 0.0.0.0
      port: 19092
  advertised_kafka_api:
    - address: 127.0.0.1
      port: 19092
  rpc_server:
    address: 0.0.0.0
    port: 33145
  advertised_rpc_api:
    address: 127.0.0.1
    port: 33145
  admin:
    - address: 0.0.0.0
      port: 9644
schema_registry:
  schema_registry_api:
    - address: 0.0.0.0
      port: 18081
pandaproxy:
  pandaproxy_api:
    - address: 0.0.0.0
      port: 18082
rpk:
  kafka_api:
    brokers:
      - 127.0.0.1:19092
  admin_api:
    addresses:
      - 127.0.0.1:9644
RPEOF
ok "Redpanda configured  (Kafka :19092 · Schema Registry :18081)"

# ─────────────────────────────────────────────────────────────────────────────
step "3 · MinIO server + client"
# ─────────────────────────────────────────────────────────────────────────────
if [[ ! -x /usr/local/bin/minio ]]; then
  log "Downloading MinIO server binary..."
  curl -fsSL "https://dl.min.io/server/minio/release/linux-amd64/minio" \
    -o /usr/local/bin/minio
  chmod +x /usr/local/bin/minio
  ok "MinIO server → /usr/local/bin/minio"
else
  ok "MinIO server already present"
fi

if [[ ! -x /usr/local/bin/mc ]]; then
  log "Downloading MinIO client (mc)..."
  curl -fsSL "https://dl.min.io/client/mc/release/linux-amd64/mc" \
    -o /usr/local/bin/mc
  chmod +x /usr/local/bin/mc
  ok "mc → /usr/local/bin/mc"
else
  ok "mc already present"
fi

# ─────────────────────────────────────────────────────────────────────────────
step "4 · Python dependencies"
# ─────────────────────────────────────────────────────────────────────────────
log "Installing gdown (Google Drive downloader)..."
pip3 install --quiet --upgrade --break-system-packages gdown

log "Installing project + dev extras (this may take ~2 min)..."
cd "$PROJECT_DIR"
pip3 install --quiet --break-system-packages -e ".[dev]"
ok "Python packages installed"

# ─────────────────────────────────────────────────────────────────────────────
step "5 · Environment file"
# ─────────────────────────────────────────────────────────────────────────────
if [[ ! -f "$PROJECT_DIR/.env" ]]; then
  cp "$PROJECT_DIR/.env.example" "$PROJECT_DIR/.env"
  ok ".env created from .env.example"
else
  ok ".env already exists — skipping"
fi

# ─────────────────────────────────────────────────────────────────────────────
step "6 · Triton model repository"
# ─────────────────────────────────────────────────────────────────────────────
log "Setting up $MODELS_DIR directory structure..."
mkdir -p "$MODELS_DIR/rfdetr_driver_behaviour_onnx/1"
mkdir -p "$MODELS_DIR/rfdetr_driver_behaviour/1"

# Detect Triton's actual --model-repository path (may differ from $MODELS_DIR)
# The RunPod DeepStream image starts Triton at container boot pointing to /workspace/models
_triton_repo=$(pgrep -a tritonserver 2>/dev/null \
  | grep -oP '(?<=--model-repository=)\S+' | head -1 || true)
_triton_repo="${_triton_repo:-$MODELS_DIR}"
log "Triton model repository: $_triton_repo"
mkdir -p "$_triton_repo/rfdetr_driver_behaviour_onnx/1"
mkdir -p "$_triton_repo/rfdetr_driver_behaviour/1"

# Write ONNX config using KIND_AUTO so Triton selects GPU if available, CPU otherwise
_write_onnx_config() {
  cat > "$1/rfdetr_driver_behaviour_onnx/config.pbtxt" << 'CFEOF'
name: "rfdetr_driver_behaviour_onnx"
platform: "onnxruntime_onnx"
max_batch_size: 0

instance_group [
  {
    count: 1
    kind: KIND_AUTO
  }
]
CFEOF
}
_write_onnx_config "$MODELS_DIR"
# Also write to Triton's active repo if different
[[ "$_triton_repo" != "$MODELS_DIR" ]] && _write_onnx_config "$_triton_repo"

# Copy TRT config as-is (used only after optional TRT build)
cp "$PROJECT_DIR/triton/model_repository/rfdetr_driver_behaviour/config.pbtxt" \
   "$MODELS_DIR/rfdetr_driver_behaviour/"
[[ "$_triton_repo" != "$MODELS_DIR" ]] && \
  cp "$PROJECT_DIR/triton/model_repository/rfdetr_driver_behaviour/config.pbtxt" \
     "$_triton_repo/rfdetr_driver_behaviour/"

ok "Triton model dirs ready"

# ─────────────────────────────────────────────────────────────────────────────
step "7 · Download ONNX model from Google Drive"
# ─────────────────────────────────────────────────────────────────────────────
ONNX_DEST="$MODELS_DIR/rfdetr_driver_behaviour_onnx/1/model.onnx"
_TRITON_ONNX="$_triton_repo/rfdetr_driver_behaviour_onnx/1/model.onnx"
if [[ -f "$ONNX_DEST" ]]; then
  ok "model.onnx already downloaded ($(du -sh "$ONNX_DEST" | cut -f1)) — skipping"
else
  log "Downloading model (ID: $GDRIVE_FILE_ID) → $ONNX_DEST"
  log "This may take a few minutes depending on model size..."
  gdown "https://drive.google.com/uc?id=${GDRIVE_FILE_ID}" -O "$ONNX_DEST"
  [[ -f "$ONNX_DEST" ]] || die "Download failed — $ONNX_DEST not found"
  ok "Model downloaded ($(du -sh "$ONNX_DEST" | cut -f1))"
fi
# Ensure Triton's repo also has the model (hard-link to avoid duplicate disk use)
if [[ "$_TRITON_ONNX" != "$ONNX_DEST" && ! -f "$_TRITON_ONNX" ]]; then
  ln "$ONNX_DEST" "$_TRITON_ONNX" 2>/dev/null || cp "$ONNX_DEST" "$_TRITON_ONNX"
  log "Synced model.onnx → $_TRITON_ONNX"
fi

# Download MediaPipe FaceLandmarker model (required by face_worker)
_FACE_MODEL="$MODELS_DIR/face_landmarker.task"
if [[ -f "$_FACE_MODEL" ]]; then
  ok "face_landmarker.task already downloaded — skipping"
else
  log "Downloading MediaPipe face_landmarker.task..."
  curl -fsSL \
    "https://storage.googleapis.com/mediapipe-models/face_landmarker/face_landmarker/float16/1/face_landmarker.task" \
    -o "$_FACE_MODEL"
  ok "face_landmarker.task downloaded ($(du -sh "$_FACE_MODEL" | cut -f1))"
fi

# ─────────────────────────────────────────────────────────────────────────────
step "8 · Load model into Triton"
# ─────────────────────────────────────────────────────────────────────────────
log "Waiting for Triton server on :8000..."
for i in $(seq 1 30); do
  if curl -fsS http://127.0.0.1:8000/v2/health/live &>/dev/null; then
    ok "Triton is live"; break
  fi
  echo "  Waiting ($i/30)..."
  sleep 3
  [[ $i -eq 30 ]] && die "Triton not responding after 90s — check $LOGS_DIR/triton.log"
done

log "Loading rfdetr_driver_behaviour_onnx (explicit model-control mode)..."
HTTP_CODE=$(curl -s -o /tmp/triton_load.json -w "%{http_code}" -X POST \
  "http://127.0.0.1:8000/v2/repository/models/rfdetr_driver_behaviour_onnx/load")
if [[ "$HTTP_CODE" == "200" ]]; then
  ok "rfdetr_driver_behaviour_onnx loaded into Triton"
else
  warn "Triton load returned HTTP $HTTP_CODE — model may take extra time to warm up"
  warn "Response: $(cat /tmp/triton_load.json 2>/dev/null || echo 'n/a')"
fi

# ─────────────────────────────────────────────────────────────────────────────
step "9 · Start Redpanda and create Kafka topics"
# ─────────────────────────────────────────────────────────────────────────────
if nc -z 127.0.0.1 19092 2>/dev/null; then
  ok "Redpanda already running on :19092"
else
  log "Starting Redpanda broker..."

  # Kill any previous Redpanda attempts that may be holding AIO slots.
  pkill -f "redpanda --redpanda-cfg" 2>/dev/null || true
  sleep 1

  # NOTE: --reactor-backend epoll only affects the *network* event loop.
  # Redpanda's disk storage queue still calls io_setup(N) for Linux native AIO
  # regardless of the reactor backend. In a RunPod container the kernel's
  # /proc/sys/fs/aio-max-nr is read-only and Triton typically consumes most of
  # the 65536 default capacity, leaving fewer than 1024 slots for Redpanda.
  #
  # Fix: supply a pre-written I/O properties file (skips the AIO benchmark that
  # would otherwise probe disk with large nr_events) and cap --max-io-requests
  # to 1 so io_setup() asks for only 1 slot instead of the default 1024.

  # Write a minimal I/O properties file so Seastar skips auto-tuning.
  cat > /etc/redpanda/io-properties.yaml << 'IOPROPS'
disks:
  - mountpoint: /var/lib/redpanda/data
    read_iops: 150000
    read_bandwidth: 1073741824
    write_iops: 100000
    write_bandwidth: 1073741824
IOPROPS

  nohup redpanda --redpanda-cfg /etc/redpanda/redpanda.yaml --overprovisioned \
    --reactor-backend epoll \
    -c 1 \
    --io-properties-file /etc/redpanda/io-properties.yaml \
    > "$LOGS_DIR/redpanda.log" 2>&1 &
  echo $! > "$PID_DIR/redpanda.pid"

  log "Waiting for Kafka listener on :19092 (up to 6 min)..."
  _rp_ready=0
  for i in $(seq 1 120); do
    if nc -z 127.0.0.1 19092 2>/dev/null; then ok "Redpanda ready"; _rp_ready=1; break; fi
    echo "  Waiting ($i/120)..."
    sleep 3
  done
  if [[ $_rp_ready -eq 0 ]]; then
    die "Redpanda did not start after 6 min. Last log:\n$(tail -20 $LOGS_DIR/redpanda.log 2>/dev/null || echo 'no log')"
  fi
fi

log "Creating Kafka topics (idempotent)..."
# Verify broker is actually reachable before attempting topic creation
if ! nc -z 127.0.0.1 19092 2>/dev/null; then
  die "Redpanda broker not reachable on :19092 — cannot create topics"
fi
TOPICS=(
  frames.raw infer.detector infer.face infer.eye infer.hand
  infer.results events.behavior events.alert events.evidence_ready dlq
)
for T in "${TOPICS[@]}"; do
  result=$(rpk topic create "$T" --brokers 127.0.0.1:19092 \
    --partitions 4 --replicas 1 -X tls.enabled=false 2>&1)
  if echo "$result" | grep -qiE "TOPIC_ALREADY_EXISTS|already exists|OK"; then
    echo "  Exists:  $T"
  elif echo "$result" | grep -qiE "Created"; then
    echo "  Created: $T"
  else
    warn "  $T: $result"
  fi
done
ok "Kafka topics ready (${#TOPICS[@]} topics)"

# ─────────────────────────────────────────────────────────────────────────────
step "10 · Start MinIO and create S3 buckets"
# ─────────────────────────────────────────────────────────────────────────────
if nc -z 127.0.0.1 9000 2>/dev/null; then
  ok "MinIO already running on :9000"
else
  log "Starting MinIO server..."
  MINIO_ROOT_USER=minio MINIO_ROOT_PASSWORD=minio12345 \
    nohup minio server "$DATA_DIR/minio" \
      --address 0.0.0.0:9000 \
      --console-address 0.0.0.0:9001 \
      > "$LOGS_DIR/minio.log" 2>&1 &
  echo $! > "$PID_DIR/minio.pid"

  for i in $(seq 1 15); do
    if nc -z 127.0.0.1 9000 2>/dev/null; then ok "MinIO ready"; break; fi
    sleep 2
    [[ $i -eq 15 ]] && warn "MinIO not ready — check $LOGS_DIR/minio.log"
  done
fi

log "Creating S3 buckets..."
mc alias set runpod http://127.0.0.1:9000 minio minio12345 &>/dev/null
mc mb --ignore-existing runpod/driver-evidence && echo "  Bucket: driver-evidence" || true
mc mb --ignore-existing runpod/driver-models   && echo "  Bucket: driver-models"   || true
ok "MinIO buckets ready"

# ─────────────────────────────────────────────────────────────────────────────
step "11 · PostgreSQL — start, configure, create database"
# ─────────────────────────────────────────────────────────────────────────────
if ! nc -z 127.0.0.1 5432 2>/dev/null; then
  log "Starting PostgreSQL 16..."
  pg_ctlcluster 16 main start || service postgresql start
  sleep 2
fi

log "Creating database driver_analytics and setting postgres password..."
su -c "psql -v ON_ERROR_STOP=0 -c \"ALTER USER postgres WITH PASSWORD 'postgres';\"" \
  postgres &>/dev/null || true
su -c "createdb driver_analytics 2>/dev/null || true" postgres
ok "PostgreSQL ready  (user: postgres / postgres · db: driver_analytics)"

# ─────────────────────────────────────────────────────────────────────────────
step "12 · Redis — start"
# ─────────────────────────────────────────────────────────────────────────────
if nc -z 127.0.0.1 6379 2>/dev/null; then
  ok "Redis already running on :6379"
else
  service redis-server start
  ok "Redis started"
fi

# ─────────────────────────────────────────────────────────────────────────────
step "13 · DB schema creation + dev seed"
# ─────────────────────────────────────────────────────────────────────────────
log "Creating tables and seeding dev tenant/stream..."
cd "$PROJECT_DIR"
python3 scripts/seed_dev.py && ok "Database seeded" || warn "Seed skipped (already present)"

# ─────────────────────────────────────────────────────────────────────────────
step "14 · (Optional) TensorRT FP16 plan"
# ─────────────────────────────────────────────────────────────────────────────
if [[ "$BUILD_TRT" == true ]]; then
  log "Building TensorRT FP16 plan on: $_GPU_NAME (${_GPU_VRAM_MB} MB VRAM)..."

  # Tune workspace and batch sizes to available VRAM.
  # ≥20 GB  → RTX 3090 / A5000 / L4 / A6000 etc. — use 8 GB workspace, bs up to 16
  # 10–20 GB → RTX 3080 / A4000 etc.              — use 4 GB workspace, bs up to 8
  # <10 GB   → RTX 3070 / T4 etc.                 — use 2 GB workspace, bs up to 4
  if [[ $_GPU_VRAM_MB -ge 20000 ]]; then
    _trt_workspace=8192; _trt_opt_bs=8; _trt_max_bs=16
  elif [[ $_GPU_VRAM_MB -ge 10000 ]]; then
    _trt_workspace=4096; _trt_opt_bs=4; _trt_max_bs=8
  else
    _trt_workspace=2048; _trt_opt_bs=2; _trt_max_bs=4
  fi

  # --noTF32 disables TF32 for FP32 matmuls. On data-centre GPUs (L4, A100 etc.)
  # TF32 can hurt reproducibility; on consumer Ampere (RTX 3090) it helps throughput.
  _trt_no_tf32=false
  if echo "$_GPU_NAME" | grep -qiE "L4|L40|A10|A30|A100|H100"; then
    _trt_no_tf32=true
  fi

  log "TRT settings: workspace=${_trt_workspace}MB  opt_bs=${_trt_opt_bs}  max_bs=${_trt_max_bs}  no_tf32=${_trt_no_tf32}"

  ONNX_PATH="$ONNX_DEST" \
  OUT_PLAN="$MODELS_DIR/rfdetr_driver_behaviour/1/model.plan" \
  MIN_BS=1 OPT_BS="$_trt_opt_bs" MAX_BS="$_trt_max_bs" \
  WORKSPACE_MB="$_trt_workspace" \
  NO_TF32="$_trt_no_tf32" \
    bash "$PROJECT_DIR/triton/scripts/onnx_to_trt.sh"

  log "Loading TRT model into Triton..."
  HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
    "http://127.0.0.1:8000/v2/repository/models/rfdetr_driver_behaviour/load")
  [[ "$HTTP_CODE" == "200" ]] \
    && ok  "rfdetr_driver_behaviour (TRT) loaded" \
    || warn "TRT load returned HTTP $HTTP_CODE"

  log "Switching detector worker to use TRT model..."
  sed -i 's/^TRITON_MODEL_DETECTOR=.*/TRITON_MODEL_DETECTOR=rfdetr_driver_behaviour/' \
    "$PROJECT_DIR/.env"
  ok "TensorRT plan built and .env updated"
else
  log "Skipping TRT build. To build later:  bash scripts/setup.sh --trt"
fi

# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}${BOLD}"
echo "  ╔══════════════════════════════════════════════════════════════╗"
echo "  ║   Setup complete!                                            ║"
echo "  ╠══════════════════════════════════════════════════════════════╣"
echo "  ║   Triton Server     →  http://localhost:8000/v2              ║"
echo "  ║   MinIO Console     →  http://localhost:9001                 ║"
echo "  ║   Redpanda (Kafka)  →  localhost:19092                       ║"
echo "  ║   PostgreSQL        →  localhost:5432  (driver_analytics)    ║"
echo "  ║   Redis             →  localhost:6379                        ║"
echo "  ╠══════════════════════════════════════════════════════════════╣"
echo "  ║   Next:  bash scripts/manage.sh                              ║"
echo "  ╚══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"
