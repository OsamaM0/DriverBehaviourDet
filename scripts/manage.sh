#!/usr/bin/env bash
# =============================================================================
# manage.sh — Start or stop the DriverBehaviourDet project on RunPod
#
# Usage:
#   bash scripts/manage.sh               # start everything (default)
#   bash scripts/manage.sh start         # start everything
#   bash scripts/manage.sh stop          # stop workers (infra stays up)
#   bash scripts/manage.sh stop --full   # stop workers + all infra services
#   bash scripts/manage.sh status        # show running workers & services
#
# Workers managed:
#   ingest · detector · face · fusion · alerts · api (uvicorn)
#
# Infrastructure managed:
#   Redis · PostgreSQL · Redpanda (Kafka) · MinIO  +  Triton (already running)
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LOGS_DIR="$PROJECT_DIR/logs"
PID_DIR="$LOGS_DIR/pids"
DATA_DIR="$PROJECT_DIR/data"
ENV_FILE="$PROJECT_DIR/.env"
MEDIAMTX_BIN="$DATA_DIR/mediamtx"
MEDIAMTX_VERSION="v1.9.3"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'
log()  { echo -e "${BLUE}[MANAGE]${NC} $*"; }
ok()   { echo -e "${GREEN}[  OK ]${NC} $*"; }
warn() { echo -e "${YELLOW}[ WARN]${NC} $*"; }
die()  { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

ACTION="${1:-start}"
FULL_STOP=false
[[ "${2:-}" == "--full" ]] && FULL_STOP=true

# ─────────────────────────────────────────────────────────────────────────────
# Helper: check if a PID file exists and the process is alive
# ─────────────────────────────────────────────────────────────────────────────
is_running() {
  local pid_file="$PID_DIR/$1.pid"
  [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null
}

# ─────────────────────────────────────────────────────────────────────────────
# start_worker NAME -m module.path [args...]
#   Launches a Python worker as a background process.
#   Logs → $LOGS_DIR/<name>.log   PID → $PID_DIR/<name>.pid
# ─────────────────────────────────────────────────────────────────────────────
start_worker() {
  local name="$1"; shift
  local log_file="$LOGS_DIR/${name}.log"
  local pid_file="$PID_DIR/${name}.pid"

  if is_running "$name"; then
    warn "$name is already running (PID $(cat "$pid_file"))"
    return
  fi

  log "Starting ${name}..."
  cd "$PROJECT_DIR"                    # workers resolve .env relative to cwd
  nohup python3 "$@" >> "$log_file" 2>&1 &
  echo $! > "$pid_file"

  sleep 1
  if is_running "$name"; then
    ok "${name} started  (PID $(cat "$pid_file"))  →  tail -f $log_file"
  else
    warn "${name} may have crashed immediately — check $log_file"
    tail -5 "$log_file" 2>/dev/null | sed 's/^/    /' || true
  fi
}

# ─────────────────────────────────────────────────────────────────────────────
# stop_worker NAME  — graceful SIGTERM → 10s wait → SIGKILL fallback
# ─────────────────────────────────────────────────────────────────────────────
stop_worker() {
  local name="$1"
  local pid_file="$PID_DIR/${name}.pid"

  if [[ ! -f "$pid_file" ]]; then
    warn "$name: no PID file — already stopped or never started"
    return
  fi

  local pid
  pid=$(cat "$pid_file")

  if kill -0 "$pid" 2>/dev/null; then
    log "Stopping ${name} (PID ${pid})..."
    kill -TERM "$pid" 2>/dev/null || true
    local count=0
    while kill -0 "$pid" 2>/dev/null && [[ $count -lt 10 ]]; do
      sleep 1
      count=$((count + 1))
    done
    if kill -0 "$pid" 2>/dev/null; then
      kill -KILL "$pid" 2>/dev/null || true
      warn "${name} force-killed (SIGKILL)"
    else
      ok "${name} stopped"
    fi
  else
    warn "${name} (PID $pid) was not running"
  fi
  rm -f "$pid_file"
}

# ─────────────────────────────────────────────────────────────────────────────
# Ensure all infrastructure services are running before starting workers
# ─────────────────────────────────────────────────────────────────────────────
ensure_infra() {
  echo -e "\n${CYAN}── Infrastructure ──${NC}"

  # Redis
  if nc -z 127.0.0.1 6379 2>/dev/null; then
    ok "Redis          :6379  already running"
  else
    log "Starting Redis..."
    service redis-server start
    ok "Redis started"
  fi

  # PostgreSQL
  if nc -z 127.0.0.1 5432 2>/dev/null; then
    ok "PostgreSQL     :5432  already running"
  else
    log "Starting PostgreSQL 16..."
    pg_ctlcluster 16 main start 2>/dev/null || service postgresql start
    sleep 2
    ok "PostgreSQL started"
  fi

  # Redpanda (Kafka)
  if nc -z 127.0.0.1 19092 2>/dev/null; then
    ok "Redpanda       :19092 already running"
  else
    log "Starting Redpanda broker..."
    mkdir -p "$LOGS_DIR" "$PID_DIR" /var/lib/redpanda/data
    nohup redpanda --redpanda-cfg /etc/redpanda/redpanda.yaml --overprovisioned \
      --reactor-backend epoll \
      -c 1 \
      --io-properties-file /etc/redpanda/io-properties.yaml \
      > "$LOGS_DIR/redpanda.log" 2>&1 &
    echo $! > "$PID_DIR/redpanda.pid"

    log "Waiting for Kafka listener on :19092..."
    for i in $(seq 1 20); do
      nc -z 127.0.0.1 19092 2>/dev/null && { ok "Redpanda ready"; break; }
      sleep 3
      [[ $i -eq 20 ]] && warn "Redpanda not ready — check $LOGS_DIR/redpanda.log"
    done
  fi

  # MediaMTX — RTSP relay server
  if nc -z 127.0.0.1 8554 2>/dev/null; then
    ok "MediaMTX       :8554  already running"
  else
    log "Setting up MediaMTX RTSP server..."
    mkdir -p "$DATA_DIR"
    if [[ ! -x "$MEDIAMTX_BIN" ]]; then
      log "Downloading MediaMTX ${MEDIAMTX_VERSION}..."
      curl -fsSL "https://github.com/bluenviron/mediamtx/releases/download/${MEDIAMTX_VERSION}/mediamtx_${MEDIAMTX_VERSION}_linux_amd64.tar.gz" \
        | tar -xzf - -C "$DATA_DIR" mediamtx
      chmod +x "$MEDIAMTX_BIN"
      ok "MediaMTX downloaded"
    fi
    # Minimal config — accept any publisher on any path
    cat > "$DATA_DIR/mediamtx.yml" << 'MTXEOF'
paths:
  driver_synth: {}
MTXEOF
    nohup "$MEDIAMTX_BIN" "$DATA_DIR/mediamtx.yml" \
      > "$LOGS_DIR/mediamtx.log" 2>&1 &
    echo $! > "$PID_DIR/mediamtx.pid"
    for i in $(seq 1 10); do
      nc -z 127.0.0.1 8554 2>/dev/null && { ok "MediaMTX ready  :8554"; break; }
      sleep 1
      [[ $i -eq 10 ]] && warn "MediaMTX not ready — check $LOGS_DIR/mediamtx.log"
    done
  fi

  # ffmpeg — continuously loops test.mp4 and pushes it to MediaMTX as RTSP
  if is_running "ffmpeg"; then
    ok "ffmpeg RTSP    already serving driver_synth"
  else
    log "Starting ffmpeg RTSP loop..."
    nohup ffmpeg -re -stream_loop -1 \
      -i "${PROJECT_DIR}/train/test.mp4" \
      -c:v copy -an -f rtsp \
      rtsp://localhost:8554/driver_synth \
      > "$LOGS_DIR/ffmpeg.log" 2>&1 &
    echo $! > "$PID_DIR/ffmpeg.pid"
    sleep 3
    if is_running "ffmpeg"; then
      ok "ffmpeg RTSP    serving driver_synth"
    else
      warn "ffmpeg may have crashed — check $LOGS_DIR/ffmpeg.log"
      tail -5 "$LOGS_DIR/ffmpeg.log" 2>/dev/null | sed 's/^/    /' || true
    fi
  fi

  # MinIO
  if nc -z 127.0.0.1 9000 2>/dev/null; then
    ok "MinIO          :9000  already running"
  else
    log "Starting MinIO..."
    mkdir -p "$DATA_DIR/minio"
    MINIO_ROOT_USER=minio MINIO_ROOT_PASSWORD=minio12345 \
      nohup minio server "$DATA_DIR/minio" \
        --address 0.0.0.0:9000 \
        --console-address 0.0.0.0:9001 \
        > "$LOGS_DIR/minio.log" 2>&1 &
    echo $! > "$PID_DIR/minio.pid"

    for i in $(seq 1 10); do
      nc -z 127.0.0.1 9000 2>/dev/null && { ok "MinIO ready"; break; }
      sleep 2
      [[ $i -eq 10 ]] && warn "MinIO not ready — check $LOGS_DIR/minio.log"
    done
  fi
}

# ─────────────────────────────────────────────────────────────────────────────
# Verify Triton is live and the ONNX detector model is loaded
# ─────────────────────────────────────────────────────────────────────────────
ensure_triton_model() {
  echo -e "\n${CYAN}── Triton ──${NC}"

  if ! curl -fsS http://127.0.0.1:8000/v2/health/live &>/dev/null; then
    die "Triton is not running!  The container start-cmd should have started it.
    Check: tail -50 $LOGS_DIR/triton.log"
  fi
  ok "Triton Server  :8000  live"

  # Detect which model is configured in .env
  local model_name
  model_name=$(grep '^TRITON_MODEL_DETECTOR=' "$ENV_FILE" 2>/dev/null \
    | cut -d= -f2 | tr -d ' "' || echo "rfdetr_driver_behaviour_onnx")
  [[ -z "$model_name" ]] && model_name="rfdetr_driver_behaviour_onnx"

  local status
  status=$(curl -s -o /dev/null -w "%{http_code}" \
    "http://127.0.0.1:8000/v2/models/${model_name}")

  if [[ "$status" == "200" ]]; then
    ok "Model '$model_name' already loaded in Triton"
  else
    log "Loading model '${model_name}' into Triton (explicit mode)..."
    HTTP_CODE=$(curl -s -o /tmp/triton_load.json -w "%{http_code}" -X POST \
      "http://127.0.0.1:8000/v2/repository/models/${model_name}/load")
    if [[ "$HTTP_CODE" == "200" ]]; then
      ok "Model '${model_name}' loaded"
    else
      warn "Triton load returned HTTP ${HTTP_CODE} — workers will retry on startup"
      warn "Response: $(cat /tmp/triton_load.json 2>/dev/null | head -1 || echo 'n/a')"
    fi
  fi
}

# ─────────────────────────────────────────────────────────────────────────────
# do_start
# ─────────────────────────────────────────────────────────────────────────────
do_start() {
  [[ -f "$ENV_FILE" ]] || die ".env not found at $ENV_FILE
  → Run 'bash scripts/setup.sh' first"

  mkdir -p "$LOGS_DIR" "$PID_DIR"

  echo -e "${BOLD}${GREEN}"
  echo "  ╔══════════════════════════════════════════════════════╗"
  echo "  ║   DriverBehaviourDet — Starting                      ║"
  echo "  ╚══════════════════════════════════════════════════════╝"
  echo -e "${NC}"

  ensure_infra
  ensure_triton_model

  echo -e "\n${CYAN}── Workers ──${NC}"

  # Start workers in pipeline order: ingest → inference → fusion → alerts → api
  # Read stream config from .env (with sensible defaults)
  _stream_id=$(grep '^STREAM_ID='      "$ENV_FILE" 2>/dev/null | cut -d= -f2 | tr -d '"' || true)
  _tenant_id=$(grep '^STREAM_TENANT_ID=' "$ENV_FILE" 2>/dev/null | cut -d= -f2 | tr -d '"' || true)
  _stream_url=$(grep '^STREAM_URL='    "$ENV_FILE" 2>/dev/null | cut -d= -f2 | tr -d '"' || true)
  export STREAM_ID="${_stream_id:-acme-cab-001}"
  export TENANT_ID="${_tenant_id:-acme}"
  export STREAM_URL="${_stream_url:-rtsp://localhost:8554/driver_synth}"
  log "Ingest → STREAM_ID=$STREAM_ID  STREAM_URL=$STREAM_URL"
  start_worker "ingest"   -m packages.ingest.rtsp_worker
  start_worker "detector" -m packages.inference.detector_worker
  start_worker "face"     -m packages.inference.face_worker
  start_worker "fusion"   -m packages.fusion.fusion_service
  start_worker "alerts"   -m packages.alerts.alert_service
  start_worker "api"      -m uvicorn packages.api.main:app \
                             --host 0.0.0.0 --port 8080
  unset STREAM_ID TENANT_ID STREAM_URL

  echo ""
  echo -e "${GREEN}${BOLD}"
  echo "  ╔══════════════════════════════════════════════════════════════╗"
  echo "  ║   All services started!                                      ║"
  echo "  ╠══════════════════════════════════════════════════════════════╣"
  echo "  ║   REST API        →  http://localhost:8080                   ║"
  echo "  ║   API Docs        →  http://localhost:8080/docs              ║"
  echo "  ║   WebSocket       →  ws://localhost:8080/ws/live-events      ║"
  echo "  ╠══════════════════════════════════════════════════════════════╣"
  echo "  ║   Triton HTTP     →  http://localhost:8000/v2                ║"
  echo "  ║   MinIO Console   →  http://localhost:9001                   ║"
  echo "  ╠══════════════════════════════════════════════════════════════╣"
  echo "  ║   Follow logs:    tail -f $LOGS_DIR/<worker>.log               ║"
  echo "  ║   Status:         bash scripts/manage.sh status              ║"
  echo "  ║   Stop:           bash scripts/manage.sh stop                ║"
  echo "  ╚══════════════════════════════════════════════════════════════╝"
  echo -e "${NC}"
}

# ─────────────────────────────────────────────────────────────────────────────
# do_stop
# ─────────────────────────────────────────────────────────────────────────────
do_stop() {
  echo -e "${BOLD}${RED}"
  echo "  ╔══════════════════════════════════════════════════════╗"
  echo "  ║   DriverBehaviourDet — Stopping                      ║"
  echo "  ╚══════════════════════════════════════════════════════╝"
  echo -e "${NC}"

  echo -e "\n${CYAN}── Workers (reverse pipeline order) ──${NC}"
  # Stop in reverse order: api → alerts → fusion → face → detector → ingest
  for W in api alerts fusion face detector ingest; do
    stop_worker "$W"
  done

  if [[ "$FULL_STOP" == true ]]; then
    echo -e "\n${CYAN}── Infrastructure ──${NC}"
    stop_worker "ffmpeg"
    stop_worker "mediamtx"
    stop_worker "minio"
    stop_worker "redpanda"
    service redis-server stop   2>/dev/null && ok "Redis stopped"      || warn "Redis not running"
    pg_ctlcluster 16 main stop  2>/dev/null && ok "PostgreSQL stopped" || \
      service postgresql stop   2>/dev/null && ok "PostgreSQL stopped" || warn "PostgreSQL not running"
  else
    echo ""
    echo -e "${YELLOW}  Infrastructure (Redis, PostgreSQL, Redpanda, MinIO) is still running."
    echo -e "  To also stop infra:  bash scripts/manage.sh stop --full${NC}"
  fi

  echo ""
  ok "Done"
}

# ─────────────────────────────────────────────────────────────────────────────
# do_status
# ─────────────────────────────────────────────────────────────────────────────
do_status() {
  echo ""
  echo -e "${BOLD}══ Workers ══════════════════════════════════${NC}"
  for W in ingest detector face fusion alerts api; do
    if is_running "$W"; then
      local pid; pid=$(cat "$PID_DIR/$W.pid" 2>/dev/null || echo '?')
      echo -e "  ${GREEN}●${NC}  ${BOLD}${W}${NC}  (PID ${pid})"
    else
      echo -e "  ${RED}○${NC}  ${W}  (stopped)"
    fi
  done

  echo ""
  echo -e "${BOLD}══ Infrastructure ═══════════════════════════${NC}"

  _svc() {
    local label="$1" host="$2" port="$3"
    nc -z "$host" "$port" 2>/dev/null \
      && echo -e "  ${GREEN}●${NC}  ${label}  :${port}" \
      || echo -e "  ${RED}○${NC}  ${label}  :${port}  (down)"
  }

  _svc "Redis          " 127.0.0.1 6379
  _svc "PostgreSQL     " 127.0.0.1 5432
  _svc "Redpanda Kafka " 127.0.0.1 19092
  _svc "Schema Registry" 127.0.0.1 18081
  _svc "MinIO S3       " 127.0.0.1 9000
  _svc "MinIO Console  " 127.0.0.1 9001
  _svc "MediaMTX RTSP  " 127.0.0.1 8554

  echo ""
  echo -e "${BOLD}══ Video Source ═════════════════════════════${NC}"
  if is_running "ffmpeg"; then
    echo -e "  ${GREEN}●${NC}  ffmpeg RTSP loop  (PID $(cat "$PID_DIR/ffmpeg.pid" 2>/dev/null))"
  else
    echo -e "  ${RED}○${NC}  ffmpeg RTSP loop  (stopped)"
  fi

  echo ""
  echo -e "${BOLD}══ Triton ════════════════════════════════════${NC}"
  if curl -fsS http://127.0.0.1:8000/v2/health/live &>/dev/null; then
    echo -e "  ${GREEN}●${NC}  Triton Server   :8000  live"
    # Show loaded models
    MODELS=$(curl -s http://127.0.0.1:8000/v2/models 2>/dev/null \
      | python3 -c "import sys,json; d=json.load(sys.stdin); \
        [print('     ↳ '+m['name']) for m in d.get('models',[])]" 2>/dev/null || true)
    [[ -n "$MODELS" ]] && echo -e "  Loaded models:\n$MODELS"
  else
    echo -e "  ${RED}○${NC}  Triton Server   :8000  (down)"
  fi

  echo ""
  echo -e "${BOLD}══ Logs ══════════════════════════════════════${NC}"
  echo "  Directory: $LOGS_DIR"
  ls -lh "$LOGS_DIR"/*.log 2>/dev/null \
    | awk '{printf "  %-35s %s\n", $NF, $5}' \
    | sed "s|$LOGS_DIR/||" || echo "  (no log files yet)"
  echo ""
}

# ─────────────────────────────────────────────────────────────────────────────
# Dispatch
# ─────────────────────────────────────────────────────────────────────────────
case "$ACTION" in
  start)  do_start  ;;
  stop)   do_stop   ;;
  status) do_status ;;
  *)      die "Unknown action: '$ACTION'
  Usage: bash scripts/manage.sh [start|stop|status] [--full]" ;;
esac
