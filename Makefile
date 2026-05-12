SHELL := /bin/bash
PY    ?= python
COMPOSE := docker compose -f docker-compose.dev.yml --env-file .env

.PHONY: help install dev-up dev-down dev-logs seed test lint fmt \
        run-ingest run-detector run-face run-fusion run-alerts run-api \
        triton-up trt-convert trt-parity perf-analyze

help:
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "%-22s %s\n",$$1,$$2}'

install: ## Install Python deps (editable + dev extras)
	$(PY) -m pip install -e ".[dev]"

dev-up: ## Bring up local dev stack
	$(COMPOSE) up -d
	@echo "Stack up. Grafana http://localhost:3000  MinIO http://localhost:9001  Triton http://localhost:8000"

dev-down: ## Tear down local dev stack
	$(COMPOSE) down -v

dev-logs: ## Follow dev logs
	$(COMPOSE) logs -f --tail=200

seed: ## Seed dev tenant + stream
	$(PY) scripts/seed_dev.py

test: ## Run all tests
	$(PY) -m pytest

lint: ## Lint
	ruff check .
	mypy packages

fmt: ## Format
	ruff format .
	ruff check --fix .

run-ingest:    ; $(PY) -m packages.ingest.rtsp_worker
run-detector:  ; $(PY) -m packages.inference.detector_worker
run-face:      ; $(PY) -m packages.inference.face_worker
run-fusion:    ; $(PY) -m packages.fusion.fusion_service
run-alerts:    ; $(PY) -m packages.alerts.alert_service
run-api:       ; uvicorn packages.api.main:app --host 0.0.0.0 --port 8080 --reload

triton-up:     ; $(COMPOSE) up -d triton
trt-convert:   ; bash triton/scripts/onnx_to_trt.sh
trt-parity:    ; $(PY) triton/scripts/verify_parity.py
perf-analyze:  ; bash triton/scripts/benchmark.sh
