#!/usr/bin/env bash
# Convert RF-DETR ONNX → TensorRT FP16 plan with dynamic batch.
# Run inside the Triton container (or any image with TensorRT 9+ + trtexec).
#
# Example:
#   docker run --rm --gpus all -v "$PWD":/work -w /work \
#       nvcr.io/nvidia/tritonserver:24.04-py3 \
#       bash triton/scripts/onnx_to_trt.sh
#
# Tunables via env:
#   ONNX_PATH, OUT_PLAN, MIN_BS, OPT_BS, MAX_BS, INPUT_NAME, INPUT_HW
set -euo pipefail

ONNX_PATH="${ONNX_PATH:-rf_detr_driver_behaviour_optimized.onnx}"
OUT_PLAN="${OUT_PLAN:-triton/model_repository/rfdetr_driver_behaviour/1/model.plan}"
INPUT_NAME="${INPUT_NAME:-input}"     # set to actual ONNX input name
INPUT_HW="${INPUT_HW:-3x576x576}"
MIN_BS="${MIN_BS:-1}"
OPT_BS="${OPT_BS:-8}"
MAX_BS="${MAX_BS:-16}"
WORKSPACE_MB="${WORKSPACE_MB:-4096}"  # trtexec workspace in MB
NO_TF32="${NO_TF32:-false}"           # set to true to add --noTF32 flag

mkdir -p "$(dirname "$OUT_PLAN")"

_no_tf32_flag=""
[[ "$NO_TF32" == "true" ]] && _no_tf32_flag="--noTF32"

trtexec \
  --onnx="$ONNX_PATH" \
  --saveEngine="$OUT_PLAN" \
  --fp16 \
  --workspace="$WORKSPACE_MB" \
  --minShapes="${INPUT_NAME}":"${MIN_BS}x${INPUT_HW}" \
  --optShapes="${INPUT_NAME}":"${OPT_BS}x${INPUT_HW}" \
  --maxShapes="${INPUT_NAME}":"${MAX_BS}x${INPUT_HW}" \
  --useCudaGraph \
  ${_no_tf32_flag:+--noTF32}

echo "Built TRT plan → $OUT_PLAN"
echo "Run parity check:  python triton/scripts/verify_parity.py"
