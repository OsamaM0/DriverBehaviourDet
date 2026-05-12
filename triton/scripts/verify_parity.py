"""
Numerical parity check: ONNX (CPU/CUDA EP) vs TensorRT plan via Triton.

Promotes a new TRT model only if Box-level results match within tolerance.
Run AFTER `onnx_to_trt.sh` and after Triton is serving both models.

Usage:
    python triton/scripts/verify_parity.py path/to/sample.jpg
"""
from __future__ import annotations

import asyncio
import os
import sys

import numpy as np
import onnxruntime as ort

from packages.common.config import settings
from packages.inference.postprocess import postprocess_single
from packages.inference.preprocess import preprocess_jpeg
from packages.inference.triton_client import TritonClient

ONNX_MODEL_PATH = os.environ.get("ONNX_MODEL_PATH", "rf_detr_driver_behaviour_optimized.onnx")
IOU_TOL = 0.02
CONF_TOL = 0.05


def iou(a, b) -> float:
    ax1, ay1, ax2, ay2 = a
    bx1, by1, bx2, by2 = b
    ix1, iy1 = max(ax1, bx1), max(ay1, by1)
    ix2, iy2 = min(ax2, bx2), min(ay2, by2)
    iw, ih = max(0.0, ix2 - ix1), max(0.0, iy2 - iy1)
    inter = iw * ih
    ua = (ax2 - ax1) * (ay2 - ay1) + (bx2 - bx1) * (by2 - by1) - inter
    return inter / ua if ua > 0 else 0.0


async def main(jpeg_path: str) -> int:
    with open(jpeg_path, "rb") as f:
        jpeg = f.read()
    nchw, ow, oh = preprocess_jpeg(jpeg)

    # ── ONNX baseline ──────────────────────────────────────────────────────────
    sess = ort.InferenceSession(ONNX_MODEL_PATH, providers=["CPUExecutionProvider"])
    in_name = sess.get_inputs()[0].name
    out_names = [o.name for o in sess.get_outputs()]
    onnx_raw = sess.run(out_names, {in_name: nchw})
    onnx_boxes = postprocess_single(onnx_raw, ow, oh)

    # ── Triton (TRT) ───────────────────────────────────────────────────────────
    triton = TritonClient()
    meta = await triton.metadata("rfdetr_driver_behaviour")
    trt_in = meta.inputs[0][0]
    trt_raw = await triton.infer("rfdetr_driver_behaviour", {trt_in: nchw.astype(np.float32)})
    trt_boxes = postprocess_single(trt_raw, ow, oh)
    await triton.close()

    # ── Compare ────────────────────────────────────────────────────────────────
    onnx_sorted = sorted(onnx_boxes, key=lambda b: (b.cls, -b.conf))
    trt_sorted = sorted(trt_boxes, key=lambda b: (b.cls, -b.conf))
    print(f"ONNX boxes: {len(onnx_sorted)}    TRT boxes: {len(trt_sorted)}")

    if len(onnx_sorted) != len(trt_sorted):
        print("FAIL: detection count differs")
        return 2

    fail = 0
    for a, b in zip(onnx_sorted, trt_sorted):
        if a.cls != b.cls:
            print(f"FAIL: class mismatch {a.cls} vs {b.cls}")
            fail += 1
            continue
        if abs(a.conf - b.conf) > CONF_TOL:
            print(f"FAIL: conf {a.conf:.3f} vs {b.conf:.3f}")
            fail += 1
        if iou(a.xyxy, b.xyxy) < 1.0 - IOU_TOL:
            print(f"FAIL: iou {iou(a.xyxy, b.xyxy):.3f}  {a.xyxy} vs {b.xyxy}")
            fail += 1

    print("PASS" if fail == 0 else f"FAIL ({fail} mismatches)")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: verify_parity.py <sample.jpg>")
        raise SystemExit(64)
    raise SystemExit(asyncio.run(main(sys.argv[1])))
