"""
RF-DETR postprocess (faithful port of `driver_beh_inferance.py::_postprocess`).

DETR-family models output per-query class logits + cxcywh boxes in [0, 1].
We sigmoid the logits, take argmax class+score, threshold, and rescale boxes
to original pixel coordinates. NMS-free (DETR design).
"""
from __future__ import annotations

import numpy as np

from packages.common.config import settings
from packages.common.schemas import Box

CLASS_NAMES: tuple[str, ...] = (
    "DriverBehaviour",  # 0
    "cigarette",        # 1
    "foodItem",         # 2
    "phone",            # 3
    "seatbelt",         # 4
    "wheel",            # 5
)


def sigmoid(x: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-x.astype(np.float32, copy=False)))


def _identify_outputs(raw_outputs: list[np.ndarray]) -> tuple[np.ndarray, np.ndarray]:
    """Find (logits, boxes) regardless of output ordering. Boxes have last dim 4."""
    logits = boxes = None
    for arr in raw_outputs:
        if arr.ndim == 3:
            if arr.shape[-1] == 4 and boxes is None:
                boxes = arr
            elif logits is None:
                logits = arr
    if logits is None or boxes is None:
        # fallback: positional
        if len(raw_outputs) >= 2:
            logits, boxes = raw_outputs[0], raw_outputs[1]
        else:
            raise ValueError("Could not identify (logits, boxes) in detector outputs")
    return logits, boxes


def postprocess_single(
    raw_outputs: list[np.ndarray],
    orig_w: int,
    orig_h: int,
    threshold: float | None = None,
) -> list[Box]:
    """Postprocess one image (batch index 0)."""
    th = threshold if threshold is not None else settings.detector_conf_threshold
    logits_b, boxes_b = _identify_outputs(raw_outputs)
    logits, boxes = logits_b[0], boxes_b[0]

    scores = sigmoid(logits)                    # [Q, C]
    class_ids = scores.argmax(axis=-1)
    confidences = scores[np.arange(len(class_ids)), class_ids]

    keep = confidences >= th
    if not keep.any():
        return []

    class_ids = class_ids[keep]
    confidences = confidences[keep]
    bx = boxes[keep]

    cx, cy, w, h = bx[:, 0], bx[:, 1], bx[:, 2], bx[:, 3]
    x1 = np.clip((cx - w / 2) * orig_w, 0, orig_w)
    y1 = np.clip((cy - h / 2) * orig_h, 0, orig_h)
    x2 = np.clip((cx + w / 2) * orig_w, 0, orig_w)
    y2 = np.clip((cy + h / 2) * orig_h, 0, orig_h)

    out: list[Box] = []
    for i in range(len(class_ids)):
        c = int(class_ids[i])
        out.append(Box(
            cls=c,
            cls_name=CLASS_NAMES[c] if 0 <= c < len(CLASS_NAMES) else str(c),
            conf=float(confidences[i]),
            xyxy=(float(x1[i]), float(y1[i]), float(x2[i]), float(y2[i])),
        ))
    return out


def postprocess_batch(
    raw_outputs: list[np.ndarray],
    sizes: list[tuple[int, int]],
    threshold: float | None = None,
) -> list[list[Box]]:
    """Postprocess a batched response. `sizes` = list of (orig_w, orig_h) per item."""
    th = threshold if threshold is not None else settings.detector_conf_threshold
    logits_b, boxes_b = _identify_outputs(raw_outputs)
    results: list[list[Box]] = []
    for i, (ow, oh) in enumerate(sizes):
        results.append(postprocess_single([logits_b[i : i + 1], boxes_b[i : i + 1]], ow, oh, th))
    return results
