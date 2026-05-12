"""
Vectorised preprocessing for the RF-DETR detector (lifted from
`driver_beh_inferance.py` and converted to OpenCV — no PIL in hot path).

Input:  HxWx3 BGR uint8 (OpenCV decode output)
Output: NCHW float32, ImageNet normalised, contiguous
"""
from __future__ import annotations

import cv2
import numpy as np

from packages.common.config import settings

IMG_MEAN = np.array([0.485, 0.456, 0.406], dtype=np.float32)
IMG_STD = np.array([0.229, 0.224, 0.225], dtype=np.float32)


def preprocess_bgr(bgr: np.ndarray, size: int | None = None) -> np.ndarray:
    """Resize → RGB → /255 → ImageNet normalise → NCHW."""
    s = size or settings.detector_input_size
    rgb = cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB)
    if rgb.shape[0] != s or rgb.shape[1] != s:
        rgb = cv2.resize(rgb, (s, s), interpolation=cv2.INTER_LINEAR)
    arr = rgb.astype(np.float32, copy=False) * (1.0 / 255.0)
    arr -= IMG_MEAN
    arr /= IMG_STD
    arr = np.transpose(arr, (2, 0, 1))                # CHW
    return np.ascontiguousarray(arr[np.newaxis], dtype=np.float32)  # NCHW


def preprocess_jpeg(jpeg: bytes, size: int | None = None) -> tuple[np.ndarray, int, int]:
    """Decode JPEG bytes and preprocess. Returns (NCHW, orig_w, orig_h)."""
    arr = np.frombuffer(jpeg, dtype=np.uint8)
    bgr = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    if bgr is None:
        raise ValueError("Failed to decode JPEG bytes")
    h, w = bgr.shape[:2]
    return preprocess_bgr(bgr, size), w, h


def preprocess_batch_bgr(bgrs: list[np.ndarray], size: int | None = None) -> np.ndarray:
    """Stack a list of BGR frames into one NCHW batch (single H2D copy)."""
    return np.concatenate([preprocess_bgr(f, size) for f in bgrs], axis=0)
