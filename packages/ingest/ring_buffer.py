"""
Per-stream in-memory evidence ring buffer (last N seconds).

Holds JPEG-encoded frames + pts so the evidence clipper can produce a
±N-second clip without re-pulling from the camera. We deliberately keep this
in-process: workers are stateless across crashes, and a missed clip is a
non-fatal degradation (alert still fires, video just unavailable).
"""
from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass


@dataclass(slots=True)
class FrameSlot:
    pts_ms: int
    ts_wall_ns: int
    jpeg: bytes
    width: int
    height: int


class RingBuffer:
    def __init__(self, seconds: int = 12) -> None:
        self._seconds = seconds
        self._frames: deque[FrameSlot] = deque()
        self._lock = threading.Lock()

    def add(self, slot: FrameSlot) -> None:
        with self._lock:
            self._frames.append(slot)
            cutoff = slot.ts_wall_ns - self._seconds * 1_000_000_000
            while self._frames and self._frames[0].ts_wall_ns < cutoff:
                self._frames.popleft()

    def slice(self, around_ts_wall_ns: int, half_window_s: int) -> list[FrameSlot]:
        lo = around_ts_wall_ns - half_window_s * 1_000_000_000
        hi = around_ts_wall_ns + half_window_s * 1_000_000_000
        with self._lock:
            return [f for f in self._frames if lo <= f.ts_wall_ns <= hi]

    def latest(self, half_window_s: int) -> list[FrameSlot]:
        return self.slice(time.time_ns(), half_window_s)
