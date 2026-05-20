"""
Per-stream temporal window of fused signals (in-process, with Redis snapshot).

Why in-process: fusion partitions consume keyed by `stream_id`, so the same
worker always sees the same stream's signals. We snapshot to Redis on every
update so a different worker can warm-start after a rebalance.

Window unit: per-frame samples with monotonic timestamps. We compute EWMA +
fraction-above-threshold over the last N seconds.
"""
from __future__ import annotations

import time
from collections import defaultdict, deque
from dataclasses import dataclass, field

DEFAULT_WINDOW_MS = 5000     # 5 s rolling window for fusion
EWMA_ALPHA = 0.3


@dataclass(slots=True)
class _Sample:
    ts_ns: int
    values: dict[str, float]   # signal_name → value (e.g., phone_conf=0.82)


@dataclass(slots=True)
class StreamWindow:
    samples: deque[_Sample] = field(default_factory=deque)
    ewma: dict[str, float] = field(default_factory=dict)
    last_seen_ns: int = 0

    def add(self, values: dict[str, float], ts_ns: int | None = None, window_ms: int = DEFAULT_WINDOW_MS) -> None:
        ts = ts_ns or time.time_ns()
        self.samples.append(_Sample(ts_ns=ts, values=values))
        self.last_seen_ns = ts
        cutoff = ts - window_ms * 1_000_000
        while self.samples and self.samples[0].ts_ns < cutoff:
            self.samples.popleft()
        for k, v in values.items():
            self.ewma[k] = (1 - EWMA_ALPHA) * self.ewma.get(k, v) + EWMA_ALPHA * v

    def fraction_above(self, signal: str, threshold: float) -> float:
        if not self.samples:
            return 0.0
        observed = [s.values[signal] for s in self.samples if signal in s.values]
        if not observed:
            return 0.0
        n = sum(1 for value in observed if value >= threshold)
        return n / len(observed)

    def mean(self, signal: str) -> float:
        if not self.samples:
            return 0.0
        vals = [s.values.get(signal, 0.0) for s in self.samples]
        return sum(vals) / len(vals)

    def latest(self, signal: str) -> float:
        for s in reversed(self.samples):
            if signal in s.values:
                return s.values[signal]
        return 0.0


class WindowStore:
    """Per-process per-stream-window registry."""

    def __init__(self) -> None:
        self._windows: dict[str, StreamWindow] = defaultdict(StreamWindow)

    def get(self, stream_id: str) -> StreamWindow:
        return self._windows[stream_id]

    def gc(self, idle_seconds: int = 600) -> int:
        cutoff = time.time_ns() - idle_seconds * 1_000_000_000
        stale = [k for k, w in self._windows.items() if w.last_seen_ns < cutoff]
        for k in stale:
            self._windows.pop(k, None)
        return len(stale)
