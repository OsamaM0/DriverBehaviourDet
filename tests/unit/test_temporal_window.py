import time

from packages.fusion.temporal_window import StreamWindow


def test_window_fraction_above() -> None:
    w = StreamWindow()
    now = time.time_ns()
    for i in range(10):
        w.add({"phone": 0.9 if i >= 5 else 0.1}, ts_ns=now + i * 100_000_000, window_ms=1000)
    assert w.fraction_above("phone", 0.5) == 0.5


def test_window_ewma_smoothing() -> None:
    w = StreamWindow()
    now = time.time_ns()
    for i in range(50):
        w.add({"drowsy": 1.0}, ts_ns=now + i * 10_000_000, window_ms=2000)
    assert w.mean("drowsy") > 0.9


def test_window_eviction() -> None:
    w = StreamWindow()
    now = time.time_ns()
    w.add({"x": 1.0}, ts_ns=now, window_ms=500)
    w.add({"x": 0.0}, ts_ns=now + 1_000_000_000, window_ms=500)
    assert w.latest("x") == 0.0
