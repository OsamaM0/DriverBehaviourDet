import time

from packages.fusion.fusion_service import _scores_from_window
from packages.fusion.temporal_window import StreamWindow


def test_scores_keep_sparse_detection_signals_undiluted() -> None:
    window = StreamWindow()
    now = time.time_ns()

    window.add({"smoking": 0.9}, ts_ns=now, window_ms=5000)
    window.add({"distracted": 0.8}, ts_ns=now + 100_000_000, window_ms=5000)

    scores = _scores_from_window(window)

    assert scores.smoking == 1.0


def test_scores_keep_missing_hand_signal_neutral() -> None:
    window = StreamWindow()
    now = time.time_ns()

    window.add({"smoking": 0.9}, ts_ns=now, window_ms=5000)
    window.add({"distracted": 0.8}, ts_ns=now + 100_000_000, window_ms=5000)

    scores = _scores_from_window(window)

    assert scores.hand_off_wheel == 0.0


def test_scores_use_only_hand_samples_for_hand_off_fraction() -> None:
    window = StreamWindow()
    now = time.time_ns()

    window.add({"hand_on_wheel": 1.0}, ts_ns=now, window_ms=5000)
    window.add({"distracted": 0.8}, ts_ns=now + 100_000_000, window_ms=5000)
    window.add({"hand_on_wheel": 0.0}, ts_ns=now + 200_000_000, window_ms=5000)

    scores = _scores_from_window(window)

    assert scores.hand_off_wheel == 0.5