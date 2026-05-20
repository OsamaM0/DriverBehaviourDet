"""
Behaviour state machine with hysteresis.

Inputs (computed by `fusion_service` from the rolling window):
  - phone:        fraction of last N s with phone detected ≥ τ
  - seatbelt:     latest seatbelt-present probability (low ⇒ unsafe)
  - smoking:      fraction of last N s with cigarette detected ≥ τ
  - eating:       fraction of last N s with foodItem detected ≥ τ
  - drowsy:       EAR-based + (later) eye-state CNN
  - distracted:   head-pose yaw/pitch over threshold for too long
  - hand_off_wheel: 1 - hand-on-wheel fraction

State transitions are enter-/exit-thresholded with a minimum dwell to prevent
single-frame flips. CRITICAL is "stuck in unsafe" or compounded signals.

Per-tenant overrides are loaded from `configs/tenants/<tenant>.yaml` and
passed in as a `Thresholds` instance.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Iterable

from packages.common.schemas import (
    AlertSeverity,
    AlertType,
    BehaviorScores,
    BehaviorStateName,
    Box,
)


@dataclass(slots=True)
class Thresholds:
    # signal-level
    phone_enter: float = 0.4
    phone_exit: float = 0.2
    seatbelt_unsafe: float = 0.4         # if seatbelt-present prob below this → unsafe
    smoking_enter: float = 0.2
    smoking_exit: float = 0.2
    eating_enter: float = 0.5
    eating_exit: float = 0.25
    drowsy_enter: float = 0.5
    drowsy_exit: float = 0.25
    distracted_enter: float = 0.5
    distracted_exit: float = 0.25
    hand_off_enter: float = 0.6
    hand_off_exit: float = 0.3
    # state-level
    min_dwell_ms: int = 1500             # min time before transitioning out
    critical_dwell_ms: int = 8000        # time in UNSAFE before escalation
    recovery_ms: int = 4000              # time in NORMAL before clearing RECOVERED


@dataclass(slots=True)
class StateRecord:
    state: BehaviorStateName = BehaviorStateName.NORMAL
    prev: BehaviorStateName = BehaviorStateName.NORMAL
    entered_ns: int = field(default_factory=time.time_ns)
    last_alerts: dict[AlertType, int] = field(default_factory=dict)   # type → ns last emitted
    latest_detection_boxes: list[Box] = field(default_factory=list)
    latest_detection_frame_width: int | None = None
    latest_detection_frame_height: int | None = None
    latest_detection_ts_ns: int = 0


@dataclass(slots=True)
class Decision:
    new_state: BehaviorStateName
    transitioned: bool
    alerts: list[tuple[AlertType, AlertSeverity]] = field(default_factory=list)


def _dwell_ms(rec: StateRecord) -> int:
    return (time.time_ns() - rec.entered_ns) // 1_000_000


def evaluate(
    rec: StateRecord,
    scores: BehaviorScores,
    th: Thresholds,
) -> Decision:
    """Compute next state + alerts based on current scores and dwell time."""
    dwell = _dwell_ms(rec)

    # --- determine candidate state from signals (priority order) -------------
    if scores.drowsy >= th.drowsy_enter:
        candidate = BehaviorStateName.DROWSY
    elif scores.phone >= th.phone_enter or scores.smoking >= th.smoking_enter:
        candidate = BehaviorStateName.UNSAFE
    elif scores.seatbelt < th.seatbelt_unsafe:
        candidate = BehaviorStateName.UNSAFE
    elif scores.hand_off_wheel >= th.hand_off_enter:
        candidate = BehaviorStateName.UNSAFE
    elif scores.distracted >= th.distracted_enter or scores.eating >= th.eating_enter:
        candidate = BehaviorStateName.DISTRACTED
    else:
        candidate = BehaviorStateName.NORMAL

    # --- hysteresis: require min dwell before downgrading severity -----------
    severity_order = {
        BehaviorStateName.NORMAL: 0,
        BehaviorStateName.RECOVERED: 1,
        BehaviorStateName.DISTRACTED: 2,
        BehaviorStateName.DROWSY: 3,
        BehaviorStateName.UNSAFE: 4,
        BehaviorStateName.CRITICAL: 5,
    }
    if severity_order[candidate] < severity_order[rec.state] and dwell < th.min_dwell_ms:
        candidate = rec.state

    # --- escalate to CRITICAL if stuck in UNSAFE ----------------------------
    if rec.state == BehaviorStateName.UNSAFE and dwell >= th.critical_dwell_ms:
        candidate = BehaviorStateName.CRITICAL

    # --- recovery ------------------------------------------------------------
    if rec.state in (BehaviorStateName.UNSAFE, BehaviorStateName.CRITICAL,
                     BehaviorStateName.DROWSY, BehaviorStateName.DISTRACTED) \
            and candidate == BehaviorStateName.NORMAL:
        candidate = BehaviorStateName.RECOVERED

    if rec.state == BehaviorStateName.RECOVERED and dwell >= th.recovery_ms \
            and candidate == BehaviorStateName.RECOVERED:
        candidate = BehaviorStateName.NORMAL

    transitioned = candidate != rec.state
    decision = Decision(new_state=candidate, transitioned=transitioned)

    # --- alert generation on transition into a "bad" state ------------------
    if transitioned and candidate in (
        BehaviorStateName.DISTRACTED,
        BehaviorStateName.DROWSY,
        BehaviorStateName.UNSAFE,
        BehaviorStateName.CRITICAL,
    ):
        decision.alerts.extend(_alerts_for(scores, th, candidate))

    return decision


def _alerts_for(scores: BehaviorScores, th: Thresholds, state: BehaviorStateName) -> Iterable[tuple[AlertType, AlertSeverity]]:
    sev = {
        BehaviorStateName.DISTRACTED: AlertSeverity.LOW,
        BehaviorStateName.DROWSY: AlertSeverity.HIGH,
        BehaviorStateName.UNSAFE: AlertSeverity.MEDIUM,
        BehaviorStateName.CRITICAL: AlertSeverity.CRITICAL,
    }[state]

    if scores.drowsy >= th.drowsy_enter:
        yield AlertType.DROWSINESS, sev
    if scores.phone >= th.phone_enter:
        yield AlertType.PHONE_USE, sev
    if scores.smoking >= th.smoking_enter:
        yield AlertType.SMOKING, sev
    if scores.eating >= th.eating_enter:
        yield AlertType.EATING, sev
    if scores.seatbelt < th.seatbelt_unsafe:
        yield AlertType.NO_SEATBELT, sev
    if scores.hand_off_wheel >= th.hand_off_enter:
        yield AlertType.HANDS_OFF_WHEEL, sev
    if scores.distracted >= th.distracted_enter:
        yield AlertType.DISTRACTION, sev


def commit(rec: StateRecord, decision: Decision) -> None:
    if decision.transitioned:
        rec.prev = rec.state
        rec.state = decision.new_state
        rec.entered_ns = time.time_ns()
