import time

from packages.common.schemas import BehaviorScores, BehaviorStateName
from packages.fusion.state_machine import StateRecord, Thresholds, commit, evaluate


def _rec(state: BehaviorStateName = BehaviorStateName.NORMAL) -> StateRecord:
    return StateRecord(state=state, prev=state, entered_ns=time.time_ns())


def test_normal_to_distracted_then_drowsy() -> None:
    th = Thresholds(min_dwell_ms=0, critical_dwell_ms=10_000)
    rec = _rec()
    d1 = evaluate(rec, BehaviorScores(distracted=0.9, seatbelt=1.0), th)
    assert d1.new_state == BehaviorStateName.DISTRACTED and d1.transitioned
    commit(rec, d1)
    d2 = evaluate(rec, BehaviorScores(drowsy=0.9, distracted=0.9, seatbelt=1.0), th)
    assert d2.new_state == BehaviorStateName.DROWSY


def test_hysteresis_prevents_flapping() -> None:
    th = Thresholds(min_dwell_ms=2000)
    rec = StateRecord(state=BehaviorStateName.UNSAFE, prev=BehaviorStateName.NORMAL,
                      entered_ns=time.time_ns())
    # signal drops just below exit immediately — must NOT downgrade before dwell
    d = evaluate(rec, BehaviorScores(phone=0.2), th)
    assert d.new_state == BehaviorStateName.UNSAFE


def test_critical_escalation() -> None:
    th = Thresholds(min_dwell_ms=0, critical_dwell_ms=1)
    rec = StateRecord(state=BehaviorStateName.UNSAFE, prev=BehaviorStateName.NORMAL,
                      entered_ns=time.time_ns() - 5_000_000_000)
    d = evaluate(rec, BehaviorScores(phone=0.95, seatbelt=1.0), th)
    assert d.new_state == BehaviorStateName.CRITICAL
