"""The precondition core (spec sections 4 and 14): units, the read log and the conflict error.

Catches: a later observation of a unit overwriting the base's expected value (R8/R9), a read
set that grows without bound instead of escalating to the whole collection (R66), and a
conflict error that loses the failed unit when it is delivered as a copy (R36).
"""
import copy
import threading

import pytest

from proto_db.errors import CONFLICT_ERROR, ProtoConflictError
from proto_db.preconditions import (ABSENT, PRECONDITION_ESCALATION_LIMIT, KeyRange, ReadLog, Unit,
                                    UnitKind)


def test_first_observation_wins():
    log = ReadLog(); u = Unit(UnitKind.KEY, "d", key="a")
    log.record(u, 1); log.record(u, 2)
    assert log.preconditions()[u] == 1


def test_escalation_after_limit():
    log = ReadLog()
    for i in range(PRECONDITION_ESCALATION_LIMIT + 1):
        log.record(Unit(UnitKind.KEY, "d", key=i), i)
    assert log.whole("d")
    assert all(u.kind is UnitKind.WHOLE for u in log.preconditions() if u.root == "d")


def test_conflict_error_unit_survives_copy():
    e = ProtoConflictError("x", unit=Unit(UnitKind.CELL, key="c"), database="db")
    assert copy.copy(e).unit == e.unit and copy.copy(e).database == "db"


def test_limit_value_and_no_escalation_at_the_limit():
    assert PRECONDITION_ESCALATION_LIMIT == 1024
    log = ReadLog()
    for i in range(PRECONDITION_ESCALATION_LIMIT):
        log.record(Unit(UnitKind.KEY, "d", key=i), i)
    assert not log.whole("d")
    assert len(log.preconditions()) == PRECONDITION_ESCALATION_LIMIT


def test_escalation_is_per_root_and_keeps_other_roots():
    log = ReadLog()
    log.record(Unit(UnitKind.KEY, "e", key="k"), 7)
    log.record(Unit(UnitKind.CELL, key="c"), 8)
    for i in range(PRECONDITION_ESCALATION_LIMIT + 1):
        log.record(Unit(UnitKind.ELEMENT, "d", key=i), i)
    pre = log.preconditions()
    assert not log.whole("e")
    assert pre[Unit(UnitKind.KEY, "e", key="k")] == 7
    assert pre[Unit(UnitKind.CELL, key="c")] == 8
    assert [u for u in pre if u.root == "d"] == [Unit(UnitKind.WHOLE, "d")]


def test_escalation_takes_the_base_token_of_the_root():
    log = ReadLog(whole_token=lambda root: ("base", root))
    for i in range(PRECONDITION_ESCALATION_LIMIT + 1):
        log.record(Unit(UnitKind.KEY, "d", key=i), i)
    assert log.preconditions()[Unit(UnitKind.WHOLE, "d")] == ("base", "d")


def test_escalation_computes_the_base_token_outside_the_log_lock():
    """The base token may load lazy tree nodes: it is computed without holding the log's lock, and
    the escalation is re-checked under it (a concurrent record made meanwhile is never lost)."""
    held = []

    def whole_token(root):
        free = log._lock.acquire(blocking=False)               # never blocks: no deadlock if it is held
        held.append(not free)
        if free:
            log._lock.release()
            log.record(Unit(UnitKind.CELL, key="during"), 1)    # a record made while the token is computed
        return ("base", root)

    log = ReadLog(whole_token=whole_token)
    for i in range(PRECONDITION_ESCALATION_LIMIT + 1):
        log.record(Unit(UnitKind.KEY, "d", key=i), i)
    assert held == [False]
    assert log.preconditions() == {Unit(UnitKind.CELL, key="during"): 1,
                                   Unit(UnitKind.WHOLE, "d"): ("base", "d")}
    assert log.whole("d")


def test_fine_records_after_whole_are_covered():
    log = ReadLog()
    log.record_range(KeyRange("d", None, 1, 5, True, False))
    log.record(Unit(UnitKind.KEY, "d", key="a"), 1)
    log.record(Unit(UnitKind.WHOLE, "d"), "token")
    log.record(Unit(UnitKind.KEY, "d", key="b"), 2)
    log.record_range(KeyRange("d", None, 6, 9, True, True))
    assert log.whole("d")
    assert log.preconditions() == {Unit(UnitKind.WHOLE, "d"): "token"}
    assert log.ranges() == ()


def test_ranges_are_recorded_once_in_order():
    log = ReadLog()
    r1 = KeyRange("d", None, 1, None, True, True)
    r2 = KeyRange("d", "by_date", None, 10, False, True)
    log.record_range(r1); log.record_range(r2); log.record_range(r1)
    assert log.ranges() == (r1, r2)


def test_absent_is_a_distinct_expected_value():
    log = ReadLog()
    u = Unit(UnitKind.CELL, key="c")
    log.record(u, ABSENT)
    log.record(u, 1)
    assert log.preconditions()[u] is ABSENT
    assert ABSENT is not None and ABSENT != 0 and bool(ABSENT) is True


def test_units_are_frozen_values():
    u = Unit(UnitKind.INDEX_GROUP, "d", key=3, index="by_x")
    assert u == Unit(UnitKind.INDEX_GROUP, "d", key=3, index="by_x")
    assert hash(u) == hash(Unit(UnitKind.INDEX_GROUP, "d", key=3, index="by_x"))
    with pytest.raises(AttributeError):
        u.key = 4
    assert {k.name for k in UnitKind} == {
        "ROOT", "ROOT_PRESENCE", "ROOT_NAMES", "CELL", "CELL_NAMES", "KEY", "ELEMENT", "COUNT", "RECORDS",
        "POSITION", "WHOLE", "INDEX_DEFS", "INDEX_GROUP", "RANGE"}


def test_concurrent_records_lose_nothing():
    log = ReadLog()
    barrier = threading.Barrier(4)

    def worker(t):
        barrier.wait()
        for i in range(200):
            log.record(Unit(UnitKind.CELL, key=f"{t}-{i}"), i)

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(4)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    assert len(log.preconditions()) == 800


def test_conflict_error_keeps_its_existing_interface():
    e = ProtoConflictError("boom")
    assert e.unit is None and e.database is None and e.code == CONFLICT_ERROR and str(e) == "boom"
    e.permit_held = True
    c = copy.copy(e)
    assert c.permit_held is True and c.message == "boom"
    assert ProtoConflictError("m", code=5).code == 5
