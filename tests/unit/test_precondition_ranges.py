"""Range invariants (spec section 14: R66, R67, R68; principle P-C).

Explicit interleavings only: open A, open B, B commits, A commits.

A query answered by a RangeIndex range scan records the range of index keys it scanned, and a
window read of Dictionary keys (``keys(after=...)``, the planner's positional window) the range
from its start to the last key it returned, or to the end when exhausted, instead of the whole
collection.  A range precondition fails when a key (Dictionary) or an index group (RangeIndex)
inside the range differs between the base and the current state.

Catches: a range read that still records the whole collection (a change outside the range
conflicts), a range not validated or validated with the wrong bounds (a change inside it, or at
an inclusive bound, commits), an exhausted window that does not cover the keys after its last
one, a positional window that misses a change before its start, an unbounded or unordered read
that records less than the whole collection, and an escalation that keeps the ranges of the root
it made whole or drops those of another root (R66).
"""
from datetime import date

import pytest

from proto_db.errors import ProtoConflictError
from proto_db.indexing.keyspec import KeySpec
from proto_db.indexing.range import RangeIndex
from proto_db.preconditions import (PRECONDITION_ESCALATION_LIMIT, KeyRange, ReadLog, Unit, UnitKind,
                                    range_violated)
from proto_db.query.planner import plan_query
from proto_db.query.terms import BETWEEN, GE, GT, LE, LT, NE, NOT_NONE, Term
from proto_db.values import order_key, rank_bounds
from tests.support.modes import db_any_mode, mode, space_any_mode  # noqa: F401 -- fixtures


@pytest.fixture
def db(db_any_mode):
    """The database ``db`` in each commit mode: standalone, group commit, cluster (spec section 12,
    Task 14; :mod:`tests.support.modes`)."""
    return db_any_mode


def whole(root):
    return Unit(UnitKind.WHOLE, root)


def seed(db, name="d", index=None, **items):
    def build(tr):
        d = tr.new_dictionary()
        for k, v in items.items():
            d = d.set_at(k, v)
        if index is not None:
            d = d.add_index(index)
        tr.set_root_object(name, d)
    db.run(build)


def write(db, name="d", **items):
    """Commit ``items`` into root ``name`` (None removes the key)."""
    def change(tr):
        d = tr.get_root_object(name)
        for k, v in items.items():
            d = d.remove_at(k) if v is None else d.set_at(k, v)
        tr.set_root_object(name, d)
    db.run(change)


def report(tr):
    """A write of another root, so the reading transaction validates at commit."""
    tr.set_root_object("report", tr.new_object(done=True))


def query(d, op, value):
    return sorted(plan_query(d, Term(None, op, value)).execute())


def by_value():
    return RangeIndex("by_value", KeySpec.element())


def conflict_unit(tr):
    with pytest.raises(ProtoConflictError) as e:
        tr.commit()
    return e.value.unit


# -- R68: a RangeIndex range scan records the range it scanned ----------------------------------

EVENTS = {"e1": date(2026, 1, 5), "e2": date(2026, 1, 20), "e3": date(2026, 2, 10), "e4": date(2026, 3, 3)}
JANUARY = (date(2026, 1, 1), date(2026, 1, 31))


def test_range_query_then_write_no_conflict_outside_range(db):
    """The January/March example: A reports January's events; B adds a March event meanwhile.
    The range A read did not change: A commits."""
    seed(db, index=by_value(), **EVENTS)
    a = db.new_transaction()
    d = a.get_root_object("d")
    assert query(d, BETWEEN, JANUARY) == [date(2026, 1, 5), date(2026, 1, 20)]
    log = a._read_log
    assert whole("d") not in log
    assert log.ranges() == (KeyRange("d", "by_value", order_key(JANUARY[0]), order_key(JANUARY[1]), True, True),)
    report(a)
    write(db, e5=date(2026, 3, 15), e3=None)       # March added, February removed: outside January
    a.commit()
    with db.new_transaction() as tr:
        assert tr.get_root_object("report").done is True


def test_range_query_conflicts_with_change_inside_range(db):
    """A change inside the range scanned conflicts on the range: an element added, one removed,
    and one more element under a key the range already held (its group changed)."""
    for change in ({"e5": date(2026, 1, 25)}, {"e1": None}, {"e5": date(2026, 1, 20)}):
        seed(db, index=by_value(), **EVENTS)
        a = db.new_transaction()
        assert len(query(a.get_root_object("d"), BETWEEN, JANUARY)) == 2
        report(a)
        write(db, **change)
        unit = conflict_unit(a)
        assert unit.kind is UnitKind.RANGE and unit.root == "d" and unit.index == "by_value"
        assert unit.key == a._read_log.ranges()[0]


@pytest.mark.parametrize("op, bound, changed, conflicts", [
    (GT, 10, 10, False), (GT, 10, 11, True),
    (GE, 10, 10, True), (GE, 10, 9, False),
    (LT, 20, 20, False), (LT, 20, 19, True),
    (LE, 20, 20, True), (LE, 20, 21, False),
    (GT, 10, "a string", False),                  # another type rank: outside every number range
])
def test_range_bounds_inclusive_exclusive(db, op, bound, changed, conflicts):
    """A change exactly at an exclusive bound does not conflict; at an inclusive bound it does.
    An ordering operator scans the operand's type rank only (D28), so the range stops there.
    Fifty string values (another rank) make the index scan cheaper than a filtered scan."""
    seed(db, index=by_value(), a=5, b=15, c=25, **{f"s{i}": f"text {i}" for i in range(50)})
    a = db.new_transaction()
    query(a.get_root_object("d"), op, bound)
    (r,) = a._read_log.ranges()
    low, high = rank_bounds(bound)
    expected = {GT: (order_key(bound), high, False, True), GE: (order_key(bound), high, True, True),
                LT: (low, order_key(bound), True, False), LE: (low, order_key(bound), True, True)}[op]
    assert (r.low, r.high, r.low_inclusive, r.high_inclusive) == expected
    report(a)
    write(db, x=changed)
    if conflicts:
        assert conflict_unit(a).kind is UnitKind.RANGE
    else:
        a.commit()


# -- R68: a window read of Dictionary keys --------------------------------------------------------

LETTERS = {k: i for i, k in enumerate("abcdefghij")}


def test_dictionary_window_conflicts_only_inside_window(db):
    """``keys(after="b")`` read three keys (c, d, e): the window is [b, e].  A change after e or
    before b commits; a key added inside the window, or a value changed in it, conflicts."""
    for change, conflicts in (({"f": 100}, False), ({"a": 100}, False), ({"z": 1}, False),
                              ({"cc": 1}, True), ({"d": 100}, True), ({"e": None}, True)):
        seed(db, **LETTERS)
        a = db.new_transaction()
        keys = a.get_root_object("d").keys(after="b")
        assert [next(keys) for _ in range(3)] == ["c", "d", "e"]
        log = a._read_log
        assert whole("d") not in log
        assert log.ranges() == (KeyRange("d", None, order_key("b"), order_key("e"), True, True),)
        report(a)
        write(db, **change)
        if conflicts:
            unit = conflict_unit(a)
            assert unit.kind is UnitKind.RANGE and unit.root == "d" and unit.index is None, change
        else:
            a.commit()


def test_exhausted_window_covers_to_end(db):
    """A window read to its end covers every later key: a key inserted beyond the last one
    returned conflicts, a change before the start does not."""
    for change, conflicts in (({"z": 1}, True), ({"a": 100}, False)):
        seed(db, **LETTERS)
        a = db.new_transaction()
        assert list(a.get_root_object("d").keys(after="h")) == ["i", "j"]
        assert a._read_log.ranges() == (KeyRange("d", None, order_key("h"), None, True, True),)
        report(a)
        write(db, **change)
        if conflicts:
            assert conflict_unit(a).kind is UnitKind.RANGE
        else:
            a.commit()


def test_window_on_own_writes_records_the_range_only(db):
    """A window over a version this transaction changed still records one range (its own writes
    in it reveal nothing of the base); a concurrent change inside it conflicts."""
    seed(db, **LETTERS)
    a = db.new_transaction()
    d = a.get_root_object("d").set_at("cc", 7)
    keys = d.keys(after="b")
    assert [next(keys), next(keys)] == ["c", "cc"]
    assert a._read_log.ranges() == (KeyRange("d", None, order_key("b"), order_key("cc"), True, True),)
    assert whole("d") not in a._read_log
    a.set_root_object("d", d)
    write(db, c=100)
    assert conflict_unit(a).kind is UnitKind.RANGE


def test_planner_window_records_range_from_the_start(db):
    """The planner's positional window over a Dictionary (``limit``/``offset``) reads by position:
    the range runs from the first key to the last row of the window.  A key added before the
    window shifts the positions and conflicts; one after the window does not."""
    for change, conflicts in (({"z": 1}, False), ({"aa": 1}, True), ({"e": 100}, False)):
        seed(db, **LETTERS)
        a = db.new_transaction()
        plan = plan_query(a.get_root_object("d"), limit=2, offset=2)
        assert list(plan.execute()) == [2, 3]          # c, d
        assert whole("d") not in a._read_log
        assert a._read_log.ranges() == (KeyRange("d", None, None, order_key("d"), True, True),)
        report(a)
        write(db, **change)
        if conflicts:
            assert conflict_unit(a).kind is UnitKind.RANGE
        else:
            a.commit()


def test_planner_window_to_the_end_is_whole(db):
    """A positional window that reaches the end reads every key: the whole collection."""
    seed(db, **LETTERS)
    a = db.new_transaction()
    assert list(plan_query(a.get_root_object("d"), limit=5, offset=8).execute()) == [8, 9]
    assert whole("d") in a._read_log and a._read_log.ranges() == ()
    a.abort()


# -- unbounded and unordered reads ------------------------------------------------------------------

def test_unbounded_scan_still_whole(db):
    """Unbounded and unordered reads record the whole collection: ``keys()``, a scan, ``!=``,
    ``?!N`` and an ordered index scan."""
    seed(db, index=by_value(), a=5, b=15, c=25)
    reads = (lambda d: list(d.keys()),
             lambda d: list(plan_query(d).execute()),
             lambda d: query(d, NE, 15),
             lambda d: list(plan_query(d, Term(None, NOT_NONE)).execute()),
             lambda d: list(plan_query(d, order_by=(None, False)).execute()))
    for read in reads:
        a = db.new_transaction()
        read(a.get_root_object("d"))
        assert whole("d") in a._read_log and a._read_log.ranges() == ()
        a.abort()


# -- range_violated (P-C: order keys only) ------------------------------------------------------------

def test_range_violated_compares_by_order_key():
    r = KeyRange("d", None, order_key(10), order_key(20), False, True)
    assert not range_violated(r, [])
    assert not range_violated(r, [10, 10.0, 21, "15"])
    assert range_violated(r, [20.0])
    assert range_violated(r, [3, 15])
    open_high = KeyRange("d", None, order_key("m"), None, True, True)
    assert range_violated(open_high, ["m"]) and range_violated(open_high, ["zzz"])
    assert not range_violated(open_high, ["a", 5])
    open_low = KeyRange("d", None, None, order_key("m"), True, False)
    assert range_violated(open_low, ["a"]) and not range_violated(open_low, ["m"])


# -- R66: escalation and ranges ---------------------------------------------------------------------

def test_escalation_drops_the_ranges_of_that_root_only():
    """Escalating a root to WHOLE drops its ranges; another root's ranges survive."""
    log = ReadLog()
    mine = KeyRange("d", None, order_key(1), order_key(5), True, True)
    other = KeyRange("e", "by_value", order_key(1), None, True, True)
    log.record_range(mine)
    log.record_range(other)
    for i in range(PRECONDITION_ESCALATION_LIMIT + 1):
        log.record(Unit(UnitKind.KEY, "d", key=i), i)
    assert log.whole("d") and not log.whole("e")
    assert log.ranges() == (other,)
    assert [u for u in log.preconditions() if u.root == "d"] == [whole("d")]


def test_ranges_count_toward_escalation():
    """A range is a fine precondition of its root (R66): the limit counts ranges and units
    together, and widening a window in place does not count again."""
    log = ReadLog()
    previous = None
    for i in range(3 * PRECONDITION_ESCALATION_LIMIT):        # one window growing: one range
        r = KeyRange("d", None, order_key(0), order_key(i), True, True)
        log.record_range(r, covers=previous)
        previous = r
    assert log.ranges() == (previous,) and not log.whole("d")
    for i in range(PRECONDITION_ESCALATION_LIMIT - 1):
        log.record(Unit(UnitKind.KEY, "d", key=i), i)
    assert not log.whole("d")
    log.record_range(KeyRange("d", "by_value", order_key(0), None, True, True))
    assert log.whole("d") and log.ranges() == ()
