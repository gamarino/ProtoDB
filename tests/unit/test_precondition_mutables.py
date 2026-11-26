"""Mutable cells under the precondition model (spec section 5 "Roots and mutables", R3, R6-R10, R35, R36).

Every write of a cell compares against the cell's value in the transaction's base: a blind
set no longer lets the last writer win, and removing a cell another transaction removed is a
conflict.  Interleavings are explicit: open A, open B, commit B, commit A.
"""
import pytest

from proto_db.errors import ProtoConflictError
from proto_db.preconditions import ABSENT, Unit, UnitKind
from tests.support.modes import db_any_mode, mode, space_any_mode  # noqa: F401 -- fixtures


@pytest.fixture
def db(db_any_mode):
    """The database ``db`` in each commit mode: standalone, group commit, cluster (spec section 12,
    Task 14; :mod:`tests.support.modes`)."""
    return db_any_mode


def cell(name):
    return Unit(UnitKind.CELL, key=name)


def read(db, name):
    return db.run(lambda tr: tr.get_mutable(name))


def test_blind_set_mutable_conflicts_with_concurrent_set(db):
    db.run(lambda tr: tr.new_mutable("c", 0))
    a, b = db.new_transaction(), db.new_transaction()
    a.set_mutable("c", 1)
    b.set_mutable("c", 2)
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == cell("c") and e.value.database == "db"
    assert read(db, "c") == 2


def test_set_mutable_other_cell_no_conflict(db):
    db.run(lambda tr: (tr.new_mutable("c1", 0), tr.new_mutable("c2", 0)))
    a, b = db.new_transaction(), db.new_transaction()
    a.set_mutable("c1", 1)
    b.set_mutable("c2", 2)
    b.commit()
    a.commit()
    assert read(db, "c1") == 1 and read(db, "c2") == 2


def test_remove_mutable_of_cell_removed_by_other_conflicts(db):
    db.run(lambda tr: tr.new_mutable("c", 0))
    a, b = db.new_transaction(), db.new_transaction()
    a.remove_mutable("c")
    b.remove_mutable("c")
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == cell("c")


def test_new_mutable_conflicts_when_other_created_it(db):
    a, b = db.new_transaction(), db.new_transaction()
    a.new_mutable("c", 1)
    b.new_mutable("c", 2)
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == cell("c")
    assert read(db, "c") == 2


def test_mutable_names_conflicts_on_concurrent_create(db):
    db.run(lambda tr: tr.new_mutable("c", 0))
    a, b = db.new_transaction(), db.new_transaction()
    assert a.mutable_names() == ["c"]
    a.set_root_object("r", 1)
    b.new_mutable("d", 1)
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == Unit(UnitKind.CELL_NAMES)


def test_mutable_names_no_conflict_on_value_change(db):
    db.run(lambda tr: tr.new_mutable("c", 0))
    a, b = db.new_transaction(), db.new_transaction()
    assert a.mutable_names() == ["c"]
    a.set_root_object("r", 1)
    b.set_mutable("c", 5)
    b.commit()
    a.commit()
    assert read(db, "c") == 5


def test_writing_base_value_keeps_precondition(db):
    db.run(lambda tr: tr.new_mutable("c", 0))
    a, b = db.new_transaction(), db.new_transaction()
    a.set_mutable("c", 0)               # R10: the base value, still a write
    b.set_mutable("c", 9)
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == cell("c")


def test_retry_starts_with_fresh_log(db):
    db.run(lambda tr: (tr.new_mutable("a", 0), tr.new_mutable("b", 0)))
    attempts = []

    def fn(tr):
        attempts.append((tr, dict(tr._read_log.preconditions())))
        if len(attempts) == 1:
            tr.set_mutable("a", tr.get_mutable("a") + 1)
            db.run(lambda other: other.set_mutable("a", 100))     # B commits before A
        else:
            tr.set_mutable("b", tr.get_mutable("b") + 1)

    db.run(fn)
    (first, first_log), (second, second_log) = attempts
    assert second is not first and first_log == {} and second_log == {}
    assert set(second._read_log.preconditions()) == {cell("b")}
    assert read(db, "a") == 100 and read(db, "b") == 1


def test_get_mutable_records_the_base_token(db):
    db.run(lambda tr: tr.new_mutable("c", 1))
    a = db.new_transaction()
    a.get_mutable("c")
    a.get_mutable("missing")
    pre = a._read_log.preconditions()
    assert pre[cell("missing")] is ABSENT and pre[cell("c")] is not ABSENT
    a.abort()


def test_read_of_own_write_adds_no_precondition(db):
    """R9: a cell created by the transaction keeps the base's absence as its only precondition."""
    a = db.new_transaction()
    a.new_mutable("c", 1)
    a.set_mutable("c", 2)
    assert a.get_mutable("c") == 2
    assert a._read_log.preconditions() == {cell("c"): ABSENT}
    a.commit()


def test_set_after_remove_and_create_keeps_the_base_precondition(db):
    """R8: removing then creating again keeps the base token, not the absence the create saw."""
    db.run(lambda tr: tr.new_mutable("c", 0))
    a, b = db.new_transaction(), db.new_transaction()
    a.remove_mutable("c")
    a.new_mutable("c", 3)
    b.set_mutable("c", 4)
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == cell("c")


def test_aba_by_value_holds(db):
    """R7: the cell went 0 -> 5 -> 0 meanwhile; the precondition holds."""
    db.run(lambda tr: tr.new_mutable("c", 0))
    a = db.new_transaction()
    a.set_mutable("c", 1)
    db.run(lambda tr: tr.set_mutable("c", 5))
    db.run(lambda tr: tr.set_mutable("c", 0))
    a.commit()
    assert read(db, "c") == 1


def test_exact_token_distinguishes_equal_values(db):
    """R6: 1 and 1.0 are equal but not the same exact token."""
    db.run(lambda tr: tr.new_mutable("c", 1))
    a = db.new_transaction()
    a.set_mutable("c", 2)
    db.run(lambda tr: tr.set_mutable("c", 1.0))
    with pytest.raises(ProtoConflictError):
        a.commit()


def test_intentions_apply_on_the_current_map(db):
    """R35: A's write lands on the map B published, which keeps B's new cell."""
    db.run(lambda tr: tr.new_mutable("c", 0))
    a, b = db.new_transaction(), db.new_transaction()
    a.set_mutable("c", 1)
    b.new_mutable("d", 2)
    b.commit()
    a.commit()
    assert read(db, "c") == 1 and read(db, "d") == 2
