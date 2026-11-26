"""The commit protocol of spec section 8 (Task 12): preparation, pre-pass and delta validation.

The pre-pass validates against the last published root R0 and reapplies on it without the space
root.  Under the space root, a root that did not change (R1 is R0) is published from the candidate
with no storage read and no reapplication (section 12 item 8, R64); a root that changed is checked
by delta validation over the diff R0 -> R1 (R65) and reapplied once.  Interleavings are forced
with hooks (the start of the lock step) and the group-commit queue, never with clocks.

Every test here runs with ``transactions._VERIFY_DELTA`` on (except the cost test, which counts
the comparisons under the lock): each delta validation is checked against a full validation of
the same root (ruling 2 of Task 12).
"""
import threading

import pytest

from proto_db import transactions
from proto_db.collections.dictionaries import Dictionary
from proto_db.errors import ProtoConflictError
from proto_db.preconditions import Unit, UnitKind
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace, Transaction
from tests.support.counting_storage import CountingStorage
from tests.support.stale_prepass import stale_prepass

pytestmark = [pytest.mark.timeout(120)]


class LockStorage(CountingStorage):
    """Counts the reads made while the space root is held, and the root lock acquisitions."""

    def __init__(self, inner):
        super().__init__(inner)
        self.held_reads: list = []
        self.root_locks = 0

    def __getattr__(self, name):
        attribute = super().__getattr__(name)
        if name == "root_lock":
            def root_lock(*args, **kwargs):
                self.root_locks += 1
                return attribute(*args, **kwargs)
            return root_lock
        if callable(attribute) and (name in ("get_atom", "get_blob") or name.startswith("read_")):
            def read(*args, **kwargs):
                if Transaction._holding_root_lock():
                    self.held_reads.append((name, args[0] if args else None))
                return attribute(*args, **kwargs)
            return read
        return attribute


@pytest.fixture(autouse=True)
def verify_delta(monkeypatch):
    monkeypatch.setattr(transactions, "_VERIFY_DELTA", True)


@pytest.fixture
def storage():
    return LockStorage(MemoryStorage())


@pytest.fixture
def space(storage):
    s = ObjectSpace(storage)
    yield s
    s.close()


@pytest.fixture
def db(space):
    return space.new_database("db")


@pytest.fixture
def reapplications(monkeypatch):
    """``{"held": n, "free": n}``: Dictionary reapplications with and without the space root."""
    counts = {"held": 0, "free": 0}
    original = Dictionary._reapplied_on

    def counted(self, current, changes=None):
        counts["held" if Transaction._holding_root_lock() else "free"] += 1
        return original(self, current, changes)
    monkeypatch.setattr(Dictionary, "_reapplied_on", counted)
    return counts


def seed(db, n=100):
    def build(tr):
        d = Dictionary(context=tr)
        for i in range(n):
            d = d.set_at(f"k{i}", i)
        tr.set_root_object("d", d)
    db.run(build)


def content(db):
    return dict(db.run(lambda tr: tr.get_root_object("d")).items())


def write(tr, key, value):
    tr.set_root_object("d", tr.get_root_object("d").set_at(key, value))


def committed(db, key, value):
    tr = db.new_transaction()
    write(tr, key, value)
    tr.commit()


def before_lock_step(monkeypatch, action):
    """Run ``action()`` once, after the next commit's pre-pass and before its lock step."""
    original = transactions._commit_root
    pending = [action]

    def hooked(*args, **kwargs):
        if pending:
            pending.pop()()
        return original(*args, **kwargs)
    monkeypatch.setattr(transactions, "_commit_root", hooked)


def test_prepass_unchanged_root_no_storage_reads_under_lock(db, storage, reapplications):
    seed(db)
    t = db.new_transaction()
    assert t.get_root_object("d").get_at("k1") == 1
    write(t, "new", 1)
    committed(db, "k2", -2)         # base != R0: the pre-pass reapplies t's intention on R0
    storage.held_reads.clear()
    reapplications.update(held=0, free=0)
    t.commit()
    assert storage.held_reads == []
    assert reapplications == {"held": 0, "free": 1}
    assert content(db)["new"] == 1 and content(db)["k2"] == -2


def test_prepass_changed_root_validates_and_reapplies_once(db, storage, monkeypatch, reapplications):
    seed(db)
    deltas = []
    original_delta = Transaction._delta_check

    def delta(self, candidate, db_root):
        deltas.append((candidate.db_root.version, db_root.version))
        return original_delta(self, candidate, db_root)
    monkeypatch.setattr(Transaction, "_delta_check", delta)
    t = db.new_transaction()
    assert t.get_root_object("d").get_at("k1") == 1
    write(t, "new", 1)
    before_lock_step(monkeypatch, lambda: committed(db, "k2", -2))
    reapplications.update(held=0, free=0)
    t.commit()
    assert reapplications == {"held": 1, "free": 0}         # R0 is t's base: reapplied once, on R1
    assert len(deltas) == 1 and deltas[0][1] == deltas[0][0] + 1
    assert content(db)["new"] == 1 and content(db)["k2"] == -2


def test_prepass_changed_root_conflict_names_the_first_unit(db, monkeypatch):
    seed(db)
    t = db.new_transaction()
    d = t.get_root_object("d")
    assert (d.get_at("k1"), d.get_at("k2"), d.get_at("k3")) == (1, 2, 3)
    t.new_mutable("cell", 0)

    def concurrent(tr):
        tr.set_root_object("d", tr.get_root_object("d").set_at("k3", -3).set_at("k2", -2))
        tr.new_mutable("cell", 5)
    before_lock_step(monkeypatch, lambda: db.run(concurrent))
    with pytest.raises(ProtoConflictError) as e:
        t.commit()
    # Full validation raises the first violated unit in recording order: k2 before k3 and the cell.
    assert e.value.unit == Unit(UnitKind.KEY, "d", key="k2")


def test_prepass_conflict_raised_without_taking_lock(db, storage, monkeypatch):
    seed(db)
    t = db.new_transaction()
    assert t.get_root_object("d").get_at("k1") == 1
    write(t, "new", 1)
    committed(db, "k1", -1)
    lock_steps = []
    original = transactions._commit_root
    monkeypatch.setattr(transactions, "_commit_root", lambda *a, **k: lock_steps.append(1) or original(*a, **k))
    storage.root_locks = 0
    with pytest.raises(ProtoConflictError) as e:
        t.commit()
    assert e.value.unit == Unit(UnitKind.KEY, "d", key="k1")
    assert storage.root_locks == 0 and lock_steps == []
    assert "new" not in content(db)


def test_delta_validation_cost_independent_of_read_set(db, monkeypatch):
    monkeypatch.setattr(transactions, "_VERIFY_DELTA", False)   # it would compare everything under the lock
    seed(db, 1000)
    comparisons = []
    for name in ("_root_violation", "_cell_violation", "_range_violation"):
        original = getattr(Transaction, name)

        def counted(self, *args, _original=original, _name=name, **kwargs):
            if Transaction._holding_root_lock():
                comparisons.append((_name, args[0]))
            return _original(self, *args, **kwargs)
        monkeypatch.setattr(Transaction, name, counted)
    a = db.new_transaction()
    d = a.get_root_object("d")
    assert sum(d.get_at(f"k{i}") for i in range(1000)) == sum(range(1000))
    assert len(a._read_log.preconditions()) >= 1000
    a.set_root_object("x", 1)
    before_lock_step(monkeypatch, lambda: committed(db, "unrelated", 1))
    a.commit()
    # The diff R0 -> R1: root "d" changed (its ROOT_PRESENCE unit is checked once), one key of it
    # changed (not read: nothing to compare).  At most the diff size, never the 1000 keys read.
    assert len(comparisons) <= 2, comparisons
    assert content(db)["unrelated"] == 1


def test_group_commit_second_entry_builds_on_first(db, space, monkeypatch, reapplications):
    seed(db)
    group = transactions._group_of(space.storage)
    lock_step = Transaction._build_root
    held, release = threading.Event(), threading.Event()
    deltas = []
    original_delta = Transaction._delta_check

    def delta(self, candidate, db_root):
        deltas.append((self, candidate.pointer, db_root.version))
        return original_delta(self, candidate, db_root)
    monkeypatch.setattr(Transaction, "_delta_check", delta)
    blocker = db.new_transaction()
    blocker.set_root_object("other", 1)

    def build_root(self, current, stamp=None):
        if self is blocker:
            held.set()
            assert release.wait(60)
            raise ProtoConflictError("held by the test: publishes nothing")
        return lock_step(self, current, stamp)
    monkeypatch.setattr(Transaction, "_build_root", build_root)
    r0 = space.storage.read_root()
    t1, t2 = db.new_transaction(), db.new_transaction()
    write(t1, "a", 1)
    write(t2, "b", 2)
    errors = {}

    def commit(tr, name):
        try:
            tr.commit()
        except BaseException as e:      # reported by the assertions
            errors[name] = e
    first = threading.Thread(target=commit, args=(blocker, "blocker"))
    first.start()
    assert held.wait(60)
    reapplications.update(held=0, free=0)
    others = [threading.Thread(target=commit, args=(t, n)) for t, n in ((t1, "t1"), (t2, "t2"))]
    for thread in others:
        thread.start()
    with group.cond:            # both pre-passes done on R0, both queued behind the held round
        assert group.cond.wait_for(lambda: len(group.queue) == 2, timeout=60)
    release.set()
    for thread in [first] + others:
        thread.join(60)
    assert set(errors) == {"blocker"}
    # One round: the first entry finds R0 (its candidate, no reapplication); the second diffs R0
    # against the root the first produced and reapplies once.
    assert space.storage.read_root().epoch == r0.epoch + 1
    assert reapplications == {"held": 1, "free": 0}         # R0 is both bases
    assert len(deltas) == 1
    second, pointer, version = deltas[0]
    assert second in (t1, t2) and pointer == r0.pointer
    data = content(db)
    assert data["a"] == 1 and data["b"] == 2 and "other" not in data


def test_read_only_transaction_never_conflicts(db, storage):
    seed(db)
    db.run(lambda tr: tr.new_mutable("cell", 0))
    t = db.new_transaction()
    assert t.get_root_object("d").get_at("k1") == 1
    assert t.get_mutable("cell") is not None

    def concurrent(tr):
        tr.set_root_object("d", tr.get_root_object("d").set_at("k1", -1))
        tr.set_mutable("cell", 9)
    db.run(concurrent)
    epoch = storage.read_root().epoch
    storage.root_locks = 0
    t.commit()                  # R23: no intention, nothing validated, nothing published
    assert storage.root_locks == 0 and storage.read_root().epoch == epoch


SCENARIOS = {
    "cell": (lambda tr: tr.get_mutable("c"), lambda tr: tr.set_mutable("c", 9), Unit(UnitKind.CELL, key="c")),
    "cell_names": (lambda tr: tr.mutable_names(), lambda tr: tr.new_mutable("z", 1), Unit(UnitKind.CELL_NAMES)),
    "root_names": (lambda tr: tr.root_names(), lambda tr: tr.set_root_object("zz", 1), Unit(UnitKind.ROOT_NAMES)),
    "object_root": (lambda tr: tr.get_root_object("obj"), lambda tr: tr.set_root_object("obj", 2),
                    Unit(UnitKind.ROOT, "obj")),
    "other_cell": (lambda tr: tr.get_mutable("c"), lambda tr: tr.set_mutable("other", 9), None),
}


@pytest.mark.parametrize("scenario", sorted(SCENARIOS))
def test_delta_validation_of_root_and_cell_units(db, monkeypatch, scenario):
    """Under the space root, a change between the pre-pass and the lock step to a cell, the cell
    names, the root names or an object root is found by the diff of the maps (R65), as full
    validation finds it; an unrelated cell change is not a conflict."""
    read, concurrent, unit = SCENARIOS[scenario]
    db.run(lambda tr: (tr.new_mutable("c", 0), tr.new_mutable("other", 0), tr.set_root_object("obj", 1)))
    t = db.new_transaction()
    read(t)
    t.set_root_object("x", 1)
    before_lock_step(monkeypatch, lambda: db.run(concurrent))
    if unit is None:
        t.commit()
        return
    with pytest.raises(ProtoConflictError) as e:
        t.commit()
    assert e.value.unit == unit


def test_stale_prepass_delta_validation_decides_under_the_lock(db, storage, monkeypatch):
    """Task 12 review, minor a: with every pre-pass forced onto its own snapshot (an older root),
    the concurrent change is decided under the space root by delta validation (checked against
    full validation by ``_VERIFY_DELTA``): an unrelated key composes, a read key conflicts."""
    stats = stale_prepass(monkeypatch)
    seed(db)
    t = db.new_transaction()
    assert t.get_root_object("d").get_at("k1") == 1
    write(t, "new", 1)
    committed(db, "k2", -2)
    stats.delta_checks = 0
    t.commit()
    assert stats.delta_checks == 1
    assert content(db)["new"] == 1 and content(db)["k2"] == -2

    t = db.new_transaction()
    assert t.get_root_object("d").get_at("k1") == 1
    write(t, "other", 1)
    committed(db, "k1", -1)
    storage.root_locks = 0
    stats.delta_checks = 0
    with pytest.raises(ProtoConflictError) as e:
        t.commit()
    assert e.value.unit == Unit(UnitKind.KEY, "d", key="k1")
    assert stats.delta_checks == 1 and storage.root_locks == 1      # decided under the lock
    assert "other" not in content(db)
