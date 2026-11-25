"""Serializability checker over the closed set of operations (spec section 12 item 2).

Threads run randomly generated programs (fixed seed) on a small state: an object root, a
Dictionary root, a HashDictionary root, a key space shared by the three, and mutable cells.
A root's kind changes too: ``root_reset`` writes a value of any kind, ``obj_set`` an object
over a collection and ``d_set`` a collection over an object, so linked Dictionary and
HashDictionary writes meet concurrent class changes (R12).  Each thread starts at one barrier
and retries a conflicting program in a fresh transaction (no sleep).  For each committed transaction the test records the database
version it published (``Transaction.committed_version``, its place in the commit order), its
program and every value it read; a transaction that published nothing (R23) records the
version it read.  :func:`tests.support.serial_model.check_history` then re-runs the programs in
commit order on the serial model: every value read and the final state must match.

The interleaving is nondeterministic; the assertion is not: whatever order the commits took,
the history must be serializable in that order (spec section 2).

Catches: a precondition missing for a read or a write (a lost update or a stale read that
commits), an intention reapplied wrongly (a key or cell value the serial run does not give),
a linked write reapplied over a root whose class changed (R12), a commit order that does not
match the published versions, and a read-only transaction that sees no consistent prefix.  The
report line counts the class changes committed and the R12 conflicts they caused.  Later tasks (6, 7, 9, 10) add their operations to the mix.
"""
import random
import threading
from collections import Counter

import pytest

from proto_db.collections.dictionaries import Dictionary
from proto_db.collections.hash_dictionaries import HashDictionary
from proto_db.errors import ProtoConflictError
from proto_db.preconditions import UnitKind
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from tests.concurrency.harness import report, run_threads
from tests.support.serial_model import (COLLECTIONS, DICTIONARY_AND_CELL_OPS, OBJECT, Op, Record, StateNames,
                                        capture, check_history, execute, kinds_by_version, random_program)

SEED = 20261124
THREADS = 4
PER_THREAD = 200

NAMES = StateNames(
    roots={"obj": OBJECT, "dict": "Dictionary", "hdict": "HashDictionary"},
    keys=tuple(f"k{i}" for i in range(6)),
    cells=tuple(f"c{i}" for i in range(4)),
)

#: The draw: every operation of Tasks 2-4, with the keyed Dictionary/HashDictionary operations
#: (the fine units) drawn three times as often, so the class changes that root_reset, obj_set
#: and d_set make do not crowd out the key-level interleavings.
MIX = DICTIONARY_AND_CELL_OPS + ("d_get", "d_has", "d_set", "d_remove") * 2

pytestmark = [pytest.mark.freethreaded, pytest.mark.timeout(240)]


@pytest.fixture
def db():
    space = ObjectSpace(MemoryStorage())
    yield space.new_database("db")
    space.close()


def _seed_state(db, rng):
    """The initial state: every root present with some keys, three of the four cells."""
    def build(tr):
        tr.set_root_object("obj", tr.new_object(**{k: rng.choice(NAMES.values) for k in NAMES.keys[:3]}))
        for name, cls in (("dict", Dictionary), ("hdict", HashDictionary)):
            d = cls(context=tr)
            for k in rng.sample(NAMES.keys, 3):
                d = d.set_at(k, rng.choice(NAMES.values))
            tr.set_root_object(name, d)
        for name in NAMES.cells[:3]:
            tr.new_mutable(name, rng.choice(NAMES.values))
    db.run(build)


def _capture(db):
    tr = db.new_transaction()
    try:
        return capture(tr, NAMES)
    finally:
        tr.abort()


#: Bound on the attempts of one program: a hang shows as a failure, not a timeout.
MAX_ATTEMPTS = 100_000
#: Operations that write a collection through its linked copy when the root holds one.
_COLLECTION_WRITES = ("d_set", "d_remove")


class Stats:
    """Attempts, commits, and each conflict as ``(unit, base version, program)``."""

    def __init__(self):
        self._lock = threading.Lock()
        self.attempts = 0
        self.commits = 0
        self.conflicts = []

    def attempt(self):
        with self._lock:
            self.attempts += 1

    def committed(self):
        with self._lock:
            self.commits += 1

    def conflict(self, unit, base, program):
        with self._lock:
            self.conflicts.append((unit, base, program))


def _run(db, program, stats):
    """Run ``program`` until it commits, each attempt a fresh transaction and no sleep (a
    conflict means another program committed); return the committed transaction and its reads."""
    for _ in range(MAX_ATTEMPTS):
        stats.attempt()
        tr = db.new_transaction()
        try:
            reads = execute(tr, program, NAMES)
            tr.commit()
            stats.committed()
            return tr, reads
        except ProtoConflictError as e:
            stats.conflict(e.unit, db.snapshot_at(tr.snapshot_ref).version, program)
        finally:
            if tr.is_active:
                tr.abort()
    pytest.fail(f"program {program} did not commit in {MAX_ATTEMPTS} attempts")


def _worker(db, thread, programs, records, stats):
    def work():
        for program in programs:
            tr, reads = _run(db, program, stats)
            version = tr.committed_version
            read_only = version is None
            if read_only:
                version = db.snapshot_at(tr.snapshot_ref).version
            records.append(Record(version, program, reads, read_only, thread))
    return work


def class_change_counts(records, initial, stats) -> dict:
    """How much the run exercised R12, from a serial replay of the history:

    * ``class_changes``: commits that left a root present with another kind than before;
    * ``r12_conflicts``: ROOT_PRESENCE conflicts whose root, after the attempt's base, next
      changed kind (not presence);
    * ``r12_linked_write_conflicts``: those of them whose program writes that root through a
      linked collection (a d_set/d_remove on a root that held a collection at the base).
    """
    kinds = kinds_by_version(records, initial)
    versions = sorted(kinds)
    changes = sum(1 for before, after in zip(versions, versions[1:])
                  if any(name in kinds[before] and kinds[before][name] != kind
                         for name, kind in kinds[after].items()))
    r12 = linked = 0
    for unit, base, program in stats.conflicts:
        if unit is None or unit.kind is not UnitKind.ROOT_PRESENCE:
            continue
        held = kinds[base].get(unit.root)
        nxt = next((kinds[v].get(unit.root) for v in versions if v > base and kinds[v].get(unit.root) != held),
                   None)
        if held is None or nxt is None:
            continue
        r12 += 1
        if held in COLLECTIONS and any(op.kind in _COLLECTION_WRITES and op.target == unit.root for op in program):
            linked += 1
    return {"class_changes": changes, "r12_conflicts": r12, "r12_linked_write_conflicts": linked}


def test_serializability_dictionary_and_cells(db):
    rng = random.Random(SEED)
    _seed_state(db, rng)
    initial = _capture(db)
    programs = [[random_program(rng, NAMES, MIX) for _ in range(PER_THREAD)]
                for _ in range(THREADS)]
    per_thread = [[] for _ in range(THREADS)]
    stats = Stats()

    elapsed = run_threads([_worker(db, t, programs[t], per_thread[t], stats) for t in range(THREADS)])

    records = [r for rs in per_thread for r in rs]
    assert len(records) == THREADS * PER_THREAD
    read_only = sum(r.read_only for r in records)
    check_history(records, initial, final=_capture(db))
    by_unit = Counter(unit.kind.name if unit is not None else "none" for unit, _, _ in stats.conflicts)
    report("serializability dictionary+cells", elapsed,
           f"{stats.commits} commits, {stats.attempts} attempts, {len(stats.conflicts)} conflicts",
           f"{len(records) - read_only} published, {read_only} read-only",
           f"conflicts by unit {dict(sorted(by_unit.items()))}",
           class_change_counts(records, initial, stats))


def test_checker_rejects_a_lost_update(db):
    """The checker itself: a history in which two increments of one cell both read the same
    value (a lost update) is not serializable in any order, and must be rejected."""
    rng = random.Random(SEED)
    _seed_state(db, rng)
    initial = _capture(db)
    cell = NAMES.cells[0]
    inc = [Op("c_inc", cell)]
    seen = initial.cells.get(cell)
    after = initial.copy()
    after.cells[cell] = 1 if seen is None else seen + 1
    records = [Record(initial.version + 1, inc, [seen]), Record(initial.version + 2, inc, [seen])]
    with pytest.raises(AssertionError, match="not serializable"):
        check_history(records, initial, final=after)


def test_random_program_rejects_undrawable_names():
    """No operation of the mix has a target: a clear error, not an endless draw."""
    names = StateNames(roots={}, keys=(), cells=())
    with pytest.raises(ValueError, match="No operation"):
        random_program(random.Random(SEED), names, ("d_get", "c_get"))
    with pytest.raises(ValueError, match="Unknown operation"):
        random_program(random.Random(SEED), NAMES, ("no_such_op",))
