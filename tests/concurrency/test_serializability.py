"""Serializability checker over the closed set of operations (spec section 12 item 2).

Threads run randomly generated programs (fixed seed) on a small state: an object root, a
Dictionary, a HashDictionary, a Set, a CountedSet, a RepeatedKeysDictionary and a List root, a
key space shared by all, and mutable cells.  A root's kind changes too: ``root_reset`` writes a
value of any kind, ``obj_set`` an object over a collection and a creating collection write
(``d_set``, ``s_add``, ``cs_add``, ``r_add``, ``l_append``) a collection over an object, so
linked collection writes meet concurrent class changes (R12).  Each thread starts at one barrier
and retries a conflicting program in a fresh transaction (no sleep).  For each committed transaction the test records the database
version it published (``Transaction.committed_version``, its place in the commit order), its
program and every value it read; a transaction that published nothing (R23) records the
version it read.  :func:`tests.support.serial_model.check_history` then re-runs the programs in
commit order on the serial model: every value read and the final state must match.  The
CountedSet and RepeatedKeysDictionary writes have no precondition: the checker proves their
composition (D26) serializable, including a decrement past 0; the List appends have none either
(appended in commit order), its ``set_at``/``get_at`` take a position unit, a negative position
and the structural operations (``insert_at``, ``remove_at``) the whole list (R14, R15).

The interleaving is nondeterministic; the assertion is not: whatever order the commits took,
the history must be serializable in that order (spec section 2).

Catches: a precondition missing for a read or a write (a lost update or a stale read that
commits), an intention reapplied wrongly (a key or cell value the serial run does not give),
a linked write reapplied over a root whose class changed (R12), a commit order that does not
match the published versions, and a read-only transaction that sees no consistent prefix.  The
report line counts the class changes committed and the R12 conflicts they caused, the conflicts
by unit and root kind, and the commits whose collection intentions were reapplied over a
concurrent change of the same root (composed).  Task 9 adds equality lookups through the
index every dictionary carries (``x_eq``, ``x_in``: one INDEX_GROUP per decade read, R28) and
so makes every dictionary write an indexed write, maintained from precomputed keys (R24, R49);
the report counts the lookups committed and the INDEX_GROUP/INDEX_DEFS conflicts, and the test
asserts that the index extractor never ran with the space root held (P-C).  Task 10 adds its
operations to the mix.
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
from tests.support.serial_model import (COLLECTIONS, COUNTED_SET_OPS, DICTIONARY_AND_CELL_OPS, LIST_OPS,
                                        LOOKUP_OPS, MULTIMAP_OPS, OBJECT, RECORDS, SET_OPS, Op, Record, StateNames,
                                        capture, check_history, decade_index, execute, extractor_calls_under_root,
                                        family_of, kinds_by_version, random_program, roots_by_version)

SEED = 20261124
THREADS = 4
PER_THREAD = 200

NAMES = StateNames(
    roots={"obj": OBJECT, "dict": "Dictionary", "hdict": "HashDictionary", "set": "Set", "cset": "CountedSet",
           "rkd": "RepeatedKeysDictionary", "list": "List"},
    keys=tuple(f"k{i}" for i in range(6)),
    cells=tuple(f"c{i}" for i in range(4)),
)

#: The draw: every operation of Tasks 2-4, 6 and 7, with the operations on fine units (keys,
#: elements, counts, records, positions) and the name-set reads drawn more often, so the class
#: changes that root_reset, obj_set and the creating writes, and the seven roots, do not crowd out
#: the unit-level interleavings.
MIX = (DICTIONARY_AND_CELL_OPS + ("root_names", "c_names") * 2 + ("d_get", "d_has", "d_set", "d_remove") * 4
       + SET_OPS + ("s_has", "s_add", "s_remove") * 3
       + COUNTED_SET_OPS + ("cs_count", "cs_has", "cs_add", "cs_remove") * 4
       + MULTIMAP_OPS + ("r_get", "r_has", "r_add", "r_remove_record") * 3
       + LIST_OPS + ("l_get", "l_set", "l_append", "l_extend") * 3
       + LOOKUP_OPS * 3)

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
            tr.set_root_object(name, d.add_index(decade_index()))
        s = tr.new_set()
        for k in rng.sample(NAMES.keys, 3):
            s = s.add(k)
        tr.set_root_object("set", s)
        c = tr.new_counted_set()
        for k in rng.sample(NAMES.keys, 3):
            for _ in range(rng.randint(1, 2)):
                c = c.add(k)
        tr.set_root_object("cset", c)
        r = tr.new_repeated_keys_dictionary()
        for k in rng.sample(NAMES.keys, 3):
            r = r.set_at(k, rng.randrange(RECORDS))
        tr.set_root_object("rkd", r)
        tr.set_root_object("list", tr.new_list().extend(rng.choice(NAMES.values) for _ in range(3)))
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
_COLLECTION_WRITES = ("d_set", "d_remove", "s_add", "s_remove", "s_union", "s_difference", "s_intersection",
                      "cs_add", "cs_remove", "r_add", "r_remove_record", "r_remove_key", "l_set", "l_append",
                      "l_extend", "l_insert", "l_remove")


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
            base = db.snapshot_at(tr.snapshot_ref).version
            read_only = version is None
            if read_only:
                version = base
            records.append(Record(version, program, reads, read_only, thread, base))
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


def composed_counts(records, initial) -> dict:
    """Per collection kind, the published commits whose linked writes of a root were reapplied
    over a concurrent change of that root (its content at the commit's base differs from the
    content just before the commit): the intentions composed with another commit's."""
    roots = roots_by_version(records, initial)
    counts = Counter()
    for record in records:
        if record.read_only:
            continue
        before = roots[record.version - 1]
        at_base = roots[record.base]
        for name in {op.target for op in record.program if op.kind in _COLLECTION_WRITES}:
            kind = at_base.get(name, (None,))[0]
            family = next(family_of(op.kind) for op in record.program if op.target == name
                          and op.kind in _COLLECTION_WRITES)
            if kind in family and before.get(name) != at_base.get(name):
                counts[kind] += 1
    return dict(sorted(counts.items()))


def conflicts_by_root_kind(records, initial, stats) -> dict:
    """``"<unit kind>@<root kind at the attempt's base>"`` -> number of conflicts."""
    kinds = kinds_by_version(records, initial)
    counts = Counter()
    for unit, base, _ in stats.conflicts:
        if unit is None:
            counts["none"] += 1
            continue
        counts[f"{unit.kind.name}@{kinds[base].get(unit.root) if unit.root else '-'}"] += 1
    return dict(sorted(counts.items()))


def test_serializability_over_the_closed_operation_set(db):
    rng = random.Random(SEED)
    _seed_state(db, rng)
    initial = _capture(db)
    programs = [[random_program(rng, NAMES, MIX) for _ in range(PER_THREAD)]
                for _ in range(THREADS)]
    per_thread = [[] for _ in range(THREADS)]
    stats = Stats()

    calls_before = extractor_calls_under_root()
    elapsed = run_threads([_worker(db, t, programs[t], per_thread[t], stats) for t in range(THREADS)])

    records = [r for rs in per_thread for r in rs]
    assert len(records) == THREADS * PER_THREAD
    read_only = sum(r.read_only for r in records)
    check_history(records, initial, final=_capture(db))
    by_unit = Counter(unit.kind.name if unit is not None else "none" for unit, _, _ in stats.conflicts)
    calls, held = (after - before for after, before in zip(extractor_calls_under_root(), calls_before))
    assert calls > 0 and held == 0, f"the index extractor ran {held} of {calls} times with the space root held"
    lookups = Counter(op.kind for r in records for op in r.program if op.kind in LOOKUP_OPS)
    report("serializability collections+cells", elapsed,
           f"{stats.commits} commits, {stats.attempts} attempts, {len(stats.conflicts)} conflicts",
           f"{len(records) - read_only} published, {read_only} read-only",
           f"conflicts by unit {dict(sorted(by_unit.items()))}",
           f"by unit@root kind {conflicts_by_root_kind(records, initial, stats)}",
           f"composed {composed_counts(records, initial)}",
           f"lookups {dict(sorted(lookups.items()))}, index group conflicts {by_unit.get('INDEX_GROUP', 0)}, "
           f"index definition conflicts {by_unit.get('INDEX_DEFS', 0)}, extractor calls {calls}",
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
