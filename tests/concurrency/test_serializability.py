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
index every dictionary carries (``x_eq``, ``x_in``: one INDEX_GROUP per group read, R28) and
so makes every dictionary write an indexed write, maintained from precomputed keys (R24, R49);
the report counts the lookups committed and the INDEX_GROUP/INDEX_DEFS conflicts, and the test
asserts that the index extractor never ran with the space root held (P-C).  Task 10 adds range
reads (R67, R68): ``x_range`` through the RangeIndex every dictionary now also carries, and the
Dictionary windows ``w_after`` (``keys(after=)``) and ``w_window`` (the planner's positional
window); a deterministic pre-run assertion (:func:`range_exposure`: a serial replay of the seeded
programs, counting only the reads that record a range there) proves that range reads and writes
inside them of other threads get enough chances to meet, and the report counts the range reads
that recorded a range and the RANGE conflicts.
"""
import random
import threading
from collections import Counter

import pytest

from proto_db import transactions
from proto_db.collections.dictionaries import Dictionary
from proto_db.collections.hash_dictionaries import HashDictionary
from proto_db.errors import ProtoConflictError
from proto_db.preconditions import UnitKind
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from tests.concurrency.harness import report, run_threads
from tests.support.forced_overlap import overlap_commits
from tests.support.stale_prepass import stale_prepass
from tests.support.serial_model import (COLLECTIONS, COUNTED_SET_OPS, DICTIONARY_AND_CELL_OPS, LIST_OPS,
                                        LOOKUP_OPS, MULTIMAP_OPS, OBJECT, PERSIST_OPS, RANGE_OPS, RECORDS, SET_OPS, Op,
                                        Record,
                                        StateNames, capture, check_history, execute, extractor_calls_under_root,
                                        family_of, group_of, kinds_by_version, random_program, range_reads,
                                        reset_range_reads, roots_by_version, with_indexes)

SEED = 20261124
THREADS = 4
PER_THREAD = 400

NAMES = StateNames(
    roots={"obj": OBJECT, "dict": "Dictionary", "hdict": "HashDictionary", "set": "Set", "cset": "CountedSet",
           "rkd": "RepeatedKeysDictionary", "list": "List"},
    keys=tuple(f"k{i}" for i in range(6)),
    cells=tuple(f"c{i}" for i in range(4)),
)

#: The draw: every operation of Tasks 2-4, 6 and 7, with the operations on fine units (keys,
#: elements, counts, records, positions) and the name-set reads drawn more often, so the class
#: changes that root_reset, obj_set and the creating writes, and the seven roots, do not crowd out
#: the unit-level interleavings.  Task 10: the Dictionary windows are drawn more often, and ``d_set``
#: more often than ``d_remove`` so that a dictionary holds most keys and a one-row window at offset 1
#: mostly ends before the last key (a range, not the whole root) with a key before it that a
#: concurrent write may add or remove; lookups keep their share (group exposure).
MIX = (DICTIONARY_AND_CELL_OPS + ("root_names", "c_names") * 2 + ("d_get", "d_has", "d_set", "d_remove") * 4
       + SET_OPS + ("s_has", "s_add", "s_remove") * 3
       + COUNTED_SET_OPS + ("cs_count", "cs_has", "cs_add", "cs_remove") * 4
       + MULTIMAP_OPS + ("r_get", "r_has", "r_add", "r_remove_record") * 3
       + LIST_OPS + ("l_get", "l_set", "l_append", "l_extend") * 3
       + LOOKUP_OPS * 4
       + RANGE_OPS * 3 + ("w_after", "w_window") * 8 + ("d_set",) * 8)

#: The final review's mix (C1): a root's value persisted elsewhere (``p_copy``, ``p_nest``) among the
#: reads and writes of the roots it is copied from and to, so a copy meets concurrent changes of
#: its source and reads of its destination.  Kept apart from :data:`MIX`, whose seeded programs the
#: exposure floors below are measured on.
PERSIST_MIX = (DICTIONARY_AND_CELL_OPS + ("d_get", "d_set", "d_remove", "d_items", "obj_get") * 3
               + SET_OPS + LIST_OPS + PERSIST_OPS * 6)
#: Deterministic floor of persisting operations in the seeded PERSIST_MIX programs.
PERSIST_OPS_MIN = 1000

#: Deterministic floors of :func:`range_exposure` for the seeded programs, counting only the reads
#: that record a range in the serial replay (seed values 350, 419, 27938, 26510; Task 10 report).
RANGE_PROGRAMS_MIN = 300
RANGE_WRITERS_MIN = 350
RANGE_PAIRS_MIN = 22000
RANGE_WINDOW_PAIRS_MIN = 20000

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
            tr.set_root_object(name, with_indexes(d))
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


def group_exposure(programs) -> dict:
    """From the seeded programs alone (no interleaving): how much they expose B4's INDEX_GROUP
    preconditions.  ``lookup_programs``: programs with an indexed lookup; ``group_writers``:
    programs whose ``d_set`` writes a value into a group some lookup of another thread reads
    (same root); ``pairs``: (lookup program, writer program) pairs of different threads on a
    common (root, group) -- the chances a lookup's group changes under it."""
    def looked(program):
        out = set()
        for op in program:
            if op.kind == "x_eq":
                out.add((op.target, op.key))
            elif op.kind == "x_in":
                out.update((op.target, g) for g in op.key)
        return out

    def written(program):
        return {(op.target, group_of(op.value)) for op in program if op.kind == "d_set"}

    looks = [[looked(p) for p in thread] for thread in programs]
    writes = [[written(p) for p in thread] for thread in programs]
    lookup_programs = sum(1 for thread in looks for g in thread if g)
    pairs = writers = 0
    for t, thread in enumerate(writes):
        others = set().union(*(g for u, th in enumerate(looks) if u != t for g in th))
        for groups in thread:
            if groups & others:
                writers += 1
    for t, thread in enumerate(looks):
        for groups in thread:
            if groups:
                pairs += sum(1 for u, th in enumerate(writes) if u != t for w in th if groups & w)
    return {"lookup_programs": lookup_programs, "group_writers": writers, "pairs": pairs}


def _range_read(model, op):
    """What ``op`` (a range read) covers when it runs on ``model``, if it records a range there:
    a predicate on a ``d_set``/``d_remove`` of the same root that may change what it read
    (``x_range``: the value set is inside its interval; ``w_after``: the key is after its start
    and, unless the window was exhausted, at most its last key; ``w_window``: the key is at most
    the window's last key, since a key before it shifts its positions); None when it records no
    range there (the root holds no Dictionary, or a window reaches the end: the whole root)."""
    kind, content = model.roots.get(op.target, (None, None))
    if op.kind == "x_range":
        if kind not in ("Dictionary", "HashDictionary"):
            return None
        low, high = op.key
        return lambda w: w.kind == "d_set" and low <= w.value <= high
    if kind != "Dictionary":
        return None
    keys = sorted(content)
    if op.kind == "w_after":
        after = [k for k in keys if k > op.key]
        if op.value is None or op.value >= len(after):
            return lambda w: w.key > op.key
        last = after[op.value - 1]
        return lambda w: op.key < w.key <= last
    stop = op.key + op.value
    if stop >= len(keys):
        return None
    last = keys[stop - 1]
    return lambda w: w.key <= last


def range_exposure(programs, initial) -> dict:
    """Deterministic (no interleaving): how much the seeded programs expose the range
    preconditions (R67, R68), counting only the range reads that record a range.  Whether a
    read records one depends on the state it reads, so the programs are replayed serially on the
    model from ``initial``, round-robin across threads (program i of each thread in turn), and
    each range read is judged on the state it meets there (:func:`_range_read`).
    ``range_programs``: programs with a read that records a range; ``range_writers``: programs
    whose ``d_set``/``d_remove`` may change what such a read of another thread covers;
    ``pairs``: (reader, writer) program pairs of different threads that may meet that way;
    ``window_pairs``: those whose reader is a ``w_window`` or a ``w_after``."""
    model = initial.copy()
    covered = [[[] for _ in thread] for thread in programs]   # thread -> program -> (kind, target, predicate)
    for i in range(max(len(thread) for thread in programs)):
        for t, thread in enumerate(programs):
            if i >= len(thread):
                continue
            for op in thread[i]:
                if op.kind in RANGE_OPS:
                    predicate = _range_read(model, op)
                    if predicate is not None:
                        covered[t][i].append((op.kind, op.target, predicate))
                model.apply(op)

    def meets(reads, writer):
        return [kind for kind, target, predicate in reads for w in writer
                if w.target == target and w.kind in ("d_set", "d_remove") and predicate(w)]

    range_programs = sum(1 for thread in covered for reads in thread if reads)
    writers = pairs = window_pairs = 0
    for t, thread in enumerate(programs):
        for program in thread:
            if any(meets(reads, program) for u, th in enumerate(covered) if u != t for reads in th if reads):
                writers += 1
    for t, thread in enumerate(covered):
        for reads in thread:
            if not reads:
                continue
            for u, th in enumerate(programs):
                if u == t:
                    continue
                for writer in th:
                    met = meets(reads, writer)
                    if met:
                        pairs += 1
                        window_pairs += any(kind != "x_range" for kind in met)
    return {"range_programs": range_programs, "range_writers": writers, "pairs": pairs,
            "window_pairs": window_pairs}


def test_serializability_over_the_closed_operation_set(db):
    _check_serializable(db, "serializability collections+cells")


@pytest.mark.parametrize("r0", ["published", "stale"])
def test_serializability_with_delta_verification(db, monkeypatch, r0):
    """Task 12 review, minor a: the checker's seed with ``transactions._VERIFY_DELTA`` on, so each
    delta validation under the space root (R65) is compared with a full validation of the same
    root.  ``stale``: every pre-pass reads its own snapshot as R0 (:func:`stale_prepass`), so every
    concurrent change is decided by delta validation under the lock, none by the pre-pass."""
    monkeypatch.setattr(transactions, "_VERIFY_DELTA", True)
    stats, overlap = _stale(monkeypatch, r0)
    _check_serializable(db, f"serializability with delta verification ({r0} R0)", overlap=overlap)
    if stats is not None:
        assert stats.forced > 0 and stats.delta_checks > 0, (stats.forced, stats.delta_checks, overlap)


@pytest.mark.parametrize("r0", ["published", "stale"])
def test_serializability_of_collections_persisted_elsewhere(db, monkeypatch, r0):
    """Final review C1: a root's linked collection (or a version derived from it) written into
    another root or nested in a new object publishes its whole content, so the transaction must
    depend on the whole source root.  Run with ``transactions._VERIFY_DELTA`` on, with the
    published R0 and with a stale one (every concurrent change decided by delta validation)."""
    monkeypatch.setattr(transactions, "_VERIFY_DELTA", True)
    stats, overlap = _stale(monkeypatch, r0)
    _check_serializable(db, f"serializability, collections persisted elsewhere ({r0} R0)", PERSIST_MIX,
                        overlap=overlap)
    if stats is not None:
        assert stats.forced > 0 and stats.delta_checks > 0, (stats.forced, stats.delta_checks, overlap)


def _stale(monkeypatch, r0):
    """For ``r0 == "stale"``: every pre-pass forced onto its snapshot (:func:`stale_prepass`) and
    every worker commit forced to overlap another one (:func:`overlap_commits`), so that a
    concurrent change reaches delta validation under the space root on every run, whatever the
    scheduler does (on a GIL build a run could otherwise end with none: the 2.0.0rc5 CI failure
    ``(1188, 0)``).  ``(None, None)`` for the published R0."""
    if r0 != "stale":
        return None, None
    return stale_prepass(monkeypatch), overlap_commits(monkeypatch, THREADS)


def _check_serializable(db, label, mix=MIX, overlap=None):
    rng = random.Random(SEED)
    _seed_state(db, rng)
    initial = _capture(db)
    programs = [[random_program(rng, NAMES, mix) for _ in range(PER_THREAD)]
                for _ in range(THREADS)]
    per_thread = [[] for _ in range(THREADS)]
    stats = Stats()
    if mix is MIX:
        # Deterministic: the seeded programs give lookups and same-group writes of other threads
        # enough chances to meet (B4); how many actually meet depends on the interleaving (reported).
        exposure = group_exposure(programs)
        assert exposure["lookup_programs"] >= 100 and exposure["group_writers"] >= 50 \
            and exposure["pairs"] >= 1000, exposure
        ranges = range_exposure(programs, initial)
        assert ranges["range_programs"] >= RANGE_PROGRAMS_MIN and ranges["range_writers"] >= RANGE_WRITERS_MIN \
            and ranges["pairs"] >= RANGE_PAIRS_MIN and ranges["window_pairs"] >= RANGE_WINDOW_PAIRS_MIN, ranges
    else:
        exposure = ranges = None
        persisting = sum(op.kind in PERSIST_OPS for thread in programs for program in thread for op in program)
        assert persisting >= PERSIST_OPS_MIN, persisting
    reset_range_reads()

    calls_before = extractor_calls_under_root()
    workers = [_worker(db, t, programs[t], per_thread[t], stats) for t in range(THREADS)]
    if overlap is not None:
        workers = [overlap.member(work) for work in workers]
    elapsed = run_threads(workers)

    records = [r for rs in per_thread for r in rs]
    assert len(records) == THREADS * PER_THREAD
    read_only = sum(r.read_only for r in records)
    check_history(records, initial, final=_capture(db))
    by_unit = Counter(unit.kind.name if unit is not None else "none" for unit, _, _ in stats.conflicts)
    calls, held = (after - before for after, before in zip(extractor_calls_under_root(), calls_before))
    assert (calls > 0 or mix is not MIX) and held == 0, f"the index extractor ran {held} of {calls} times with the space root held"
    lookups = Counter(op.kind for r in records for op in r.program if op.kind in LOOKUP_OPS)
    report(label, elapsed,
           f"{stats.commits} commits, {stats.attempts} attempts, {len(stats.conflicts)} conflicts",
           f"{len(records) - read_only} published, {read_only} read-only",
           f"conflicts by unit {dict(sorted(by_unit.items()))}",
           f"by unit@root kind {conflicts_by_root_kind(records, initial, stats)}",
           f"composed {composed_counts(records, initial)}",
           f"group exposure {exposure}",
           f"lookups {dict(sorted(lookups.items()))}, index group conflicts {by_unit.get('INDEX_GROUP', 0)}, "
           f"index definition conflicts {by_unit.get('INDEX_DEFS', 0)}, extractor calls {calls}",
           f"range exposure {ranges}, range reads (ranged, not) {range_reads()}, "
           f"range conflicts {by_unit.get('RANGE', 0)}",
           class_change_counts(records, initial, stats),
           *([f"forced overlap {overlap}"] if overlap is not None else []))


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
