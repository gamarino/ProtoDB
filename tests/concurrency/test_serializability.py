"""Serializability checker over the closed set of operations (spec section 12 item 2).

Threads run randomly generated programs (fixed seed) on a small state: an object root, a
Dictionary root, a HashDictionary root, a key space shared by the three, and mutable cells.
Each thread starts at one barrier and retries a conflicting program in a fresh transaction
(``Database.run``, no backoff).  For each committed transaction the test records the database
version it published (``Transaction.committed_version``, its place in the commit order), its
program and every value it read; a transaction that published nothing (R23) records the
version it read.  :func:`tests.support.serial_model.check_history` then re-runs the programs in
commit order on the serial model: every value read and the final state must match.

The interleaving is nondeterministic; the assertion is not: whatever order the commits took,
the history must be serializable in that order (spec section 2).

Catches: a precondition missing for a read or a write (a lost update or a stale read that
commits), an intention reapplied wrongly (a key or cell value the serial run does not give),
a commit order that does not match the published versions, and a read-only transaction that
sees no consistent prefix.  Later tasks (6, 7, 9, 10) add their operations to the mix.
"""
import random

import pytest

from proto_db.collections.dictionaries import Dictionary
from proto_db.collections.hash_dictionaries import HashDictionary
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from tests.concurrency.harness import Attempts, report, run_threads
from tests.support.serial_model import (DICTIONARY_AND_CELL_OPS, OBJECT, Op, Record, StateNames, capture,
                                        check_history, execute, random_program)

SEED = 20261124
THREADS = 4
PER_THREAD = 200
#: No backoff: a conflict means another program committed, so the retries always progress.
RUN_KW = dict(retries=100_000, backoff=(0.0, 0.0))

NAMES = StateNames(
    roots={"obj": OBJECT, "dict": "Dictionary", "hdict": "HashDictionary"},
    keys=tuple(f"k{i}" for i in range(6)),
    cells=tuple(f"c{i}" for i in range(4)),
)

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


def _worker(db, thread, programs, records, attempts):
    def work():
        for program in programs:
            last = {}

            def attempt(tr):
                last["tr"] = tr
                return execute(tr, program, NAMES)

            reads = attempts.run(db, attempt, **RUN_KW)
            tr = last["tr"]
            version = tr.committed_version
            read_only = version is None
            if read_only:
                version = db.snapshot_at(tr.snapshot_ref).version
            records.append(Record(version, program, reads, read_only, thread))
    return work


def test_serializability_dictionary_and_cells(db):
    rng = random.Random(SEED)
    _seed_state(db, rng)
    initial = _capture(db)
    programs = [[random_program(rng, NAMES, DICTIONARY_AND_CELL_OPS) for _ in range(PER_THREAD)]
                for _ in range(THREADS)]
    per_thread = [[] for _ in range(THREADS)]
    attempts = Attempts()

    elapsed = run_threads([_worker(db, t, programs[t], per_thread[t], attempts) for t in range(THREADS)])

    records = [r for rs in per_thread for r in rs]
    assert len(records) == THREADS * PER_THREAD
    read_only = sum(r.read_only for r in records)
    check_history(records, initial, final=_capture(db))
    report("serializability dictionary+cells", elapsed, attempts,
           f"{len(records) - read_only} published, {read_only} read-only")


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
