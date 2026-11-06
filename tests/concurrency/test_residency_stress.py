"""Atoms unloaded and reloaded in place while other threads read them (Task 8.3m, review 8.3m I1).

Adapted from the reviewer's ``swap_sharp.py``: the values are non-immortal objects carrying a tag
(a use-after-free or a torn pointer read shows as a wrong tag or a crash, which small ints would
hide), the readers use literal attribute loads in a hot function (``LOAD_ATTR`` specializes), and
the atoms are real ones: stubs of a space whose decoded-atom cache holds 16 of them, unloaded by
eviction and by a dedicated unloader thread, reloaded in place by whoever reads them next.

Every read must return the stored value: never ``None`` (DBObject's answer for an absent
attribute), never ``AttributeError``, never another atom's value.  Multi-attribute reads that
mix two load generations are legal (the content of a pointer never changes, ruling R104) and are
counted, not refused.  ``_state`` / ``has_defined_attr`` / KeySpec extraction / index queries
run in the same storm.
"""
import random
import threading
import time

import pytest

from proto_db import MemoryStorage, ObjectSpace
from proto_db.indexing import EqualityIndex
from proto_db.indexing.keyspec import KeySpec
from proto_db.query.linq import F, from_collection

from .harness import run_threads

pytestmark = [pytest.mark.freethreaded, pytest.mark.timeout(120)]

N = 128
KEYS = ("a", "b", "c", "d", "e")
SECONDS = 3.0


def _tag(i, k):
    return f"{i}:{k}:" + "x" * 8      # a fresh str per atom and key: never immortal


def _space(entries):
    space = ObjectSpace(MemoryStorage(), atom_cache_entries=entries)
    db = space.new_database("db")

    def build(tr):
        d = tr.new_dictionary().add_index(EqualityIndex("by_g", "g"))
        for i in range(N):
            d = d.set_at(i, tr.new_object(i=i, g=i % 4, **{k: (_tag(i, k), i) for k in KEYS}))
        tr.set_root_object("d", d)
    db.run(build)
    with db.new_transaction() as tr:
        d = tr.get_root_object("d")
        objs = [d.get_at(i) for i in range(N)]
    space._atoms._unload_all()
    return space, db, objs


def _hot(o, i):
    a = o.a
    b = o.b
    c = o.c
    d = o.d
    e = o.e
    if a[0] != _tag(i, "a") or b[0] != _tag(i, "b") or c[0] != _tag(i, "c") or d[0] != _tag(i, "d") \
            or e[0] != _tag(i, "e"):
        raise AssertionError(f"atom {i} read {(a, b, c, d, e)!r}")


@pytest.mark.parametrize("entries", [16, 1])
def test_reads_stay_exact_while_atoms_are_unloaded_and_reloaded(entries):
    space, db, objs = _space(entries)
    stop = threading.Event()
    reads = [0] * 8
    unloads = [0]
    spec = KeySpec.path("c")

    def unloader(seed):
        def body():
            rng = random.Random(seed)
            n = 0
            while not stop.is_set():
                if rng.random() < 0.02:
                    space._atoms._unload_all()
                else:
                    objs[rng.randrange(N)]._unload()
                n += 1
            unloads[0] += n
        return body

    def reader(slot):
        def body():
            rng = random.Random(100 + slot)
            n = 0
            deadline = time.monotonic() + SECONDS
            while time.monotonic() < deadline:
                for i, o in enumerate(objs):
                    _hot(o, i)
                    n += 1
                i = rng.randrange(N)
                o = objs[i]
                state = o._state()
                assert set(state) == {"i", "g", *KEYS} and state["e"][0] == _tag(i, "e"), state
                assert o.has_defined_attr("d") and not o.has_defined_attr("zz")
                assert spec.extract(o)[0] == _tag(i, "c")
                assert o.zz is None
            reads[slot] = n
        return body

    def querier():
        deadline = time.monotonic() + SECONDS
        while time.monotonic() < deadline:
            with db.new_transaction() as tr:
                d = tr.get_root_object("d")
                got = sorted(r.i for r in from_collection(d).where(F.g == 1))
                assert got == [i for i in range(N) if i % 4 == 1], got

    def stopper():
        time.sleep(SECONDS)
        stop.set()

    run_threads([unloader(s) for s in range(3)] + [reader(s) for s in range(6)] + [querier, stopper])
    assert sum(reads) > N and unloads[0] > 0, (reads, unloads)
    assert space.atom_cache_stats()["entries"] <= entries
    space.close()


def test_writers_and_readers_under_continuous_eviction():
    """Commits build on unloaded atoms while readers read every version they see."""
    space = ObjectSpace(MemoryStorage(), atom_cache_entries=16)
    db = space.new_database("db")
    db.run(lambda tr: tr.set_root_object("d", tr.new_dictionary().add_index(EqualityIndex("by_w", "w"))))
    per_writer = 60

    def writer(w):
        def body():
            for j in range(per_writer):
                def fn(tr, j=j):
                    d = tr.get_root_object("d")
                    key = w * 1000 + j
                    tr.set_root_object("d", d.set_at(key, tr.new_object(w=w, j=j, tag=_tag(key, "t"))))
                db.run(fn, retries=1000)
        return body

    done = threading.Event()

    def reader():
        while not done.is_set():
            with db.new_transaction() as tr:
                d = tr.get_root_object("d")
                rows = list(d.items())
                for key, r in rows:
                    assert r.tag == _tag(key, "t") and r.w * 1000 + r.j == key
                for w in range(4):
                    got = sorted(r.j for r in from_collection(d).where(F.w == w))
                    assert got == sorted(k % 1000 for k, r in rows if r.w == w), (w, got)

    def evictor():
        while not done.is_set():
            space._atoms._unload_all()
            time.sleep(0.001)

    writers = [writer(w) for w in range(4)]

    def writers_then_stop():
        run_threads(writers)
        done.set()

    run_threads([writers_then_stop, reader, reader, evictor])
    with db.new_transaction() as tr:
        d = tr.get_root_object("d")
        assert d.count == 4 * per_writer
        assert all(r.tag == _tag(k, "t") for k, r in d.items())
    space.close()

