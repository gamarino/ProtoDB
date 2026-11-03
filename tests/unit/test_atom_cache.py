"""Decoded atoms shared across transactions by pointer (ruling R75, D53).

A pointer's content never changes within a storage instance, so an :class:`ObjectSpace`
keeps decoded atoms in one bounded cache that every transaction and thread reads through;
it needs no invalidation.  Checks: a second transaction reuses the decoded atoms of the
first (no storage read), never sees stale content after other commits, children are shared
too, the cache is bounded and can be disabled, and the values read are identical with and
without it under concurrent writers.
"""
import threading

import pytest

from proto_db import MemoryStorage, ObjectSpace
from proto_db.errors import ProtoValidationError
from proto_db.storage.file import FileStorage


def _space(tmp_path=None, **kw):
    storage = MemoryStorage() if tmp_path is None else FileStorage(str(tmp_path / "s"), durability="none")
    space = ObjectSpace(storage, **kw)
    space.new_database("db")
    return space


def _fill(db, n=2000):
    db.run(lambda tr: tr.set_root_object("d", tr.new_dictionary().__class__.from_items(
        ((i, i * 3) for i in range(n)), context=tr)))


def _count_reads(storage):
    counter = []
    original = storage.get_atom

    def get_atom(ptr):
        counter.append(ptr)
        return original(ptr)
    storage.get_atom = get_atom
    return counter


def test_a_later_transaction_reuses_the_decoded_atoms(tmp_path):
    space = _space(tmp_path)
    db = space.open_database("db")
    _fill(db)
    with db.new_transaction() as tr:
        assert tr.get_root_object("d").get_at(1234) == 3702
    reads = _count_reads(space.storage)
    with db.new_transaction() as tr:
        d = tr.get_root_object("d")
        assert d.get_at(1234) == 3702
    assert reads == [], "the second transaction decoded again what the first had decoded"
    with db.new_transaction() as tr:
        assert tr.get_root_object("d").get_at(1235) == 3705      # a neighbour: mostly shared nodes
    stats = space.atom_cache_stats()
    assert stats["hits"] > 0 and stats["entries"] > 0 and len(reads) < 5
    space.close()


def test_content_is_never_stale_after_other_commits(tmp_path):
    space = _space(tmp_path)
    db = space.open_database("db")
    _fill(db, 200)
    for i in range(50):
        db.run(lambda tr, i=i: tr.set_root_object("d", tr.get_root_object("d").set_at(i, -i)))
        with db.new_transaction() as tr:
            d = tr.get_root_object("d")
            assert [d.get_at(j) for j in range(i + 2)] == [-j for j in range(i + 1)] + [3 * (i + 1)]
    space.close()


def test_the_cache_is_bounded_and_can_be_disabled():
    space = _space(atom_cache_entries=10)
    db = space.open_database("db")
    _fill(db)
    with db.new_transaction() as tr:
        assert sum(tr.get_root_object("d").get_at(i) for i in range(0, 2000, 7)) == sum(3 * i for i in range(0, 2000, 7))
    assert space.atom_cache_stats()["entries"] <= 10
    space.close()
    off = _space(atom_cache_entries=0)
    assert off.atom_cache_stats() is None
    db = off.open_database("db")
    _fill(db, 50)
    with db.new_transaction() as tr:
        assert tr.get_root_object("d").get_at(7) == 21
    off.close()
    for bad in (-1, 1.5, None):
        with pytest.raises(ProtoValidationError):
            ObjectSpace(MemoryStorage(), atom_cache_entries=bad)


@pytest.mark.parametrize("entries", [0, 64, 50_000])
def test_concurrent_readers_and_writers_read_the_same_values(entries):
    """Writers increment every key of their own slice; readers check that each snapshot is
    internally consistent (every key of a slice carries the same generation)."""
    space = _space(atom_cache_entries=entries)
    db = space.open_database("db")
    slices, width, rounds = 4, 25, 15
    db.run(lambda tr: tr.set_root_object("d", tr.new_dictionary().__class__.from_items(
        (((s, k), 0) for s in range(slices) for k in range(width)), context=tr)))
    errors = []
    stop = threading.Event()

    def writer(s):
        for _ in range(rounds):
            def bump(tr):
                d = tr.get_root_object("d")
                for k in range(width):
                    d = d.set_at((s, k), d.get_at((s, k)) + 1)
                tr.set_root_object("d", d)
            db.run(bump, retries=1000, backoff=(0.0001, 0.002))

    def reader():
        while not stop.is_set():
            with db.new_transaction() as tr:
                d = tr.get_root_object("d")
                for s in range(slices):
                    values = {d.get_at((s, k)) for k in range(width)}
                    if len(values) != 1:
                        errors.append((s, values))
    readers = [threading.Thread(target=reader) for _ in range(3)]
    writers = [threading.Thread(target=writer, args=(s,)) for s in range(slices)]
    for t in readers + writers:
        t.start()
    for t in writers:
        t.join()
    stop.set()
    for t in readers:
        t.join()
    assert errors == []
    with db.new_transaction() as tr:
        d = tr.get_root_object("d")
        assert all(d.get_at((s, k)) == rounds for s in range(slices) for k in range(width))
    space.close()
