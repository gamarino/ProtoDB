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


# -- the cache receives the atoms a commit writes (Task 8.3i) ---------------------------------

def _state_of(atom):
    """The persisted state of a (loaded) atom, children by pointer."""
    from proto_db.core.atom import Atom
    atom._load()
    out = {}
    for name, value in atom._state().items():
        out[name] = ("@atom", value.atom_pointer) if isinstance(value, Atom) else value
    return out


def test_the_next_build_finds_the_atoms_the_previous_commit_wrote(tmp_path):
    """A commit's build reads nothing from storage when its base is the root the previous
    commit of this space wrote: every atom it touches went into the cache when it was saved."""
    space = _space(tmp_path)
    db = space.open_database("db")
    db.run(lambda tr: tr.set_root_object("ids", tr.new_counted_set().add(0)))
    for i in range(1, 30):
        db.run(lambda tr, i=i: tr.set_root_object("ids", tr.new_counted_set().add(i)))   # blind: merged
    reads = _count_reads(space.storage)
    for i in range(30, 60):
        db.run(lambda tr, i=i: tr.set_root_object("ids", tr.new_counted_set().add(i)))
    assert reads == [], f"{len(reads)} atoms fetched although the previous commits had just written them"
    with db.new_transaction() as tr:
        assert sorted(tr.get_root_object("ids").elements()) == list(range(60))
    space.close()


def test_a_cached_saved_atom_is_what_a_load_produces(tmp_path):
    """What the cache receives on save is never the live object: it is a new object, decoded
    already, with the runtime state of a loaded atom (an empty, incomplete op log), its children
    the cache's own atoms, and the same state, type for type, as a cache-less read of the
    storage decodes."""
    space = _space(tmp_path)
    db = space.open_database("db")
    tr = db.new_transaction()
    live = tr.new_counted_set().add(1).add(2)
    tr.set_root_object("ids", live)
    tr.commit()
    ptr = live.atom_pointer
    cached = space._atoms._cache.get(ptr)
    assert cached is not None and cached is not live and type(cached) is type(live)
    assert cached.__dict__.get("_loaded") is True, "the cached atom is decoded already"
    assert cached.context is space._atoms
    assert cached._op_log == () and cached.log_is_complete is False
    assert live._op_log != ()                          # the live version keeps its own log
    assert cached.root is not live.root and cached.root is space._atoms._cache.get(live.root.atom_pointer)
    reference = ObjectSpace(space.storage, atom_cache_entries=0)
    with reference.open_database("db").new_transaction() as rtr:
        stored = rtr.load(ptr, type(live).__type_name__)
        assert _state_of(cached) == _state_of(stored)
        assert {k: type(v) for k, v in cached._state().items()} == {k: type(v) for k, v in stored._state().items()}
        node, ref_node = cached.root, stored.root
        assert node.value == ref_node.value and type(node.value) is type(ref_node.value)
        assert cached.oid == stored.oid == live.oid
    space.close()


def test_cached_saved_atoms_are_never_mutated_afterwards(tmp_path):
    """Immutability (P1): once in the cache, a saved atom's persisted state and runtime state
    never change, whatever later transactions derive from it, commit or abort, and whatever the
    commit does to the live object after publishing (D33 resets its log flag)."""
    space = _space(tmp_path)
    db = space.open_database("db")
    db.run(lambda tr: tr.set_root_object("d", tr.new_dictionary().set_at("k", 1)))
    with db.new_transaction() as tr:
        root_ptr = tr.get_root_object("d").atom_pointer
    cached = space._atoms._cache.get(root_ptr)
    before_state = _state_of(cached)
    before_dict = {k: v for k, v in cached.__dict__.items() if k != "_okey"}
    for i in range(20):
        db.run(lambda tr, i=i: tr.set_root_object("d", tr.get_root_object("d").set_at(i, i)))
        db.run(lambda tr, i=i: tr.set_root_object("d", tr.new_dictionary().set_at(("b", i), i)))
        tr = db.new_transaction()
        tr.get_root_object("d").set_at("k", 99)
        tr.abort()
    assert _state_of(cached) == before_state
    assert {k: v for k, v in cached.__dict__.items() if k != "_okey"} == before_dict
    space.close()


@pytest.mark.parametrize("entries", [0, 50_000])
def test_a_committed_nested_collection_reused_blind_merges_as_if_loaded(entries):
    """D33 with the cache filled on save: a collection committed nested in another object and
    then reused for a blind write must behave as a loaded version (incomplete log: no replay of
    the earlier commit's ops).  The outcome must not depend on whether the cache is enabled."""
    space = _space(atom_cache_entries=entries)
    db = space.open_database("db")
    db.run(lambda tr: tr.set_root_object("ids", tr.new_counted_set().add(0)))
    db.run(lambda tr: tr.set_root_object("holder", tr.new_object(s=tr.new_counted_set().add(1))))

    def reuse(tr):
        nested = tr.get_root_object("holder").s
        tr.set_root_object("ids", nested.add(2))           # blind write of a derived version
    db.run(reuse)
    with db.new_transaction() as tr:
        ids = tr.get_root_object("ids")
        assert sorted(ids.items()) == [(1, 1), (2, 1)]
    space.close()


def test_large_value_boxes_are_not_cached_on_save():
    """A ValueBox holds one large value out of line (D51); a build keeps it by reference and
    never decodes it, so caching it on save would only retain its bytes (the cache is bounded by
    entries, not bytes).  The nodes that reference it are cached, with the box as the unloaded
    stub a decode of the node would make."""
    from proto_db.collections.avl import ValueBox
    space = _space()
    db = space.open_database("db")
    db.run(lambda tr: tr.set_root_object("blobs", tr.new_dictionary().set_at(1, b"v" * 100_000)))
    cached = [entry[0] for entry in space._atoms._cache._items.values()]
    boxes = [atom for atom in cached if isinstance(atom, ValueBox)]
    assert cached and all(box.__dict__.get("_loaded") is False and "value" not in box.__dict__
                          for box in boxes), "a box is cached at most as an unloaded stub, as a decode makes it"
    with db.new_transaction() as tr:
        assert tr.get_root_object("blobs").get_at(1) == b"v" * 100_000
    space.close()
