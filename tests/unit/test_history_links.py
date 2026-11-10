"""History links never pin loaded state (Task 8.3l, ruling R92, P1).

A root links the root it replaced (``DatabaseRoot.previous``, and in cluster mode
``SpaceRoot.cluster_previous``).  Before 8.3l the link held the loaded predecessor, which held
its own, so every root a process committed or loaded stayed in memory with everything loaded
through it: the decoded-atom cache's entry bound was not a memory bound (6,000 roots after
6,000 commits).  A link is now held as a pointer and loaded on demand through the atom's
context (the space's bounded cache, or a fresh stub): history lives on disk, never in memory.

Checks: the loaded roots alive stay within the cache bound however long the history is (and
none outlive their use with the cache disabled); the link does not hold the loaded
predecessor; the stored form is unchanged (an ``@ref`` to the predecessor); a reader holding
an old root and an "as of" walk of ``previous`` read correct content after the loaded forms
were evicted.
"""
import gc
import weakref

import pytest

from proto_db.core.atom import Atom
from proto_db.errors import ProtoValidationError
from proto_db.storage.file import FileStorage
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import DatabaseRoot, ObjectSpace, SpaceRoot
from tests.support.dict_storage import DictStorage
from tests.support.persistence import DirectContext


def _space(kind, tmp_path, cache):
    storage = MemoryStorage() if kind == "memory" else FileStorage(str(tmp_path / "s"), durability="none")
    space = ObjectSpace(storage, atom_cache_entries=cache)
    space.new_database("db")
    space.open_database("db").run(lambda tr: tr.new_mutable("m", 0))
    return space


def _commit(db, i):
    def body(tr):
        tr.set_root_object("n", i)
        tr.set_mutable("m", i * 10)
    db.run(body)


def loaded_atoms(storage) -> dict:
    """Loaded atoms of ``storage`` alive in the process, by class name (after a collection)."""
    gc.collect()
    counts = {}
    for o in gc.get_objects():
        if isinstance(o, Atom) and o.__dict__.get("_loaded") and \
                getattr(o.__dict__.get("context"), "storage", None) is storage:
            name = type(o).__name__
            counts[name] = counts.get(name, 0) + 1
    return counts


@pytest.mark.parametrize("kind", ["memory", "file"])
def test_loaded_roots_stay_within_the_cache_bound_however_long_the_history(kind, tmp_path):
    cache = 100
    space = _space(kind, tmp_path, cache)
    db = space.open_database("db")
    for i in range(300):
        _commit(db, i)
    early = loaded_atoms(space.storage)
    for i in range(300, 1500):
        _commit(db, i)
    late = loaded_atoms(space.storage)
    # Every loaded root alive is a cache entry or the current one: the history is on disk.
    assert late.get("DatabaseRoot", 0) <= cache, late
    assert late.get("SpaceRoot", 0) <= cache, late
    # Nothing loaded grows with the history: five times the commits, no more loaded atoms.
    assert sum(late.values()) <= sum(early.values()) + 10, (early, late)
    space.close()


def test_with_the_cache_disabled_no_loaded_root_outlives_its_use(tmp_path):
    space = _space("file", tmp_path, 0)
    db = space.open_database("db")
    for i in range(400):
        _commit(db, i)
    assert loaded_atoms(space.storage).get("DatabaseRoot", 0) == 0
    with db.new_transaction() as tr:
        assert tr.get_root_object("n") == 399
        # The transaction holds its snapshot root; its 400 predecessors are not loaded.
        assert loaded_atoms(space.storage).get("DatabaseRoot", 0) == 1
    space.close()


def test_the_link_does_not_hold_the_loaded_predecessor(tmp_path):
    space = _space("file", tmp_path, 0)
    db = space.open_database("db")
    for i in range(3):
        _commit(db, i)
    tr = db.new_transaction()
    root = tr._db_root
    previous = root.previous
    assert type(previous) is DatabaseRoot and previous.version == root.version - 1
    assert previous.objects.get_at("n") == 1
    gone = weakref.ref(previous)
    del previous
    gc.collect()
    assert gone() is None, "the root still holds its loaded predecessor"
    assert root.previous.version == root.version - 1       # loaded again, on demand
    while root.version > 0:
        root = root.previous
    assert root.previous is None                            # version 0 has no predecessor
    tr.abort()
    space.close()


def test_the_stored_form_is_unchanged(tmp_path):
    space = _space("file", tmp_path, 1000)
    db = space.open_database("db")
    _commit(db, 1)
    with db.new_transaction() as tr:
        root = tr._db_root
        previous_ptr = root.previous.atom_pointer
        state = space.storage.get_atom(root.atom_pointer)["s"]
    ref = state["previous"]["@ref"]
    assert ref[0] == previous_ptr.segment.bytes and ref[1] == previous_ptr.offset
    # ``origin`` (the database's identity, D56 fix round 1, R99 I2): 16 bytes, carried unchanged.
    assert set(state) == {"objects", "mutables", "previous", "version", "committed_at", "origin"}
    with db.new_transaction() as tr:
        root = tr._db_root
        assert type(root.origin) is bytes and len(root.origin) == 16 and root.previous.origin == root.origin
    space.close()


def test_a_new_root_built_in_this_process_links_its_base_by_pointer(tmp_path):
    space = _space("memory", tmp_path, 1000)
    db = space.open_database("db")
    _commit(db, 1)
    _commit(db, 2)
    with db.new_transaction() as tr:
        root = tr._db_root
        assert "previous" not in vars(root)
        assert root.previous.version == root.version - 1 and root.previous.objects.get_at("n") == 1
        space_root = tr._snapshot_space
        assert type(space_root) is SpaceRoot and space_root.cluster_previous is None   # not a cluster root
    space.close()


@pytest.mark.parametrize("cache", [0, 50])
def test_a_reader_holding_an_old_root_reads_it_after_eviction(cache, tmp_path):
    space = _space("file", tmp_path, cache)
    db = space.open_database("db")
    db.run(lambda tr: tr.set_root_object("d", tr.new_dictionary().__class__.from_items(
        ((k, k) for k in range(300)), context=tr)))
    _commit(db, 7)
    old = db.new_transaction()          # holds the root of version 3; reads nothing yet
    for i in range(8, 600):
        _commit(db, i)
    db.run(lambda tr: tr.set_root_object("d", tr.get_root_object("d").set_at(5, "changed")))
    assert old.get_root_object("n") == 7
    d = old.get_root_object("d")
    assert [d.get_at(k) for k in range(300)] == list(range(300))
    assert old.get_mutable("m") == 70
    old.abort()
    with db.new_transaction() as tr:
        assert tr.get_root_object("d").get_at(5) == "changed"
    space.close()


@pytest.mark.parametrize("cache", [0, 50])
def test_as_of_reads_walk_previous_after_eviction(cache, tmp_path):
    space = _space("file", tmp_path, cache)
    db = space.open_database("db")
    for i in range(1, 501):
        _commit(db, i)
    with db.new_transaction() as tr:
        root = tr._db_root
        seen = 0
        while root.version > 1:         # version 1 created the mutable cell
            assert root.objects.get_at("n") == root.version - 1
            assert root.mutables.get_at("m") == (root.version - 1) * 10
            root = root.previous
            seen += 1
        assert seen == 500
    history = db.history()
    assert [h.version for h in history] == list(range(501, -1, -1))
    assert [h.get_mutable("m") for h in history[:-1]] == [v * 10 for v in range(500, -1, -1)]
    space.close()


# -- the mechanism on a plain atom -------------------------------------------------------------

class _Version(Atom):
    """An atom with one history link, ``before``."""

    __type_name__ = "tests.history_links.Version"
    _history_links = ("before",)

    def __init__(self, n, before, *, context=None):
        super().__init__(context=context)
        self.n = n
        self._set_link("before", before)


def test_a_link_to_an_unsaved_atom_holds_it_until_it_is_saved():
    context = DirectContext(DictStorage())
    first = _Version(1, None, context=context)
    second = _Version(2, first, context=context)
    assert second.before is first                  # not saved: held as itself
    ptr = context.save(second)                      # saves first as a child
    assert first.atom_pointer is not None
    assert second.before == first                   # now loaded through the context, by pointer
    assert second.__dict__["_links"]["before"] == (first.atom_pointer, _Version)
    loaded = context.load(ptr, _Version.__type_name__)
    assert loaded.n == 2 and loaded.before.n == 1 and loaded.before.before is None
    assert "before" not in vars(loaded)


def test_a_link_set_without_a_context_holds_the_atom():
    context = DirectContext(DictStorage())
    first = _Version(1, None, context=context)
    context.save(first)
    second = _Version(2, first)                     # no context: it could not load again
    assert second.before is first
    context.save(second)                            # adopts the context
    assert second.before == first and second.before.n == 1


def test_assigning_a_link_after_construction_is_refused_at_save():
    # Review 8.3l M1: a plain assignment would shadow the link in memory and be lost on save.
    context = DirectContext(DictStorage())
    first = _Version(1, None, context=context)
    context.save(first)
    second = _Version(2, None, context=context)
    second.before = first
    with pytest.raises(ProtoValidationError, match="history link"):
        context.save(second)
