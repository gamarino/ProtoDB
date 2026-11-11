"""Residency of the decoded-atom cache (Task 8.3m, rulings R102-R104, D53 amendment).

The space's cache bounds the number of LOADED shared atoms; an atom it evicts goes back to the
stub state (pointer, class, context, oid) and is loaded again, through the existing lazy-load
path, by whoever reads it next.  Checks, most of them deterministic interleavings:

- the bound: loaded shared atoms never exceed the capacity, and no loaded shared atom outside the
  cache is reachable from it (fails before 8.3m: every atom a commit writes stays loaded);
- every collection, index and root reads correctly after every atom was unloaded, and under
  continuous eviction (capacity 1 and 16);
- one object per pointer while it is held (identity map), and the type-mismatch guard (M2);
- only cache-produced atoms are ever unloaded: a saved live collection keeps its op log (I4);
- a load whose decode raced an unload never publishes into the newer stub (C1);
- every check-then-read site survives an unload between its check and its read (condition 2);
- a stub read after its space is closed raises ProtoClosedError (I3, R104).

Planted mutations (strict xfail) show each guard is what makes its test pass.
"""
import gc
import os
import subprocess
import sys
import threading

import pytest

from proto_db import MemoryStorage, ObjectSpace
from proto_db.collections import avl
from proto_db.core import atom as atom_mod
from proto_db.core import codec
from proto_db.core.atom import Atom
from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoClosedError, ProtoValidationError
from proto_db.indexing import EqualityIndex, RangeIndex
from proto_db.indexing.keyspec import KeySpec
from proto_db.query.linq import F, from_collection
from proto_db.storage.file import FileStorage


_REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _space(entries, tmp_path=None):
    storage = MemoryStorage() if tmp_path is None else FileStorage(str(tmp_path / "s"), durability="none")
    return ObjectSpace(storage, atom_cache_entries=entries)


def _children(atom):
    """Atoms in the persisted state of ``atom`` (attributes, nested tuples / lists / dicts)."""
    out = []

    def walk(v):
        if isinstance(v, Atom):
            out.append(v)
        elif isinstance(v, (tuple, list)):
            for x in v:
                walk(x)
        elif isinstance(v, dict):
            for x in v.values():
                walk(x)
    for k, v in list(atom.__dict__.items()):
        if not k.startswith("_") and k not in atom_mod.RESERVED_NAMES:
            walk(v)
    return out


def _build_inventory(db, commits, per_commit):
    def init(tr):
        tr.set_root_object("objs", tr.new_dictionary().add_index(EqualityIndex("by_g", "g"))
                           .add_index(RangeIndex("by_v", "v")))
    db.run(init)
    for c in range(commits):
        def fn(tr, c=c):
            d = tr.get_root_object("objs")
            for i in range(c * per_commit, (c + 1) * per_commit):
                d = d.set_at(i, tr.new_object(id=i, g=i % 17, v=(i * 7919) % 1000, name=f"n{i}"))
            tr.set_root_object("objs", d)
        db.run(fn)


# -- the bound -------------------------------------------------------------------------------------

@pytest.mark.parametrize("capacity", [200, 2000])
def test_loaded_shared_atoms_stay_within_the_capacity(capacity):
    space = _space(capacity)
    db = space.new_database("db")
    _build_inventory(db, commits=12, per_commit=1000)
    with db.new_transaction() as tr:            # a full read: before 8.3m it left every node loaded
        assert sum(1 for _, r in tr.get_root_object("objs").items() if r.name) == 12_000
    gc.collect()
    # Atoms of this space only (whatever their context: the shared cache or a transaction), so
    # atoms another test left loaded in the process do not count (full-suite 3e07e0b).
    loaded_saved = [o for o in gc.get_objects() if isinstance(o, Atom) and o.__dict__.get("_loaded")
                    and o.__dict__.get("atom_pointer") is not None
                    and getattr(o.__dict__.get("context"), "storage", None) is space.storage]
    # Before 8.3m (26f3b85): 24,000+ loaded saved atoms here whatever the capacity.
    assert len(loaded_saved) <= capacity + 1, f"{len(loaded_saved)} loaded saved atoms, capacity {capacity}"
    resident = {id(a) for a, _ in list(space._atoms._cache._items.values())}
    # Reachability: from the cache entries, through loaded atoms only, nothing loaded is outside it.
    seen, frontier, escaped = set(resident), [a for a, _ in list(space._atoms._cache._items.values())], []
    while frontier:
        nxt = []
        for a in frontier:
            for c in _children(a):
                if c.__dict__.get("_loaded") and id(c) not in seen:
                    seen.add(id(c))
                    nxt.append(c)
                    if c.__dict__.get("context") is space._atoms:
                        escaped.append(c)
        frontier = nxt
    assert not escaped, f"{len(escaped)} loaded shared atoms reachable from the cache but not resident"
    with db.new_transaction() as tr:
        d = tr.get_root_object("objs")
        assert d.count == 12_000 and d.get_at(4321).name == "n4321"
        assert sorted(x.id for x in from_collection(d).where(F.g == 5)) == [i for i in range(12_000) if i % 17 == 5]
    space.close()


def test_stats_report_residency():
    space = _space(50)
    db = space.new_database("db")
    _build_inventory(db, commits=2, per_commit=200)
    stats = space.atom_cache_stats()
    assert set(stats) >= {"hits", "misses", "entries", "capacity", "loads", "saved", "unloads",
                          "read_unloads", "identity_map"}
    assert stats["entries"] <= 50 and stats["unloads"] > 0 and stats["saved"] > 0
    space.close()


def test_unloads_per_load_tell_a_fitting_working_set_from_thrashing():
    """The thrash signal of D53 "Sizing" (rulings R105, R114 N4): over an interval of reads,
    ``read_unloads`` per ``loads`` is about 0 when the working set fits and about 1 when it does not."""
    def ratio(capacity):
        space = _space(capacity)
        db = space.new_database("db")
        _build_inventory(db, commits=2, per_commit=200)
        space._atoms._unload_all()
        with db.new_transaction() as tr:
            d = tr.get_root_object("objs")
            for i in range(400):
                assert d.get_at(i).id == i      # warm-up: loads the path and the record
            before = space.atom_cache_stats()
            for _ in range(3):
                for i in range(400):
                    assert d.get_at(i).id == i
            after = space.atom_cache_stats()
        loads = after["loads"] - before["loads"]
        unloads = after["read_unloads"] - before["read_unloads"]
        space.close()
        return loads, unloads
    loads, unloads = ratio(100_000)
    assert loads == 0 and unloads == 0                  # fits: nothing loads again
    loads, unloads = ratio(50)
    assert loads > 100 and unloads / loads > 0.9, (loads, unloads)


def test_the_read_signal_ignores_the_unloads_commits_cause():
    """Re-review 8.3m N4: a saved atom's decoded twin is admitted without a load and evicts a
    resident atom, so over an interval with commits ``unloads`` per ``loads`` is far above 1.
    ``read_unloads`` counts only what read-path loads evicted: at most one per load."""
    space = _space(50)
    db = space.new_database("db")
    _build_inventory(db, commits=4, per_commit=200)
    stats = space.atom_cache_stats()
    space.close()
    assert stats["unloads"] > 1.5 * max(1, stats["loads"]), stats     # the skew of the total (2.4x)
    assert stats["read_unloads"] <= stats["loads"], stats              # the read signal: <= 1


# -- every structure after unloading -----------------------------------------------------------------

def _all_structures(tr):
    rows = [tr.new_object(i=i, g=i % 5, v=(i * 37) % 101, s=f"s{i}") for i in range(300)]
    lst = tr.new_list()
    for r in rows:
        lst = lst.append_last(r)
    tr.set_root_object("list", lst.add_index(EqualityIndex("by_g", "g")))
    d = tr.new_dictionary().add_index(EqualityIndex("by_g", "g")).add_index(RangeIndex("by_v", "v"))
    for r in rows:
        d = d.set_at(r.i, r)
    tr.set_root_object("dict", d)
    h = tr.new_hash_dictionary()
    for r in rows:
        h = h.set_at(f"k{r.i}", r)
    tr.set_root_object("hdict", h)
    s = tr.new_set()
    for i in range(300):
        s = s.add(i * 3)
    tr.set_root_object("set", s)
    c = tr.new_counted_set()
    for i in range(300):
        c = c.add(i % 40)
    tr.set_root_object("cset", c)
    rk = tr.new_repeated_keys_dictionary()
    for r in rows:
        rk = rk.set_at(r.g, r)
    tr.set_root_object("rkd", rk)
    tr.set_root_object("holder", tr.new_object(child=rows[7], many=(rows[1], rows[2]), m={"x": rows[3]}))


def _check_all_structures(tr):
    lst = tr.get_root_object("list")
    assert [r.i for r in lst.elements()] == list(range(300))
    assert sorted(r.i for r in from_collection(lst).where(F.g == 2)) == [i for i in range(300) if i % 5 == 2]
    d = tr.get_root_object("dict")
    assert d.count == 300 and d.get_at(123).s == "s123"
    assert sorted(r.i for r in from_collection(d).where(F.g == 3)) == [i for i in range(300) if i % 5 == 3]
    assert sorted(r.i for r in from_collection(d).where(F.v < 10)) == sorted(i for i in range(300) if (i * 37) % 101 < 10)
    by_v = [r.v for r in from_collection(d).order_by(F.v)]
    assert by_v == sorted((i * 37) % 101 for i in range(300))
    h = tr.get_root_object("hdict")
    assert h.get_at("k299").i == 299 and sum(1 for _ in h.items()) == 300
    s = tr.get_root_object("set")
    assert sorted(s.elements()) == [i * 3 for i in range(300)] and 297 in s
    c = tr.get_root_object("cset")
    assert sorted(c.items()) == sorted((k, sum(1 for i in range(300) if i % 40 == k)) for k in range(40))
    rk = tr.get_root_object("rkd")
    assert sorted(r.i for r in rk.get_at(4).elements()) == [i for i in range(300) if i % 5 == 4]
    holder = tr.get_root_object("holder")
    assert holder.child.i == 7 and [r.i for r in holder.many] == [1, 2] and holder.m["x"].i == 3


@pytest.mark.parametrize("capacity", [1, 16, 100_000])
def test_every_structure_reads_correctly_after_every_atom_is_unloaded(capacity):
    space = _space(capacity)
    db = space.new_database("db")
    db.run(_all_structures)
    for _ in range(3):
        space._atoms._unload_all()
        with db.new_transaction() as tr:
            _check_all_structures(tr)
    # Derived versions built over unloaded atoms are correct too (commit path).
    db.run(lambda tr: tr.set_root_object("dict", tr.get_root_object("dict").set_at(1000, tr.new_object(i=1000, g=0, v=5, s="x"))))
    space._atoms._unload_all()
    with db.new_transaction() as tr:
        d = tr.get_root_object("dict")
        assert d.count == 301 and sorted(r.i for r in from_collection(d).where(F.v == 5))[-1] == 1000
    space.close()


def test_history_and_roots_read_correctly_after_unloading():
    space = _space(4)
    db = space.new_database("db")
    for i in range(10):
        db.run(lambda tr, i=i: tr.set_root_object("n", i))
    space._atoms._unload_all()
    entries = db.history()
    assert [e.version for e in entries][:3] == [10, 9, 8]
    root = db.new_transaction()._db_root
    seen = []
    while root is not None:
        space._atoms._unload_all()
        seen.append(root.version)
        root = root.previous
    assert seen == list(range(10, -1, -1))
    space.close()


# -- identity ---------------------------------------------------------------------------------------

def test_one_object_per_pointer_while_held_and_oid_without_loading():
    space = _space(10)
    db = space.new_database("db")
    db.run(lambda tr: tr.set_root_object("o", tr.new_object(a=1)))
    with db.new_transaction() as tr:
        o = tr.get_root_object("o")
        oid, ptr = o.oid, o.atom_pointer
    space._atoms._unload_all()
    assert o.__dict__["_loaded"] is atom_mod._UNLOADED and "a" not in o.__dict__
    assert space._atoms.load(ptr, DBObject.__type_name__) is o
    assert o.oid == oid and hash(o) == hash(oid) and not o.__dict__["_loaded"], "oid and hash never load"
    assert o.a == 1 and o.__dict__["_loaded"]
    space.close()


def test_a_type_mismatched_load_never_displaces_the_shared_atom():
    """Review M2: load(ptr, other type) returns a one-off stub; it is never admitted, so its load
    cannot displace (and unload) the shared atom of that pointer."""
    from proto_db.collections.dictionaries import Dictionary
    space = _space(10)
    db = space.new_database("db")
    db.run(lambda tr: tr.set_root_object("o", tr.new_object(a=1)))
    with db.new_transaction() as tr:
        o = tr.get_root_object("o")
        assert o.a == 1
    odd = space._atoms.load(o.atom_pointer, Dictionary.__type_name__)
    assert odd is not o and odd.__dict__["_loaded"] is False, "a one-off stub carries no mark"
    with pytest.raises(Exception):
        odd.count                       # a corrupt type: the load refuses it
    assert o.__dict__["_loaded"] and o.a == 1
    space.close()


def test_key_remove_of_an_absent_key_returns_the_same_root_after_eviction():
    """Review M3: avl.key_remove returns ``root`` itself when the key is absent; under eviction this
    holds because the identity map returns the child object still held."""
    space = _space(8)
    db = space.new_database("db")
    db.run(lambda tr: tr.set_root_object("d", tr.new_dictionary().__class__.from_items(
        ((i, i) for i in range(500)), context=tr)))
    with db.new_transaction() as tr:
        d = tr.get_root_object("d")
        for k in (-1, 10_000, 250.5):
            space._atoms._unload_all()
            root = d.root
            new_root = avl.key_remove(root, k)[0]
            assert new_root is root, f"a no-op remove of {k!r} path-copied the tree"
    space.close()


# -- instance layouts, checked in a fresh interpreter ------------------------------------------------
# Whether CPython keeps an instance's values inline depends on its class's shared key table, which
# belongs to the process: every instance of the class that any earlier test created or loaded has
# shaped it.  Run after the cluster unit tests, for instance, the space roots of this module's own
# space lose their inline values (the power-loss tests of test_cluster_acceptor_durability.py, which
# reload cluster roots, are enough), with or without this cache.  The layout checks therefore run in
# a fresh interpreter, in the same GIL mode, where only their own objects have shaped the tables.

def _in_a_fresh_interpreter(check: str) -> None:
    """Run the module-level function ``check`` of this module in a new interpreter."""
    flags = ["-X", f"gil={int(sys._is_gil_enabled())}"] if hasattr(sys, "_is_gil_enabled") else []
    code = f"from tests.unit.test_atom_residency import {check}; {check}()"
    proc = subprocess.run([sys.executable, *flags, "-c", code], cwd=_REPO, capture_output=True, text=True,
                          timeout=240, env=dict(os.environ, PYTHONPATH=_REPO))
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_cache_atoms_keep_the_inline_values_layout():
    """The mark lives in the value of ``_loaded``, never in an extra key: a key added after the
    class's usual ones materializes the instance dict, and every attribute read of that atom then
    takes the slower dict path (measured: query paths 15-35 % slower with a ``_cached`` key)."""
    pytest.importorskip("_testinternalcapi")
    _in_a_fresh_interpreter("_check_inline_values_layout")


def _check_inline_values_layout():
    import _testinternalcapi as internal
    space = _space(10**6)
    db = space.new_database("db")
    _build_inventory(db, commits=2, per_commit=500)

    def layouts():
        # DBObject is left out: records of every attribute set share one key table per process
        # (other tests' records fill it), with or without this cache.  Tree nodes, collections,
        # indexes and roots have fixed attribute sets.
        gc.collect()
        return {(type(o).__name__, internal.has_inline_values(o)) for o in gc.get_objects()
                if isinstance(o, Atom) and type(o) is not DBObject
                and o.__dict__.get("context") is space._atoms and o.__dict__.get("_loaded")}
    twins = layouts()
    assert twins and all(inline for _, inline in twins), sorted(twins)
    space._atoms._unload_all()
    with db.new_transaction() as tr:
        d = tr.get_root_object("objs")
        assert sum(r.v for _, r in d.items()) == sum((i * 7919) % 1000 for i in range(1000))
        assert len(list(from_collection(d).where(F.g == 3))) == len([i for i in range(1000) if i % 17 == 3])
    reloaded = layouts()
    assert ("CountedKeyNode", True) in reloaded and all(inline for _, inline in reloaded), sorted(reloaded)
    space.close()


def _materialized(atom) -> bool:
    """Whether ``atom``'s instance dict exists as an object (``gc`` lists it as a referent)."""
    return any(type(r) is dict and "_loaded" in r for r in gc.get_referents(atom))


class _Row(DBObject):
    """Records of this module only: DBObject's key table is shared by every test's records (other
    attribute sets fill it, and CPython then materializes their dicts itself)."""

    __type_name__ = "tests.unit.test_atom_residency.Row"


def test_the_residency_paths_never_materialize_the_instance_dict():
    """Once an object's ``__dict__`` exists, CPython 3.14 never specializes attribute reads on it
    (``LOAD_ATTR_INSTANCE_VALUE``; measured 43 against 9 ns per read).  Saving (the twin),
    admission, a lookup by pointer, eviction (unload), a reload, ``has_defined_attr``, KeySpec
    extraction and ``_state`` must therefore read attributes, never the dict.  The tree nodes'
    key tables belong to the process, so the check runs in a fresh interpreter (see above)."""
    _in_a_fresh_interpreter("_check_residency_paths_never_materialize")


def _check_residency_paths_never_materialize():
    space = _space(10**6)
    db = space.new_database("db")

    def build(tr):
        d = tr.new_dictionary().add_index(EqualityIndex("by_g", "g"))
        for i in range(200):
            d = d.set_at(i, _Row(id=i, g=i % 17, v=(i * 7919) % 1000, name=f"n{i}"))
        tr.set_root_object("objs", d)
    db.run(build)
    gc.collect()

    def cache_atoms():
        return [o for o in gc.get_objects() if type(o) in (_Row, avl.KeyNode, avl.CountedKeyNode)
                and _get_context(o) is space._atoms]
    twins = cache_atoms()
    assert len(twins) > 200 and not [o for o in twins if _materialized(o)], "saving or admission materialized a dict"
    with db.new_transaction() as tr:
        d = tr.get_root_object("objs")
        assert d.get_at(42).name == "n42"
    space._atoms._unload_all()
    assert not [o for o in cache_atoms() if _materialized(o)], "an unload materialized a dict"
    with db.new_transaction() as tr:
        d = tr.get_root_object("objs")
        rows = [r for _, r in d.items()]
        assert sum(r.v for r in rows) == sum((i * 7919) % 1000 for i in range(200))
        assert all(r.has_defined_attr("g") and not r.has_defined_attr("zz") for r in rows)
        assert [KeySpec.path("id").extract(r) for r in rows[:5]] == [0, 1, 2, 3, 4]
        assert rows[0]._state() == {"id": 0, "g": 0, "v": 0, "name": "n0"}
        assert len(list(from_collection(d).where(F.g == 3))) == len([i for i in range(200) if i % 17 == 3])
    reloaded = [o for o in cache_atoms() if object.__getattribute__(o, "_loaded")]
    assert len(reloaded) > 200 and not [o for o in reloaded if _materialized(o)], \
        "a reload or a reader materialized a dict"
    space.close()


def _get_context(atom):
    try:
        return object.__getattribute__(atom, "context")
    except AttributeError:
        return None


# -- eligibility (I4) -------------------------------------------------------------------------------

def _eligibility_check(planted):
    space = _space(4)
    db = space.new_database("db")
    db.run(lambda tr: tr.set_root_object("ids", tr.new_counted_set().add(0)))
    tr = db.new_transaction()
    live = tr.get_root_object("ids").add(1).add(2)
    tr.set_root_object("ids", live)
    tr.commit()
    assert live.atom_pointer is not None and live._op_log, "a saved live collection with an op log"
    space._atoms._admit(live)                       # what a stray admission would do
    space._atoms._unload_all()
    for i in range(50):                             # and eviction pressure
        db.run(lambda tr, i=i: tr.set_root_object(f"r{i}", i))
    assert live.__dict__.get("_loaded") and live._op_log, "the live collection lost its loaded state"
    # The next transaction rebases over it as D33 expects.
    db.run(lambda tr: tr.set_root_object("ids", live.add(3)))
    with db.new_transaction() as tr:
        assert sorted(tr.get_root_object("ids").items()) == [(0, 1), (1, 1), (2, 1), (3, 1)]
    space.close()


def test_only_cache_produced_atoms_are_ever_unloaded():
    _eligibility_check(planted=False)


@pytest.mark.xfail(strict=True, reason="planted: eligibility by 'has a pointer' unloads a saved live collection")
def test_planted_pointer_only_eligibility_loses_the_op_log(monkeypatch):
    monkeypatch.setattr(atom_mod, "_is_cache_produced",
                        lambda a: bool(a.__dict__.get("_loaded")) and a.__dict__.get("atom_pointer") is not None)
    _eligibility_check(planted=True)


# -- C1: a load racing an unload ----------------------------------------------------------------

def _race_republish(monkeypatch):
    """Thread B decodes S while thread A loads S, the cache admits S and evicts (unloads) it; B's
    publish must not land in the new stub generation (review 8.3m C1)."""
    space = _space(1000)
    db = space.new_database("d")
    db.run(lambda tr: (tr.set_root_object("o", tr.new_object(a=1, b=2)), tr.set_root_object("p", tr.new_object(a=3))))
    with db.new_transaction() as t:
        ptr, ptr2 = t.get_root_object("o").atom_pointer, t.get_root_object("p").atom_pointer
    cache = space._atoms
    cache._unload_all()
    gc.collect()
    stub = cache.load(ptr, DBObject.__type_name__)
    assert not stub.__dict__["_loaded"]
    orig = codec.decode_state
    b_in_decode, b_go = threading.Event(), threading.Event()

    def decode_state(data, load):
        if threading.current_thread().name == "B":
            b_in_decode.set()
            b_go.wait(10)
        return orig(data, load)
    monkeypatch.setattr(codec, "decode_state", decode_state)
    result = {}

    def reader_b():
        try:
            result["b"] = (stub.a, stub.b, getattr(stub, "a", "DEFAULT"))
        except BaseException as e:  # noqa: BLE001
            result["b_error"] = repr(e)
    tb = threading.Thread(target=reader_b, name="B")
    tb.start()
    assert b_in_decode.wait(10)
    assert stub.a == 1                              # A loads, publishes, is admitted
    cache._cache._max = 1
    other = cache.load(ptr2, DBObject.__type_name__)
    assert other.a == 3                             # admission evicts and unloads the stub
    assert not stub.__dict__["_loaded"]
    b_go.set()
    tb.join(10)
    resident = any(a is stub for a, _ in list(cache._cache._items.values()))
    return result, stub, resident, space


def test_a_load_racing_an_unload_never_publishes_into_the_newer_stub(monkeypatch):
    result, stub, resident, space = _race_republish(monkeypatch)
    assert result.get("b") == (1, 2, 1), result
    assert set(stub.__dict__) <= set(atom_mod._STUB_KEYS) | {"a", "b"}, sorted(stub.__dict__)
    assert not stub.__dict__["_loaded"] or resident, "a loaded shared atom escaped the cache bound"
    space.close()


@pytest.mark.xfail(strict=True, reason="planted: the reviewed design -- a publish into a newer generation, not admitted")
def test_planted_unadmitted_publish_escapes_the_bound(monkeypatch):
    """Since the payload travels in ``_loaded`` (no ``_payload`` key), a publish into a newer
    generation carries equal content (P1) and is admitted like any other, so removing the
    generation check alone is not observable.  The guard that matters is that every publish is
    admitted: planting the reviewed design's failure -- B's publish lands in the newer stub and
    is not admitted -- leaves a loaded shared atom outside the bound."""
    from proto_db.transactions import _AtomCache
    monkeypatch.setattr(atom_mod, "_same_generation", lambda d, pending: True)
    admit = _AtomCache._atom_loaded
    monkeypatch.setattr(_AtomCache, "_atom_loaded",
                        lambda self, atom: None if threading.current_thread().name == "B" else admit(self, atom))
    result, stub, resident, space = _race_republish(monkeypatch)
    assert result.get("b") == (1, 2, 1), result
    assert not stub.__dict__["_loaded"] or resident, "a loaded shared atom escaped the cache bound"


# -- check-then-read sites (condition 2) ---------------------------------------------------------

class _Trap(atom_mod._Generation):
    """A load-generation token whose first truth test unloads its atom: an unload lands exactly
    between a reader's check of ``_loaded`` and its read of the attribute."""

    __slots__ = ("atom", "fired")

    def __init__(self, atom, names=()):
        super().__init__(names)
        self.atom, self.fired = atom, False

    def __bool__(self):
        if not self.fired:
            self.fired = True
            self.atom._unload()
        return True


def _trapped(atom):
    atom._load()
    trap = _Trap(atom, atom.__dict__["_loaded"].names)
    object.__setattr__(atom, "_loaded", trap)
    return trap


def _loaded_object(space, db, **attrs):
    db.run(lambda tr: tr.set_root_object("o", tr.new_object(**attrs)))
    with db.new_transaction() as tr:
        o = tr.get_root_object("o")
    space._atoms._unload_all()
    return o


def test_has_defined_attr_survives_an_unload_between_check_and_read():
    space = _space(10)
    db = space.new_database("db")
    o = _loaded_object(space, db, a=1, b=None)
    trap = _trapped(o)
    assert o.has_defined_attr("a") is True and trap.fired
    trap = _trapped(o)
    assert o.has_defined_attr("b") is True and trap.fired
    assert o.has_defined_attr("zzz") is False
    space.close()


@pytest.mark.xfail(strict=True, reason="planted: a probe without the generation re-check")
def test_planted_probe_without_recheck_reports_a_defined_attribute_absent(monkeypatch):
    def probe(self, name):
        d = self.__dict__
        if not d.get("_loaded", True):
            self._load()
        return d.get(name, atom_mod._ABSENT)
    monkeypatch.setattr(Atom, "_probe", probe)
    space = _space(10)
    db = space.new_database("db")
    o = _loaded_object(space, db, a=1)
    _trapped(o)
    assert o.has_defined_attr("a") is True


def _trap_read(atom, name):
    """Read ``name`` with an unload landing between the reader's generation check and its read:
    the attribute is removed from the loaded state first, so the read goes to __getattr__."""
    _trapped(atom)
    if name in atom.__dict__:
        object.__delattr__(atom, name)
    return getattr(atom, name)


def test_getattr_never_reports_an_existing_attribute_absent_across_an_unload():
    """DBObject returns None for an absent attribute: across an unload a defined one must never
    read as absent, nor raise AttributeError."""
    space = _space(10)
    db = space.new_database("db")
    o = _loaded_object(space, db, a=1, b=2)
    assert _trap_read(o, "a") == 1 and _trap_read(o, "b") == 2
    assert _trap_read(o, "nope") is None
    space.close()


@pytest.mark.xfail(strict=True, reason="planted: __getattr__ without the generation re-check")
def test_planted_getattr_without_recheck_reads_none(monkeypatch):
    def getattr_(self, name):                    # the product's __getattr__ minus the re-check
        if name.startswith("_"):
            raise AttributeError(name)
        d = object.__getattribute__(self, "__dict__")
        while True:
            token = d.get("_loaded")
            if token is None:
                raise AttributeError(name)
            if not token:
                snapshot = self._load()
                if snapshot is not None:
                    return self._attribute_in(snapshot, name)
                continue
            value = d.get(name, atom_mod._ABSENT)
            if value is not atom_mod._ABSENT:
                return value
            return self._missing_attribute(d.get("_links"), name)
    monkeypatch.setattr(Atom, "__getattr__", getattr_)
    space = _space(10)
    db = space.new_database("db")
    o = _loaded_object(space, db, a=1)
    assert _trap_read(o, "a") == 1


def test_unload_after_load_returns_and_before_the_read(monkeypatch):
    space = _space(10)
    db = space.new_database("db")
    o = _loaded_object(space, db, a=1, b=(1, 2))
    orig = Atom._load
    fired = []

    def load_then_unload(self):
        out = orig(self)
        if not fired and self is o:
            fired.append(1)
            self._unload()
            return None
        return out
    monkeypatch.setattr(Atom, "_load", load_then_unload)
    assert o.a == 1 and fired
    fired.clear()
    space._atoms._unload_all()
    assert o._state() == {"a": 1, "b": (1, 2)} and fired
    fired.clear()
    space._atoms._unload_all()
    assert o.has_defined_attr("b") and fired
    space.close()


def _pause_unload_midway(monkeypatch):
    """Patch _drop_state so an unload stops after deleting half the keys, holding the stripe."""
    paused, go = threading.Event(), threading.Event()
    orig = atom_mod._drop_state

    def drop(atom, names):
        half = [k for k in names if k not in atom_mod._STUB_KEYS]
        for k in half[: len(half) // 2]:
            object.__delattr__(atom, k)
        paused.set()
        go.wait(10)
        orig(atom, names)
    monkeypatch.setattr(atom_mod, "_drop_state", drop)
    return paused, go


def _state_during_unload(monkeypatch):
    space = _space(10)
    db = space.new_database("db")
    o = _loaded_object(space, db, a=1, b=2, c=3, d=4, e=5, f=6)
    o._load()
    paused, go = _pause_unload_midway(monkeypatch)
    entered = threading.Event()
    orig_load = Atom._load

    def load(self):
        if self is o:
            entered.set()
        return orig_load(self)
    monkeypatch.setattr(Atom, "_load", load)
    t = threading.Thread(target=o._unload)
    t.start()
    assert paused.wait(10)
    out = {}
    r = threading.Thread(target=lambda: out.update(state=o._state(), probe=o.has_defined_attr("f")))
    r.start()
    entered.wait(2)                         # the reader saw the stub and waits on the stripe
    go.set()
    t.join(10)
    r.join(10)
    space.close()
    return out


def test_state_copy_during_an_unload_is_complete(monkeypatch):
    out = _state_during_unload(monkeypatch)
    assert out["state"] == {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": 6} and out["probe"] is True


@pytest.mark.xfail(strict=True, reason="planted: a state copy that does not check the load generation")
def test_planted_unchecked_state_copy_is_torn(monkeypatch):
    monkeypatch.setattr(Atom, "_snapshot", lambda self: dict(self.__dict__))
    out = _state_during_unload(monkeypatch)
    assert out["state"] == {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": 6}


def test_root_fields_survive_an_unload_between_check_and_read():
    """DatabaseRoot.previous / origin and the SpaceRoot fields default to None when absent (review
    4.2), so a torn read through the cache would be silent."""
    space = _space(10)
    db = space.new_database("db")
    db.run(lambda tr: tr.set_root_object("n", 1))
    db.run(lambda tr: tr.set_root_object("n", 2))
    with db.new_transaction() as tr:
        root = tr._db_root
        space_root = tr._snapshot_space
    want = {"version": root.version, "origin": root.origin, "committed_at": root.committed_at,
            "prev": root.previous.version}
    want_space = {"databases": space_root.databases.atom_pointer, "cluster_epoch": space_root.cluster_epoch}
    for _ in range(2):
        space._atoms._unload_all()
        assert _trap_read(root, "version") == want["version"]
        assert _trap_read(root, "origin") == want["origin"]
        assert _trap_read(root, "committed_at") == want["committed_at"]
        assert _trap_read(root, "previous").version == want["prev"]
        assert _trap_read(space_root, "databases").atom_pointer == want_space["databases"]
        assert _trap_read(space_root, "cluster_epoch") == want_space["cluster_epoch"]
    space.close()


def test_keyspec_paths_read_across_an_unload():
    space = _space(10)
    db = space.new_database("db")
    o = _loaded_object(space, db, a=1, nested=None)
    _trapped(o)
    assert KeySpec.path("a").extract(o) == 1
    space.close()


# -- after close (I3, R104) -----------------------------------------------------------------------

def test_a_stub_read_after_its_space_closed_raises_protoclosederror(tmp_path):
    space = _space(10, tmp_path)
    db = space.new_database("d")

    def build(tr):
        c = tr.new_dictionary()
        for i in range(100):
            c = c.set_at(i, tr.new_object(id=i, name=f"n{i}"))
        tr.set_root_object("c", c)
    db.run(build)
    with db.new_transaction() as tr:
        rows = [tr.get_root_object("c").get_at(i) for i in range(100)]
        assert all(r.name == f"n{i}" for i, r in enumerate(rows))
    space.close()
    ok = closed = 0
    for i, r in enumerate(rows):
        try:
            assert r.name == f"n{i}"
            ok += 1
        except ProtoClosedError:
            closed += 1
    assert ok + closed == 100 and closed >= 80, (ok, closed)
    assert issubclass(ProtoClosedError, ProtoValidationError)
    assert space.atom_cache_stats()["entries"] == 0 and space.atom_cache_stats()["identity_map"] == 0
    stub = next(r for r in rows if not r.__dict__["_loaded"])
    with pytest.raises(ProtoClosedError):
        getattr(stub, "name", "default")       # never a silent default
    with pytest.raises(ProtoClosedError):
        stub.has_defined_attr("name")


def test_repr_never_materializes_the_instance_dict():
    """Review 8.3m M4: describing a cached atom (logging, an error message) must not cost it its
    specialized attribute reads."""
    space = _space(1000)
    db = space.new_database("d")

    def build(tr):
        tr.set_root_object("row", _Row(a=1, context=tr))
        tr.set_root_object("dict", tr.new_dictionary().set_at(1, "x"))
        tr.set_root_object("list", tr.new_list().append_last(1))
        tr.set_root_object("set", tr.new_set().add(1))
        tr.set_root_object("hash", tr.new_hash_dictionary().set_at(1, "x"))
        tr.set_root_object("rep", tr.new_repeated_keys_dictionary().set_at(1, "x"))
    db.run(build)
    names = ["row", "dict", "list", "set", "hash", "rep"]
    with db.new_transaction() as tr:
        atoms = [tr.get_root_object(n) for n in names]
        for a in atoms:
            repr(a)                             # loaded (decoded twins)
        space._atoms._unload_all()
        for a in atoms:
            repr(a)                             # unloaded stubs
        bad = [n for n, a in zip(names, atoms) if _materialized(a)]
    assert not bad, bad


def test_an_admission_racing_close_leaves_nothing_resident(monkeypatch):
    """Review 8.3m M2: an admission that passed the closed check before ``close()`` and puts after
    it must not leave the atom resident in the closed cache."""
    space = _space(100)
    db = space.new_database("d")
    db.run(lambda tr: tr.set_root_object("o", tr.new_object(a=1)))
    cache = space._atoms
    cache._unload_all()
    real = cache._cache.put_evicting

    def close_first(key, value, *args):
        cache.close()                   # close() runs entirely between the check and the put
        return real(key, value, *args)
    monkeypatch.setattr(cache._cache, "put_evicting", close_first)
    with db.new_transaction() as tr:
        assert tr.get_root_object("o").a == 1
    assert cache.stats()["entries"] == 0


# -- the _restore contract (review 8.3m I1 / M7, ruling R105) ------------------------------------

class _Renaming(DBObject):
    """A load-time rename: ``_restore`` installs ``new`` for a persisted ``old``."""
    __type_name__ = "tests.unit.test_atom_residency.Renaming"

    def _restore(self, state):
        state = dict(state)
        if "old" in state:
            state["new"] = state.pop("old")
        super()._restore(state)


class _Deriving(DBObject):
    """``_restore`` installs a derived key besides the persisted ones (it would survive an unload)."""
    __type_name__ = "tests.unit.test_atom_residency.Deriving"

    def _restore(self, state):
        super()._restore(state)
        object.__setattr__(self, "derived", 1)


class _Keeping(DBObject):
    """A legitimate override: values transformed, the key set kept (like ``Index._restore``)."""
    __type_name__ = "tests.unit.test_atom_residency.Keeping"

    def _restore(self, state):
        super()._restore({k: (v * 2 if k == "x" else v) for k, v in state.items()})


def _bounded(fn, seconds=20, unstick=None):
    """Run ``fn`` in a thread; return ``(raised, value)``; fail the test if it does not finish
    (a residency loop that never terminates must fail, not hang the suite: ``unstick`` releases
    it before the failure is reported)."""
    out = {}

    def run():
        try:
            out["value"] = fn()
        except BaseException as e:      # reported to the caller
            out["raised"] = e
    t = threading.Thread(target=run, daemon=True)
    t.start()
    t.join(seconds)
    if t.is_alive() and unstick is not None:
        unstick()
        t.join(seconds)
    assert "raised" in out or "value" in out, "did not terminate (a residency loop spins)"
    return out.get("raised"), out.get("value")


def _assert_contract_error(raised, cls, detail):
    from proto_db.errors import ProtoUserError
    assert isinstance(raised, ProtoUserError), repr(raised)
    msg = str(raised)
    assert cls.__type_name__ in msg and "_restore" in msg and detail in msg, msg


@pytest.mark.parametrize("cls, detail", [(_Renaming, "'old'"), (_Deriving, "'derived'")])
def test_a_restore_that_breaks_the_contract_raises_one_clear_error_on_load(cls, detail):
    space = _space(0)               # no decoded twin on save: the error comes from the stub load
    db = space.new_database("db")
    db.run(lambda tr: tr.set_root_object("o", cls(old=1, context=tr)))
    space2 = ObjectSpace(space._storage, atom_cache_entries=100)     # a stub load from storage

    def read():
        with space2.open_database("db").new_transaction() as tr:
            o = tr.get_root_object("o")
            return getattr(o, "new", None), getattr(o, "old", None)
    raised, _ = _bounded(read)
    _assert_contract_error(raised, cls, detail)


@pytest.mark.parametrize("cls, detail", [(_Renaming, "'old'"), (_Deriving, "'derived'")])
def test_a_restore_that_breaks_the_contract_raises_on_the_saved_twin(cls, detail):
    space = _space(100)
    db = space.new_database("db")
    raised, _ = _bounded(lambda: db.run(lambda tr: tr.set_root_object("o", cls(old=1, context=tr))))
    _assert_contract_error(raised, cls, detail)


def test_a_restore_that_keeps_the_key_set_loads_and_unloads():
    space = _space(100)
    db = space.new_database("db")
    db.run(lambda tr: tr.set_root_object("o", _Keeping(x=1, y=2, context=tr)))
    space2 = ObjectSpace(space._storage, atom_cache_entries=100)
    db2 = space2.open_database("db")
    with db2.new_transaction() as tr:
        o = tr.get_root_object("o")
        assert (o.x, o.y) == (2, 2)
        assert o._state() == {"x": 2, "y": 2}
    space2._atoms._unload_all()
    with db2.new_transaction() as tr:
        assert tr.get_root_object("o").x == 2


def test_a_snapshot_whose_recorded_name_is_missing_raises_instead_of_spinning():
    """Defence in depth: a generation token naming a key that is not installed, with the token
    still current, is an invariant violation -- one error, never a retry loop."""
    space = _space(100)
    db = space.new_database("db")
    db.run(lambda tr: tr.set_root_object("o", tr.new_object(a=1)))
    with db.new_transaction() as tr:
        o = tr.get_root_object("o")
        token = o._loaded
        assert isinstance(token, atom_mod._Generation)
        object.__setattr__(o, "_loaded", atom_mod._Generation(token.names + ("ghost",)))
        raised, _ = _bounded(o._snapshot, 10, lambda: object.__setattr__(o, "_loaded", True))
    _assert_contract_error(raised, type(o), "'ghost'")


class _InPlace(DBObject):
    """A legitimate override that transforms a value in place in the decoded state (key set kept),
    counting its runs: the hook must run once per load (re-review 8.3m N2, ruling R114)."""
    __type_name__ = "tests.unit.test_atom_residency.InPlace"
    runs = 0

    def _restore(self, state):
        type(self).runs += 1
        state["x"] = state["x"] * 2
        super()._restore(state)


@pytest.mark.parametrize("entries", [0, 100])
def test_an_in_place_transforming_restore_runs_once_per_load(entries):
    space = _space(entries)
    db = space.new_database("db")
    _InPlace.runs = 0
    db.run(lambda tr: tr.set_root_object("o", _InPlace(x=1, context=tr)))
    twins = _InPlace.runs                       # one per decoded twin of a save (0 without a cache)
    assert twins == (1 if entries else 0)
    if entries:
        with db.new_transaction() as tr:        # the saved twin
            assert tr.get_root_object("o").x == 2
    space2 = ObjectSpace(space._storage, atom_cache_entries=100)
    with space2.open_database("db").new_transaction() as tr:   # a stub load from storage
        o = tr.get_root_object("o")
        assert o.x == 2
        assert o._state() == {"x": 2}
    assert _InPlace.runs == twins + 1


def test_a_snapshot_error_names_the_key_that_is_missing():
    """Re-review 8.3m N8: a missing ``context`` is reported as such, not as the last recorded name."""
    space = _space(100)
    db = space.new_database("db")
    db.run(lambda tr: tr.set_root_object("o", tr.new_object(a=1)))
    with db.new_transaction() as tr:
        o = tr.get_root_object("o")
        assert isinstance(o._loaded, atom_mod._Generation)
        context = o.context
        object.__delattr__(o, "context")
        try:
            raised, _ = _bounded(o._snapshot, 10, lambda: object.__setattr__(o, "_loaded", True))
        finally:
            object.__setattr__(o, "context", context)
    _assert_contract_error(raised, type(o), "'context'")
    assert "'a'" not in str(raised)
