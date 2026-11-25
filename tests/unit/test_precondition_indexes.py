"""Indexes under the precondition model (spec section 7: R24-R26, R28, R30; B4, B5, B7, B8).

Explicit interleavings only: open A, open B, B commits, A commits.

Catches: a write reapplied with keys that do not cover a concurrently added index (R25), an
index definition change that read the collection composed with a concurrent write instead of
conflicting (R26; one that read nothing catches up, R69), an
equality lookup that conflicts with changes of other groups or misses changes of its own (R28,
B4), a raw index read that records less than the whole collection (B5), a format-3 index
rebuilt under the root lock instead of in preparation (B8), an ivf_flat index maintained under
the root lock (B7), and vector indexes not maintained from the precomputed vector (R30).
"""
import pytest

from proto_db.collections import avl
from proto_db.collections.avl import CountedKeyNode
from proto_db.errors import ProtoConflictError
from proto_db.indexing.equality import EqualityIndex
from proto_db.indexing.keyspec import KeySpec
from proto_db.indexing.range import RangeIndex
from proto_db.indexing.vector import VectorIndex
from proto_db.preconditions import Unit, UnitKind
from proto_db.query.planner import plan_query
from proto_db.query.terms import EQ, GT, IN, Term
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from tests.support.recording_index import RootRecordingExtractor


@pytest.fixture
def space():
    s = ObjectSpace(MemoryStorage())
    yield s
    s.close()


@pytest.fixture
def db(space):
    return space.new_database("db")


def whole(root):
    return Unit(UnitKind.WHOLE, root)


def defs(root):
    return Unit(UnitKind.INDEX_DEFS, root)


def group(root, index, key):
    return Unit(UnitKind.INDEX_GROUP, root, key=key, index=index)


def seed(db, name="d", index=None, **items):
    def build(tr):
        d = tr.new_dictionary()
        for k, v in items.items():
            d = d.set_at(k, v)
        if index is not None:
            d = d.add_index(index)
        tr.set_root_object(name, d)
    db.run(build)


def content(db, name="d"):
    with db.new_transaction() as tr:
        return dict(tr.get_root_object(name).items())


def lookup(d, *values):
    term = Term(None, EQ, values[0]) if len(values) == 1 else Term(None, IN, values)
    return sorted(plan_query(d, term).execute())


# -- R25, R26: index definitions --------------------------------------------------------------------

def test_concurrent_add_index_conflicts_with_write(db):
    """A write's keys were computed for the base's indexes: an index another commit added is not
    covered, so the write conflicts on the index definitions (R25).  An add_index takes the whole
    collection (R26): a concurrent write conflicts with it."""
    seed(db, a=1, b=2)
    a, b = db.new_transaction(), db.new_transaction()
    a.set_root_object("d", a.get_root_object("d").set_at("a", 5))
    b.set_root_object("d", b.get_root_object("d").add_index(EqualityIndex("e", KeySpec.element())))
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == defs("d")
    # The retry sees the index and maintains it.
    db.run(lambda tr: tr.set_root_object("d", tr.get_root_object("d").set_at("a", 5)))
    with db.new_transaction() as tr:
        d = tr.get_root_object("d")
        assert lookup(d, 5) == [5]
        d.get_index("e").validate(list(d.elements()))

    # The other way round: an index change that also read the collection conflicts with a
    # concurrent write (whole collection, R26); one that read nothing catches up (R69,
    # tests/unit/test_index_catch_up.py).
    a, b = db.new_transaction(), db.new_transaction()
    d = a.get_root_object("d")
    assert d.get_at("a") == 5
    a.set_root_object("d", d.add_index(RangeIndex("r", KeySpec.element())))
    b.set_root_object("d", b.get_root_object("d").set_at("c", 3))
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == whole("d")

    # remove_index likewise conflicts with a concurrent write, and a write with it.
    a, b = db.new_transaction(), db.new_transaction()
    a.set_root_object("d", a.get_root_object("d").set_at("z", 9))
    b.set_root_object("d", b.get_root_object("d").remove_index("e"))
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == defs("d")


def test_write_records_index_definitions_and_composes_with_other_keys(db):
    seed(db, index=EqualityIndex("e", KeySpec.element()), a=1, b=2)
    a, b = db.new_transaction(), db.new_transaction()
    a.set_root_object("d", a.get_root_object("d").set_at("a", 7))
    b.set_root_object("d", b.get_root_object("d").set_at("b", 7))
    b.commit()
    a.commit()
    assert defs("d") in a._read_log
    assert content(db) == {"a": 7, "b": 7}
    with db.new_transaction() as tr:
        d = tr.get_root_object("d")
        assert lookup(d, 7) == [7, 7] and lookup(d, 1) == [] and lookup(d, 2) == []


# -- R28, B4: equality lookups ---------------------------------------------------------------------

def test_equality_lookup_conflicts_only_same_group(db):
    seed(db, index=EqualityIndex("e", KeySpec.element()), a=5, b=6)
    # A change in another group composes with the lookup.
    a, b = db.new_transaction(), db.new_transaction()
    assert lookup(a.get_root_object("d"), 5) == [5]
    assert group("d", "e", 5) in a._read_log and whole("d") not in a._read_log
    a.set_root_object("other", a.new_object(n=1))
    b.set_root_object("d", b.get_root_object("d").set_at("c", 7))
    b.commit()
    a.commit()
    # A change in the same group conflicts, naming the group.
    a, b = db.new_transaction(), db.new_transaction()
    assert lookup(a.get_root_object("d"), 5) == [5]
    a.set_root_object("other", a.new_object(n=2))
    b.set_root_object("d", b.get_root_object("d").set_at("z", 5))
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == group("d", "e", 5)
    # An absent group: a later insert under its key conflicts too.
    a, b = db.new_transaction(), db.new_transaction()
    assert lookup(a.get_root_object("d"), 8, 9) == []
    assert group("d", "e", 8) in a._read_log and group("d", "e", 9) in a._read_log
    a.set_root_object("other", a.new_object(n=3))
    b.set_root_object("d", b.get_root_object("d").set_at("y", 9))
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == group("d", "e", 9)


def test_lookup_on_a_written_version_expects_the_base_group(db):
    """R18: a lookup on a version the transaction changed records the base's group."""
    seed(db, index=EqualityIndex("e", KeySpec.element()), a=5)
    a, b = db.new_transaction(), db.new_transaction()
    d = a.get_root_object("d").set_at("n", 5)
    assert lookup(d, 5) == [5, 5]
    a.set_root_object("d", d)
    b.set_root_object("d", b.get_root_object("d").remove_at("a"))
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == group("d", "e", 5)


def test_range_index_eq_lookup_records_group(db):
    seed(db, index=RangeIndex("r", KeySpec.element()), a=5, b=6)
    tr = db.new_transaction()
    d = tr.get_root_object("d")
    assert lookup(d, 5) == [5]
    assert group("d", "r", 5) in tr._read_log and whole("d") not in tr._read_log
    assert defs("d") in tr._read_log
    # A range scan reads the range it scanned (R68), not the whole collection; a scan reads it all.
    assert sorted(plan_query(d, Term(None, GT, 5)).execute()) == [6]
    assert whole("d") not in tr._read_log
    assert [(r.root, r.index) for r in tr._read_log.ranges()] == [("d", "r")]
    tr.abort()
    tr = db.new_transaction()
    assert list(plan_query(tr.get_root_object("d")).execute()) == [5, 6]
    assert whole("d") in tr._read_log
    tr.abort()


def test_unindexed_lookup_records_whole(db):
    seed(db, a=5, b=6)
    tr = db.new_transaction()
    assert lookup(tr.get_root_object("d"), 5) == [5]
    assert whole("d") in tr._read_log
    tr.abort()


# -- B5: raw index reads ----------------------------------------------------------------------------

def test_get_index_records_whole(db):
    seed(db, index=EqualityIndex("e", KeySpec.element()), a=5)
    tr = db.new_transaction()
    d = tr.get_root_object("d")
    assert d.index_names() == ["e"]
    assert defs("d") in tr._read_log and whole("d") not in tr._read_log
    index = d.get_index("e")
    assert index is not None and whole("d") in tr._read_log
    tr.abort()
    a, b = db.new_transaction(), db.new_transaction()
    a.get_root_object("d").get_index("e")
    a.set_root_object("other", a.new_object(n=1))
    b.set_root_object("d", b.get_root_object("d").set_at("q", 1))
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == whole("d")


# -- B8: format-3 indexes ---------------------------------------------------------------------------

def _legacy(index):
    """``index`` as a format-3 writer left it: member tokens of the old Python scheme, no marker."""
    groups = None
    for key, (members, total) in avl.key_iter(index.groups):
        old = None
        for (eq, _tok), (element, n) in avl.key_iter(members):
            old = avl.key_put(old, (eq, ("i", element)), (element, n), node_class=CountedKeyNode)[0]
        groups = avl.key_put(groups, key, (old, total), node_class=CountedKeyNode)[0]
    legacy = index._with(groups=groups)
    object.__delattr__(legacy, "member_tokens")
    return legacy


def test_stale_format3_index_rebuilt_in_preparation(db):
    with RootRecordingExtractor("b8_ident") as extractor:
        def build(tr):
            d = tr.new_dictionary().set_at("a", 1).set_at("b", 2).add_index(EqualityIndex("e", extractor.key_spec))
            legacy = _legacy(d.get_index("e"))
            tr.set_root_object("d", d._with(indexes=d.indexes.without("e").with_index(legacy)))
        db.run(build)
        with db.new_transaction() as tr:
            assert not tr.get_root_object("d").get_index("e").has_current_tokens()
        # A concurrent write to the root: the stale index makes A take the whole collection.
        a, b = db.new_transaction(), db.new_transaction()
        a.set_root_object("d", a.get_root_object("d").remove_at("b"))
        b.set_root_object("d", b.get_root_object("d").set_at("c", 3))
        b.commit()
        with pytest.raises(ProtoConflictError) as e:
            a.commit()
        assert e.value.unit == whole("d")
        assert extractor.held_calls() == 0
        # Alone, the write publishes its version with the index rebuilt in preparation (B's
        # commit rebuilt it the same way: the root is written stale again first).
        db.run(build)
        with db.new_transaction() as tr:
            assert not tr.get_root_object("d").get_index("e").has_current_tokens()
        extractor.reset()
        a = db.new_transaction()
        a.set_root_object("d", a.get_root_object("d").remove_at("zz"))   # changes no index entry
        a.commit()
        assert whole("d") in a._read_log
        assert extractor.calls and extractor.held_calls() == 0
        with db.new_transaction() as tr:
            d = tr.get_root_object("d")
            index = d.get_index("e")
            assert index.has_current_tokens()
            index.validate(list(d.elements()))


# -- B7, R30: vector indexes ------------------------------------------------------------------------

def test_ivf_flat_write_takes_whole_collection(db):
    def build(tr):
        d = tr.new_dictionary()
        for i in range(6):
            d = d.set_at(f"k{i}", (float(i), 1.0))
        tr.set_root_object("d", d.add_index(VectorIndex("v", KeySpec.element(), 2, metric="l2", backend="ivf_flat",
                                                        nlist=2)))
    db.run(build)
    a, b = db.new_transaction(), db.new_transaction()
    a.set_root_object("d", a.get_root_object("d").set_at("n", (9.0, 1.0)))
    b.set_root_object("d", b.get_root_object("d").set_at("m", (8.0, 1.0)))
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == whole("d")
    assert whole("d") in a._read_log
    db.run(lambda tr: tr.set_root_object("d", tr.get_root_object("d").set_at("n", (9.0, 1.0))))
    with db.new_transaction() as tr:
        d = tr.get_root_object("d")
        assert len(d) == 8
        d.get_index("v").validate(list(d.elements()))


@pytest.mark.parametrize("backend", ["exact", "hnsw"])
def test_hnsw_and_exact_reapply_from_vector(db, backend):
    with RootRecordingExtractor(f"vec_{backend}") as extractor:
        def build(tr):
            d = tr.new_dictionary().set_at("a", (1.0, 0.0)).set_at("b", (0.0, 1.0))
            tr.set_root_object("d", d.add_index(VectorIndex("v", extractor.key_spec, 2, backend=backend)))
        db.run(build)
        a, b = db.new_transaction(), db.new_transaction()
        a.set_root_object("d", a.get_root_object("d").set_at("a", (2.0, 0.0)).set_at("n", (1.0, 1.0)))
        b.set_root_object("d", b.get_root_object("d").set_at("b", (0.0, 3.0)).set_at("m", "not a vector"))
        b.commit()
        extractor.reset()
        a.commit()                                   # reapplied over B (exact and hnsw keep keyed hooks)
        assert extractor.calls and extractor.held_calls() == 0
        assert whole("d") not in a._read_log
        with db.new_transaction() as tr:
            d = tr.get_root_object("d")
            assert dict(d.items()) == {"a": (2.0, 0.0), "b": (0.0, 3.0), "m": "not a vector", "n": (1.0, 1.0)}
            index = d.get_index("v")
            index.validate(list(d.elements()))
            assert index.explain()["invalid"] == 1 and index.explain()["vectors"] == 3


# -- Removals of the D26 types on an indexed collection (declared fine preconditions) ---------------

def _rkd(db, indexed):
    def build(tr):
        r = tr.new_repeated_keys_dictionary().set_at("k", 1).set_at("j", 5)
        tr.set_root_object("r", r.add_index(EqualityIndex("e", KeySpec.element())) if indexed else r)
    db.run(build)


def _rkd_content(db):
    with db.new_transaction() as tr:
        r = tr.get_root_object("r")
        return sorted((k, rec) for k in r.keys() for rec in r.get_at(k).elements())


@pytest.mark.parametrize("remove", ["remove_key", "remove_at"])
def test_indexed_rkd_remove_key_conflicts_on_records(db, remove):
    """T1 adds a record under k and commits; T2 clears k.  Reapplied, the clear would take out
    T1's record, whose key T2's preparation never computed: on an indexed multimap the clear
    records Unit(RECORDS, k) and the conflict is declared on it (R36), not a reapplication
    failure."""
    _rkd(db, indexed=True)
    t1, t2 = db.new_transaction(), db.new_transaction()
    t1.set_root_object("r", t1.get_root_object("r").set_at("k", 2))
    t2.set_root_object("r", getattr(t2.get_root_object("r"), remove)("k"))
    assert Unit(UnitKind.RECORDS, "r", key="k") in t2._read_log
    t1.commit()
    with pytest.raises(ProtoConflictError) as e:
        t2.commit()
    assert e.value.unit == Unit(UnitKind.RECORDS, "r", key="k")
    assert _rkd_content(db) == [("j", 5), ("k", 1), ("k", 2)]


def test_unindexed_rkd_remove_key_composes(db):
    """The same interleaving without an index: the clear composes after T1 (D26), no precondition."""
    _rkd(db, indexed=False)
    t1, t2 = db.new_transaction(), db.new_transaction()
    t1.set_root_object("r", t1.get_root_object("r").set_at("k", 2))
    t2.set_root_object("r", t2.get_root_object("r").remove_key("k"))
    assert Unit(UnitKind.RECORDS, "r", key="k") not in t2._read_log
    t1.commit()
    t2.commit()
    assert _rkd_content(db) == [("j", 5)]


def test_indexed_rkd_other_key_change_composes(db):
    _rkd(db, indexed=True)
    t1, t2 = db.new_transaction(), db.new_transaction()
    t1.set_root_object("r", t1.get_root_object("r").set_at("j", 6).remove_record("j", 5))
    t2.set_root_object("r", t2.get_root_object("r").remove_key("k").remove_record("j", 9).set_at("n", 7))
    t1.commit()
    with pytest.raises(ProtoConflictError) as e:      # t2's remove_record on j meets t1's change of j
        t2.commit()
    assert e.value.unit == Unit(UnitKind.RECORDS, "r", key="j")
    t3 = db.new_transaction()
    t3.set_root_object("r", t3.get_root_object("r").remove_key("k").set_at("n", 7))
    t4 = db.new_transaction()
    t4.set_root_object("r", t4.get_root_object("r").set_at("j", 8))
    t4.commit()
    t3.commit()                                       # k and n untouched by t4: composes
    assert _rkd_content(db) == [("j", 6), ("j", 8), ("n", 7)]
    with db.new_transaction() as tr:
        r = tr.get_root_object("r")
        r.get_index("e").validate(list(r.elements()))


@pytest.mark.parametrize("indexed", [True, False], ids=["indexed", "unindexed"])
def test_counted_set_exact_form_concurrent_change(db, indexed):
    """A concurrent commit replaced the stored 10 by an equal 10.0 with the same count.  Indexed:
    A's decrement would take 10.0 out of the index with a key never computed, so ``remove``
    records Unit(COUNT, 10) whose token holds the exact stored member: a declared conflict.
    Unindexed: the decrement composes (D26)."""
    def build(tr):
        c = tr.new_counted_set().add(10).add(20)
        tr.set_root_object("c", c.add_index(EqualityIndex("e", KeySpec.element())) if indexed else c)
    db.run(build)
    a, b = db.new_transaction(), db.new_transaction()
    a.set_root_object("c", a.get_root_object("c").remove(10))
    b.set_root_object("c", b.get_root_object("c").remove(10).add(10.0))
    b.commit()
    if indexed:
        with pytest.raises(ProtoConflictError) as e:
            a.commit()
        assert e.value.unit == Unit(UnitKind.COUNT, "c", key=10)
        expected = [(10.0, 1), (20, 1)]
    else:
        a.commit()
        expected = [(20, 1)]
    with db.new_transaction() as tr:
        c = tr.get_root_object("c")
        assert sorted(c.items()) == expected
        assert [type(e) for e, _ in sorted(c.items())] == [type(e) for e, _ in expected]


def test_indexed_counted_set_concurrent_counts_compose(db):
    """On an indexed counted set increments take no precondition and a decrement composes with
    concurrent changes of other elements; a concurrent change of the decremented element's
    count conflicts, declared on its COUNT unit."""
    def build(tr):
        tr.set_root_object("c", tr.new_counted_set().add(10).add(10).add(20).add_index(
            EqualityIndex("e", KeySpec.element())))
    db.run(build)
    a, b = db.new_transaction(), db.new_transaction()
    a.set_root_object("c", a.get_root_object("c").remove(10).add(30))
    b.set_root_object("c", b.get_root_object("c").add(20).add(30))
    b.commit()
    a.commit()
    with db.new_transaction() as tr:
        c = tr.get_root_object("c")
        assert sorted(c.items()) == [(10, 1), (20, 2), (30, 2)]
        c.get_index("e").validate(list(c.elements()))
    a, b = db.new_transaction(), db.new_transaction()
    a.set_root_object("c", a.get_root_object("c").remove(10))
    b.set_root_object("c", b.get_root_object("c").add(10))
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == Unit(UnitKind.COUNT, "c", key=10)


# -- Own write, then a removal or replacement, against a concurrent exact-form swap ----------------
# Each indexed class: A writes a key/element, then removes or replaces it (R9 would skip the
# second recording); B swaps the stored 10 for 10.0 (or adds an equal member of another form).
# The conflict must be declared on the unit before any reapplication, never the reapplication's
# fail-closed WHOLE ("cannot take this transaction's changes").

def _swap_cases():
    k = lambda kind, key=None: (kind, key)
    return [
        ("Set", lambda tr: tr.new_set().add(10).add(20), lambda s: s.add(10).remove(10),
         lambda s: s.remove(10).add(10.0), k(UnitKind.ELEMENT, 10)),
        ("Set-union", lambda tr: tr.new_set().add(10).add(20), lambda s: s.union([10]).remove(10),
         lambda s: s.remove(10).add(10.0), k(UnitKind.ELEMENT, 10)),
        ("Set-absent", lambda tr: tr.new_set().add(10).add(20), lambda s: s.add(30).remove(30),
         lambda s: s.add(30.0), k(UnitKind.ELEMENT, 30)),
        ("CountedSet", lambda tr: tr.new_counted_set().add(10).add(20), lambda s: s.add(10).remove(10).remove(10),
         lambda s: s.remove(10).add(10.0), k(UnitKind.COUNT, 10)),
        ("Dictionary", lambda tr: tr.new_dictionary().set_at("a", 10), lambda d: d.set_at("a", 11).remove_at("a"),
         lambda d: d.set_at("a", 10.0), k(UnitKind.KEY, "a")),
        ("Dictionary-replace", lambda tr: tr.new_dictionary().set_at("a", 10),
         lambda d: d.set_at("a", 11).set_at("a", 12), lambda d: d.set_at("a", 10.0), k(UnitKind.KEY, "a")),
        ("HashDictionary", lambda tr: tr.new_hash_dictionary().set_at("a", 10),
         lambda d: d.set_at("a", 11).remove_at("a"), lambda d: d.set_at("a", 10.0), k(UnitKind.KEY, "a")),
        ("HashDictionary-replace", lambda tr: tr.new_hash_dictionary().set_at("a", 10),
         lambda d: d.set_at("a", 11).set_at("a", 12), lambda d: d.set_at("a", 10.0), k(UnitKind.KEY, "a")),
        ("RepeatedKeysDictionary", lambda tr: tr.new_repeated_keys_dictionary().set_at("k", 10),
         lambda r: r.set_at("k", 10).remove_record("k", 10), lambda r: r.remove_record("k", 10).set_at("k", 10.0),
         k(UnitKind.RECORDS, "k")),
        ("RepeatedKeysDictionary-clear", lambda tr: tr.new_repeated_keys_dictionary().set_at("k", 10),
         lambda r: r.remove_key("k").set_at("k", 1).remove_key("k"),
         lambda r: r.remove_record("k", 10).set_at("k", 10.0), k(UnitKind.RECORDS, "k")),
        ("List", lambda tr: tr.new_list().extend([10, 20]), lambda l: l.set_at(0, 11).set_at(0, 12),
         lambda l: l.set_at(0, 10.0), k(UnitKind.POSITION, 0)),
    ]


@pytest.mark.parametrize("case", _swap_cases(), ids=lambda c: c[0])
def test_own_write_then_removal_against_exact_form_swap(db, case):
    _, build, write_a, write_b, (kind, key) = case
    db.run(lambda tr: tr.set_root_object("x", build(tr).add_index(EqualityIndex("e", KeySpec.element()))))
    a, b = db.new_transaction(), db.new_transaction()
    a.set_root_object("x", write_a(a.get_root_object("x")))
    b.set_root_object("x", write_b(b.get_root_object("x")))
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == Unit(kind, "x", key=key)
    assert "cannot take this transaction" not in str(e.value)


def test_unindexed_set_add_then_remove_keeps_r9(db):
    """Unindexed: the removal after an own add records nothing (R9) and composes over the swap."""
    db.run(lambda tr: tr.set_root_object("x", tr.new_set().add(10).add(20)))
    a, b = db.new_transaction(), db.new_transaction()
    a.set_root_object("x", a.get_root_object("x").add(10).remove(10))
    assert Unit(UnitKind.ELEMENT, "x", key=10) not in a._read_log
    b.set_root_object("x", b.get_root_object("x").remove(10).add(10.0))
    b.commit()
    a.commit()
    with db.new_transaction() as tr:
        assert sorted(tr.get_root_object("x").elements()) == [20]
