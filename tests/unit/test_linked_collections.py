"""Linked collections (spec section 9: R40-R47; R5, R11, R13, R18, N5, B3).

``get_root_object`` on a Dictionary or HashDictionary root returns a linked copy: an instance of
the transaction's own carrying the link (transaction, root name, base pointer).  Every version a
mutator derives from it inherits the link and carries its own intentions (``_changes``); reads
are recorded in the transaction.  Interleavings are explicit: open A, open B, commit B, commit A.
"""
import pytest

from proto_db.collections.dictionaries import Dictionary
from proto_db.collections.linking import Link, fine_grained, link_of
from proto_db.errors import ProtoConflictError
from proto_db.preconditions import Unit, UnitKind
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace


@pytest.fixture
def space():
    s = ObjectSpace(MemoryStorage())
    yield s
    s.close()


@pytest.fixture
def db(space):
    return space.new_database("db")


def key(root, k):
    return Unit(UnitKind.KEY, root, key=k)


def whole(root):
    return Unit(UnitKind.WHOLE, root)


def content(db, name):
    return dict(db.run(lambda tr: tr.get_root_object(name)).items())


def seed(db, name="d", **items):
    def build(tr):
        d = tr.new_dictionary()
        for k, v in items.items():
            d = d.set_at(k, v)
        tr.set_root_object(name, d)
    db.run(build)


def test_get_root_object_returns_linked_copy_not_cached_atom(db):
    seed(db, a=1)
    tr = db.new_transaction()
    c = tr.get_root_object("d")
    cached = tr._objects.get_at("d")
    assert c is not cached
    assert c.atom_pointer == cached.atom_pointer and c.atom_pointer is not None
    assert link_of(cached) is None and getattr(cached, "_link", None) is None
    link = link_of(c)
    assert isinstance(link, Link)
    assert link.root == "d" and link.transaction is tr and link.base_pointer == cached.atom_pointer
    assert tr.get_root_object("d") is c          # one linked copy per root and transaction
    tr.abort()


def test_link_inherited_by_mutators(db):
    seed(db, a=1)
    tr = db.new_transaction()
    c = tr.get_root_object("d")
    d2 = c.set_at("b", 2)
    d3 = d2.remove_at("a")
    assert link_of(d2) == link_of(c) and link_of(d3) == link_of(c)
    assert d3._changes == (("set", "b", 2), ("remove", "a"))
    assert c._changes == ()
    assert dict(d3.items()) == {"b": 2}
    tr.abort()


@pytest.mark.parametrize("end", ["commit", "abort"])
def test_link_invalid_after_commit_and_abort(db, end):
    seed(db, a=1)
    tr = db.new_transaction()
    c = tr.get_root_object("d")
    tr.set_root_object("d", c.set_at("b", 2))
    assert link_of(c) is not None
    getattr(tr, end)()
    assert link_of(c) is None
    # Reused in another transaction it is not linked (R47): written to "d" it is an object root.
    other = db.new_transaction()
    other.set_root_object("d", c.set_at("c", 3))
    assert Unit(UnitKind.ROOT, "d") in other._read_log
    other.abort()


def test_collection_from_other_root_is_unlinked(db):
    seed(db, "d", a=1)
    seed(db, "e", z=0)
    a, b = db.new_transaction(), db.new_transaction()
    value = a.get_root_object("d").set_at("b", 2)
    a.set_root_object("d", value)
    a.set_root_object("e", value)                 # linked to "d", not to "e" (R46)
    assert Unit(UnitKind.ROOT, "e") in a._read_log
    assert Unit(UnitKind.ROOT, "d") not in a._read_log
    b.set_root_object("e", b.get_root_object("e").set_at("y", 1))
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == Unit(UnitKind.ROOT, "e")


def test_collection_from_other_transaction_is_unlinked(db):
    seed(db, a=1)
    a, b, c = db.new_transaction(), db.new_transaction(), db.new_transaction()
    foreign = a.get_root_object("d").set_at("x", 1)
    b.set_root_object("d", foreign)               # a's link names a, not b (R46)
    assert Unit(UnitKind.ROOT, "d") in b._read_log
    c.set_root_object("d", c.get_root_object("d").set_at("y", 2))
    c.commit()
    with pytest.raises(ProtoConflictError) as e:
        b.commit()
    assert e.value.unit == Unit(UnitKind.ROOT, "d")
    a.abort()


def test_unassigned_version_reads_still_validated(db):
    """Review focus 1: a read on a derived version that is never assigned stays recorded (R44, R45)."""
    seed(db, k=1, j=1)
    a, b = db.new_transaction(), db.new_transaction()
    branch = a.get_root_object("d").set_at("x", 1)       # never assigned
    assert branch.get_at("k") == 1
    a.set_root_object("other", 5)
    b.set_root_object("d", b.get_root_object("d").set_at("k", 2))
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == key("d", "k")
    assert content(db, "d") == {"k": 2, "j": 1}


def test_two_branches_only_assigned_published(db):
    """Review focus 2: only the assigned version's intentions are published (R43, R45)."""
    seed(db, a=1)
    a, b = db.new_transaction(), db.new_transaction()
    base = a.get_root_object("d")
    base.set_at("x", 1)                                   # discarded branch
    a.set_root_object("d", base.set_at("y", 2))
    b.set_root_object("d", b.get_root_object("d").set_at("z", 3))
    b.commit()
    a.commit()
    assert content(db, "d") == {"a": 1, "y": 2, "z": 3}


def test_count_read_on_modified_version_records_whole(db):
    """R18: count after a set_at reveals the base count, so the whole collection is recorded."""
    seed(db, a=1)
    a, b = db.new_transaction(), db.new_transaction()
    d2 = a.get_root_object("d").set_at("x", 1)
    assert whole("d") not in a._read_log
    assert d2.count == 2
    assert whole("d") in a._read_log
    a.set_root_object("d", d2)
    b.set_root_object("d", b.get_root_object("d").set_at("z", 3))
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == whole("d")


def test_read_of_own_written_key_records_nothing(db):
    """R9: the write recorded the base value of the key; reading it back adds nothing."""
    seed(db, k=1)
    tr = db.new_transaction()
    d2 = tr.get_root_object("d").set_at("k", 5)
    before = tr._read_log.preconditions()
    assert key("d", "k") in before
    assert d2.get_at("k") == 5 and d2.has("k") and "k" in d2
    assert tr._read_log.preconditions() == before
    tr.abort()


class OverridingDictionary(Dictionary):
    """An application subclass that overrides a listed operation (N5): an object root."""

    __type_name__ = "tests.linked.OverridingDictionary"

    def set_at(self, key, value):
        return super().set_at(key, value)


class PlainSubclass(Dictionary):
    """An application subclass that overrides none of the listed operations: fine-grained (N5)."""

    __type_name__ = "tests.linked.PlainSubclass"


def test_subclass_overriding_set_at_is_object_root(db):
    assert fine_grained(Dictionary) and fine_grained(PlainSubclass)
    assert not fine_grained(OverridingDictionary)
    db.run(lambda tr: tr.set_root_object("d", OverridingDictionary(context=tr).set_at("a", 1)))
    a, b = db.new_transaction(), db.new_transaction()
    d = a.get_root_object("d")
    assert link_of(d) is None
    assert Unit(UnitKind.ROOT, "d") in a._read_log
    a.set_root_object("d", d.set_at("x", 1))
    b.set_root_object("d", b.get_root_object("d").set_at("y", 2))      # another key
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == Unit(UnitKind.ROOT, "d")


def test_slice_result_assigned_takes_whole_collection(db):
    """P-B: a version from an unlisted operation (List.slice; Dictionary.rebuilt) is published with a
    whole-collection precondition.  The List is linked in Task 7; until then its read records the
    whole root (transitional), with the same effect."""
    db.run(lambda tr: (tr.set_root_object("l", tr.new_list().extend([1, 2, 3])),
                       tr.set_root_object("d", tr.new_dictionary().set_at("a", 1))))
    a, b = db.new_transaction(), db.new_transaction()
    a.set_root_object("l", a.get_root_object("l").slice(0, 2))
    rebuilt = a.get_root_object("d").rebuilt()
    assert rebuilt._whole and link_of(rebuilt) is not None
    a.set_root_object("d", rebuilt)
    assert whole("d") in a._read_log and whole("l") in a._read_log
    b.set_root_object("d", b.get_root_object("d").set_at("z", 1))
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == whole("d")
    # Without a concurrent change the transaction's value is published as is.
    with db.new_transaction() as tr:
        tr.set_root_object("d", tr.get_root_object("d").remove_at("z").rebuilt())
    assert content(db, "d") == {"a": 1}


def test_query_on_a_linked_collection_records_whole(db, space):
    """Section 6, R22: a query (a cached one too) reads the whole collection until index units land."""
    from proto_db.query.linq import from_collection
    seed(db, a=1, b=2)
    with db.new_transaction() as warm:                    # fill the query cache with this version
        from_collection(warm.get_root_object("d")).cached(space.query_cache)
    tr = db.new_transaction()
    d = tr.get_root_object("d")
    assert whole("d") not in tr._read_log
    result = from_collection(d).cached(space.query_cache)
    assert sorted(result) == [1, 2]
    assert whole("d") in tr._read_log
    tr.abort()


class ClearingDictionary(Dictionary):
    """A fine-grained subclass (it overrides no listed operation, N5) whose own method builds a
    version through ``_with`` without logging an op and without reading a persisted field."""

    __type_name__ = "tests.linked.ClearingDictionary"

    def clear(self):
        return self._with(root=None, count=0)


@pytest.mark.parametrize("concurrent", ["none", "other root", "same root"])
def test_unlogged_content_change_takes_whole_collection(db, concurrent):
    """P-B: a content change that extends no ``_changes`` cannot be reapplied, so the version is
    whole: it is published as is, or conflicts on WHOLE; it is never silently dropped."""
    assert fine_grained(ClearingDictionary)
    db.run(lambda tr: (tr.set_root_object("d", ClearingDictionary(context=tr).set_at("a", 1).set_at("b", 2)),
                       tr.set_root_object("e", 0)))
    a = db.new_transaction()
    cleared = a.get_root_object("d").clear()
    assert cleared._whole and link_of(cleared) is not None
    a.set_root_object("d", cleared)
    assert whole("d") in a._read_log
    if concurrent == "other root":
        db.run(lambda tr: tr.set_root_object("e", 1))
    elif concurrent == "same root":
        db.run(lambda tr: tr.set_root_object("d", tr.get_root_object("d").set_at("z", 3)))
    if concurrent == "same root":
        with pytest.raises(ProtoConflictError) as e:
            a.commit()
        assert e.value.unit == whole("d")
        assert content(db, "d") == {"a": 1, "b": 2, "z": 3}
    else:
        a.commit()
        assert content(db, "d") == {}
