"""Set, CountedSet and RepeatedKeysDictionary under the precondition model (spec sections 5 and 6,
R5, R9, R18, D26, N5).

One pair per row of the Set, CountedSet and RepeatedKeysDictionary tables: the operation against
a concurrent change of the same unit (conflict, naming the unit, or composition) and of another
one (both published).  Interleavings are explicit: open A, open B, commit B, commit A.
"""
from collections import Counter

import pytest

from proto_db.collections.dictionaries import Dictionary, RepeatedKeysDictionary
from proto_db.collections.hash_dictionaries import HashDictionary
from proto_db.collections.linking import fine_grained, link_of
from proto_db.collections.sets import CountedSet, Set
from proto_db.errors import ProtoConflictError, ProtoValidationError
from proto_db.preconditions import Unit, UnitKind, token
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


def element(e, root="s"):
    return Unit(UnitKind.ELEMENT, root, key=e)


def count(e, root="s"):
    return Unit(UnitKind.COUNT, root, key=e)


def records(k, root="r"):
    return Unit(UnitKind.RECORDS, root, key=k)


def whole(root="s"):
    return Unit(UnitKind.WHOLE, root)


def seed_set(db, *elements, cls=Set):
    def build(tr):
        s = cls(context=tr)
        for e in elements:
            s = s.add(e)
        tr.set_root_object("s", s)
    db.run(build)


def seed_rkd(db, *pairs):
    def build(tr):
        r = tr.new_repeated_keys_dictionary()
        for k, record in pairs:
            r = r.set_at(k, record)
        tr.set_root_object("r", r)
    db.run(build)


def write(tr, fn, root="s"):
    tr.set_root_object(root, fn(tr.get_root_object(root)))


def members(db):
    return set(db.run(lambda tr: tr.get_root_object("s")).elements())


def counts(db):
    return dict(db.run(lambda tr: tr.get_root_object("s")).items())


def rkd_content(db):
    r = db.run(lambda tr: tr.get_root_object("r"))
    return {k: dict(cs.items()) for k, cs in r.items()}


def conflict(tr):
    with pytest.raises(ProtoConflictError) as e:
        tr.commit()
    assert e.value.database == "db"
    return e.value.unit


def preconditions(tr):
    return tr._read_log.preconditions()


# -- classification (R1, N5) ------------------------------------------------------------------

def test_sets_and_repeated_keys_dictionary_are_fine_grained(db):
    seed_set(db, "a")
    assert fine_grained(Set) and fine_grained(CountedSet) and fine_grained(RepeatedKeysDictionary)
    tr = db.new_transaction()
    assert link_of(tr.get_root_object("s")) is not None
    tr.abort()


def test_subclass_overriding_bucket_hash_is_an_object_root():
    """``_bucket_hash`` runs under the root lock when a unit is validated or an intention is
    reapplied: a subclass overriding it is an object root (P-C, N5)."""
    class Colliding(Set):
        __type_name__ = "tests.unit.precondition_sets.Colliding"

        def _bucket_hash(self, e):
            return 0

    class CollidingHash(HashDictionary):
        __type_name__ = "tests.unit.precondition_sets.CollidingHash"

        def _bucket_hash(self, k):
            return 0

    assert not fine_grained(Colliding) and not fine_grained(CollidingHash)


# -- Set: section 5 ---------------------------------------------------------------------------

def test_set_add_concurrent_remove_leaves_present(db):
    seed_set(db, "e")
    a, b = db.new_transaction(), db.new_transaction()
    write(a, lambda s: s.add("e"))
    write(b, lambda s: s.remove("e"))
    b.commit()
    a.commit()                          # add has no precondition: serial order B, A
    assert members(db) == {"e"}


def test_set_remove_present_conflicts_with_concurrent_remove(db):
    seed_set(db, "e", "x")
    a, b = db.new_transaction(), db.new_transaction()
    write(a, lambda s: s.remove("e"))
    assert preconditions(a)[element("e")] is not None
    write(b, lambda s: s.remove("e"))
    b.commit()
    assert conflict(a) == element("e")
    assert members(db) == {"x"}


def test_set_remove_absent_conflicts_with_concurrent_add(db):
    """The third row: a remove that found nothing expects "still absent"; a concurrent add of the
    element conflicts (no serial order gives both outcomes)."""
    seed_set(db, "x")
    a, b = db.new_transaction(), db.new_transaction()
    write(a, lambda s: s.remove("e"))
    write(b, lambda s: s.add("e"))
    b.commit()
    assert conflict(a) == element("e")
    assert members(db) == {"x", "e"}


def test_set_remove_other_element_no_conflict(db):
    seed_set(db, "e", "x")
    a, b = db.new_transaction(), db.new_transaction()
    write(a, lambda s: s.remove("e"))
    write(b, lambda s: s.remove("x").add("y"))
    b.commit()
    a.commit()
    assert members(db) == {"y"}


def test_set_add_different_elements_no_conflict(db):
    seed_set(db, "a")
    a, b = db.new_transaction(), db.new_transaction()
    write(a, lambda s: s.add("x"))
    write(b, lambda s: s.add("y"))
    b.commit()
    a.commit()
    assert members(db) == {"a", "x", "y"}


def test_set_union_and_difference_are_adds_and_removes(db):
    seed_set(db, "a", "b")
    a, b = db.new_transaction(), db.new_transaction()
    write(a, lambda s: s.union(["x", "y"]).difference(["a", "z"]))
    assert {u for u in preconditions(a) if u.kind is UnitKind.ELEMENT} == {element("a"), element("z")}
    write(b, lambda s: s.add("c").remove("b"))
    b.commit()
    a.commit()
    assert members(db) == {"c", "x", "y"}


def test_set_difference_conflicts_with_concurrent_add_of_removed(db):
    seed_set(db, "a")
    a, b = db.new_transaction(), db.new_transaction()
    write(a, lambda s: s.difference(["z"]))
    write(b, lambda s: s.add("z"))
    b.commit()
    assert conflict(a) == element("z")


def test_set_intersection_conflicts_with_any_change(db):
    seed_set(db, "a", "b")
    a, b = db.new_transaction(), db.new_transaction()
    write(a, lambda s: s.intersection(["a"]))
    write(b, lambda s: s.add("c"))
    b.commit()
    assert conflict(a) == whole()
    assert members(db) == {"a", "b", "c"}


def test_set_intersection_without_concurrent_change_publishes(db):
    seed_set(db, "a", "b")
    a, b = db.new_transaction(), db.new_transaction()
    write(a, lambda s: s.intersection(["a"]))
    b.set_root_object("other", 1)
    b.commit()
    a.commit()
    assert members(db) == {"a"}


# -- Set: section 6 ---------------------------------------------------------------------------

@pytest.mark.parametrize("read", ["has", "in"])
@pytest.mark.parametrize("same", [True, False], ids=["same", "other"])
def test_set_has_pair(db, read, same):
    seed_set(db, "e", "x")
    a, b = db.new_transaction(), db.new_transaction()
    s = a.get_root_object("s")
    assert (s.has("e") if read == "has" else "e" in s) is True
    a.new_mutable("m", 1)
    write(b, lambda s: s.remove("e" if same else "x"))
    b.commit()
    if same:
        assert conflict(a) == element("e")
    else:
        a.commit()


def test_set_has_absent_records_absence(db):
    seed_set(db, "x")
    a, b = db.new_transaction(), db.new_transaction()
    assert not a.get_root_object("s").has("e")
    a.new_mutable("m", 1)
    write(b, lambda s: s.add("e"))
    b.commit()
    assert conflict(a) == element("e")


def test_set_read_of_an_element_written_records_nothing(db):
    """R9: an element this version added or removed reads its own value, with no precondition;
    a concurrent change of it does not conflict, and the intention is the last one."""
    seed_set(db, "e")
    a, b = db.new_transaction(), db.new_transaction()
    s = a.get_root_object("s").add("z")
    assert s.has("z")
    s = s.remove("z")
    assert not s.has("z") and "z" not in s
    assert element("z") not in preconditions(a)
    a.set_root_object("s", s)
    write(b, lambda s: s.add("z"))
    b.commit()
    a.commit()                          # serial order B, A: A's remove is the last intention
    assert members(db) == {"e"}


def test_set_other_branch_read_records_base(db):
    """R9 is per version: a branch that did not write the element records its read."""
    seed_set(db, "e")
    a, b = db.new_transaction(), db.new_transaction()
    base = a.get_root_object("s")
    base.remove("e")                    # another branch, never assigned
    assert base.has("e")
    assert element("e") in preconditions(a)


def test_set_len_records_whole(db):
    seed_set(db, "a")
    a, b = db.new_transaction(), db.new_transaction()
    write(a, lambda s: s.add(str(len(s))))
    write(b, lambda s: s.add("x"))
    b.commit()
    assert conflict(a) == whole()


# -- CountedSet -------------------------------------------------------------------------------

def test_counted_set_concurrent_increments_compose(db):
    seed_set(db, "e", cls=CountedSet)
    a, b = db.new_transaction(), db.new_transaction()
    write(a, lambda s: s.add("e"))
    write(b, lambda s: s.add("e"))
    b.commit()
    a.commit()
    assert counts(db) == {"e": 3}


def test_counted_set_double_decrement_floor_zero(db):
    seed_set(db, "e", "x", cls=CountedSet)
    a, b = db.new_transaction(), db.new_transaction()
    write(a, lambda s: s.remove("e"))
    write(b, lambda s: s.remove("e"))
    b.commit()
    a.commit()                          # the decrement past 0 is the serial no-op
    assert counts(db) == {"x": 1}
    s = db.run(lambda tr: tr.get_root_object("s"))
    assert s.count == 1 and s.total_count == 1


@pytest.mark.parametrize("read", ["count_of", "has"])
def test_counted_set_count_of_conflicts_with_concurrent_increment(db, read):
    seed_set(db, "e", cls=CountedSet)
    a, b = db.new_transaction(), db.new_transaction()
    s = a.get_root_object("s")
    assert getattr(s, read)("e") in (1, True)
    a.set_root_object("s", s.add("y"))
    write(b, lambda s: s.add("e"))
    b.commit()
    assert conflict(a) == count("e")


def test_counted_set_count_of_other_element_no_conflict(db):
    seed_set(db, "e", cls=CountedSet)
    a, b = db.new_transaction(), db.new_transaction()
    s = a.get_root_object("s")
    assert s.count_of("e") == 1
    a.set_root_object("s", s.add("e"))
    write(b, lambda s: s.add("x"))
    b.commit()
    a.commit()
    assert counts(db) == {"e": 2, "x": 1}


def test_counted_set_count_after_own_add_records_base_count(db):
    """R18: the count read after an own increment reveals the base count, so it is recorded with
    the base's count (not the version's)."""
    seed_set(db, "e", cls=CountedSet)
    a, b = db.new_transaction(), db.new_transaction()
    s = a.get_root_object("s").add("e")
    assert s.count_of("e") == 2
    assert preconditions(a)[count("e")] == token(1)      # the base count, not 2
    a.set_root_object("s", s)
    write(b, lambda s: s.add("e"))
    b.commit()
    assert conflict(a) == count("e")


# -- RepeatedKeysDictionary -------------------------------------------------------------------

def test_rkd_records_compose(db):
    seed_rkd(db, ("k", "r0"), ("j", "x"))
    a, b = db.new_transaction(), db.new_transaction()
    write(a, lambda r: r.set_at("k", "r1").set_at("k", "r0").remove_record("j", "x"), root="r")
    write(b, lambda r: r.set_at("k", "r2").remove_key("j"), root="r")
    b.commit()
    a.commit()
    assert rkd_content(db) == {"k": {"r0": 2, "r1": 1, "r2": 1}}


def test_rkd_remove_key_is_a_precondition_free_clear(db):
    seed_rkd(db, ("k", "r0"))
    a, b = db.new_transaction(), db.new_transaction()
    write(a, lambda r: r.remove_key("k"), root="r")
    write(b, lambda r: r.set_at("k", "r1"), root="r")
    b.commit()
    a.commit()                          # serial order B, A: the clear removes r1 too
    assert rkd_content(db) == {}


@pytest.mark.parametrize("read", ["get_at", "get", "has", "in"])
def test_rkd_get_at_conflicts_with_record_change_same_key(db, read):
    seed_rkd(db, ("k", "r0"))
    a, b = db.new_transaction(), db.new_transaction()
    r = a.get_root_object("r")
    seen = "k" in r if read == "in" else getattr(r, read)("k")
    assert seen if read in ("has", "in") else dict(seen.items()) == {"r0": 1}
    a.set_root_object("r", r.set_at("j", "y"))
    write(b, lambda r: r.set_at("k", "r1"), root="r")
    b.commit()
    assert conflict(a) == records("k")


def test_rkd_get_at_other_key_no_conflict(db):
    seed_rkd(db, ("k", "r0"))
    a, b = db.new_transaction(), db.new_transaction()
    r = a.get_root_object("r")
    assert dict(r.get_at("k").items()) == {"r0": 1}
    a.set_root_object("r", r.set_at("k", "r2"))
    write(b, lambda r: r.set_at("j", "r1"), root="r")
    b.commit()
    a.commit()
    assert rkd_content(db) == {"k": {"r0": 1, "r2": 1}, "j": {"r1": 1}}


def test_rkd_nested_set_has_no_units(db):
    """R5: the CountedSet a get_at returns is a value: reading it records nothing more."""
    seed_rkd(db, ("k", "r0"))
    tr = db.new_transaction()
    nested = tr.get_root_object("r").get_at("k")
    before = preconditions(tr)
    assert link_of(nested) is None
    assert nested.count_of("r0") == 1 and len(nested) == 1 and list(nested.items()) == [("r0", 1)]
    assert preconditions(tr) == before
    tr.abort()


def test_rkd_get_after_own_clear_records_nothing(db):
    """R9: after remove_key the records of k are the version's own: no precondition."""
    seed_rkd(db, ("k", "r0"))
    a, b = db.new_transaction(), db.new_transaction()
    r = a.get_root_object("r").remove_key("k").set_at("k", "n")
    assert dict(r.get_at("k").items()) == {"n": 1}
    assert records("k") not in preconditions(a)
    a.set_root_object("r", r)
    write(b, lambda r: r.set_at("k", "r1"), root="r")
    b.commit()
    a.commit()
    assert rkd_content(db) == {"k": {"n": 1}}


# -- an unlinked value replaces (R11; replaces the retired log-completeness merge, D33) ---------

UNLINKED = {
    "Set": (lambda tr: tr.new_set().add("a"), lambda cls: cls.from_iterable(["z"])),
    "CountedSet": (lambda tr: tr.new_counted_set().add("a"), lambda cls: cls.from_iterable(["z", "z"])),
    "RepeatedKeysDictionary": (lambda tr: tr.new_repeated_keys_dictionary().set_at("a", 1),
                               lambda cls: cls.from_items([("z", 1)])),
    "Dictionary": (lambda tr: tr.new_dictionary().set_at("a", 1), lambda cls: cls.from_items([("z", 1)])),
    "HashDictionary": (lambda tr: tr.new_hash_dictionary().set_at("a", 1),
                       lambda cls: cls.from_items([("z", 1)])),
}


def _content_of(value):
    if isinstance(value, RepeatedKeysDictionary):
        return {k: dict(cs.items()) for k, cs in value.items()}
    if isinstance(value, Set):
        return set(value.elements())
    return dict(value.items())


@pytest.mark.parametrize("name", sorted(UNLINKED))
@pytest.mark.parametrize("source", ["bulk", "other_root"])
def test_unlinked_collection_is_stored_as_written(db, name, source):
    """A bulk-built collection, or one read from another root, written to an existing root is not
    linked to it: it replaces the root with all its content (nothing is replayed onto the current
    value, so no content its record does not describe is dropped)."""
    fresh, bulk = UNLINKED[name]
    db.run(lambda tr: tr.set_root_object("x", fresh(tr)))
    cls = type(db.run(lambda tr: tr.get_root_object("x")))

    def build(tr):
        tr.set_root_object("src", bulk(cls)) if source == "other_root" else None
    db.run(build)
    expected = _content_of(bulk(cls))
    with db.new_transaction() as tr:
        value = tr.get_root_object("src") if source == "other_root" else bulk(cls)
        tr.set_root_object("x", value)
    assert _content_of(db.run(lambda tr: tr.get_root_object("x"))) == expected


@pytest.mark.parametrize("name", sorted(UNLINKED))
def test_unlinked_collection_conflicts_with_concurrent_change(db, name):
    fresh, bulk = UNLINKED[name]
    db.run(lambda tr: tr.set_root_object("x", fresh(tr)))
    cls = type(db.run(lambda tr: tr.get_root_object("x")))
    a, b = db.new_transaction(), db.new_transaction()
    a.set_root_object("x", fresh(a))                 # built separately: not linked (R11)
    b.set_root_object("x", bulk(cls))
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == Unit(UnitKind.ROOT, "x")


# -- validation before recording (ruling 2) ---------------------------------------------------

@pytest.mark.parametrize("call", [
    lambda c: c.has([1]),
    lambda c: [1] in c,
    lambda c: c.remove([1]),
], ids=["has", "in", "remove"])
def test_set_invalid_element_raises_validation_error(db, call):
    seed_set(db, "a")
    tr = db.new_transaction()
    with pytest.raises(ProtoValidationError):
        call(tr.get_root_object("s"))
    tr.abort()


@pytest.mark.parametrize("call", [lambda c: c.count_of([1]), lambda c: c.has([1])], ids=["count_of", "has"])
def test_counted_set_invalid_element_raises_validation_error(db, call):
    seed_set(db, "a", cls=CountedSet)
    tr = db.new_transaction()
    with pytest.raises(ProtoValidationError):
        call(tr.get_root_object("s"))
    tr.abort()


@pytest.mark.parametrize("call", [lambda c: c.get_at([1]), lambda c: c.has([1])], ids=["get_at", "has"])
def test_rkd_invalid_key_raises_validation_error(db, call):
    seed_rkd(db, ("k", "r0"))
    tr = db.new_transaction()
    with pytest.raises(ProtoValidationError):
        call(tr.get_root_object("r"))
    tr.abort()


@pytest.mark.parametrize("cls", [Dictionary, HashDictionary])
@pytest.mark.parametrize("call", [lambda c: c.get_at([1]), lambda c: c.has([1]), lambda c: c.set_at([1], 1)],
                         ids=["get_at", "has", "set_at"])
def test_dictionary_invalid_key_raises_validation_error(db, cls, call):
    db.run(lambda tr: tr.set_root_object("d", cls(context=tr).set_at("a", 1)))
    tr = db.new_transaction()
    with pytest.raises(ProtoValidationError):
        call(tr.get_root_object("d"))
    tr.abort()


# -- the counter workloads, now through linked roots (D26) ------------------------------------

def test_counted_set_linked_adds_from_many_transactions_compose(db):
    seed_set(db, "seed", cls=CountedSet)
    trs = [db.new_transaction() for _ in range(5)]
    for i, tr in enumerate(trs):
        write(tr, lambda s, i=i: s.add("hit").add(f"t{i}"))
    for tr in reversed(trs):
        tr.commit()
    assert Counter(counts(db)) == Counter({"seed": 1, "hit": 5, **{f"t{i}": 1 for i in range(5)}})
