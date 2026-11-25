"""Object roots and root names under the precondition model (spec sections 4-6: R1, R2, R7, R9, R11,
R12, R36, R37; section 5 "Roots and mutables").

Every root write compares against the root's value in the transaction's base: a blind write no
longer lets the last writer win, and a collection assigned with ``set_root_object`` that is not
linked to that root of the transaction (R11, R46) replaces the root, comparing against the base,
instead of being merged.  Interleavings are explicit: open A, open B, commit B, commit A.
"""
import pytest

from proto_db.errors import ProtoConflictError
from proto_db.preconditions import ABSENT, Unit, UnitKind
from proto_db.registry import BASE
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from proto_db.values import exact_token

_PACKAGE = "tests.unit.registry_modules"
MAPPING = {name: f"{_PACKAGE}.{name}" for name in ("reg7_a", "reg7_b")}


@pytest.fixture
def space():
    s = ObjectSpace(MemoryStorage(), modules_mapping=MAPPING)
    yield s
    s.close()


@pytest.fixture
def db(space):
    return space.new_database("db")


def root(name):
    return Unit(UnitKind.ROOT, name)


def read(db, name):
    return db.run(lambda tr: tr.get_root_object(name))


def test_object_root_write_conflicts_with_concurrent_write(db):
    db.run(lambda tr: tr.set_root_object("o", tr.new_object(v=0)))
    a, b = db.new_transaction(), db.new_transaction()
    a.set_root_object("o", a.new_object(v=1))
    b.set_root_object("o", b.new_object(v=2))
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == root("o") and e.value.database == "db"
    assert read(db, "o").v == 2


def test_object_roots_different_names_no_conflict(db):
    db.run(lambda tr: (tr.set_root_object("o1", 0), tr.set_root_object("o2", 0)))
    a, b = db.new_transaction(), db.new_transaction()
    a.set_root_object("o1", 1)
    b.set_root_object("o2", 2)
    b.commit()
    a.commit()
    assert read(db, "o1") == 1 and read(db, "o2") == 2


def test_object_root_attribute_writes_conflict(db):
    """R2: no per-attribute merge; two writers of different attributes of one object conflict."""
    db.run(lambda tr: tr.set_root_object("o", tr.new_object(x=0, y=0)))
    a, b = db.new_transaction(), db.new_transaction()
    a.set_root_object("o", a.get_root_object("o").with_(x=1))
    b.set_root_object("o", b.get_root_object("o").with_(y=1))
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == root("o")
    o = read(db, "o")
    assert (o.x, o.y) == (0, 1)


def test_absent_root_create_conflicts_with_concurrent_create(db):
    a, b = db.new_transaction(), db.new_transaction()
    a.set_root_object("n", 1)            # blind: never read, still compared with the base (absent)
    b.set_root_object("n", 2)
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == root("n")
    assert read(db, "n") == 2


def test_remove_root_conflicts_with_concurrent_write(db):
    db.run(lambda tr: tr.set_root_object("x", 1))
    a, b = db.new_transaction(), db.new_transaction()
    a.remove_root_object("x")
    b.set_root_object("x", 2)
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == root("x")
    assert read(db, "x") == 2


def test_remove_of_an_absent_root_conflicts_with_a_concurrent_create(db):
    a, b = db.new_transaction(), db.new_transaction()
    a.remove_root_object("x")            # absent in the base: the precondition is "still absent"
    b.set_root_object("x", 2)
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == root("x")


def test_root_names_conflicts_on_concurrent_create(db):
    db.run(lambda tr: tr.set_root_object("a", 1))
    a, b = db.new_transaction(), db.new_transaction()
    assert a.root_names() == ["a"]
    a.set_root_object("count", len(a.root_names()))
    b.set_root_object("z", 1)
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == Unit(UnitKind.ROOT_NAMES) and e.value.database == "db"


def test_root_names_holds_when_only_values_change(db):
    db.run(lambda tr: tr.set_root_object("a", 1))
    a, b = db.new_transaction(), db.new_transaction()
    a.root_names()
    a.set_root_object("b", 1)
    b.set_root_object("a", 2)            # a value change, not a name change
    b.commit()
    a.commit()
    assert read(db, "a") == 2 and read(db, "b") == 1


def test_non_mergeable_collection_replaces_and_compares_base(db):
    """A bulk-built collection (not linked, R11) replaces the root, compared against the base
    (section 5)."""
    from proto_db.collections.dictionaries import Dictionary
    db.run(lambda tr: tr.set_root_object("d", tr.new_dictionary().set_at("a", 1)))
    a, b = db.new_transaction(), db.new_transaction()
    a.set_root_object("d", Dictionary.from_items([("x", 1)], context=a))
    b.set_root_object("d", b.get_root_object("d").set_at("b", 2))
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == root("d")
    assert dict(read(db, "d").items()) == {"a": 1, "b": 2}


def test_collection_over_a_concurrently_removed_root_conflicts(db):
    """An unlinked collection replaces the root, and its precondition (the base value) applies,
    checked at commit (a Dictionary is never merged: R11)."""
    db.run(lambda tr: tr.set_root_object("d", tr.new_dictionary().set_at("a", 1)))
    a = db.new_transaction()
    a.set_root_object("d", a.new_dictionary().set_at("x", 1))
    db.run(lambda tr: tr.remove_root_object("d"))
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == root("d") and e.value.database == "db"
    assert read(db, "d") is None


def test_unlinked_collection_replaces_and_compares_base(db):
    db.run(lambda tr: tr.set_root_object("d", tr.new_dictionary().set_at("a", 1)))
    a, b = db.new_transaction(), db.new_transaction()
    a.set_root_object("d", a.new_dictionary().set_at("x", 1))      # built separately: unlinked (R11)
    b.set_root_object("d", b.get_root_object("d").set_at("b", 2))
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()                       # no merge: compared against the base value of "d"
    assert e.value.unit == root("d")
    assert dict(read(db, "d").items()) == {"a": 1, "b": 2}
    with db.new_transaction() as tr:     # without a concurrent change it replaces the root
        tr.set_root_object("d", tr.new_dictionary().set_at("x", 1))
    assert dict(read(db, "d").items()) == {"x": 1}


def test_set_root_object_rejects_replace_keyword(db):
    """Section 10: ``replace=`` is removed; assigning an unlinked value always replaces, compared
    against the base."""
    tr = db.new_transaction()
    with pytest.raises(TypeError):
        tr.set_root_object("x", 1, replace=True)
    tr.abort()


def test_class_change_conflicts(db):
    """R12: a collection root read by A and replaced by another class conflicts at A's commit."""
    db.run(lambda tr: tr.set_root_object("c", tr.new_dictionary().set_at("k", 1)))
    a, b = db.new_transaction(), db.new_transaction()
    assert a.get_root_object("c").get_at("k") == 1
    b.set_root_object("c", b.new_set().add(1))
    b.commit()
    a.new_mutable("m", 1)
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == Unit(UnitKind.ROOT_PRESENCE, "c") and e.value.database == "db"


@pytest.mark.parametrize("plain", [5, "s"])
def test_class_change_to_a_plain_value_conflicts(db, plain):
    """R12, R36: a collection root replaced by a plain value is a conflict, not an AttributeError."""
    db.run(lambda tr: tr.set_root_object("c", tr.new_dictionary().set_at("k", 1)))
    a, b = db.new_transaction(), db.new_transaction()
    a.get_root_object("c")
    b.set_root_object("c", plain)
    b.commit()
    a.new_mutable("m", 1)
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == Unit(UnitKind.ROOT_PRESENCE, "c") and e.value.database == "db"


def test_blind_object_root_writes_conflict(db):
    """R2 without a read: A and B write different attributes of one object root without reading it
    (each builds a new object); the write alone declares the precondition, and the second conflicts."""
    db.run(lambda tr: tr.set_root_object("o", tr.new_object(x=0, y=0)))
    a, b = db.new_transaction(), db.new_transaction()
    a.set_root_object("o", a.new_object(x=1, y=0))
    b.set_root_object("o", b.new_object(x=0, y=1))
    assert set(a._read_log.preconditions()) == {root("o")}         # declared by the write alone
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == root("o")
    o = read(db, "o")
    assert (o.x, o.y) == (0, 1)


def test_removed_collection_root_conflicts_with_its_reader(db):
    db.run(lambda tr: tr.set_root_object("c", tr.new_set().add(1)))
    a, b = db.new_transaction(), db.new_transaction()
    a.get_root_object("c")
    b.remove_root_object("c")
    b.commit()
    a.new_mutable("m", 1)
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == Unit(UnitKind.ROOT_PRESENCE, "c")


def test_reads_record_their_units(db):
    db.run(lambda tr: (tr.set_root_object("o", tr.new_object(v=1)), tr.set_root_object("plain", 3),
                       tr.set_root_object("c", tr.new_dictionary()), tr.set_root_object("s", tr.new_set()),
                       tr.set_root_object("l", tr.new_list())))
    tr = db.new_transaction()
    o, c, s = tr.get_root_object("o"), tr.get_root_object("c"), tr.get_root_object("s")
    tr.get_root_object("plain")
    tr.get_root_object("missing")
    tr.root_names()
    pre = tr._read_log.preconditions()
    assert pre[root("o")] == ("@atom", o.atom_pointer)                # atoms by storage pointer (R6)
    assert pre[root("plain")] == exact_token(3)
    assert pre[root("missing")] is ABSENT
    assert pre[Unit(UnitKind.ROOT_PRESENCE, "c")] == type(c).__type_name__
    # A linked collection (a Dictionary, a Set) records its content through its own reads (section 9).
    assert Unit(UnitKind.WHOLE, "c") not in pre
    assert root("c") not in pre
    assert pre[Unit(UnitKind.ROOT_PRESENCE, "s")] == type(s).__type_name__
    assert Unit(UnitKind.WHOLE, "s") not in pre
    # A List is linked too (Task 7): reading the root records presence and class only.
    lst = tr.get_root_object("l")
    pre = tr._read_log.preconditions()
    assert pre[Unit(UnitKind.ROOT_PRESENCE, "l")] == type(lst).__type_name__
    assert Unit(UnitKind.WHOLE, "l") not in pre
    assert Unit(UnitKind.ROOT_NAMES) in pre
    tr.abort()


def test_reading_a_root_it_wrote_records_nothing(db):
    tr = db.new_transaction()
    tr.set_root_object("x", 1)
    before = tr._read_log.preconditions()
    assert before == {root("x"): ABSENT}
    assert tr.get_root_object("x") == 1
    tr.set_root_object("x", 2)
    assert tr._read_log.preconditions() == before                   # R9
    tr.abort()


def test_object_root_aba_by_value_holds(db):
    """R7: the root changed and came back to the base value: the precondition holds."""
    db.run(lambda tr: tr.set_root_object("x", 1))
    a = db.new_transaction()
    a.set_root_object("x", 10)
    db.run(lambda tr: tr.set_root_object("x", 2))
    db.run(lambda tr: tr.set_root_object("x", 1))
    a.commit()
    assert read(db, "x") == 10


def test_reserved_roots_keep_their_merge(db):
    """R37: the registry roots keep their add-if-absent merge, outside the model."""
    first, second = db.new_transaction(), db.new_transaction()
    first.install_module("reg7_a")
    second.install_module("reg7_b")
    first.commit()
    second.commit()                      # no conflict: merged against the current root
    with db.new_transaction() as tr:
        assert tr.installed_modules() == (BASE, "reg7_a", "reg7_b")
