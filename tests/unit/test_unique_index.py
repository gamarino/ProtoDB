"""Unique equality indexes (spec R29, B6; section 12 item 7).

``EqualityIndex(name, key_spec, unique=True)`` refuses a second element under one key: against
the transaction's own view at the write (``ProtoValidationError``), against a concurrent commit
at commit (``ProtoConflictError`` on ``Unit(INDEX_GROUP)``), the retry then seeing the duplicate.
MISSING and None never count; ``1`` and ``1.0`` are one key; an unorderable key is refused;
replacing a Dictionary entry by a value with the same key is not a duplicate.  ``unique`` is
persisted in the index definition only when True; an index without it reads as not unique.

Catches: two concurrent inserts of one value both committing, a duplicate accepted in the own
view, a uniqueness check that fails a reapplication only because of the order its intentions
replay in, and a ``unique`` flag lost (or invented) by persistence.
"""
import pytest

from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoConflictError, ProtoValidationError
from proto_db.indexing.equality import EqualityIndex
from proto_db.indexing.keyspec import KeySpec
from proto_db.preconditions import Unit, UnitKind
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace


@pytest.fixture
def storage():
    return MemoryStorage()


@pytest.fixture
def db(storage):
    space = ObjectSpace(storage)
    yield space.new_database("db")
    space.close()


def unique_index(name="u", key_spec=None):
    return EqualityIndex(name, key_spec or KeySpec.element(), unique=True)


def seed(db, **items):
    def build(tr):
        d = tr.new_dictionary()
        for k, v in items.items():
            d = d.set_at(k, v)
        tr.set_root_object("d", d.add_index(unique_index()))
    db.run(build)


def content(db):
    with db.new_transaction() as tr:
        return dict(tr.get_root_object("d").items())


def test_unique_concurrent_same_value_one_commits(db):
    """Section 12 item 7: two transactions insert different keys with the same indexed value."""
    seed(db, a=1)
    a, b = db.new_transaction(), db.new_transaction()
    a.set_root_object("d", a.get_root_object("d").set_at("x", 5))
    b.set_root_object("d", b.get_root_object("d").set_at("y", 5))
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == Unit(UnitKind.INDEX_GROUP, "d", key=5, index="u")
    assert content(db) == {"a": 1, "y": 5}


def test_unique_violation_own_view_is_validation_error(db):
    seed(db, a=5)
    with db.new_transaction() as tr:
        d = tr.get_root_object("d")
        with pytest.raises(ProtoValidationError, match="unique"):
            d.set_at("b", 5)
        with pytest.raises(ProtoValidationError, match="unique"):
            d.set_at("b", 6).set_at("c", 6)
        tr.set_root_object("d", d.set_at("b", 7))
    assert content(db) == {"a": 5, "b": 7}
    # Building a unique index over duplicates is refused too.
    with db.new_transaction() as tr:
        plain = tr.new_dictionary().set_at("a", 1).set_at("b", 1)
        with pytest.raises(ProtoValidationError, match="unique"):
            plain.add_index(unique_index("v"))
        tr.abort()


def test_unique_retry_sees_duplicate_validation_error(db):
    seed(db, a=1)
    attempts = []

    def insert(tr):
        attempts.append(1)
        d = tr.get_root_object("d").set_at("x", 5)
        if len(attempts) == 1:
            # A concurrent commit of the same value, before this attempt commits.
            other = db.new_transaction()
            other.set_root_object("d", other.get_root_object("d").set_at("y", 5))
            other.commit()
        tr.set_root_object("d", d)

    with pytest.raises(ProtoValidationError, match="unique"):
        db.run(insert)
    assert len(attempts) == 2            # the first attempt conflicted; the retry saw the duplicate
    assert content(db) == {"a": 1, "y": 5}


def test_unique_missing_and_none_allowed(db):
    def build(tr):
        d = tr.new_dictionary().add_index(EqualityIndex("by_email", "email", unique=True))
        d = d.set_at("a", DBObject(name="a")).set_at("b", DBObject(name="b"))        # MISSING twice
        d = d.set_at("c", DBObject(email=None)).set_at("e", DBObject(email=None))    # None twice
        d = d.set_at("f", DBObject(email="f@x"))
        tr.set_root_object("d", d)
    db.run(build)
    a, b = db.new_transaction(), db.new_transaction()
    a.set_root_object("d", a.get_root_object("d").set_at("g", DBObject(email=None)))
    b.set_root_object("d", b.get_root_object("d").set_at("h", DBObject(email=None)))
    b.commit()
    a.commit()
    with db.new_transaction() as tr:
        d = tr.get_root_object("d")
        assert len(d) == 7
        d.get_index("by_email").validate(list(d.elements()))
        with pytest.raises(ProtoValidationError, match="unique"):
            d.set_at("z", DBObject(email="f@x"))


def test_unique_int_float_duplicate(db):
    seed(db, a=1)
    with db.new_transaction() as tr:
        d = tr.get_root_object("d")
        with pytest.raises(ProtoValidationError, match="unique"):
            d.set_at("b", 1.0)
        tr.abort()
    a, b = db.new_transaction(), db.new_transaction()
    a.set_root_object("d", a.get_root_object("d").set_at("x", 2))
    b.set_root_object("d", b.get_root_object("d").set_at("y", 2.0))
    b.commit()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit.kind is UnitKind.INDEX_GROUP and e.value.unit.index == "u"


@pytest.mark.parametrize("value", [[1, 2], {"k": 1}, float("nan")], ids=["list", "dict", "nan"])
def test_unique_unorderable_key_refused(db, value):
    seed(db, a=1)
    with db.new_transaction() as tr:
        with pytest.raises(ProtoValidationError, match="unique"):
            tr.get_root_object("d").set_at("b", value)
        with pytest.raises(ProtoValidationError, match="unique"):
            tr.new_dictionary().set_at("b", value).add_index(unique_index("v"))
        tr.abort()


def test_unique_replace_same_dictionary_key_allowed(db):
    seed(db, a=5, b=6)
    with db.new_transaction() as tr:
        tr.set_root_object("d", tr.get_root_object("d").set_at("a", 5).set_at("a", 5.0))
    assert content(db) == {"a": 5.0, "b": 6}
    # Reapplied over a concurrent commit: the replaced entry is the one at the same key.
    a, b = db.new_transaction(), db.new_transaction()
    a.set_root_object("d", a.get_root_object("d").set_at("a", 5))
    b.set_root_object("d", b.get_root_object("d").set_at("c", 7))
    b.commit()
    a.commit()
    assert content(db) == {"a": 5, "b": 6, "c": 7}


def test_unique_checked_on_the_final_reapplied_state(db):
    """A moves the value 5 from key a to key b, then gives a another value.  Replayed in the
    order of last occurrence (set b, set a) the group of 5 holds two entries for a moment; only
    the final state counts, so A commits over an unrelated concurrent change."""
    seed(db, a=5)
    a, b = db.new_transaction(), db.new_transaction()
    a.set_root_object("d", a.get_root_object("d").remove_at("a").set_at("b", 5).set_at("a", 6))
    b.set_root_object("d", b.get_root_object("d").set_at("c", 7))
    b.commit()
    a.commit()
    assert content(db) == {"a": 6, "b": 5, "c": 7}
    with db.new_transaction() as tr:
        d = tr.get_root_object("d")
        d.get_index("u").validate(list(d.elements()))


def test_unique_attribute_persisted_and_old_index_reads_false(storage, db):
    plain = EqualityIndex("p", KeySpec.element())
    assert plain.unique is False and "unique" not in plain._public_state()
    assert unique_index().unique is True and unique_index()._public_state()["unique"] is True
    assert dict(plain.definition_data()[1])["unique"] is False
    with pytest.raises(ProtoValidationError):
        EqualityIndex("x", KeySpec.element(), unique=1)

    def build(tr):
        d = tr.new_dictionary().set_at("a", 1).add_index(unique_index()).add_index(plain)
        tr.set_root_object("d", d)
    db.run(build)
    # A fresh space over the same storage decodes both indexes from their records.
    other = ObjectSpace(storage)
    try:
        with other.open_database("db").new_transaction() as tr:
            d = tr.get_root_object("d")
            assert d.get_index("u").unique is True and d.get_index("u").explain()["unique"] is True
            assert d.get_index("p").unique is False and "unique" not in d.get_index("p").explain()
            assert d.get_index("p").empty().unique is False and d.get_index("u").empty().unique is True
            with pytest.raises(ProtoValidationError, match="unique"):
                d.set_at("b", 1)
            tr.set_root_object("d", d.set_at("b", 2))
    finally:
        other.close()
