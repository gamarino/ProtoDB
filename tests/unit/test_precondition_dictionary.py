"""Dictionary and HashDictionary under the precondition model (spec sections 5 and 6, D51, R17).

One pair per row of the Dictionary/HashDictionary tables, for both classes: the operation against
a concurrent change of the same key (conflict, naming the unit) and of another key (both
published).  Interleavings are explicit: open A, open B, commit B, commit A.
"""
import pytest

from proto_db.collections import avl
from proto_db.collections.dictionaries import Dictionary
from proto_db.collections.hash_dictionaries import HashDictionary
from proto_db.errors import ProtoConflictError
from proto_db.preconditions import Unit, UnitKind
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from tests.support.counting_storage import CountingStorage
from tests.support.modes import db_any_mode, mode, space_any_mode  # noqa: F401 -- fixtures

CLASSES = pytest.mark.parametrize("cls", [Dictionary, HashDictionary], ids=["Dictionary", "HashDictionary"])


@pytest.fixture
def storage():
    return CountingStorage(MemoryStorage())


@pytest.fixture
def standalone_db(storage):
    """A standalone database over a counting storage, for the test that counts storage reads."""
    s = ObjectSpace(storage)
    yield s.new_database("db")
    s.close()


@pytest.fixture
def db(db_any_mode):
    """The database ``db`` in each commit mode: standalone, group commit, cluster (spec section 12,
    Task 14; :mod:`tests.support.modes`)."""
    return db_any_mode


def key(k, root="d"):
    return Unit(UnitKind.KEY, root, key=k)


def whole(root="d"):
    return Unit(UnitKind.WHOLE, root)


def seed(db, cls, **items):
    def build(tr):
        d = cls(context=tr)
        for k, v in items.items():
            d = d.set_at(k, v)
        tr.set_root_object("d", d)
    db.run(build)


def content(db):
    return dict(db.run(lambda tr: tr.get_root_object("d")).items())


def write(tr, fn):
    tr.set_root_object("d", fn(tr.get_root_object("d")))


def conflict(tr):
    with pytest.raises(ProtoConflictError) as e:
        tr.commit()
    assert e.value.database == "db"
    return e.value.unit


# -- section 5: writes ----------------------------------------------------------------------

@CLASSES
def test_set_at_other_key_no_conflict_both_present(db, cls):
    seed(db, cls, a=0)
    a, b = db.new_transaction(), db.new_transaction()
    write(a, lambda d: d.set_at("k", 1))
    write(b, lambda d: d.set_at("j", 2))
    b.commit()
    a.commit()
    assert content(db) == {"a": 0, "k": 1, "j": 2}


@CLASSES
def test_set_at_same_key_conflicts(db, cls):
    seed(db, cls, k=0)
    a, b = db.new_transaction(), db.new_transaction()
    write(a, lambda d: d.set_at("k", 1))
    write(b, lambda d: d.set_at("k", 2))
    b.commit()
    assert conflict(a) == key("k")
    assert content(db) == {"k": 2}


@CLASSES
def test_insert_same_new_key_conflicts_even_same_value(db, cls):
    seed(db, cls, a=0)
    a, b = db.new_transaction(), db.new_transaction()
    write(a, lambda d: d.set_at("k", 1))
    write(b, lambda d: d.set_at("k", 1))
    b.commit()
    assert conflict(a) == key("k")


@CLASSES
def test_remove_at_other_key_no_conflict(db, cls):
    seed(db, cls, k=0, j=0)
    a, b = db.new_transaction(), db.new_transaction()
    write(a, lambda d: d.remove_at("k"))
    write(b, lambda d: d.set_at("j", 5))
    b.commit()
    a.commit()
    assert content(db) == {"j": 5}


@CLASSES
def test_remove_at_same_key_conflicts(db, cls):
    seed(db, cls, k=0)
    a, b = db.new_transaction(), db.new_transaction()
    write(a, lambda d: d.remove_at("k"))
    write(b, lambda d: d.set_at("k", 5))
    b.commit()
    assert conflict(a) == key("k")
    assert content(db) == {"k": 5}


@CLASSES
def test_remove_at_absent_key_conflicts_with_concurrent_insert(db, cls):
    """``remove_at`` of an absent key records "still absent" (absent included)."""
    seed(db, cls, a=0)
    a, b = db.new_transaction(), db.new_transaction()
    write(a, lambda d: d.remove_at("k"))
    write(b, lambda d: d.set_at("k", 5))
    b.commit()
    assert conflict(a) == key("k")


# -- section 6: reads -----------------------------------------------------------------------

@CLASSES
def test_get_at_then_other_key_change_no_conflict(db, cls):
    seed(db, cls, k=1, j=1)
    a, b = db.new_transaction(), db.new_transaction()
    d = a.get_root_object("d")
    a.set_root_object("d", d.set_at("x", d.get_at("k") + 1))
    write(b, lambda d: d.set_at("j", 2))
    b.commit()
    a.commit()
    assert content(db) == {"k": 1, "j": 2, "x": 2}


@CLASSES
def test_get_at_same_key_change_conflicts(db, cls):
    seed(db, cls, k=1)
    a, b = db.new_transaction(), db.new_transaction()
    d = a.get_root_object("d")
    a.set_root_object("d", d.set_at("x", d.get_at("k") + 1))
    write(b, lambda d: d.set_at("k", 2))
    b.commit()
    assert conflict(a) == key("k")


@CLASSES
def test_get_at_absent_then_concurrent_insert_conflicts(db, cls):
    seed(db, cls, a=0)
    a, b = db.new_transaction(), db.new_transaction()
    d = a.get_root_object("d")
    assert d.get_at("k") is None
    a.set_root_object("d", d.set_at("x", 1))
    write(b, lambda d: d.set_at("k", 2))
    b.commit()
    assert conflict(a) == key("k")


@CLASSES
@pytest.mark.parametrize("probe", ["has", "in"])
def test_has_same_pair(db, cls, probe):
    """``has(k)`` / ``k in d``: a concurrent change of k conflicts, of another key does not."""
    seed(db, cls, k=1, j=1)

    def check(d, k):
        return d.has(k) if probe == "has" else k in d

    a, b = db.new_transaction(), db.new_transaction()
    d = a.get_root_object("d")
    assert check(d, "k") and not check(d, "absent")
    a.set_root_object("d", d.set_at("x", 1))
    write(b, lambda d: d.set_at("j", 2))
    b.commit()
    a.commit()                              # another key: no conflict
    a, b = db.new_transaction(), db.new_transaction()
    d = a.get_root_object("d")
    assert not check(d, "absent")
    a.set_root_object("d", d.set_at("y", 1))
    write(b, lambda d: d.set_at("absent", 2))
    b.commit()
    assert conflict(a) == key("absent")


@CLASSES
@pytest.mark.parametrize("read", ["items", "keys", "values", "elements", "iter", "len", "count",
                                  "content_equals"])
def test_iteration_conflicts_with_any_change(db, cls, read):
    seed(db, cls, k=1, j=1)
    readers = {"items": lambda d: list(d.items()), "keys": lambda d: list(d.keys()),
               "values": lambda d: list(d.values()), "elements": lambda d: list(d.elements()),
               "iter": lambda d: list(d), "len": len, "count": lambda d: d.count,
               "content_equals": lambda d: d.content_equals(type(d)())}
    a, b = db.new_transaction(), db.new_transaction()
    d = a.get_root_object("d")
    readers[read](d)
    assert whole() in a._read_log
    a.set_root_object("d", d.set_at("x", 1))
    write(b, lambda d: d.set_at("other", 2))
    b.commit()
    assert conflict(a) == whole()
    assert content(db) == {"k": 1, "j": 1, "other": 2}


@pytest.mark.parametrize("read", ["key_at_position", "position_of"])
def test_key_at_position_conflicts_with_any_change(db, read):
    """R17 (Dictionary only: a HashDictionary has no key order): positional reads take the whole
    collection."""
    seed(db, Dictionary, a=1, b=2, c=3)
    a, b = db.new_transaction(), db.new_transaction()
    d = a.get_root_object("d")
    if read == "key_at_position":
        assert [d.key_at_position(i) for i in range(3)] == ["a", "b", "c"]
        assert d.key_at_position(-1) == "c"
    else:
        assert [d.position_of(k) for k in "abc"] == [0, 1, 2]
    assert whole() in a._read_log
    a.set_root_object("d", d.set_at("x", 1))
    write(b, lambda d: d.set_at("zz", 2))
    b.commit()
    assert conflict(a) == whole()


@CLASSES
def test_boxed_value_compared_by_box_pointer(standalone_db, storage, cls):
    """D51: a key holding a large value is compared by its stored form (a Dictionary keeps it in a
    ValueBox, compared by pointer; a HashDictionary keeps it inline in its bucket), so validating
    the precondition never reads the value.  Standalone only: it counts the storage's reads."""
    db = standalone_db
    large = "x" * (avl.VALUE_BOX_MIN * 8)
    seed(db, cls, big=large, j=0)
    probe = db.new_transaction()
    stored = probe.get_root_object("d")._stored_at("big")
    probe.abort()
    a, b = db.new_transaction(), db.new_transaction()
    d = a.get_root_object("d")
    assert d.has("big")
    a.set_root_object("d", d.set_at("x", 2))
    write(b, lambda d: d.set_at("j", 2))
    b.commit()
    if cls is Dictionary:
        assert type(stored) is avl.ValueBox
        assert a._read_log.preconditions()[key("big")] == ("@atom", stored.atom_pointer)
    else:
        assert type(stored) is str           # inline: its token is its exact content
    storage.reset()
    a.commit()
    if cls is Dictionary:
        assert stored.atom_pointer not in storage.pointers_read()
    assert content(db) == {"big": large, "j": 2, "x": 2}
