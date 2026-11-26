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


class _DictionaryOverridingGet(Dictionary):
    __type_name__ = "tests.precondition_dictionary.DictionaryOverridingGet"

    def get(self, key, default=None):
        return default


class _HashDictionaryOverridingGet(HashDictionary):
    __type_name__ = "tests.precondition_dictionary.HashDictionaryOverridingGet"

    def get(self, key, default=None):
        return default


_OVERRIDING_GET = {Dictionary: _DictionaryOverridingGet, HashDictionary: _HashDictionaryOverridingGet}


@CLASSES
def test_get_is_get_at_with_the_same_key_unit(db, cls):
    """Spec section 6 lists ``get(k)`` with ``get_at(k)``: the same KEY unit, the same answer and
    default; a listed operation, so a subclass overriding it is an object root (N5)."""
    from proto_db.collections.linking import fine_grained
    seed(db, cls, k=1, j=1)
    a, b = db.new_transaction(), db.new_transaction()
    d = a.get_root_object("d")
    assert d.get("k") == 1 and d.get("absent") is None and d.get("absent", 7) == 7
    assert key("k") in a._read_log and key("absent") in a._read_log and key("j") not in a._read_log
    a.set_root_object("d", d.set_at("x", d.get("k") + 1))
    write(b, lambda d: d.set_at("j", 2))
    b.commit()
    a.commit()
    assert content(db) == {"k": 1, "j": 2, "x": 2}
    c, e = db.new_transaction(), db.new_transaction()
    d = c.get_root_object("d")
    c.set_root_object("d", d.set_at("y", d.get("k")))
    write(e, lambda d: d.set_at("k", 5))
    e.commit()
    assert conflict(c) == key("k")
    assert not fine_grained(_OVERRIDING_GET[cls])


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


# -- Reapplication keeps the exact key form the transaction's own sequence leaves (R34) -------------
# Fix round 1 of Task 14 (the defect class of 07a8ccd): ``10`` and ``10.0`` are one key of two
# forms.  A concurrent commit of another root forces the reapplication; the expected state is the
# serial one (the concurrent commit, then the transaction's operations).

def _moved(db):
    db.run(lambda tr: tr.set_root_object("other", (tr.get_root_object("other") or 0) + 1))


def _typed_items(db):
    return sorted((type(k).__name__, k, v) for k, v in content(db).items())


@CLASSES
@pytest.mark.parametrize("case", [
    ("remove_then_set", {10: "a", "o": 0}, lambda d: d.remove_at(10).set_at(10.0, "b"),
     [("float", 10.0, "b"), ("str", "o", 0)]),
    ("first_set_creates", {"o": 0}, lambda d: d.set_at(10.0, "a").set_at(10, "b"),
     [("float", 10.0, "b"), ("str", "o", 0)]),
    ("set_remove_set", {"o": 0}, lambda d: d.set_at(10, "a").remove_at(10).set_at(10.0, "b").set_at(10, "c"),
     [("float", 10.0, "c"), ("str", "o", 0)]),
    ("present_set_keeps", {10: "a", "o": 0}, lambda d: d.set_at(10.0, "b"),
     [("int", 10, "b"), ("str", "o", 0)]),
    ("remove_last", {10: "a", "o": 0}, lambda d: d.set_at(10.0, "b").remove_at(10), [("str", "o", 0)]),
], ids=lambda c: c[0] if isinstance(c, tuple) else c)
def test_reapplied_dictionary_keeps_the_exact_key_form(db, cls, case):
    _, base, fn, expected = case

    def build(tr):
        d = cls(context=tr)
        for k, v in base.items():
            d = d.set_at(k, v)
        tr.set_root_object("d", d)
    db.run(build)
    a = db.new_transaction()
    write(a, fn)
    _moved(db)
    a.commit()
    assert _typed_items(db) == expected


# -- R66 escalation, end to end (final review M4) ------------------------------------------------

@CLASSES
@pytest.mark.parametrize("reads", ["at the limit", "past the limit"])
def test_escalation_past_the_limit_makes_the_reads_one_whole_collection_unit(db, cls, reads):
    """R66: up to ``PRECONDITION_ESCALATION_LIMIT`` (1024) fine reads of one linked Dictionary stay
    key units, and a concurrent write of a key not read composes; one more read replaces them by
    ``Unit(WHOLE)``, and the same concurrent write then conflicts on the whole collection."""
    from proto_db.preconditions import PRECONDITION_ESCALATION_LIMIT as LIMIT
    n = LIMIT if reads == "at the limit" else LIMIT + 1
    seed(db, cls, **{f"k{i:04d}": i for i in range(n)}, other=0)
    a, b = db.new_transaction(), db.new_transaction()
    d = a.get_root_object("d")
    for i in range(n):
        assert d.get_at(f"k{i:04d}") == i
    fine = [u for u in a._read_log.preconditions() if u.root == "d" and u.kind is UnitKind.KEY]
    a.set_root_object("w", 1)
    write(b, lambda d: d.set_at("other", 1))
    b.commit()
    if reads == "at the limit":
        assert len(fine) == LIMIT and not a._read_log.whole("d")
        a.commit()
        assert db.run(lambda t: t.get_root_object("w")) == 1
    else:
        assert fine == [] and a._read_log.whole("d")
        assert conflict(a) == whole()
        assert db.run(lambda t: t.get_root_object("w")) is None
