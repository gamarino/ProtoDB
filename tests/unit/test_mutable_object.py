"""MutableObject: named mutable cells of a database (Task 5.3, spec section 7, D11, D18, D35).

Catches: mutables that bypass the read set (lost updates), mutables merged like roots, two
concurrent creators of one name both succeeding, a set that silently recreates a cell
removed concurrently, mutable reads conflicting with root writes (shared namespace), cells
lost across reopen, handles that do not survive the codec, and history without mutables.
"""
import threading
import uuid

import pytest

from proto_db.core import codec
from proto_db.core.atom import AtomPointer
from proto_db.core.dbobject import DBObject, MutableObject
from proto_db.errors import (ProtoConflictError, ProtoCorruptionError, ProtoDBError, ProtoUserError,
                             ProtoValidationError)
from proto_db.storage.file import FileStorage
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from proto_db.values import canonical, exact_token, hash_key, order_key, values_equal


@pytest.fixture(params=["memory", "file"])
def space(request, tmp_path):
    storage = MemoryStorage() if request.param == "memory" else FileStorage(str(tmp_path / "space"))
    s = ObjectSpace(storage)
    yield s
    s.close()


@pytest.fixture
def db(space):
    return space.new_database("db")


def read_mutable(db, name):
    tr = db.new_transaction()
    try:
        return tr.get_mutable(name)
    finally:
        tr.abort()


# -- the handle --------------------------------------------------------------------------------

def test_handle_equality_hash_and_immutability():
    a, b, c = MutableObject("acct"), MutableObject("acct"), MutableObject("other")
    assert a == b and hash(a) == hash(b) and a != c and a.name == "acct"
    assert a != "acct" and len({a, b, c}) == 2
    with pytest.raises((AttributeError, ProtoUserError)):
        a.name = "x"
    for bad in ("", None, 3, b"x"):
        with pytest.raises(ProtoValidationError):
            MutableObject(bad)
    assert "acct" in repr(a)


def test_handle_is_a_canonical_orderable_value():
    """Handles are values: storable, keys ordered after atoms (rank appended, D35), equal by name."""
    h = MutableObject("n")
    assert canonical(h) is h
    assert canonical((h, [h]))[0] is h
    assert order_key(MutableObject("a")) < order_key(MutableObject("b"))
    assert order_key(("z",)) < order_key(h)
    assert values_equal(h, MutableObject("n")) and not values_equal(h, "n")
    assert hash_key(h) == hash_key(MutableObject("n")) != hash_key("n")
    assert exact_token(h) == exact_token(MutableObject("n")) != exact_token("n")


def test_handle_codec_round_trip():
    h = MutableObject("acct")
    assert codec.encode_value(h, None) == {"@mut": "acct"}
    assert codec.decode_value({"@mut": "acct"}, None) == h
    state = codec.decode_state(codec.encode_state({"owner": h, "both": (h, 1)}, None), None)
    assert state["owner"] == h and type(state["owner"]) is MutableObject and state["both"] == (h, 1)


@pytest.mark.parametrize("bad", [{"@mut": 3}, {"@mut": ""}, {"@mut": None}, {"@mut": ["a"]}])
def test_malformed_handle_is_corruption(bad):
    with pytest.raises(ProtoCorruptionError):
        codec.decode_value(bad, None)


def test_handle_resolves_through_a_transaction(db):
    with db.new_transaction() as tr:
        h = tr.new_mutable("acct", 10)
        assert isinstance(h, MutableObject) and h == MutableObject("acct")
        assert h.get(tr) == 10
        h.set(tr, 11)
        assert tr.get_mutable("acct") == 11
    assert read_mutable(db, "acct") == 11


# -- basic operations --------------------------------------------------------------------------

def test_create_get_set_remove(db):
    with db.new_transaction() as tr:
        tr.new_mutable("a", 1)
        tr.new_mutable("b", [1, 2])
        assert tr.mutable_names() == ["a", "b"]
    with db.new_transaction() as tr:
        assert tr.get_mutable("a") == 1 and tr.get_mutable("b") == [1, 2]
        tr.set_mutable("a", 2)
        tr.remove_mutable("b")
        assert tr.get_mutable("b") is None and tr.mutable_names() == ["a"]
    with db.new_transaction() as tr:
        assert tr.get_mutable("a") == 2 and tr.get_mutable("b") is None
        assert tr.mutable_names() == ["a"]


def test_absent_mutable_reads_none(db):
    assert read_mutable(db, "nope") is None


def test_new_mutable_on_existing_name_raises(db):
    with db.new_transaction() as tr:
        tr.new_mutable("a", 1)
        with pytest.raises(ProtoValidationError):
            tr.new_mutable("a", 2)
    tr = db.new_transaction()
    with pytest.raises(ProtoValidationError):
        tr.new_mutable("a", 3)
    tr.abort()


def test_set_and_remove_of_absent_mutable_raise(db):
    tr = db.new_transaction()
    with pytest.raises(ProtoValidationError):
        tr.set_mutable("a", 1)
    with pytest.raises(ProtoValidationError):
        tr.remove_mutable("a")
    tr.abort()


def test_remove_then_new_in_one_transaction(db):
    with db.new_transaction() as tr:
        tr.new_mutable("a", 1)
    with db.new_transaction() as tr:
        tr.remove_mutable("a")
        tr.new_mutable("a", "again")
    assert read_mutable(db, "a") == "again"


def test_values_are_canonical_and_validated(db):
    with db.new_transaction() as tr:
        tr.new_mutable("l", [1, {"k": 2}])
        with pytest.raises(ProtoValidationError):
            tr.new_mutable("bad", object())
        with pytest.raises(ProtoValidationError):
            tr.new_mutable("", 1)
        with pytest.raises(ProtoValidationError):
            tr.get_mutable(3)
    lst = read_mutable(db, "l")
    with pytest.raises(ProtoValidationError):
        lst.append(3)  # stored lists are FrozenList (R13)


def test_atoms_are_stored_by_pointer(db):
    with db.new_transaction() as tr:
        tr.new_mutable("obj", tr.new_object(x=1))
        tr.new_mutable("coll", tr.new_dictionary().set_at("k", "v"))
    obj = read_mutable(db, "obj")
    assert isinstance(obj, DBObject) and obj.x == 1
    assert object.__getattribute__(obj, "atom_pointer") is not None
    assert read_mutable(db, "coll").get_at("k") == "v"


def test_mutables_and_roots_are_separate_namespaces(db):
    with db.new_transaction() as tr:
        tr.set_root_object("x", "root")
        tr.new_mutable("x", "mutable")
        assert tr.root_names() == ["x"] and tr.mutable_names() == ["x"]
    with db.new_transaction() as tr:
        assert tr.get_root_object("x") == "root" and tr.get_mutable("x") == "mutable"
        tr.remove_root_object("x")
    assert read_mutable(db, "x") == "mutable"


def test_ended_transaction_refuses_mutable_calls(db):
    tr = db.new_transaction()
    tr.abort()
    for call in (lambda: tr.get_mutable("a"), lambda: tr.new_mutable("a", 1), lambda: tr.set_mutable("a", 1),
                 lambda: tr.remove_mutable("a"), lambda: tr.mutable_names()):
        with pytest.raises(ProtoUserError):
            call()


def test_only_mutable_writes_publish_a_version(db):
    with db.new_transaction() as tr:
        tr.new_mutable("a", 1)
    assert db.history()[0].version == 1
    tr = db.new_transaction()
    tr.get_mutable("a")
    tr.commit()
    assert db.history()[0].version == 1


def test_written_collection_is_never_merged(db):
    """A mutable holds exactly what was written: a fresh collection replaces, it is not replayed (D35)."""
    with db.new_transaction() as tr:
        tr.new_mutable("d", tr.new_dictionary().set_at("a", 1))
    with db.new_transaction() as tr:
        tr.set_mutable("d", tr.new_dictionary().set_at("b", 2))
    assert dict(read_mutable(db, "d").items()) == {"b": 2}


# -- persistence -------------------------------------------------------------------------------

def test_mutables_survive_reopen(tmp_path):
    path = str(tmp_path / "space")
    with ObjectSpace(FileStorage(path)) as s:
        db = s.new_database("db")
        with db.new_transaction() as tr:
            tr.new_mutable("n", 41)
            tr.new_mutable("obj", tr.new_object(label="x"))
        with db.new_transaction() as tr:
            tr.set_mutable("n", 42)
    with ObjectSpace(FileStorage(path)) as s:
        db = s.open_database("db")
        tr = db.new_transaction()
        assert tr.get_mutable("n") == 42 and tr.get_mutable("obj").label == "x"
        assert tr.mutable_names() == ["n", "obj"]
        tr.abort()


def test_handle_inside_a_dbobject_resolves_after_reload(tmp_path):
    """The entity-identity pattern of D18: versions of a record share a handle to one cell."""
    path = str(tmp_path / "space")
    with ObjectSpace(FileStorage(path)) as s:
        db = s.new_database("db")
        with db.new_transaction() as tr:
            account = tr.new_mutable("account:1", tr.new_object(balance=100))
            tr.set_root_object("order", tr.new_object(item="book", account=account))
    with ObjectSpace(FileStorage(path)) as s:
        db = s.open_database("db")
        with db.new_transaction() as tr:
            handle = tr.get_root_object("order").account
            assert type(handle) is MutableObject and handle == MutableObject("account:1")
            current = handle.get(tr)
            handle.set(tr, current.with_(balance=current.balance - 30))
        tr = db.new_transaction()
        assert tr.get_root_object("order").account.get(tr).balance == 70
        tr.abort()


def test_history_shows_mutable_changes(db):
    with db.new_transaction() as tr:
        tr.new_mutable("m", "v1")
    with db.new_transaction() as tr:
        tr.set_mutable("m", "v2")
    with db.new_transaction() as tr:
        tr.remove_mutable("m")
    history = db.history()
    assert [h.version for h in history] == [3, 2, 1, 0]
    assert [h.mutable_names for h in history] == [(), ("m",), ("m",), ()]
    assert [h.get_mutable("m") for h in history] == [None, "v2", "v1", None]
    assert all(h.root_names == () for h in history)


# -- conflicts ---------------------------------------------------------------------------------

def test_concurrent_read_modify_write_conflicts(space, db):
    with db.new_transaction() as tr:
        tr.new_mutable("n", 0)
    t1, t2 = db.new_transaction(), db.new_transaction()
    t1.set_mutable("n", t1.get_mutable("n") + 1)
    t2.set_mutable("n", t2.get_mutable("n") + 1)
    t1.commit()
    published = space.storage.read_root().epoch
    with pytest.raises(ProtoConflictError):
        t2.commit()
    assert not t2.is_active and space.storage.read_root().epoch == published
    assert read_mutable(db, "n") == 1


def test_exact_token_rewrite_conflicts_for_a_reader(db):
    with db.new_transaction() as tr:
        tr.new_mutable("n", 1)
    t1 = db.new_transaction()
    t1.get_mutable("n")
    with db.new_transaction() as tr:
        tr.set_mutable("n", 1.0)
    t1.set_root_object("other", 1)
    with pytest.raises(ProtoConflictError):
        t1.commit()


def test_blind_writes_last_writer_wins(db):
    with db.new_transaction() as tr:
        tr.new_mutable("n", 0)
    t1, t2 = db.new_transaction(), db.new_transaction()
    t1.set_mutable("n", "first")
    t2.set_mutable("n", "second")
    t1.commit()
    t2.commit()
    assert read_mutable(db, "n") == "second"


def test_concurrent_creators_conflict(db):
    t1, t2 = db.new_transaction(), db.new_transaction()
    t1.new_mutable("x", 1)
    t2.new_mutable("x", 2)
    t1.commit()
    with pytest.raises(ProtoConflictError):
        t2.commit()
    assert read_mutable(db, "x") == 1


def test_blind_set_of_a_concurrently_removed_mutable_conflicts(db):
    """A set never resurrects a cell another transaction removed (D35)."""
    with db.new_transaction() as tr:
        tr.new_mutable("x", 1)
    t1 = db.new_transaction()
    t1.set_mutable("x", 2)
    with db.new_transaction() as tr:
        tr.remove_mutable("x")
    with pytest.raises(ProtoConflictError):
        t1.commit()
    assert read_mutable(db, "x") is None


def test_blind_remove_of_a_concurrently_removed_mutable_is_a_no_op(db):
    with db.new_transaction() as tr:
        tr.new_mutable("x", 1)
    t1 = db.new_transaction()
    t1.remove_mutable("x")
    with db.new_transaction() as tr:
        tr.remove_mutable("x")
    t1.commit()
    assert read_mutable(db, "x") is None


def test_read_then_concurrent_remove_conflicts(db):
    with db.new_transaction() as tr:
        tr.new_mutable("x", 1)
    t1 = db.new_transaction()
    t1.get_mutable("x")
    t1.set_root_object("r", 1)
    with db.new_transaction() as tr:
        tr.remove_mutable("x")
    with pytest.raises(ProtoConflictError):
        t1.commit()


def test_absent_read_then_concurrent_create_conflicts(db):
    t1 = db.new_transaction()
    assert t1.get_mutable("x") is None
    t1.set_root_object("r", 1)
    with db.new_transaction() as tr:
        tr.new_mutable("x", 1)
    with pytest.raises(ProtoConflictError):
        t1.commit()


def test_mutable_reads_do_not_conflict_with_root_writes(db):
    with db.new_transaction() as tr:
        tr.new_mutable("x", 1)
        tr.set_root_object("x", 1)
    t1 = db.new_transaction()
    t1.set_mutable("x", t1.get_mutable("x") + 1)
    with db.new_transaction() as tr:
        tr.set_root_object("x", 100)
    t1.commit()
    assert read_mutable(db, "x") == 2


def test_root_reads_do_not_conflict_with_mutable_writes(db):
    with db.new_transaction() as tr:
        tr.new_mutable("x", 1)
        tr.set_root_object("x", 1)
    t1 = db.new_transaction()
    t1.set_root_object("x", t1.get_root_object("x") + 1)
    with db.new_transaction() as tr:
        tr.set_mutable("x", 100)
    t1.commit()
    tr = db.new_transaction()
    assert tr.get_root_object("x") == 2 and tr.get_mutable("x") == 100
    tr.abort()


def test_read_mutable_atom_check_does_not_load_it(tmp_path, monkeypatch):
    """Atom values in mutables are tokenised by pointer, like roots (D34): the check never loads them."""
    path = str(tmp_path / "space")
    with ObjectSpace(FileStorage(path)) as s:
        db = s.new_database("db")
        with db.new_transaction() as tr:
            tr.new_mutable("l", tr.new_list().append_last(1))
    with ObjectSpace(FileStorage(path)) as s:
        db = s.open_database("db")
        fetched = []
        original = s.storage.get_atom
        monkeypatch.setattr(s.storage, "get_atom", lambda ptr: (fetched.append(ptr), original(ptr))[1])
        tr = db.new_transaction()
        lst = tr.get_mutable("l")
        ptr = object.__getattribute__(lst, "atom_pointer")
        with db.new_transaction() as other:
            other.set_root_object("x", 1)
        tr.set_root_object("y", 2)
        tr.commit()
        assert ptr not in fetched


# -- free-threading ----------------------------------------------------------------------------

def test_sixteen_threads_increment_a_mutable_counter_exactly(space):
    db = space.new_database("counter")
    with db.new_transaction() as tr:
        counter = tr.new_mutable("hits", 0)
    threads, per_thread = 16, 10
    errors = []
    start = threading.Barrier(threads)

    def work():
        try:
            start.wait()
            for _ in range(per_thread):
                db.run(lambda tr: counter.set(tr, counter.get(tr) + 1), retries=2000, backoff=(0, 0.002))
        except ProtoDBError as e:  # pragma: no cover - reported below
            errors.append(e)

    ts = [threading.Thread(target=work) for _ in range(threads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert not errors
    assert read_mutable(db, "hits") == threads * per_thread
    assert db.history()[0].version == threads * per_thread + 1
