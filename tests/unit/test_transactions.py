"""ObjectSpace / Database / Transaction (Task 5.1, spec section 7, D9, D32-D34).

Catches: lost updates (read set ignoring absent roots), merges applied to roots the
transaction read, merges that drop keys or ignore blind writes (the 0.1 merge was tuned
to pass tests), anything published after a conflict, commits that raise after the root
is published, unknown publish outcomes reported wrongly, transactions usable after they
end, and retries of non-conflict errors.
"""
import datetime
import threading
import time
from decimal import Decimal

import pytest

from proto_db.collections.dictionaries import Dictionary
from proto_db.collections.lists import List
from proto_db.collections.sets import CountedSet, Set
from proto_db.core.dbobject import DBObject
from proto_db.errors import (ProtoConflictError, ProtoDBError, ProtoLockingError, ProtoTimeoutError,
                             ProtoUnexpectedError, ProtoUserError, ProtoValidationError)
from proto_db.storage.file import FileStorage
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import Database, ObjectSpace, Transaction
from tests.support.fault_storage import PublishFaultStorage
from tests.support.faulty_fs import FaultPlan, FaultyFS


@pytest.fixture(params=["memory", "file"])
def space(request, tmp_path):
    storage = MemoryStorage() if request.param == "memory" else FileStorage(str(tmp_path / "space"))
    s = ObjectSpace(storage)
    yield s
    s.close()


@pytest.fixture
def db(space):
    return space.new_database("db")


def read_root(db, name):
    tr = db.new_transaction()
    try:
        return tr.get_root_object(name)
    finally:
        tr.abort()


def epoch(space):
    root = space.storage.read_root()
    return root.epoch if root else 0


# -- errors ----------------------------------------------------------------------------------

def test_conflict_error_is_a_locking_error():
    assert issubclass(ProtoConflictError, ProtoLockingError)


# -- F0 merge tests, ported (8 = 4 tests x memory/file) ---------------------------------------

class TestDictionaryCommitMerge:
    """Committing a Dictionary root stores exactly what the transaction wrote."""

    @pytest.fixture(autouse=True)
    def _setup(self, space):
        self.db = space.new_database("MergeDB")
        tr = self.db.new_transaction()
        d = tr.new_dictionary()
        d = d.set_at("a", 1).set_at("b", 5).set_at("flag", False).set_at("gone", 7).set_at("s", "x")
        tr.set_root_object("d", d)
        tr.commit()

    def _update(self, fn):
        tr = self.db.new_transaction()
        d = fn(tr.get_root_object("d"))
        tr.set_root_object("d", d)
        tr.commit()

    def _read(self, key):
        return read_root(self.db, "d").get_at(key)

    def test_untouched_keys_keep_their_values(self):
        for i in range(3):
            self._update(lambda d, i=i: d.set_at("s", f"y{i}"))
        assert self._read("a") == 1
        assert self._read("b") == 5
        assert self._read("flag") is False

    def test_integer_write_stores_the_written_value(self):
        self._update(lambda d: d.set_at("a", 0))
        assert self._read("a") == 0
        self._update(lambda d: d.set_at("b", 70))
        assert self._read("b") == 70

    def test_removal_is_persisted(self):
        self._update(lambda d: d.remove_at("gone"))
        assert self._read("gone") is None
        assert self._read("a") == 1

    def test_blind_write_merges_only_its_own_keys(self):
        self._update(lambda d: d.set_at("a", 10))
        tr = self.db.new_transaction()
        tr.set_root_object("d", tr.new_dictionary().set_at("c", 3))
        tr.commit()
        assert self._read("a") == 10
        assert self._read("b") == 5
        assert self._read("c") == 3


# -- space and databases ---------------------------------------------------------------------

def test_databases_are_created_listed_and_opened(space):
    assert space.list_databases() == []
    space.new_database("b")
    space.new_database("a")
    assert space.list_databases() == ["a", "b"]
    assert isinstance(space.open_database("a"), Database)
    with pytest.raises(ProtoValidationError):
        space.new_database("a")
    with pytest.raises(ProtoValidationError):
        space.open_database("missing")
    with pytest.raises(ProtoValidationError):
        space.new_database("")


def test_databases_are_independent(space):
    a, b = space.new_database("a"), space.new_database("b")
    with a.new_transaction() as tr:
        tr.set_root_object("x", 1)
    with b.new_transaction() as tr:
        tr.set_root_object("x", 2)
    assert read_root(a, "x") == 1 and read_root(b, "x") == 2


def test_commit_on_another_database_is_not_a_conflict(space):
    a, b = space.new_database("a"), space.new_database("b")
    tr = a.new_transaction()
    assert tr.get_root_object("x") is None
    with b.new_transaction() as other:
        other.set_root_object("x", 2)
    tr.set_root_object("x", 1)
    tr.commit()
    assert read_root(a, "x") == 1


def test_closed_space_refuses_work(space):
    db = space.new_database("db")
    space.close()
    space.close()
    with pytest.raises(ProtoValidationError):
        db.new_transaction()
    with pytest.raises(ProtoValidationError):
        space.list_databases()


# -- transaction basics ----------------------------------------------------------------------

def test_roots_round_trip_with_every_factory(db):
    with db.new_transaction() as tr:
        tr.set_root_object("list", tr.new_list().append_last(1).append_last(2))
        tr.set_root_object("dict", tr.new_dictionary().set_at("k", "v"))
        tr.set_root_object("hdict", tr.new_hash_dictionary().set_at(("t", 1), 2))
        tr.set_root_object("set", tr.new_set().add("x"))
        tr.set_root_object("cset", tr.new_counted_set().add("x").add("x"))
        tr.set_root_object("rkd", tr.new_repeated_keys_dictionary().set_at("k", 1).set_at("k", 2))
        tr.set_root_object("obj", tr.new_object(name="ann", age=3))
        tr.set_root_object("int", 42)
    tr = db.new_transaction()
    assert list(tr.get_root_object("list")) == [1, 2]
    assert tr.get_root_object("dict").get_at("k") == "v"
    assert tr.get_root_object("hdict").get_at(("t", 1)) == 2
    assert tr.get_root_object("set").has("x")
    assert tr.get_root_object("cset").count_of("x") == 2
    assert sorted(tr.get_root_object("rkd").get_at("k").elements()) == [1, 2]
    obj = tr.get_root_object("obj")
    assert isinstance(obj, DBObject) and obj.name == "ann" and obj.age == 3
    assert tr.get_root_object("int") == 42
    assert tr.root_names() == ["cset", "dict", "hdict", "int", "list", "obj", "rkd", "set"]
    tr.abort()


def test_atoms_created_in_a_transaction_carry_it_as_context(db):
    tr = db.new_transaction()
    assert isinstance(tr.new_list(), List) and tr.new_list().context is tr
    assert tr.new_object(a=1).context is tr
    assert tr.storage is db.space.storage
    tr.abort()


def test_snapshot_isolation(db):
    with db.new_transaction() as tr:
        tr.set_root_object("x", 1)
    reader = db.new_transaction()
    with db.new_transaction() as tr:
        tr.set_root_object("x", 2)
        tr.set_root_object("y", 3)
    assert reader.get_root_object("x") == 1
    assert reader.get_root_object("y") is None
    assert reader.root_names() == ["x"]
    reader.abort()


def test_read_your_writes_and_remove(db):
    with db.new_transaction() as tr:
        tr.set_root_object("x", 1)
        tr.set_root_object("y", 2)
    tr = db.new_transaction()
    tr.set_root_object("x", 10)
    tr.remove_root_object("y")
    tr.set_root_object("z", 5)
    assert tr.get_root_object("x") == 10 and tr.get_root_object("y") is None
    assert tr.root_names() == ["x", "z"]
    tr.commit()
    assert read_root(db, "y") is None and read_root(db, "x") == 10 and read_root(db, "z") == 5


def test_invalid_root_names_and_values(db):
    tr = db.new_transaction()
    for bad in ("", 3, None):
        with pytest.raises(ProtoValidationError):
            tr.set_root_object(bad, 1)
        with pytest.raises(ProtoValidationError):
            tr.get_root_object(bad)
    with pytest.raises(ProtoValidationError):
        tr.set_root_object("x", object())
    tr.abort()


def test_read_only_transaction_publishes_nothing(space, db):
    with db.new_transaction() as tr:
        tr.set_root_object("x", 1)
    before = space.storage.read_root()
    tr = db.new_transaction()
    assert tr.get_root_object("x") == 1
    tr.commit()
    assert space.storage.read_root() == before


def test_transaction_unusable_after_commit_or_abort(db):
    for end in ("commit", "abort"):
        tr = db.new_transaction()
        getattr(tr, end)()
        assert not tr.is_active
        for call in (lambda: tr.get_root_object("x"), lambda: tr.set_root_object("x", 1),
                     lambda: tr.remove_root_object("x"), tr.root_names, tr.commit, tr.abort,
                     tr.new_list, tr.new_dictionary, lambda: tr.new_object(a=1)):
            with pytest.raises(ProtoUserError):
                call()


def test_failed_commit_leaves_the_transaction_aborted(db):
    with db.new_transaction() as tr:
        tr.set_root_object("x", 1)
    tr = db.new_transaction()
    tr.get_root_object("x")
    with db.new_transaction() as other:
        other.set_root_object("x", 2)
    tr.set_root_object("x", 3)
    with pytest.raises(ProtoConflictError):
        tr.commit()
    assert not tr.is_active
    with pytest.raises(ProtoUserError):
        tr.commit()


def test_context_manager_commits_on_success_and_aborts_on_exception(space, db):
    with db.new_transaction() as tr:
        tr.set_root_object("x", 1)
    assert not tr.is_active and read_root(db, "x") == 1
    before = epoch(space)
    with pytest.raises(KeyError):
        with db.new_transaction() as tr:
            tr.set_root_object("x", 2)
            raise KeyError("boom")
    assert not tr.is_active and read_root(db, "x") == 1 and epoch(space) == before


def test_context_manager_tolerates_explicit_end(db):
    with db.new_transaction() as tr:
        tr.set_root_object("x", 1)
        tr.commit()
    with db.new_transaction() as tr:
        tr.abort()
    assert read_root(db, "x") == 1


def test_atom_from_another_space_is_rejected(db):
    other = ObjectSpace(MemoryStorage())
    try:
        tr_other = other.new_database("o").new_transaction()
        foreign = tr_other.new_list().append_last(1)
        tr = db.new_transaction()
        tr.set_root_object("x", foreign)
        with pytest.raises(ProtoUserError):
            tr.commit()
        assert not tr.is_active
        tr_other.abort()
    finally:
        other.close()


def test_values_survive_the_transaction_that_loaded_them(db):
    with db.new_transaction() as tr:
        tr.set_root_object("d", tr.new_dictionary().set_at("a", tr.new_list().append_last(1)))
    tr1 = db.new_transaction()
    d = tr1.get_root_object("d")
    tr1.abort()
    # Loaded after the end of tr1: lazy loads through an ended transaction still work.
    assert list(d.get_at("a")) == [1]
    tr2 = db.new_transaction()
    tr2.set_root_object("copy", d.set_at("b", 2))
    tr2.commit()
    copy = read_root(db, "copy")
    assert list(copy.get_at("a")) == [1] and copy.get_at("b") == 2


# -- read set and conflicts ------------------------------------------------------------------

def test_two_creators_of_the_same_absent_root(space, db):
    tr1, tr2 = db.new_transaction(), db.new_transaction()
    assert tr1.get_root_object("x") is None and tr2.get_root_object("x") is None
    tr1.set_root_object("x", tr1.new_dictionary().set_at("who", 1))
    tr2.set_root_object("x", tr2.new_dictionary().set_at("who", 2))
    tr1.commit()
    before = space.storage.read_root()
    with pytest.raises(ProtoConflictError):
        tr2.commit()
    assert space.storage.read_root() == before
    assert read_root(db, "x").get_at("who") == 1


def test_read_then_concurrent_remove_is_a_conflict(db):
    with db.new_transaction() as tr:
        tr.set_root_object("x", 1)
    tr = db.new_transaction()
    assert tr.get_root_object("x") == 1
    with db.new_transaction() as other:
        other.remove_root_object("x")
    tr.set_root_object("y", 2)
    with pytest.raises(ProtoConflictError):
        tr.commit()


def test_conflict_leaves_nothing_published(space, db):
    with db.new_transaction() as tr:
        tr.set_root_object("d", tr.new_dictionary().set_at("a", 1))
    tr = db.new_transaction()
    d = tr.get_root_object("d")
    with db.new_transaction() as other:
        other.set_root_object("d", other.get_root_object("d").set_at("a", 2))
    root_before = space.storage.read_root()
    tr.set_root_object("d", d.set_at("b", 9))
    tr.set_root_object("other", 5)
    with pytest.raises(ProtoConflictError):
        tr.commit()
    assert space.storage.read_root() == root_before
    after = db.new_transaction()
    assert after.get_root_object("d").get_at("a") == 2
    assert after.get_root_object("d").get_at("b") is None
    assert after.get_root_object("other") is None
    after.abort()
    assert [h.version for h in db.history()][0] == 2


def test_read_root_is_not_merged_even_if_concurrent_optimized(db):
    with db.new_transaction() as tr:
        tr.set_root_object("d", tr.new_dictionary().set_at("a", 1))
    tr = db.new_transaction()
    d = tr.get_root_object("d")
    with db.new_transaction() as other:
        other.set_root_object("d", other.new_dictionary().set_at("z", 1))
    tr.set_root_object("d", d.set_at("b", 2))
    with pytest.raises(ProtoConflictError):
        tr.commit()


def test_unchanged_read_root_written_back_wins_as_written(db):
    with db.new_transaction() as tr:
        tr.set_root_object("d", tr.new_dictionary().set_at("a", 1).set_at("b", 2))
    with db.new_transaction() as tr:
        tr.set_root_object("d", tr.get_root_object("d").remove_at("a"))
    d = read_root(db, "d")
    assert list(d.keys()) == ["b"]


def test_concurrent_reader_of_an_unrelated_root_does_not_conflict(db):
    tr = db.new_transaction()
    tr.get_root_object("a")
    with db.new_transaction() as other:
        other.set_root_object("b", 1)
    tr.set_root_object("c", 1)
    tr.commit()
    assert read_root(db, "b") == 1 and read_root(db, "c") == 1


# -- blind writes (intent merge) -------------------------------------------------------------

def test_blind_writes_of_two_transactions_both_survive(db):
    with db.new_transaction() as tr:
        tr.set_root_object("d", tr.new_dictionary().set_at("a", 1))
    tr1, tr2 = db.new_transaction(), db.new_transaction()
    tr1.set_root_object("d", tr1.new_dictionary().set_at("x", 1))
    tr2.set_root_object("d", tr2.new_dictionary().set_at("y", 2))
    tr1.commit()
    tr2.commit()
    d = read_root(db, "d")
    assert dict(d.items()) == {"a": 1, "x": 1, "y": 2}


def test_blind_remove_of_absent_key_over_concurrent_add(db):
    with db.new_transaction() as tr:
        tr.set_root_object("d", tr.new_dictionary().set_at("a", 1))
    blind = db.new_transaction()
    blind.set_root_object("d", blind.new_dictionary().remove_at("k"))
    with db.new_transaction() as other:
        other.set_root_object("d", other.get_root_object("d").set_at("k", 5))
    blind.commit()
    assert dict(read_root(db, "d").items()) == {"a": 1}


def test_blind_set_and_counted_set_merge_by_intent(db):
    with db.new_transaction() as tr:
        tr.set_root_object("s", tr.new_set().add(1))
        tr.set_root_object("c", tr.new_counted_set().add("x"))
    tr1, tr2 = db.new_transaction(), db.new_transaction()
    tr1.set_root_object("s", tr1.new_set().add(2).remove(1))
    tr1.set_root_object("c", tr1.new_counted_set().add("x"))
    tr2.set_root_object("s", tr2.new_set().add(3))
    tr2.set_root_object("c", tr2.new_counted_set().add("x"))
    tr1.commit()
    tr2.commit()
    tr = db.new_transaction()
    assert sorted(tr.get_root_object("s")) == [2, 3]
    assert tr.get_root_object("c").count_of("x") == 3
    tr.abort()


def test_blind_write_of_a_new_root_stores_it_as_written(db):
    with db.new_transaction() as tr:
        tr.set_root_object("d", tr.new_dictionary().set_at("a", 1))
    assert dict(read_root(db, "d").items()) == {"a": 1}


def test_blind_write_of_a_value_whose_log_is_incomplete_replaces(db):
    """A bulk-built or loaded collection carries content its log does not describe: last writer wins."""
    with db.new_transaction() as tr:
        tr.set_root_object("d", tr.new_dictionary().set_at("a", 1))
        tr.set_root_object("src", Dictionary.from_items([("z", 26)], context=tr))
    with db.new_transaction() as tr:
        tr.set_root_object("d", tr.get_root_object("src"))
    assert dict(read_root(db, "d").items()) == {"z": 26}
    with db.new_transaction() as tr:
        tr.set_root_object("d", Dictionary.from_items([("q", 1)], context=tr))
    assert dict(read_root(db, "d").items()) == {"q": 1}


def test_blind_write_over_a_different_type_replaces(db):
    with db.new_transaction() as tr:
        tr.set_root_object("d", tr.new_list().append_last(1))
    with db.new_transaction() as tr:
        tr.set_root_object("d", tr.new_dictionary().set_at("a", 1))
    assert dict(read_root(db, "d").items()) == {"a": 1}


def test_blind_write_of_non_mergeable_type_is_last_writer_wins(db):
    with db.new_transaction() as tr:
        tr.set_root_object("l", tr.new_list().append_last(1))
    tr1, tr2 = db.new_transaction(), db.new_transaction()
    tr1.set_root_object("l", tr1.new_list().append_last(2))
    tr2.set_root_object("l", tr2.new_list().append_last(3))
    tr1.commit()
    tr2.commit()
    assert list(read_root(db, "l")) == [3]


def test_rebase_conflict_is_a_conflict_and_publishes_nothing(space, db):
    from proto_db.indexing import EqualityIndex
    with db.new_transaction() as tr:
        tr.set_root_object("d", tr.new_dictionary())
    tr1, tr2 = db.new_transaction(), db.new_transaction()
    tr1.set_root_object("d", tr1.new_dictionary().add_index(EqualityIndex("i", "v")))
    tr2.set_root_object("d", tr2.new_dictionary().add_index(EqualityIndex("i", "v")))
    tr1.commit()
    before = space.storage.read_root()
    with pytest.raises(ProtoConflictError):
        tr2.commit()
    assert space.storage.read_root() == before


def test_merge_always_rebases_the_original_version(db):
    """A conflict-free commit after several concurrent commits replays the original log once."""
    with db.new_transaction() as tr:
        tr.set_root_object("c", tr.new_counted_set())
    blind = db.new_transaction()
    blind.set_root_object("c", blind.new_counted_set().add("x"))
    for _ in range(3):
        with db.new_transaction() as tr:
            tr.set_root_object("c", tr.get_root_object("c").add("x"))
    blind.commit()
    assert read_root(db, "c").count_of("x") == 4


def test_blind_remove_root_is_last_writer_wins(db):
    with db.new_transaction() as tr:
        tr.set_root_object("x", 1)
    tr = db.new_transaction()
    with db.new_transaction() as other:
        other.set_root_object("x", 2)
    tr.remove_root_object("x")
    tr.commit()
    assert read_root(db, "x") is None


# -- history ---------------------------------------------------------------------------------

def test_history_lists_versions_newest_first(db):
    assert [h.version for h in db.history()] == [0]
    for i in range(3):
        with db.new_transaction() as tr:
            tr.set_root_object("x", i)
    history = db.history()
    assert [h.version for h in history] == [3, 2, 1, 0]
    assert all(a.committed_at >= b.committed_at for a, b in zip(history, history[1:]))
    assert history[0].root_names == ("x",)
    assert history[-1].root_names == ()


# -- run() -----------------------------------------------------------------------------------

def test_run_returns_the_result_and_commits(db):
    assert db.run(lambda tr: tr.set_root_object("x", 1) or "done") == "done"
    assert read_root(db, "x") == 1


def test_run_retries_conflicts_then_succeeds(db):
    with db.new_transaction() as tr:
        tr.set_root_object("n", 0)
    attempts = []

    def bump(tr):
        attempts.append(tr)
        n = tr.get_root_object("n")
        if len(attempts) < 3:
            with db.new_transaction() as other:
                other.set_root_object("n", other.get_root_object("n") + 100)
        tr.set_root_object("n", n + 1)
        return len(attempts)

    assert db.run(bump, backoff=(0, 0)) == 3
    assert read_root(db, "n") == 201
    assert len({id(t) for t in attempts}) == 3 and not any(t.is_active for t in attempts)


def test_run_exhausts_retries_and_reraises_the_conflict(space, db):
    calls = []

    def always_conflicts(tr):
        calls.append(1)
        tr.get_root_object("n")
        with db.new_transaction() as other:
            other.set_root_object("n", len(calls))
        tr.set_root_object("n", -1)

    with pytest.raises(ProtoConflictError):
        db.run(always_conflicts, retries=2, backoff=(0, 0))
    assert len(calls) == 3
    assert read_root(db, "n") == 3


def test_run_retries_conflicts_raised_by_rebase(db):
    from proto_db.indexing import EqualityIndex
    with db.new_transaction() as tr:
        tr.set_root_object("d", tr.new_dictionary())
    calls = []

    def add_index(tr):
        calls.append(1)
        if len(calls) == 1:
            with db.new_transaction() as other:
                other.set_root_object("d", other.new_dictionary().add_index(EqualityIndex("i", "v")))
        tr.set_root_object("d", tr.new_dictionary().add_index(EqualityIndex("i", "v")))

    with pytest.raises(ProtoConflictError):
        db.run(add_index, retries=1, backoff=(0, 0))
    assert len(calls) == 2


def test_run_does_not_retry_other_errors(space, db):
    calls = []

    def fails(tr):
        calls.append(tr)
        tr.set_root_object("x", 1)
        raise ValueError("not a conflict")

    before = epoch(space)
    with pytest.raises(ValueError):
        db.run(fails)
    assert len(calls) == 1 and not calls[0].is_active and epoch(space) == before


def test_run_backoff_is_bounded(db, monkeypatch):
    import proto_db.transactions as txmod
    sleeps = []
    monkeypatch.setattr(txmod.time, "sleep", sleeps.append)

    def always_conflicts(tr):
        tr.get_root_object("n")
        with db.new_transaction() as other:
            other.set_root_object("n", len(sleeps))
        tr.set_root_object("n", -1)

    with pytest.raises(ProtoConflictError):
        db.run(always_conflicts, retries=6, backoff=(0.001, 0.01))
    assert len(sleeps) == 6
    assert all(0 <= s <= 0.01 for s in sleeps)
    assert max(sleeps) > 0.001


def test_run_validates_arguments(db):
    with pytest.raises(ProtoValidationError):
        db.run(lambda tr: None, retries=-1)
    with pytest.raises(ProtoValidationError):
        db.run(lambda tr: None, backoff=(0.5, 0.1))
    with pytest.raises(ProtoValidationError):
        db.run(lambda tr: None, backoff=(-1, 0.1))


def test_run_respects_a_transaction_ended_by_fn(db):
    def commits_itself(tr):
        tr.set_root_object("x", 7)
        tr.commit()
        return "ok"
    assert db.run(commits_itself) == "ok"
    assert read_root(db, "x") == 7


# -- locking ---------------------------------------------------------------------------------

def test_root_lock_timeout_raises_and_publishes_nothing():
    storage = MemoryStorage()
    space = ObjectSpace(storage, lock_timeout=0.05)
    try:
        db = space.new_database("db")
        tr = db.new_transaction()
        tr.set_root_object("x", 1)
        held, release = threading.Event(), threading.Event()

        def holder():
            with storage.root_lock():
                held.set()
                release.wait(5)

        t = threading.Thread(target=holder)
        t.start()
        held.wait(5)
        before = storage.read_root()
        try:
            with pytest.raises(ProtoTimeoutError):
                tr.commit()
        finally:
            release.set()
            t.join()
        assert storage.read_root() == before and not tr.is_active
    finally:
        space.close()


# -- unknown publish outcome -----------------------------------------------------------------

@pytest.fixture
def faulty():
    storage = PublishFaultStorage()
    s = ObjectSpace(storage)
    db = s.new_database("db")
    with db.new_transaction() as tr:
        tr.set_root_object("x", 0)
    yield storage, db
    s.close()


def test_publish_failure_with_root_visible_is_a_successful_commit(faulty):
    storage, db = faulty
    storage.fail_next_publish("visible")
    tr = db.new_transaction()
    tr.set_root_object("x", 1)
    tr.commit()
    assert not tr.is_active and read_root(db, "x") == 1


def test_publish_failure_with_root_not_visible_raises(faulty):
    storage, db = faulty
    before = storage.read_root()
    storage.fail_next_publish("lost")
    tr = db.new_transaction()
    tr.set_root_object("x", 1)
    with pytest.raises(ProtoUnexpectedError):
        tr.commit()
    assert not tr.is_active and storage.read_root() == before and read_root(db, "x") == 0


def test_publish_failure_with_another_root_visible_is_a_conflict(faulty):
    storage, db = faulty
    storage.fail_next_publish("foreign")
    tr = db.new_transaction()
    tr.set_root_object("x", 1)
    with pytest.raises(ProtoConflictError):
        tr.commit()
    assert not tr.is_active


def test_raw_oserror_from_publish_surfaces_as_proto_error(faulty):
    storage, db = faulty
    storage.fail_next_publish("oserror")
    tr = db.new_transaction()
    tr.set_root_object("x", 1)
    with pytest.raises(ProtoUnexpectedError) as info:
        tr.commit()
    assert isinstance(info.value.__cause__, OSError) and not tr.is_active


def test_publish_failure_with_unreadable_root_reports_an_unknown_outcome(faulty):
    storage, db = faulty
    storage.fail_next_publish("visible")
    storage.fail_read_root_after_fault = True
    tr = db.new_transaction()
    tr.set_root_object("x", 1)
    with pytest.raises(ProtoUnexpectedError, match="outcome"):
        tr.commit()
    assert not tr.is_active


# -- FileStorage write failures (real fault injection) ---------------------------------------

def _file_space(tmp_path):
    fs = FaultyFS(FaultPlan(0, "write"))
    storage = FileStorage(str(tmp_path / "space"), _fs=fs)
    space = ObjectSpace(storage)
    db = space.new_database("db")
    with db.new_transaction() as tr:
        tr.set_root_object("x", 0)
    return fs, storage, space, db


def _arm(fs, op, nth=1):
    fs.plan.op = op
    fs.plan.fail_at = fs.plan.counts[op] + nth
    fs.plan.fired = False


def test_segment_write_failure_aborts_and_disables_storage(tmp_path):
    fs, storage, space, db = _file_space(tmp_path)
    before = storage.read_root()
    _arm(fs, "write")
    tr = db.new_transaction()
    tr.set_root_object("x", 1)
    with pytest.raises(ProtoDBError):
        tr.commit()
    assert not tr.is_active and storage.read_root() == before
    tr = db.new_transaction()
    tr.set_root_object("x", 2)
    with pytest.raises(ProtoUnexpectedError):
        tr.commit()
    space.close()
    reopened = ObjectSpace(FileStorage(str(tmp_path / "space")))
    assert read_root(reopened.open_database("db"), "x") == 0
    reopened.close()


def test_rename_failure_is_not_a_commit(tmp_path):
    fs, storage, space, db = _file_space(tmp_path)
    _arm(fs, "rename")
    tr = db.new_transaction()
    tr.set_root_object("x", 1)
    with pytest.raises(ProtoUnexpectedError):
        tr.commit()
    assert not tr.is_active
    space.close()
    reopened = ObjectSpace(FileStorage(str(tmp_path / "space")))
    assert read_root(reopened.open_database("db"), "x") == 0
    reopened.close()


def test_directory_fsync_failure_after_rename_is_a_commit(tmp_path):
    fs, storage, space, db = _file_space(tmp_path)
    _arm(fs, "dirsync")
    tr = db.new_transaction()
    tr.set_root_object("x", 1)
    tr.commit()
    assert fs.plan.fired and not tr.is_active
    space.close()
    reopened = ObjectSpace(FileStorage(str(tmp_path / "space")))
    assert read_root(reopened.open_database("db"), "x") == 1
    reopened.close()


# -- post-publish steps never raise ----------------------------------------------------------

class _UnlockFails:
    """Storage double whose root lock release raises (a post-publish failure)."""

    def __init__(self):
        self.inner = MemoryStorage()
        self.fail_release = False

    def __getattr__(self, name):
        return getattr(self.inner, name)

    def root_lock(self, timeout=None):
        import contextlib
        outer = self

        @contextlib.contextmanager
        def cm():
            with self.inner.root_lock(timeout):
                yield
            if outer.fail_release:
                raise ProtoUnexpectedError("injected unlock failure")
        return cm()


def test_failure_after_publish_does_not_fail_the_commit(caplog):
    storage = _UnlockFails()
    space = ObjectSpace(storage)
    db = space.new_database("db")
    storage.fail_release = True
    tr = db.new_transaction()
    tr.set_root_object("x", 1)
    tr.commit()
    assert not tr.is_active and read_root(db, "x") == 1
    assert "injected unlock failure" in caplog.text
    space.close()


# -- free-threading --------------------------------------------------------------------------

def test_concurrent_counters_lose_no_update(space):
    db = space.new_database("db")
    with db.new_transaction() as tr:
        tr.set_root_object("n", 0)
        tr.set_root_object("c", tr.new_counted_set())
    threads, per_thread = 8, 15
    errors = []

    def work():
        try:
            for _ in range(per_thread):
                db.run(lambda tr: tr.set_root_object("n", tr.get_root_object("n") + 1),
                       retries=1000, backoff=(0, 0.002))
                db.run(lambda tr: tr.set_root_object("c", tr.new_counted_set().add("hit")),
                       retries=1000, backoff=(0, 0.002))
        except ProtoDBError as e:  # pragma: no cover - reported below
            errors.append(e)

    ts = [threading.Thread(target=work) for _ in range(threads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert not errors
    tr = db.new_transaction()
    assert tr.get_root_object("n") == threads * per_thread
    assert tr.get_root_object("c").count_of("hit") == threads * per_thread
    tr.abort()
    assert db.history()[0].version == 2 * threads * per_thread + 1


def test_concurrent_creators_of_distinct_databases(space):
    names = [f"db{i}" for i in range(8)]
    errors = []

    def create(name):
        for attempt in range(1000):
            try:
                space.new_database(name)
                return
            except ProtoConflictError:
                time.sleep(0.001)
        errors.append(name)  # pragma: no cover

    ts = [threading.Thread(target=create, args=(n,)) for n in names]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert not errors and space.list_databases() == sorted(names)


def test_transaction_class_is_a_persistence_context(db):
    from proto_db.core.atom import PersistenceContext
    tr = db.new_transaction()
    assert isinstance(tr, Transaction) and isinstance(tr, PersistenceContext)
    tr.abort()


def test_read_root_replaced_by_a_fresh_collection_is_stored_as_written(db):
    """A root the transaction read is never merged: a fresh value written over it replaces it."""
    with db.new_transaction() as tr:
        tr.set_root_object("d", tr.new_dictionary().set_at("a", 1))
    with db.new_transaction() as tr:
        assert tr.get_root_object("d").get_at("a") == 1
        tr.set_root_object("d", tr.new_dictionary().set_at("b", 2))
    assert dict(read_root(db, "d").items()) == {"b": 2}


# -- fix round 1 -----------------------------------------------------------------------------

@pytest.mark.parametrize("before, after", [
    (1, 1.0),
    (Decimal("1"), Decimal("1.00")),
    (datetime.datetime(2026, 1, 1, 12, tzinfo=datetime.timezone.utc),
     datetime.datetime(2026, 1, 1, 13, tzinfo=datetime.timezone(datetime.timedelta(hours=1)))),
    (0.0, -0.0),
    ((1, "a"), (1.0, "a")),
    (True, 1),
])
def test_exactly_different_plain_values_conflict(db, before, after):
    with db.new_transaction() as tr:
        tr.set_root_object("x", before)
    tr = db.new_transaction()
    tr.get_root_object("x")
    with db.new_transaction() as other:
        other.set_root_object("x", after)
    tr.set_root_object("y", 1)
    with pytest.raises(ProtoConflictError):
        tr.commit()


def test_identical_plain_value_rewrite_is_not_a_conflict(db):
    with db.new_transaction() as tr:
        tr.set_root_object("x", (1, "a", Decimal("2.50")))
    tr = db.new_transaction()
    tr.get_root_object("x")
    with db.new_transaction() as other:
        other.set_root_object("x", (1, "a", Decimal("2.50")))
    tr.set_root_object("y", 1)
    tr.commit()


def test_tuple_holding_atoms_compares_atoms_by_pointer(db):
    with db.new_transaction() as tr:
        tr.set_root_object("x", (tr.new_object(a=1),))
    tr = db.new_transaction()
    tr.get_root_object("x")
    with db.new_transaction() as other:
        other.set_root_object("x", (other.new_object(a=1),))
    tr.set_root_object("y", 1)
    with pytest.raises(ProtoConflictError):
        tr.commit()


def test_remove_then_set_replaces(db):
    with db.new_transaction() as tr:
        tr.set_root_object("x", tr.new_dictionary().set_at("a", 1).set_at("b", 2))
    with db.new_transaction() as tr:
        tr.remove_root_object("x")
        tr.set_root_object("x", tr.new_dictionary().set_at("c", 3))
    assert dict(read_root(db, "x").items()) == {"c": 3}


def test_set_with_replace_substitutes_a_blind_root(db):
    with db.new_transaction() as tr:
        tr.set_root_object("x", tr.new_dictionary().set_at("a", 1))
    tr1, tr2 = db.new_transaction(), db.new_transaction()
    tr1.set_root_object("x", tr1.new_dictionary().set_at("b", 2))
    tr2.set_root_object("x", tr2.new_dictionary().set_at("c", 3), replace=True)
    tr1.commit()
    tr2.commit()
    assert dict(read_root(db, "x").items()) == {"c": 3}


def test_replace_mark_is_sticky_within_the_transaction(db):
    with db.new_transaction() as tr:
        tr.set_root_object("x", tr.new_dictionary().set_at("a", 1))
    with db.new_transaction() as tr:
        tr.set_root_object("x", tr.new_dictionary().set_at("b", 2), replace=True)
        tr.set_root_object("x", tr.new_dictionary().set_at("c", 3))
    assert dict(read_root(db, "x").items()) == {"c": 3}


def test_set_without_replace_still_merges(db):
    with db.new_transaction() as tr:
        tr.set_root_object("x", tr.new_dictionary().set_at("a", 1))
    with db.new_transaction() as tr:
        tr.set_root_object("x", tr.new_dictionary().set_at("c", 3), replace=False)
    assert dict(read_root(db, "x").items()) == {"a": 1, "c": 3}


def test_replace_must_be_a_bool(db):
    tr = db.new_transaction()
    with pytest.raises(ProtoValidationError):
        tr.set_root_object("x", 1, replace=1)
    tr.abort()


def test_committed_value_reused_later_does_not_replay_its_ops(db):
    """Reported case: a value committed blind and reused in a later transaction must not
    re-apply the earlier transaction's ops (here: re-adding a key removed meanwhile)."""
    with db.new_transaction() as tr:
        tr.set_root_object("d", tr.new_dictionary().set_at("a", 1))
    tr = db.new_transaction()
    held = tr.new_dictionary().set_at("k", 1)
    tr.set_root_object("d", held)
    tr.commit()
    assert not held.log_is_complete
    with db.new_transaction() as tr:
        tr.set_root_object("d", tr.get_root_object("d").remove_at("k"))
    with db.new_transaction() as tr:
        tr.set_root_object("other", held)
    assert dict(read_root(db, "d").items()) == {"a": 1}
    with db.new_transaction() as tr:
        tr.set_root_object("d", held.set_at("z", 9))
    assert dict(read_root(db, "d").items()) == {"k": 1, "z": 9}


def test_failed_commit_keeps_the_value_mergeable(db):
    with db.new_transaction() as tr:
        tr.set_root_object("d", tr.new_dictionary().set_at("a", 1))
    tr = db.new_transaction()
    tr.get_root_object("d")
    held = tr.new_dictionary().set_at("k", 1)
    tr.set_root_object("d2", held)
    with db.new_transaction() as other:
        other.set_root_object("d", other.get_root_object("d").set_at("a", 2))
    with pytest.raises(ProtoConflictError):
        tr.commit()
    assert held.log_is_complete


class _ReleaseFailsOnError:
    """Storage double whose lock release raises when leaving with an exception."""

    def __init__(self):
        self.inner = MemoryStorage()

    def __getattr__(self, name):
        return getattr(self.inner, name)

    def root_lock(self, timeout=None):
        import contextlib

        @contextlib.contextmanager
        def cm():
            with self.inner.root_lock(timeout):
                try:
                    yield
                except ProtoDBError:
                    raise ProtoUnexpectedError("injected release failure")
        return cm()


def test_release_failure_does_not_mask_a_conflict(caplog):
    space = ObjectSpace(_ReleaseFailsOnError())
    db = space.new_database("db")
    with db.new_transaction() as tr:
        tr.set_root_object("x", 1)
    tr = db.new_transaction()
    tr.get_root_object("x")
    with db.new_transaction() as other:
        other.set_root_object("x", 2)
    tr.set_root_object("x", 3)
    with pytest.raises(ProtoConflictError):
        tr.commit()
    assert "injected release failure" in caplog.text
    space.close()
