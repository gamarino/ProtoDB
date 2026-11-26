"""The precondition model in a cluster (spec R38, Review Focus 5; Task 14 rulings 1b and 5).

A commit's validation and reapplication run on the node that builds it, under its write
permission, with the same rules as a standalone commit: the pre-pass validates against the
newest committed root the node knows (R0, outside the permission) and, when the permission's
base R1 is another root, the build validates by the diff R0 -> R1 (R65) and reapplies on R1.

Every interleaving is forced, never timed: the concurrent commit runs from the one-shot hook on
``_commit_root`` (between the pre-pass and the lock step), the re-committed base is forced at
``_base_needs_commit``, and an unavailable R0 is an injected tier read error.  Waits are for
conditions only.
"""
import threading

import pytest

from proto_db import transactions
from proto_db.collections.dictionaries import Dictionary
from proto_db.errors import ProtoConflictError, ProtoUnexpectedError
from proto_db.preconditions import Unit, UnitKind
from proto_db.transactions import Transaction
from tests.support.cluster_load import await_condition
from tests.support.tiered_cluster import RUN_KW, TieredCluster


def _seed(tc, node="n0", **items):
    def build(tr):
        d = Dictionary(context=tr)
        for k, v in items.items():
            d = d.set_at(k, v)
        tr.set_root_object("d", d)
    tc.create_database(node)
    tc.commit(node, build)


def _content(tc, node):
    return dict(tc.db(node).run(lambda tr: tr.get_root_object("d")).items())


def _set(tr, key, value):
    tr.set_root_object("d", tr.get_root_object("d").set_at(key, value))


class _Validations:
    """Records, per transaction, which validation ran, whether the space root was held, and on
    which node (the coordinator of the transaction's space)."""

    def __init__(self, monkeypatch):
        self.calls = []
        delta, full = Transaction._delta_check, Transaction._check_roots

        def spy_delta(tr, candidate, db_root):
            self.calls.append((tr, "delta", Transaction._holding_root_lock(), _node(tr)))
            return delta(tr, candidate, db_root)

        def spy_full(tr, objects):
            self.calls.append((tr, "full", Transaction._holding_root_lock(), _node(tr)))
            return full(tr, objects)
        monkeypatch.setattr(Transaction, "_delta_check", spy_delta)
        monkeypatch.setattr(Transaction, "_check_roots", spy_full)

    def of(self, tr):
        return [(kind, held, node) for t, kind, held, node in self.calls if t is tr]


def _node(tr):
    coordinator = tr._database.space.coordinator
    return coordinator.node_id if coordinator is not None else None


def _between_prepass_and_lock(monkeypatch, tc, node, concurrent):
    """Arm a one-shot hook: the next commit of ``node`` that reaches its lock step first runs
    ``concurrent()`` to completion in another thread (a commit acknowledged by a majority)."""
    armed = [True]
    lock_step = transactions._commit_root

    def hook(storage, lock_timeout, build, coordinator=None):
        if armed[0] and coordinator is tc[node]:
            armed[0] = False
            t = threading.Thread(target=concurrent)
            t.start()
            t.join()
        return lock_step(storage, lock_timeout, build, coordinator)
    monkeypatch.setattr(transactions, "_commit_root", hook)
    return armed


def test_cluster_dictionary_keys_no_conflict(tmp_path, monkeypatch):
    """Different keys of one Dictionary written on two nodes: n1's commit lands between n0's
    pre-pass and its permission, so n0 validates by delta against n1's root and reapplies its
    key on it, under its permission, on n0 (R38).  Both keys are published."""
    with TieredCluster(tmp_path) as tc:
        _seed(tc, a=0)
        seen = _Validations(monkeypatch)
        a = tc.db("n0").new_transaction()
        _set(a, "k", 1)
        armed = _between_prepass_and_lock(monkeypatch, tc, "n0",
                                          lambda: tc.db("n1").run(lambda tr: _set(tr, "j", 2), **RUN_KW))
        a.commit()
        assert not armed[0]
        assert seen.of(a) == [("delta", True, "n0")]
        assert _content(tc, "n0") == {"a": 0, "k": 1, "j": 2}
        await_condition(lambda: _content(tc, "n2") == {"a": 0, "k": 1, "j": 2}, what="n2 learns both keys")


def test_cluster_same_key_conflicts(tmp_path, monkeypatch):
    """The same key written on two nodes: n0's delta validation under its permission finds the
    key changed since R0 and raises ``ProtoConflictError`` naming the unit (R36); only n1's
    value is published."""
    with TieredCluster(tmp_path) as tc:
        _seed(tc, a=0)
        seen = _Validations(monkeypatch)
        a = tc.db("n0").new_transaction()
        _set(a, "k", 1)
        _between_prepass_and_lock(monkeypatch, tc, "n0",
                                  lambda: tc.db("n1").run(lambda tr: _set(tr, "k", 2), **RUN_KW))
        with pytest.raises(ProtoConflictError) as e:
            a.commit()
        assert e.value.unit == Unit(UnitKind.KEY, "d", key="k") and e.value.database == "db"
        assert seen.of(a) == [("delta", True, "n0")]
        assert _content(tc, "n0") == {"a": 0, "k": 2}


def test_cluster_recommitted_base_is_not_a_conflict(tmp_path, monkeypatch):
    """Review Focus 5: the permission's base is re-committed first (``_commit_base``: an
    unchanged root on top of it), so the root the build finds (R1) differs from the pre-pass's
    R0 by pointer only.  The delta validation compares content units, not root pointers: the
    diff R0 -> R1 is empty and the commit, which read a key and wrote another, lands."""
    with TieredCluster(tmp_path) as tc:
        _seed(tc, a=0)
        tc.commit("n0", lambda tr: _set(tr, "b", 0))     # n0 knows the newest root, its own
        seen = _Validations(monkeypatch)
        forced, recommitted = [True], []
        needs_commit, commit_base = transactions._base_needs_commit, transactions._commit_base

        def force(coordinator, permission, current):
            if forced[0] and coordinator is tc["n0"]:
                forced[0] = False
                return True
            return needs_commit(coordinator, permission, current)

        def spy_commit_base(coordinator, tier, lease, label, base, **kw):
            record = commit_base(coordinator, tier, lease, label, base, **kw)
            recommitted.append((base, record))
            return record
        monkeypatch.setattr(transactions, "_base_needs_commit", force)
        monkeypatch.setattr(transactions, "_commit_base", spy_commit_base)
        a = tc.db("n0").new_transaction()
        assert a.get_root_object("d").get_at("a") == 0      # a recorded read of key "a"
        _set(a, "k", 1)
        a.commit()
        assert not forced[0] and len(recommitted) == 1
        base, record = recommitted[0]
        assert record.pointer != base.pointer
        assert seen.of(a) == [("delta", True, "n0")]
        assert _content(tc, "n0") == {"a": 0, "b": 0, "k": 1}


@pytest.mark.parametrize("same_key", [False, True], ids=["other_key", "same_key"])
def test_cluster_prepass_base_unavailable_validates_under_the_permission(tmp_path, monkeypatch, same_key):
    """Ruling 1b: the pre-pass reads R0 outside the permission.  When a record of R0 is
    unavailable there (``ProtoUnexpectedError``, as the tier raises for a segment not uploaded
    while some peer is silent, R230), the commit runs without a pre-pass: the full validation
    against the permission's base, under the permission (R65's "R0 unavailable").  A
    concurrent change of another key composes; of the same key, conflicts."""
    with TieredCluster(tmp_path) as tc:
        _seed(tc, a=0)
        seen = _Validations(monkeypatch)
        a = tc.db("n0").new_transaction()
        _set(a, "k", 1)
        tc.db("n1").run(lambda tr: _set(tr, "k" if same_key else "j", 2), **RUN_KW)
        newest = tc["n1"].current_root()
        await_condition(lambda: tc["n0"].current_root() == newest, what="n0 learns n1's root")
        storage = tc.storages["n0"]
        read, prepass = storage._record, Transaction._prepass
        state = {"in_prepass": False, "raised": 0}

        def unavailable(ptr):
            if state["in_prepass"] and not storage.in_critical():
                state["raised"] += 1
                raise ProtoUnexpectedError(f"The record at {ptr} is unavailable: the object store does not "
                                           f"hold its segment (not uploaded yet), and some peer was not heard "
                                           f"(injected)")
            return read(ptr)

        def marked(tr):
            state["in_prepass"] = tr is a
            try:
                return prepass(tr)
            finally:
                state["in_prepass"] = False
        monkeypatch.setattr(storage, "_record", unavailable)
        monkeypatch.setattr(Transaction, "_prepass", marked)
        if same_key:
            with pytest.raises(ProtoConflictError) as e:
                a.commit()
            assert e.value.unit == Unit(UnitKind.KEY, "d", key="k")
        else:
            a.commit()
        assert state["raised"] == 1
        assert seen.of(a) == [("full", True, "n0")]
        assert _content(tc, "n0") == ({"a": 0, "k": 2} if same_key else {"a": 0, "k": 1, "j": 2})


def test_cluster_prepass_conflict_takes_the_normal_backoff(tmp_path, monkeypatch):
    """A conflict the pre-pass finds (R0 already holds the concurrent change) fails before the lock
    step: no permission is requested for it, and the error does not keep one (``permit_held``
    false), so ``Database.run`` retries it after its ordinary backoff (Task 14)."""
    with TieredCluster(tmp_path) as tc:
        _seed(tc, a=0)
        a = tc.db("n0").new_transaction()
        assert a.get_root_object("d").get_at("a") == 0
        _set(a, "k", 1)
        tc.db("n1").run(lambda tr: _set(tr, "a", 2), **RUN_KW)
        newest = tc["n1"].current_root()
        await_condition(lambda: tc["n0"].current_root() == newest, what="n0 learns n1's root")
        lock_steps = []
        lock_step = transactions._commit_root

        def counted(storage, lock_timeout, build, coordinator=None):
            lock_steps.append(coordinator)
            return lock_step(storage, lock_timeout, build, coordinator)
        monkeypatch.setattr(transactions, "_commit_root", counted)
        with pytest.raises(ProtoConflictError) as e:
            a.commit()
        assert e.value.unit == Unit(UnitKind.KEY, "d", key="a")
        assert not getattr(e.value, "permit_held", False)
        assert lock_steps == []
        assert _content(tc, "n0") == {"a": 2}
