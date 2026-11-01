"""Cluster commits over tiered storage: the object store off the commit path (Task 8.3d, D46)."""
import threading
import time

import pytest

from proto_db.errors import ProtoTimeoutError
from proto_db.storage.cloud import CloudStorage
from proto_db.transactions import ObjectSpace
from tests.support.fake_object_store import FakeObjectStore
from tests.support.tiered_cluster import RUN_KW, TieredCluster, incr

pytestmark = [pytest.mark.timeout(120)]


def _remote_value(store):
    """The counter in the space persisted in ``store`` (read without any cluster), or ``None``."""
    space = ObjectSpace(CloudStorage(store))
    try:
        if "db" not in space.list_databases():
            return None
        tr = space.open_database("db").new_transaction()
        try:
            return tr.get_root_object("n")
        finally:
            tr.abort()
    finally:
        space.close()


def _commit(tc, nodes, count):
    for i in range(count):
        tc.db(nodes[i % len(nodes)]).run(incr, **RUN_KW)


def _wait(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            return False
        time.sleep(0.01)
    return True


def test_commits_are_persisted_asynchronously_and_sync_waits_for_them(tmp_path):
    with TieredCluster(tmp_path) as tc:
        tc.spaces["n0"].new_database("db")
        _commit(tc, tc.ids, 12)
        space = tc.spaces["n2"]                          # the last committer (others learn it on release)
        space.sync(timeout=10)
        assert space.persisted_epoch >= space.committed_epoch == 13
        assert _remote_value(tc.store) == 12
        assert all(tc.storages[n].stats()["root_writes_in_critical"] == 0 for n in tc.ids)


def test_an_object_store_outage_never_stops_commits_and_catch_up_converges(tmp_path):
    with TieredCluster(tmp_path) as tc:
        tc.spaces["n0"].new_database("db")
        _commit(tc, tc.ids, 6)
        tc.spaces["n0"].sync(timeout=10)
        before = time.perf_counter()
        _commit(tc, tc.ids, 30)
        normal = time.perf_counter() - before
        tc.store.set_outage(True)
        before = time.perf_counter()
        _commit(tc, tc.ids, 30)
        during = time.perf_counter() - before
        assert tc.eventually("n2", 66)
        assert _wait(lambda: tc.store.outage_rejections > 0)
        with pytest.raises(ProtoTimeoutError):
            tc.spaces["n0"].sync(timeout=0.2)
        assert tc.spaces["n0"].persisted_epoch < tc.spaces["n0"].committed_epoch
        # Commit latency does not depend on the store (generous bound for a loaded machine).
        assert during < max(1.0, normal * 3), (normal, during)
        tc.store.set_outage(False)
        for n in tc.ids:
            tc.spaces[n].sync(timeout=15)
        assert _remote_value(tc.store) == 66 and tc.eventually("n0", 66)


def test_a_fresh_peer_joining_during_an_outage_recovers_from_its_peers(tmp_path):
    with TieredCluster(tmp_path) as tc:
        tc.spaces["n0"].new_database("db")
        _commit(tc, ["n0", "n1"], 5)
        tc.store.set_outage(True)
        _commit(tc, ["n0", "n1"], 5)
        tc.wipe("n2")
        tc.revive("n2")
        assert tc.eventually("n2", 10)                     # root and records from the peers
        assert tc.storages["n2"].stats()["reads_remote"] == 0
        tc.db("n2").run(incr, **RUN_KW)
        assert tc.eventually("n0", 11)
        tc.store.set_outage(False)
        tc.spaces["n2"].sync(timeout=15)
        assert _remote_value(tc.store) == 11


def test_a_restarted_cluster_on_empty_disks_recovers_from_the_object_store(tmp_path):
    store = FakeObjectStore()
    with TieredCluster(tmp_path / "a", store=store) as tc:
        tc.spaces["n0"].new_database("db")
        _commit(tc, tc.ids, 9)
        for n in tc.ids:
            tc.spaces[n].sync(timeout=10)
    with TieredCluster(tmp_path / "b", store=store) as tc:
        assert tc.eventually("n1", 9)                       # the persisted root
        assert tc.storages["n1"].stats()["reads_remote"] > 0
        tc.db("n2").run(incr, **RUN_KW)
        assert tc.eventually("n0", 10)
        tc.spaces["n2"].sync(timeout=10)
        assert _remote_value(store) == 10


def test_a_node_restart_loses_nothing(tmp_path):
    with TieredCluster(tmp_path) as tc:
        tc.spaces["n0"].new_database("db")
        tc.store.set_outage(True)                        # nothing is persisted meanwhile
        _commit(tc, tc.ids, 9)
        for n in tc.ids:                                 # every node restarts, one at a time
            tc.kill(n)
            tc.revive(n)
            tc.db(n).run(incr, **RUN_KW)
        assert tc.eventually("n0", 12) and tc.eventually("n2", 12)
        tc.store.set_outage(False)
        tc.spaces["n0"].sync(timeout=15)
        assert _remote_value(tc.store) == 12


def test_the_whole_cluster_restarting_on_its_disks_loses_nothing_unpersisted(tmp_path):
    store = FakeObjectStore()
    with TieredCluster(tmp_path, store=store) as tc:
        tc.spaces["n0"].new_database("db")
        store.set_outage(True)
        _commit(tc, tc.ids, 7)
    store.set_outage(False)
    assert _remote_value(store) is None or _remote_value(store) < 7
    with TieredCluster(tmp_path, store=store) as tc:     # same local directories
        assert tc.eventually("n1", 7)
        tc.db("n1").run(incr, **RUN_KW)
        tc.spaces["n1"].sync(timeout=15)
        assert _remote_value(store) == 8


def test_losing_a_minority_loses_nothing(tmp_path):
    with TieredCluster(tmp_path) as tc:
        tc.spaces["n0"].new_database("db")
        tc.store.set_outage(True)
        _commit(tc, tc.ids, 9)
        tc.wipe("n2")                                    # a disk lost before anything was persisted
        _commit(tc, ["n0", "n1"], 3)
        assert tc.eventually("n1", 12)
        tc.store.set_outage(False)
        tc.revive("n2")
        assert tc.eventually("n2", 12)


def test_losing_a_majority_before_persistence_loses_only_unpersisted_commits(tmp_path):
    with TieredCluster(tmp_path) as tc:
        tc.spaces["n0"].new_database("db")
        _commit(tc, tc.ids, 4)
        for n in tc.ids:
            tc.spaces[n].sync(timeout=10)
        persisted = _remote_value(tc.store)
        assert persisted == 4
        tc.store.set_outage(True)
        _commit(tc, ["n0", "n1"], 5)
        tc.wipe("n0")
        tc.wipe("n2")                                    # n1 alone holds the newest commits
        tc.kill("n1")
        tc.store.set_outage(False)
        # The persisted state is intact and readable without the cluster.
        assert _remote_value(tc.store) == persisted
        tc.revive("n1")
        tc.revive("n0")
        # n1 survived with every commit it accepted and is in the first quorum: nothing is lost.
        assert tc.eventually("n1", 9)
        tc.revive("n2")
        assert tc.eventually("n2", 9) and tc.eventually("n0", 9)
        tc.db("n2").run(incr, **RUN_KW)
        assert tc.eventually("n1", 10)
        tc.store.set_outage(False)
        tc.spaces["n1"].sync(timeout=15)
        assert _remote_value(tc.store) == 10


def test_losing_a_majority_with_the_survivor_outside_the_quorum_rolls_back_to_the_persisted_state(tmp_path):
    with TieredCluster(tmp_path) as tc:
        tc.spaces["n0"].new_database("db")
        _commit(tc, tc.ids, 4)
        for n in tc.ids:
            tc.spaces[n].sync(timeout=10)
        tc.store.set_outage(True)
        _commit(tc, ["n0", "n1"], 5)
        tc.wipe("n0")
        tc.wipe("n2")
        tc.kill("n1")
        tc.store.set_outage(False)
        tc.revive("n0", force_rejoin=True)               # the operator's disaster recovery
        tc.revive("n2", force_rejoin=True)               # a quorum of empty disks
        assert tc.eventually("n0", 4)                       # the persisted state: unpersisted commits lost
        tc.db("n2").run(incr, **RUN_KW)
        tc.revive("n1")                                  # the survivor's newer roots are superseded
        assert tc.eventually("n1", 5) and tc.eventually("n0", 5)
        tc.spaces["n2"].sync(timeout=15)
        assert _remote_value(tc.store) == 5


def test_steady_state_commits_read_and_write_nothing_in_the_object_store_under_the_lease(tmp_path):
    calls = []

    class Recording(FakeObjectStore):
        """Records the thread of every store call."""

        def _enter(self, operation, key):
            calls.append(threading.current_thread().name)
            super()._enter(operation, key)

    store = Recording()
    with TieredCluster(tmp_path, store=store) as tc:
        tc.spaces["n0"].new_database("db")
        _commit(tc, tc.ids, 6)                           # warm up
        for n in tc.ids:
            tc.spaces[n].sync(timeout=10)
        base = {n: tc.storages[n].stats() for n in tc.ids}
        before = {n: tc[n].stats()["critical_commits"] for n in tc.ids}
        calls.clear()
        errors = []

        def committer(node):
            try:
                for _ in range(10):
                    tc.db(node).run(incr, **RUN_KW)
            except BaseException as e:      # reported by the assertion below
                errors.append(e)
                raise

        threads = [threading.Thread(target=committer, args=(n,), name=f"committer-{n}") for n in tc.ids]
        for t in threads:
            t.start()
        for t in threads:
            t.join(60)
        assert not errors
        assert tc.eventually("n0", 36)
        assert not [name for name in calls if name.startswith("committer-")], calls[:5]
        for n in tc.ids:
            stats = tc.storages[n].stats()
            assert stats["critical_misses"] == base[n]["critical_misses"], n
            assert stats["root_writes_in_critical"] == 0
        assert sum(tc[n].stats()["critical_commits"] - before[n] for n in tc.ids) == 30


def test_the_backlog_limit_refuses_commits_until_the_store_drains_it(tmp_path):
    alerts = []
    kw = dict(backlog_limit_bytes=6000, on_backlog_alert=alerts.append, ship_interval=0.02)
    with TieredCluster(tmp_path, storage_kw=kw) as tc:
        tc.spaces["n0"].new_database("db")
        tc.store.set_outage(True)
        db = tc.db("n0")
        refused = None
        for i in range(500):
            try:
                db.run(lambda tr: tr.set_root_object(f"k{i}", "x" * 200), retries=0)
            except Exception as e:      # the expected refusal ends the loop; anything else fails below
                refused = e
                break
        assert type(refused).__name__ == "ProtoBacklogFullError", refused
        assert _wait(lambda: alerts)                    # raised by the persister thread
        assert alerts[0]["bytes"] >= 4800
        tc.db("n1").run(incr, **RUN_KW)                 # other nodes keep committing
        tc.store.set_outage(False)
        assert _wait(lambda: not tc.storages["n0"].backlog()["over_limit"])
        db.run(incr, **RUN_KW)
        assert tc.eventually("n2", 2)


def test_a_fresh_peer_joining_after_persistence_reads_from_the_object_store(tmp_path):
    with TieredCluster(tmp_path) as tc:
        tc.spaces["n0"].new_database("db")
        _commit(tc, tc.ids, 9)
        for n in tc.ids:
            tc.spaces[n].sync(timeout=10)
        assert _wait(lambda: all(tc.storages[n].stats()["segments_uploaded"] > 0 for n in tc.ids))
        tc.wipe("n2")
        tc.revive("n2")
        assert tc.eventually("n2", 9)
        stats = tc.storages["n2"].stats()
        assert stats["reads_remote"] > 0 and stats["reads_peer"] == 0     # remote data: the store serves it


def test_no_thread_outlives_the_cluster(tmp_path):
    def ours():
        return {t for t in threading.enumerate() if t.name.startswith(("proto-", "proto_"))}

    before = ours()
    with TieredCluster(tmp_path) as tc:
        tc.spaces["n0"].new_database("db")
        _commit(tc, tc.ids, 3)
        tc.kill("n1")
        tc.revive("n1")
        assert ours() - before
    assert _wait(lambda: not (ours() - before), timeout=5), ours() - before
