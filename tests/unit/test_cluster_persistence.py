"""Cluster commits over tiered storage: the object store off the commit path (Task 8.3d, D46)."""
import tempfile
import threading
import time

import pytest

from proto_db.errors import ProtoOutcomeUnknownError, ProtoTimeoutError
from proto_db.storage.tiered import TieredStorage
from proto_db.transactions import ObjectSpace
from tests.support.fake_object_store import FakeObjectStore
from tests.support.tiered_cluster import FAST, RUN_KW, TieredCluster, incr

pytestmark = [pytest.mark.timeout(120)]


def _remote_value(store):
    """The counter in the space persisted in ``store`` (read without any cluster), or ``None``.

    Read through a ``TieredStorage`` on an empty local directory: a lagging persister may
    have uploaded the records of a node that had not shipped yet as part objects (D46),
    which only the tiered storage reads (``CloudStorage`` reads sealed segment objects)."""
    with tempfile.TemporaryDirectory() as local:
        return _remote_value_in(store, local)


def _remote_value_in(store, local):
    space = ObjectSpace(TieredStorage(local, store, durability="none"))
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
    # Same local directories, so the default startup grace (a shorter one is for a cluster's first
    # start only).
    with TieredCluster(tmp_path, store=store, startup_grace=None) as tc:
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
        # A majority of disks is lost: the operator forces the rejoins (R49).  n1 survived with
        # every commit it accepted and answers both: nothing is lost.
        tc.revive("n0", force_rejoin=True)
        assert tc.eventually("n1", 9)
        tc.revive("n2", force_rejoin=True)
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


def test_steady_state_commits_write_nothing_in_the_object_store_under_the_lease(tmp_path):
    """D46: committing threads never write the object store, and no root is written in the
    critical section.  Since bounded replication (Task 8.3g, D54) a committer that is not a
    data replica of its base reads the base's records on demand -- from a peer, or from the
    store once the replicas released them -- so critical-section cache misses and store
    reads are expected; they are counted and reported, never forbidden (R70a: remote data is
    a normal case).  Ruling R89: every commit's records reach every member's cache with the
    commit (best effort, no durable copy), so in steady state a committer finds its base's
    records cached and fetches next to nothing from a peer under a permission.

    That steady state is what the miss bound is asserted on (review 8.3m I3, ruling R105): the
    30 commits run one at a time, rotating among the members, and each next commit starts once
    the records the previous one pushed are in every other member's record cache -- an observed
    condition, never a delay, because warming is asynchronous and best effort.  A commit whose
    outcome is unknown (P5: a lease ending under CPU contention) is resolved as R86 rules: every
    acknowledged commit is applied, an unknown one at most once; the commit after it is not in
    steady state and its misses are reported, not bounded."""
    calls = []

    class Recording(FakeObjectStore):
        """Records the thread and the operation of every store call."""

        def _enter(self, operation, key):
            calls.append((threading.current_thread().name, operation))
            super()._enter(operation, key)

    store = Recording()
    with TieredCluster(tmp_path, store=store) as tc:
        tc.spaces["n0"].new_database("db")
        _commit(tc, tc.ids, 6)                           # warm up
        for n in tc.ids:
            tc.spaces[n].sync(timeout=10)
        # The observable end of R89 warming (review 8.3m I3, ruling R105): what each member's
        # record cache received (warmed or cached on receipt), and what each commit pushed.
        cached = {n: set() for n in tc.ids}
        pushed = {n: [] for n in tc.ids}
        for n in tc.ids:
            tier = tc.storages[n]

            def cache_records(records, _n=n, _real=tier.cache_records):
                got = _real(records)
                cached[_n].update((raw, offset) for raw, offset, framed in records if framed is not None)
                return got

            def push_atom(payload, _n=n, _real=tier.push_atom):
                ptr = _real(payload)
                if threading.current_thread().name.startswith("committer-"):
                    pushed[_n].append((ptr.segment.bytes, ptr.offset))
                return ptr
            tier.cache_records = cache_records
            tier.push_atom = push_atom
        base = {n: tc.storages[n].stats() for n in tc.ids}
        before = {n: tc[n].stats()["critical_commits"] for n in tc.ids}
        base_coord = {n: tc[n].stats() for n in tc.ids}
        calls.clear()
        outcomes = []
        misses = 0          # critical-section misses of commits whose base was warmed

        def critical_misses():
            return sum(tc.storages[n].stats()["critical_misses"] for n in tc.ids)

        def warmed(node, records):
            return all(set(records) <= cached[m] for m in tc.ids if m != node)

        steady = True       # the previous commit was acknowledged and its records reached every member
        for i in range(30):
            node = tc.ids[i % len(tc.ids)]
            pushed[node].clear()
            start = critical_misses()
            result = {}

            def committer(_node=node):
                try:
                    tc.db(_node).run(incr, **RUN_KW)
                    result["outcome"] = "acked"
                except ProtoOutcomeUnknownError:
                    result["outcome"] = "unknown"   # P5 / R86: a conservative outcome, resolved below
            t = threading.Thread(target=committer, name=f"committer-{node}")
            t.start()
            t.join(60)
            assert "outcome" in result, f"commit {i} on {node} raised or hung"
            outcomes.append(result["outcome"])
            if steady:
                misses += critical_misses() - start
            steady = result["outcome"] == "acked"
            if steady:
                records = list(pushed[node])
                assert records, f"commit {i} pushed nothing: the warming wait would prove nothing"
                deadline = time.monotonic() + 10
                while not warmed(node, records):            # an observable condition, not a delay
                    assert time.monotonic() < deadline, f"commit {i}: its records never reached every member"
                    time.sleep(0.002)
        acked, unknown = outcomes.count("acked"), outcomes.count("unknown")
        # R86: every acknowledged commit is applied; an unknown one at most once.
        assert unknown <= 3, outcomes                                     # a generous progress bound
        final = None
        deadline = time.monotonic() + 10
        while final is None or final < 6 + acked:
            assert time.monotonic() < deadline, (final, outcomes)
            final = tc.value("n0")
            time.sleep(0.01)
        assert 6 + acked <= final <= 6 + acked + unknown, (final, outcomes)
        committer_calls = [op for name, op in calls if name.startswith("committer-")]
        assert set(committer_calls) <= {"get"}, committer_calls[:5]       # reads only, never writes
        for n in tc.ids:
            assert tc.storages[n].stats()["root_writes_in_critical"] == 0
        fetched = {k: sum(tc[n].stats()[k] - base_coord[n][k] for n in tc.ids)
                   for k in ("record_fetches", "record_fetches_permitted", "records_warmed")}
        total_misses = critical_misses() - sum(base[n]["critical_misses"] for n in tc.ids)
        print(f"\n30 commits ({acked} acked, {unknown} unknown): {misses} critical-section cache misses "
              f"after warmed bases ({total_misses} in all), {len(committer_calls)} store reads by "
              f"committing threads, {fetched}")
        # Review M2: recorded bounds -- near zero in steady state under R89, asserted once the
        # base's records reached every member (R105: the warming is waited for, never timed).
        assert misses <= 3, misses
        assert len(committer_calls) <= 3, committer_calls
        assert fetched["record_fetches_permitted"] <= 3, fetched
        assert fetched["records_warmed"] > 0, fetched
        critical = sum(tc[n].stats()["critical_commits"] - before[n] for n in tc.ids)
        assert acked <= critical <= acked + unknown, (critical, outcomes)


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
