"""Cluster commits over tiered storage: the object store off the commit path (Task 8.3d, D46)."""
import tempfile
import threading
import time

import pytest

from proto_db.errors import ProtoOutcomeUnknownError, ProtoTimeoutError
from proto_db.storage.tiered import TieredStorage
from proto_db.transactions import ObjectSpace
from tests.support.fake_object_store import FakeObjectStore
from tests.support.outcomes import CommitTokens, assert_outcomes_resolved, read_tokens, snapshot_of, write_token
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


#: Bounds of the concurrent schedule, about twice the worst of 40 measured runs (re-review 8.3m
#: N3): misses and record fetches after warmed rounds 0-5 per run with the GIL off, 2-11 with it
#: on; with warming observed but nothing stored (the reviewer's plant) 66-98 misses and 177-207
#: fetches.  Log: benchmarks/results/2026-11-06-8.3m-cluster-test-proofs.log.
CONCURRENT_MISS_BOUND = 24
CONCURRENT_FETCH_BOUND = 24


def _commit(tc, nodes, count):
    for i in range(count):
        tc.commit(nodes[i % len(nodes)], incr)


def _wait(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            return False
        time.sleep(0.01)
    return True


def test_commits_are_persisted_asynchronously_and_sync_waits_for_them(tmp_path):
    with TieredCluster(tmp_path) as tc:
        tc.create_database("n0")
        _commit(tc, tc.ids, 12)
        space = tc.spaces["n2"]                          # the last committer (others learn it on release)
        space.sync(timeout=10)
        # At least one epoch per commit: a re-commit of a base under a new lease adds one with the
        # same data (``_commit_base``), so the count is a floor, not an equality (Task 11.1, A3:
        # 14 under load).  The store's value below counts the commits exactly.
        assert space.persisted_epoch >= space.committed_epoch >= 13
        assert _remote_value(tc.store) == 12
        assert all(tc.storages[n].stats()["root_writes_in_critical"] == 0 for n in tc.ids)


def test_an_object_store_outage_never_stops_commits_and_catch_up_converges(tmp_path):
    with TieredCluster(tmp_path) as tc:
        tc.create_database("n0")
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
        tc.create_database("n0")
        _commit(tc, ["n0", "n1"], 5)
        tc.store.set_outage(True)
        _commit(tc, ["n0", "n1"], 5)
        tc.wipe("n2")
        tc.revive("n2")
        assert tc.eventually("n2", 10)                     # root and records from the peers
        assert tc.storages["n2"].stats()["reads_remote"] == 0
        tc.commit("n2", incr)
        assert tc.eventually("n0", 11)
        tc.store.set_outage(False)
        tc.spaces["n2"].sync(timeout=15)
        assert _remote_value(tc.store) == 11


def test_a_restarted_cluster_on_empty_disks_recovers_from_the_object_store(tmp_path):
    store = FakeObjectStore()
    with TieredCluster(tmp_path / "a", store=store) as tc:
        tc.create_database("n0")
        _commit(tc, tc.ids, 9)
        for n in tc.ids:
            tc.spaces[n].sync(timeout=10)
    with TieredCluster(tmp_path / "b", store=store) as tc:
        assert tc.eventually("n1", 9)                       # the persisted root
        assert tc.storages["n1"].stats()["reads_remote"] > 0
        tc.commit("n2", incr)
        assert tc.eventually("n0", 10)
        tc.spaces["n2"].sync(timeout=10)
        assert _remote_value(store) == 10


def test_a_node_restart_loses_nothing(tmp_path):
    with TieredCluster(tmp_path) as tc:
        tc.create_database("n0")
        tc.store.set_outage(True)                        # nothing is persisted meanwhile
        _commit(tc, tc.ids, 9)
        for n in tc.ids:                                 # every node restarts, one at a time
            tc.kill(n)
            tc.revive(n)
            tc.commit(n, incr)
        assert tc.eventually("n0", 12) and tc.eventually("n2", 12)
        tc.store.set_outage(False)
        tc.spaces["n0"].sync(timeout=15)
        assert _remote_value(tc.store) == 12


def test_the_whole_cluster_restarting_on_its_disks_loses_nothing_unpersisted(tmp_path):
    store = FakeObjectStore()
    with TieredCluster(tmp_path, store=store) as tc:
        tc.create_database("n0")
        store.set_outage(True)
        _commit(tc, tc.ids, 7)
    store.set_outage(False)
    assert _remote_value(store) is None or _remote_value(store) < 7
    # Same local directories, so the default startup grace (a shorter one is for a cluster's first
    # start only).
    with TieredCluster(tmp_path, store=store, startup_grace=None) as tc:
        assert tc.eventually("n1", 7)
        tc.commit("n1", incr)
        tc.spaces["n1"].sync(timeout=15)
        assert _remote_value(store) == 8


def test_losing_a_minority_loses_nothing(tmp_path):
    with TieredCluster(tmp_path) as tc:
        tc.create_database("n0")
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
        tc.create_database("n0")
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
        tc.commit("n2", incr)
        assert tc.eventually("n1", 10)
        tc.store.set_outage(False)
        tc.spaces["n1"].sync(timeout=15)
        assert _remote_value(tc.store) == 10


def test_losing_a_majority_with_the_survivor_outside_the_quorum_rolls_back_to_the_persisted_state(tmp_path):
    with TieredCluster(tmp_path) as tc:
        tc.create_database("n0")
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
        tc.commit("n2", incr)
        tc.revive("n1")                                  # the survivor's newer roots are superseded
        assert tc.eventually("n1", 5) and tc.eventually("n0", 5)
        tc.spaces["n2"].sync(timeout=15)
        assert _remote_value(tc.store) == 5


def _steady_state(tmp_path, rounds: int, per_round: int) -> dict:
    """Commit ``rounds`` rounds of ``per_round`` concurrent commits on distinct members (one
    thread each, named ``committer-<member>``), each an increment plus a unique token (R114 N1),
    after 6 warm-up commits (with tokens too).  Before each next round, wait until the records every acknowledged
    commit of the round pushed are in every other member's record cache (the observable end of
    R89 warming, R105).  Return the measurements; assert what holds for every schedule."""
    calls = []

    class Recording(FakeObjectStore):
        """Records the thread and the operation of every store call."""

        def _enter(self, operation, key):
            calls.append((threading.current_thread().name, operation))
            super()._enter(operation, key)

    store = Recording()
    tokens = CommitTokens()
    with TieredCluster(tmp_path, store=store) as tc:
        tc.create_database("n0")
        for i in range(6):                               # warm up: with tokens too (P5 / R86)
            token = tokens.new(f"warm{i}")
            try:
                tc.db(tc.ids[i % len(tc.ids)]).run(lambda tr, t=token: (incr(tr), write_token(tr, t)), **RUN_KW)
                tokens.acked(token)
            except ProtoOutcomeUnknownError:
                tokens.unknown(token)
        for n in tc.ids:
            tc.spaces[n].sync(timeout=10)
        # What each member's record cache received (warmed or cached on receipt), and what each
        # member's committing thread pushed in its current attempt.
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
        misses = 0          # critical-section misses of rounds whose bases were warmed
        steady_fetches = 0  # record fetches by those rounds

        def critical_misses():
            return sum(tc.storages[n].stats()["critical_misses"] for n in tc.ids)

        def record_fetches():
            return sum(tc[n].stats()["record_fetches"] for n in tc.ids)

        def warmed(node, records):
            return all(set(records) <= cached[m] for m in tc.ids if m != node)

        steady = True       # every commit of the previous round was acknowledged and warmed
        for r in range(rounds):
            members = [tc.ids[(r * per_round + k) % len(tc.ids)] for k in range(per_round)]
            assert len(set(members)) == per_round
            start, start_fetches = critical_misses(), record_fetches()
            result = {}
            go = threading.Barrier(per_round)

            def committer(_node):
                token = tokens.new(_node)

                def bump(tr):
                    pushed[_node].clear()               # this attempt's records only
                    tokens.observe(read_tokens(tr))     # P1: a committed base is history
                    incr(tr)
                    write_token(tr, token)
                go.wait()
                try:
                    tc.db(_node).run(bump, **RUN_KW)
                    tokens.acked(token)
                    result[_node] = "acked"
                except ProtoOutcomeUnknownError:
                    tokens.unknown(token)               # P5: resolved by its token below (R86)
                    result[_node] = "unknown"
            threads = [threading.Thread(target=committer, args=(m,), name=f"committer-{m}") for m in members]
            for t in threads:
                t.start()
            for t in threads:
                t.join(60)
            assert set(result) == set(members), f"round {r}: a commit raised or hung ({result})"
            outcomes.extend(result.values())
            if steady:
                misses += critical_misses() - start
                steady_fetches += record_fetches() - start_fetches
            steady = all(o == "acked" for o in result.values())
            for m in members:
                if result[m] != "acked":
                    continue
                records = list(pushed[m])
                assert records, f"round {r}: {m} pushed nothing: the warming wait would prove nothing"
                deadline = time.monotonic() + 10
                while not warmed(m, records):               # an observable condition, not a delay
                    assert time.monotonic() < deadline, f"round {r}: {m}'s records never reached every member"
                    time.sleep(0.002)
        acked, unknown = outcomes.count("acked"), outcomes.count("unknown")
        assert unknown <= 3, outcomes                                     # a generous progress bound
        # R114 N1: every outcome resolved by its token, and the counter, read in the same
        # transaction as the tokens (P4), matches them on every member.
        res = assert_outcomes_resolved(
            {n: (lambda n=n: snapshot_of(tc.db(n), lambda tr: tr.get_root_object("n"))) for n in tc.ids}, tokens)
        assert set(res.values.values()) == {len(res.present)}, (res.values, len(res.present), outcomes)
        committer_calls = [op for name, op in calls if name.startswith("committer-")]
        assert set(committer_calls) <= {"get"}, committer_calls[:5]       # reads only, never writes
        for n in tc.ids:
            assert tc.storages[n].stats()["root_writes_in_critical"] == 0
        fetched = {k: sum(tc[n].stats()[k] - base_coord[n][k] for n in tc.ids)
                   for k in ("record_fetches", "record_fetches_permitted", "records_warmed")}
        total_misses = critical_misses() - sum(base[n]["critical_misses"] for n in tc.ids)
        critical = sum(tc[n].stats()["critical_commits"] - before[n] for n in tc.ids)
        assert acked <= critical <= acked + unknown, (critical, outcomes)
        print(f"\n{rounds} x {per_round} commits ({acked} acked, {unknown} unknown): {misses} "
              f"critical-section cache misses and {steady_fetches} record fetches after warmed "
              f"bases ({total_misses} misses in all), {len(committer_calls)} store reads by "
              f"committing threads, {fetched}")
        return {"misses": misses, "steady_fetches": steady_fetches, "fetched": fetched,
                "committer_calls": committer_calls}


def test_steady_state_commits_write_nothing_in_the_object_store_under_the_lease(tmp_path):
    """D46: committing threads never write the object store, and no root is written in the
    critical section.  Since bounded replication (Task 8.3g, D54) a committer that is not a
    data replica of its base reads the base's records on demand -- from a peer, or from the
    store once the replicas released them -- so critical-section cache misses and store
    reads are expected; they are counted and reported, never forbidden (R70a: remote data is
    a normal case).  Ruling R89: every commit's records reach every member's cache with the
    commit (best effort, no durable copy), so in steady state a committer finds its base's
    records cached and fetches next to nothing from a peer under a permission.

    Sequential steady state (review 8.3m I3, ruling R105): the 30 commits run one at a time,
    rotating among the members, and each next commit starts once the records the previous one
    pushed are in every other member's record cache -- an observed condition, never a delay.
    In this schedule a committer fetches what it lacks before its critical section, so the
    live bound is on record fetches (re-review 8.3m N3: with warming observed and nothing
    stored, the misses stay 0 and the fetches are 27); the concurrent schedule is
    :func:`test_concurrent_steady_state_bounds_misses_after_warming`.  Each commit's outcome
    is resolved by its token (P5 / R86, ruling R114 N1)."""
    got = _steady_state(tmp_path, rounds=30, per_round=1)
    assert got["misses"] <= 3, got
    assert got["fetched"]["record_fetches"] <= 3, got["fetched"]
    assert len(got["committer_calls"]) <= 3, got["committer_calls"]
    assert got["fetched"]["record_fetches_permitted"] <= 3, got["fetched"]
    assert got["fetched"]["records_warmed"] > 0, got["fetched"]


def test_concurrent_steady_state_bounds_misses_after_warming(tmp_path):
    """The miss bound under concurrent commits (re-review 8.3m N3, ruling R114): 10 rounds of
    one commit on each of the 3 members at once; before each next round, the records every
    commit of the round pushed are in every other member's cache (observed).  A commit whose
    base is a commit of its own round may miss (R70a: its records are in flight); bases of
    earlier rounds were warmed.  The bounds are on what the rounds after a warmed round
    missed and fetched (:data:`CONCURRENT_MISS_BOUND`, :data:`CONCURRENT_FETCH_BOUND`)."""
    got = _steady_state(tmp_path, rounds=10, per_round=3)
    assert got["misses"] <= CONCURRENT_MISS_BOUND, got
    assert got["steady_fetches"] <= CONCURRENT_FETCH_BOUND, got
    assert len(got["committer_calls"]) <= 3, got["committer_calls"]
    assert got["fetched"]["records_warmed"] > 0, got["fetched"]


def test_the_backlog_limit_refuses_commits_until_the_store_drains_it(tmp_path):
    alerts = []
    kw = dict(backlog_limit_bytes=6000, on_backlog_alert=alerts.append, ship_interval=0.02)
    with TieredCluster(tmp_path, storage_kw=kw) as tc:
        tc.create_database("n0")
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
        tc.commit("n1", incr)                 # other nodes keep committing
        tc.store.set_outage(False)
        assert _wait(lambda: not tc.storages["n0"].backlog()["over_limit"])
        tc.commit("n0", incr)
        assert tc.eventually("n2", 2)


def test_a_fresh_peer_joining_after_persistence_reads_from_the_object_store(tmp_path):
    with TieredCluster(tmp_path) as tc:
        tc.create_database("n0")
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
        tc.create_database("n0")
        _commit(tc, tc.ids, 3)
        tc.kill("n1")
        tc.revive("n1")
        assert ours() - before
    assert _wait(lambda: not (ours() - before), timeout=5), ours() - before
