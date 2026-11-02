"""Contention resolved among the peers, never in the storage (Tasks 8.3c and 8.3d, D44-D46).

Four in-process peers, each with its own storage instance over one shared space (as four
processes would have: a tiered storage per node over one object store, or one shared
directory), run four threads each.  Every ``Database.run`` attempt increments
a plain ``int`` root (read set: concurrent commits conflict and are retried) and adds a
unique id to a ``CountedSet`` root written blind (intent merge).  Checks:

* the final count is exact and every committed id is present exactly once (no lost and
  no duplicated update);
* no committing thread wrote a storage root (only persister threads did, D46), no root
  write was unconditional, no replication failed (no commit with an unknown outcome) and
  every node stayed healthy: contention never reached the storage.

A node learns that another node's commit is committed from that commit's release, handled
by its worker; a read on a node is therefore stale until then (never lost).  The checks
read only once every node knows the same committed root (``_settle``), and a
deterministic test (a delayed release) shows both the stale read and its end.

A second test measures commits per second with 1, 2 and 4 peers (blind adds only, so the
figure is the lease round trip plus majority replication, not conflict retries).  A third
compares, with 30 ms injected object-store latency, the tiered cluster against the
single-writer CAS-only path (every commit writes the root object).  The numbers are
printed for the Phase 9 record.
"""
import itertools
import threading
import time

import pytest

from proto_db.storage.cloud import CloudStorage
from proto_db.storage.file import FileStorage
from proto_db.storage.tiered import TieredStorage
from proto_db.transactions import ObjectSpace
from tests.concurrency.harness import run_threads
from tests.support.cluster import CasCountingStorage, Cluster
from tests.support.fake_object_store import FakeObjectStore

PEERS = 4
THREADS = 4
PER_THREAD = 12
RUN_KW = dict(retries=10_000, backoff=(0.0005, 0.01))
CLUSTER_KW = dict(lease_duration=2.0, drift_margin=0.2, request_timeout=0.2, startup_grace=0.0,
                  backoff=(0.001, 0.02))

pytestmark = [pytest.mark.freethreaded, pytest.mark.timeout(240)]


def _tiered_storages(tmp_path, n, store=None):
    store = store if store is not None else FakeObjectStore()
    return store, [CasCountingStorage(TieredStorage(str(tmp_path / f"n{i}"), store, durability="none",
                                                    ship_interval=0.05, backoff=(0.001, 0.01)))
                   for i in range(n)]


def _file_storages(tmp_path, n):
    return None, [CasCountingStorage(FileStorage(str(tmp_path / "space"), durability="none")) for _ in range(n)]


BACKENDS = {"tiered": _tiered_storages, "file": _file_storages}


def _storage_untouched_by_commits(storages, spaces):
    """No committing thread published a root; the persisters' writes are all conditional."""
    for s in storages:
        assert s.unconditional == 0
        assert all(name.startswith("proto-persister-") for name in s.publishers), s.publishers
    for space in spaces:
        stats = getattr(space.storage, "stats", None)
        if callable(stats) and "root_writes_in_critical" in stats():
            assert stats()["root_writes_in_critical"] == 0


def _settle(cl, timeout=15.0):
    """Wait until every node has handled its queued messages and knows the same committed
    root: the last commit's release reached everyone."""
    deadline = time.monotonic() + timeout
    while True:
        idle = all(cl[n].wait_idle(timeout=0.0) for n in cl.ids)
        roots = {cl[n].committed_root()[:3] for n in cl.ids}
        if idle and len(roots) == 1:
            return
        assert time.monotonic() < deadline, f"the cluster did not settle: {roots}"
        time.sleep(0.005)


def _read(space):
    tr = space.open_database("db").new_transaction()
    try:
        return tr.get_root_object("n"), tr.get_root_object("ids")
    finally:
        tr.abort()


@pytest.mark.parametrize("backend", sorted(BACKENDS))
def test_contending_peers_never_reach_the_storage(tmp_path, backend):
    store, storages = BACKENDS[backend](tmp_path, PEERS)
    ids = itertools.count()
    with Cluster(PEERS, **CLUSTER_KW) as cl:
        spaces = [ObjectSpace(s, coordinator=cl[n]) for s, n in zip(storages, cl.ids)]
        cl.wait_rejoined()      # every first start done first (a late member waits, R51)
        spaces[0].new_database("db")
        dbs = [s.open_database("db") for s in spaces]

        def worker_for(db):
            def worker():
                for _ in range(PER_THREAD):
                    uid = next(ids)

                    def fn(tr):
                        tr.set_root_object("n", (tr.get_root_object("n") or 0) + 1)
                        tr.set_root_object("ids", tr.new_counted_set().add(uid))
                    db.run(fn, **RUN_KW)
            return worker

        elapsed = run_threads([worker_for(dbs[i % PEERS]) for i in range(PEERS * THREADS)])
        stats = [cl[n].stats() for n in cl.ids]
        _settle(cl)
        n, counted = _read(spaces[1])
        total = PEERS * THREADS * PER_THREAD
        assert n == total
        assert sorted(counted.elements()) == list(range(total))
        assert all(counted.count_of(i) == 1 for i in range(total))
        spaces[1].sync(timeout=30)
        _storage_untouched_by_commits(storages, spaces)
        for space in spaces:
            space.close()
    assert sum(st["replication_failures"] for st in stats) == 0
    assert all(st["healthy"] for st in stats)
    print(f"\n[{backend}] {total} increments by {PEERS}x{THREADS} threads in {elapsed:.2f}s; "
          f"lease attempts {sum(st['attempts'] for st in stats)}, "
          f"acquired {sum(st['acquired'] for st in stats)}, "
          f"storage root reads {sum(st['storage_root_reads'] for st in stats)}")


def _throughput(storages_factory, tmp_path, peers, threads=4, seconds=1.5, breakdown=None):
    store, storages = storages_factory(tmp_path, peers)
    counter = itertools.count()
    with Cluster(peers, **CLUSTER_KW) as cl:
        spaces = [ObjectSpace(s, coordinator=cl[n]) for s, n in zip(storages, cl.ids)]
        cl.wait_rejoined()      # every first start done first (a late member waits, R51)
        spaces[0].new_database("db")
        dbs = [s.open_database("db") for s in spaces]
        stop_at = []
        done = []

        def worker_for(db):
            def worker():
                if not stop_at:
                    stop_at.append(time.monotonic() + seconds)
                commits = 0
                while time.monotonic() < stop_at[0]:
                    uid = next(counter)
                    db.run(lambda tr: tr.set_root_object("ids", tr.new_counted_set().add(uid)), **RUN_KW)
                    commits += 1
                done.append(commits)
            return worker

        elapsed = run_threads([worker_for(dbs[i % peers]) for i in range(peers * threads)])
        failures = sum(cl[n].stats()["replication_failures"] for n in cl.ids)
        critical = [cl[n].stats() for n in cl.ids]
        _settle(cl)                    # the last commits' releases reached node 0
        _, counted = _read(spaces[0])
        commits = sum(done)
        # No lost update: the final state holds exactly the acknowledged commits.
        assert sorted(counted.elements()) == list(range(commits))     # every committed id, no other
        assert all(c == 1 for _, c in counted.items())                 # none applied twice
        _storage_untouched_by_commits(storages, spaces)
        for space in spaces:
            space.close()
    assert failures == 0
    if breakdown is not None:
        n = sum(st["critical_commits"] for st in critical) or 1
        for key in ("validation", "replay", "path_copy", "replication"):
            breakdown[key] = sum(st["critical"][key] for st in critical) / n
    return commits / elapsed


def _delay_releases(node, gate):
    """Make ``node``'s worker handle winners' releases only once ``gate`` is set (the wait
    releases the coordinator lock, as a slow worker would not hold it)."""
    original = node._on_release

    def delayed(sender, msg):
        if msg["held"]:
            node._cond.wait_for(gate.is_set, timeout=10.0)
        return original(sender, msg)

    node._on_release = delayed


def test_a_read_before_the_release_is_handled_is_stale_never_lost(tmp_path):
    """Root cause of the one-off ``test_throughput_with_1_2_4_peers[tiered]`` failure (8.3d
    re-review): the check read node 0 right after the threads joined, while node 0's worker
    had not yet handled the release of the last commit made on another node.  Delaying that
    release deterministically reproduces the missing id; once it is handled the id is there."""
    store, storages = _tiered_storages(tmp_path, 2)
    with Cluster(2, **CLUSTER_KW) as cl:
        spaces = [ObjectSpace(s, coordinator=cl[n]) for s, n in zip(storages, cl.ids)]
        cl.wait_rejoined()      # every first start done first (a late member waits, R51)
        spaces[0].new_database("db")
        dbs = [s.open_database("db") for s in spaces]
        dbs[1].run(lambda tr: tr.set_root_object("ids", tr.new_counted_set().add(0)), **RUN_KW)
        _settle(cl)
        gate = threading.Event()
        _delay_releases(cl["n0"], gate)
        try:
            dbs[1].run(lambda tr: tr.set_root_object("ids", tr.new_counted_set().add(1)), **RUN_KW)
            assert cl["n0"].accepted_root()[1] == cl["n1"].committed_root()[1]   # n0 acknowledged it
            _, counted = _read(spaces[0])
            assert sorted(counted.elements()) == [0]     # acknowledged, not yet known committed at n0
        finally:
            gate.set()
            with cl["n0"]._cond:
                cl["n0"]._cond.notify_all()
        _settle(cl)
        _, counted = _read(spaces[0])
        assert sorted(counted.elements()) == [0, 1]      # stale, never lost
        for space in spaces:
            space.close()


@pytest.mark.parametrize("backend", sorted(BACKENDS))
def test_throughput_with_1_2_4_peers(tmp_path, backend, capsys):
    rates = {}
    for peers in (1, 2, 4):
        rates[peers] = _throughput(BACKENDS[backend], tmp_path / f"p{peers}", peers)
    with capsys.disabled():
        print(f"\nTHROUGHPUT in-process [{backend}] commits/s: "
              + ", ".join(f"{p} peer(s)={r:.0f}" for p, r in rates.items()))
    assert all(r > 0 for r in rates.values())


def _cas_only_rate(store, seconds=1.5):
    """Commits per second of one CloudStorage writer without a cluster: each commit uploads its
    segment and compare-and-swaps the root object on the commit path."""
    space = ObjectSpace(CloudStorage(store, backoff=(0.0, 0.001)))
    db = space.new_database("db")
    commits, stop_at, started = 0, time.monotonic() + seconds, time.perf_counter()
    while time.monotonic() < stop_at:
        db.run(lambda tr: tr.set_root_object("n", (tr.get_root_object("n") or 0) + 1), **RUN_KW)
        commits += 1
    elapsed = time.perf_counter() - started
    space.close()
    return commits / elapsed


def test_throughput_with_a_30ms_object_store(tmp_path, capsys):
    """Informational (Phase 9 record): the store's latency is off the cluster's commit path."""
    slow = FakeObjectStore()
    slow.latency = 0.030
    breakdown = {}
    tiered = _throughput(lambda p, n: _tiered_storages(p, n, store=slow), tmp_path / "tiered", 3,
                         breakdown=breakdown)
    cas_store = FakeObjectStore()
    cas_store.latency = 0.030
    cas = _cas_only_rate(cas_store)
    with capsys.disabled():
        print(f"\nTHROUGHPUT with 30 ms store latency: tiered cluster (3 peers x 4 threads) {tiered:.0f} "
              f"commits/s; CAS-only single writer {cas:.1f} commits/s")
        print("CRITICAL SECTION mean per commit (ms): "
              + ", ".join(f"{k}={v * 1000:.3f}" for k, v in breakdown.items()))
    assert tiered > cas
