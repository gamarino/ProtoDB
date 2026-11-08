"""Contention resolved among the peers, never in the storage (Tasks 8.3c and 8.3d, D44-D46).

Four in-process peers, each with its own storage instance over one shared space (as four
processes would have: a tiered storage per node over one object store, or one shared
directory), run four threads each.  Every ``Database.run`` attempt increments
a plain ``int`` root (read set: concurrent commits conflict and are retried) and adds a
unique id to a ``CountedSet`` root written blind (intent merge).  Checks:

* no lost and no duplicated update (ruling R86): every acknowledged id is present exactly
  once, every other id present is one whose commit ended with ``ProtoOutcomeUnknownError``
  (a conservative outcome, P5: it may or may not take effect), no id is present twice, and
  the count equals the ids present, on every member (the ids are the commits' tokens,
  :mod:`tests.support.outcomes`).  A worker that gets
  an unknown outcome records it and goes on with its next id (never a blind retry of the
  same one).  Progress: every peer acknowledged at least one commit;
* no committing thread wrote a storage root (only persister threads did, D46), no root
  write was unconditional, every replication failure surfaced as an unknown outcome or, when
  nothing a voter may accept left the node (rule H7), as a conflict ``Database.run`` retries
  (that branch is forced deterministically in the last test), and every node stayed healthy:
  contention never reached the storage.

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

from proto_db.errors import ProtoConflictError, ProtoOutcomeUnknownError
from proto_db.storage.cloud import CloudStorage
from proto_db.storage.file import FileStorage
from proto_db.storage.tiered import TieredStorage
from proto_db.transactions import ObjectSpace
from tests.concurrency.harness import run_threads
from tests.support.cluster import CasCountingStorage, Cluster
from tests.support.fake_object_store import FakeObjectStore
from tests.support.outcomes import CommitTokens, add_blind_token, assert_outcomes_resolved, blind_snapshot_of

PEERS = 4
THREADS = 4
PER_THREAD = 12
RUN_KW = dict(retries=10_000, backoff=(0.0005, 0.01))
CLUSTER_KW = dict(lease_duration=2.0, request_timeout=0.2, startup_grace=0.0,
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
    acked, unknown = {}, {}               # uid -> peer index, per outcome
    outcomes = threading.Lock()
    with Cluster(PEERS, **CLUSTER_KW) as cl:
        spaces = [ObjectSpace(s, coordinator=cl[n]) for s, n in zip(storages, cl.ids)]
        cl.wait_rejoined()      # every first start done first (a late member waits, R51)
        spaces[0].new_database("db")
        dbs = [s.open_database("db") for s in spaces]
        refused = _count_failed_replications(cl)

        def worker_for(peer):
            db = dbs[peer]

            def worker():
                for _ in range(PER_THREAD):
                    uid = next(ids)

                    def fn(tr):
                        tr.set_root_object("n", (tr.get_root_object("n") or 0) + 1)
                        tr.set_root_object("ids", tr.new_counted_set().add(uid))
                    try:
                        db.run(fn, **RUN_KW)
                        outcome = acked
                    except ProtoOutcomeUnknownError:
                        outcome = unknown     # conservative (R86): recorded, never retried blindly
                    with outcomes:
                        outcome[uid] = peer
            return worker

        elapsed = run_threads([worker_for(i % PEERS) for i in range(PEERS * THREADS)])
        stats = [cl[n].stats() for n in cl.ids]
        _settle(cl)
        total = PEERS * THREADS * PER_THREAD
        assert len(acked) + len(unknown) == total
        # Every outcome resolved by its id on EVERY member (R114, Task 8.3n): no acknowledged id
        # lost, each unknown present everywhere or nowhere, none doubled or stray; the count,
        # read in the same snapshot as the ids, moved with them.
        tokens = CommitTokens()
        for uid in acked:
            tokens.acked(uid)
        for uid in unknown:
            tokens.unknown(uid)
        res = assert_outcomes_resolved(
            {i: (lambda db=db: blind_snapshot_of(db, lambda tr: tr.get_root_object("n"), root="ids"))
             for i, db in enumerate(dbs)}, tokens)
        present = res.present
        extra = set(present) - set(acked)
        assert set(res.values.values()) == {len(present)}, (res.values, len(present))
        assert set(acked.values()) == set(range(PEERS)), "a peer acknowledged no commit"     # progress
        spaces[1].sync(timeout=30)
        _storage_untouched_by_commits(storages, spaces)
        for space in spaces:
            space.close()
    _assert_failures_surfaced(sum(st["replication_failures"] for st in stats), refused, len(unknown))
    assert all(st["healthy"] for st in stats)
    print(f"\n[{backend}] {total} increments by {PEERS}x{THREADS} threads in {elapsed:.2f}s "
          f"({len(acked)} acknowledged, {len(unknown)} unknown, {len(extra)} of those took effect); "
          f"lease attempts {sum(st['attempts'] for st in stats)}, "
          f"acquired {sum(st['acquired'] for st in stats)}, "
          f"storage root reads {sum(st['storage_root_reads'] for st in stats)}")


def _assert_failures_surfaced(failures: int, refused: dict, unknown: int) -> None:
    """The replication-failure bound: every failed replication raised, as an unknown outcome
    or -- when nothing a voter may accept left the node (rule H7) -- as a retryable conflict
    that ``Database.run`` retries; never silently.  Every unknown outcome raised reached a
    commit, which recorded it (one round may carry several commits).  ``refused`` comes from
    :func:`_count_failed_replications`; the TCP test applies the same bound per peer."""
    print(f"\nreplication failures {failures}, raised as unknown {refused['unknown']}, as conflicts "
          f"{refused['conflict']}; unknown outcomes {unknown}")
    assert failures <= refused["unknown"] + refused["conflict"], (failures, refused)
    assert refused["unknown"] <= unknown, (refused, unknown)


def _count_failed_replications(cl) -> dict:
    """Count, per outcome, the replications that raised on every member of ``cl``."""
    counts = {"unknown": 0, "conflict": 0}
    lock = threading.Lock()
    for node in cl.ids:
        coordinator = cl[node]
        real = coordinator.replicate

        def replicate(*a, _real=real, **kw):
            try:
                return _real(*a, **kw)
            except ProtoOutcomeUnknownError:
                with lock:
                    counts["unknown"] += 1
                raise
            except ProtoConflictError:
                with lock:
                    counts["conflict"] += 1
                raise
        coordinator.replicate = replicate
    return counts


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
        tokens = CommitTokens()
        refused = _count_failed_replications(cl)

        def worker_for(db):
            def worker():
                if not stop_at:
                    stop_at.append(time.monotonic() + seconds)
                commits = 0
                while time.monotonic() < stop_at[0]:
                    uid = next(counter)
                    try:
                        db.run(lambda tr: add_blind_token(tr, uid, root="ids"), **RUN_KW)
                    except ProtoOutcomeUnknownError:
                        tokens.unknown(uid)   # P5 / R86: resolved below, never retried blindly
                        continue
                    tokens.acked(uid)
                    commits += 1
                done.append(commits)
            return worker

        elapsed = run_threads([worker_for(dbs[i % peers]) for i in range(peers * threads)])
        failures = sum(cl[n].stats()["replication_failures"] for n in cl.ids)
        critical = [cl[n].stats() for n in cl.ids]
        _settle(cl)                    # the last commits' releases reached node 0
        commits = sum(done)
        # No lost update, on every member: every acknowledged id once, every unknown one resolved
        # alike, no other (R86, R114; Task 8.3n: before, any unknown outcome killed a worker).
        assert_outcomes_resolved({i: (lambda db=db: blind_snapshot_of(db, root="ids")) for i, db in enumerate(dbs)},
                                 tokens)
        _storage_untouched_by_commits(storages, spaces)
        for space in spaces:
            space.close()
    # Review M5: the figure behind the bound (it was ``failures == 0`` before Task 8.3n;
    # ``failures <= unknown`` alone failed once in the 8.3n fix-round-1 proof: 2 failures, both
    # H7 conflicts, 0 unknown outcomes).  The branch with failures is forced in
    # ``test_forced_replication_failures_surface_and_lose_nothing``.
    _assert_failures_surfaced(failures, refused, len(tokens.unknown_tokens))
    if breakdown is not None:
        n = sum(st["critical_commits"] for st in critical) or 1
        for key in ("validation", "replay", "path_copy", "replication"):
            breakdown[key] = sum(st["critical"][key] for st in critical) / n
    return commits / elapsed


def _delay_releases(node, gate):
    """Make ``node``'s worker handle winners' releases only once ``gate`` is set (the wait
    releases the coordinator lock, as a slow worker would not hold it)."""
    original = node._on_release
    original_done = node._on_done

    def delayed(sender, msg):
        if msg["held"]:
            node._cond.wait_for(gate.is_set, timeout=10.0)
        return original(sender, msg)

    def delayed_done(msg):
        # D50: a write permission's end ("done") carries the committed root like a release.
        if msg["outcome"] == "committed":
            with node._cond:
                node._cond.wait_for(gate.is_set, timeout=10.0)
        return original_done(msg)

    node._on_release = delayed
    node._on_done = delayed_done


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


def test_forced_replication_failures_surface_and_lose_nothing(tmp_path):
    """Task 8.3n fix round 2, item 3: the branch of the replication-failure bound that no run
    of the workloads above reached (every run after db68f49 had no failed replication), with
    one failure of each kind forced, deterministically.  The schedule is that of
    ``test_a_failing_predecessor_turns_its_successor_into_a_retried_conflict``:

    * n0's round proposes its root, its records never leave n0, and its own sync fails once
      the data stage was sent: an unknown outcome (the root may have left, P5);
    * n1's pipelined permission, issued on that proposal, sends its ``store``; its gate never
      opens, so nothing a voter may accept leaves n1 (rule H7): a conflict, which
      ``Database.run`` retries in a later term until it is acknowledged.

    Each commit adds its token blind, as in the throughput workload.  The bound holds with
    both kinds counted (``failures <= unknown + conflict``; the old ``failures <= unknown``
    fails here), and every token resolves alike on every member: a lost acknowledged commit
    still fails the test in this branch (plant ``forced_lost``)."""
    from tests.concurrency.test_cluster_pipelined_permits import _await, _cluster as _pipelined_cluster, _confirmed
    from tests.support.tiered_cluster import RUN_KW as TIERED_RUN_KW, incr

    tokens = CommitTokens()
    with _pipelined_cluster(tmp_path) as tc:
        tc.commit("n0", incr)
        _confirmed(tc)
        refused = _count_failed_replications(tc.cluster)
        a, b = tc["n0"], tc["n1"]
        hold = threading.Event()

        def failing_own(*args, **kw):
            hold.wait(5)
            raise OSError("the holder's own sync failed")
        a._self_accept_with_records = failing_own
        send = a.transport.send
        a.transport.send = lambda to, msg: None if msg["type"] in ("records", "replicate") else send(to, msg)
        outcomes = {}

        def run(node):
            token = tokens.new(node)
            try:
                tc.db(node).run(lambda tr: add_blind_token(tr, token, root="ids"), **TIERED_RUN_KW)
                tokens.acked(token)
                outcomes[node] = "acked"
            except ProtoOutcomeUnknownError:
                tokens.unknown(token)
                outcomes[node] = "unknown"
        proposed = a.stats()["proposals"]
        ta = threading.Thread(target=run, args=("n0",))
        ta.start()
        _await(lambda: a.stats()["proposals"] > proposed, what="n0's proposal")
        tb = threading.Thread(target=run, args=("n1",))
        tb.start()
        _await(lambda: b.stats()["stores_sent"] >= 1, what="n1's store", tc=tc)
        hold.set()
        ta.join(30)
        a.transport.send = send
        tb.join(60)
        assert not ta.is_alive() and not tb.is_alive(), "a commit hung"
        failures = sum(tc[n].stats()["replication_failures"] for n in tc.ids)
        # The schedule ran: n0's round ended unknown, n1's gate closed and its retry committed.
        assert outcomes == {"n0": "unknown", "n1": "acked"}, outcomes
        assert b.stats()["gate_closures"] >= 1
        assert refused["unknown"] >= 1 and refused["conflict"] >= 1, refused
        _assert_failures_surfaced(failures, refused, len(tokens.unknown_tokens))
        # n0's tier failed with its own sync: the space is reopened on a healthy disk (module
        # docstring of the coordinator, "Health"), then every member is read.
        tc.kill("n0")
        tc.revive("n0")
        assert_outcomes_resolved({n: (lambda n=n: blind_snapshot_of(tc.db(n), root="ids")) for n in tc.ids}, tokens)
