"""Contention resolved among the peers, never in the storage (Task 8.3c, D44, D45).

Four in-process peers, each with its own storage instance over one shared space (as four
processes would have), run four threads each.  Every ``Database.run`` attempt increments
a plain ``int`` root (read set: concurrent commits conflict and are retried) and adds a
unique id to a ``CountedSet`` root written blind (intent merge).  Checks:

* the final count is exact and every committed id is present exactly once (no lost and
  no duplicated update);
* the storage saw zero failed conditional root writes (``FakeObjectStore``'s precondition
  counter for the cloud space, a counting proxy for the file space) and the coordinators
  saw zero fencing events: contention never reached the storage.

A second test measures commits per second with 1, 2 and 4 peers (blind adds only, so the
figure is the lease round trip plus the root publish, not conflict retries); the numbers
are printed for the Phase 9 record.
"""
import itertools
import time

import pytest

from proto_db.storage.cloud import CloudStorage
from proto_db.storage.file import FileStorage
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


def _cloud_storages(tmp_path, n):
    store = FakeObjectStore()
    return store, [CasCountingStorage(CloudStorage(store, backoff=(0.0, 0.001))) for _ in range(n)]


def _file_storages(tmp_path, n):
    return None, [CasCountingStorage(FileStorage(str(tmp_path / "space"), durability="none")) for _ in range(n)]


BACKENDS = {"cloud": _cloud_storages, "file": _file_storages}


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
        n, counted = _read(spaces[1])
    total = PEERS * THREADS * PER_THREAD
    assert n == total
    assert sorted(counted.elements()) == list(range(total))
    assert all(counted.count_of(i) == 1 for i in range(total))
    assert sum(s.failed_cas for s in storages) == 0
    assert sum(s.unconditional for s in storages) == 0
    assert sum(st["fencing_events"] for st in stats) == 0
    if store is not None:
        assert store.failed_preconditions.get(storages[0].root_key, 0) == 0
    print(f"\n[{backend}] {total} increments by {PEERS}x{THREADS} threads in {elapsed:.2f}s; "
          f"lease attempts {sum(st['attempts'] for st in stats)}, "
          f"acquired {sum(st['acquired'] for st in stats)}, "
          f"storage root reads {sum(st['storage_root_reads'] for st in stats)}")


def _throughput(storages_factory, tmp_path, peers, threads=4, seconds=1.5):
    store, storages = storages_factory(tmp_path, peers)
    counter = itertools.count()
    with Cluster(peers, **CLUSTER_KW) as cl:
        spaces = [ObjectSpace(s, coordinator=cl[n]) for s, n in zip(storages, cl.ids)]
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
        fencing = sum(cl[n].stats()["fencing_events"] for n in cl.ids)
        _, counted = _read(spaces[0])
    commits = sum(done)
    assert sorted(counted.elements()) == list(range(commits))     # every committed id, no other
    assert all(c == 1 for _, c in counted.items())                 # none applied twice
    assert sum(s.failed_cas for s in storages) == 0 and fencing == 0
    return commits / elapsed


@pytest.mark.parametrize("backend", sorted(BACKENDS))
def test_throughput_with_1_2_4_peers(tmp_path, backend, capsys):
    rates = {}
    for peers in (1, 2, 4):
        rates[peers] = _throughput(BACKENDS[backend], tmp_path / f"p{peers}", peers)
    with capsys.disabled():
        print(f"\nTHROUGHPUT in-process [{backend}] commits/s: "
              + ", ".join(f"{p} peer(s)={r:.0f}" for p, r in rates.items()))
    assert all(r > 0 for r in rates.values())
