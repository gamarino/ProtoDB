"""Readers of old roots and of history while writers commit (Task 8.3l, ruling R92, P1).

History links are held as pointers and loaded on demand (``DatabaseRoot.previous``,
``SpaceRoot.cluster_previous``), so the loaded form of an old version can be evicted and
loaded again at any time, by any thread.  Writers commit a counter while readers, with a
small (or no) decoded-atom cache so that evictions happen all the time:

* keep one transaction opened before the writers started and read it again and again: its
  content never changes;
* take fresh snapshots and walk ``previous`` back from them, checking every version they
  pass ("as of" reads): version ``v`` holds ``n == v``, the dictionary entry ``v % 50 == v``
  and the mutable cell ``m == v``;
* list ``Database.history()`` and read the mutable cell of the newest versions.

In cluster mode (three in-process nodes) the writers commit on two nodes and the readers run
on the third, which also walks ``SpaceRoot.cluster_previous``.

Run with the GIL on and off (``-X gil=1``).  Catches: a link that loads a different version
than it was built on, a torn load of a re-loaded old root, and a link whose target cannot be
loaded again once evicted.
"""
import threading

import pytest

from proto_db.errors import ProtoOutcomeUnknownError
from proto_db.storage.file import FileStorage
from proto_db.transactions import ObjectSpace
from tests.concurrency.harness import Attempts, report, run_threads
from tests.support.tiered_cluster import TieredCluster

WRITERS = 3
READERS = 4
COMMITS_PER_WRITER = 60
WALK = 40
RUN_KW = dict(retries=10_000, backoff=(0.001, 0.05))

pytestmark = [pytest.mark.freethreaded, pytest.mark.timeout(180)]


def _setup(tr):
    tr.set_root_object("n", 0)
    tr.set_root_object("d", tr.new_dictionary())
    tr.new_mutable("m", 0)


def _commit(tr):
    """Version ``v`` (counting from the setup commit's version) holds ``n == d[v % 50] == m == v``."""
    n = tr.get_root_object("n") + 1
    tr.set_root_object("n", n)
    tr.set_root_object("d", tr.get_root_object("d").set_at(n % 50, n))
    tr.set_mutable("m", n)


def _check_root(root, base: int, where: str) -> None:
    v = root.version - base
    assert root.objects.get_at("n") == v, f"{where}: n of version {root.version}"
    assert root.objects.get_at("d").get_at(v % 50) == v or v == 0, f"{where}: d of version {root.version}"
    assert root.mutables.get_at("m") == v, f"{where}: m of version {root.version}"


def _walk(root, base: int, where: str) -> int:
    """Check ``root`` and up to ``WALK`` predecessors down to the setup version; return the count."""
    seen = 0
    while root is not None and root.version >= base and seen < WALK:
        _check_root(root, base, where)
        previous = root.previous
        assert previous is None or previous.version == root.version - 1, f"{where}: link of {root.version}"
        root = previous
        seen += 1
    return seen


def _workload(dbs_w, dbs_r, base: int, extra_check=None):
    """Writers on ``dbs_w``, readers on ``dbs_r``; return (elapsed, attempts, versions checked,
    outcomes unknown).  An unknown outcome (cluster mode, timing-bound and conservative, P5)
    is counted, not raised: either way the versions keep the invariant."""
    attempts = Attempts()
    done = threading.Event()
    left = [WRITERS]
    lock = threading.Lock()
    checked = [0]
    unknown = [0]
    old = dbs_r[0].new_transaction()            # opened before the writers start
    old_root = old._db_root
    old_v = old.get_root_object("n")

    def writer(i):
        def body():
            try:
                for _ in range(COMMITS_PER_WRITER):
                    try:
                        attempts.run(dbs_w[i % len(dbs_w)], _commit, **RUN_KW)
                    except ProtoOutcomeUnknownError:
                        with lock:
                            unknown[0] += 1
            finally:
                with lock:
                    left[0] -= 1
                    if not left[0]:
                        done.set()
        return body

    def reader(i):
        def body():
            db = dbs_r[i % len(dbs_r)]
            rounds = 0
            while not done.is_set() or rounds < 2:
                assert old.get_root_object("n") == old_v, "the old snapshot changed"
                _check_root(old_root, base, "old root")
                tr = db.new_transaction()
                try:
                    n = _walk(tr._db_root, base, f"reader {i}")
                    if extra_check is not None:
                        extra_check(tr)
                finally:
                    tr.abort()
                if rounds % 4 == 0:
                    history = db.history()
                    for entry in history[:10]:
                        if entry.version >= base:
                            assert entry.get_mutable("m") == entry.version - base, f"history {entry.version}"
                with lock:
                    checked[0] += n
                rounds += 1
        return body

    elapsed = run_threads([writer(i) for i in range(WRITERS)] + [reader(i) for i in range(READERS)])
    old.abort()
    return elapsed, attempts, checked[0], unknown[0]


@pytest.mark.parametrize("cache", [0, 16])
def test_readers_walk_history_while_writers_commit(tmp_path, cache):
    space = ObjectSpace(FileStorage(str(tmp_path / "s"), durability="none"), atom_cache_entries=cache)
    try:
        db = space.new_database("db")
        db.run(_setup)
        base = db.history()[0].version
        elapsed, attempts, checked, unknown = _workload([db], [db], base)
        assert unknown == 0
        with db.new_transaction() as tr:
            assert tr.get_root_object("n") == WRITERS * COMMITS_PER_WRITER
            assert _walk(tr._db_root, base, "final") == WALK
        report(f"history readers, cache {cache}", elapsed, attempts, f"{checked} versions checked")
    finally:
        space.close()


def test_cluster_readers_walk_both_links_while_two_nodes_commit(tmp_path):
    cluster = TieredCluster(tmp_path, 3, lease_duration=3.0, space_kw=dict(atom_cache_entries=16))
    try:
        cluster.spaces["n0"].new_database("db")
        cluster.db("n0").run(_setup, **RUN_KW)
        base = cluster.db("n0").history()[0].version
        assert cluster.eventually("n2", 0)

        def space_chain(tr):
            # The cluster link: each committed space root names the one it was built on,
            # loaded again on demand; the database root it holds is one version older or equal.
            space = tr._snapshot_space
            for _ in range(10):
                previous = space.cluster_previous
                if previous is None:
                    break
                assert previous.cluster_epoch < space.cluster_epoch
                assert previous.databases.get_at("db").version <= space.databases.get_at("db").version
                space = previous

        elapsed, attempts, checked, unknown = _workload([cluster.db("n0"), cluster.db("n1")],
                                                        [cluster.db("n2")], base, space_chain)
        committed = attempts.commits
        cluster.db("n0").run(lambda tr: None, **RUN_KW)     # learn the newest committed root
        final = cluster.value("n0")
        assert committed <= final <= committed + unknown
        assert cluster.eventually("n2", final, timeout=10.0)
        report("cluster history readers", elapsed, attempts, f"{checked} versions checked",
               f"{unknown} outcomes unknown")
    finally:
        cluster.close()
