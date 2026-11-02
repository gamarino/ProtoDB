"""In-process clusters over tiered storages and one shared object store (Task 8.3d, D46).

``TieredCluster(tmp_path, n, store=...)`` starts a :class:`~tests.support.cluster.Cluster`
of ``n`` coordinators and, per node, a :class:`~proto_db.storage.tiered.TieredStorage` in
``tmp_path/<node>`` over ``store`` (a ``FakeObjectStore`` by default) with an
:class:`~proto_db.transactions.ObjectSpace` attached to the node's coordinator.  The
constructor returns once every member has finished its first rejoin.

* ``kill(node)``: the process dies: its transport and persister stop, its space and
  storage are closed (the local directory survives, as a disk does);
* ``wipe(node)``: also deletes the local directory (the disk is lost);
* ``revive(node, force_rejoin=False)``: a fresh coordinator with the same id, a storage
  reopened on the local directory (empty after a wipe) and a new space; after a wipe the
  node rejoins before voting, and ``force_rejoin`` is the operator's disaster-recovery
  decision when a majority of disks is lost
  (:meth:`~proto_db.cluster.coordinator.ClusterCoordinator.force_rejoin`).  A revived node
  starts with the coordinator's default startup grace, ``lease_duration + drift_margin``
  (unless overridden): a shorter grace is accepted only on a cluster's first start (D46,
  rulings R50 and R51), because a restarted node does not remember when its last grant ends;
* ``value(node)``: the counter ``n`` of database ``db`` read by a new transaction;
* ``eventually(node, expected)``: True once ``value(node) == expected`` (within 5 s): a
  node learns another node's commit when its release arrives.
"""
import os
import shutil
import time

from proto_db.storage.tiered import TieredStorage
from proto_db.transactions import ObjectSpace
from tests.support.cluster import Cluster
from tests.support.fake_object_store import FakeObjectStore

FAST = dict(lease_duration=0.4, drift_margin=0.04, request_timeout=0.1, startup_grace=0.0,
            backoff=(0.001, 0.02), persist_interval=0.02, persist_lag=0.3)
RUN_KW = dict(retries=200, backoff=(0.001, 0.01))


def incr(tr):
    """Increment the root counter ``n``."""
    tr.set_root_object("n", (tr.get_root_object("n") or 0) + 1)


class TieredCluster:
    """``n`` nodes, each with a tiered storage and a space (see the module docstring)."""

    def __init__(self, tmp_path, n: int = 3, *, store=None, storage_kw=None, **coordinator_kw):
        kw = dict(FAST)
        kw.update(coordinator_kw)
        self.path = str(tmp_path)
        self.store = store if store is not None else FakeObjectStore()
        self.storage_kw = dict(durability="none", ship_interval=0.02, backoff=(0.001, 0.01), retries=2)
        self.storage_kw.update(storage_kw or {})
        self.cluster = Cluster(n, **kw)
        self.ids = self.cluster.ids
        self.storages = {}
        self.spaces = {}
        try:
            for node in self.ids:
                self._open(node)
            self._await_first_rejoins()
        except BaseException:
            self.close()
            raise

    def _await_first_rejoins(self, timeout: float = 10.0) -> None:
        """Return once no member is rejoining: a test starts on a formed cluster.  A member
        that finished a first start after another committed would otherwise wait out a lease
        (R50: an empty log cannot tell it from a wiped node), racing the test's faults."""
        deadline = time.monotonic() + timeout
        while any(self.cluster[n].stats()["rejoining"] for n in self.ids):
            if time.monotonic() > deadline:
                raise TimeoutError(f"members still rejoining after {timeout} s: "
                                   f"{[n for n in self.ids if self.cluster[n].stats()['rejoining']]}")
            time.sleep(0.005)

    def _open(self, node: str) -> None:
        storage = TieredStorage(os.path.join(self.path, node), self.store, **self.storage_kw)
        self.storages[node] = storage
        self.spaces[node] = ObjectSpace(storage, coordinator=self.cluster[node], lock_timeout=5.0)

    def __getitem__(self, node: str):
        return self.cluster[node]

    def db(self, node: str):
        """Database ``db`` opened on ``node``'s space."""
        return self.spaces[node].open_database("db")

    def value(self, node: str):
        """The counter read by a new transaction on ``node``."""
        tr = self.db(node).new_transaction()
        try:
            return tr.get_root_object("n")
        finally:
            tr.abort()

    def eventually(self, node: str, expected, timeout: float = 5.0) -> bool:
        """Poll :meth:`value` until it equals ``expected``; False after ``timeout`` seconds."""
        deadline = time.monotonic() + timeout
        while self.value(node) != expected:
            if time.monotonic() > deadline:
                return False
            time.sleep(0.01)
        return True

    def kill(self, node: str) -> None:
        """The node's process dies; its local directory survives."""
        coordinator = self.cluster[node]
        self.cluster.crash(node)
        if coordinator.persister is not None:
            coordinator.persister.stop()
        space = self.spaces.pop(node, None)
        if space is not None:
            space.close()
        self.storages.pop(node, None)

    def wipe(self, node: str) -> None:
        """The node dies and its disk is lost."""
        self.kill(node)
        shutil.rmtree(os.path.join(self.path, node), ignore_errors=True)

    def revive(self, node: str, *, force_rejoin: bool = False, **overrides) -> None:
        """Start the node again (a fresh coordinator and a reopened storage), with the default
        (safe) startup grace unless ``overrides`` sets one."""
        overrides.setdefault("startup_grace", None)
        coordinator = self.cluster.restart(node, **overrides)
        if force_rejoin:
            coordinator.force_rejoin()
        self._open(node)

    def close(self) -> None:
        """Close every space, then stop the cluster."""
        for space in list(self.spaces.values()):
            space.close()
        self.spaces.clear()
        self.cluster.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False
