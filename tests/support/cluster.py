"""In-process clusters of ClusterCoordinator nodes for tests (Task 8.3c).

``Cluster(n, ...)`` starts ``n`` coordinators named ``n0`` .. ``n{n-1}`` on one
:class:`~proto_db.cluster.transport.InProcessNetwork`; extra keyword arguments go to
every coordinator; each coordinator's jitter generator is seeded from ``seed`` and its
index.  ``crash(node)`` stops the node's transport only (a crashed or frozen
process: it keeps running its code but neither sends nor receives).  ``restart(node)``
starts a fresh coordinator with the same id on a new endpoint.  ``close()`` stops every
node and the network, joining all their threads.

A node grants and acknowledges only with a cluster tier attached (D46: its promises must
be recorded somewhere).  Tests that open spaces attach their storage's tier; lease-only
tests pass ``tiers=True``: every node then gets a :class:`MemoryTier` (an in-memory
acceptor log that survives ``restart``, like a disk, and a persister that never writes),
and the cluster waits until every node has rejoined (an empty acceptor log learns the
promised terms from its peers before it votes, D46); ``wipe(node)`` waits for that node.

``CasCountingStorage(storage)`` delegates to a storage and counts failed conditional root
writes (``ProtoConflictError`` from ``publish_root``), publishes, and publishes sent
without an expected epoch; ``publishers`` is the set of thread names that published.

``DriftClock(rate, offset)`` is a node clock running at ``rate`` times real time;
``to_real(t)`` maps one of its readings back to ``time.monotonic()``, so a harness can
compare intervals measured on different nodes' clocks in global real time.
"""
import threading
import time

from proto_db.cluster.coordinator import ClusterCoordinator
from proto_db.cluster.persister import PlainTier
from proto_db.errors import ProtoConflictError
from proto_db.cluster.transport import InProcessNetwork
from proto_db.storage.memory import MemoryStorage
from tests.support import stress


class MemoryTier(PlainTier):
    """A cluster tier for lease-level tests: an in-memory acceptor log per (storage, node), no
    persisted root, nothing persisted."""

    def read_persisted(self):
        return None

    def persist(self, label, record):
        return False


class DriftClock:
    """A monotonic clock drifting from real time by a constant rate."""

    def __init__(self, rate: float = 1.0, offset: float = 0.0):
        self.rate = rate
        self.offset = offset
        self._origin = time.monotonic()

    def __call__(self) -> float:
        return self.offset + (time.monotonic() - self._origin) * self.rate

    def to_real(self, t: float) -> float:
        """The ``time.monotonic()`` instant at which this clock reads ``t``."""
        return self._origin + (t - self.offset) / self.rate


class Cluster:
    """``n`` in-process coordinators on one network (see the module docstring)."""

    def __init__(self, n: int, *, seed: int = 0, clocks=None, tiers: bool = False, **coordinator_kwargs):
        self.seed = seed
        self.tiers = tiers
        self.disks = {}               # node -> MemoryStorage holding its in-memory acceptor log
        self.net = InProcessNetwork(seed=seed)
        self.ids = [f"n{i}" for i in range(n)]
        self.peers = [(node_id, "127.0.0.1", 0) for node_id in self.ids]
        self.kwargs = dict(coordinator_kwargs)
        self.clocks = dict(clocks or {})
        self.nodes = {}
        self.retired = []
        try:
            for node_id in self.ids:
                self.start(node_id)
            if tiers:
                self.wait_rejoined()
        except BaseException:
            self.close()
            raise

    def start(self, node_id: str, **overrides) -> ClusterCoordinator:
        kwargs = dict(self.kwargs)
        kwargs.setdefault("seed", self.seed * 1000 + self.ids.index(node_id))   # deterministic jitter
        if node_id in self.clocks:
            kwargs["clock"] = self.clocks[node_id]
        kwargs.update(overrides)
        node = ClusterCoordinator(node_id, self.peers, transport=self.net.endpoint(), **kwargs)
        node.start()
        self.nodes[node_id] = node
        if self.tiers:
            node.attach(MemoryTier(self.disks.setdefault(node_id, MemoryStorage()), node_id))
        return node

    def wipe(self, node_id: str, *, wait: bool = True, **overrides) -> ClusterCoordinator:
        """Restart ``node_id`` with an empty acceptor log (its disk is lost); with ``wait``,
        return once it has rejoined."""
        self.disks.pop(node_id, None)
        node = self.restart(node_id, **overrides)
        if wait and self.tiers:
            self.wait_rejoined([node_id])
        return node

    def wait_rejoined(self, nodes=None, timeout: float = 10.0) -> None:
        """Wait until ``nodes`` (default all) have rejoined (no empty acceptor log learning)."""
        start = time.monotonic()
        late = False                  # under stress (R142): the quiet bound was missed and recorded
        for node_id in nodes or self.ids:
            while self.nodes[node_id].stats()["rejoining"]:
                elapsed = time.monotonic() - start
                if elapsed > timeout and not late:
                    late = not stress.floor(False, f"{node_id} did not rejoin within {timeout} s")
                if elapsed > stress.budget(timeout):
                    return            # stress only: the test goes on to its safety checks
                time.sleep(0.002)

    def __getitem__(self, node_id: str) -> ClusterCoordinator:
        return self.nodes[node_id]

    def crash(self, node_id: str) -> None:
        self.nodes[node_id].transport.stop()

    def restart(self, node_id: str, **overrides) -> ClusterCoordinator:
        old = self.nodes[node_id]        # stays visible until the new node replaces it
        old.stop()
        self.retired.append(old)
        return self.start(node_id, **overrides)

    def close(self) -> None:
        for node in list(self.nodes.values()) + self.retired:
            node.stop()
        self.net.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False


class CasCountingStorage:
    """Delegating storage proxy counting conditional root writes and their failures."""

    def __init__(self, inner):
        self._inner = inner
        self._count_lock = threading.Lock()
        self.publishes = 0
        self.failed_cas = 0
        self.unconditional = 0
        self.publishers = set()

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def publish_root(self, ptr, expected_epoch):
        with self._count_lock:
            self.publishes += 1
            self.publishers.add(threading.current_thread().name)
            if expected_epoch is None:
                self.unconditional += 1
        try:
            return self._inner.publish_root(ptr, expected_epoch)
        except ProtoConflictError:
            with self._count_lock:
                self.failed_cas += 1
            raise
