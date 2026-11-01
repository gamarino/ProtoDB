"""Deterministic clusters for schedule tests: every message is held until the test delivers or
drops it, and all nodes share one fake clock advanced by hand (Task 8.3d, D46).

``Harness(names)`` starts one real :class:`~proto_db.cluster.coordinator.ClusterCoordinator`
per name, each with a :class:`~tests.support.cluster.MemoryTier`: an acceptor log kept per
node in a ``MemoryStorage`` "disk", so ``restart(name)`` keeps it (a crash with the disk
intact) and ``restart(name, wipe=True)`` loses it.  Roots are fake records (no records ride
with them); commits go through the real replicate path.

* ``attempt(name, granters, informed=())``: ``name``'s ``acquire``; its requests reach
  ``granters`` (whose replies come back) and ``informed`` (whose replies are lost);
* ``replicate(name, lease, root, to, expire=False)``: the commit point, replicated to ``to``
  only; ``expire`` advances the clock past the lease when no majority can answer;
* ``release(name, lease, to)``; ``recover(name, responders)``: a recovery round answered
  by ``responders``; ``base(name, lease, responders)``: the base a commit under ``lease``
  would build on (the lease root when certain, else the recovered root).
"""
import itertools
import threading
import time
import uuid

from proto_db.cluster.coordinator import ClusterCoordinator
from proto_db.errors import ProtoDBError
from proto_db.storage.format import FORMAT_VERSION, RootRecord
from proto_db.storage.memory import MemoryStorage
from tests.support.cluster import MemoryTier

D = 10.0
_ids = itertools.count(1)


def rec(epoch: int) -> RootRecord:
    """A fake root record at ``epoch`` with a pointer of its own."""
    return RootRecord(FORMAT_VERSION, uuid.UUID(int=next(_ids)), 64, epoch)


class FakeClock:
    """A clock advanced by hand."""

    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


class ManualNet:
    """Holds every message until the test delivers (synchronously) or drops it."""

    def __init__(self):
        self.lock = threading.Lock()
        self.pending = []
        self.handlers = {}

    def transport(self):
        net = self

        class Manual:
            def start(self, node_id, peers, handler):
                net.handlers[node_id] = handler
                self.node = node_id

            def send(self, node_id, message):
                with net.lock:
                    net.pending.append((self.node, node_id, message))

            def stop(self):
                pass

        return Manual()

    def take(self, src, dst, kinds):
        with self.lock:
            hit = [m for m in self.pending if (src is None or m[0] == src) and (dst is None or m[1] in dst)
                   and m[2]["type"] in kinds]
            self.pending = [m for m in self.pending if m not in hit]
        return hit

    def deliver(self, src=None, dst=None, kinds=("request", "grant", "reject", "release")):
        for _, to, msg in self.take(src, dst, kinds):
            self.handlers[to](msg)
            self.handlers[to].__self__.wait_idle(5.0)    # queued messages are handled by a worker

    def drop(self, src=None, dst=None, kinds=("request", "grant", "reject", "release")):
        self.take(src, dst, kinds)


class Harness:
    """Real coordinators over a :class:`ManualNet` and a :class:`FakeClock` (module docstring)."""

    def __init__(self, names="abc"):
        self.names = list(names)
        self.net = ManualNet()
        self.clock = FakeClock()
        self.peers = [(n, "h", 0) for n in self.names]
        self.disks = {n: MemoryStorage() for n in self.names}
        self.nodes = {n: self._node(n, 0.0) for n in self.names}
        self.retired = []
        for n in self.names:                # empty acceptor logs: every node rejoins first
            self.rejoin(n)

    def _node(self, name, grace):
        node = ClusterCoordinator(name, self.peers, transport=self.net.transport(), lease_duration=D,
                                  drift_margin=1.0, request_timeout=0.5, startup_grace=grace,
                                  clock=self.clock, seed=1, persist_interval=60.0, persist_lag=60.0)
        node.start()
        node.attach(MemoryTier(self.disks[name], name))
        return node

    def restart(self, name, *, wipe=False, responders=None):
        """Restart ``name`` with a startup grace of one lease; ``wipe`` loses its disk, and the
        node then rejoins with the answers of ``responders`` (default: every other node; an
        empty set leaves it rejoining)."""
        old = self.nodes[name]
        old.stop()
        self.retired.append(old)
        if wipe:
            self.disks[name] = MemoryStorage()
        self.nodes[name] = self._node(name, D)
        if wipe and (responders is None or responders):
            self.rejoin(name, responders)
        return self.nodes[name]

    def rejoin(self, name, responders=None, timeout=5.0):
        """Deliver ``name``'s rejoin queries to ``responders`` (default: every other node) and
        their answers until it has rejoined; return True, or False after ``timeout``."""
        responders = set(self.names) - {name} if responders is None else set(responders)
        node = self.nodes[name]
        deadline = time.monotonic() + timeout
        while node.stats()["rejoining"]:
            if time.monotonic() > deadline:
                return False
            self.net.deliver(src=name, dst=responders, kinds=("query",))
            self.net.drop(src=name, kinds=("query",))
            self.net.deliver(src=None, dst={name}, kinds=("answer",))
            time.sleep(0.001)
        return True

    def close(self):
        for node in list(self.nodes.values()) + self.retired:
            node.stop()

    def _run(self, fn, pump):
        """Run ``fn`` in a thread, calling ``pump()`` until it returns; return its result or
        the ProtoDB error it raised."""
        out = {}

        def run():
            try:
                out["value"] = fn()
            except ProtoDBError as e:
                out["error"] = e

        t = threading.Thread(target=run)
        t.start()
        while t.is_alive():
            pump()
            time.sleep(0.001)
        t.join()
        pump()
        return out.get("value", out.get("error"))

    def attempt(self, name, granters, *, informed=(), timeout=5.0, deliver_cancels=True):
        """Run ``name``'s acquire (see the module docstring); return the lease or the error."""
        granters, informed = set(granters), set(informed)
        others = set(self.names) - {name}
        kinds = ("request", "release") if deliver_cancels else ("request",)

        def pump():
            self.net.deliver(src=name, dst=granters | informed, kinds=kinds)
            self.net.drop(src=name, dst=others - granters - informed, kinds=kinds)
            for peer in informed:
                self.net.drop(src=peer, dst={name}, kinds=("grant", "reject"))
            self.net.deliver(src=None, dst={name}, kinds=("grant", "reject"))

        return self._run(lambda: self.nodes[name].acquire(timeout=timeout), pump)

    def replicate(self, name, lease, root, to, *, expire=False):
        """Replicate ``root`` under ``lease`` to ``to`` only; return the seconds or the error."""
        to = set(to)
        others = set(self.names) - {name}
        state = {"sent": False, "before": self.nodes[name].stats()["replications"]}

        def pump():
            self.net.deliver(src=name, dst=to, kinds=("records", "replicate"))
            self.net.drop(src=name, dst=others - to, kinds=("records", "replicate"))
            self.net.deliver(src=None, dst={name}, kinds=("ack",))
            # Expire only once this replication passed its validity check (counted after it).
            if expire and not state["sent"] and self.nodes[name].stats()["replications"] > state["before"]:
                state["sent"] = True
                self.clock.t = max(self.clock.t, lease.valid_until + 0.001)

        return self._run(lambda: self.nodes[name].replicate(lease, root), pump)

    def release(self, name, lease, to=()):
        self.nodes[name].release(lease)
        self.net.deliver(src=name, dst=set(to), kinds=("release",))
        self.net.drop(src=name, kinds=("release",))

    def recover(self, name, responders):
        """A recovery round of ``name`` answered by ``responders``; ``(known, root, label)``."""
        responders = set(responders)

        def pump():
            self.net.deliver(src=name, dst=responders, kinds=("query",))
            self.net.drop(src=name, kinds=("query",))
            self.net.deliver(src=None, dst={name}, kinds=("answer",))

        return self._run(lambda: self.nodes[name].recover_root(), pump)

    def base(self, name, lease, responders):
        """``(root, label)`` a commit under ``lease`` builds on: the lease's root when certain,
        else the root a recovery round answered by ``responders`` returns."""
        if lease.root_certain:
            return lease.root, lease.root_term
        known, root, label = self.recover(name, responders)
        return root, label

    def bootstrap(self, name="a"):
        """``name`` commits a first root with every node (each logs it); return it."""
        others = set(self.names) - {name}
        lease = self.attempt(name, others)
        root = rec(1)
        assert self.replicate(name, lease, root, others) is not None
        self.release(name, lease, to=others)
        return root
