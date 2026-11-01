"""Schedules where the propagated root is reported certain although it is stale (review R39 item 2).

Each test drives three real coordinators (``a``, ``b``, ``c``) through a manual transport:
every message is held until the test delivers or drops it, and all nodes share one fake
clock advanced by hand.  The invariant checked is the one Task 8.3d must establish with
majority-acknowledged root replication: a lease whose root is reported certain carries the
latest published root (``truth``).  With the 8.3c release-carried propagation each schedule
violates it, so they are strict xfails: Task 8.3d must make them pass (and drop the marker).
Safety of the data does not depend on this flag: a stale certain root only costs a fencing
event (a failed conditional root write), never a lost update.
"""
import threading
import time
import uuid

import pytest

from proto_db.cluster.coordinator import ClusterCoordinator
from proto_db.errors import ProtoTimeoutError
from proto_db.storage.format import FORMAT_VERSION, RootRecord

pytestmark = [pytest.mark.timeout(30),
              pytest.mark.xfail(strict=True, reason="Task 8.3d: majority-acknowledged root replication")]

D = 10.0


def _rec(epoch):
    return RootRecord(FORMAT_VERSION, uuid.UUID(int=epoch), 64, epoch)


class FakeClock:
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

    def _take(self, src, dst, kinds):
        with self.lock:
            hit = [m for m in self.pending if (src is None or m[0] == src) and (dst is None or m[1] in dst)
                   and m[2]["type"] in kinds]
            self.pending = [m for m in self.pending if m not in hit]
        return hit

    def deliver(self, src=None, dst=None, kinds=("request", "grant", "reject", "release")):
        for _, to, msg in self._take(src, dst, kinds):
            self.handlers[to](msg)

    def drop(self, src=None, dst=None, kinds=("request", "grant", "reject", "release")):
        self._take(src, dst, kinds)

    def wait_for(self, src, kind, timeout=5.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self.lock:
                if any(m[0] == src and m[2]["type"] == kind for m in self.pending):
                    return
            time.sleep(0.001)
        raise AssertionError(f"{src} sent no {kind}")


class Harness:
    def __init__(self):
        self.net = ManualNet()
        self.clock = FakeClock()
        self.peers = [(n, "h", 0) for n in "abc"]
        self.nodes = {n: self._node(n, 0.0) for n in "abc"}
        for node in self.nodes.values():
            node.observe_root(_rec(1), from_storage=True)

    def _node(self, name, grace):
        node = ClusterCoordinator(name, self.peers, transport=self.net.transport(), lease_duration=D,
                                  drift_margin=1.0, request_timeout=0.5, startup_grace=grace,
                                  clock=self.clock, seed=1)
        return node.start()

    def restart(self, name):
        self.nodes[name] = self._node(name, D)
        return self.nodes[name]

    def attempt(self, name, granters, *, timeout=5.0, deliver_replies=True, deliver_cancels=True):
        """Run ``name``'s acquire, delivering its requests (and cancels) to ``granters`` only,
        and the replies back unless ``deliver_replies`` is False, until it returns."""
        out = {}

        def run():
            try:
                out["lease"] = self.nodes[name].acquire(timeout=timeout)
            except ProtoTimeoutError as e:
                out["error"] = e

        t = threading.Thread(target=run)
        t.start()
        others = set("abc") - {name}
        kinds = ("request", "release") if deliver_cancels else ("request",)
        while t.is_alive():
            self.net.deliver(src=name, dst=set(granters), kinds=kinds)
            self.net.drop(src=name, dst=others - set(granters), kinds=kinds)
            if deliver_replies:
                self.net.deliver(dst={name}, kinds=("grant", "reject"))
            else:
                self.net.drop(dst={name}, kinds=("grant", "reject"))
            time.sleep(0.001)
        t.join()
        return out.get("lease")

    def release(self, name, lease, to=(), root=None):
        if root is not None:
            self.nodes[name].published(lease, root)
        self.nodes[name].release(lease)
        self.net.deliver(src=name, dst=set(to), kinds=("release",))
        self.net.drop(src=name, kinds=("release",))


def _check(lease, truth):
    assert lease is not None
    assert not lease.root_certain or lease.root == truth, \
        f"certain root epoch {lease.root.epoch if lease.root else 0}, latest published {truth.epoch}"


def test_a_stale_certain_release_clears_a_peers_suspicion():
    h = Harness()
    lease_c = h.attempt("c", {"b"})                 # c holds term 1 (b granted)
    h.clock.t += D                                  # c's lease and b's grant expire
    lease_b = h.attempt("b", {"a"})                 # b: uncertain (its grant to c expired) ...
    h.nodes["b"].observe_root(_rec(1), from_storage=True)   # ... so it reads the storage
    h.release("b", lease_b, to=(), root=_rec(2))    # publishes epoch 2; the release is lost
    h.nodes["c"].release(lease_c)                   # c, frozen until now, releases certain epoch 1
    h.clock.t += D                                  # a's grant to b expires: a may have missed a root
    h.net.deliver(src="c", dst={"a"}, kinds=("release",))   # ... but c's stale release clears it
    h.net.drop(src="c")
    _check(h.attempt("a", {"c"}), truth=_rec(2))


def test_a_same_holder_request_frees_a_grant_without_marking_a_missed_root():
    h = Harness()
    lease_b = h.attempt("b", {"a"})
    h.release("b", lease_b, to=(), root=_rec(2))    # epoch 2 published; release lost
    h.restart("b")                                  # b forgets everything (startup grace)
    # Same id, same term: a re-grants; b (starting) refuses itself, loses, and its cancel
    # frees a's grant without marking an expiry.
    assert h.attempt("b", {"a"}, timeout=0.01) is None
    h.net.drop(src="b")
    _check(h.attempt("c", {"a"}), truth=_rec(2))


def test_an_uncertain_release_does_not_taint_the_receiver():
    h = Harness()
    lease_b = h.attempt("b", {"a"})
    # Another writer published epoch 2; b's conditional write failed (fencing): b is suspect.
    h.nodes["b"].record_fencing_event(lease_b, RuntimeError("precondition failed"))
    h.release("b", lease_b, to=("a",))              # release certain=False with epoch 1
    _check(h.attempt("c", {"a"}), truth=_rec(2))


def test_the_holders_own_newer_root_is_ignored_when_its_own_vote_is_refused():
    h = Harness()
    h.nodes["a"].observe_root(_rec(2), from_storage=True)   # a read epoch 2 from the storage
    assert h.attempt("b", {"a"}, timeout=0.01, deliver_replies=False, deliver_cancels=False) is None
    # a's grant to b stays: b's cancel never reaches it.
    h.net.drop(src="b")
    _check(h.attempt("a", {"b", "c"}), truth=_rec(2))       # a's own vote: busy; b, c report epoch 1
