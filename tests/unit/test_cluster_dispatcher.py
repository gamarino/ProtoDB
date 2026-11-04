"""The dispatcher never waits on a disk (ruling R81, D50 "Message lanes").

The dispatcher is the transport's receiving thread.  Every step that writes the acceptor log (a
grant's promise, a ballot's promise, an acceptance, a committed root) runs on the acceptor lane
and replies when its write is durable; record and commit queries run on the read lane.  Here every
acceptor-log write takes ``SLOW`` seconds: the dispatcher's longest message must stay far below
it, while grants still go out only after their promise was written.
"""
import threading
import time

import pytest

from proto_db.cluster.persister import PlainTier
from tests.support.cluster import Cluster

pytestmark = [pytest.mark.timeout(60)]

SLOW = 0.2


@pytest.fixture
def slow_log(monkeypatch):
    """Every promise and root written to an acceptor log takes ``SLOW`` seconds; returns the
    (thread name, method) of each write."""
    writes = []
    lock = threading.Lock()
    for name in ("log_promise", "log_root"):
        original = getattr(PlainTier, name)

        def slow(self, *a, _original=original, _name=name, **kw):
            with lock:
                writes.append((threading.current_thread().name, _name))
            time.sleep(SLOW)
            return _original(self, *a, **kw)
        monkeypatch.setattr(PlainTier, name, slow)
    return writes


def test_the_dispatcher_never_waits_on_an_acceptor_log_write(slow_log):
    with Cluster(3, tiers=True, lease_duration=6.0, request_timeout=0.5, startup_grace=0.0) as cl:
        cl.wait_rejoined()
        for name in ("n0", "n1", "n2"):              # three terms: grants on every node
            node = cl[name]
            permission = node.permit(timeout=20)
            node.finish(permission)
            other = cl["n1" if name != "n1" else "n2"]
            permission = other.permit(timeout=20)    # a permission from the owner (dispatcher)
            other.finish(permission)
        for name in ("n0", "n1", "n2"):
            cl[name].wait_idle(10.0)
        promises = [t for t, what in slow_log if what == "log_promise"]
        assert promises, "no grant was made"
        dispatchers = [t for t in promises if t.startswith("proto-cluster-inproc-")]
        assert dispatchers == [], promises           # the in-process transport's receiving threads
        for name in ("n0", "n1", "n2"):
            stats = cl[name].stats()
            assert stats["dispatch_seconds_max"] < SLOW / 4, (name, stats["dispatch_seconds_max"])


def test_a_grant_is_sent_only_after_its_promise_is_durable_even_when_duplicated(monkeypatch):
    """Duplicated and competing requests while a promise is being written: every grant leaves
    its node only after a promise of at least its term was written there, and no term is
    granted to two requesters (consequences (a) and (b) of ruling R81)."""
    lock = threading.Lock()
    durable = {}                                     # tier -> highest promise written
    original = PlainTier.log_promise

    def slow_promise(self, term):
        time.sleep(SLOW / 4)
        original(self, term)
        with lock:
            durable[id(self)] = max(durable.get(id(self), 0), term)
    monkeypatch.setattr(PlainTier, "log_promise", slow_promise)
    with Cluster(3, tiers=True, lease_duration=6.0, request_timeout=0.5, startup_grace=0.0) as cl:
        cl.wait_rejoined()
        grants, bad = [], []
        for name in ("n0", "n1", "n2"):
            node = cl[name]
            send = node._transport.send

            def checked(dst, msg, _node=node, _send=send):
                if msg["type"] == "grant":
                    with lock:
                        written = durable.get(id(_node._tier), 0)
                        grants.append((_node.node_id, dst, msg["term"]))
                        if written < msg["term"]:
                            bad.append((_node.node_id, dst, msg["term"], written))
                _send(dst, msg)
            node._transport.send = checked
        cl.net.set_faults(duplicate=1.0)
        done = []

        def work(name):
            for _ in range(3):
                permission = cl[name].permit(timeout=30)
                cl[name].finish(permission)
            done.append(name)
        threads = [threading.Thread(target=work, args=(n,)) for n in ("n0", "n1", "n2")]
        for t in threads:
            t.start()
        for t in threads:
            t.join(40)
        assert sorted(done) == ["n0", "n1", "n2"]
        assert grants and bad == [], bad
        holders = {}
        for granter, requester, term in grants:
            holders.setdefault(term, set()).add(requester)
        assert all(len(r) == 1 for r in holders.values()), holders
        for name in ("n0", "n1", "n2"):
            assert cl[name].stats()["dispatch_seconds_max"] < SLOW / 4


def test_an_owner_serving_only_others_ends_its_term_after_owner_idle_release():
    """Ruling R80: n0 owns a term and then only n1 asks; after ``owner_idle_release`` seconds
    without a request of its own threads, n0 ends its term and n1 takes the ownership."""
    with Cluster(3, tiers=True, lease_duration=3.0, request_timeout=0.25, startup_grace=0.0,
                 owner_idle_release=0.5) as cl:
        cl.wait_rejoined()
        n0, n1 = cl["n0"], cl["n1"]
        n0.finish(n0.permit(timeout=5))
        owners = []
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            permission = n1.permit(timeout=5)       # n1 keeps asking, every 50 ms
            owners.append(permission.owner)
            n1.finish(permission)
            if owners[-1] == "n1":
                break
            time.sleep(0.05)
        assert owners[0] == "n0" and owners[-1] == "n1", owners
        assert n0.stats()["terms_released_idle_local"] == 1


def test_without_owner_idle_release_a_requested_term_is_kept():
    with Cluster(3, tiers=True, lease_duration=3.0, request_timeout=0.25, startup_grace=0.0) as cl:
        cl.wait_rejoined()
        n0, n1 = cl["n0"], cl["n1"]
        n0.finish(n0.permit(timeout=5))
        owners = []
        for _ in range(20):                          # 1 s of requests, all served by n0
            permission = n1.permit(timeout=5)
            owners.append(permission.owner)
            n1.finish(permission)
            time.sleep(0.05)
        assert set(owners) == {"n0"}, owners
        assert n0.stats()["terms_released_idle_local"] == 0
