"""The holder's rules for pipelined permissions (Task 8.3k, D55 rules H1, H2, H5, H7, V1).

Driven on a coordinator that is never started (no threads, an in-process transport whose
sends are recorded) or, where a real tier is needed, on a running cluster:

* H2: a permit issued on a proposal (``after``) is never certain, and its base is not
  learnt committed on arrival, whatever the message says;
* H5: the gate opens on committed knowledge of the base (from any source) or on the fallback
  query's answer; it closes on a lease expiry, a stop, a newer term or a committed root above
  the base, and returns at once once closed;
* V1: a ``store`` is never an acceptance and never moves the permission sequence.

Kind-6 store-only entries are never replayed as accepted after a restart:
``tests/unit/test_tier_replica_entries.py`` (8.3g) proves it on the tier.
"""
import threading
import time

import pytest

from proto_db.cluster.coordinator import ClusterCoordinator
from proto_db.cluster.lease import Permission
from proto_db.cluster.protocol import encode_root, make_message
from proto_db.cluster.transport import InProcessNetwork
from tests.support.manual_cluster import rec

T = 7 << 12
K = rec(20)          # the proposal: root k, the base of permission k+1
PEERS = [(n, "127.0.0.1", 0) for n in ("a", "b", "c")]


@pytest.fixture
def node():
    net = InProcessNetwork()
    c = ClusterCoordinator("b", PEERS, transport=net.endpoint(), lease_duration=10.0, request_timeout=0.05)
    c._state = "running"
    sent = []
    c._transport.send = lambda to, msg: sent.append((to, msg))
    c.sent = sent
    yield c
    net.close()


def _permit(c, *, after=None, valid=5.0, root=None):
    return Permission(term=T, seq=2, owner="a", acquired_at=c._clock(), valid_until=c._clock() + valid,
                      root=root or K, root_certain=after is None, clock=c._clock, root_term=T,
                      after=after)


# -- H2 --------------------------------------------------------------------------------------

def test_h2_a_permit_on_a_proposal_is_never_certain_and_teaches_nothing_committed(node):
    from proto_db.cluster.coordinator import _PermitWait
    rid = 41
    node._permit_waits[rid] = wait = _PermitWait(rid, node._clock())
    before = node._committed
    msg = make_message("permit", "a", term=T, seq=2, rid=rid, inc=node._inc, valid=5.0,
                       root=encode_root(K, known=True, term=T), certain=True, after=[1, "a"])
    node._on_permit_reply("a", msg)
    p = wait.reply
    assert isinstance(p, Permission)
    assert p.after == (1, "a") and not p.root_certain and p.issued_base
    assert node._committed == before        # the proposal is not learnt committed on arrival


def test_h2_a_proposal_base_is_never_re_proposed():
    from proto_db import transactions

    class _C:
        def known_root(self):
            return (False, None, 0)
    assert not transactions._base_needs_commit(_C(), _permit_like(after=(1, "a")), rec(20))
    assert transactions._base_needs_commit(_C(), _permit_like(after=None), rec(20))


def _permit_like(after):
    class _P:
        pass
    p = _P()
    p.after = after
    return p


# -- H5: the gate ------------------------------------------------------------------------------

def _gate(c, lease, base):
    out = {}

    def run():
        out["open"] = c._await_gate(lease, base, None)
    t = threading.Thread(target=run)
    t.start()
    return t, out


def test_h5_the_gate_opens_on_committed_knowledge_of_its_base(node):
    base = (True, K, T)
    lease = _permit(node, after=(1, "a"))
    t, out = _gate(node, lease, base)
    time.sleep(0.01)
    assert t.is_alive()                     # closed: nothing committed known yet
    with node._cond:
        # A done, a permit, a release, a recovery answer: every source learns through this.
        node._observe_committed(True, K, T, "a")
        node._cond.notify_all()
    t.join(2)
    assert out == {"open": True}
    assert node.stats()["gate_waits"] == 1 and node.stats()["gate_closures"] == 0


def test_h5_a_committed_root_read_from_storage_does_not_open_the_gate(node):
    """Knowledge from the persisted root (``accept=False``) is not accepted knowledge: the
    holder's own acceptance would be refused under V2', so the gate keeps waiting (ruling)."""
    base = (True, K, T)
    lease = _permit(node, after=(1, "a"), valid=0.3)
    with node._cond:
        node._observe_committed(True, K, T, None, accept=False)
    t, out = _gate(node, lease, base)
    time.sleep(0.02)
    assert t.is_alive()
    with node._cond:
        node._observe_committed(True, K, T, "a")      # a done: accepted knowledge
        node._cond.notify_all()
    t.join(2)
    assert out == {"open": True}


def test_h5_the_fallback_query_goes_to_the_proposals_holder_and_the_voters(node):
    base = (True, K, T)
    lease = _permit(node, after=(1, "a"))
    t, out = _gate(node, lease, base)
    deadline = time.monotonic() + 2
    while not node.sent and time.monotonic() < deadline:
        time.sleep(0.005)
    queries = [(to, m) for to, m in node.sent if m["type"] == "query"]
    assert {to for to, _ in queries} == {"a", "c"}
    assert all(m["ballot"] == 0 for _, m in queries)
    rid = queries[0][1]["rid"]
    with node._cond:
        waiter = node._waiters[rid]
        waiter.answers["c"] = ((True, K, T), (True, K, T), True)
        node._cond.notify_all()
    t.join(2)
    assert out == {"open": True}
    assert node.stats()["gate_fallbacks"] == 1
    assert node._committed == base          # learnt (and logged when a tier is attached)
    assert rid not in node._waiters


def test_h5_an_answer_with_another_committed_root_does_not_open_the_gate(node):
    base = (True, K, T)
    lease = _permit(node, after=(1, "a"), valid=0.3)
    t, out = _gate(node, lease, base)
    deadline = time.monotonic() + 2
    while not node.sent and time.monotonic() < deadline:
        time.sleep(0.005)
    rid = node.sent[0][1]["rid"]
    with node._cond:
        node._waiters[rid].answers["c"] = ((True, rec(19), T), (True, rec(19), T), True)
        node._cond.notify_all()
    t.join(2)
    assert out == {"open": False}           # closed at the lease's expiry
    assert node.stats()["gate_closures"] == 1


@pytest.mark.parametrize("cause", ["expiry", "stop", "newer_term", "higher_committed"])
def test_h5_the_gate_closes_and_stays_closed(node, cause):
    base = (True, K, T)
    lease = _permit(node, after=(1, "a"), valid=0.2 if cause == "expiry" else 5.0)
    t, out = _gate(node, lease, base)
    time.sleep(0.01)
    with node._cond:
        if cause == "stop":
            node._state = "stopping"
        elif cause == "newer_term":
            node._owner_hint = (T + (1 << 12), "c")
        elif cause == "higher_committed":
            node._committed = (True, rec(21), T)
        node._cond.notify_all()
    t.join(2)
    assert out == {"open": False}
    # Closed means closed: committed knowledge of the base arriving later changes nothing,
    # the gate has returned and the round has ended with it (H7 decides its outcome).
    with node._cond:
        node._state = "running"
        node._committed = base
    assert node.stats()["gate_closures"] == 1


# -- H7 and H1 need a tier and a running cluster: tests/concurrency/test_cluster_pipelined_permits.py
#    (P2: a closed gate after a store is a conflict that never takes effect; P1 under V2': a
#    root stage that left is unknown; P4: an own acceptance logged is unknown; a failing
#    predecessor: the successor's conflict is retried; P5's run: every proposing round ends its
#    permission, H1, and no term is abandoned).
