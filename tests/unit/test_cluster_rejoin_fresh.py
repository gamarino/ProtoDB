"""A rejoin decides only on answers gathered after the leases of the node's previous life ended
(8.3d re-review 5, C1; ruling R53).

A node that lost its acceptor log may have accepted and acknowledged a root R just before the
loss, under a lease whose holder has not yet counted the acknowledgement.  Answers gathered while
that lease runs may all miss R: the other acceptors have not received it yet, and the holder
self-accepts after its broadcast (or its answer is lost).  A rejoin that completed on such a
snapshot adopted an older root, and once the holder reported R committed a majority without R
existed: the next holder built on the older root.  Every rejoin other than a cluster's first
start (``"new"``) therefore decides on a round of answers *sent* at or after
``start() + restart_grace``, whatever the node's startup grace; by then every lease granted
before the loss has ended on its holder's view, so each acceptor counted for a reported commit
had durably accepted the root before it answered.  The reviewer's probes, as tests:

* ``probe_stale_snapshot_net``: N=5, network faults only (a delayed copy, a lost answer);
* ``probe_stale_snapshot``: N=3, the holder descheduled between its broadcast and its own accept;
* ``probe_rootless_stale``: the ``"rootless"`` outcome of a first start on the same stale snapshot;
* plus freshness judged by when a round was sent, not when its answers arrived.
"""
import threading
import time

import pytest

from proto_db.errors import ProtoDBError, ProtoTimeoutError
from proto_db.storage.memory import MemoryStorage
from tests.support.manual_cluster import GRACE, Harness, rec

pytestmark = [pytest.mark.timeout(120)]


@pytest.fixture
def harness():
    made = []

    def make(names="abc", **kw):
        h = Harness(names, **kw)
        made.append(h)
        return h

    yield make
    for h in made:
        h.close()


def _replicate_in_background(node, lease, root):
    """Start ``node.replicate(lease, root)`` in a thread; return (thread, result dict)."""
    out = {}

    def run():
        try:
            out["r"] = node.replicate(lease, root)
        except ProtoDBError as e:   # the outcome under test
            out["r"] = e

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return t, out


def _finish(h, holder, t, out, acks, delayed=()):
    """Deliver the ``delayed`` copies and the held ``acks``, then pump acknowledgements to
    ``holder`` until its replication returns; return its outcome."""
    for _, to, msg in delayed:
        h.net.handlers[to](msg)
        h.net.handlers[to].__self__.wait_idle(5.0)
    for _, to, msg in acks:
        h.net.handlers[to](msg)
    deadline = time.monotonic() + 20.0
    while t.is_alive():
        assert time.monotonic() < deadline, "the replication never returned"
        h.net.deliver(src=None, dst={holder}, kinds=("ack",))
        time.sleep(0.001)
    return out["r"]


def _wipe(h, name):
    """``name`` loses its disk and restarts with the default (safe) grace; no rejoin yet."""
    old = h.nodes[name]
    old.stop()
    h.retired.append(old)
    h.disks[name] = MemoryStorage()
    h.nodes[name] = h._node(name, GRACE)
    return h.nodes[name]


def _assert_blocked_on_freshness(node):
    deadline = time.monotonic() + 10.0
    while not node.stats().get("rejoin_blocked") and time.monotonic() < deadline:
        time.sleep(0.01)
    stats = node.stats()
    assert stats["rejoining"], "the rejoin completed on answers gathered before the safe grace"
    assert "gathered" in (stats["rejoin_blocked"] or ""), stats["rejoin_blocked"]


def test_c1_a_rejoin_with_the_default_grace_ignores_answers_gathered_before_the_safe_grace_network_faults(
        harness):
    """``probe_stale_snapshot_net``.  N=5; a holds a lease granted by c and x and replicates R:
    old c accepts and acknowledges (the ack is held), the copy to x is delayed, y and z never get
    it.  c loses its disk and restarts with the default grace; a's answer is lost, so c hears
    from x, y and z (``N - majority + 1``), none of which has R yet.  Then x gets R and a counts
    old c's ack, x's and its own: R is committed.  Before the fix c completed at once on that
    snapshot, and y's lease from {y, z, c} built on R0 with ``root_certain=True``: R lost."""
    h = harness("acxyz")
    r0 = h.bootstrap("a")
    a = h.nodes["a"]
    lease_a = h.attempt("a", {"c", "x"})
    R = rec(2)
    t, out = _replicate_in_background(a, lease_a, R)
    deadline = time.monotonic() + 10.0
    while not [m for m in h.net.pending if m[2]["type"] == "replicate"]:
        assert time.monotonic() < deadline
        time.sleep(0.001)
    h.net.deliver(src="a", dst={"c"}, kinds=("records", "replicate"))
    acks = h.net.take("c", {"a"}, ("ack",))
    delayed_x = h.net.take("a", {"x"}, ("records", "replicate"))
    h.net.drop(src="a", dst={"y", "z"}, kinds=("records", "replicate"))
    assert h.nodes["c"].accepted_root()[1] == R
    c = _wipe(h, "c")
    assert not h.rejoin("c", responders={"x", "y", "z"}, timeout=1.5)     # a's answer is lost
    _assert_blocked_on_freshness(c)
    outcome = _finish(h, "a", t, out, acks, delayed_x)
    assert isinstance(outcome, float), outcome
    assert a.committed_root()[1] == R                                     # R was reported committed
    h.clock.t += GRACE + 0.5                                              # a's lease and c's wait end
    assert h.rejoin("c", responders={"x", "y", "z"})
    assert c.accepted_root()[1] == R
    lease_y = h.attempt("y", {"z", "c"})
    assert not isinstance(lease_y, Exception), lease_y
    base, _ = h.base("y", lease_y, responders={"z", "c"})
    assert base == R and base != r0


def test_c1_a_rejoin_with_the_default_grace_ignores_answers_gathered_before_the_safe_grace_paused_holder(
        harness):
    """``probe_stale_snapshot``.  N=3; a holds a lease granted by c and is descheduled between
    its broadcast of R and its own accept.  Old c accepts and acknowledges R (held), b never gets
    it.  c loses its disk and restarts with the default grace; a's and b's answers both lack R.
    a then resumes and counts old c's ack: R committed.  Before the fix c completed at once, and
    b's lease from {b, c} built on R0 with ``root_certain=True``."""
    h = harness()
    r0 = h.bootstrap("a")
    a = h.nodes["a"]
    lease_a = h.attempt("a", {"c"})
    gate, sent = threading.Event(), threading.Event()
    orig = a._broadcast

    def paused(msg):
        orig(msg)
        if msg["type"] == "replicate":
            sent.set()
            gate.wait(20)               # a's thread is descheduled between broadcast and self-accept

    a._broadcast = paused
    R = rec(2)
    t, out = _replicate_in_background(a, lease_a, R)
    assert sent.wait(10)
    h.net.deliver(src="a", dst={"c"}, kinds=("records", "replicate"))
    acks = h.net.take("c", {"a"}, ("ack",))
    h.net.drop(src="a", dst={"b"}, kinds=("records", "replicate"))
    assert h.nodes["c"].accepted_root()[1] == R
    c = _wipe(h, "c")
    try:
        assert not h.rejoin("c", timeout=1.5)
        _assert_blocked_on_freshness(c)
    finally:
        gate.set()
    outcome = _finish(h, "a", t, out, acks)
    assert isinstance(outcome, float), outcome
    assert a.committed_root()[1] == R
    h.clock.t += GRACE + 0.5
    assert h.rejoin("c")
    assert c.accepted_root()[1] == R
    lease_b = h.attempt("b", {"c"})
    assert not isinstance(lease_b, Exception), lease_b
    base, _ = h.base("b", lease_b, responders={"c"})
    assert base == R and base != r0


def test_c1_a_rootless_rejoin_does_not_complete_on_a_stale_snapshot(harness):
    """``probe_rootless_stale``.  N=5 first start with late members y and z (empty logs, never
    served).  a holds a lease granted by c and x and replicates the first root R; old c accepts
    and acknowledges (held), x's copy is delayed, a is descheduled before its own accept.  c loses
    its disk and restarts with the default grace.  c, y and z ask at once: every member answers,
    none has accepted a root, a reports its term -- ``"rootless"``.  Before the fix all three
    completed on that snapshot, a then counted old c's and x's acks (R committed), and y's lease
    from {y, z, c} had no root at all.

    With the fix they wait; once R is committed the fresh answers show it (a and x accepted it),
    so this is no longer a rootless cluster, and three empty logs of five are more than the
    one lost acceptor log the protocol tolerates without an operator (ruling R48): they stay
    rejoining and no lease is granted without R.  Safe, and unavailable by design."""
    h = harness("acxyz")
    for n in "yz":
        h.restart(n, wipe=True, responders=set())      # late first-start members: still rejoining
    a = h.nodes["a"]
    lease_a = h.attempt("a", {"c", "x"})
    assert not isinstance(lease_a, Exception), lease_a
    gate, sent = threading.Event(), threading.Event()
    orig = a._broadcast

    def paused(msg):
        orig(msg)
        if msg["type"] == "replicate":
            sent.set()
            gate.wait(20)

    a._broadcast = paused
    R = rec(1)
    t, out = _replicate_in_background(a, lease_a, R)
    assert sent.wait(10)
    h.net.deliver(src="a", dst={"c"}, kinds=("records", "replicate"))
    acks = h.net.take("c", {"a"}, ("ack",))
    delayed_x = h.net.take("a", {"x"}, ("records", "replicate"))
    h.net.drop(src="a", dst={"y", "z"}, kinds=("records", "replicate"))
    assert h.nodes["c"].accepted_root()[1] == R
    _wipe(h, "c")
    try:
        for n in "cyz":
            assert not h.rejoin(n, timeout=1.5), f"{n} completed on a stale snapshot"
            _assert_blocked_on_freshness(h.nodes[n])
    finally:
        gate.set()
    outcome = _finish(h, "a", t, out, acks, delayed_x)
    assert isinstance(outcome, float), outcome
    assert a.committed_root()[1] == R
    h.clock.t += GRACE + 0.5
    for n in "cyz":
        if h.rejoin(n, timeout=1.5):
            assert h.nodes[n].accepted_root()[1] == R, f"{n} rejoined without the committed root"
    lease_y = h.attempt("y", {"z", "c"}, timeout=1.0)
    if not isinstance(lease_y, Exception):
        base, _ = h.base("y", lease_y, responders={"z", "c"})
        assert base == R
    else:
        assert isinstance(lease_y, ProtoTimeoutError), lease_y


def test_c1_a_round_sent_before_the_safe_grace_does_not_count_even_if_its_answers_arrive_after(harness):
    """Freshness is judged by when the round was *sent*, not when its answers arrived: answers
    produced before ``start() + restart_grace`` and delivered after it are as stale as any.
    The paused-holder schedule again, with c's first query answered by a and b before R is
    committed, and those answers held in the network until the safe grace has passed."""
    from proto_db.cluster.coordinator import ClusterCoordinator
    from tests.support.cluster import MemoryTier
    from tests.support.manual_cluster import D, MARGIN
    h = harness()
    r0 = h.bootstrap("a")
    a = h.nodes["a"]
    lease_a = h.attempt("a", {"c"})
    gate, sent = threading.Event(), threading.Event()
    orig = a._broadcast

    def paused(msg):
        orig(msg)
        if msg["type"] == "replicate":
            sent.set()
            gate.wait(20)

    a._broadcast = paused
    R = rec(2)
    t, out = _replicate_in_background(a, lease_a, R)
    assert sent.wait(10)
    h.net.deliver(src="a", dst={"c"}, kinds=("records", "replicate"))
    acks = h.net.take("c", {"a"}, ("ack",))
    h.net.drop(src="a", dst={"b"}, kinds=("records", "replicate"))
    old = h.nodes["c"]
    old.stop()
    h.retired.append(old)
    h.disks["c"] = MemoryStorage()
    # A long request timeout keeps the first round open while its answers are held.
    c = ClusterCoordinator("c", h.peers, transport=h.net.transport(), lease_duration=D, drift_margin=MARGIN,
                           request_timeout=5.0, startup_grace=GRACE, clock=h.clock, seed=1,
                           persist_interval=60.0, persist_lag=60.0)
    c.start()
    c.attach(MemoryTier(h.disks["c"], "c"))
    h.nodes["c"] = c
    deadline = time.monotonic() + 10.0
    while not [m for m in h.net.pending if m[0] == "c" and m[2]["type"] == "query"]:
        assert time.monotonic() < deadline
        time.sleep(0.001)
    try:
        h.net.deliver(src="c", dst={"a", "b"}, kinds=("query",))     # answered now: neither has R
        held = h.net.take(None, {"c"}, ("answer",))
        assert len(held) == 2
    finally:
        gate.set()
    outcome = _finish(h, "a", t, out, acks)
    assert isinstance(outcome, float), outcome
    assert a.committed_root()[1] == R
    h.clock.t += GRACE + 0.5
    for _, to, msg in held:                                           # the stale answers arrive late
        h.net.handlers[to](msg)
    assert h.rejoin("c")
    assert c.accepted_root()[1] == R, "c completed on answers produced before the safe grace"
    lease_b = h.attempt("b", {"c"})
    assert not isinstance(lease_b, Exception), lease_b
    base, _ = h.base("b", lease_b, responders={"c"})
    assert base == R and base != r0


def test_a_recovery_round_does_not_clear_a_suspicion_that_arose_after_it_was_sent(harness):
    """The same stale-answer pattern in the recovery round (re-review 5, C1, reviewed beyond
    the rejoin).  A recovery clears this node's suspicion -- a granted term whose outcome it
    could not know -- because a majority's answers show that term's outcome.  Answers produced
    before the suspicion arose cannot: b's round is answered by c while a's lease (granted by b)
    still runs; a's lease then expires without a release and c's request makes b suspect a's
    term; the held answer then completes the round.  b must keep the suspicion (its grants stay
    uncertain); before the fix the round cleared it."""
    from proto_db.cluster.coordinator import ClusterCoordinator
    from tests.support.cluster import MemoryTier
    from tests.support.manual_cluster import D, MARGIN
    h = harness()
    h.bootstrap("a")
    old = h.nodes["b"]
    old.stop()
    h.retired.append(old)
    # b restarts on its intact disk with a long request timeout, so the round stays open.
    b = ClusterCoordinator("b", h.peers, transport=h.net.transport(), lease_duration=D, drift_margin=MARGIN,
                           request_timeout=5.0, startup_grace=GRACE, clock=h.clock, seed=1,
                           persist_interval=60.0, persist_lag=60.0)
    b.start()
    b.attach(MemoryTier(h.disks["b"], "b"))
    h.nodes["b"] = b
    h.clock.t += GRACE
    lease_a = h.attempt("a", {"b"})
    assert not isinstance(lease_a, Exception), lease_a
    out = {}

    def run():
        try:
            out["r"] = b.recover_root()
        except ProtoDBError as e:   # the outcome under test
            out["r"] = e

    t = threading.Thread(target=run, daemon=True)
    t.start()
    deadline = time.monotonic() + 10.0
    while not [m for m in h.net.pending if m[0] == "b" and m[2]["type"] == "query"]:
        assert time.monotonic() < deadline
        time.sleep(0.001)
    h.net.deliver(src="b", dst={"c"}, kinds=("query",))
    held = h.net.take("c", {"b"}, ("answer",))
    assert len(held) == 1
    h.net.drop(src="b", kinds=("query",))
    h.clock.t += GRACE                          # a's lease ends, never released
    lease_c = h.attempt("c", {"b"})
    assert not isinstance(lease_c, Exception), lease_c
    assert not lease_c.root_certain             # b reported a's term as unknown
    with b._lock:
        assert b._suspect_term == lease_a.term
    for _, to, msg in held:
        h.net.handlers[to](msg)
    t.join(10.0)
    assert not t.is_alive() and not isinstance(out["r"], Exception), out
    with b._lock:
        assert b._suspect_term == lease_a.term, "a round sent before the suspicion cleared it"
