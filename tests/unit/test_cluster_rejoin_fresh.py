"""A rejoin of an empty acceptor log never completes on a snapshot that misses a root it may
have acknowledged (8.3d re-review 5, C1; rulings R53, R78a).

A node that lost its acceptor log may have accepted and acknowledged a root R just before the
loss, under a lease whose holder has not yet counted the acknowledgement.  Answers gathered while
that lease runs may all miss R: the other acceptors have not received it yet, and the holder
self-accepts after its broadcast (or its answer is lost).  A rejoin that completed on such a
snapshot adopted an older root, and once the holder reported R committed a majority without R
existed: the next holder built on the older root.  Rulings R53/R54 made such a rejoin wait
``restart_grace`` (a clock-rate assumption); since ruling R78a every rejoin other than a
cluster's first start (``"new"``) is a ballot instead: the witnesses durably promise a fresh
ballot above every term they report and answer after that promise, so either an answer shows R
or R is never accepted again by the counted witnesses (the ballot rule) and cannot be reported
committed.  No clock is advanced in these tests.  The reviewer's probes, as tests:

* ``probe_stale_snapshot_net``: N=5, network faults only (a delayed copy, a lost answer);
* ``probe_stale_snapshot``: N=3, the holder descheduled between its broadcast and its own accept;
* ``probe_rootless_stale``: the ``"rootless"`` outcome of a first start on the same stale snapshot;
* plus plain answers held in the network and delivered late: they never complete a rejoin.
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


def _finish(h, holder, t, out, acks, delayed=(), expire=None):
    """Deliver the ``delayed`` copies and the held ``acks``, then pump acknowledgements to
    ``holder`` until its replication returns; return its outcome.  With ``expire`` (the
    holder's lease), the clock passes the lease's end once the acknowledgements are in: a
    replication short of a majority then returns."""
    for _, to, msg in delayed:
        h.net.handlers[to](msg)
        h.net.handlers[to].__self__.wait_idle(5.0)
    for _, to, msg in acks:
        h.net.handlers[to](msg)
    start = time.monotonic()
    deadline = start + 20.0
    while t.is_alive():
        assert time.monotonic() < deadline, "the replication never returned"
        h.net.deliver(src=None, dst={holder}, kinds=("ack",))
        if expire is not None and time.monotonic() - start > 0.3:
            h.clock.t = max(h.clock.t, expire.valid_until + 0.001)
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


def _assert_rejoined_by_ballot(h, name, responders=None):
    """``name`` rejoins at once (no clock advance) through one ballot round."""
    node = h.nodes[name]
    assert h.rejoin(name, responders=responders), f"{name} did not rejoin"
    assert node.stats()["rejoin_ballots"] == 1


def _assert_not_committed(outcome, holder, R):
    """The holder's replication of ``R`` did not report a commit: the counted witnesses
    promised the rejoin's ballot first and refuse ``R``'s term."""
    assert isinstance(outcome, ProtoDBError), f"R was reported committed: {outcome}"
    assert holder.committed_root()[1] != R


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
    _assert_rejoined_by_ballot(h, "c", responders={"x", "y", "z"})        # a's answer is lost
    assert c.accepted_root()[1] == r0                                     # nobody it heard has R
    outcome = _finish(h, "a", t, out, acks, delayed_x, expire=lease_a)    # x refuses R now
    _assert_not_committed(outcome, a, R)
    h.clock.t += GRACE + 0.5          # c's startup grace for GRANTING (its rejoin did not wait)
    lease_y = h.attempt("y", {"z", "c"})
    assert not isinstance(lease_y, Exception), lease_y
    base, _ = h.base("y", lease_y, responders={"z", "c"})
    assert base == r0


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
        _assert_rejoined_by_ballot(h, "c")          # a promises the ballot while descheduled
        assert c.accepted_root()[1] == r0
    finally:
        gate.set()
    outcome = _finish(h, "a", t, out, acks, expire=lease_a)     # a's own acceptance: ballot rule
    _assert_not_committed(outcome, a, R)
    h.clock.t += GRACE + 0.5          # c's startup grace for GRANTING (its rejoin did not wait)
    lease_b = h.attempt("b", {"c"})
    assert not isinstance(lease_b, Exception), lease_b
    base, _ = h.base("b", lease_b, responders={"c"})
    assert base == r0


def test_c1_a_rootless_rejoin_does_not_complete_on_a_stale_snapshot(harness):
    """``probe_rootless_stale``.  N=5 first start with late members y and z (empty logs, never
    served).  a holds a lease granted by c and x and replicates the first root R; old c accepts
    and acknowledges (held), x's copy is delayed, a is descheduled before its own accept.  c loses
    its disk and restarts with the default grace.  c, y and z ask at once: every member answers,
    none has accepted a root, a reports its term -- ``"rootless"``.  Before the fix all three
    completed on that snapshot, a then counted old c's and x's acks (R committed), and y's lease
    from {y, z, c} had no root at all.

    With the ballot (ruling R78a) the rootless rejoins complete only after a and x durably
    promised a ballot above a's term: a's own acceptance and x's delayed copy are then
    refused, so R is never committed, and the cluster continues from no root.  Safe and
    available."""
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
            assert h.rejoin(n, timeout=5.0), f"{n} did not rejoin"
            assert h.nodes[n].stats()["rejoin_ballots"] == 1
            assert h.nodes[n].accepted_root() == (False, None, 0)
    finally:
        gate.set()
    outcome = _finish(h, "a", t, out, acks, delayed_x, expire=lease_a)
    _assert_not_committed(outcome, a, R)
    lease_y = h.attempt("y", {"z", "c"}, timeout=1.0)
    if not isinstance(lease_y, Exception):
        base, _ = h.base("y", lease_y, responders={"z", "c"})
        assert base != R
    else:
        assert isinstance(lease_y, ProtoTimeoutError), lease_y


def test_c1_plain_answers_delivered_late_never_complete_a_rejoin(harness):
    """Answers of a plain round never complete a rejoin outside a first start, however late
    they arrive: the paused-holder schedule, with c's first (plain) query answered by a and b
    before R is committed and those answers held until after the commit.  The ballot round
    that follows reads a's answer after a's promise: it reports R."""
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
                           request_timeout=3.0, startup_grace=GRACE, clock=h.clock, seed=1,
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
    for _, to, msg in held:                                           # the stale answers arrive late
        h.net.handlers[to](msg)
    assert h.rejoin("c")
    assert c.stats()["rejoin_ballots"] == 1
    assert c.accepted_root()[1] == R, "c completed on answers that miss the committed root"
    h.clock.t += GRACE + 0.5          # c's startup grace for GRANTING (its rejoin did not wait)
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
                           request_timeout=3.0, startup_grace=GRACE, clock=h.clock, seed=1,
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


def _reattach_empty(h, name):
    """``name``'s space is closed and reopened over a replaced, empty disk on the same running
    coordinator (``ObjectSpace.close()`` detaches without stopping it); no rejoin yet."""
    node = h.nodes[name]
    node.detach(node.tier)
    h.disks[name] = MemoryStorage()
    node.attach(h.tier_class(h.disks[name], name))
    return node


def test_c2_a_reattach_over_an_empty_disk_rejoins_by_ballot(harness):
    """``probe_reattach_stale`` (re-review 6, C2).  An empty log also arrives through
    ``detach()`` + ``attach()`` on a coordinator that keeps running, long after ``start()``;
    its rejoin is a ballot like any other (ruling R78a).
    N=3; the coordinators started long ago.  a holds a lease granted by c and is descheduled
    between its broadcast of R and its own accept; old-disk c accepts and acknowledges (held), b
    never gets R.  c's space closes, its disk is replaced, and the space reopens on the same
    coordinator: before the fix its rejoin completed at once on answers lacking R and logged R0;
    a then counted old c's ack (R committed); c restarted normally (disk intact, R0 loaded), and
    b's lease from {b, c} built on R0 with ``root_certain=True``: an acknowledged commit lost."""
    h = harness()
    r0 = h.bootstrap("a")
    h.clock.t += 3 * GRACE                       # the coordinators started long ago
    a = h.nodes["a"]
    lease_a = h.attempt("a", {"c"})
    assert not isinstance(lease_a, Exception), lease_a
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
    assert h.nodes["c"].accepted_root()[1] == R
    c = _reattach_empty(h, "c")
    try:
        _assert_rejoined_by_ballot(h, "c")
        assert c.accepted_root()[1] == r0
    finally:
        gate.set()
    outcome = _finish(h, "a", t, out, acks, expire=lease_a)
    _assert_not_committed(outcome, a, R)
    h.restart("c")                               # a clean restart: c reloads what its log holds
    h.clock.t += GRACE + 0.5
    assert h.nodes["c"].accepted_root()[1] == r0
    h.clock.t += GRACE + 0.5          # c's startup grace for GRANTING (its rejoin did not wait)
    lease_b = h.attempt("b", {"c"})
    assert not isinstance(lease_b, Exception), lease_b
    base, _ = h.base("b", lease_b, responders={"c"})
    assert base == r0


def test_c2_audit_a_reattached_log_replaces_the_acceptor_state_held_in_memory(harness):
    """Round 7 audit of per-attach state.  A node's acceptor state is what its attached log
    holds, as after a restart: an empty log attached to a coordinator that keeps running must
    not inherit the accepted root of the log it replaced.  N=3 with records: a (lease granted
    by c) replicates R; only c accepts it, and R's record reaches c's disk only; a crashes
    before its own accept and restarts on its intact disk (no R).  c's disk is replaced on the
    running coordinator and c rejoins on fresh answers (R0 from a and b).  Before the fix c
    still reported R as accepted (memory) while its log held R0, so b's lease from {b, c}
    adopted R as its base, whose record is nowhere: every lease c granted was stuck until c
    restarted (a root adopted without its records, ruling R50)."""
    h = harness(records=True)
    r0 = h.bootstrap("a")
    h.clock.t += 3 * GRACE
    a = h.nodes["a"]
    lease_a = h.attempt("a", {"c"})
    assert not isinstance(lease_a, Exception), lease_a
    gate, sent = threading.Event(), threading.Event()
    orig = a._broadcast

    def paused(msg):
        orig(msg)
        if msg["type"] == "replicate":
            sent.set()
            gate.wait(20)

    a._broadcast = paused
    R = rec(2)
    t, _ = _replicate_in_background(a, lease_a, R)
    try:
        assert sent.wait(10)
        h.net.deliver(src="a", dst={"c"}, kinds=("records", "replicate"))
        assert h.nodes["c"].wait_idle(5.0)
        h.nodes["c"].tier.held.add((R.segment, R.offset))      # R's record rode to c only
        h.net.drop(src="c", dst={"a"}, kinds=("ack",))
        h.net.drop(src="a", dst={"b"}, kinds=("records", "replicate"))
        assert h.nodes["c"].accepted_root()[1] == R
        a.stop()                                                # a crashes before its own accept
    finally:
        gate.set()
    t.join(10.0)
    h.retired.append(a)
    h.nodes["a"] = h._node("a", GRACE)
    assert h.nodes["a"].accepted_root()[1] == r0
    c = _reattach_empty(h, "c")
    assert c.accepted_root() == (False, None, 0), "the replaced log's accepted root survived in memory"
    h.clock.t += GRACE + 0.5
    assert h.rejoin("c")
    logged = c.tier.load_roots()[0]
    assert logged is not None and logged[1] == r0
    assert c.accepted_root()[1] == r0, "c's memory disagrees with its log"
    h.clock.t += GRACE + 0.5          # c's startup grace for GRANTING (its rejoin did not wait)
    lease_b = h.attempt("b", {"c"})
    assert not isinstance(lease_b, Exception), lease_b
    base, _ = h.base("b", lease_b, responders={"c"})
    assert base == r0


def test_c2_audit_a_forced_rejoin_does_not_outlive_the_log_it_was_forced_for(harness):
    """Round 7 audit of per-attach state.  ``force_rejoin()`` is an operator decision about one
    acceptor log (a majority of logs lost).  Forced while a rejoin runs, it must not carry over
    to a different empty log attached after a detach, as it does not survive a restart: with
    the other members back, a forced rejoin from a bare majority could miss a committed root."""
    h = harness("abcde")
    h.bootstrap("a")
    h.clock.t += 3 * GRACE
    c = _reattach_empty(h, "c")
    c.force_rejoin()                             # the operator forces this log's rejoin
    c.detach(c.tier)                             # ... which is abandoned: the space closes
    h.disks["c"] = MemoryStorage()
    c.attach(h.tier_class(h.disks["c"], "c"))
    h.clock.t += GRACE + 0.5
    # A bare majority (majority - 1 = 2 answers) completes only a forced rejoin; this one needs
    # N - majority + 1 = 3 witnesses.
    assert not h.rejoin("c", responders={"a", "b"}, timeout=5.0), "the abandoned force carried over"
    assert h.rejoin("c", responders={"a", "b", "d"})


@pytest.mark.parametrize("rate", [1.0, 1.3, 2.0, 3.0])
def test_r7_a_rejoin_is_safe_whatever_the_rate_of_the_wiped_nodes_timer(harness, rate):
    """``probe_drift_boundary`` (8.3f review inventory, #3): the wiped node c runs its timer at
    ``rate`` times the holder's.  Under the timed rule (rulings R53/R54) c's first counted round
    came before the end of a's lease for rates above 11/9, a then counted old c's ack, and b's
    lease built without R: lost at 1.3.  The ballot rejoin (ruling R78a) uses no timer: c
    rejoins at once, and either R is in its answers or R is never committed."""
    from proto_db.cluster.coordinator import ClusterCoordinator
    from tests.support.cluster import MemoryTier
    from tests.support.manual_cluster import D, MARGIN
    h = harness()
    r0 = h.bootstrap("a")
    h.clock.t += 2 * GRACE
    lease_a = h.attempt("a", {"c"})
    assert not isinstance(lease_a, Exception), lease_a
    a = h.nodes["a"]
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
    tw = h.clock.t
    c = ClusterCoordinator("c", h.peers, transport=h.net.transport(), lease_duration=D, drift_margin=MARGIN,
                           request_timeout=0.5, startup_grace=GRACE, clock=lambda: tw + rate * (h.clock.t - tw),
                           seed=1, persist_interval=60.0, persist_lag=60.0)
    c.start()
    c.attach(MemoryTier(h.disks["c"], "c"))
    h.nodes["c"] = c
    try:
        assert h.rejoin("c")                    # no clock advance at all
    finally:
        gate.set()
    outcome = _finish(h, "a", t, out, acks, expire=lease_a)
    h.clock.t += 2 * GRACE
    lease_b = h.attempt("b", {"c"})
    assert not isinstance(lease_b, Exception), lease_b
    base, _ = h.base("b", lease_b, responders={"c"})
    if isinstance(outcome, float):
        assert base == R, "R was reported committed and lost"
    else:
        assert a.committed_root()[1] != R
