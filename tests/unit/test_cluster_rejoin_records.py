"""An adopted root travels with its records (8.3d re-review 3, ruling R50).

* Y1: a node that lost its acceptor log adopts the highest accepted root of the witnesses
  (Paxos re-proposes the highest-ballot accepted value), but it becomes an acceptor and a
  witness again only once every record of that root is durable on its own disk (copied
  from the peers that hold them) or present in the object store.  Records it cannot
  obtain leave it unavailable -- still rejoining, never marked -- with the reason in
  ``stats()``.  The reviewer's schedules: three sequential single-disk losses while the
  object store is down (``probe_sequential_wipes``), and a root acknowledged by its holder
  alone that a rejoin adopts before the holder's disk is lost (``probe_uncommitted_copied``).
* The same rule for a lease holder that commits a base nobody knows committed: the base is
  re-proposed with its records, so a single disk loss afterwards loses nothing.
* Startup grace: ``0`` is accepted only on a cluster's first start.
* Y3: after a majority of acceptor logs is lost, a survivor that knows a lost commit is told
  how to recover (``force_rejoin()``), and a persisted root the witnesses lost is refounded
  from instead of being refused.
"""
import time

import pytest

from proto_db.errors import ProtoCorruptionError, ProtoOutcomeUnknownError, ProtoTimeoutError, ProtoValidationError
from tests.support.cluster import Cluster
from tests.support.manual_cluster import D, Harness, rec
from tests.support.tiered_cluster import RUN_KW, TieredCluster, incr

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


def _wait_rejoined(node, timeout=15.0) -> bool:
    deadline = time.monotonic() + timeout
    while node.stats()["rejoining"]:
        if time.monotonic() > deadline:
            return False
        time.sleep(0.01)
    return True


# -- Y1: records of the adopted root -----------------------------------------------------------

def test_y1_sequential_single_disk_losses_with_the_store_down_lose_nothing(tmp_path):
    """probe_sequential_wipes: each node in turn loses its disk and rejoins (the previous
    rejoin complete), while the object store is unreachable.  Every committed value must stay
    readable, and the cluster must keep committing and persist it all once the store returns."""
    with TieredCluster(tmp_path) as tc:
        tc.spaces["n0"].new_database("db")
        tc.store.set_outage(True)
        for i in range(6):
            tc.db(tc.ids[i % 3]).run(incr, **RUN_KW)
        for node in ("n2", "n0", "n1"):
            tc.wipe(node)
            tc.revive(node)
            assert _wait_rejoined(tc[node]), tc[node].stats().get("rejoin_blocked")
            assert tc[node].stats()["rejoin_blocked"] is None
        for node in tc.ids:
            assert tc.eventually(node, 6), (node, tc.value(node))
        tc.db("n1").run(incr, **RUN_KW)
        for node in tc.ids:
            assert tc.eventually(node, 7)
        tc.store.set_outage(False)
        tc.spaces["n1"].sync(timeout=20)


def test_y1_a_rejoin_copies_the_records_of_the_unacknowledged_root_it_adopts(harness):
    """probe_uncommitted_copied: a's root Z reached nobody; b's rejoin adopts it (the highest
    accepted root) and must copy its record, so a's disk loss afterwards loses nothing."""
    h = harness(records=True)
    h.bootstrap("a")
    h.clock.t += D
    lease = h.attempt("a", {"b"})
    z = rec(2)
    assert isinstance(h.replicate("a", lease, z, to=set(), expire=True), ProtoOutcomeUnknownError)
    h.net.drop(kinds=("release", "replicate", "records", "ack"))
    assert h.holds("a", z) and not h.holds("c", z)
    h.clock.t += D
    h.restart("b", wipe=True)                         # answered by a and c
    assert h.nodes["b"].accepted_root()[1] == z
    assert h.holds("b", z), "b adopted Z without its record"
    h.restart("a", wipe=True)                         # Z's record now lives on b only
    assert h.nodes["a"].accepted_root()[1] == z and h.holds("a", z)
    h.clock.t += D
    lease_c = h.attempt("c", {"a", "b"})
    base, _ = h.base("c", lease_c, {"a", "b"})
    assert base == z
    for n in "abc":
        if h.nodes[n].accepted_root()[1] == z:
            assert h.holds(n, z), f"{n} accepted Z without its record"


def test_y1_a_rejoin_whose_records_are_unobtainable_stays_unavailable_and_says_why(harness):
    h = harness(records=True)
    h.bootstrap("a")
    h.clock.t += D
    lease = h.attempt("a", {"b"})
    z = rec(2)
    assert isinstance(h.replicate("a", lease, z, to=set(), expire=True), ProtoOutcomeUnknownError)
    h.net.drop(kinds=("release", "replicate", "records", "ack"))
    h.clock.t += D
    b = h.restart("b", wipe=True, responders=())
    # a and c answer, but only c (which never had Z) serves records: Z's record is unobtainable.
    assert not h.rejoin("b", {"a", "c"}, timeout=4.0, servers={"c"})      # beyond one fetch_timeout
    stats = b.stats()
    assert stats["rejoining"]
    assert stats["rejoin_blocked"] and "epoch 2" in stats["rejoin_blocked"], stats["rejoin_blocked"]
    assert not h.holds("b", z)
    h.restart("b", responders=())                     # an interrupted rejoin left no marker
    assert h.nodes["b"].stats()["rejoining"]
    h.clock.t += D
    assert isinstance(h.attempt("c", {"b"}, timeout=0.3), ProtoTimeoutError)   # b grants nothing
    assert h.rejoin("b", {"a", "c"}, timeout=5.0)    # a serves Z's record now
    assert h.holds("b", z) and h.nodes["b"].stats()["rejoin_blocked"] is None


def _z_value(tc, node):
    tr = tc.db(node).new_transaction()
    try:
        z = tr.get_root_object("z")
        return None if z is None else dict(z.items())
    finally:
        tr.abort()


def _eventually_z(tc, node, expected, timeout=5.0) -> bool:
    deadline = time.monotonic() + timeout
    while _z_value(tc, node) != expected:
        if time.monotonic() > deadline:
            return False
        time.sleep(0.01)
    return True


def test_y1_a_recommitted_base_carries_its_records(tmp_path):
    """n0's root Z reaches nobody (n0 is cut off during its replication); Z creates the
    collection ``z``.  n1 then commits with n0 as its only granter: its base is Z, unknown as
    committed, so it re-proposes Z with an unchanged root on top -- and must ship Z's records
    with it.  n0's disk is lost afterwards while the store is down: ``z`` (whose records only
    Z wrote) must stay readable, and everything must be persisted once the store returns."""
    with TieredCluster(tmp_path) as tc:
        net = tc.cluster.net
        tc.spaces["n0"].new_database("db")
        tc.store.set_outage(True)
        tc.db("n0").run(incr, **RUN_KW)
        n0 = tc["n0"]
        replicate = n0.replicate

        def cut_off(lease, root, **kw):
            net.partition({"n0"}, {"n1", "n2"})
            return replicate(lease, root, **kw)

        def create_z(tr):
            tr.set_root_object("z", tr.new_counted_set().add(42))

        n0.replicate = cut_off
        with pytest.raises(ProtoOutcomeUnknownError):
            tc.db("n0").run(create_z, retries=0)
        n0.replicate = replicate
        assert n0.accepted_root()[2] > n0.committed_root()[2]
        net.heal()
        net.partition({"n2"}, {"n0", "n1"})
        tc.db("n1").run(incr, **RUN_KW)           # base Z re-committed, then n = 2 on top
        net.heal()
        assert tc.eventually("n1", 2) and _eventually_z(tc, "n1", {42: 1})
        tc.wipe("n0")                             # Z's own records were on n0's disk
        tc.kill("n1")                             # forget n1's caches; its disk stays
        tc.revive("n1")
        assert _eventually_z(tc, "n1", {42: 1}), "the records of the re-committed base were lost with n0"
        assert _eventually_z(tc, "n2", {42: 1})
        tc.revive("n0")
        assert _wait_rejoined(tc["n0"]), tc["n0"].stats().get("rejoin_blocked")
        tc.db("n0").run(incr, **RUN_KW)
        for node in tc.ids:
            assert tc.eventually(node, 3) and _eventually_z(tc, node, {42: 1})
        tc.store.set_outage(False)
        tc.spaces["n1"].sync(timeout=20)


def test_y1_any_acknowledger_of_a_recommitted_base_can_persist_it(tmp_path):
    """Z (n0's, reached nobody) is re-committed by n1 and acknowledged by n0 and n2.  Then
    n0's disk is lost and n1 goes down: n2 alone must be able to persist everything once the
    store returns, so it must know where Z's records are (they rode with the re-commit)."""
    with TieredCluster(tmp_path) as tc:
        net = tc.cluster.net
        tc.spaces["n0"].new_database("db")
        tc.store.set_outage(True)
        tc.db("n0").run(incr, **RUN_KW)
        n0 = tc["n0"]
        replicate = n0.replicate

        def cut_off(lease, root, **kw):
            net.partition({"n0"}, {"n1", "n2"})
            return replicate(lease, root, **kw)

        n0.replicate = cut_off
        with pytest.raises(ProtoOutcomeUnknownError):
            tc.db("n0").run(lambda tr: tr.set_root_object("z", tr.new_counted_set().add(42)), retries=0)
        n0.replicate = replicate
        net.heal()
        net.set_link_delay("n2", "n1", 0.3)       # n2's grants come too late: n1's majority is n0
        tc.db("n1").run(incr, **RUN_KW)
        net.set_link_delay("n2", "n1", 0)
        assert tc.eventually("n2", 2) and _eventually_z(tc, "n2", {42: 1})
        assert tc["n2"].wait_idle(5.0)
        tc.wipe("n0")
        tc.kill("n1")
        tc.store.set_outage(False)
        tc.spaces["n2"].sync(timeout=20)
        assert _eventually_z(tc, "n2", {42: 1})


# -- startup grace -------------------------------------------------------------------------------

def test_a_startup_grace_of_zero_is_refused_once_the_node_has_a_log():
    with Cluster(3, tiers=True, lease_duration=1.0, drift_margin=0.1, request_timeout=0.2,
                 startup_grace=0.0) as cl:           # a cluster's first start: 0 is fine
        with pytest.raises(ProtoValidationError, match="startup_grace"):
            cl.restart("n1")                      # the same grace, on an existing log
        cl.restart("n1", startup_grace=0.2)


def test_a_wiped_node_with_a_startup_grace_of_zero_waits_a_lease_before_rejoining(harness):
    """An empty log cannot tell a wiped node from a late member of a first start: with a
    grace of 0, a rejoin into a cluster that accepted roots waits the default grace."""
    from proto_db.storage.memory import MemoryStorage
    h = harness()
    h.bootstrap("a")                              # a cluster that accepted a root: not a first start
    old = h.nodes["b"]
    old.stop()
    h.retired.append(old)
    h.disks["b"] = MemoryStorage()                # b's disk is lost
    b = h.nodes["b"] = h._node("b", 0.0)
    deadline = time.monotonic() + 10.0
    while not b.stats().get("rejoin_blocked") and time.monotonic() < deadline:
        assert not h.rejoin("b", timeout=0.2)
    stats = b.stats()
    assert stats["rejoining"] and "startup_grace" in (stats["rejoin_blocked"] or "")
    h.clock.t += D * 0.9                          # still inside one lease from its start
    assert not h.rejoin("b", timeout=1.5)
    h.clock.t += D * 0.2
    assert h.rejoin("b")


# -- Y3: after a majority loss -------------------------------------------------------------------

def _majority_loss(h):
    """a and c accepted R; b learnt R committed from a recovery answer only (it accepted r1);
    a and c lose their disks and the operator forces their rejoins from b."""
    r1 = h.bootstrap("a")
    h.clock.t += D
    lease = h.attempt("a", {"c"})
    r = rec(2)
    assert not isinstance(h.replicate("a", lease, r, to={"c"}), Exception)
    h.release("a", lease, to=())
    h.recover("b", {"a"})
    assert h.nodes["b"].committed_root()[1] == r and h.nodes["b"].accepted_root()[1] == r1
    for n in ("a", "c"):
        h.restart(n, wipe=True, responders=())
        h.nodes[n].force_rejoin()
        assert h.rejoin(n, {"b"}, timeout=3.0)
    return r1, r


def test_y3_a_forced_rejoin_does_not_inherit_a_committed_root_above_what_it_adopted(harness):
    h = harness()
    r1, r = _majority_loss(h)
    for n in ("a", "c"):
        assert h.nodes[n].accepted_root()[1] == r1
        assert h.nodes[n].committed_root()[1] != r


def _lease_base_via(h, name, lease, responders):
    """``_lease_base`` for ``name`` under ``lease``, a recovery round answered by ``responders``;
    the result or the ProtoDB error raised."""
    from proto_db.transactions import _lease_base
    node = h.nodes[name]

    def pump():
        h.net.deliver(src=name, dst=set(responders), kinds=("query",))
        h.net.drop(src=name, kinds=("query",))
        h.net.deliver(src=None, dst={name}, kinds=("answer",))

    return h._run(lambda: _lease_base(node, node.tier, lease), pump)


def _with_lease(h, name, granters, fn):
    h.clock.t += D
    lease = h.attempt(name, granters)
    try:
        return lease, fn(lease)
    finally:
        h.nodes[name].release(lease)
        h.net.drop()


def test_y3_the_guard_names_its_remedy_and_force_rejoin_on_the_survivor_applies_it(harness):
    h = harness()
    r1, r = _majority_loss(h)
    b = h.nodes["b"]
    _, error = _with_lease(h, "b", {"c"}, lambda lease: _lease_base_via(h, "b", lease, {"c"}))
    assert isinstance(error, ProtoCorruptionError) and "force_rejoin()" in str(error), error
    b.force_rejoin()                              # the operator accepts that R is lost
    assert b.committed_root()[1] != r and b.needs_confirmation
    _, base = _with_lease(h, "b", {"c"}, lambda lease: _lease_base_via(h, "b", lease, {"c"}))
    assert base is not None and not isinstance(base, Exception) and base[0] == r1, base


def test_y3_a_persisted_root_the_witnesses_lost_is_refounded_from_not_refused(harness):
    """The committed root the survivor knows is the persisted one (its records are in the
    object store): the cluster refounds from it, as D46 prescribes for a majority loss."""
    from proto_db.cluster.coordinator import REFOUND_TERM_STEP
    h = harness()
    r1, r = _majority_loss(h)
    b = h.nodes["b"]
    label = b.committed_root()[2]
    b.tier.read_persisted = lambda: (label, r)
    _, first = _with_lease(h, "b", {"c"}, lambda lease: _lease_base_via(h, "b", lease, {"c"}))
    assert first is None                          # the term floor was raised by the refound
    lease, base = _with_lease(h, "b", {"c"}, lambda lease: _lease_base_via(h, "b", lease, {"c"}))
    assert lease.term >= label + REFOUND_TERM_STEP and base == (r, label, False), base
