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

from proto_db.cluster.rootlog import compare_roots

from proto_db.errors import ProtoCorruptionError, ProtoOutcomeUnknownError, ProtoTimeoutError, ProtoValidationError
from tests.support.cluster import Cluster
from tests.support.manual_cluster import D, GRACE, MARGIN, Harness, rec
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
    h.clock.t += GRACE
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
    h.clock.t += GRACE                                # b's answers are fresh from now on (R53)
    # a and c answer, but only c (which never had Z) serves records: Z's record is unobtainable.
    assert not h.rejoin("b", {"a", "c"}, timeout=4.0, servers={"c"})      # beyond one fetch_timeout
    stats = b.stats()
    assert stats["rejoining"]
    assert stats["rejoin_blocked"] and "epoch 2" in stats["rejoin_blocked"], stats["rejoin_blocked"]
    assert not h.holds("b", z)
    h.restart("b", responders=())                     # an interrupted rejoin left no marker
    assert h.nodes["b"].stats()["rejoining"]
    h.clock.t += GRACE
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
        # Z is accepted on n0 only, above what n0 knows committed (D50: possibly with the
        # same label, a root of the same owner term).
        acc, com = n0.accepted_root(), n0.committed_root()
        assert compare_roots(acc[2], acc[1], com[2], com[1]) > 0
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
    n0's disk is lost (ONE disk) and n1 goes down (its disk kept): nothing is lost.

    Bounded replication (Task 8.3g, D54): each commit's records live on its committer and one
    data replica, so n0's first commit lives on n0 and n1 only; with n0's disk lost and n1
    down, n2 alone cannot persist (an availability limit, not a loss) yet still reads every
    commit and knows where each commit's records are.  Once n1 is back, everything persists
    (review M1 restored the original schedule)."""
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
        known, top, _ = tc["n2"].committed_root()[:3]
        assert known and tc.storages["n2"].commit_ranges(top), "n2 does not know where the records are"
        assert tc.value("n2") == 2 and _eventually_z(tc, "n2", {42: 1})
        tc.store.set_outage(False)
        with pytest.raises(ProtoTimeoutError):
            tc.spaces["n2"].sync(timeout=2)         # n0's first commit lives on n0 and n1 only
        tc.revive("n1")
        tc.spaces["n2"].sync(timeout=20)
        assert _eventually_z(tc, "n2", {42: 1})


# -- startup grace (rulings R50, R51) -------------------------------------------------------------

def test_a_restart_on_a_log_needs_a_grace_of_one_lease_plus_the_drift_margin():
    """M1 (re-review 4): a restarted node does not remember when the grants of its previous
    life end, so any startup grace shorter than ``lease_duration + drift_margin`` is refused on
    a log that is not empty -- not only 0."""
    with Cluster(3, tiers=True, lease_duration=1.0, request_timeout=0.2,
                 startup_grace=0.0) as cl:           # a cluster's first start: 0 is fine
        safe = 1.0 + 2.0 / 3.0                          # the default margin is 2/3 D (ruling R78b)
        assert cl["n0"].restart_grace == pytest.approx(safe)
        for unsafe in (0.0, 0.2, 1.0, safe - 0.01):
            with pytest.raises(ProtoValidationError, match="startup_grace"):
                cl.restart("n1", startup_grace=unsafe)
        cl.restart("n1", startup_grace=safe)
        node = cl.restart("n2", startup_grace=None)   # the default grace is the safe one
        assert node.stats()["rejoining"] is False


@pytest.mark.parametrize("grace", [0.0, MARGIN])
def test_a_wiped_node_rejoins_by_a_ballot_whatever_its_startup_grace(harness, grace):
    """An empty log cannot tell a wiped node from a late member of a first start.  Rulings
    R53/R54 made such a rejoin wait one lease plus the drift margin; since ruling R78a it is
    a ballot round instead, so it completes at once (no clock advance) whatever the grace."""
    from proto_db.storage.memory import MemoryStorage
    h = harness()
    h.bootstrap("a")                              # a cluster that accepted a root: not a first start
    old = h.nodes["b"]
    old.stop()
    h.retired.append(old)
    h.disks["b"] = MemoryStorage()                # b's disk is lost
    b = h.nodes["b"] = h._node("b", grace)
    assert h.rejoin("b")
    assert b.stats()["rejoin_ballots"] == 1
    assert b._grants.granted_top > h.nodes["a"]._grants.terms_seen - 1


def test_i1_a_wiped_granter_ends_the_lease_it_granted_by_its_rejoin_ballot(harness):
    """probe_grace0_new_overlap (re-review 4, I1).  A new cluster has accepted no root yet, but
    a holds a lease that c granted.  c loses its disk and restarts with a grace of 0.  "No
    member accepted a root" does not mean "no member granted a lease": the answers report a
    promised term, so this is not a first start.  c's rejoin is a ballot (ruling R78a): a and b
    durably promise a ballot above a's term before answering, so a's lease can no longer
    commit anything (its own acceptance and b's refuse its term), and b may take a new lease
    from c at once.  Two members may then BELIEVE they hold a lease; only one can commit
    (lease exclusivity is a liveness aid, not a safety premise: 8.3f review inventory, #1)."""
    from proto_db.storage.memory import MemoryStorage
    h = harness()
    lease_a = h.attempt("a", {"c"})
    assert not isinstance(lease_a, Exception) and lease_a.is_valid()
    old = h.nodes["c"]
    old.stop()
    h.retired.append(old)
    h.disks["c"] = MemoryStorage()                # c, a granter of a's lease, loses its disk
    c = h.nodes["c"] = h._node("c", 0.0)
    assert h.rejoin("c")
    assert c.stats()["rejoin_ballots"] == 1
    lease_b = h.attempt("b", {"c"})
    assert not isinstance(lease_b, Exception) and lease_b.is_valid()
    assert lease_b.term > lease_a.term
    R = rec(1)
    assert isinstance(h.replicate("a", lease_a, R, to={"b", "c"}, expire=True), Exception)
    assert all(h.nodes[n].accepted_root()[1] != R for n in "abc")


def test_a_late_first_start_member_after_a_term_was_requested_completes_by_ballot(harness):
    """The bootstrap race R51's rule creates if applied literally: a member finished its first
    start and requested a lease at once (its answers now report a term), while the other two
    are still empty.  Not a first start any more, and too few witnesses: without a fallback
    both empty members stayed rejoining for good (seen as a 30 s lease timeout in
    test_sync_waits_for_the_persisted_epoch_and_exposes_both_epochs).  Every member answered
    and none accepted a root (``"rootless"``): the ballot round, confirmed by the witness a,
    completes each rejoin at once (ruling R78a; it used to wait the safe grace)."""
    h = harness()
    for n in ("b", "c"):                          # b and c are empty: their first start is not done
        h.restart(n, wipe=True, responders=())
        old = h.nodes[n]
        old.stop()
        h.retired.append(old)
        h.nodes[n] = h._node(n, 0.0)
    assert isinstance(h.attempt("a", set(), timeout=0.3), ProtoTimeoutError)   # a saw its own term
    b = h.nodes["b"]
    assert h.rejoin("b", {"a", "c"})
    assert h.rejoin("c", {"a", "b"})
    assert b.stats()["rejoin_ballots"] == 1
    assert b._grants.granted_top >= h.nodes["a"]._grants.terms_seen


def test_i1_a_genuine_first_start_with_a_grace_of_zero_does_not_wait(harness):
    """Every member starts on an empty log with a grace of 0 and nobody has asked for a term:
    the rejoins complete at once, without the clock moving."""
    h = harness()
    assert h.clock.t == 1000.0
    assert all(not node.stats()["rejoining"] for node in h.nodes.values())
    lease = h.attempt("a", {"b"})
    assert not isinstance(lease, Exception) and lease.is_valid()


def test_a_late_first_start_member_cut_off_by_a_partition_rejoins_once_it_heals():
    """The availability cost R51 accepts, bounded (re-review 5): a member whose first start
    begins after another member requested a term is not a first start; with a partition cutting
    it from every witness it stays rejoining well past the safe grace, and it rejoins -- and
    then votes and proposes -- once the partition heals.  Real clocks, real background rejoin."""
    with Cluster(3, tiers=True, lease_duration=0.5, request_timeout=0.1,
                 startup_grace=0.0) as cl:
        first = cl["n0"].acquire(timeout=5.0)        # a term is now known: no longer a first start
        cl["n0"].release(first)
        cl.net.partition(["n0", "n1"], ["n2"])
        late = cl.wipe("n2", wait=False, startup_grace=0.0)   # an empty log, as a late member has
        cut_until = time.monotonic() + 3 * late.restart_grace
        while time.monotonic() < cut_until:
            assert late.stats()["rejoining"], "rejoined without a single reachable witness"
            time.sleep(0.01)
        cl.net.heal()
        cl.wait_rejoined(["n2"], timeout=10.0)
        assert late.stats()["rejoin_blocked"] is None
        lease = late.acquire(timeout=5.0)
        assert lease.term > first.term
        late.release(lease)


# -- M2: an outcome observed after the lease -----------------------------------------------------

def test_a_majority_counted_after_the_lease_ended_is_reported_as_such(harness):
    """M2 (re-review 4): the committing thread woke after its lease had ended while a majority
    had acknowledged.  The outcome stays unknown (conservative), and the message says what
    happened instead of "only 3 of the 2 acknowledgements needed arrived"."""
    h = harness()
    lease = h.attempt("a", {"b", "c"})
    node = h.nodes["a"]
    valid = lease.is_valid

    def is_valid():               # the lease ends the moment a majority has acknowledged
        waiters = list(node._waiters.values())
        return valid() and not any(w.kind == "replicate" and len(w.acks) >= node.majority for w in waiters)

    lease.is_valid = is_valid
    error = h.replicate("a", lease, rec(2), to={"b", "c"})
    assert isinstance(error, ProtoOutcomeUnknownError), error
    text = str(error)
    assert "lease ended before" in text and "observed" in text, text
    assert "acknowledgements needed arrived" not in text, text


def test_too_few_acknowledgements_are_still_reported_as_too_few(harness):
    h = harness()
    lease = h.attempt("a", {"b", "c"})
    error = h.replicate("a", lease, rec(2), to=set(), expire=True)
    assert isinstance(error, ProtoOutcomeUnknownError), error
    assert "only 1 of the 2 acknowledgements needed" in str(error), error


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
    h.clock.t += GRACE                            # a forced rejoin also needs fresh answers (R53)
    for n in ("a", "c"):
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
    h.clock.t += GRACE
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


# -- M3: what force_rejoin() drops, and what persists ---------------------------------------------

def test_committed_knowledge_force_rejoin_drops_is_not_reloaded_by_a_restart(harness):
    """M3 (re-review 4): the committed root force_rejoin() drops on a survivor was learnt from
    an answer (accept=False knowledge), which is never logged, so a restart does not bring it
    back and the guard does not ask for force_rejoin() again."""
    h = harness()
    r1, r = _majority_loss(h)
    h.nodes["b"].force_rejoin()
    b = h.restart("b")                            # disk intact, before any new commit
    assert b.committed_root()[1] != r
    _, base = _with_lease(h, "b", {"c"}, lambda lease: _lease_base_via(h, "b", lease, {"c"}))
    assert base is not None and not isinstance(base, Exception) and base[0] == r1, base


def test_committed_knowledge_from_a_release_is_logged_accepted_and_kept_by_force_rejoin(harness):
    """M3: a committed root learnt from a release is logged; being known committed through the
    cluster it is accepted knowledge too, so it never orders above the node's accepted root,
    force_rejoin() keeps it, and a restart reloads it as both committed and accepted."""
    h = harness()
    h.bootstrap("a")
    h.clock.t += D
    lease = h.attempt("a", {"c"})
    r = rec(2)
    assert not isinstance(h.replicate("a", lease, r, to={"c"}), Exception)
    h.release("a", lease, to={"b"})               # b learns R committed from the release only
    b = h.nodes["b"]
    assert b.committed_root()[1] == r and b.accepted_root()[1] == r
    b.force_rejoin()
    assert b.committed_root()[1] == r
    b = h.restart("b")
    assert b.committed_root()[1] == r and b.accepted_root()[1] == r
    _, base = _with_lease(h, "b", {"a"}, lambda lease: _lease_base_via(h, "b", lease, {"a"}))
    assert base is not None and not isinstance(base, Exception) and base[0] == r, base
