"""Pipelined write permissions (Task 8.3k, D55; rulings R91, R93, R94).

When holder k has built its root in a round where no commit conflicted, it proposes the root
to the owner, which may issue permission k+1 on it before k is committed.  Holder k+1 builds
on the proposal; its data stage runs at once, but nothing a voter may accept leaves it, and no
own acceptance is logged, until it knows root k committed (the gate, H5).  Safety rests on the
voters alone: a voter accepts a root of the term of its predecessor only by extension of the
root it holds, which includes a root it learnt committed (V2', Raft's log matching on roots).

The planted tests prove which rule carries what:

* P1 (the gate removed) stays safe; with the voter catch-up restored (P1v), or with D55
  revision 1's V2 as written (its first-root-of-a-newer-term exemption applying to a root
  built on a root of its own term), an acknowledged commit is lost: V2' is load-bearing;
* P2 (a ``store`` accepted), P4 (H7 broken: an own acceptance classified as a conflict) and
  P5 (``_commit_base`` run on a proposal base) are each caught.

Plus: the overlap itself (fail-before evidence), the first root of every term (``_commit_base``
included) still accepted under the older-label exemption, the gate's fallback query after a
lost ``done``, a failing predecessor, and the R86 safety bounds under random fault schedules
(``PROTODB_PIPELINE_RUNS`` schedules per size, default 6; 50 for the task's acceptance runs).
"""
import os
import random
import threading
import time

import pytest

from proto_db.cluster import coordinator as coordinator_module
from proto_db.cluster.coordinator import ClusterCoordinator, _siblings
from proto_db.cluster.rootlog import compare_roots
from proto_db.errors import ProtoConflictError, ProtoDBError, ProtoOutcomeUnknownError
from proto_db import transactions
from tests.concurrency.test_cluster_owner_failures import Load, _check, _FrozenPowerLoss
from tests.support.tiered_cluster import RUN_KW, TieredCluster, incr

pytestmark = [pytest.mark.timeout(300)]

LEASE = dict(lease_duration=0.9, request_timeout=0.1)
SECONDS = 1.6
RUNS = int(os.environ.get("PROTODB_PIPELINE_RUNS", "6"))


def _cluster(tmp_path, n=3, *, storage_kw=None, **kw):
    tc = TieredCluster(tmp_path, n, storage_kw=dict(durability="fsync", **(storage_kw or {})), **dict(LEASE, **kw))
    tc.spaces[tc.ids[0]].new_database("db")
    return tc


def _setter(key):
    def write(tr):
        tr.set_root_object(key, (tr.get_root_object(key) or 0) + 1)
    return write


def _read_keys(tc, node, keys):
    tr = tc.db(node).new_transaction()
    try:
        return {k: tr.get_root_object(k) for k in keys}
    finally:
        tr.abort()


def _await(predicate, timeout=5.0, what="condition", tc=None):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            stats = {} if tc is None else {n: {k: v for k, v in tc[n].stats().items() if isinstance(v, int) and v}
                                            for n in tc.ids if n in tc.spaces}
            raise AssertionError(f"timed out waiting for {what}; {stats}")
        time.sleep(0.002)


def _confirmed(tc):
    """Every member commits once: its first commit after attach is a confirmation round (an
    empty commit, ``needs_confirmation``), which a schedule must not take for the commit it
    drives."""
    for node in tc.ids:
        tc.db(node).run(_setter("w"), **RUN_KW)


def _no_owner(tc, timeout=5.0):
    _await(lambda: all(tc[n]._ownership is None for n in tc.ids if n in tc.spaces), timeout, "the term to end")


# -- the chain rules a planted test restores ----------------------------------------------------

def _old_may_accept(catch_up: bool):
    """``_may_accept`` as D55 revision 1 specified V2 (``catch_up`` False: the exemptions for a
    node without an accepted root and for the first root of a newer term apply to every root)
    or as protocol 12 ran it (``catch_up`` True: plus the holder's claim that ``pred`` committed,
    which the holder made whenever it knew so -- and which a buggy or absent gate makes always)."""
    def may_accept(self, term, root, label, pred=None, seq=0):
        if self._state != "running" or self._refusal() is not None or self._grants.granted_top > term:
            return False
        if not self._accepted[0]:
            return True
        _, accepted, accepted_label = self._accepted
        if _siblings(label, root, accepted_label, accepted):
            return False
        order = compare_roots(label, root, accepted_label, accepted)
        if order <= 0:
            return order == 0
        if accepted_label != label:
            return True
        if self._seq_seen[0] == label and seq < self._seq_seen[1]:
            return False
        if pred is None or not pred[0] or _siblings(pred[2], pred[1], accepted_label, accepted):
            return False
        link = compare_roots(pred[2], pred[1], accepted_label, accepted)
        return link == 0 or (link > 0 and catch_up)
    return may_accept


# -- fail-before evidence (a) and (b): the pipeline overlaps rounds ----------------------------

def _slow_acceptor_writes(tc, seconds):
    """Slow every acceptor-log write that carries records (the data stage: a records-bearing
    ``replicate``, a ``store``, the holder's own append) by ``seconds``.  Root-only
    acceptances keep their speed: slowed too, the root stage after a late gate costs as much
    as the whole serial round (measured 22.4 serial against 21.9 rounds/s pipelined; the M2
    discount at its extreme -- Task 8.3k report)."""
    for node in tc.ids:
        st = tc.storages[node]
        append = st.accept_replication

        def slow(label, root, records, *a, _append=append, **kw):
            if records:
                time.sleep(seconds)
            return _append(label, root, records, *a, **kw)
        st.accept_replication = slow


def _rounds_per_second(tmp_path, monkeypatch, depth):
    monkeypatch.setattr(coordinator_module, "DEPTH", depth)
    with _cluster(tmp_path / f"d{depth}", lease_duration=3.0, request_timeout=0.25) as tc:
        _slow_acceptor_writes(tc, 0.02)
        stop = time.monotonic() + 3.0
        done = []

        def work(node, i):
            write = _setter(f"{node}-{i}")
            while time.monotonic() < stop:
                tc.db(node).run(write, **RUN_KW)
                done.append(1)
        threads = [threading.Thread(target=work, args=(n, i)) for n in tc.ids for i in range(2)]
        started = time.monotonic()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        elapsed = time.monotonic() - started
        stats = [tc[n].stats() for n in tc.ids]
        issued = sum(s["permits_issued"] for s in stats)
        pipelined = sum(s["pipelined_permits"] for s in stats)
        rounds = sum(s["replications"] for s in stats)
        return rounds / elapsed, pipelined / max(1, issued), sum(s["terms_abandoned"] for s in stats)


def test_permits_overlap_the_predecessors_durability_and_raise_the_round_rate(tmp_path, monkeypatch):
    """Records-bearing acceptor-log writes slowed to 20 ms at 3 members: at least 80 % of the permits are issued
    before their predecessor's ``done`` (on a proposal), against 0 % serially (``DEPTH = 0``
    issues no permit while one is outstanding: the base behaviour), and the cluster runs at
    least 1.5x the rounds per second."""
    serial_rate, serial_pipelined, _ = _rounds_per_second(tmp_path, monkeypatch, 0)
    rate, pipelined, abandoned = _rounds_per_second(tmp_path, monkeypatch, 1)
    print(f"\nrounds/s serial {serial_rate:.1f}, pipelined {rate:.1f}; pipelined permits {pipelined:.0%}")
    assert serial_pipelined == 0
    assert pipelined >= 0.8, f"only {pipelined:.0%} of the permits were issued on a proposal"
    assert rate >= 1.5 * serial_rate, (rate, serial_rate)
    assert abandoned <= 1          # the first term may be abandoned at start-up (base behaviour)


# -- P1, P1v and V2 as written: the voters carry safety ------------------------------------------

def _p1_schedule(tmp_path, monkeypatch, *, first_of_term: bool, rule: str):
    """Holder A (n0) builds root k and proposes it; its data message to its replica B (n1) is
    lost before B's acceptor lane makes it durable (B cached the records on receipt, so it
    can build on them), and A loses power before its own sync.  B builds k+1 on k under the
    pipelined permit; the gate is removed (P1), so B sends its root stage right after its data
    stage.  Then the whole cluster loses power.  ``rule``: ``"V2'"`` (the code), ``"V2"`` (D55
    revision 1 as written) or ``"P1v"`` (the voter catch-up restored).  Returns whether B's
    commit was acknowledged, and what every member reads afterwards."""
    fs = _FrozenPowerLoss(monkeypatch)
    monkeypatch.setattr(ClusterCoordinator, "_await_gate", lambda self, lease, base, tier: True)   # P1
    if rule != "V2'":
        monkeypatch.setattr(ClusterCoordinator, "_may_accept", _old_may_accept(rule == "P1v"))
    with _cluster(tmp_path) as tc:
        fs.writeback(str(tmp_path))
        tc.store.set_outage(True)       # the object store never holds root k's records
        for node in tc.ids:             # every member has confirmed its committed root
            tc.db(node).run(_setter("w"), **RUN_KW)
        _no_owner(tc)                   # A's next commit opens a term
        if not first_of_term:
            tc.db("n0").run(_setter("w"), **RUN_KW)    # the term's first root; k is the next one
        assert tc["n0"]._ownership is not None or first_of_term
        a, b = tc["n0"], tc["n1"]
        if a.persister is not None:
            a.persister.stop()
        lost = threading.Event()
        apply_b = b._apply_replicate

        def b_lane(msg):
            if msg["from"] == "n0" and msg["records"]:
                with b._lock:
                    b._note_owner(msg["term"])          # B learns the owner, not the records
                lost.set()
                return
            apply_b(msg)
        b._apply_replicate = b_lane
        power = threading.Event()

        def a_own_append(*args, **kw):
            power.wait(30)
            raise OSError("power lost before the holder's own sync")
        a._self_accept_with_records = a_own_append
        outcome = {}

        def commit(node, key):
            try:
                tc.db(node).run(_setter(key), retries=0)
                outcome[key] = "acked"
            except ProtoOutcomeUnknownError:
                outcome[key] = "unknown"
            except ProtoDBError as e:
                outcome[key] = type(e).__name__
        proposed = a.stats()["proposals"]
        ta = threading.Thread(target=commit, args=("n0", "a"))
        ta.start()
        _await(lambda: lost.is_set() and a.stats()["proposals"] > proposed, what="A's proposal")
        tb = threading.Thread(target=commit, args=("n1", "b"))
        tb.start()
        tb.join(30)
        assert b.stats()["pipelined_permits"] == 0 and a.stats()["pipelined_permits"] >= 1
        # The whole cluster loses power.
        for node in list(tc.ids):
            path = os.path.realpath(os.path.join(str(tmp_path), node))
            fs.frozen.add(path)
            tc.kill(node)
            fs.power_loss(path)
        power.set()
        ta.join(30)
        fs.frozen.clear()
        for node in tc.ids:
            tc.revive(node)
        reads = {}
        for node in tc.ids:
            try:
                for _ in range(10):
                    try:
                        tc.db(node).run(_setter("after"), **RUN_KW)
                        break
                    except ProtoOutcomeUnknownError:
                        continue
                reads[node] = _read_keys(tc, node, ("a", "b", "w"))
                # History is never lost (P1): every version stays readable, root k's included.
                reads[node]["versions"] = len(tc.db(node).history())
            except Exception as e:      # reported by the assertion below
                reads[node] = f"{type(e).__name__}: {e}"
        return outcome, reads


@pytest.mark.parametrize("first_of_term,rule", [
    (True, "V2'"),
    (False, "V2'"),
    (False, "V2"),       # V2 as written refuses here (no catch-up) ...
    pytest.param(True, "V2", marks=pytest.mark.xfail(strict=True, reason="... but not when k is the first root of "
                                                                           "its term: the probe's gap (V2')")),
    pytest.param(False, "P1v", marks=pytest.mark.xfail(strict=True, reason="P1v: the voter catch-up loses it")),
    pytest.param(True, "P1v", marks=pytest.mark.xfail(strict=True, reason="P1v: the voter catch-up loses it")),
])
def test_p1_without_the_gate_the_voters_alone_keep_every_acknowledged_commit(tmp_path, monkeypatch,
                                                                            first_of_term, rule):
    outcome, reads = _p1_schedule(tmp_path, monkeypatch, first_of_term=first_of_term, rule=rule)
    assert outcome.get("a") != "acked"          # A lost power inside its round
    for node, read in reads.items():
        assert isinstance(read, dict), f"{node} cannot read the history: {read} (outcomes {outcome})"
        if outcome.get("b") == "acked":
            assert read["b"] == 1, f"{node} lost B's acknowledged commit: {read}"
    print(f"\n{first_of_term=} {rule=}: outcomes {outcome}, reads {reads}")
    if rule == "V2'":
        assert outcome.get("b") == "unknown", outcome     # a root stage left: never a conflict (H7)


# -- every first root of a term passes the older-label exemption, _commit_base included ----------

def test_the_first_root_of_every_term_is_accepted_through_the_older_label_exemption(tmp_path, monkeypatch):
    """n0 commits; nobody learns it committed (its ``done`` and ``release`` are lost) and n0
    dies.  n1's next term recovers n0's root as its base and re-proposes it with
    ``_commit_base``: a root labelled with the new term, built on a base of an older label,
    which every voter accepts although its accepted root carries the older label."""
    reproposals = []
    commit_base = transactions._commit_base

    def spy(coordinator, tier, lease, label, base):
        reproposals.append((label, lease.term))
        return commit_base(coordinator, tier, lease, label, base)
    monkeypatch.setattr(transactions, "_commit_base", spy)
    with _cluster(tmp_path) as tc:
        tc.db("n0").run(incr, **RUN_KW)
        decisions = []
        for node in ("n1", "n2"):
            c = tc[node]
            orig = c._may_accept

            def recording(term, root, label, pred=None, seq=0, _orig=orig, _c=c):
                ok = _orig(term, root, label, pred, seq)
                decisions.append((_c.node_id, label, pred[2] if pred else None, _c._accepted[2], ok))
                return ok
            c._may_accept = recording
        send = tc["n0"].transport.send
        tc["n0"].transport.send = lambda to, msg: None if msg["type"] in ("done", "release") else send(to, msg)
        tc.db("n0").run(incr, **RUN_KW)
        tc.kill("n0")
        for _ in range(10):
            try:
                tc.db("n1").run(incr, **RUN_KW)
                break
            except ProtoOutcomeUnknownError:
                continue
        assert reproposals and all(term > label for label, term in reproposals), reproposals
        firsts = [d for d in decisions if d[2] is not None and d[2] < d[1] and d[3] < d[1]]
        assert firsts and all(d[4] for d in firsts), decisions
        assert tc.value("n1") == 3


# -- P2: a store is never an acceptance ----------------------------------------------------------

def _p2_mutation(self, tier, msg, label, root, records, ranges):
    """P2: the store path accepts when the chain rule allows (what V1 forbids)."""
    with self._log_lock:
        with self._lock:
            ok = self._may_accept(msg["term"], root, label, None, 0) or (
                self._accepted[0] and self._accepted[2] == label)
        tier.accept_replication(label, root, records, ranges=ranges, accepted=ok)
        if ok:
            with self._lock:
                self._observe_accepted(True, root, label)


@pytest.mark.parametrize("planted", [False, pytest.param(True, marks=pytest.mark.xfail(strict=True,
                                                                                       reason="P2"))])
def test_p2_a_closed_gate_after_a_store_is_a_truthful_conflict(tmp_path, monkeypatch, planted):
    """k is committed, but its ``done`` and the gate's fallback answers never reach k+1's
    holder, whose gate closes after its ``store`` left: its caller gets a conflict, which
    promises that the commit never takes effect (``retries=0`` here, so the caller sees it).
    With P2 planted, k+1's replica accepted k+1 in its store entry, the next phase 1 adopts
    it, and the "conflicted" commit becomes visible -- under ``Database.run``'s retry, the
    increment would apply twice."""
    if planted:
        monkeypatch.setattr(ClusterCoordinator, "_store_entry", _p2_mutation)
    with _cluster(tmp_path) as tc:
        tc.db("n0").run(incr, **RUN_KW)
        _confirmed(tc)
        tc.store.set_outage(True)       # no persister teaches B committed roots from the store
        a, b = tc["n0"], tc["n1"]
        # n1 never learns committed knowledge from anyone while k+1 is open.
        blocked = threading.Event()
        blocked.set()
        for node in ("n0", "n2"):
            send = tc[node].transport.send

            def filtered(to, msg, _send=send):
                if blocked.is_set() and to == "n1" and msg["type"] in ("done", "answer", "release"):
                    return None
                return _send(to, msg)
            tc[node].transport.send = filtered
        hold = threading.Event()
        own = a._self_accept_with_records

        def slow_own(*args, **kw):
            hold.wait(5)
            return own(*args, **kw)
        a._self_accept_with_records = slow_own
        results = {}

        def run(node):
            try:
                tc.db(node).run(_setter(node), **(RUN_KW if node == "n0" else dict(retries=0)))
                results[node] = "acked"
            except ProtoOutcomeUnknownError:
                results[node] = "unknown"
            except ProtoConflictError:
                results[node] = "conflict"
        proposed = a.stats()["proposals"]
        ta = threading.Thread(target=run, args=("n0",))
        ta.start()
        _await(lambda: a.stats()["proposals"] > proposed, what="A's proposal")
        tb = threading.Thread(target=run, args=("n1",))
        tb.start()
        _await(lambda: b.stats()["stores_sent"] >= 1, what="B's store", tc=tc)
        hold.set()
        ta.join(30)
        _await(lambda: b.stats()["gate_closures"] >= 1 or not tb.is_alive(), timeout=10, what="B's gate closing", tc=tc)
        blocked.clear()
        tb.join(60)
        assert b.stats()["gate_fallbacks"] >= 1
        # An owner change: a fresh phase 1 decides what the term left behind.
        _no_owner(tc, timeout=10)
        tc.db("n2").run(_setter("n2"), **RUN_KW)
        _await(lambda: len({str(_read_keys(tc, n, ("n1",))) for n in tc.ids}) == 1, what="convergence")
        value = _read_keys(tc, "n2", ("n1",))["n1"]
        assert results.get("n1") == "conflict", results
        assert value is None, f"the conflicted commit took effect: {value} ({results})"


# -- P4: an own acceptance logged is never a conflict -------------------------------------------

@pytest.mark.parametrize("planted", [False, pytest.param(True, marks=pytest.mark.xfail(strict=True,
                                                                                       reason="P4"))])
def test_p4_a_failed_data_stage_after_an_own_acceptance_is_unknown(tmp_path, monkeypatch, planted):
    """The gate is open at the own append, so the holder logs its own kind-5 acceptance; every
    data replica's copy is lost (``copies is None``).  The outcome must be unknown.  With P4
    planted it is reported as a conflict, the caller retries after an owner change, the new
    phase 1 adopts the root the holder accepted, and the increment applies twice."""
    if planted:
        monkeypatch.setattr(ClusterCoordinator, "_data_stage_acceptable", staticmethod(lambda use_store: False))
    with _cluster(tmp_path) as tc:
        tc.db("n0").run(incr, **RUN_KW)
        _await(lambda: all(tc.value(n) == 1 for n in tc.ids), what="convergence")
        _no_owner(tc)
        a = tc["n0"]
        send = a.transport.send
        dropping = threading.Event()
        dropping.set()
        a.transport.send = lambda to, msg: None if dropping.is_set() and (
            msg["type"] in ("records", "store") or (msg["type"] == "replicate" and msg["records"])) \
            else send(to, msg)
        acked, unknown = 1, 0
        try:
            tc.db("n0").run(incr, retries=0)
            acked += 1
        except ProtoOutcomeUnknownError:
            unknown += 1
            retry = False
        except ProtoConflictError:
            retry = True
        else:
            retry = False
        dropping.clear()
        # The caller's policy (Database.run's): a conflict is retried -- after an owner change.
        _no_owner(tc, timeout=10)
        tc.kill(next(n for n in tc.ids if n != "n0" and n != "n1"))
        if retry:
            for _ in range(10):
                try:
                    tc.db("n0").run(incr, **RUN_KW)
                    acked += 1
                    break
                except ProtoOutcomeUnknownError:
                    unknown += 1
        else:
            tc.db("n1").run(lambda tr: None, **RUN_KW)
        live = ["n0", "n1"]
        _await(lambda: len({tc.value(n) for n in live}) == 1, timeout=10, what="convergence")
        value = tc.value("n1")
        assert acked <= value <= acked + unknown, (value, acked, unknown)


# -- P5: a proposal base is never re-proposed --------------------------------------------------

@pytest.mark.parametrize("planted", [False, pytest.param(True, marks=pytest.mark.xfail(strict=True,
                                                                                       reason="P5"))])
def test_p5_a_proposal_base_is_never_re_proposed(tmp_path, monkeypatch, planted):
    """H2: a round on a proposal base sends one root, its own, built on that base, and proposes
    it.  With P5 planted (``_commit_base`` runs because the base is not known committed), the
    round sends a re-proposal root first and its own root no longer extends the issued base,
    so it cannot propose: in a fault-free, conflict-free run nearly every round must propose,
    no term is abandoned, and every acknowledged commit is read back.  The planted slip shows
    as either symptom (proposals below 80 % of replications, or an abandoned term: the owner
    sees a permission end without a root while another is outstanding), so both are one
    stated assertion.

    The design's P5 expected a safety symptom (a re-proposal sent before the gate).  Here the
    gate guards every first root of a pipelined permit inside ``replicate`` itself, the
    re-proposal included, so the planted slip costs the pipeline, never safety; the test pins
    it by that cost."""
    if planted:
        monkeypatch.setattr(transactions, "_base_needs_commit",
                            lambda coordinator, permission, current: coordinator.known_root() != (True, current))
    with _cluster(tmp_path, lease_duration=3.0, request_timeout=0.25) as tc:
        _confirmed(tc)
        before = {n: tc[n].stats() for n in tc.ids}
        stop = time.monotonic() + SECONDS
        acked = {}

        def work(node, i):
            key = f"{node}-{i}"
            write = _setter(key)
            while time.monotonic() < stop:
                tc.db(node).run(write, **RUN_KW)
                acked[key] = acked.get(key, 0) + 1
        threads = [threading.Thread(target=work, args=(n, i)) for n in tc.ids for i in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        after = {n: tc[n].stats() for n in tc.ids}

        def delta(key):
            return sum(after[n][key] - before[n][key] for n in tc.ids)
        assert delta("pipelined_permits") > 10          # precondition: holds with P5 planted too
        # The one assertion P5 trips.  Its symptom shows either way, nondeterministically
        # (review 8.3k, M2): a round whose root no longer extends its issued base cannot
        # propose, so proposals fall below 80 % of replications; or the owner sees a permission
        # end without a root while another is outstanding, and abandons its term.
        proposals, replications, abandoned = delta("proposals"), delta("replications"), delta("terms_abandoned")
        assert abandoned == 0 and proposals >= 0.8 * replications, \
            f"the pipeline collapsed: {proposals} proposals in {replications} replications, {abandoned} terms abandoned"
        time.sleep(0.2)
        for node in tc.ids:
            values = _read_keys(tc, node, list(acked))
            assert all((values[k] or 0) >= v for k, v in acked.items()), (node, values, acked)


# -- the gate's fallback after a lost done (I4) and a failing predecessor ------------------------

def test_a_lost_done_to_the_successor_is_answered_by_the_fallback_query(tmp_path):
    """Only the ``done`` from k's holder to k+1's holder is dropped: the gate opens through
    the plain query/answer fallback (ruling R94) within about ``request_timeout``, the term is
    not abandoned, and both commits are acknowledged."""
    with _cluster(tmp_path) as tc:
        tc.db("n0").run(incr, **RUN_KW)
        _confirmed(tc)
        tc.store.set_outage(True)       # no persister teaches B committed roots from the store
        a, b = tc["n0"], tc["n1"]
        send = a.transport.send
        a.transport.send = lambda to, msg: None if (to == "n1" and msg["type"] == "done") else send(to, msg)
        hold = threading.Event()
        own = a._self_accept_with_records

        def slow_own(*args, **kw):
            hold.wait(5)
            return own(*args, **kw)
        a._self_accept_with_records = slow_own
        results = {}

        def run(node):
            try:
                tc.db(node).run(_setter(node), **RUN_KW)
                results[node] = "acked"
            except ProtoDBError as e:
                results[node] = repr(e)
        proposed = a.stats()["proposals"]
        ta = threading.Thread(target=run, args=("n0",))
        ta.start()
        _await(lambda: a.stats()["proposals"] > proposed, what="A's proposal")
        abandoned = sum(tc[n].stats()["terms_abandoned"] for n in tc.ids)
        tb = threading.Thread(target=run, args=("n1",))
        tb.start()
        _await(lambda: b.stats()["stores_sent"] >= 1, what="B's store", tc=tc)
        hold.set()
        ta.join(30)
        tb.join(30)
        assert results == {"n0": "acked", "n1": "acked"}, results
        assert b.stats()["gate_fallbacks"] >= 1 and b.stats()["gate_closures"] == 0
        assert sum(tc[n].stats()["terms_abandoned"] for n in tc.ids) == abandoned
        _await(lambda: all(_read_keys(tc, n, ("n0", "n1")) == {"n0": 1, "n1": 1} for n in tc.ids),
               what="every member reading both commits")


@pytest.mark.parametrize("outage", [True, False])
def test_a_successor_whose_base_records_are_nowhere_gets_a_retryable_conflict(tmp_path, outage):
    """Review 8.3k, I1.  The owner is n2 (it owns the term, and A's proposal reaches it before
    B asks: B's permit is issued on root k).  A (n0) proposes root k, and none of A's record
    copies has left A; the permit for B (n1) on root k is in flight when A dies.  B's build
    finds root k's records in no tier.  Before D55 a base always had ``f + 1`` copies, so
    that read failure meant corruption; on a proposal base it is the normal consequence of
    a failed predecessor.  B waits for its gate: it closes (k never committed), and B's
    commit gets a truthful, retryable ``ProtoConflictError`` -- nothing was sent (Lemma N)
    -- never ``ProtoCorruptionError`` nor ``ProtoUnexpectedError`` (7b2a7c4 gave the latter in
    both cases)."""
    with _cluster(tmp_path) as tc:
        _confirmed(tc)
        _no_owner(tc)
        tc.store.set_outage(outage)
        a, b, o = tc["n0"], tc["n1"], tc["n2"]
        tc.db("n2").run(_setter("o"), **RUN_KW)          # n2 owns the term
        assert o._ownership is not None
        send_a = a.transport.send
        a.transport.send = lambda to, msg: None if msg["type"] in ("warm", "records", "store") or (
            msg["type"] == "replicate" and msg["records"]) else send_a(to, msg)
        permit_out = threading.Event()
        send_o = o.transport.send

        def owner_send(to, msg):
            if to == "n1" and msg["type"] == "permit" and msg["after"] is not None:
                permit_out.set()
                threading.Timer(0.3, send_o, args=(to, msg)).start()
                return None
            return send_o(to, msg)
        o.transport.send = owner_send
        hold = threading.Event()

        def stuck_own(*args, **kw):
            hold.wait(10)
            raise OSError("A died before its own sync")
        a._self_accept_with_records = stuck_own
        results = {}

        def run(node, key):
            try:
                tc.db(node).run(_setter(key), **dict(RUN_KW, retries=0))
                results[key] = "acked"
            except ProtoOutcomeUnknownError:
                results[key] = "unknown"
            except ProtoConflictError:
                results[key] = "conflict"
            except ProtoDBError as e:
                results[key] = f"{type(e).__name__}: {str(e)[:160]}"
        ta = threading.Thread(target=run, args=("n0", "a"))
        tb = threading.Thread(target=run, args=("n1", "b"))
        ta.start()

        def proposed_to_owner():
            # A's proposal reached the owner: B's permit can then only be issued on it (O2).
            # (A's ``proposals`` counter is no witness: ``_confirmed`` made A propose already,
            # so B could otherwise be served first -- review 8.3k re-review, N1.)
            with o._cond:
                own = o._ownership
                return own is not None and any(e.node == "n0" and e.proposal is not None for e in own.pipeline)
        _await(proposed_to_owner, what="A's proposal at the owner")
        tb.start()
        _await(permit_out.is_set, what="B's permit on root k")
        tc.cluster.crash("n0")
        hold.set()
        tb.join(30)
        ta.join(30)
        assert results.get("b") == "conflict", results
        if outage:          # with the store up, A's persister may have uploaded root k's segment:
            assert b.stats()["gate_closures"] >= 1, b.stats()     # B then builds, and its gate closes
        # The conflict is truthful: in the next term, b was never applied.
        tc.store.set_outage(False)
        _no_owner(tc, timeout=10)
        tc.db("n2").run(_setter("c"), **RUN_KW)
        for node in ("n1", "n2"):
            _await(lambda: _read_keys(tc, node, ("c",))["c"] == 1, what=f"the next term at {node}")
            assert _read_keys(tc, node, ("b",))["b"] is None, node


def test_acks_after_the_predecessors_validity_give_an_unknown_that_took_effect(tmp_path):
    """Review 8.3k, Priority 1 (the 50-row benchmark's failure signature).  Holder A (n0)
    proposes root k; the owner issues k+1 to B (n1) on root k; B stores its records and its
    gate stays closed.  n1 and n2 accept root k durably, but every ack reaches A 1.5 s late,
    after A's validity: A reports unknown, nobody learns k committed, B's gate closes (on A's
    ``done`` saying unknown, or when its fallback query is answered without k) and B's
    conflict is retried; the owner abandons its term.  R86: b is applied exactly once; a (unknown) is applied at most once -- here it
    took effect, an EXTRA outcome R86 allows."""
    with _cluster(tmp_path) as tc:
        _confirmed(tc)
        _no_owner(tc)
        a, b = tc["n0"], tc["n1"]
        armed = threading.Event()
        for member in ("n1", "n2"):
            send = tc[member].transport.send

            def late(to, msg, _send=send):
                if armed.is_set() and to == "n0" and msg["type"] == "ack" and msg["ok"]:
                    threading.Timer(1.5, _send, args=(to, msg)).start()
                    return None
                return _send(to, msg)
            tc[member].transport.send = late
        hold = threading.Event()
        own = a._self_accept_with_records

        def slow_own(*args, **kw):
            hold.wait(5)
            return own(*args, **kw)
        a._self_accept_with_records = slow_own
        results = {}

        def run(node, key, retries):
            try:
                tc.db(node).run(_setter(key), **dict(RUN_KW, retries=retries))
                results[key] = "acked"
            except ProtoOutcomeUnknownError:
                results[key] = "unknown"
            except ProtoConflictError:
                results[key] = "conflict"
        abandoned = {m: tc[m].stats()["terms_abandoned"] for m in tc.ids}
        armed.set()
        ta = threading.Thread(target=run, args=("n0", "a", 0))
        ta.start()
        owner = {}

        def proposed_to_owner():
            # A's proposal reached the owner, so B's permit is issued on it.  A's ``proposals``
            # counter is no witness: it counts sends (re-review N1; the earlier test of this file
            # waits the same way).
            for m in tc.ids:
                with tc[m]._cond:
                    own = tc[m]._ownership
                    if own is not None and any(e.node == "n0" and e.proposal is not None for e in own.pipeline):
                        owner["term"], owner["node"] = own.term, m
                        return True
            return False
        _await(proposed_to_owner, what="A's proposal at the owner")
        # B must also know that owner's term: it asks the owner of the term it learnt last, and an
        # owner refuses a request for another term ("not_owner").  Under CPU contention B had not
        # always processed the new term's ballot yet: refused, B took the unpipelined path and
        # never reached its data stage (6 of 40 GIL-on runs; review 8.3m, ruling R105).
        _await(lambda: b._owner_hint == (owner["term"], owner["node"]), what="B knowing the owner's term", tc=tc)
        tb = threading.Thread(target=run, args=("n1", "b", 200))
        tb.start()
        _await(lambda: b.stats()["stores_sent"] >= 1 or b.stats()["gate_open_at_send"] >= 1,
               what="B's data stage", tc=tc)
        hold.set()
        ta.join(30)
        tb.join(60)
        armed.clear()
        _no_owner(tc, timeout=10)
        tc.db("n2").run(_setter("c"), **RUN_KW)
        _await(lambda: len({str(_read_keys(tc, m, ("a", "b", "c"))) for m in tc.ids}) == 1, what="convergence")
        reads = _read_keys(tc, "n2", ("a", "b", "c"))
        assert results == {"a": "unknown", "b": "acked"}, results
        assert reads["b"] == 1 and reads["a"] in (None, 1) and reads["c"] == 1, reads
        assert b.stats()["gate_closures"] >= 1
        assert a.stats()["replication_failures"] >= 1
        assert sum(tc[m].stats()["terms_abandoned"] - abandoned[m] for m in tc.ids) >= 1


def test_a_failing_predecessor_turns_its_successor_into_a_retried_conflict(tmp_path):
    """k's holder fails inside its round (its own sync fails: unknown); its successor's gate
    never opens, so the successor's commit ends with a conflict, which ``Database.run``
    retries in a later term; nothing of the conflicted round is ever visible (R86)."""
    with _cluster(tmp_path) as tc:
        tc.db("n0").run(incr, **RUN_KW)
        _confirmed(tc)
        a, b = tc["n0"], tc["n1"]
        hold = threading.Event()

        def failing_own(*args, **kw):
            hold.wait(5)
            raise OSError("the holder's own sync failed")
        a._self_accept_with_records = failing_own
        send = a.transport.send
        a.transport.send = lambda to, msg: None if msg["type"] in ("records",) or (
            msg["type"] == "replicate") else send(to, msg)
        results = {}

        def run(node, key):
            try:
                tc.db(node).run(_setter(key), **RUN_KW)
                results[key] = "acked"
            except ProtoOutcomeUnknownError:
                results[key] = "unknown"
        proposed = a.stats()["proposals"]
        ta = threading.Thread(target=run, args=("n0", "a"))
        ta.start()
        _await(lambda: a.stats()["proposals"] > proposed, what="A's proposal")
        tb = threading.Thread(target=run, args=("n1", "b"))
        tb.start()
        _await(lambda: b.stats()["stores_sent"] >= 1, what="B's store", tc=tc)
        hold.set()
        ta.join(30)
        del a._self_accept_with_records
        a.transport.send = send
        tb.join(60)
        assert results["a"] == "unknown" and results["b"] == "acked", results
        assert b.stats()["gate_closures"] >= 1
        values = _read_keys(tc, "n1", ("a", "b"))
        assert values["b"] == 1 and values["a"] in (None, 1), values


def test_a_gate_closed_early_ends_its_permission(tmp_path, monkeypatch):
    """Re-review 8.3k, N2.  The owner is n2.  A (n0) proposes root k; none of its record copies
    leaves it and it serves no fetch; B (n1) is issued k+1 on root k, its build finds root k's
    records nowhere (the store is out) and it waits at its gate (I1).  A's own sync then fails: its ``done`` says
    unknown and closes B's gate at once (M4), while B's permission is still valid.  That
    permission must end there: before the fix its held retry rebuilt on the dead proposal
    (another I1 conflict) for up to ``_PERMIT_RETRIES`` rounds.  B's commit is retried under
    a later permission and acknowledged; b is applied once."""
    with _cluster(tmp_path) as tc:
        _confirmed(tc)
        _no_owner(tc)
        tc.store.set_outage(True)
        a, b, o = tc["n0"], tc["n1"], tc["n2"]
        tc.db("n2").run(_setter("o"), **RUN_KW)          # n2 owns the term
        assert o._ownership is not None
        closed, late_rounds = set(), []
        wait = b._await_gate

        def recording_gate(lease, base, tier):
            opened = wait(lease, base, tier)
            if not opened:
                closed.add(id(lease))
            return opened
        b._await_gate = recording_gate
        permit_round = transactions._permit_round

        def recording_round(coordinator, tier, permission, *args, **kw):
            if coordinator is b and id(permission) in closed:
                late_rounds.append(permission.seq)
            return permit_round(coordinator, tier, permission, *args, **kw)
        monkeypatch.setattr(transactions, "_permit_round", recording_round)
        send_a = a.transport.send
        # No copy of A's records leaves A, and A serves no fetch: root k's records are nowhere
        # B can read them, while A stays alive to send its done.
        a.transport.send = lambda to, msg: None if msg["type"] in ("warm", "records", "store", "fetched") or (
            msg["type"] == "replicate" and msg["records"]) else send_a(to, msg)
        hold = threading.Event()

        def failing_own(*args, **kw):
            hold.wait(10)
            raise OSError("A's own sync failed")
        a._self_accept_with_records = failing_own
        results = {}

        def run(node, key):
            try:
                tc.db(node).run(_setter(key), **RUN_KW)
                results[key] = "acked"
            except ProtoOutcomeUnknownError:
                results[key] = "unknown"
        ta = threading.Thread(target=run, args=("n0", "a"))
        tb = threading.Thread(target=run, args=("n1", "b"))
        ta.start()

        def proposed_to_owner():
            with o._cond:
                own = o._ownership
                return own is not None and any(e.node == "n0" and e.proposal is not None for e in own.pipeline)
        _await(proposed_to_owner, what="A's proposal at the owner")
        tb.start()
        _await(lambda: b.stats()["gate_waits"] >= 1, what="B waiting at its gate", tc=tc)
        hold.set()                      # A's done(unknown) closes B's gate early
        ta.join(30)
        del a._self_accept_with_records
        a.transport.send = send_a
        tc.store.set_outage(False)
        tb.join(60)
        assert results == {"a": "unknown", "b": "acked"}, results
        assert closed, "B's gate never closed"
        assert late_rounds == [], f"rounds ran on permissions whose gate had closed: {late_rounds}"
        assert _read_keys(tc, "n1", ("b",))["b"] == 1


def test_a_successor_that_finishes_first_waits_at_the_gate(tmp_path):
    """The predecessor's own sync is held until the successor's data stage has left: the
    successor's own acceptance and root stage come only after it learnt ``committed(root_k)``,
    and both commits are acknowledged."""
    with _cluster(tmp_path, lease_duration=3.0, request_timeout=0.5) as tc:
        tc.db("n0").run(incr, **RUN_KW)
        _confirmed(tc)
        a, b = tc["n0"], tc["n1"]
        events = []
        hold = threading.Event()
        own = a._self_accept_with_records

        def slow_own(*args, **kw):
            hold.wait(5)
            return own(*args, **kw)
        a._self_accept_with_records = slow_own
        observe = b._observe_committed

        def watch(known, root, label, by, **kw):
            learnt = observe(known, root, label, by, **kw)
            if learnt:
                events.append(("learnt", root))
            return learnt
        b._observe_committed = watch
        accept_root = b._self_accept_root

        def own_root(tier, waiter, label, root, seq, pred, *rest):
            events.append(("own acceptance", pred[1]))
            return accept_root(tier, waiter, label, root, seq, pred, *rest)
        b._self_accept_root = own_root
        proposed = a.stats()["proposals"]
        ta = threading.Thread(target=lambda: tc.db("n0").run(_setter("n0"), **RUN_KW))
        ta.start()
        _await(lambda: a.stats()["proposals"] > proposed, what="A's proposal")
        tb = threading.Thread(target=lambda: tc.db("n1").run(_setter("n1"), **RUN_KW))
        tb.start()
        _await(lambda: b.stats()["stores_sent"] >= 1, what="B's store", tc=tc)
        assert not any(e[0] == "own acceptance" for e in events)
        hold.set()
        ta.join(30)
        tb.join(30)
        own = [i for i, e in enumerate(events) if e[0] == "own acceptance"]
        assert own, events
        base = events[own[0]][1]
        assert any(e == ("learnt", base) for e in events[:own[0]]), events
        _await(lambda: all(_read_keys(tc, n, ("n0", "n1")) == {"n0": 1, "n1": 1} for n in tc.ids),
               what="every member reading both commits")


# -- V1: a store is never an acceptance ----------------------------------------------------------

def test_a_store_is_never_an_acceptance(tmp_path):
    """The successor's data replica applies its ``store``: one synced store-only entry; the
    replica's accepted root and permission sequence do not move, its ack says ``stored`` and
    never ``ok``, and its root log replays no acceptance of that root (kind 6)."""
    with _cluster(tmp_path) as tc:
        tc.db("n0").run(incr, **RUN_KW)
        _confirmed(tc)
        a = tc["n0"]
        hold = threading.Event()
        own = a._self_accept_with_records

        def slow_own(*args, **kw):
            hold.wait(5)
            return own(*args, **kw)
        a._self_accept_with_records = slow_own
        seen, acks = [], []
        for name in ("n0", "n2"):               # whichever member is the successor's replica
            replica = tc[name]
            apply = replica._apply_store

            def watched(msg, _apply=apply, _r=replica):
                with _r._lock:
                    before = (_r._accepted, _r._seq_seen, _r._stats["accepted"])
                _apply(msg)
                with _r._lock:
                    after = (_r._accepted, _r._seq_seen, _r._stats["accepted"])
                seen.append((_r, msg, before, after))
            replica._apply_store = watched
            send = replica.transport.send

            def acked(to, msg, _send=send):
                if msg["type"] == "ack" and to == "n1":
                    acks.append(msg)
                return _send(to, msg)
            replica.transport.send = acked
        proposed = a.stats()["proposals"]
        ta = threading.Thread(target=lambda: tc.db("n0").run(_setter("n0"), **RUN_KW))
        ta.start()
        _await(lambda: a.stats()["proposals"] > proposed, what="A's proposal")
        tb = threading.Thread(target=lambda: tc.db("n1").run(_setter("n1"), **RUN_KW))
        tb.start()
        _await(lambda: seen, what="the replica's store", tc=tc)
        hold.set()
        ta.join(30)
        tb.join(30)
        replica, msg, before, after = seen[0]
        assert before == after, (before, after)
        # The first ack of the round's rid answers the store; a later one answers the root stage.
        rid_acks = [m for m in acks if m["rid"] == msg["rid"] and m["from"] == replica.node_id]
        assert rid_acks and rid_acks[0]["ok"] is False and rid_acks[0]["stored"], rid_acks
        assert replica.stats()["stores_applied"] >= 1
        from proto_db.cluster.protocol import decode_labeled_root
        _, root, label = decode_labeled_root(msg["root"])
        logged = tc.storages[replica.node_id].load_roots()
        # Accepted later through the root stage (a root-only kind-5 entry), never by the store.
        assert logged.accepted is None or logged.accepted != (label, root) or replica.stats()["accepted"] > after[2]


# -- test 19: V2' in serial rounds on a lossy link ----------------------------------------------

def test_the_cluster_progresses_at_20_percent_loss(tmp_path):
    """A voter that missed root k and every message teaching ``committed(k)`` refuses k+1 once
    and extends from the next ``done``: the cost of V2' is liveness only.  At 20 % message
    loss the cluster keeps committing, and the R86 bounds hold."""
    with _cluster(tmp_path, lease_duration=1.5, request_timeout=0.1) as tc:
        tc.cluster.net.set_faults(drop=0.2)
        with Load(tc) as load:
            time.sleep(3 * SECONDS)
        stats = [tc[n].stats() for n in tc.ids]
        print(f"\n20 % loss: {load.acked} acknowledged, {load.unknown} unknown in {3 * SECONDS:.1f} s; "
              f"refusals {sum(s['refused'] for s in stats)}, terms abandoned "
              f"{sum(s['terms_abandoned'] for s in stats)}")
        assert load.acked >= 5
        _check(tc, load, tc.ids)


# -- 15/18: R86 safety under random fault schedules with the pipeline running -------------------

#: Faults aimed at the predecessor's window (review 8.3k, I2): each one aimed must land.
WINDOW_FAULTS = ("crash_after_propose", "crash_after_root_stage", "late_acks")
#: A holder killed at its gate: lands only when some successor waits there (with the GIL off
#: most pipelined permits find their gate open at send), so it is counted, not required.
AIMED_FAULTS = WINDOW_FAULTS + ("gate",)


def _arm(tc, load, how):
    """Arm the targeted fault ``how`` on every live member's current incarnation: the first
    member it hits is the victim.  Return ``(victim list, fired, disarm)``.  One lock-taken arm
    (a fault fires at most once); a crash stops the captured incarnation's transport only
    (never a revived one); the caller disarms every unfired hook (the review's race-free
    arming)."""
    fired = threading.Event()
    lock = threading.Lock()
    armed = [True]
    victim = []         # the member hit, then (term, seq) of the proposing permission hit
    undo = []
    late = []           # late_acks: the proposing permission whose acks are delayed

    def take(member):
        with lock:
            if not armed[0]:
                return False
            armed[0] = False
            victim.append(member)
        return True

    def disarm():
        with lock:
            armed[0] = False
        for fn in undo:
            fn()
    for member in [m for m in tc.ids if m not in load.down]:
        c = tc.cluster[member]

        def crash(_c=c, _m=member):
            load.down.add(_m)
            _c.transport.stop()             # this incarnation only
            fired.set()
        if how == "gate":
            # A holder waiting at its gate (after its store left) dies.
            def at_gate(lease, base, tier, _wait=c._await_gate, _m=member, _crash=crash):
                if take(_m):
                    _crash()
                return _wait(lease, base, tier)
            c._await_gate = at_gate
            undo.append(lambda _c=c: _c.__dict__.pop("_await_gate", None))
        elif how == "crash_after_propose":
            # The predecessor dies between its proposal and its data stage.
            def after_propose(lease, proposal, _propose=c._propose, _m=member, _crash=crash):
                _propose(lease, proposal)
                if take(_m):
                    victim.append((lease.term, lease.seq))
                    _crash()
            c._propose = after_propose
            undo.append(lambda _c=c: _c.__dict__.pop("_propose", None))
        elif how == "crash_after_root_stage":
            # A proposing holder dies right after its root stage left (it may be committed).
            def after_root(to, msg, _send=c.transport.send, _c=c, _m=member, _crash=crash):
                _send(to, msg)
                p = _c._permit
                if msg["type"] == "replicate" and not msg["records"] and getattr(p, "proposed", False) \
                        and take(_m):
                    victim.append((p.term, p.seq))
                    _crash()
            c.transport.send = after_root
            undo.append(lambda _c=c: _c.transport.__dict__.pop("send", None))
        else:
            # late_acks: every ok ack of one proposing permission arrives after that permission's
            # validity (the 8.3k benchmark signature: an unknown outcome that took effect).
            def delayed(to, msg, _send=c.transport.send):
                if msg["type"] == "ack" and msg["ok"]:
                    p = getattr(tc.cluster[to], "_permit", None) if to in tc.ids else None
                    with lock:
                        if not victim and armed[0] and p is not None and getattr(p, "proposed", False):
                            armed[0] = False
                            victim.extend([to, (p.term, p.seq)])
                            late.append(p)
                            fired.set()
                    if late and late[0] is p and victim and victim[0] == to:
                        threading.Timer(LEASE["lease_duration"] * 1.2, _send, args=(to, msg)).start()
                        return None
                return _send(to, msg)
            c.transport.send = delayed
            undo.append(lambda _c=c: _c.transport.__dict__.pop("send", None))
    return victim, fired, disarm


@pytest.fixture(scope="module")
def window_tally():
    """Across the fault schedules of one run: at the acceptance size (50 per member count),
    at least one landed window fault must have hit a proposal that a successor was issued on
    (re-review 8.3k, N3).  Smaller runs only report the count."""
    tally = {"schedules": 0, "overlapped": 0}
    yield tally
    print(f"\nwindow faults with a successor in flight: {tally['overlapped']} in {tally['schedules']} schedules")
    if tally["schedules"] >= 100:
        assert tally["overlapped"] >= 1, tally


@pytest.mark.timeout(600)
@pytest.mark.parametrize("n", [3, 5])
@pytest.mark.parametrize("seed", range(RUNS))
def test_no_acknowledged_commit_is_lost_with_the_pipeline_under_faults(tmp_path, monkeypatch, n, seed, window_tally):
    """Every member commits (two threads each) while links duplicate (up to 100 %), reorder
    (delay jitter) and the schedule kills members, partitions, and aims faults at the
    pipeline: a holder killed at its gate, a predecessor killed right after its proposal or
    right after its root stage, a proposer's acknowledgements delayed past its validity.  Each
    aimed fault must land (counted), and ruling R86 bounds hold: ``acked <= n <= acked +
    unknown``."""
    rng = random.Random(seed * 104729 + n)
    issued_on_proposals = [0]       # across incarnations: a revived member's counters restart
    successors = set()              # (term, seq, holder) of every proposal a permit was issued on
    dispatch = ClusterCoordinator._dispatch

    def counting(self, own):
        before = self._stats["pipelined_permits"]
        fresh = [e for e in own.pipeline if e.proposal is not None and not e.consumed]
        sends = dispatch(self, own)
        issued_on_proposals[0] += self._stats["pipelined_permits"] - before
        successors.update((own.term, e.seq, e.node) for e in fresh if e.consumed)
        return sends
    monkeypatch.setattr(ClusterCoordinator, "_dispatch", counting)
    aimed, landed, hits = [], [], []
    with _cluster(tmp_path, n) as tc:
        net = tc.cluster.net
        net.set_faults(duplicate=rng.choice([0.0, 0.3, 1.0]), delay=(0.0, rng.choice([0.0, 0.004, 0.02])))
        with Load(tc) as load:
            # At least one predecessor-window fault per schedule, then a random mix.
            kinds = [rng.choice(WINDOW_FAULTS)] + [rng.choice(("kill", "partition") + AIMED_FAULTS)
                                                   for _ in range(rng.randint(0, 2))]
            for how in kinds:
                time.sleep(rng.uniform(0.1, SECONDS / 3))
                if how in AIMED_FAULTS:
                    aimed.append(how)
                    hit, fired, disarm = _arm(tc, load, how)
                    # The hot counter conflicts often, and under 20 ms jitter at 5 members few
                    # rounds propose: a window fault may need tens of seconds to find one.
                    fired.wait(60.0 if how in WINDOW_FAULTS else 20 * SECONDS)
                    if how != "late_acks":
                        disarm()
                    if not fired.is_set():
                        disarm()
                        continue
                    landed.append(how)
                    victim = hit[0]
                    if len(hit) > 1:
                        hits.append((how, hit[1][0], hit[1][1], victim))
                    if how == "late_acks":
                        time.sleep(LEASE["lease_duration"] * 1.5)      # the delayed acks arrive
                        disarm()
                        continue
                    tc.kill(victim)
                else:
                    victim = rng.choice([m for m in tc.ids if m not in load.down])
                    if how == "kill":
                        load.down.add(victim)
                        tc.kill(victim)
                    else:
                        net.partition({victim}, set(tc.ids) - {victim})
                time.sleep(rng.uniform(0.05, LEASE["lease_duration"] * 1.5))
                if how == "partition":
                    net.heal()
                else:
                    tc.revive(victim)
                    load.down.discard(victim)
            time.sleep(SECONDS / 3)
        pipelined = issued_on_proposals[0]
        # N3: a landed window fault whose victim's proposal had a successor issued on it -- a
        # pipelined round in flight across the fault, not only a proposer hit.
        overlapped = [how for how, term, seq, member in hits if (term, seq, member) in successors]
        window_tally["schedules"] += 1
        window_tally["overlapped"] += len(overlapped)
        print(f"\n{n} members, seed {seed}: {load.acked} acked, {load.unknown} unknown, {pipelined} pipelined "
              f"permits, aimed {aimed}, landed {landed}, with a successor {overlapped}")
        _check(tc, load, tc.ids)
        assert pipelined > 0
        required = [how for how in aimed if how in WINDOW_FAULTS]
        assert [how for how in landed if how in WINDOW_FAULTS] == required, \
            f"window faults that never fired: aimed {aimed}, landed {landed}"
