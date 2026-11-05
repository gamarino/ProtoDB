"""The owner's sequencing of pipelined permissions (Task 8.3k, D55 rules O1-O8, depth 1).

The owner keeps a pipeline of issued permissions.  It issues the next one on the newest
entry's proposal once that entry proposed (O1, O2: once per proposal), retires an entry and
every older one on a committed ``done`` (O3), validates each ``propose`` (O4: unknown sender
ignored, duplicate ignored, a changed root, a proposal from an entry that is no longer the
newest, or one not built on the issued base abandons the term), restores the committed base
only after a ``none`` of a lone entry that did not propose (O5), abandons the term on any
other outcome it cannot know (O6), and answers a retried request with the same permit (O7).

The ``_Ownership`` is driven directly on a coordinator that is never started: these rules
read only the owner's own state (R79 puts them on the trusted side, so they get exhaustive
tests, including a Hypothesis-generated schedule over two entries).
"""
import itertools

import pytest
from hypothesis import given, settings, strategies as st

from proto_db.cluster.coordinator import DEPTH, ClusterCoordinator, _Ownership
from proto_db.cluster.lease import Lease
from proto_db.cluster.protocol import decode_labeled_root, encode_root
from proto_db.cluster.transport import InProcessNetwork
from tests.support.manual_cluster import rec

T = 7 << 12
PEERS = [(n, "127.0.0.1", 0) for n in ("a", "b", "c")]


class _Clock:
    t = 100.0

    def __call__(self):
        return self.t


@pytest.fixture
def owner():
    net = InProcessNetwork()
    clock = _Clock()
    node = ClusterCoordinator("a", PEERS, transport=net.endpoint(), lease_duration=10.0, request_timeout=0.25,
                              clock=clock)
    node._state = "running"             # the rules below never send nor wait
    lease = Lease(term=T, acquired_at=clock(), valid_until=clock() + 5.0, root=rec(1), root_certain=True,
                  clock=clock, root_term=T - (1 << 12))
    own = _Ownership(lease, (True, lease.root, lease.root_term), True, 0.0)
    node._ownership = own
    yield node, own, clock
    net.close()


def _ask(node, own, member, rid):
    with node._cond:
        own.queue.append((member, rid, 1000 + ord(member), None))
        return node._dispatch(own)


def _propose(node, member, seq, root, pred, inc=None):
    msg = {"term": T, "seq": seq, "inc": 1000 + ord(member) if inc is None else inc,
           "root": encode_root(root, known=True, term=T), "pred": encode_root(pred[1], known=True, term=pred[2])}
    node._on_propose(member, msg)


def _done(node, own, member, seq, outcome, root=None, inc=None):
    with node._cond:
        issued = own.entry(member, seq)
        if issued is None:
            return None                 # retired already: _on_done_owner ignores it
        if inc is not None and issued is not None and inc != issued.inc:
            outcome = "unknown"         # as _on_done_owner does
        return node._permit_outcome(own, issued, outcome, root, T)


def _permit(sends):
    [(target, msg)] = sends
    return target, msg


def test_o1_the_next_permit_waits_for_the_newest_entrys_proposal(owner):
    node, own, _ = owner
    target, p1 = _permit(_ask(node, own, "b", 1))
    assert target == "b" and p1["after"] is None and p1["certain"] is True
    assert _ask(node, own, "c", 2) == []                    # b has not proposed
    k = rec(2)
    _propose(node, "b", 1, k, (True, own.base[1], own.base[2]))
    assert len(own.pipeline) == 2
    target, p2 = own.pipeline[1].node, own.pipeline[1].permit
    assert target == "c" and p2["after"] == [1, "b"] and p2["certain"] is False      # H2
    assert decode_labeled_root(p2["root"])[1:] == (k, T)
    assert node.stats()["pipelined_permits"] == 1


def test_o1_depth_one_issues_no_third_permit_before_the_oldest_is_done(owner):
    node, own, _ = owner
    _ask(node, own, "b", 1)
    k = rec(2)
    _propose(node, "b", 1, k, (True, own.base[1], own.base[2]))
    _ask(node, own, "c", 2)
    k1 = rec(3)
    _propose(node, "c", 2, k1, (True, k, T))
    assert DEPTH == 1
    assert _ask(node, own, "b", 3) == [] and len(own.pipeline) == 2     # b's first one not done yet
    sends = _done(node, own, "b", 1, "committed", k)
    assert [s[0] for s in sends] == ["b"] and sends[0][1]["after"] == [2, "c"]
    assert [e.seq for e in own.pipeline] == [2, 3]


def test_o2_a_proposal_is_issued_on_once(owner):
    node, own, _ = owner
    _ask(node, own, "b", 1)
    k = rec(2)
    _propose(node, "b", 1, k, (True, own.base[1], own.base[2]))
    _ask(node, own, "c", 2)
    _propose(node, "b", 1, k, (True, own.base[1], own.base[2]))         # a duplicate (O4.2)
    assert len(own.pipeline) == 2 and own.pipeline[0].consumed
    assert not own.abandoned


def test_o3_a_successors_commit_retires_its_predecessor_and_moves_the_base(owner):
    node, own, _ = owner
    _ask(node, own, "b", 1)
    k = rec(2)
    _propose(node, "b", 1, k, (True, own.base[1], own.base[2]))
    _ask(node, own, "c", 2)
    k1 = rec(3)
    _propose(node, "c", 2, k1, (True, k, T))
    _done(node, own, "c", 2, "committed", k1)              # done(k+1) overtook done(k)
    assert own.pipeline == [] and own.base == (True, k1, T) and own.committed
    assert _done(node, own, "b", 1, "committed", k) is None or not own.abandoned   # stale: entry gone
    assert own.base == (True, k1, T)
    target, p = _permit(_ask(node, own, "b", 3))
    assert p["after"] is None and decode_labeled_root(p["root"])[1] == k1 and p["certain"]


def test_o3_a_proposing_entry_that_commits_another_root_abandons(owner):
    node, own, _ = owner
    _ask(node, own, "b", 1)
    _propose(node, "b", 1, rec(2), (True, own.base[1], own.base[2]))
    _done(node, own, "b", 1, "committed", rec(5))
    assert own.abandoned


@pytest.mark.parametrize("case", ["changed root, newest", "changed root, not newest", "not newest, no proposal",
                                  "pred is not the issued base"])
def test_o4_precedence_abandons(owner, case):
    node, own, _ = owner
    base = (True, own.base[1], own.base[2])
    _ask(node, own, "b", 1)
    k = rec(2)
    if case == "changed root, newest":
        _propose(node, "b", 1, k, base)
        _propose(node, "b", 1, rec(9), base)
    elif case == "changed root, not newest":
        _propose(node, "b", 1, k, base)
        _ask(node, own, "c", 2)
        _propose(node, "b", 1, rec(9), base)                # M5: abandons, not ignored
    elif case == "not newest, no proposal":
        # Correct code cannot produce this: plant a second entry behind a silent first one.
        own.pipeline[0].proposal = (k, T)
        _ask(node, own, "c", 2)
        own.pipeline[0].proposal = None
        _propose(node, "b", 1, k, base)
    else:
        _propose(node, "b", 1, k, (True, rec(8), T))
    assert own.abandoned and own.pipeline == []


def test_o4_a_proposal_from_an_unknown_sender_or_incarnation_is_ignored(owner):
    node, own, _ = owner
    _ask(node, own, "b", 1)
    base = (True, own.base[1], own.base[2])
    _propose(node, "c", 1, rec(2), base)                     # no such entry
    _propose(node, "b", 1, rec(2), base, inc=5)              # another incarnation
    _propose(node, "b", 7, rec(2), base)                     # another seq
    assert node.stats()["foreign_proposals"] == 3
    assert own.pipeline[0].proposal is None and not own.abandoned


def test_o5_a_lone_none_restores_the_committed_base(owner):
    node, own, _ = owner
    first = own.base
    _ask(node, own, "b", 1)
    sends = _done(node, own, "b", 1, "none")
    assert own.pipeline == [] and not own.abandoned and own.base == first
    assert sends == []
    target, p = _permit(_ask(node, own, "c", 2))
    assert p["after"] is None and decode_labeled_root(p["root"])[1] == first[1]


def test_o5_a_lone_none_after_a_proposal_base_restores_the_predecessors_committed_root(owner):
    node, own, _ = owner
    _ask(node, own, "b", 1)
    k = rec(2)
    _propose(node, "b", 1, k, (True, own.base[1], own.base[2]))
    _ask(node, own, "c", 2)
    _done(node, own, "b", 1, "committed", k)
    _done(node, own, "c", 2, "none")                         # c's gate closed, or its base superseded
    assert not own.abandoned and own.base == (True, k, T)
    target, p = _permit(_ask(node, own, "b", 3))
    assert decode_labeled_root(p["root"])[1] == k and p["after"] is None


@pytest.mark.parametrize("case", ["none after proposing", "none with two outstanding"])
def test_o5_every_other_none_abandons(owner, case):
    node, own, _ = owner
    _ask(node, own, "b", 1)
    _propose(node, "b", 1, rec(2), (True, own.base[1], own.base[2]))
    if case == "none after proposing":
        _done(node, own, "b", 1, "none")
    else:
        _ask(node, own, "c", 2)
        _done(node, own, "c", 2, "none")
    assert own.abandoned


def test_o6_unknown_and_foreign_incarnation_done_abandon(owner):
    node, own, _ = owner
    _ask(node, own, "b", 1)
    _done(node, own, "b", 1, "committed", rec(2), inc=99)
    assert own.abandoned


def test_o6_each_entry_has_its_deadline_and_a_lease_end_with_two_entries_abandons(owner):
    node, own, clock = owner
    _ask(node, own, "b", 1)
    _propose(node, "b", 1, rec(2), (True, own.base[1], own.base[2]))
    _ask(node, own, "c", 2)
    assert own.pipeline[0].deadline <= own.pipeline[1].deadline
    clock.t += 5.0 + 0.001                     # the lease ends; both deadlines lie beyond it
    assert not own.lease.is_valid() and len(own.pipeline) == 2
    # The ownership loop's rule (exercised end to end in test_cluster_permit_incarnations).
    with node._cond:
        if not own.lease.is_valid() and own.pipeline:
            node._abandon(own, "lease expired")
    assert own.abandoned and own.pipeline == []


def test_o7_a_retried_request_gets_its_entrys_permit_again(owner):
    node, own, _ = owner
    _, p1 = _permit(_ask(node, own, "b", 1))
    _propose(node, "b", 1, rec(2), (True, own.base[1], own.base[2]))
    _, p2 = _permit(_ask(node, own, "c", 2))
    sent = []
    node._send_all = lambda sends: sent.extend(sends)
    own.lease.root_certain = True
    for member, rid, expected in (("b", 1, p1), ("c", 2, p2)):
        node._on_permit_req(member, {"term": T, "rid": rid, "inc": 1000 + ord(member)})
        assert sent[-1] == (member, expected)
    assert node.stats()["permits_issued"] == 2


# -- Hypothesis: no two permits share a base, except after a lone none without a proposal --

_EVENTS = st.lists(st.sampled_from(["ask", "propose_new", "propose_old", "propose_dup", "propose_other",
                                    "done_c_new", "done_c_old", "done_n_new", "done_n_old", "done_u"]),
                   min_size=1, max_size=40)


@settings(max_examples=300, deadline=None)
@given(_EVENTS)
def test_owner_schedules_never_issue_one_base_twice_outside_o5(events):
    net = InProcessNetwork()
    clock = _Clock()
    node = ClusterCoordinator("a", PEERS, transport=net.endpoint(), lease_duration=10.0, request_timeout=0.25,
                              clock=clock)
    node._state = "running"
    lease = Lease(term=T, acquired_at=clock(), valid_until=clock() + 5.0, root=rec(1), root_certain=True,
                  clock=clock, root_term=T - (1 << 12))
    own = _Ownership(lease, (True, lease.root, lease.root_term), True, 0.0)
    node._ownership = own
    issued_bases = []           # (base, reason it may repeat)
    roots = {}
    members = itertools.cycle("bc")
    rids = itertools.count(1)
    try:
        for event in events:
            if own.abandoned:
                break
            before = len(own.pipeline)
            restore_ok = False
            if event == "ask":
                _ask(node, own, next(members), next(rids))
            elif event.startswith("propose") and own.pipeline:
                entry = own.pipeline[-1] if event in ("propose_new", "propose_dup", "propose_other") \
                    else own.pipeline[0]
                if event == "propose_dup" and entry.proposal is not None:
                    root = entry.proposal[0]
                elif event == "propose_other":
                    root = rec(entry.seq * 100 + 50)
                else:
                    root = roots.setdefault(entry.seq, rec(entry.seq * 100 + 10))
                _propose(node, entry.node, entry.seq, root, entry.base)
            elif event.startswith("done") and own.pipeline:
                entry = own.pipeline[-1] if event.endswith("new") else own.pipeline[0]
                if event.startswith("done_c"):
                    root = entry.proposal[0] if entry.proposal else roots.setdefault(entry.seq, rec(entry.seq * 100 + 10))
                    _done(node, own, entry.node, entry.seq, "committed", root)
                elif event.startswith("done_n"):
                    restore_ok = own.pipeline == [entry] and entry.proposal is None
                    _done(node, own, entry.node, entry.seq, "none")
                else:
                    _done(node, own, entry.node, entry.seq, "unknown")
            for entry in own.pipeline[before:] if len(own.pipeline) > before else ():
                issued_bases.append(entry.base)
            if restore_ok:
                issued_bases.append("O5")
            assert len(own.pipeline) <= DEPTH + 1
            assert sum(1 for e in own.pipeline if e.proposal is None) <= 1
            assert all(e.proposal is not None for e in own.pipeline[:-1])
        # Two permits on one base only across an O5 restore (generation bumps at each one).
        seen, generation = set(), 0
        for item in issued_bases:
            if item == "O5":
                generation += 1
                continue
            assert (generation, item) not in seen, f"base {item} issued twice without an O5 restore"
            seen.add((generation, item))
    finally:
        net.close()
