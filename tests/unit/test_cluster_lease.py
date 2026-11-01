"""Lease state machine: grant rules, term monotonicity, majority, validity window, fencing (Task 8.3c, D45)."""
import uuid

import pytest

from proto_db.cluster.lease import FencingToken, Lease, LeaseAttempt, PeerGrants, majority
from proto_db.storage.format import FORMAT_VERSION, RootRecord


class FakeClock:
    def __init__(self, t=100.0):
        self.t = t

    def __call__(self):
        return self.t


def _grants(clock, duration=1.0, grace=0.0):
    return PeerGrants(duration, clock, startup_grace=grace)


# -- majority ---------------------------------------------------------------------------------

@pytest.mark.parametrize("n,m", [(1, 1), (2, 2), (3, 2), (4, 3), (5, 3), (6, 4), (7, 4)])
def test_majority_is_floor_half_plus_one(n, m):
    assert majority(n) == m


# -- PeerGrants: grant rules ------------------------------------------------------------------

def test_first_request_is_granted():
    g = _grants(FakeClock())
    d = g.on_request("a", 1)
    assert d.granted and g.highest_term == 1 and g.holder == "a"


def test_at_most_one_holder_while_the_lease_runs():
    clock = FakeClock()
    g = _grants(clock)
    assert g.on_request("a", 1).granted
    clock.t += 0.4
    d = g.on_request("b", 2)
    assert not d.granted and d.reason == "busy"
    assert d.remaining == pytest.approx(0.6)


def test_grant_after_expiry_on_the_peer_clock_and_reports_the_missed_release():
    clock = FakeClock()
    g = _grants(clock)
    g.on_request("a", 1)
    clock.t += 1.0               # exactly at expiry: the promise is over
    d = g.on_request("b", 2)
    assert d.granted and d.previous_expired
    assert g.holder == "b"


def test_a_released_grant_frees_the_peer_without_marking_an_expiry():
    g = _grants(FakeClock())
    g.on_request("a", 1)
    g.on_release("a", 1)
    d = g.on_request("b", 2)
    assert d.granted and not d.previous_expired


def test_release_of_another_holder_or_term_does_not_free_the_grant():
    g = _grants(FakeClock())
    g.on_request("a", 3)
    g.on_release("b", 3)
    g.on_release("a", 2)
    assert not g.on_request("b", 4).granted


def test_lower_terms_are_rejected():
    g = _grants(FakeClock())
    g.on_request("a", 5)
    g.on_release("a", 5)
    d = g.on_request("b", 4)
    assert not d.granted and d.reason == "stale_term" and d.highest == 5


def test_a_term_is_granted_to_one_holder_only_even_after_release():
    g = _grants(FakeClock())
    g.on_request("a", 5)
    g.on_release("a", 5)
    d = g.on_request("b", 5)
    assert not d.granted and d.reason == "term_taken"


def test_a_term_seen_in_a_rejected_request_can_still_be_granted_to_another_requester():
    clock = FakeClock()
    g = _grants(clock)
    g.on_request("a", 1)
    assert not g.on_request("b", 2).granted      # busy; term 2 seen, not granted
    g.on_release("a", 1)
    assert g.on_request("c", 2).granted          # nobody holds term 2 at this peer
    assert not g.on_request("b", 2).granted      # but only one holder per term


def test_a_duplicate_request_is_idempotent_and_never_extends_the_promise():
    clock = FakeClock()
    g = _grants(clock)
    g.on_request("a", 1)
    clock.t += 0.5
    d = g.on_request("a", 1)
    assert d.granted
    clock.t += 0.5               # the original expiry, not 0.5 later
    assert g.on_request("b", 2).granted


def test_a_late_duplicate_request_after_release_is_rejected():
    g = _grants(FakeClock())
    g.on_request("a", 1)
    g.on_release("a", 1)
    assert not g.on_request("a", 1).granted


def test_a_release_overtaking_its_request_closes_the_term():
    g = _grants(FakeClock())
    g.on_release("a", 7)          # reordered: the cancel arrives first
    d = g.on_request("a", 7)
    assert not d.granted and d.reason == "term_taken"
    assert g.highest_term == 7


def test_a_holder_may_renew_with_a_higher_term():
    g = _grants(FakeClock())
    g.on_request("a", 1)
    assert g.on_request("a", 2).granted


def test_startup_grace_refuses_every_grant_for_its_duration():
    clock = FakeClock()
    g = PeerGrants(1.0, clock, startup_grace=1.0)
    d = g.on_request("a", 1)
    assert not d.granted and d.reason == "starting" and d.remaining == pytest.approx(1.0)
    clock.t += 1.0
    assert g.on_request("a", 2).granted


# -- LeaseAttempt -----------------------------------------------------------------------------

def _rec(epoch):
    return RootRecord(FORMAT_VERSION, uuid.UUID(int=epoch), 64, epoch)


def _attempt(n=3, sent=10.0, duration=1.0, margin=0.1):
    return LeaseAttempt(term=4, rid=1, sent_at=sent, cluster_size=n, lease_duration=duration,
                        drift_margin=margin)


def test_attempt_is_won_at_a_majority_and_counts_a_peer_once():
    a = _attempt(n=4)
    a.on_grant("x", True, _rec(2), True)
    a.on_grant("x", True, _rec(2), True)          # duplicate
    a.on_grant("y", True, _rec(2), True)
    assert a.state == "pending"
    a.on_grant("z", True, _rec(2), True)
    assert a.state == "won"


def test_attempt_is_lost_once_a_majority_is_impossible():
    a = _attempt(n=5)
    a.on_reject("p", "busy", 4, 0.3)
    a.on_reject("q", "busy", 6, 0.2)
    assert a.state == "pending"
    a.on_reject("r", "stale_term", 9, 0.0)
    assert a.state == "lost"
    assert a.highest_seen == 9
    assert a.busy_remaining == pytest.approx(0.3)


def test_a_grant_after_a_reject_from_the_same_peer_is_ignored():
    a = _attempt(n=3)
    a.on_reject("p", "busy", 4, 0.3)
    a.on_grant("p", True, _rec(1), True)
    assert "p" not in a.granted_by


def test_valid_until_is_measured_from_the_send_time():
    a = _attempt(sent=10.0, duration=1.0, margin=0.1)
    assert a.valid_until == pytest.approx(10.9)


def test_best_root_is_the_highest_label_then_epoch_and_certain_unless_a_granter_is_unsure():
    a = _attempt(n=3)
    a.on_grant("x", True, _rec(4), True, 2)
    a.on_grant("y", True, _rec(4), True, 2)
    assert a.best_root() == (True, _rec(4), True, 2)
    b = _attempt(n=3)
    b.on_grant("x", True, _rec(4), True, 2)
    b.on_grant("y", True, _rec(5), True, 2)            # y missed nothing: x lags behind, still certain
    assert b.best_root() == (True, _rec(5), True, 2)
    f = _attempt(n=3)
    f.on_grant("x", True, _rec(6), True, 1)            # epoch 6 from an older term ...
    f.on_grant("y", True, _rec(5), True, 3)            # ... loses to epoch 5 of a newer term (D46)
    assert f.best_root() == (True, _rec(5), True, 3)
    c = _attempt(n=3)
    c.on_grant("x", True, _rec(4), True)
    c.on_grant("y", True, _rec(4), False)           # one granter may have missed a root
    assert c.best_root() == (True, _rec(4), False, 0)
    d = _attempt(n=3)
    d.on_grant("x", False, None, True)              # root unknown at x
    d.on_grant("y", True, _rec(4), True)
    assert d.best_root() == (True, _rec(4), False, 0)
    e = _attempt(n=3)
    e.on_grant("x", True, None, True)               # known empty space (epoch 0)
    e.on_grant("y", True, None, True)
    assert e.best_root() == (True, None, True, 0)
    g = _attempt(n=3)
    g.on_grant("x", True, _rec(4), True)
    g.on_grant("y", True, _rec(4), True)
    assert g.best_root(own=(True, _rec(7), 0)) == (True, _rec(7), True, 0)   # the requester's own root counts


def test_a_request_from_a_new_incarnation_ends_the_old_grant():
    clock = FakeClock(0.0)
    g = PeerGrants(1.0, clock, startup_grace=0.0)
    assert g.on_request("a", 1, 100).granted
    d = g.on_request("a", 1, 200)                    # "a" restarted and reuses term 1
    assert not d.granted and d.reason == "term_taken"
    assert d.previous_expired and d.ended_term == 1
    assert g.holder is None
    g.on_release("a", 1, 200)                        # the new life's cancel
    assert g.on_request("b", 2).granted
    assert g.granted_top == 2


def test_granted_top_ignores_terms_closed_by_releases():
    g = PeerGrants(1.0, FakeClock(0.0), startup_grace=0.0)
    g.on_request("a", 1)
    g.on_release("b", 9)                             # a cancel of term 9 closes it here
    assert g.granted_top == 1 and g.highest_term == 9


# -- Lease and fencing tokens -----------------------------------------------------------------

def test_lease_is_valid_until_its_deadline_or_release():
    clock = FakeClock(10.0)
    lease = Lease(term=3, acquired_at=10.0, valid_until=10.5, root=_rec(2), root_certain=True, clock=clock)
    assert lease.is_valid() and lease.remaining() == pytest.approx(0.5)
    clock.t = 10.5
    assert not lease.is_valid()
    clock.t = 10.1
    lease._mark_released()
    assert not lease.is_valid()


def test_fencing_token_combines_term_and_the_base_epoch():
    lease = Lease(term=3, acquired_at=0, valid_until=1, root=_rec(2), root_certain=True, clock=FakeClock(0))
    assert lease.fencing_token == FencingToken(3, 2)
    lease.bind_root(_rec(7))
    assert lease.fencing_token == FencingToken(3, 7) and lease.expected_epoch == 7
    empty = Lease(term=1, acquired_at=0, valid_until=1, root=None, root_certain=True, clock=FakeClock(0))
    assert empty.expected_epoch == 0


def test_fencing_tokens_order_by_term_first():
    assert FencingToken(3, 9) < FencingToken(4, 0) < FencingToken(4, 1)
