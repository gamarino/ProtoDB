"""Root certainty under D46 when an acknowledger loses its disk (review R39 item 2, 8.3d
review M1, re-review-2 X1).

Every root here is committed through the real replicate path (majority acknowledgement), and
every node keeps an acceptor log.  The granting majority of any lease intersects the
acknowledging majority of every committed root, so the newest committed root is reported by
some granter -- unless that granter lost its state (a wiped disk: within the failure model
when it is a minority).  Since ruling R49 a wiped node rejoins by learning the highest
promise AND the highest accepted root from ``N - majority + 1`` witnesses, which intersect
every acknowledging majority: it reports the committed root again, so the holder builds on
it.  Each test asserts that the lease's root (when certain) or the recovered root (when not)
is the newest committed root.  The certainty check and recovery stay as defence in depth.
"""
import pytest

from tests.support.manual_cluster import GRACE, Harness, rec

pytestmark = [pytest.mark.timeout(30)]


@pytest.fixture
def harness():
    made = []

    def make(names="abc"):
        h = Harness(names)
        made.append(h)
        return h

    yield make
    for h in made:
        h.close()


def _check(h, holder, lease, truth, responders):
    assert lease is not None and not isinstance(lease, Exception), lease
    root, _ = h.base(holder, lease, responders)
    assert root == truth, f"the commit would build on epoch {root.epoch if root else 0}, not {truth.epoch}"


def test_a_wiped_acknowledger_granting_another_holder_reports_the_committed_root(harness):
    h = harness()
    h.bootstrap("a")                                       # r1 everywhere
    lease_b = h.attempt("b", {"a", "c"})
    truth = rec(2)
    h.replicate("b", lease_b, truth, to={"a"})             # committed by b and a; c keeps r1
    h.release("b", lease_b, to=())                         # the release is lost
    h.restart("a", wipe=True)                              # a, an acknowledger, loses its disk
    h.clock.t += GRACE                                         # grace over, b's grants expired
    lease_c = h.attempt("c", {"a"})                        # granters: a (rejoined), c (r1)
    assert lease_c.root == truth                           # a learnt it from the witnesses
    _check(h, "c", lease_c, truth, responders={"a", "b"})


def test_a_wiped_acknowledger_requesting_the_lease_builds_on_the_committed_root(harness):
    h = harness()
    h.bootstrap("a")
    lease_b = h.attempt("b", {"a", "c"})
    truth = rec(2)
    h.replicate("b", lease_b, truth, to={"a"})
    h.release("b", lease_b, to=())
    h.restart("a", wipe=True)                              # the next holder itself lost its disk
    h.clock.t += GRACE
    lease_a = h.attempt("a", {"c"})                        # granters: a (rejoined), c (r1)
    assert lease_a.root == truth
    _check(h, "a", lease_a, truth, responders={"b", "c"})


def test_two_wiped_acknowledgers_of_five_report_the_committed_root(harness):
    h = harness("abcde")
    h.bootstrap("a")
    lease_a = h.attempt("a", {"b", "c", "d", "e"})
    truth = rec(2)
    h.replicate("a", lease_a, truth, to={"b", "c"})        # committed by a, b, c (3 of 5)
    h.release("a", lease_a, to=())
    h.restart("b", wipe=True)                              # a minority (2 of 5) loses its disks
    h.restart("c", wipe=True)
    h.clock.t += GRACE
    lease_d = h.attempt("d", {"b", "c"})                   # granters: b, c (rejoined), d (r1)
    assert lease_d.root == truth
    _check(h, "d", lease_d, truth, responders={"a", "e"})
