"""``permit()``'s back-off between lost lease attempts (Task 8.3n fix round 3, re-review 1,
finding N7).

Measured under the random-faults schedules (fix round 3 report): while no majority of voters
could grant (seed 3, 5 members: one voter partitioned, another killed), every member made
230-1,124 lost attempts in one 1-2.7 s window -- an attempt every 2-4 ms -- because every
release ended every waiter's back-off at once, and a lost attempt broadcasts a release (its
cancel).  ``_acquire`` already honours the refusing granters' ``busy_remaining`` and spreads
woken waiters; ``permit()`` did neither.

Now a lost attempt waits its jittered exponential back-off, extended to the granters'
``busy_remaining`` (``_back_off``, shared with ``_acquire`` since re-review 2, M2).  A cancel (a lost attempt's release) can end only that
extension, never the back-off itself; a winner's release, a ``done`` or a newly learnt owner
ends the whole wait (they are progress: a granter is free, or an owner can serve).  A woken
waiter that was not named for the turn adds a small random spread.

Real coordinators over a :class:`~tests.support.manual_cluster.ManualNet` with one fake clock.
"""
import threading
import time

import pytest

from proto_db.cluster.protocol import make_message
from proto_db.errors import ProtoTimeoutError
from tests.support.manual_cluster import Harness

WINDOW = 0.3            # seconds of cancels delivered to the refused requester


def _cancel(node):
    """A lost attempt's release from ``node`` (``held=False``)."""
    return make_message("release", node.node_id, term=node._grants.highest_term + 1, inc=node._inc,
                        root=node._wire(node._accepted), committed=node._wire(node._committed),
                        certain=False, held=False, next=None, more=True)


@pytest.mark.parametrize("how", ["permit", "acquire"])
def test_cancels_do_not_end_the_back_off_of_a_refused_permit_request(how):
    """``how``: ``permit()``, or a direct ``acquire()`` (re-review 2, M2: ``_acquire`` let any
    release end its whole back-off; both now share ``_back_off``)."""
    h = Harness("abc")
    try:
        a, b, c = h.nodes["a"], h.nodes["b"], h.nodes["c"]
        lease = h.attempt("a", {"b", "c"})          # a holds the lease: b and c are busy for it
        assert not isinstance(lease, Exception), lease
        out = {}

        def ask():
            try:
                out["p"] = getattr(b, how)(timeout=WINDOW + 0.3)
            except ProtoTimeoutError as e:
                out["e"] = e
        asker = threading.Thread(target=ask)
        before = b.stats()["attempts"]
        asker.start()
        end = time.monotonic() + WINDOW
        while time.monotonic() < end:
            h.net.deliver(src="b", dst={"a", "c"}, kinds=("request",))
            h.net.drop(src="b", kinds=("release",))
            h.net.deliver(src=None, dst={"b"}, kinds=("grant", "reject"))
            h.net.handlers["b"](_cancel(c))          # another member's lost attempt, every 2 ms
            time.sleep(0.002)
        attempts = b.stats()["attempts"] - before
        asker.join(5)
        h.net.drop(kinds=("request", "grant", "reject", "release"))
        assert "e" in out, out                        # a holds the lease throughout: no permission
        # Back-off 1, 2, 4, 8, 16, 32, 50, 50... ms (x 0.5-1 jitter) plus each attempt's own
        # round: at most about a dozen attempts in 0.3 s.  Before the fix every cancel ended the
        # wait: one attempt per delivered cancel.
        assert attempts <= 20, f"{attempts} lost attempts in {WINDOW} s: cancels ended the back-off"
    finally:
        h.close()


def test_a_winners_release_ends_the_back_off_at_once():
    """Progress ends the wait: a's release of its lease frees b and c, and b's next attempt
    follows within milliseconds, not after its granters' ``busy_remaining`` (a whole lease on
    the fake clock)."""
    h = Harness("abc")
    try:
        a, b = h.nodes["a"], h.nodes["b"]
        lease = h.attempt("a", {"b", "c"})
        out = {}

        def ask():
            try:
                out["p"] = b.permit(timeout=5)
            except Exception as e:                  # reported by the assertion below
                out["e"] = e
        asker = threading.Thread(target=ask)
        asker.start()
        deadline = time.monotonic() + 2
        while b.stats()["lost_attempts"] < 1:         # refused: busy for a's lease
            assert time.monotonic() < deadline, b.stats()
            h.net.deliver(src="b", dst={"a", "c"}, kinds=("request",))
            h.net.drop(src="b", kinds=("release",))
            h.net.deliver(src=None, dst={"b"}, kinds=("grant", "reject"))
            time.sleep(0.001)
        h.release("a", lease, to={"b", "c"})          # a winner's release: progress
        released = time.monotonic()
        # Its granters refused b for a whole lease on the fake clock (``busy_remaining``, 10 s
        # here): without the release b would wait that long.
        while asker.is_alive():
            h.net.deliver(src="b", dst={"a", "c"}, kinds=("request", "release"))
            h.net.deliver(src=None, dst={"b"}, kinds=("grant", "reject"))
            time.sleep(0.001)
            assert time.monotonic() - released < 2, "b kept waiting after the lease was released"
        assert "p" in out, out
        h.nodes["b"].finish(out["p"])
    finally:
        h.close()
