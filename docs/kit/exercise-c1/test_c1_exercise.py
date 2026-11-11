"""The C1 exercise: an acknowledged commit must never be lost.

Five voters, a to e, run the real cluster coordinator.  Nothing here is simulated except the
network and the clock: :class:`tests.support.manual_cluster.Harness` holds every message until
the test delivers or drops it, and every node reads one fake clock that the test moves by hand.

The schedule, step by step:

1. a commits r1.  Time passes.
2. e becomes the holder of a term T' (its lease is granted by b and c).  e's own timer then stands
   still: its machine was suspended, or its clock runs slow.  The failure model allows this
   (a timer-rate fault); no safety rule may depend on timers.
3. e replicates a new root R'.  The message reaches b; the copy for c is delayed in the network.
4. Time passes on everybody else's clock, so the promises b and c made to e expire for them.
   a becomes the holder of a newer term T (its lease is granted by b and d).
5. a starts a commit.  Its lease is not certain of the newest root, so it runs a recovery round
   (``recover_root``), answered by c and d.
6. e's delayed message reaches c; c accepts R'.  e counts b, c and itself: a majority of five,
   inside what its frozen timer still shows as a valid lease.  **R' is acknowledged.**
7. a commits a new root on the base it chose in step 5, acknowledged by a, c and d.

The invariant: a committed root extends every acknowledged root.  The test asserts it on the
root a commits in step 7: what a replicated it as built on (its ``pred``) must be R'.

The test judges safety only.  A fix may also change the schedule: c may refuse R' in step 6, so
R' is never acknowledged, or a may find no base or fail to commit in step 7.  Nothing acknowledged
is lost then, so the test passes, and it reports what did not complete ("not completed (safe)")
as a warning in pytest's summary.  Whether your fix still makes progress is a separate question;
the README says how to check it.

Run it from the root of the exercise copy::

    python -m pytest exercise_c1/test_c1_exercise.py
"""
import threading
import time
import warnings

import pytest

from proto_db.cluster.protocol import decode_labeled_root
from proto_db.transactions import _lease_base
from tests.support.manual_cluster import D, MARGIN, Harness, rec

pytestmark = [pytest.mark.timeout(120)]


def _call(fn, *args):
    try:
        return fn(*args)
    except Exception as ex:     # the outcome under test, reported by the assertions
        return ex


class NotCompleted(UserWarning):
    """A step of the schedule did not complete under the code tested: a liveness report, not a
    safety failure."""


def _not_completed(what: str) -> None:
    warnings.warn(NotCompleted(f"not completed (safe): {what}"), stacklevel=2)


def test_an_acknowledged_root_is_never_lost():
    h = Harness("abcde")
    try:
        a, b, c = h.nodes["a"], h.nodes["b"], h.nodes["c"]

        # Step 1: a commits r1; time passes.
        r1 = h.bootstrap("a")
        h.clock.t += 2 * (D + MARGIN)

        # Step 2: e holds T', granted by b and c; e's timer stands still from now on.
        lease_e = h.attempt("e", {"b", "c"})
        assert not isinstance(lease_e, Exception), lease_e
        frozen = h.clock.t
        lease_e._clock = lambda: frozen

        # Step 3: e replicates R'; b accepts it, the copy for c is held back.
        r_prime = rec(r1.epoch + 1)
        out = {}
        e_commit = threading.Thread(target=lambda: out.setdefault("r", _call(h.nodes["e"].replicate, lease_e, r_prime)))
        e_commit.start()
        deadline = time.monotonic() + 10
        while b.accepted_root()[1] != r_prime:
            h.net.deliver(src="e", dst={"b"}, kinds=("records", "replicate"))
            assert time.monotonic() < deadline, "b never accepted R'"
            time.sleep(0.001)
        held_for_c = h.net.take("e", {"c"}, ("records", "replicate"))
        h.net.drop(src="e", dst={"a", "d"}, kinds=("records", "replicate"))
        assert held_for_c, "e sent nothing to c"

        # Step 4: the promises to e expire on the other clocks; a holds T, granted by b and d.
        h.clock.t += D + MARGIN + 1
        lease_a = h.attempt("a", {"b", "d"})
        assert not isinstance(lease_a, Exception), lease_a
        assert not lease_a.root_certain, "the schedule needs a lease that is not certain of its root"

        # Step 5: a chooses its commit base; its recovery round is answered by c and d.
        def pump():
            h.net.deliver(src="a", dst={"c", "d"}, kinds=("query",))
            h.net.drop(src="a", kinds=("query",))
            h.net.deliver(src=None, dst={"a"}, kinds=("answer",))
        base = h._run(lambda: _lease_base(a, a.tier, lease_a), pump)

        # Step 6: e's delayed message reaches c; if c accepts it, e counts a majority and R' is
        # acknowledged.  The wait is bounded: when R' gets no majority, e's frozen timer would keep
        # its replication waiting forever, so the test then lets e's lease expire.
        for _, dst, msg in held_for_c:
            h.net.handlers[dst](msg)
            h.nodes[dst].wait_idle(5.0)
        deadline = time.monotonic() + 5
        while e_commit.is_alive() and time.monotonic() < deadline:
            h.net.deliver(src=None, dst={"e"}, kinds=("ack",))
            time.sleep(0.001)
        if e_commit.is_alive():
            h.clock.t = max(h.clock.t, lease_e.valid_until + 1)
            lease_e._clock = h.clock
            h.net.drop(src="e", kinds=("records", "replicate", "ack"))
            e_commit.join(10)
            assert not e_commit.is_alive(), "e's replication did not end after its lease expired"
        acknowledged = not isinstance(out.get("r"), Exception)
        if not acknowledged:
            _not_completed(f"R' was never acknowledged (c accepted it: {c.accepted_root()[1] == r_prime}); "
                           f"e's replication ended with {out['r']!r}.  Nothing acknowledged can be lost.")

        # Step 7: a commits on its base, with c and d, recording what it replicated as built on.
        if not isinstance(base, tuple):
            _not_completed(f"a found no base to commit on: {base!r}")
            return
        current, label, _ = base
        lease_a.bind_root(current, label)
        new_root = rec(current.epoch + 1)
        preds = []
        send = a._transport.send

        def recording(to, msg):
            if msg["type"] == "replicate":
                preds.append(decode_labeled_root(msg["pred"]))
            return send(to, msg)
        a._transport.send = recording
        result = h.replicate("a", lease_a, new_root, {"c", "d"})
        if isinstance(result, Exception):
            _not_completed(f"a's commit in step 7 did not complete: {result!r}")
            return
        known, committed, committed_label = a.committed_root()[:3]
        assert known and committed == new_root and committed_label == lease_a.term

        # The invariant: the committed root extends the acknowledged R'.
        if not acknowledged:
            return
        assert preds and all(p == (True, r_prime, lease_e.term) for p in preds), (
            f"an acknowledged root (R', epoch {r_prime.epoch}, term {lease_e.term}) was superseded by a "
            f"committed root not built on it: a's commit was based on epoch {current.epoch}, term {label}")
    finally:
        h.close()
