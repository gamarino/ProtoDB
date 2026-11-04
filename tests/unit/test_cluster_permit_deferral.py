"""A permission request deferred by the fairness turn asks a known owner (Task 8.3f-B).

Measured on 3 TCP peers: a member whose request was deferred to a named turn waited in
``_await_turn`` while the named member owned the term and renewed it under load -- up to
``lease_duration`` (4.6-4.7 s with a 6 s lease) without one commit, while the owner served
the other members.  A deferral is timing only; once another member is known to own a term,
the request goes to that owner for a permission instead.
"""
import threading
import time

import pytest

from tests.support.cluster import Cluster

pytestmark = [pytest.mark.timeout(60)]

LEASE = 6.0


def test_a_deferred_request_is_served_by_the_busy_owner():
    """n1 defers to a named turn (n2, whose request was seen: up to ``lease_duration``); then
    n0 takes a term and keeps using it.  Before the fix n1 waited the whole lease (measured
    6.0 s here); now its request goes to n0 as soon as n0's term is known."""
    with Cluster(3, tiers=True, lease_duration=LEASE, request_timeout=0.25, startup_grace=0.0) as cl:
        cl.wait_rejoined()
        n0, n1 = cl["n0"], cl["n1"]
        with n1._cond:
            now = time.monotonic()
            n1._turn = ("n2", now, now)              # a release named n2, whose request was seen
            n1._owner_hint = (0, None)               # n1 does not know the owner yet
        stop = threading.Event()
        errors = []

        def busy():                                  # the owner keeps working: it never idles
            try:
                while not stop.is_set():
                    permission = n0.permit(timeout=5)
                    time.sleep(0.005)
                    n0.finish(permission)
            except Exception as e:                   # reported by the assertion below
                errors.append(e)
        got = {}

        def ask():
            permission = n1.permit(timeout=LEASE * 2)
            got["at"], got["owner"] = time.monotonic(), permission.owner
            n1.finish(permission)
        asker = threading.Thread(target=ask)
        asker.start()
        deadline = time.monotonic() + 5
        while n1.stats()["deferrals"] == 0 and time.monotonic() < deadline:
            time.sleep(0.001)                        # no owner exists yet: n1 defers to the turn
        assert n1.stats()["deferrals"] == 1
        worker = threading.Thread(target=busy)
        start = time.monotonic()
        worker.start()
        try:
            asker.join(LEASE * 2)
        finally:
            stop.set()
            worker.join(10)
        assert errors == [] and not asker.is_alive()
        assert got["owner"] == "n0", got
        seconds = got["at"] - start
        assert seconds < LEASE / 4, f"the deferred request waited {seconds:.2f} s for the turn"
