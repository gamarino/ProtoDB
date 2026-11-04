"""Every permission whose holder has a transaction to commit commits one (D50, D-LIVE-2).

A holder's transactions start from the committed root it knows.  Under a delayed link
from the owner, other members commit while the holder's permit is in flight.  The root
the permit carries is then newer than every snapshot the holder took before it arrived,
so a read-modify-write of a hot key conflicts in the permit's first round, always.  The
permit commits only through a retry that starts from the root the permit carries and is
built while the permit is still held.

Measured on b4259ce (Task 8.3f-B, ruling R82 check): with a 2.4 s lease and the
owner-to-holder link delayed by 0.25 s, holders held 69 and 185 permits in 30 s and
committed nothing; every round ended with ``ProtoConflictError``.
"""
import threading
import time

import pytest

import proto_db.transactions as tx
from tests.support.tiered_cluster import RUN_KW, TieredCluster, incr

pytestmark = [pytest.mark.timeout(120)]

WINDOW = 4.0
#: Validity a permit must still have when the holder gets it to be counted below: a permit
#: that arrives already expired cannot commit anything, by construction.
USABLE = 0.05


def _watch(tc, holder):
    """Record the holder's permits ``(validity at arrival, outcome)`` and, per thread, the
    delay between a conflicting commit and that thread's next commit attempt."""
    node = tc[holder]
    permits, retry_gaps, lock = [], [], threading.Lock()
    original_permit, original_finish = node.permit, node.finish
    held = {}

    def permit(*a, **k):
        p = original_permit(*a, **k)
        if p is not None:
            held[id(p)] = p.remaining()
        return p

    def finish(p):
        with lock:
            permits.append((held.pop(id(p), None), p.outcome, id(p) in conflicted))
        return original_finish(p)
    node.permit, node.finish = permit, finish
    conflicted = set()
    original_round = tx._permit_round

    def permit_round(coordinator, tier, permission, entries, group, **kw):
        result = original_round(coordinator, tier, permission, entries, group, **kw)
        if coordinator is node and result:
            conflicted.add(id(permission))
        return result
    tx._permit_round = permit_round
    original_commit = tx._commit_under_lease
    last_conflict = threading.local()

    def commit_under_lease(coordinator, lock_timeout, build):
        if coordinator is node:
            since = getattr(last_conflict, "at", None)
            if since is not None:
                with lock:
                    retry_gaps.append(time.monotonic() - since)
                last_conflict.at = None
        try:
            return original_commit(coordinator, lock_timeout, build)
        except tx.ProtoConflictError:
            if coordinator is node:
                last_conflict.at = time.monotonic()
            raise
    tx._commit_under_lease = commit_under_lease
    def undo():
        tx._commit_under_lease, tx._permit_round = original_commit, original_round
    return permits, retry_gaps, undo


def _owner(tc, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for node in tc.ids:
            if tc[node].owner == node:
                return node
        time.sleep(0.005)
    raise AssertionError("no owner")


@pytest.mark.parametrize("threads", [1, 2])
@pytest.mark.parametrize("round_", range(2))
def test_each_usable_permit_of_a_delayed_holder_commits(tmp_path, round_, threads):
    """With one committing thread per member, the holder's only commit is always the runner's
    own: it can be retried under the permission only if the runner keeps the permission for
    its own retry."""
    tc = TieredCluster(tmp_path, 3, storage_kw=dict(durability="fsync"), lease_duration=2.4,
                       request_timeout=0.1)
    with tc:
        tc.spaces[tc.ids[0]].new_database("db")
        stop = threading.Event()
        acked = {n: 0 for n in tc.ids}
        lock = threading.Lock()

        def work(node):
            db = tc.spaces[node].open_database("db")
            while not stop.is_set():
                try:
                    db.run(incr, **RUN_KW)
                except (tx.ProtoConflictError, tx.ProtoTimeoutError):
                    continue
                with lock:
                    acked[node] += 1
        owner_threads = [threading.Thread(target=work, args=(tc.ids[0],), daemon=True) for _ in range(threads)]
        for t in owner_threads:
            t.start()
        owner = _owner(tc)
        holder = next(n for n in tc.ids if n != owner)
        tc.cluster.net.set_link_delay(owner, holder, 0.25)
        permits, gaps, undo = _watch(tc, holder)
        try:
            others = [threading.Thread(target=work, args=(n,), daemon=True)
                      for n in tc.ids[1:] for _ in range(threads)]
            for t in others:
                t.start()
            time.sleep(WINDOW)
            stop.set()
            for t in owner_threads + others:
                t.join(30)
        finally:
            undo()
        # A permit counts when it reached the holder usable and the holder had a transaction
        # that conflicted in it: it must then commit (the retry from the permit's root).
        usable = [(o, c) for v, o, c in permits if v is not None and v >= USABLE and c]
        wasted = [o for o, c in usable if o != "committed"]
        gaps = sorted(gaps)
        print(f"holder {holder}: permits {len(permits)}, usable {len(usable)}, without a commit "
              f"{len(wasted)} {sorted(set(wasted))}; acked {acked}; retry gap ms p50 "
              f"{gaps[len(gaps) // 2] * 1000 if gaps else None} max {gaps[-1] * 1000 if gaps else None}")
        assert usable, "the holder held no usable permit"
        assert wasted == [], f"{len(wasted)} of {len(usable)} usable permits committed nothing"
