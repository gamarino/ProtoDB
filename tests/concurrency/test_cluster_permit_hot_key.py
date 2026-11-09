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
from tests.support.outcomes import CommitTokens, assert_outcomes_resolved, read_tokens, snapshot_of, write_token
from tests.support.tiered_cluster import RUN_KW, TieredCluster, incr

pytestmark = [pytest.mark.timeout(120)]

WINDOW = 4.0


#: Observed causes that end a conflicted permission without a retry under it, each allowed:
#: the keep's ``_RETRY_HOLD`` timer ended it, ``_take_kept`` found it no longer usable, the
#: retry wait timed out (timing, R82), or a D55 rule ended it (a proposed root, a closed gate,
#: an unknown outcome).
EXCUSES = {"keep timer", "take refused", "retry wait timed out", "proposed", "gate closed", "unknown"}


def _watch(tc, holder):
    """Record the holder's permits ``(rounds, outcome, causes)`` -- ``rounds`` lists, per round run
    under the permission, ``(conflicted, usable when it returned)`` (valid with at least
    ``send_reserve`` left); ``causes`` the observed
    events of :data:`EXCUSES` and ``"kept"`` -- and, per thread, the delay between a conflicting
    commit and that thread's next commit attempt."""
    node = tc[holder]
    retry_gaps, lock = [], threading.Lock()
    seen = {}           # id -> (permission, [rounds], {causes}); the permission keeps its id unique

    def entry(permission):
        return seen.setdefault(id(permission), (permission, [], set()))

    def note(permission, cause):
        with lock:
            entry(permission)[2].add(cause)

    def permits():
        """``(rounds, outcome, causes)`` of every permission the holder ran a round under; a
        round is ``(conflicted, valid when it returned)``, ``None`` while it runs."""
        with lock:
            return [(list(r), p.outcome, set(c)) for p, r, c in seen.values()]
    originals = {name: getattr(tx, name) for name in
                 ("_permit_round", "_keep", "_take_kept", "_end_permission", "_await_retries")}

    def permit_round(coordinator, tier, permission, entries, group, **kw):
        if coordinator is not node:
            return originals["_permit_round"](coordinator, tier, permission, entries, group, **kw)
        with lock:
            rounds = entry(permission)[1]
            rounds.append(None)
            i = len(rounds) - 1
        result = originals["_permit_round"](coordinator, tier, permission, entries, group, **kw)
        with lock:
            # Usable when it returned: valid with at least ``send_reserve`` left (Task 8.3n fix
            # round 3, R121).  With less, the holder sends nothing acceptable by rule: a
            # truthful conflict caused by the clock, like an expiry.
            rounds[i] = (bool(result), permission.remaining() >= node._send_reserve)
            causes = entry(permission)[2]
            if getattr(permission, "proposed", False):
                causes.add("proposed")
            if getattr(permission, "gate_closed", False):
                causes.add("gate closed")
            if permission.outcome == "unknown":
                causes.add("unknown")
        return result

    def keep(group, permission, tier, rounds, coordinator):
        if coordinator is node:
            note(permission, "kept")
        return originals["_keep"](group, permission, tier, rounds, coordinator)

    def take_kept(group, coordinator, tier):
        with group.cond:
            kept = group.kept
        result = originals["_take_kept"](group, coordinator, tier)
        if coordinator is node and kept is not None and result is None:
            note(kept[0], "take refused")
        return result

    def end_permission(coordinator, permission):
        if coordinator is node and isinstance(threading.current_thread(), threading.Timer):
            note(permission, "keep timer")
        return originals["_end_permission"](coordinator, permission)

    def await_retries(group, conflicted, tier, permission):
        result = originals["_await_retries"](group, conflicted, tier, permission)
        if not result:
            note(permission, "retry wait timed out")
        return result
    tx._permit_round, tx._keep, tx._take_kept = permit_round, keep, take_kept
    tx._end_permission, tx._await_retries = end_permission, await_retries
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
        tx._commit_under_lease = original_commit
        for name, fn in originals.items():
            setattr(tx, name, fn)
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
        tc.create_database(tc.ids[0])
        stop = threading.Event()
        acked = {n: 0 for n in tc.ids}
        lock = threading.Lock()
        tokens = CommitTokens()
        errors = []

        def work(node):
            db = tc.spaces[node].open_database("db")
            while not stop.is_set():
                token = tokens.new(node)

                def bump(tr, token=token):
                    tokens.observe(read_tokens(tr))     # P1: a snapshot is a committed root
                    incr(tr)
                    write_token(tr, token)
                try:
                    db.run(bump, **RUN_KW)
                except (tx.ProtoConflictError, tx.ProtoTimeoutError):
                    continue                            # nothing applied: its token must stay absent
                except tx.ProtoOutcomeUnknownError:
                    tokens.unknown(token)               # P5 / R86: resolved by its token below
                    continue
                except BaseException as e:              # reported by the assertion below
                    errors.append(repr(e))
                    return
                tokens.acked(token)
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
        assert not errors, errors
        # A permit counts when its first round conflicted and a retry round ran under it (Task
        # 8.3n: an observed condition; before, "usable" meant 50 ms of validity at arrival, and a
        # retry delayed past that by the scheduler failed the test on timing alone).  The retry
        # builds on the permit's root, so it must send its root: "committed", or "unknown" when
        # no majority answered in time (P5 / R86, resolved by the tokens below).  A retry round
        # that returned after the permit expired, or with less than ``send_reserve`` left (the
        # holder then sends nothing acceptable by rule, R121), is excused: the clock ended it.
        permits = permits()
        retried = [(r, o, c) for r, o, c in permits if len(r) >= 2 and r[0] and r[0][0]]
        wasted = [o for r, o, c in retried if o not in ("committed", "unknown") and r[-1] and r[-1][1]]
        expired = [o for r, o, c in retried if o not in ("committed", "unknown") and not (r[-1] and r[-1][1])]
        unkept = [c for r, o, c in permits if len(r) == 1 and r[0] and r[0][0] and r[0][1]]
        unexcused = [sorted(c) for c in unkept if not c & EXCUSES]
        gaps = sorted(gaps)
        print(f"holder {holder}: permits {len(permits)}, retried {len(retried)}, wasted {len(wasted)} "
              f"{sorted(set(wasted))}, expired in the retry {len(expired)}, conflicted and not retried "
              f"{len(unkept)} {[sorted(c) for c in unkept]}; sends refused for the reserve "
              f"{tc[holder].stats()['sends_refused_reserve']}; acked {acked}, unknown {len(tokens.unknown_tokens)}; "
              f"retry gap ms p50 {gaps[len(gaps) // 2] * 1000 if gaps else None} "
              f"max {gaps[-1] * 1000 if gaps else None}")
        assert any(o == "committed" for _, o, _ in retried), "no retry under a permit committed (D-LIVE-2)"
        # Fix round 1 (review I4): a conflicted permit that was still valid and ran no second
        # round must have an observed cause in EXCUSES (the keep's hold timer or validity ended
        # it, the retry did not come in time, or a D55 rule ended it).  A lost keep has none,
        # with one thread or two.  Before, a ratio bound let half of them go unretried.
        assert unexcused == [], f"{len(unexcused)} conflicted permits ended unretried with no cause: {unexcused}"
        # A retried permit that ended neither committed nor unknown: either its last round
        # returned while it was still usable (wasted, never allowed), or its validity fell below
        # ``send_reserve`` or ended in that round (the clock ended it, an observed cause).
        assert wasted == [], f"{len(wasted)} of {len(retried)} retried permits sent nothing while valid"
        # Every outcome resolved by its token; the counter, read in the same snapshot, matches.
        res = assert_outcomes_resolved(
            {n: (lambda n=n: snapshot_of(tc.db(n), lambda tr: tr.get_root_object("n"))) for n in tc.ids}, tokens)
        assert set(res.values.values()) == {len(res.present)}, (res.values, len(res.present))


def test_a_permission_kept_for_a_slow_retry_is_released_for_the_other_members(tmp_path):
    """Review I2 (Task 8.3f-B fix round 1): a permission kept for the runner's own retry was
    bounded only by its validity, so a retry whose ``fn`` worked for a second stalled every
    other member for that second (a8a87a7: 0.5 s of work, 502 ms for another member's single
    commit; 1.5 s, a term abandoned).  The keep is now bounded by ``_RETRY_HOLD``."""
    slow = 1.0
    with TieredCluster(tmp_path, 3, storage_kw=dict(durability="fsync"), lease_duration=2.4,
                       request_timeout=0.1) as tc:
        a, b, c = tc.ids
        tc.create_database(a)
        for n in tc.ids:
            tc.commit(n, incr)
        time.sleep(0.3)
        abandoned = sum(tc[n].stats().get("terms_abandoned", 0) for n in tc.ids)
        started, latency = [], []

        def fn(tr):
            group = tx._GROUPS.get(tc[b])
            kept = group is not None and group.kept is not None
            v = tr.get_root_object("n") or 0
            if kept and not started:
                started.append(1)

                def other():
                    t0 = time.monotonic()
                    tc.db(c).run(incr, **RUN_KW)
                    latency.append(time.monotonic() - t0)
                threading.Thread(target=other, daemon=True).start()
                time.sleep(slow)
            elif not started:
                # Another member commits first: this commit conflicts and its permission is kept.
                t = threading.Thread(target=lambda: tc.db(a).run(incr, **RUN_KW))
                t.start()
                t.join()
            tr.set_root_object("n", v + 1)

        for _ in range(10):
            tc.commit(b, fn)
            if started:
                break
        assert started, "the schedule never kept a permission"
        deadline = time.monotonic() + 10
        while not latency and time.monotonic() < deadline:
            time.sleep(0.01)
        assert latency and latency[0] < slow / 2, latency
        assert sum(tc[n].stats().get("terms_abandoned", 0) for n in tc.ids) == abandoned
