"""A kept write permission is never neither kept nor finished (Task KEPT-PERMIT, ruling R148).

A permission kept for the immediate retry of a runner's own commit (D50 Amendment "held
retries") is ended by the expiry timer (``_keep``), by ``_drop_kept`` or by the next runner
(``_take_kept``).  Each takes it out of ``group.kept`` under ``group.cond`` and calls
``coordinator.finish`` after releasing that lock.  Before the fix, a runner that looked in
between found nothing kept and called ``coordinator.permit()`` while the node's permission slot
was still taken: ``ProtoUserError("Node n1 already holds a write permission")``, a misuse error
that ``Database.run`` does not retry, for a correct commit that sent nothing (seen in CI as
``test_cluster_idle_release.py::test_the_busy_member_keeps_committing_after_an_idle_owner_
releases[1]``).  Now ``group.kept`` holds an *ending* marker until ``finish`` returns, and a
runner that finds it waits, bounded by the permit's own timeout.

The window is widened by delaying ``_end_permission`` for the kept permission only; product
logic is unchanged."""
import threading
import time

import pytest

import proto_db.transactions as T
from proto_db.errors import ProtoTimeoutError
from tests.support.tiered_cluster import TieredCluster, incr

#: How long the end of the kept permission is delayed: the window, widened.
_WIDENED = 0.3


def _kept_permission(tc, monkeypatch, delay, *, hold=None):
    """Keep a permission of n1 in n1's commit group; return ``(group, ending, ended, enders)``:
    ``ending`` is set when its end starts, ``ended`` once ``coordinator.finish`` returned,
    ``enders`` the threads that ended it.  ``delay()`` runs between the two (the widened
    window).  ``hold``: the keep's ``_RETRY_HOLD`` (long, so that its timer cannot end the
    permission before the path under test does)."""
    real_end = T._end_permission
    ending, ended = threading.Event(), threading.Event()
    enders = []
    ours = []

    def slow_end(coordinator, permission):
        if ours and permission is ours[0]:
            enders.append(threading.current_thread())
            ending.set()
            delay()
            real_end(coordinator, permission)
            ended.set()
            return
        real_end(coordinator, permission)
    monkeypatch.setattr(T, "_end_permission", slow_end)
    if hold is not None:
        monkeypatch.setattr(T, "_RETRY_HOLD", hold)
    c = tc["n1"]
    permission = c.permit(30)
    ours.append(permission)
    group = T._group_of(c)
    T._keep(group, permission, c.tier, 0, c)
    return group, ending, ended, enders


def _drop_from_another_thread(group):
    """The keeper's ``Database.run`` returned instead of retrying: ``_drop_kept`` in its thread,
    which is returned."""
    def drop():
        with group.cond:
            group.kept = group.kept[:3] + (threading.get_ident(),) + group.kept[4:]
        T._RUN.kept_group = group
        T._drop_kept()
    thread = threading.Thread(target=drop, daemon=True)
    thread.start()
    return thread


@pytest.mark.parametrize("path", ["expiry timer", "drop_kept"])
def test_a_commit_inside_the_end_of_a_kept_permission_waits_for_it_and_commits(tmp_path, monkeypatch, path):
    with TieredCluster(tmp_path) as tc:
        tc.create_database("n0")
        tc.commit("n1", incr)
        before = tc.value("n1")
        group, ending, ended, enders = _kept_permission(tc, monkeypatch, lambda: time.sleep(_WIDENED),
                                                        hold=30.0 if path == "drop_kept" else None)
        dropper = _drop_from_another_thread(group) if path == "drop_kept" else None
        assert ending.wait(5), "the kept permission was not ended"
        # The path under test ended it (under load the 0.1 s timer could otherwise win).
        if dropper is None:
            assert isinstance(enders[0], threading.Timer), enders
        else:
            assert enders == [dropper], enders
        assert not ended.is_set(), "the widened window closed before the commit started"
        # What a correct application does in that window: one commit, no retry.  Before the fix:
        # ProtoUserError("Node n1 already holds a write permission").
        tc.db("n1").run(incr, retries=0)
        assert ended.is_set(), "the commit obtained a permission before the kept one was finished"
        assert tc.value("n1") == before + 1
        assert group.kept is None


def test_a_finish_that_hangs_bounds_the_next_runner_with_the_permit_timeout(tmp_path, monkeypatch):
    """R82: a kept permission whose ``finish`` does not return holds every runner of the node
    only for the permit's own timeout, which then surfaces as ``ProtoTimeoutError`` (as
    ``permit()`` reports its own wait), never as a hang or a misuse error."""
    release = threading.Event()
    with TieredCluster(tmp_path) as tc:
        tc.create_database("n0")
        tc.commit("n1", incr)
        before = tc.value("n1")
        group, ending, ended, _ = _kept_permission(tc, monkeypatch, lambda: release.wait(30))
        try:
            assert ending.wait(5), "the kept permission was not ended"
            started = time.monotonic()
            with pytest.raises(ProtoTimeoutError, match="still being ended"):
                T._commit_under_lease(tc["n1"], 0.5, T._confirm_build)
            waited = time.monotonic() - started
            assert 0.45 <= waited < 5.0, f"the runner waited {waited:.3f} s for a 0.5 s permit timeout"
            assert not ended.is_set()
        finally:
            release.set()
        assert ended.wait(5)
        tc.db("n1").run(incr, retries=0)       # the node commits again once the end returned
        assert tc.value("n1") == before + 1


def test_a_kept_permission_no_longer_usable_is_ended_by_the_next_runner_which_then_commits(tmp_path, monkeypatch):
    """The ``_take_kept`` branch: the next runner finds the kept permission past its hold
    deadline, ends it itself (under the marker, for uniformity: ``group.running`` serializes
    runners, so no other runner can look in between), then asks for a new one and commits."""
    with TieredCluster(tmp_path) as tc:
        tc.create_database("n0")
        tc.commit("n1", incr)
        before = tc.value("n1")
        group, ending, ended, enders = _kept_permission(tc, monkeypatch, lambda: time.sleep(0.2), hold=30.0)
        with group.cond:
            kept = group.kept
            group.kept = kept[:5] + (time.monotonic() - 1.0,) + kept[6:]     # past its hold deadline
        tc.db("n1").run(incr, retries=0)
        assert ended.is_set()
        assert enders == [threading.current_thread()], enders
        assert tc.value("n1") == before + 1
        assert group.kept is None


def test_a_finish_that_hangs_bounds_a_runner_without_lock_timeout_by_acquire_timeout(tmp_path, monkeypatch):
    """``lock_timeout=None``: the wait for an ending kept permission is bounded by the
    coordinator's ``acquire_timeout``, the default permit timeout."""
    release = threading.Event()
    with TieredCluster(tmp_path) as tc:
        tc.create_database("n0")
        tc.commit("n1", incr)
        c = tc["n1"]
        group, ending, ended, _ = _kept_permission(tc, monkeypatch, lambda: release.wait(30))
        monkeypatch.setattr(c, "_acquire_timeout", 0.4)
        try:
            assert ending.wait(5), "the kept permission was not ended"
            started = time.monotonic()
            with pytest.raises(ProtoTimeoutError, match=r"within 0\.4 s: its kept permission is still being ended"):
                T._commit_under_lease(c, None, T._confirm_build)
            waited = time.monotonic() - started
            assert 0.35 <= waited < 3.0, f"the runner waited {waited:.3f} s for a 0.4 s acquire_timeout"
        finally:
            release.set()
        assert ended.wait(5)


def test_the_permit_gets_what_the_wait_for_an_ending_kept_permission_left(tmp_path, monkeypatch):
    """The wait is part of obtaining a permission: ``permit()`` gets the remaining budget, not
    the full timeout again."""
    with TieredCluster(tmp_path) as tc:
        tc.create_database("n0")
        tc.commit("n1", incr)
        c = tc["n1"]
        group, ending, ended, _ = _kept_permission(tc, monkeypatch, lambda: time.sleep(_WIDENED))
        seen = []
        real_permit = c.permit

        def permit(timeout=None):
            seen.append(timeout)
            return real_permit(timeout)
        monkeypatch.setattr(c, "permit", permit)
        assert ending.wait(5), "the kept permission was not ended"
        T._commit_under_lease(c, 2.0, T._confirm_build)
        assert ended.is_set()
        assert seen and seen[0] is not None, seen
        assert seen[0] < 2.0, f"permit() got the full timeout after waiting: {seen}"


def test_a_wait_that_leaves_no_budget_names_the_configured_bound(tmp_path, monkeypatch):
    """A permit that times out after the wait reports the configured bound and the wait, never
    ``permit()``'s "within 0.0 s" (review of R148)."""
    with TieredCluster(tmp_path) as tc:
        tc.create_database("n0")
        tc.commit("n1", incr)
        c = tc["n1"]
        group, ending, ended, _ = _kept_permission(tc, monkeypatch, lambda: time.sleep(_WIDENED))
        real_permit = c.permit

        def permit(timeout=None):
            return real_permit(0.0)     # no budget left: permit's own timeout fires at once
        monkeypatch.setattr(c, "permit", permit)
        monkeypatch.setattr(c, "_permission_from_owner", lambda deadline: None)
        monkeypatch.setattr(c, "_become_owner", lambda deadline: (False, False))
        assert ending.wait(5), "the kept permission was not ended"
        with pytest.raises(ProtoTimeoutError, match=r"within 2\.0 s, .* s of it spent waiting") as info:
            T._commit_under_lease(c, 2.0, T._confirm_build)
        assert "within 0.0 s" not in str(info.value)
