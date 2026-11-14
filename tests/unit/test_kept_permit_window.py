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


def _kept_permission(tc, monkeypatch, delay):
    """Keep a permission of n1 in n1's commit group; return ``(group, ending, ended)``:
    ``ending`` is set when its end starts, ``ended`` once ``coordinator.finish`` returned.
    ``delay()`` runs between the two (the widened window)."""
    real_end = T._end_permission
    ending, ended = threading.Event(), threading.Event()
    ours = []

    def slow_end(coordinator, permission):
        if ours and permission is ours[0]:
            ending.set()
            delay()
            real_end(coordinator, permission)
            ended.set()
            return
        real_end(coordinator, permission)
    monkeypatch.setattr(T, "_end_permission", slow_end)
    c = tc["n1"]
    permission = c.permit(30)
    ours.append(permission)
    group = T._group_of(c)
    T._keep(group, permission, c.tier, 0, c)
    return group, ending, ended


def _drop_from_another_thread(group):
    """The keeper's ``Database.run`` returned instead of retrying: ``_drop_kept`` in its thread."""
    def drop():
        with group.cond:
            group.kept = group.kept[:3] + (threading.get_ident(),) + group.kept[4:]
        T._RUN.kept_group = group
        T._drop_kept()
    threading.Thread(target=drop, daemon=True).start()


@pytest.mark.parametrize("path", ["expiry timer", "drop_kept"])
def test_a_commit_inside_the_end_of_a_kept_permission_waits_for_it_and_commits(tmp_path, monkeypatch, path):
    with TieredCluster(tmp_path) as tc:
        tc.create_database("n0")
        tc.commit("n1", incr)
        before = tc.value("n1")
        group, ending, ended = _kept_permission(tc, monkeypatch, lambda: time.sleep(_WIDENED))
        if path == "drop_kept":
            _drop_from_another_thread(group)
        assert ending.wait(5), "the kept permission was not ended"
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
        group, ending, ended = _kept_permission(tc, monkeypatch, lambda: release.wait(30))
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
