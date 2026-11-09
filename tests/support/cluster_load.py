"""Shared helpers of the cluster failure tests (Task 8.3n fix round 3, re-review 1, N8).

They lived in ``tests/concurrency/test_cluster_owner_failures.py`` and
``tests/concurrency/test_cluster_pipelined_permits.py`` and were imported from there by other
test modules, so a change to one test module silently changed the others.  Every user now
imports them from here, where a change is visibly shared (ruling R117: a ``tests/support``
change counts for every test that uses the helper).

* :class:`Load`: two committing threads per member, each commit with a token;
* :func:`check_after_faults`: the final commits after the faults heal and the resolution of
  every outcome by its token on every live member (the module docstring of
  ``test_cluster_owner_failures.py``);
* :func:`fsync_cluster`, :func:`confirm_every_member`, :func:`await_condition`,
  :func:`setter`: the pipelined-permit tests' cluster and waits;
* :func:`owner_of`, :func:`hook_replicate`.
"""
import threading
import time

import pytest

from proto_db.errors import ProtoConflictError, ProtoOutcomeUnknownError, ProtoTimeoutError
from tests.support.outcomes import CommitTokens, assert_outcomes_resolved, read_tokens, snapshot_of, write_token
from tests.support.tiered_cluster import RUN_KW, TieredCluster, incr

LEASE = dict(lease_duration=0.9, request_timeout=0.1)
STORAGE = dict(durability="fsync")
THREADS = 2
SECONDS = 1.6


def fsync_cluster(tmp_path, n=3, *, storage_kw=None, **kw):
    """``n`` tiered members in ``durability="fsync"`` with :data:`LEASE`; the database is
    created on the first."""
    tc = TieredCluster(tmp_path, n, storage_kw=dict(durability="fsync", **(storage_kw or {})), **dict(LEASE, **kw))
    tc.create_database(tc.ids[0])
    return tc


def setter(key):
    """A commit function that increments the root ``key``."""
    def write(tr):
        tr.set_root_object(key, (tr.get_root_object(key) or 0) + 1)
    return write


def await_condition(predicate, timeout=5.0, what="condition", tc=None):
    """Wait until ``predicate()``; fail naming ``what`` (and the members' nonzero stats)."""
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            stats = {} if tc is None else {n: {k: v for k, v in tc[n].stats().items() if isinstance(v, int) and v}
                                            for n in tc.ids if n in tc.spaces}
            raise AssertionError(f"timed out waiting for {what}; {stats}")
        time.sleep(0.002)


def confirm_every_member(tc):
    """Every member commits once: its first commit after attach is a confirmation round (an
    empty commit, ``needs_confirmation``), which a schedule must not take for the commit it
    drives."""
    for node in tc.ids:
        tc.commit(node, setter("w"))


def token_incr(tokens: CommitTokens, token: str):
    """An increment of ``n`` that records ``token`` and observes its base's tokens (P1)."""
    def fn(tr):
        tokens.observe(read_tokens(tr))
        incr(tr)
        write_token(tr, token)
    return fn


class Load:
    """Two committing threads per member; counts acknowledged and unknown commits, each with a
    token (:mod:`tests.support.outcomes`)."""

    def __init__(self, tc: TieredCluster):
        self.tc = tc
        self.lock = threading.Lock()
        self.acked = 0
        self.unknown = 0
        self.tokens = CommitTokens()
        self.stop = threading.Event()
        self.down = set()
        self.threads = [threading.Thread(target=self._worker, args=(node,), daemon=True)
                        for node in tc.ids for _ in range(THREADS)]

    def _worker(self, node: str) -> None:
        while not self.stop.is_set():
            if node in self.down:
                time.sleep(0.01)
                continue
            space = self.tc.spaces.get(node)
            if space is None:
                time.sleep(0.01)
                continue
            token = self.tokens.new(node)
            try:
                space.open_database("db").run(token_incr(self.tokens, token), **RUN_KW)
            except ProtoOutcomeUnknownError:
                self.tokens.unknown(token)
                with self.lock:
                    self.unknown += 1
                continue
            except (ProtoConflictError, ProtoTimeoutError):
                continue                # nothing applied: its token must stay absent
            except Exception:           # the space closed under a killed node: not acknowledged
                if node in self.down or self.tc.spaces.get(node) is not space:
                    self.tokens.unknown(token)      # killed mid-commit: it may have taken effect
                    continue
                raise
            self.tokens.acked(token)
            with self.lock:
                self.acked += 1

    def __enter__(self):
        for t in self.threads:
            t.start()
        return self

    def __exit__(self, *exc):
        self.stop.set()
        for t in self.threads:
            t.join(30)
        assert not any(t.is_alive() for t in self.threads), "a committing thread hung"


def owner_of(tc: TieredCluster, timeout: float = 5.0) -> str:
    """The member owning the current term, as the members see it (waits for one)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for node in tc.ids:
            try:
                if tc[node].owner == node:
                    return node
            except KeyError:
                continue
        time.sleep(0.005)
    raise AssertionError("no member owns a term under load")


#: Longest the final commits of :func:`check_after_faults` may take, once every live member is ready, to
#: get one commit acknowledged on each member: a liveness detector (R82), generous on
#: purpose.  Measured recoveries are printed by every run (fix round 2, item 2).
RECOVERY_TIMEOUT = 60.0


def await_ready(tc: TieredCluster, live, timeout: float = RECOVERY_TIMEOUT) -> None:
    """Wait until every live member can take part in a commit: its space is open (a killed
    member was revived), its coordinator is healthy (no fenced node, no failed tier: a member
    whose tier failed refuses every vote until its space is reopened) and it is not rejoining
    (Task 8.3n fix round 2, item 2).  A member that never gets there is named."""
    deadline = time.monotonic() + timeout
    while True:
        waiting = {}
        for node in live:
            if node not in tc.spaces:
                waiting[node] = "no space open"
                continue
            st = tc[node].stats()
            if not st["healthy"] or st["rejoining"]:
                waiting[node] = f"healthy={st['healthy']} rejoining={st['rejoining']} failure={st['failure']}"
        if not waiting:
            return
        if time.monotonic() > deadline:
            pytest.fail(f"members not ready {timeout:g} s after the faults healed: {waiting}")
        time.sleep(0.01)


def recovery_bound(tc: TieredCluster, live) -> float:
    """The longest the final commits of :func:`check_after_faults` may take once every live member is
    ready and the network is healed (re-review 1, N3), from the configured timeouts:

    - a term from before the heal may still be promised by its granters for one
      ``lease_duration`` on their timers; after it, one election (attempts of
      ``request_timeout`` with a back-off of at most 50 ms) makes a new owner, and its term
      serves every member from then on: one ``lease_duration`` for the term to end and one
      more for the election and the renewals of the new term, ``2 * lease_duration``;
    - one commit's permission wait is bounded by the space's ``lock_timeout`` (a longer one
      raises ``ProtoTimeoutError`` and is retried): ``lock_timeout`` for the first commit.

    The members' remaining commits each take one round under that owner, a few milliseconds.
    Measured: at most 1.10 s over 8,399 calls with ``lease_duration`` 0.9 s and ``lock_timeout``
    5 s (fix round 2), against a bound of 6.8 s."""
    node = live[0]
    return tc.spaces[node]._lock_timeout + 2 * tc[node]._duration


def check_after_faults(tc: TieredCluster, load: Load, live) -> None:
    """Every live member commits again, then every outcome is resolved by its token on every
    live member, and the counter equals the tokens present (module docstring).

    Liveness after the faults (fix round 2, item 2): the final commits start once every live
    member is ready (:func:`await_ready`), and each member commits until one commit is
    acknowledged.  An unknown outcome (conservative, ruling R86) is resolved by its token
    below; a conflict or a timeout applied nothing (its token must stay absent) and is
    retried.  No attempt count: :data:`RECOVERY_TIMEOUT` is the hang detector, and the measured
    recovery must stay within :func:`recovery_bound` of the configured timeouts (re-review 1,
    N3), so a regression to several seconds fails too."""
    tc.cluster.net.set_faults(drop=0.0, duplicate=0.0, delay=(0.0, 0.0))
    tc.cluster.net.heal()
    tokens = load.tokens
    healed = time.monotonic()
    await_ready(tc, live)
    ready = time.monotonic()
    attempts = {}
    for node in live:
        outcomes = attempts[node] = []
        while True:
            token = tokens.new(f"{node}.final")
            try:
                tc.db(node).run(token_incr(tokens, token), **RUN_KW)
            except ProtoOutcomeUnknownError as e:
                tokens.unknown(token)
                load.unknown += 1
                outcomes.append(f"unknown: {e}")
            except (ProtoConflictError, ProtoTimeoutError) as e:
                outcomes.append(f"{type(e).__name__}: {e}")     # nothing applied: retried
            else:
                tokens.acked(token)
                load.acked += 1
                break
            if time.monotonic() - ready > RECOVERY_TIMEOUT:
                pytest.fail(f"{node} got no commit acknowledged {RECOVERY_TIMEOUT:g} s after every member was "
                            f"ready; its attempts: {outcomes}")
    done = time.monotonic()
    retried = {node: len(o) for node, o in attempts.items() if o}
    bound = recovery_bound(tc, live)
    print(f"\nrecovery: members ready {ready - healed:.2f} s after the heal, one acknowledged commit per member "
          f"{done - ready:.2f} s later (bound {bound:.1f} s); attempts that did not commit: {retried or 'none'}")
    assert done - ready <= bound, \
        f"recovery took {done - ready:.2f} s, beyond {bound:.1f} s; attempts that did not commit: {attempts}"
    # A member learns the others' last commits from their "done": the helper waits for every
    # member to show every acknowledged token.
    res = assert_outcomes_resolved(
        {node: (lambda node=node: snapshot_of(tc.db(node), lambda tr: tr.get_root_object("n"))) for node in live},
        tokens)
    assert set(res.values.values()) == {len(res.present)}, \
        f"n = {res.values}, tokens {len(res.present)}, acknowledged {load.acked}, unknown {load.unknown}"
    assert load.acked > 2 * len(live)


def hook_replicate(tc: TieredCluster, node: str, action):
    """Run ``action()`` once, right before ``node`` sends its next replication."""
    coordinator = tc[node]
    replicate = coordinator.replicate
    fired = threading.Event()

    def hooked(lease, root, **kw):
        if not fired.is_set():
            fired.set()
            action()
        return replicate(lease, root, **kw)

    coordinator.replicate = hooked
    return fired
