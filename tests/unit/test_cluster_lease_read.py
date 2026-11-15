"""A read after the held lease expired is not a hard, non-retryable error (Task LEASE-READ, R212).

The peer page request (``fetch_record``) and the ``commit_query`` ask were capped at what
remains of the lease this node holds, for every read on the node.  The owner keeps its term
lease (``_held``) for the whole term, so between the lease's expiry and its release every read
on the owner asked its peers with a timeout of 0, fell through to the object store and, with
the store down or the segment not uploaded yet, raised ``ProtoUnexpectedError`` while a
majority held the record.  In a commit, ``_permit_round`` passed that error through although
nothing was sent, and ``Database.run`` does not retry it.

Ruling R212: the cap applies only to a read made inside a commit's critical section (a commit
never waits past its permission); every other read waits the full ``fetch_timeout``.  A commit
whose base read fails with exactly ``ProtoUnexpectedError`` once its permission is no longer
valid gets a retryable ``ProtoConflictError`` (nothing was sent).  A valid permission and a
failed read stay an honest error.

The coordinator cases use :class:`~tests.support.manual_cluster.Harness`: one fake clock
advanced by hand and every message held until delivered, so the lease's expiry is a clock
step and the wait a read is given is the ``timeout`` it passes to ``_ask`` (recorded; no wall
time is measured).
"""
import threading

import pytest

from proto_db.core.atom import AtomPointer
from proto_db.errors import (ProtoConflictError, ProtoOutcomeUnknownError, ProtoUnexpectedError,
                             ProtoWriteRefusedError)
from proto_db.transactions import _Entry, _Group, _permit_round
from tests.support.manual_cluster import GRACE, Harness, rec
from tests.support.tiered_cluster import RUN_KW, TieredCluster, incr

pytestmark = [pytest.mark.timeout(120)]


def _record_asks(node):
    """Wrap ``node._ask`` to record ``(kind, timeout)`` of every ask; returns the list."""
    asks = []
    ask = node._ask

    def recording(kind, msg_fields, done, timeout, targets=None):
        asks.append((kind, timeout))
        return ask(kind, msg_fields, done, timeout, targets=targets)

    node._ask = recording
    return asks


def _serve(h, name, peers, kinds=("fetch", "commit_query")):
    """A pump delivering ``name``'s ``kinds`` requests to ``peers`` and their replies back."""
    def pump():
        h.net.deliver(src=name, dst=set(peers), kinds=kinds)
        h.net.drop(src=name, kinds=kinds)
        h.net.deliver(src=None, dst={name}, kinds=("fetched", "commit_info"))
    return pump


def _expired_holder():
    """A harness whose node ``a`` holds a lease that expired and was not released (the owner
    loop lags), and a record ``r`` that only ``b`` and ``c`` hold (a majority)."""
    h = Harness("abc", records=True)
    h.bootstrap("a")
    h.clock.t += 2 * GRACE
    lease = h.attempt("a", {"b", "c"})
    assert not isinstance(lease, Exception), lease
    r = rec(7)
    for n in "bc":
        h.nodes[n].tier.held.add((r.segment, r.offset))
    h.clock.t = lease.valid_until + 1.0
    a = h.nodes["a"]
    assert a._held is lease and not lease.is_valid() and lease.remaining() == 0.0
    assert a.tier.local_record(AtomPointer(r.segment, r.offset)) is None
    return h, a, lease, r


def test_a_read_after_the_held_lease_expired_waits_the_full_fetch_timeout():
    """A read on ``a`` (no commit) after its lease expired asks the peers with the full
    ``fetch_timeout`` and gets the record a majority holds.  Before R212 it asked with 0 s
    and got nothing (the store is the only fallback)."""
    h, a, lease, r = _expired_holder()
    try:
        asks = _record_asks(a)
        got = h._run(lambda: a.fetch_record(AtomPointer(r.segment, r.offset)), _serve(h, "a", "bc"))
        assert asks and all(kind == "fetch" and t == a._fetch_timeout for kind, t in asks), \
            f"a read outside a commit was capped at the expired lease: {asks}"
        assert got == b"record", f"the peer fetch returned {got!r} while b and c hold the record"
    finally:
        h.close()


def test_a_commit_query_after_the_held_lease_expired_waits_the_full_fetch_timeout():
    """The ``commit_query`` ask outside a commit (a rejoin, a reader) is not capped either."""
    h, a, lease, r = _expired_holder()
    try:
        asks = _record_asks(a)
        got = h._run(lambda: a._ask_commit(lease.term, r), _serve(h, "a", "bc"))
        assert asks and all(kind == "commit_query" and t == a._fetch_timeout for kind, t in asks), \
            f"a commit_query outside a commit was capped at the expired lease: {asks}"
        assert got == (False, [(r.segment, r.offset, r.offset + 1)]), got
    finally:
        h.close()


def _owner_permission(h, name="a"):
    """``name`` owns a term and holds its own write permission (D50); returns it."""
    h.bootstrap(name)
    h.clock.t += 2 * GRACE
    node, others = h.nodes[name], set(h.names) - {name}

    def pump():
        h.net.deliver(src=name, dst=others, kinds=("request", "release"))
        h.net.deliver(src=None, dst={name}, kinds=("grant", "reject"))
    permission = h._run(lambda: node.permit(timeout=5), pump)
    assert not isinstance(permission, Exception), permission
    assert node._permit is permission and permission.is_valid()
    return permission


def test_a_read_inside_a_commit_waits_no_longer_than_its_valid_permission():
    """Inside a commit's critical section the cap still applies: the fetch and the
    ``commit_query`` ask wait at most what remains of the permission (R212: a commit never
    waits past its permission)."""
    h = Harness("abc", records=True)
    try:
        a = h.nodes["a"]
        permission = _owner_permission(h)
        h.clock.t = permission.valid_until - a._fetch_timeout / 4
        assert permission.is_valid()
        left = permission.remaining()
        assert 0 < left < a._fetch_timeout
        r = rec(7)
        for n in "bc":
            h.nodes[n].tier.held.add((r.segment, r.offset))
        asks = _record_asks(a)

        def in_commit():
            with a.tier.critical_section():
                return a.fetch_record(AtomPointer(r.segment, r.offset)), a._ask_commit(permission.term, r)

        fetched, _ = h._run(in_commit, _serve(h, "a", "bc"))
        assert [k for k, _ in asks] == ["fetch", "commit_query"], asks
        assert all(t == pytest.approx(left) for _, t in asks), \
            f"a read inside a commit waited beyond its permission ({left} s left): {asks}"
        assert fetched == b"record"
    finally:
        h.close()


def _round(h, name, permission, build, peers):
    """One ``_permit_round`` of ``name`` under ``permission`` with ``build``; returns
    ``(entry, messages a voter may accept that left name)``."""
    node = h.nodes[name]
    group = _Group()
    entry = _Entry(build, node.tier, group)
    sent = []

    def pump():
        h.net.deliver(src=name, dst=set(peers), kinds=("query", "commit_query", "fetch"))
        sent.extend(h.net.take(name, None, ("records", "replicate")))
        h.net.drop(src=name, kinds=("query", "commit_query", "fetch"))
        h.net.deliver(src=None, dst={name}, kinds=("answer", "commit_info", "fetched"))

    def run():
        with node.tier.critical_section():
            return _permit_round(node, node.tier, permission, [entry], group)

    h._run(run, pump)
    return entry, sent


def _expiring_build(h, permission, error):
    """A build whose base read fails with ``error`` after the permission expired (the clock
    passes its validity while the read waits)."""
    def build(current, stamp):
        h.clock.t = max(h.clock.t, permission.valid_until + 0.001)
        raise error
    return build


def test_a_base_read_failing_after_the_permission_expired_is_a_retryable_conflict():
    """``ProtoUnexpectedError`` from a base read once the permission is no longer valid:
    ``ProtoConflictError`` (chained from it), nothing replicated, the outcome not unknown."""
    h = Harness("abc", records=True)
    try:
        permission = _owner_permission(h)
        cause = ProtoUnexpectedError("Reading segment x failed after 3 attempt(s): store unreachable")
        entry, sent = _round(h, "a", permission, _expiring_build(h, permission, cause), "bc")
        assert isinstance(entry.error, ProtoConflictError), \
            f"an expired permission's failed base read was not retryable: {entry.error!r}"
        assert "expired before its base could be read; nothing was sent" in str(entry.error)
        assert entry.error.__cause__ is cause
        assert not sent, f"something left a: {sent}"
        assert permission.outcome != "unknown"
    finally:
        h.close()


@pytest.mark.parametrize("error", [
    ProtoOutcomeUnknownError("planted outcome unknown"),
    ProtoWriteRefusedError("planted write refused"),
], ids=lambda e: type(e).__name__)
def test_a_subclass_of_the_unexpected_error_is_never_made_retryable(error):
    """Only exactly ``ProtoUnexpectedError`` is converted: a subclass keeps its meaning."""
    h = Harness("abc", records=True)
    try:
        permission = _owner_permission(h)
        entry, sent = _round(h, "a", permission, _expiring_build(h, permission, error), "bc")
        assert entry.error is error, f"{type(error).__name__} became {entry.error!r}"
        assert not sent
    finally:
        h.close()


def test_a_base_read_failing_under_a_valid_permission_stays_an_honest_error():
    """Ruling R212, 3: a valid permission and a failed read (peers slower than the cap, the
    store down) is ``ProtoUnexpectedError``, not a conflict."""
    h = Harness("abc", records=True)
    try:
        permission = _owner_permission(h)
        cause = ProtoUnexpectedError("store unreachable")

        def build(current, stamp):
            raise cause

        entry, sent = _round(h, "a", permission, build, "bc")
        assert permission.is_valid()
        assert entry.error is cause, entry.error
        assert not sent
    finally:
        h.close()


def test_database_run_retries_a_commit_whose_permission_expired_before_its_base_read(tmp_path):
    """End to end: the first attempt's base read (inside the critical section, on a base a
    concurrent commit produced) fails with ``ProtoUnexpectedError`` once its permission
    expired; ``Database.run`` retries the conflict and the commit lands exactly once."""
    with TieredCluster(tmp_path) as tc:
        tc.create_database("n0")
        tc.store.set_outage(True)                       # the store cannot serve the read
        tc.commit("n0", incr)
        n2, storage = tc["n2"], tc.storages["n2"]
        read = storage._record
        state = {"attempts": 0, "fired": None}

        def failing_read(ptr):
            permission = n2._permit
            if state["fired"] is None and storage.in_critical() and permission is not None:
                # The read outlived the permission (the injectable lease clock passes its end),
                # then failed as an unreachable store does.
                permission._clock = lambda: permission.valid_until + 1.0
                state["fired"] = permission
                raise ProtoUnexpectedError(f"Reading the record at {ptr} failed after 3 attempt(s): "
                                           f"store unreachable (injected)")
            return read(ptr)

        storage._record = failing_read
        replications = n2.stats()["replications"]

        def step(tr):
            state["attempts"] += 1
            if state["attempts"] == 1:
                tc.commit("n0", incr)                   # the base moves: n2's commit reads it
            incr(tr)

        try:
            tc.db("n2").run(step, **RUN_KW)
        finally:
            storage._record = read
        assert state["fired"] is not None, "the schedule did not run: no base read inside the commit"
        assert state["attempts"] >= 2
        assert n2.stats()["replications"] >= replications + 1
        assert tc.value("n2") == 3
        for node in ("n0", "n1"):
            assert tc.eventually(node, 3), f"{node} does not see the retried commit"
