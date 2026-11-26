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
import uuid

import pytest

from proto_db.core.atom import AtomPointer
from proto_db.errors import (ProtoConflictError, ProtoCorruptionError, ProtoDBError, ProtoOutcomeUnknownError,
                             ProtoUnexpectedError, ProtoWriteRefusedError)
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

        h._run(in_commit, _serve(h, "a", "bc"))
        assert [k for k, _ in asks] == ["fetch", "commit_query"], asks
        assert all(t == pytest.approx(left) for _, t in asks), \
            f"a read inside a commit waited beyond its permission ({left} s left): {asks}"
        # Whether the answers arrive within that real-time wait is not asserted: the cap is.
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


def test_database_run_retries_a_commit_whose_permission_expired_before_its_base_read(tmp_path, monkeypatch):
    """End to end: the first attempt's base read (inside the critical section, on a base a
    concurrent commit produced) fails with ``ProtoUnexpectedError`` once its permission
    expired; ``Database.run`` retries the conflict and the commit lands exactly once.

    The base moves between the commit's pre-pass and its lock step (spec section 8), so the new
    base is read only inside the critical section."""
    from proto_db import transactions
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

        pending = []
        lock_step = transactions._commit_root

        def after_prepass(storage_, lock_timeout, build, coordinator=None):
            if pending and coordinator is n2:
                pending.clear()
                tc.commit("n0", incr)                   # the base moves: n2's commit reads it
            return lock_step(storage_, lock_timeout, build, coordinator)
        monkeypatch.setattr(transactions, "_commit_root", after_prepass)

        def step(tr):
            state["attempts"] += 1
            if state["attempts"] == 1:
                pending.append(1)
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


# -- fix round 1 (review of LEASE-READ; rulings R230-R233) ------------------------------------

#: Nothing ships to the object store; a lease long enough that a loaded machine does not turn the
#: setup's commits into unknown outcomes (their reads depend on the peers alone).
NOT_UPLOADED = dict(persist_interval=3600.0, storage_kw=dict(ship_interval=3600.0),
                    lease_duration=3.0, request_timeout=0.25)


def _commit_without(tc, node, missing, fn=incr):
    """Commit ``fn`` on ``node`` while its replication never reaches ``missing`` (the other
    members are a majority): ``missing`` holds none of its records."""
    send = tc[node]._transport.send

    def hold_back(to, msg):
        if to == missing and msg["type"] in ("replicate", "records", "warm"):
            return
        send(to, msg)

    tc[node]._transport.send = hold_back
    try:
        tc.commit(node, fn)
    finally:
        tc[node]._transport.send = send


def _silence_fetches_to(tc, reader):
    """The other members' page-request replies to ``reader`` are lost (slow or cut-off peers);
    returns a function restoring them."""
    sends = {n: tc[n]._transport.send for n in tc.ids if n != reader}
    for n, send in sends.items():
        def lossy(to, msg, _send=send):
            if to == reader and msg["type"] == "fetched":
                return
            _send(to, msg)
        tc[n]._transport.send = lossy

    def restore():
        for n, send in sends.items():
            tc[n]._transport.send = send
    return restore


def test_a_segment_not_uploaded_yet_with_peers_unheard_is_unavailable_not_corrupt(tmp_path):
    """R230: the store does not hold the segment yet (nothing ships), and the peers that hold
    the record do not answer the page request: the read is ``ProtoUnexpectedError``
    (unavailable).  Before, it was ``ProtoCorruptionError`` ("in no tier"), a verdict the
    timeout produced."""
    with TieredCluster(tmp_path, **NOT_UPLOADED) as tc:
        tc.create_database("n0")
        _commit_without(tc, "n0", "n2")
        r = tc["n0"].committed_root()[1]
        ptr = AtomPointer(r.segment, r.offset)
        st = tc.storages["n2"]
        assert st.local_record(ptr) is None, "n2 holds the record"
        assert not any(r.segment.hex in k for k in tc.store.keys()), "the segment was uploaded"
        restore = _silence_fetches_to(tc, "n2")
        try:
            with pytest.raises(ProtoDBError) as info:
                st.get_atom(ptr)
        finally:
            restore()
        assert type(info.value) is ProtoUnexpectedError, \
            f"an unavailable record was reported as {type(info.value).__name__}: {info.value}"
        assert "unavailable" in str(info.value)
        assert isinstance(info.value.__cause__, ProtoCorruptionError)


def test_a_record_every_peer_and_the_store_lack_is_still_corruption(tmp_path):
    """The other half of R230: every peer answered that it lacks the record and the store has
    no such segment -- the record is in no tier: ``ProtoCorruptionError``."""
    with TieredCluster(tmp_path, **NOT_UPLOADED) as tc:
        tc.create_database("n0")
        ptr = AtomPointer(uuid.uuid4(), 64)
        with pytest.raises(ProtoCorruptionError, match="no full copy"):
            tc.storages["n2"].get_atom(ptr)


def test_database_run_retries_a_commit_whose_base_segment_is_not_uploaded_and_permission_expired(tmp_path, monkeypatch):
    """R230 with R212, on one natural schedule: a concurrent commit's records never reached
    n2 and its segment is not in the store.  n2's commit reads that base inside its critical
    section; the permission expires as the page request starts (the injectable lease clock),
    so the request is capped at 0 s, the store lacks the segment, and the read is
    unavailable -- a retryable conflict.  ``Database.run`` retries and the commit lands once.
    Before, the read raised ``ProtoCorruptionError`` and the commit failed.

    The base moves between the commit's pre-pass and its lock step (spec section 8): the pre-pass
    reads the root n2 knows, so the new base is read only under the permission, whatever n2 learnt
    of the concurrent commit before."""
    from proto_db import transactions
    with TieredCluster(tmp_path, **NOT_UPLOADED) as tc:
        tc.create_database("n0")
        tc.commit("n0", incr)
        n2, st = tc["n2"], tc.storages["n2"]
        wait = n2._read_wait
        state = {"attempts": 0, "fired": None}

        def expiring_wait(permitted):                   # called under n2's lock, per page request
            permission = n2._permit
            if state["fired"] is None and permitted and permission is not None:
                permission._clock = lambda: permission.valid_until + 1.0
                state["fired"] = permission
            return wait(permitted)

        n2._read_wait = expiring_wait
        read, raised = st._record, []

        def observed(ptr):                              # observes only: records what a read raised
            try:
                return read(ptr)
            except ProtoDBError as e:
                raised.append(e)
                raise
        st._record = observed

        pending = []
        lock_step = transactions._commit_root

        def after_prepass(storage, lock_timeout, build, coordinator=None):
            if pending and coordinator is n2:
                pending.clear()
                _commit_without(tc, "n0", "n2")         # the base moves; n2 lacks its records
            return lock_step(storage, lock_timeout, build, coordinator)
        monkeypatch.setattr(transactions, "_commit_root", after_prepass)

        def step(tr):
            state["attempts"] += 1
            if state["attempts"] == 1:
                pending.append(1)
            incr(tr)

        try:
            tc.db("n2").run(step, **RUN_KW)
        finally:
            n2._read_wait = wait
            st._record = read
        assert state["fired"] is not None, "the schedule did not run: no page request inside the commit"
        assert raised and type(raised[0]) is ProtoUnexpectedError and "unavailable" in str(raised[0]), \
            f"the capped read did not fail as unavailable: {raised!r}"
        assert state["attempts"] >= 2
        assert tc.value("n2") == 3
        for node in ("n0", "n1"):
            assert tc.eventually(node, 3), f"{node} does not see the retried commit"


def test_a_poisoned_tier_keeps_its_own_error_under_an_expired_permission():
    """R231: a build whose write poisoned the tier raises exactly ``ProtoUnexpectedError``; once
    the permission expired it is still that error, not "expired before its base could be
    read"."""
    h = Harness("abc", records=True)
    try:
        permission = _owner_permission(h)
        cause = ProtoUnexpectedError("Writing manifest failed: disk full")
        h.nodes["a"].tier.failure = cause            # the local tier poisoned by that write
        entry, sent = _round(h, "a", permission, _expiring_build(h, permission, cause), "bc")
        assert entry.error is cause, f"a poisoned tier's write error became {entry.error!r}"
        assert not sent
    finally:
        h.close()


def test_a_non_owner_commit_read_waits_no_longer_than_its_permission():
    """R232: ``b`` holds a permission from the owner ``a`` and no term lease of its own; a read
    inside its commit waits at most what remains of that permission.  At 88ca21d it waited
    the full ``fetch_timeout`` (only a held term lease capped it)."""
    h = Harness("abc", records=True)
    try:
        h.own("a")
        permission, _ = h.permit_from("b", "a")
        b = h.nodes["b"]
        assert b._held is None and b._permit is permission
        h.clock.t = permission.valid_until - b._fetch_timeout / 4
        left = permission.remaining()
        assert 0 < left < b._fetch_timeout
        r = rec(9)
        for n in "ac":
            h.nodes[n].tier.held.add((r.segment, r.offset))
        asks = _record_asks(b)

        def in_commit():
            with b.tier.critical_section():
                return b.fetch_record(AtomPointer(r.segment, r.offset))

        h._run(in_commit, _serve(h, "b", "ac"))     # the answer's arrival in time is not asserted
        assert asks and all(t == pytest.approx(left) for _, t in asks), \
            f"a non-owner's commit read waited beyond its permission ({left} s left): {asks}"
    finally:
        h.close()


def _failing_base_commit(h, monkeypatch, permission, error):
    """A round that must re-commit its base (:func:`_commit_base`) whose read of the base's
    records fails with ``error``; returns ``(entry, sent)``."""
    from proto_db import transactions
    a = h.nodes["a"]
    monkeypatch.setattr(transactions, "_base_needs_commit", lambda coordinator, permission, current: True)

    def failing(label, root):
        raise error
    monkeypatch.setattr(a, "commit_records", failing)
    built = []
    return _round(h, "a", permission, lambda current, stamp: built.append(current), "bc")


def test_a_base_re_commit_whose_read_fails_sent_nothing(monkeypatch):
    """R233: the reads of ``_commit_base`` come before anything a voter may accept leaves the
    node, so a failed read there does not make the outcome unknown (the owner does not
    abandon the term for it), and the error stands under a valid permission."""
    h = Harness("abc", records=True)
    try:
        permission = _owner_permission(h)
        cause = ProtoUnexpectedError("store unreachable")
        entry, sent = _failing_base_commit(h, monkeypatch, permission, cause)
        assert not sent
        assert entry.error is cause, entry.error
        assert permission.outcome != "unknown", \
            "a base re-commit that sent nothing ended its permission unknown"
    finally:
        h.close()


def test_a_base_re_commit_read_failing_after_the_permission_expired_is_retryable(monkeypatch):
    """R233 with R212: the same failed read once the permission expired is a retryable
    conflict."""
    h = Harness("abc", records=True)
    try:
        permission = _owner_permission(h)
        cause = ProtoUnexpectedError("store unreachable")

        def expire_then_fail(label, root):
            h.clock.t = max(h.clock.t, permission.valid_until + 0.001)
            raise cause
        from proto_db import transactions
        monkeypatch.setattr(transactions, "_base_needs_commit", lambda coordinator, permission, current: True)
        monkeypatch.setattr(h.nodes["a"], "commit_records", expire_then_fail)
        entry, sent = _round(h, "a", permission, lambda current, stamp: None, "bc")
        assert isinstance(entry.error, ProtoConflictError), entry.error
        assert entry.error.__cause__ is cause
        assert not sent and permission.outcome != "unknown"
    finally:
        h.close()


# -- fix round 2 (re-review 1 of LEASE-READ; rulings R234L-R239L) ------------------------------

def _duplicating_pump(h, name, duplicated, silent, kinds, replies):
    """Deliver ``name``'s ``kinds`` requests to ``duplicated`` twice each (the transport may
    duplicate, transport.py), never to ``silent`` (slow or cut off), and the replies back."""
    def pump():
        for _, to, msg in h.net.take(name, set(duplicated), kinds):
            for _ in range(2):
                h.net.handlers[to](msg)
                h.nodes[to].wait_idle(5.0)
        h.net.take(name, set(silent), kinds)
        h.net.deliver(src=None, dst={name}, kinds=replies)
    return pump


def test_a_duplicated_miss_never_makes_every_peer_answered():
    """R234L: ``b``'s miss arrives twice and ``c``, which holds the record, is never heard: the
    page request is not complete.  Counted per message, two misses from one member made
    ``(None, True)`` -- every peer answered -- so an available record was called corrupt
    (re-review 1, N-A; probe ``rr1-1794800000/probe/probe_dup.py``)."""
    h = Harness("abc", records=True)
    try:
        h.bootstrap("a")
        a = h.nodes["a"]
        r = rec(7)
        h.nodes["c"].tier.held.add((r.segment, r.offset))
        got = h._run(lambda: a.fetch_answer(AtomPointer(r.segment, r.offset)),
                     _duplicating_pump(h, "a", "b", "c", ("fetch",), ("fetched",)))
        assert got == (None, False), f"a duplicated miss counted as c's answer: {got}"
    finally:
        h.close()


def test_a_duplicated_commit_info_miss_never_ends_the_wait_as_answered():
    """R234L, the ``commit_query`` counter: with ``b``'s "unknown" twice and ``c`` silent, the
    ask's wait ends by its timeout, never because its "every peer answered" condition holds."""
    h = Harness("abc", records=True)
    try:
        h.bootstrap("a")
        a = h.nodes["a"]
        seen = []
        ask = a._ask

        def recording(kind, msg_fields, done, timeout, targets=None):
            waiter = ask(kind, msg_fields, done, timeout, targets=targets)
            seen.append(done(waiter))
            return waiter

        a._ask = recording
        r = rec(7)
        got = h._run(lambda: a._ask_commit(1, r),
                     _duplicating_pump(h, "a", "b", "c", ("commit_query",), ("commit_info",)))
        assert got == (False, None)
        assert seen == [False], "a duplicated commit_info miss made every peer look answered"
    finally:
        h.close()


def _corrupting(tc, reader):
    """The other members answer ``reader``'s page requests with corrupt bytes; returns a
    function restoring them."""
    sends = {n: tc[n]._transport.send for n in tc.ids if n != reader}
    for n, send in sends.items():
        def corrupt(to, msg, _send=send):
            if to == reader and msg["type"] == "fetched" and msg["data"] is not None:
                msg = dict(msg, data=b"\x00" * len(msg["data"]))
            _send(to, msg)
        tc[n]._transport.send = corrupt

    def restore():
        for n, send in sends.items():
            tc[n]._transport.send = send
    return restore


def test_a_corrupt_peer_answer_is_not_an_answer_that_the_record_is_nowhere(tmp_path):
    """R235L: every peer answers corrupt bytes.  A corrupt answer is no answer that the record is
    absent (since R242L the request asks the other members again, and they answer corrupt
    bytes too): with the store lacking the segment, the read is unavailable
    (``ProtoUnexpectedError``), not "no reachable peer holds it" (``ProtoCorruptionError``)."""
    with TieredCluster(tmp_path, **NOT_UPLOADED) as tc:
        tc.create_database("n0")
        _commit_without(tc, "n0", "n2")
        r = tc["n0"].committed_root()[1]
        st = tc.storages["n2"]
        restore = _corrupting(tc, "n2")
        try:
            with pytest.raises(ProtoDBError) as info:
                st.get_atom(AtomPointer(r.segment, r.offset))
        finally:
            restore()
        assert type(info.value) is ProtoUnexpectedError, \
            f"a corrupt peer answer made the read {type(info.value).__name__}: {info.value}"


def test_the_fetcher_and_its_answer_function_are_installed_together(tmp_path):
    """R236L: one setter installs the page request and its answer function, so re-installing
    the coordinator's pair (as a test that wraps the fetcher does) keeps R230; a fetcher
    installed alone is a plain fetcher whose ``None`` is definitive."""
    with TieredCluster(tmp_path, **NOT_UPLOADED) as tc:
        tc.create_database("n0")
        _commit_without(tc, "n0", "n2")
        r = tc["n0"].committed_root()[1]
        n2, st = tc["n2"], tc.storages["n2"]
        st.set_fetcher(n2.fetch_record, answer=n2.fetch_answer)
        restore = _silence_fetches_to(tc, "n2")
        try:
            with pytest.raises(ProtoUnexpectedError, match="unavailable"):
                st.get_atom(AtomPointer(r.segment, r.offset))
            st.set_fetcher(n2.fetch_record)                     # alone: definitive misses
            with pytest.raises(ProtoCorruptionError):
                st.get_atom(AtomPointer(r.segment, r.offset))
        finally:
            restore()
            st.set_fetcher(n2.fetch_record, answer=n2.fetch_answer)


def test_a_really_poisoned_tier_reports_its_failure_and_keeps_its_error(tmp_path, monkeypatch):
    """R237L: ``TieredStorage.failure`` on a tier poisoned by a failed manifest sync, and the
    R231 skip it drives: that tier's exact ``ProtoUnexpectedError`` is never converted, even
    under an expired permission."""
    import os
    from proto_db import transactions
    from proto_db.storage import tiered
    from proto_db.storage.tiered import TieredStorage
    from tests.support.fake_object_store import FakeObjectStore
    st = TieredStorage(str(tmp_path / "t"), FakeObjectStore(), durability="none")
    try:
        assert st.failure is None
        st.push_atom({"k": 1})                                  # registers an own segment
        fsync = os.fsync
        broken = OSError(5, "injected EIO")

        def failing(fd):
            if fd == st._manifest_fd:
                raise broken
            return fsync(fd)
        monkeypatch.setattr(tiered.os, "fsync", failing)
        with pytest.raises(ProtoUnexpectedError, match="Syncing"):
            st.flush_local(force=True)
        monkeypatch.setattr(tiered.os, "fsync", fsync)
        assert st.failure is broken, f"the poisoned tier reports {st.failure!r}"

        class Expired:
            def is_valid(self):
                return False
        error = ProtoUnexpectedError("Writing manifest failed")
        transactions._check_expired_read(Expired(), error, st)  # no conversion: returns
        with pytest.raises(ProtoConflictError):
            transactions._check_expired_read(Expired(), error, None)
    finally:
        st.close()


def test_a_base_re_commit_that_sent_its_root_is_never_a_retryable_conflict(monkeypatch):
    """R238L: ``_commit_base``'s replicate sends its root, then fails with exactly
    ``ProtoUnexpectedError`` after the permission expired.  Something a voter may accept left
    the node, so the outcome is unknown and the error is never turned into a retryable
    conflict -- whatever its type (not only the subclass ``replicate`` raises in practice)."""
    from proto_db import transactions
    from proto_db.collections.dictionaries import Dictionary
    h = Harness("abc", records=True)
    try:
        permission = _owner_permission(h)
        a = h.nodes["a"]
        monkeypatch.setattr(transactions, "_base_needs_commit", lambda coordinator, permission, current: True)
        monkeypatch.setattr(a, "commit_records", lambda label, root: ("persisted", None, None))
        monkeypatch.setattr(transactions, "_load_space", lambda context, record: transactions._new_space_root(
            Dictionary(context=context), context, None, None))
        cause = ProtoUnexpectedError("the link failed after the root was sent")

        def replicate(lease, record, base=None, tier=None):
            a._transport.send("b", {"type": "replicate", "from": "a", "planted": True})   # the root left
            h.clock.t = max(h.clock.t, permission.valid_until + 0.001)
            raise cause
        monkeypatch.setattr(a, "replicate", replicate)
        entry, sent = _round(h, "a", permission, lambda current, stamp: None, "bc")
        assert sent, "the stand-in replicate sent nothing"
        assert not isinstance(entry.error, ProtoConflictError), \
            f"a re-commit that sent its root became a retryable conflict: {entry.error!r}"
        assert permission.outcome == "unknown", permission.outcome
    finally:
        h.close()


# -- fix round 3 (re-review 2 of LEASE-READ; rulings R240L-R242L) ------------------------------

def _detached_holders(h, names="bc"):
    """``names`` hold a record on disk but run with no tier attached (a restart before attach,
    a closed space); returns the record."""
    r = rec(7)
    for n in names:
        h.nodes[n].tier.held.add((r.segment, r.offset))
    for n in names:
        h.nodes[n].detach(h.nodes[n].tier)
    return r


def test_a_member_without_storage_attached_is_never_a_miss():
    """R240L: ``b`` and ``c`` hold the record but have no tier attached.  They answer that they
    cannot tell, so the page request is not complete -- before, their ``data=None`` counted
    as "answered without the record" and an available record was called corrupt (re-review
    2, R2-A; probe ``rr2-1794900000/probe_unattached.py``)."""
    h = Harness("abc", records=True)
    try:
        h.bootstrap("a")
        a = h.nodes["a"]
        r = _detached_holders(h)

        def pump():
            h.net.deliver(src="a", dst={"b", "c"}, kinds=("fetch",))
            h.net.deliver(src=None, dst={"a"}, kinds=("fetched",))
        got = h._run(lambda: a.fetch_answer(AtomPointer(r.segment, r.offset)), pump)
        assert got == (None, False), f"members without storage counted as lacking the record: {got}"
    finally:
        h.close()


def test_a_commit_query_to_members_without_storage_counts_no_miss():
    """R240L, ``commit_info``: the same members answer that they cannot tell; the ask ends (they
    were heard) without counting them as members that do not know the commit."""
    h = Harness("abc", records=True)
    try:
        h.bootstrap("a")
        a = h.nodes["a"]
        r = _detached_holders(h)
        waiters = []
        ask = a._ask

        def recording(kind, msg_fields, done, timeout, targets=None):
            waiter = ask(kind, msg_fields, done, timeout, targets=targets)
            waiters.append(waiter)
            return waiter
        a._ask = recording

        def pump():
            h.net.deliver(src="a", dst={"b", "c"}, kinds=("commit_query",))
            h.net.deliver(src=None, dst={"a"}, kinds=("commit_info",))
        assert h._run(lambda: a._ask_commit(1, r), pump) == (False, None)
        (waiter,) = waiters
        assert not waiter.missing, f"members without storage counted as misses: {waiter.missing}"
        assert waiter.unknown == {"b", "c"}, waiter.unknown
    finally:
        h.close()


def _first_fetch_reply_lost(tc, node, reader):
    """``node``'s first ``fetched`` reply to ``reader`` is lost; later ones go through.  Returns
    a function restoring its transport."""
    send = tc[node]._transport.send
    lost = []

    def lossy(to, msg):
        if to == reader and msg["type"] == "fetched" and not lost:
            lost.append(msg)
            return
        send(to, msg)
    tc[node]._transport.send = lossy

    def restore():
        tc[node]._transport.send = send
    return restore, lost


def test_a_peer_answering_corrupt_bytes_does_not_end_the_page_request(tmp_path):
    """R242L: ``n0`` answers corrupt bytes and ``n1``'s first reply is lost; the read asks again
    the members not heard yet and gets the record from ``n1``.  Before, the corrupt answer
    ended the request, and with the store lacking the segment the read failed although
    ``n1`` holds the record."""
    with TieredCluster(tmp_path, **NOT_UPLOADED) as tc:
        tc.create_database("n0")
        _commit_without(tc, "n0", "n2")
        r = tc["n0"].committed_root()[1]
        st = tc.storages["n2"]
        n0_send = tc["n0"]._transport.send

        def corrupt(to, msg):
            if to == "n2" and msg["type"] == "fetched" and msg["data"] is not None:
                msg = dict(msg, data=b"\x00" * len(msg["data"]))
            n0_send(to, msg)
        tc["n0"]._transport.send = corrupt
        restore, lost = _first_fetch_reply_lost(tc, "n1", "n2")
        try:
            reads = st.stats()["reads_peer"]
            st._record(AtomPointer(r.segment, r.offset))     # raises when unavailable
        finally:
            restore()
            tc["n0"]._transport.send = n0_send
        assert lost, "the schedule did not run: n1 sent no reply"
        assert st.stats()["reads_peer"] == reads + 1, "the record did not come from a peer"


def test_the_unavailable_error_names_a_corrupt_answer(tmp_path):
    """R241L: when the only answers were corrupt bytes, the unavailable error says a peer may
    have answered corrupt bytes, not only that a peer was not heard."""
    with TieredCluster(tmp_path, **NOT_UPLOADED) as tc:
        tc.create_database("n0")
        _commit_without(tc, "n0", "n2")
        r = tc["n0"].committed_root()[1]
        restore = _corrupting(tc, "n2")
        try:
            with pytest.raises(ProtoUnexpectedError) as info:
                tc.storages["n2"].get_atom(AtomPointer(r.segment, r.offset))
        finally:
            restore()
        assert "corrupt bytes" in str(info.value), str(info.value)


# -- fix round 4 (re-review 3 of LEASE-READ; rulings R243L-R246L) ------------------------------

def _closing_while_serving(tc, nodes):
    """``nodes``' spaces close (``ObjectSpace.close``: detach, then the tier's close) after the
    node's page-request service took its tier and before it reads the record -- the window of
    re-review 3, R3-A, forced on the reader lane instead of left to timing.  Returns the list
    of nodes whose service ran the close."""
    closed = []
    for n in nodes:
        st, space = tc.storages[n], tc.spaces[n]
        read = st.local_record

        def closing(ptr, _n=n, _st=st, _space=space, _read=read):
            if _n not in closed:
                closed.append(_n)
                _space.close()
            return _read(ptr)
        st.local_record = closing
    return closed


def test_a_member_whose_space_closes_while_serving_a_fetch_cannot_tell(tmp_path):
    """R243L: ``n0`` and ``n1`` hold the record (its segment is not uploaded) and each closes its
    space between taking its tier and reading the record.  Their reply says they cannot tell,
    so ``n2``'s read is unavailable -- before, the closed tier's ``None`` went out as "I lack
    the record", and the read called an available record corrupt ("no reachable peer holds
    it"; probe ``rr3-1795000000/probe_closed_race.py``)."""
    with TieredCluster(tmp_path, **NOT_UPLOADED) as tc:
        tc.create_database("n0")
        _commit_without(tc, "n0", "n2")
        r = tc["n0"].committed_root()[1]
        ptr = AtomPointer(r.segment, r.offset)
        for n in ("n0", "n1"):
            assert tc.storages[n].local_record(ptr) is not None, f"{n} does not hold the record"
        closed = _closing_while_serving(tc, ("n0", "n1"))
        with pytest.raises(ProtoDBError) as info:
            tc.storages["n2"].get_atom(ptr)
        assert sorted(closed) == ["n0", "n1"], f"the schedule did not run on every holder: {closed}"
        assert type(info.value) is ProtoUnexpectedError, \
            f"members closing while serving made the read {type(info.value).__name__}: {info.value}"
        assert "unavailable" in str(info.value)


def _recording_waiters(node):
    """Wrap ``node._ask`` to keep every waiter it returns; returns the list."""
    waiters = []
    ask = node._ask

    def recording(kind, msg_fields, done, timeout, targets=None):
        waiter = ask(kind, msg_fields, done, timeout, targets=targets)
        waiters.append(waiter)
        return waiter
    node._ask = recording
    return waiters


def _commit_query_pump(h):
    def pump():
        h.net.deliver(src="a", dst={"b", "c"}, kinds=("commit_query",))
        h.net.deliver(src=None, dst={"a"}, kinds=("commit_info",))
    return pump


def test_a_commit_query_served_while_the_space_closes_counts_no_miss():
    """R243L, ``commit_info``: ``b`` and ``c`` detach their tier (a closing space) after their
    service took it and before it looked up the commit's ranges.  They reply that they cannot
    tell; before, the detached tier's ``None`` was a miss."""
    h = Harness("abc", records=True)
    try:
        h.bootstrap("a")
        a = h.nodes["a"]
        closed = []
        for n in "bc":
            node = h.nodes[n]
            tier = node.tier
            ranges = tier.commit_ranges

            def detaching(root, _n=n, _node=node, _tier=tier, _ranges=ranges):
                closed.append(_n)
                _node.detach(_tier)
                return _ranges(root)
            tier.commit_ranges = detaching
        waiters = _recording_waiters(a)
        assert h._run(lambda: a._ask_commit(1, rec(7)), _commit_query_pump(h)) == (False, None)
        assert sorted(closed) == ["b", "c"], f"the schedule did not run on every member: {closed}"
        (waiter,) = waiters
        assert not waiter.missing, f"members closing while serving counted as misses: {waiter.missing}"
        assert waiter.unknown == {"b", "c"}, waiter.unknown
    finally:
        h.close()


def test_a_commit_query_answered_while_attaching_counts_no_miss():
    """R244L (re-review 3, M3): ``b`` and ``c`` are attaching -- the tier is set, its roots are
    still loading and no persister runs yet -- and they no longer hold the commit's ranges.
    They cannot tell whether their persisted root covers the commit, so they reply unknown;
    before, they answered a miss."""
    import threading
    h = Harness("abc", records=True)
    try:
        h.bootstrap("a")
        a = h.nodes["a"]
        release, loading, attaching = threading.Event(), [], []
        for n in "bc":
            node = h.nodes[n]
            tier = node.tier
            node.detach(tier)
            load = tier.load_roots

            def blocked(_n=n, _load=load):
                loading.append(_n)
                assert release.wait(30.0), "the test never released the attach"
                return _load()
            tier.load_roots = blocked
            # The attach may refuse the harness's zero startup grace once the roots load; only
            # the window before that matters here.
            t = threading.Thread(target=_attempt, args=(node.attach, tier), daemon=True)
            t.start()
            attaching.append(t)
        for _ in range(30000):
            if len(loading) == 2:
                break
            threading.Event().wait(0.001)
        assert sorted(loading) == ["b", "c"], f"the attaches did not reach load_roots: {loading}"
        try:
            waiters = _recording_waiters(a)
            assert h._run(lambda: a._ask_commit(1, rec(7)), _commit_query_pump(h)) == (False, None)
        finally:
            release.set()
            for t in attaching:
                t.join(30.0)
        (waiter,) = waiters
        assert not waiter.missing, f"members answering while attaching counted as misses: {waiter.missing}"
        assert waiter.unknown == {"b", "c"}, waiter.unknown
    finally:
        h.close()


def _attempt(fn, *args):
    try:
        fn(*args)
    except ProtoDBError:
        pass


def _corrupt_from_b(h, r):
    """``b`` holds ``r`` and answers ``a``'s page request; ``c`` is never heard (slow or cut off)."""
    h.nodes["b"].tier.held.add((r.segment, r.offset))

    def pump():
        h.net.deliver(src="a", dst={"b"}, kinds=("fetch",))
        h.net.drop(src="a", dst={"c"}, kinds=("fetch",))
        h.net.deliver(src=None, dst={"a"}, kinds=("fetched",))
    return pump


def test_a_node_stopping_before_a_re_ask_reports_the_record_unavailable():
    """R244L (re-review 3, M1): ``b``'s answer is rejected as corrupt and ``a`` stops before it
    asks ``c`` again.  The request ends as not complete -- the reader's "unavailable" -- and
    never raises the stopped coordinator's ``ProtoValidationError``."""
    h = Harness("abc", records=True)
    try:
        h.bootstrap("a")
        a = h.nodes["a"]
        r = rec(7)

        def corrupt_and_stop(data):
            a.stop()
            return False
        got = h._run(lambda: a.fetch_answer(AtomPointer(r.segment, r.offset), valid=corrupt_and_stop),
                     _corrupt_from_b(h, r))
        assert got == (None, False), f"a stop before the re-ask surfaced as {got!r}"
    finally:
        h.close()


def test_a_re_ask_with_no_time_left_sends_nothing(monkeypatch):
    """R244L (re-review 3, M2): ``b``'s answer, rejected as corrupt, arrives once the round's wait
    has run out.  The request ends (not complete) without asking ``c`` again; before, it sent
    ``c`` a page request with a wait of 0 that nobody waited for."""
    import time as real_time
    from proto_db.cluster import coordinator as coordinator_module

    class Shifted:
        """``time`` with a monotonic clock the test moves forward (the coordinator's only)."""
        offset = 0.0

        def __getattr__(self, name):
            return getattr(real_time, name)

        def monotonic(self):
            return real_time.monotonic() + self.offset
    shifted = Shifted()
    h = Harness("abc", records=True)
    try:
        h.bootstrap("a")
        a = h.nodes["a"]
        r = rec(7)
        h.nodes["b"].tier.held.add((r.segment, r.offset))
        asks = _record_asks(a)
        monkeypatch.setattr(coordinator_module, "time", shifted)

        def pump():
            h.net.deliver(src="a", dst={"b"}, kinds=("fetch",))
            h.net.drop(src="a", dst={"c"}, kinds=("fetch",))
            replies = h.net.take(None, {"a"}, ("fetched",))
            if replies and not shifted.offset:
                shifted.offset = 3600.0     # the round's wait runs out before b's answer arrives
            for _, to, msg in replies:
                h.net.handlers[to](msg)
                h.nodes[to].wait_idle(5.0)
        got = h._run(lambda: a.fetch_answer(AtomPointer(r.segment, r.offset), valid=lambda data: False), pump)
        assert shifted.offset, "the schedule did not run: b sent no reply"
        assert got == (None, False), got
        assert [k for k, _ in asks] == ["fetch"], f"a re-ask with no time left was sent: {asks}"
    finally:
        monkeypatch.undo()
        h.close()


def test_a_plain_fetcher_answering_corrupt_bytes_is_not_a_definitive_miss(tmp_path):
    """R245L (re-review 3, M5): the tier's own check of a peer's answer.  A plain fetcher (no
    answer function) does not validate what it returns, so a corrupt answer reaches
    ``_peer_answer``'s decode: it is no record, and not an answer that the record is nowhere
    -- with the store lacking the segment, the read is unavailable, not corruption."""
    with TieredCluster(tmp_path, **NOT_UPLOADED) as tc:
        tc.create_database("n0")
        _commit_without(tc, "n0", "n2")
        r = tc["n0"].committed_root()[1]
        n2, st = tc["n2"], tc.storages["n2"]
        st.set_fetcher(lambda ptr: b"\x00" * 64)                 # a plain fetcher, corrupt bytes
        try:
            with pytest.raises(ProtoDBError) as info:
                st.get_atom(AtomPointer(r.segment, r.offset))
        finally:
            st.set_fetcher(n2.fetch_record, answer=n2.fetch_answer)
        assert type(info.value) is ProtoUnexpectedError, \
            f"a plain fetcher's corrupt answer made the read {type(info.value).__name__}: {info.value}"
