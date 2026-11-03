"""A failed write on the attached tier is never acknowledged, and the node stops acting as an
acceptor until the tier is replaced (8.3d re-review 9, B1 and its coverage notes; ruling R57).

* A failed acceptor-log write -- an acceptance in the worker, a grant's promise, a committed
  root learnt from a release -- is never acknowledged nor granted.  The tier is then *failed*:
  the node refuses every grant and replication (``"tier_failed"``), answers ``unknown``,
  ``acquire`` raises, ``healthy`` is False, and ``detach`` + ``attach`` of a healthy tier (the
  space reopened) brings it back.  Before, the failed append escaped to the worker loop,
  which only logged it, and the next acceptance through the poisoned log was acknowledged.
* The A1 refusal path (re-review 8) on a real :class:`~proto_db.storage.tiered.TieredStorage`:
  a worker paused at the ranges lock of its ``apply`` while the space closes and reopens is
  refused by the closed storage and treats it as "tier gone", not as a failed tier.
* ``_secure_chain``: a refused ``store_commit`` on a detached tier returns "the tier was
  detached"; on a still-attached tier it fails the tier instead of retrying forever.
"""
import errno
import os
import threading
import uuid

import pytest

from proto_db.errors import ProtoUnexpectedError, ProtoValidationError
from proto_db.storage.tiered import TieredStorage
from proto_db.storage.format import RecordKind, encode_record
from tests.support.cluster import MemoryTier
from tests.support.fake_object_store import FakeObjectStore
from tests.support.manual_cluster import GRACE, rec
from tests.unit.test_cluster_acceptor_writes import (_file_backed, _logged, _start_commit,  # noqa: F401
                                                     harness)

pytestmark = [pytest.mark.timeout(120)]

REAL_WRITE = os.write


def _sent(node):
    """Record every message ``node`` sends (still forwarded to the harness network)."""
    sent = []
    transport = node._transport
    orig = transport.send

    def send(to, msg):
        sent.append((to, msg))
        return orig(to, msg)

    transport.send = send
    return sent


def _fail_next_write(monkeypatch, log):
    """Make the next ``os.write`` through ``log``'s descriptor fail (``ENOSPC``)."""
    state = {"armed": True, "fired": False}

    def write(fd, data):
        if state["armed"] and fd == log._fd:
            state["armed"] = False
            state["fired"] = True
            raise OSError(errno.ENOSPC, "No space left on device (injected)")
        return REAL_WRITE(fd, data)

    monkeypatch.setattr(os, "write", write)
    return state


def _file_cluster(h, tmp_path):
    """c on a file-backed acceptor log, r0 committed by a with every node, c restarted with the
    safe grace; returns ``(r0, c)``."""
    _file_backed(h, "c", tmp_path / "c")
    r0 = h.bootstrap("a")
    h.restart("c")
    h.clock.t += 3 * GRACE
    return r0, h.nodes["c"]


def _assert_failed_acceptor(h, c):
    stats = c.stats()
    assert stats["tier_failed"] and not stats["healthy"] and not stats["fenced"] and not c.healthy
    with c._lock:
        assert c._refusal() == "tier_failed" and c._witness_state() == "unknown"
    sent = _sent(c)
    lease_b = h.attempt("b", {"c"}, timeout=0.3)
    if not isinstance(lease_b, Exception):
        h.release("b", lease_b, to=())
    replies = [m for to, m in sent if to == "b" and m["type"] in ("grant", "reject")]
    assert replies and all(m["type"] == "reject" and m["reason"] == "tier_failed" for m in replies), replies
    result = h.attempt("c", {"a", "b"})
    assert isinstance(result, ProtoUnexpectedError), result


def _reopen(h, c):
    tier = c.tier
    c.detach(tier)
    tier.close()
    c.attach(h.tier_class(h.disks["c"], "c"))
    stats = c.stats()
    assert not stats["tier_failed"] and stats["healthy"] and c.healthy


def test_a_failed_acceptance_write_is_refused_and_fails_the_tier(harness, tmp_path, monkeypatch):
    """c's acceptor-log append of R fails.  c refuses R (an ``ok=False`` ack, never ``ok=True``),
    publishes nothing, and its tier is failed: it refuses grants and the next replication,
    answers ``unknown`` and cannot acquire.  Reopened on its disk, it holds r0 and is healthy.
    Before, the failure only reached the worker loop's log (no answer at all), and the next
    acceptance through the poisoned log was acknowledged."""
    h = harness()
    r0, c = _file_cluster(h, tmp_path)
    lease_a = h.attempt("a", {"c"})
    assert not isinstance(lease_a, Exception), lease_a
    accepted = c.stats()["accepted"]
    sent = _sent(c)
    fault = _fail_next_write(monkeypatch, c.tier._log)
    result = h.replicate("a", lease_a, rec(2), {"c"}, expire=True)
    assert fault["fired"]
    assert isinstance(result, Exception), result
    acks = [m["ok"] for _, m in sent if m["type"] == "ack"]
    assert acks == [False], acks
    assert c.stats()["accepted"] == accepted
    assert c.accepted_root()[1] == r0 and _logged(c.tier) == r0
    monkeypatch.undo()
    h.release("a", lease_a, to={"c"})
    h.clock.t += 3 * GRACE
    lease_a = h.attempt("a", {"b"})
    assert not isinstance(lease_a, Exception), lease_a
    del sent[:]
    result = h.replicate("a", lease_a, rec(3), {"c"}, expire=True)    # refused without writing
    assert [m["ok"] for _, m in sent if m["type"] == "ack"] == [False]
    assert _logged(c.tier) == r0
    h.release("a", lease_a, to=())
    _assert_failed_acceptor(h, c)
    _reopen(h, c)
    assert _logged(c.tier) == r0 and c.accepted_root()[1] == r0


def test_a_failed_promise_write_withholds_the_grant_and_fails_the_tier(harness, tmp_path, monkeypatch):
    """c cannot make a grant's promise durable: the grant is not sent (never counted), and the
    tier is failed until it is replaced."""
    h = harness()
    r0, c = _file_cluster(h, tmp_path)
    sent = _sent(c)
    fault = _fail_next_write(monkeypatch, c.tier._log)
    result = h.attempt("b", {"c"}, timeout=0.5)
    assert fault["fired"]
    assert not any(m["type"] == "grant" for _, m in sent), sent
    monkeypatch.undo()
    if not isinstance(result, Exception):
        h.release("b", result, to=())
    _assert_failed_acceptor(h, c)
    _reopen(h, c)


def test_a_failed_committed_root_write_fails_the_tier(harness, tmp_path, monkeypatch):
    """c learns R committed from a's release and cannot log it: its tier is failed (it was
    only logged before, and c went on acting as an acceptor on a poisoned log)."""
    h = harness()
    r0, c = _file_cluster(h, tmp_path)
    lease_a = h.attempt("a", {"c"})
    assert not isinstance(lease_a, Exception), lease_a
    R = rec(2)
    assert isinstance(h.replicate("a", lease_a, R, {"c"}), float)
    fault = _fail_next_write(monkeypatch, c.tier._log)
    h.release("a", lease_a, to={"c"})
    assert c.wait_idle(5.0)
    assert fault["fired"]
    monkeypatch.undo()
    _assert_failed_acceptor(h, c)
    _reopen(h, c)
    assert _logged(c.tier) == R


# -- the A1 refusal path on a real TieredStorage ------------------------------------------------

class _PausedLock:
    """Wraps a lock: the thread named ``worker`` waits before acquiring it until ``gate`` is set."""

    def __init__(self, lock, worker):
        self._lock, self._worker = lock, worker
        self.waiting, self.gate = threading.Event(), threading.Event()

    def __enter__(self):
        if threading.current_thread().name == self._worker and not self.gate.is_set():
            self.waiting.set()
            assert self.gate.wait(20)
        self._lock.acquire()
        return self

    def __exit__(self, *exc):
        self._lock.release()
        return False


def test_a1_a_detach_waits_for_an_acceptance_in_flight_on_a_tiered_storage(harness, tmp_path):
    """Re-review 9 coverage note, adapted to Task 8.3f phase B: the A1 path on a real
    ``TieredStorage``.  An acceptance (its kind-5 entry and the data writes it covers) runs
    under the coordinator's acceptor lock, so a detach that races it waits until it has
    completed on the still-attached tier: c's worker, paused inside the acceptance (before the
    ranges lock), acknowledges once released, and only then is the tier detached.  The closed
    storage is never written again (no file changes after ``close()``), c stays healthy, and the
    root is in the closed storage's acceptor log."""
    h = harness()
    store = FakeObjectStore()
    path = str(tmp_path / "c")

    def tier_class(disk, name):
        if name != "c":
            return MemoryTier(disk, name)
        return TieredStorage(path, store, durability="fsync", ship_interval=0.0, backoff=(0.0, 0.0), retries=0)

    old = h.nodes["c"]
    old.stop()
    h.retired.append(old)
    h.tier_class = tier_class
    h.nodes["c"] = h._node("c", 0.0)
    assert h.rejoin("c")
    h.bootstrap("a")
    t0 = h.nodes["c"].tier
    h.nodes["c"].detach(t0)
    t0.close()
    h.restart("c")                      # a reattach of a non-empty log needs the safe grace
    h.clock.t += 3 * GRACE
    lease_a = h.attempt("a", {"c"})
    assert not isinstance(lease_a, Exception), lease_a
    a, c = h.nodes["a"], h.nodes["c"]
    a.tier.outgoing = lambda: [[uuid.uuid4().bytes, 64, encode_record(RecordKind.BLOB, b"record")]]
    t1 = c.tier
    paused = _PausedLock(t1._ranges_lock, "proto-cluster-worker-c")
    t1._ranges_lock = paused
    R = rec(2)
    t, out = _start_commit(h, lease_a, R)
    detached = threading.Event()

    def detach():
        c.detach(t1)
        detached.set()

    d = threading.Thread(target=detach)
    try:
        assert paused.waiting.wait(10), "c's worker never stored the records"
        d.start()
        assert not detached.wait(0.3), "a detach did not wait for the acceptance in flight"
    finally:
        paused.gate.set()
    d.join(10)
    assert detached.is_set()
    t1.close()
    before = _snapshot(path)
    assert c.wait_idle(5.0)
    acks = [m["ok"] for _, _, m in h.net.take("c", {"a"}, ("ack",))]
    assert acks == [True], acks
    stats = c.stats()
    assert c.healthy and not stats["fenced"] and not stats["tier_failed"]
    a.stop()
    t.join(10.0)
    h.retired.append(a)
    assert _snapshot(path) == before, "a write after close() changed a file"
    assert t1.load_roots().accepted[1] == R


def _snapshot(root) -> dict:
    out = {}
    for directory, _, names in os.walk(root):
        for name in names:
            full = os.path.join(directory, name)
            with open(full, "rb") as f:
                out[os.path.relpath(full, root)] = f.read()
    return out


# -- _secure_chain ----------------------------------------------------------------------------------

@pytest.mark.parametrize("detached", [True, False])
def test_secure_chain_tells_a_detached_tier_from_a_failed_one(harness, monkeypatch, detached):
    """Re-review 9 coverage note.  The rejoin stores the records of the adopted root and
    ``store_commit`` is refused.  On a tier detached meanwhile (``detached``) the chain returns
    "the tier was detached", the rejoin records nothing, and the node is healthy.  On a tier
    still attached, the tier is failed and the reason returned, instead of the error escaping
    to the rejoin loop, which retried it forever on a poisoned storage."""
    h = harness(records=True)
    c = h.nodes["c"]
    tier = c.tier
    root = rec(5)
    monkeypatch.setattr(c, "commit_records", lambda label, r: ("records", [], []))

    def refused(records, r, label, ranges):
        if detached:
            c.detach(tier)
            raise ProtoValidationError("Storage is closed: storing replicated records refused")
        raise ProtoUnexpectedError("Storing replicated records failed: [Errno 5] Input/output error")

    tier.store_commit = refused
    reason = c._secure_chain(tier, 5 << 12, root)
    if detached:
        assert reason == "the tier was detached"
        assert c.tier is None and c.healthy and not c.stats()["tier_failed"]
        assert c._block_rejoin(tier, reason) is False and c.stats()["rejoin_blocked"] is None
    else:
        assert reason is not None and "the tier failed" in reason
        assert c.stats()["tier_failed"] and not c.healthy
        with c._lock:
            assert c._witness_state() == "unknown"


def test_r2_an_acceptor_reopened_after_a_torn_append_keeps_what_it_acknowledges_and_promises(harness, tmp_path,
                                                                                            monkeypatch):
    """Re-review 10, R2 (ruling R58): c's acceptance write tears (9 bytes, then ``ENOSPC``), its
    tier fails, and the documented recovery (detach, close, reopen the space on its disk)
    brings it back.  c then grants a new lease and acknowledges R3.  After a restart its log
    holds R3 and the new promise.  On 0f9bf1e the reopened log appended after the torn bytes:
    the reader stopped at them, and after the restart c's log held r0 and the old promise --
    an acknowledged acceptance and a sent promise lost with no disk lost."""
    h = harness()
    r0, c = _file_cluster(h, tmp_path)
    lease_a = h.attempt("a", {"c"})
    assert not isinstance(lease_a, Exception), lease_a
    first_promise = c.tier.load_roots().promised
    log = c.tier._log
    state = {"armed": True}

    def write(fd, data):
        if state["armed"] and fd == log._fd:
            state["armed"] = False
            REAL_WRITE(fd, bytes(data)[:9])          # a torn entry
            raise OSError(errno.ENOSPC, "No space left on device (injected)")
        return REAL_WRITE(fd, data)

    monkeypatch.setattr(os, "write", write)
    h.replicate("a", lease_a, rec(2), {"c"}, expire=True)
    monkeypatch.undo()
    assert not state["armed"] and c.stats()["tier_failed"]
    h.release("a", lease_a, to={"c"})
    _reopen(h, c)
    h.clock.t += 3 * GRACE
    lease_a = h.attempt("a", {"c"})
    assert not isinstance(lease_a, Exception), lease_a
    promised = c.tier.load_roots().promised
    assert promised > first_promise
    sent = _sent(c)
    R3 = rec(3)
    h.replicate("a", lease_a, R3, {"c"})
    acks = [m["ok"] for _, m in sent if m["type"] == "ack"]
    assert acks == [True], acks
    assert _logged(c.tier) == R3, "c acknowledged R3 but its own log does not return it"
    h.release("a", lease_a, to=())
    tier = c.tier
    c.detach(tier)
    tier.close()
    restarted = h.tier_class(h.disks["c"], "c")
    roots = restarted.load_roots()
    assert roots.accepted[1] == R3, "the acknowledged R3 is missing from c's log after a restart"
    assert roots.promised == promised, "the promise c sent is missing from its log after a restart"
    restarted.close()
