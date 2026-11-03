"""Acceptor-log writes go only to the log that is attached when they are made (8.3d re-review
7, N1 and N4; ruling R55).

An acceptance is decided on the tier its records went to, logged on that tier while it is
still attached (no ``detach()``/``attach()`` can run between the check and the append), and
only then published in memory; the acknowledgement follows.  A closed acceptor-log handle
refuses writes on every backend, the in-memory one included.

* N1, the reviewer's ``probe_inflight_accept_reattach``: an acceptance in flight in c's worker
  while c's space is closed and reopened over the same disk on the running coordinator.
  ``after``: the worker is logging the root when the space closes; ``before``: the worker has
  stored the records on the old tier and not yet decided.  Before the fix, ``after`` over an
  in-memory log acknowledged R while c's memory, reset by the attach, forgot it, and b's next
  lease lost the acknowledged commit; ``before`` over a file log left memory ahead of the log.
* The same two windows over a *moved* disk (a copy of the detached disk attached as another
  storage object, the old handle still open): they separate the two halves of the fix, the
  tier-identity check and its atomicity with the append.
* A log closed while still attached (worker and holder): nothing is acknowledged and memory
  stays equal to the log.
* N4: a stale worker does not refill the stored-records set after the reattach.
* A1 (re-review 8; ruling R56): a tier refuses writes once closed, so a worker whose tier was
  closed under its ``apply`` treats the refusal as "tier gone": it refuses the replication
  (never acknowledges it), stays healthy, and does not count the records as stored.
"""
import os
import threading
import time
import uuid

import pytest

from proto_db.cluster import coordinator as coordinator_module
from proto_db.cluster.rootlog import memory_log_for, root_key
from proto_db.errors import ProtoDBError, ProtoLockingError, ProtoOutcomeUnknownError, ProtoValidationError
from proto_db.storage.memory import MemoryStorage
from tests.support.cluster import MemoryTier
from tests.support.manual_cluster import GRACE, Harness, rec

pytestmark = [pytest.mark.timeout(120)]


class DirStorage(MemoryStorage):
    """A ``MemoryStorage`` with a directory: its ``PlainTier`` keeps a real ``RootsLog`` file."""

    def __init__(self, path):
        super().__init__()
        self.path = str(path)
        self.durability = "fsync"


@pytest.fixture
def harness():
    made = []

    def make(names="abc", **kw):
        h = Harness(names, **kw)
        made.append(h)
        return h

    yield make
    for h in made:
        h.close()


def _file_backed(h, name, path):
    """Give ``name`` a file-backed acceptor log (a fresh one, rejoined at the first start)."""
    old = h.nodes[name]
    old.stop()
    h.retired.append(old)
    os.makedirs(path)
    h.disks[name] = DirStorage(path)
    h.nodes[name] = h._node(name, 0.0)
    assert h.rejoin(name)


def _setup(h, kind, tmp_path):
    """r0 committed everywhere; c restarted with the safe grace (a reattach of a non-empty log
    needs it); a holds a lease granted by c.  Returns ``(r0, lease_a)``."""
    if kind == "file":
        _file_backed(h, "c", tmp_path / "c")
    r0 = h.bootstrap("a")
    h.restart("c")
    h.clock.t += 3 * GRACE
    lease_a = h.attempt("a", {"c"})
    assert not isinstance(lease_a, Exception), lease_a
    return r0, lease_a


def _replicate_in_background(node, lease, root):
    out = {}

    def run():
        try:
            out["r"] = node.replicate(lease, root)
        except ProtoDBError as e:   # the outcome under test
            out["r"] = e

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return t, out


def _start_commit(h, lease, root):
    """a replicates ``root``; its copy to b is dropped, its copy to c is handed to c's worker."""
    t, out = _replicate_in_background(h.nodes["a"], lease, root)
    deadline = time.monotonic() + 10.0
    handed = False
    while not handed:
        assert time.monotonic() < deadline, "a never sent its replication"
        h.net.drop(src="a", dst={"b"}, kinds=("records", "replicate"))
        for _, to, msg in h.net.take("a", {"c"}, ("records", "replicate")):
            h.net.handlers[to](msg)
            handed = handed or msg["type"] == "replicate"
        time.sleep(0.001)
    return t, out


def _pause_log_write(tier, worker):
    """Pause ``worker`` inside its next ``accepted`` append on ``tier``; ``(in_window, gate)``."""
    in_window, gate = threading.Event(), threading.Event()
    orig = tier.log_root

    def paused(kind, label, record):
        if kind == "accepted" and threading.current_thread().name == worker:
            in_window.set()
            gate.wait(20)
        return orig(kind, label, record)

    tier.log_root = paused
    return in_window, gate


def _pause_before_decision(monkeypatch, worker):
    """Pause ``worker`` in ``_apply_replicate`` after it stored the records on the tier it
    captured and before it decides (the decode of the replication's empty ``base``)."""
    in_window, gate = threading.Event(), threading.Event()
    orig = coordinator_module.decode_labeled_root

    def paused(value):
        if value is None and threading.current_thread().name == worker and not gate.is_set():
            in_window.set()
            gate.wait(20)
        return orig(value)

    monkeypatch.setattr(coordinator_module, "decode_labeled_root", paused)
    return in_window, gate


class StoringTier(MemoryTier):
    """A :class:`MemoryTier` whose ``apply`` stores the records it receives and refuses once
    the tier is closed, checked under the lock ``close()`` takes, as ``TieredStorage`` does
    (ruling R56)."""

    def __init__(self, storage, node_id):
        super().__init__(storage, node_id)
        self._store_lock = threading.Lock()
        self._closed = False
        self.applied = []

    def apply(self, records, root=None, label=0):
        with self._store_lock:
            if self._closed:
                raise ProtoValidationError("The tier is closed: storing replicated records refused")
            self.applied.append((label, root))

    def close(self):
        with self._store_lock:
            self._closed = True
        super().close()


def _acks(h):
    return [m["ok"] for _, _, m in h.net.take("c", {"a"}, ("ack",))]


def _logged(tier):
    accepted = tier.load_roots().accepted
    return None if accepted is None else accepted[1]


def _moved_disk(old_tier, name):
    """A copy of ``old_tier``'s (detached) in-memory acceptor log, as another storage object."""
    disk = MemoryStorage()
    roots = old_tier.load_roots()
    log = memory_log_for(disk, name)
    if roots.accepted is not None:
        log.append("accepted", *roots.accepted)
    if roots.committed is not None:
        log.append("committed", *roots.committed)
    if roots.promised:
        log.promise(roots.promised)
    if not roots.empty:
        log.mark_rejoined()
    return disk


@pytest.mark.parametrize("kind", ["memory", "file"])
def test_n1_an_acceptance_being_logged_finishes_before_the_space_closes(harness, tmp_path, kind):
    """``probe_inflight_accept_reattach after``.  c's worker is logging R when c's space is
    closed and reopened over the same disk.  The detach waits until the append is done, so the
    reattached log holds R, c's memory holds R, the acknowledgement went out for a logged root,
    and once a crashes before its release, b's lease from {b, c} builds on R.  Before the fix,
    over an in-memory log, the attach reset c's memory to R0 while the old handle appended R and
    the ack went out: b's base was R0, an acknowledged commit lost with no disk lost."""
    h = harness()
    r0, lease_a = _setup(h, kind, tmp_path)
    a, c = h.nodes["a"], h.nodes["c"]
    t1 = c.tier
    in_window, gate = _pause_log_write(t1, "proto-cluster-worker-c")
    R = rec(2)
    t, out = _start_commit(h, lease_a, R)

    def reopen():
        c.detach(t1)
        t1.close()
        c.attach(h.tier_class(h.disks["c"], "c"))

    reopened = threading.Thread(target=reopen, daemon=True)
    try:
        assert in_window.wait(10), "c's worker never logged R"
        reopened.start()
        reopened.join(0.5)
        assert reopened.is_alive(), "the space closed while an acceptance was being logged"
    finally:
        gate.set()
    reopened.join(10.0)
    assert not reopened.is_alive()
    assert c.wait_idle(5.0)
    acks = h.net.take("c", {"a"}, ("ack",))
    assert [m["ok"] for _, _, m in acks] == [True]
    for _, to, msg in acks:
        h.net.handlers[to](msg)
    t.join(10.0)
    assert isinstance(out.get("r"), float), out          # R committed with a + c
    assert c.tier is not t1
    assert _logged(c.tier) == R, "acknowledged, but not in the attached log"
    assert c.accepted_root()[1] == R, "c's memory forgot a root it acknowledged"
    h.net.drop(src="a", kinds=("release",))               # a crashes before its release
    a.stop()
    h.retired.append(a)
    h.clock.t += GRACE + 0.5
    lease_b = h.attempt("b", {"c"})
    assert not isinstance(lease_b, Exception), lease_b
    base, _ = h.base("b", lease_b, responders={"c"})
    assert base == R and base != r0, "an acknowledged commit was lost"


@pytest.mark.parametrize("kind", ["memory", "file"])
def test_n1_an_acceptance_decided_after_a_reattach_is_refused(harness, tmp_path, monkeypatch, kind):
    """``probe_inflight_accept_reattach before``.  c's worker stored R's records on the old
    tier, then c's space is closed and reopened over the same disk before the worker decides.
    The worker refuses (its tier is no longer attached): a refusal goes out, nothing is logged,
    memory equals the attached log, and the stored-records set is not refilled (N4).  Before
    the fix the decision was judged against the new tier, and over a file log memory was left
    ahead of the log (R in memory, R0 logged; the append went to the closed handle)."""
    h = harness()
    r0, lease_a = _setup(h, kind, tmp_path)
    a, c = h.nodes["a"], h.nodes["c"]
    t1 = c.tier
    in_window, gate = _pause_before_decision(monkeypatch, "proto-cluster-worker-c")
    R = rec(2)
    t, out = _start_commit(h, lease_a, R)
    try:
        assert in_window.wait(10), "c's worker never reached its decision"
        c.detach(t1)
        t1.close()
        c.attach(h.tier_class(h.disks["c"], "c"))
    finally:
        gate.set()
    assert c.wait_idle(5.0)
    assert _acks(h) == [False], "a replication decided on a detached tier was acknowledged"
    assert _logged(c.tier) == r0
    assert c.accepted_root()[1] == r0, "c's memory is ahead of its log"
    with c._lock:
        assert root_key(lease_a.term, R) not in c._stored, "a stale worker refilled the stored records"
    a.stop()                                              # wakes the holder, which never got a majority
    t.join(10.0)
    h.retired.append(a)
    assert isinstance(out.get("r"), ProtoOutcomeUnknownError), out


def test_n1_a_decision_after_a_move_to_another_disk_writes_nothing_on_the_old_one(harness, tmp_path, monkeypatch):
    """The tier-identity half of the fix.  As above, but the space is reopened over a copy of
    the detached disk (another storage object) and the old handle is still open.  Without the
    identity check the worker accepted on the new tier's state, appended R to the old, detached
    log and acknowledged: c's attached log lacked a root c acknowledged."""
    h = harness()
    r0, lease_a = _setup(h, "memory", tmp_path)
    c = h.nodes["c"]
    t1 = c.tier
    in_window, gate = _pause_before_decision(monkeypatch, "proto-cluster-worker-c")
    t, out = _start_commit(h, lease_a, rec(2))
    try:
        assert in_window.wait(10)
        c.detach(t1)
        h.disks["c"] = _moved_disk(t1, "c")
        c.attach(h.tier_class(h.disks["c"], "c"))
    finally:
        gate.set()
    assert c.wait_idle(5.0)
    assert _acks(h) == [False]
    assert _logged(t1) == r0, "the worker wrote on a detached log"
    assert _logged(c.tier) == r0 and c.accepted_root()[1] == r0
    t1.close()
    h.nodes["a"].stop()
    t.join(10.0)


def test_n1_a_move_to_another_disk_waits_for_the_acceptance_being_logged(harness, tmp_path):
    """The atomicity half of the fix.  c's worker is logging R when the space is moved to a
    copy of its disk (the old handle stays open).  The detach waits for the append, so the copy
    holds R.  With a window between the check and the append, the copy was taken without R, the
    worker appended R to the old log, published it and acknowledged: memory ahead of the
    attached log, and a root acknowledged that the attached log lacks."""
    h = harness()
    r0, lease_a = _setup(h, "memory", tmp_path)
    c = h.nodes["c"]
    t1 = c.tier
    in_window, gate = _pause_log_write(t1, "proto-cluster-worker-c")
    R = rec(2)
    t, out = _start_commit(h, lease_a, R)

    def move():
        c.detach(t1)
        h.disks["c"] = _moved_disk(t1, "c")
        c.attach(h.tier_class(h.disks["c"], "c"))

    moved = threading.Thread(target=move, daemon=True)
    try:
        assert in_window.wait(10)
        moved.start()
        moved.join(0.5)
        assert moved.is_alive(), "the space moved while an acceptance was being logged"
    finally:
        gate.set()
    moved.join(10.0)
    assert not moved.is_alive()
    assert c.wait_idle(5.0)
    acks = _acks(h)
    assert acks == [True]
    assert _logged(c.tier) == R, "acknowledged, but not in the attached log"
    assert c.accepted_root()[1] == R
    t1.close()
    h.nodes["a"].stop()
    t.join(10.0)


def test_n1_an_acceptor_whose_log_was_closed_under_it_acknowledges_nothing(harness):
    """A closed acceptor log refuses the append (every backend), so the worker sends no
    acknowledgement, and memory, published only after the append, stays equal to the log."""
    h = harness()
    r0 = h.bootstrap("a")
    h.clock.t += 3 * GRACE
    lease_a = h.attempt("a", {"c"})
    assert not isinstance(lease_a, Exception), lease_a
    c = h.nodes["c"]
    c.tier.close()                                         # closed, still attached
    accepted = c.stats()["accepted"]
    R = rec(2)
    result = h.replicate("a", lease_a, R, {"c"}, expire=True)
    assert isinstance(result, ProtoOutcomeUnknownError), result
    assert c.stats()["accepted"] == accepted
    assert c.accepted_root()[1] == r0, "memory ahead of a log that refused the root"


def test_n1_a_holder_whose_log_was_closed_under_it_does_not_count_itself(harness):
    """The holder's own acceptance follows the same rule: its log refuses the append, so it
    is not counted, and its memory does not adopt the root."""
    h = harness()
    r0 = h.bootstrap("a")
    h.clock.t += 3 * GRACE
    lease_a = h.attempt("a", {"c"})
    assert not isinstance(lease_a, Exception), lease_a
    a = h.nodes["a"]
    a.tier.close()                                         # closed, still attached
    R = rec(2)
    result = h.replicate("a", lease_a, R, {"b", "c"})
    assert isinstance(result, ProtoOutcomeUnknownError), result
    assert a.accepted_root()[1] == r0, "memory ahead of a log that refused the root"


def test_n1_a_holder_moved_to_another_disk_mid_commit_does_not_accept_on_the_old_one(harness):
    """The holder's self-acceptance is decided on the tier its commit started on.  a's space
    moves to a copy of its disk between its broadcast and its own acceptance (the old handle
    still open): a does not accept, writes nothing on the old log, and is not counted."""
    h = harness()
    r0 = h.bootstrap("a")
    h.restart("a")                                         # a reattach of a non-empty log needs the safe grace
    h.clock.t += 3 * GRACE
    a = h.nodes["a"]
    lease_a = h.attempt("a", {"c"})
    assert not isinstance(lease_a, Exception), lease_a
    t1 = a.tier
    gate, sent = threading.Event(), threading.Event()
    orig = a._broadcast

    def paused(msg):
        orig(msg)
        if msg["type"] == "replicate":
            sent.set()
            gate.wait(20)

    a._broadcast = paused
    R = rec(2)
    t, out = _replicate_in_background(a, lease_a, R)
    try:
        assert sent.wait(10)
        a.detach(t1)
        h.disks["a"] = _moved_disk(t1, "a")
        a.attach(h.tier_class(h.disks["a"], "a"))
    finally:
        gate.set()
    h.net.drop(src="a", kinds=("records", "replicate"))
    deadline = time.monotonic() + 5.0
    while t.is_alive() and time.monotonic() < deadline:
        h.clock.t = max(h.clock.t, lease_a.valid_until + 0.001)    # nobody else answers
        t.join(0.01)
    assert isinstance(out.get("r"), ProtoOutcomeUnknownError), out
    assert _logged(t1) == r0, "the holder wrote on a detached log"
    assert _logged(a.tier) == r0 and a.accepted_root()[1] == r0
    t1.close()


@pytest.mark.parametrize("kind", ["memory", "file"])
def test_a_closed_acceptor_log_handle_refuses_writes_on_every_backend(tmp_path, kind):
    """Every backend's acceptor-log handle refuses writes once closed (``ProtoValidationError``);
    another handle on the same log (the space reopened) is unaffected and sees what was
    written.  The in-memory log used to ignore ``close()``.  A file log admits one live handle
    (ruling R59), so there the second handle is opened after the first is closed."""
    disk = MemoryStorage() if kind == "memory" else DirStorage(tmp_path)
    one = MemoryTier(disk, "n")
    two = MemoryTier(disk, "n") if kind == "memory" else None
    r1, r2 = rec(1), rec(2)
    one.log_root("accepted", 1, r1)
    if kind == "file":
        with pytest.raises(ProtoLockingError):
            MemoryTier(disk, "n")
    else:
        assert _logged(two) == r1
    one.close()
    if two is None:
        two = MemoryTier(disk, "n")
        assert _logged(two) == r1
    one.close()                                            # idempotent
    with pytest.raises(ProtoValidationError):
        one.log_root("accepted", 2, r2)
    with pytest.raises(ProtoValidationError):
        one.log_root("committed", 2, r2)
    with pytest.raises(ProtoValidationError):
        one.log_promise(9)
    with pytest.raises(ProtoValidationError):
        one.log_rejoined()
    two.log_root("accepted", 2, r2)
    two.log_promise(9)
    if kind == "file":
        two.close()
    third = MemoryTier(disk, "n")
    roots = third.load_roots()
    assert roots.accepted[1] == r2 and roots.promised == 9
    third.close()
    two.close()


def test_a_commit_whose_space_was_reopened_replicates_nothing(harness):
    """Round 8 audit of captured tiers: a commit passes the tier its records were written to.
    Once that tier is no longer attached (the space closed, or closed and reopened, while the
    commit ran), ``replicate`` sends nothing and says so, instead of replicating a root whose
    records are on another tier."""
    h = harness()
    h.bootstrap("a")
    h.restart("a")
    h.clock.t += 3 * GRACE
    a = h.nodes["a"]
    lease_a = h.attempt("a", {"c"})
    assert not isinstance(lease_a, Exception), lease_a
    t1 = a.tier
    a.detach(t1)
    t1.close()
    a.attach(h.tier_class(h.disks["a"], "a"))
    before = a.stats()["replications"]
    sent = []

    def refuse_to_send(msg):
        # Fails the test at once (an AssertionError is not a ProtoDBError: replicate lets it
        # through) instead of letting a commit that ignored its tier wait for acknowledgements
        # on the fake clock until the test's timeout (re-review 8, M1).
        sent.append(msg["type"])
        raise AssertionError(f"replicate sent a {msg['type']!r} message for a commit whose tier was closed")

    a._broadcast = refuse_to_send
    with pytest.raises(ProtoValidationError, match="nothing was replicated"):
        a.replicate(lease_a, rec(2), tier=t1)
    assert sent == []
    assert a.stats()["replications"] == before
    assert not h.net.take("a", None, ("records", "replicate"))


def test_a1_a_worker_whose_tier_closed_under_its_apply_refuses_and_stays_healthy(harness, tmp_path):
    """Re-review 8, A1.  c's worker is about to store a replication's records on its tier when
    c's space is closed and reopened over the same disk.  The closed tier refuses the write
    (``ProtoValidationError``, as every closed storage does), and the worker treats that as
    "tier gone": it sends a refusal, so the holder need not wait for c; nothing is logged, c
    stays healthy (a closed space is no fault of the node), and the records are not counted as
    stored.  Before, the refusal escaped to the worker loop: no answer at all, and an error
    logged as if the node had failed."""
    h = harness()
    h.tier_class = StoringTier                            # c restarts on a storing tier
    r0, lease_a = _setup(h, "memory", tmp_path)
    a, c = h.nodes["a"], h.nodes["c"]
    a.tier.outgoing = lambda: [[uuid.uuid4().bytes, 64, b"record"]]
    t1 = c.tier
    in_window, gate = threading.Event(), threading.Event()
    apply = t1.apply

    def paused(records, root=None, label=0):
        if threading.current_thread().name == "proto-cluster-worker-c":
            in_window.set()
            assert gate.wait(20)
        return apply(records, root, label)

    t1.apply = paused
    R = rec(2)
    t, out = _start_commit(h, lease_a, R)
    try:
        assert in_window.wait(10), "c's worker never stored the records"
        c.detach(t1)
        t1.close()
        c.attach(h.tier_class(h.disks["c"], "c"))
    finally:
        gate.set()
    assert c.wait_idle(5.0)
    assert _acks(h) == [False], "a replication whose records the closed tier refused was not refused"
    assert t1.applied == [], "the closed tier stored records"
    assert c.healthy and not c.stats()["fenced"]
    assert _logged(c.tier) == r0 and c.accepted_root()[1] == r0
    with c._lock:
        assert root_key(lease_a.term, R) not in c._stored
    a.stop()
    t.join(10.0)
    h.retired.append(a)
    assert isinstance(out.get("r"), ProtoOutcomeUnknownError), out
