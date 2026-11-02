"""No storage writes anything once ``close()`` has started (8.3d re-review 8, A1; ruling R56).

A thread that captured a storage before it closed (a cluster worker applying a replication,
a holder recording a commit, the persister, a user thread) must never write through it
afterwards: a closed descriptor's number is handed to the next file the process opens, so a
stale ``os.write`` lands in another file -- another space's acceptor log, say -- and a path
write lands in a directory that a reopened storage now owns.  Every write checks the closed
flag under the lock that ``close()`` takes before it releases what the write uses, and a
refused write raises ``ProtoValidationError``.

* The reviewer's ``probe_stale_fd_apply``: an ``apply`` paused after its open check, outside
  every lock, while the storage closes and the descriptor number is reused (by a file of the
  test, deterministically, or by another storage's ``roots.log`` as in the probe's ``other2``).
  Before the fix the stale ``ranges.log`` entry landed in that file and, in ``other2``, the
  other storage's acceptor log lost an accepted root on reload.
* One test per guarded write, each paused *at* its lock (a wrapper on the lock pauses the
  writer before it acquires it): the closed check must be made under the lock, not before it.
"""
import os
import threading
import uuid

import pytest

from proto_db.cluster.rootlog import root_key
from proto_db.errors import ProtoValidationError
from proto_db.storage import tiered as tiered_module
from proto_db.storage.cloud import CloudStorage
from proto_db.storage.file import FileStorage
from proto_db.storage.format import FORMAT_VERSION, RootRecord
from proto_db.storage.tiered import TieredStorage
from tests.support.fake_object_store import FakeObjectStore

pytestmark = [pytest.mark.timeout(60)]

WRITER = "proto-test-stale-writer"


class _GatedLock:
    """Wraps a lock: the :data:`WRITER` thread waits at its ``skip + 1``-th acquisition until
    :attr:`gate` is set, before acquiring the lock (``inside`` False) or right after it
    (``inside`` True, holding it); every other thread passes straight through."""

    def __init__(self, lock, skip: int = 0, inside: bool = False):
        self._lock = lock
        self._skip = skip
        self._inside = inside
        self.waiting = threading.Event()
        self.gate = threading.Event()

    def _pause(self):
        if threading.current_thread().name == WRITER and not self.gate.is_set():
            if self._skip:
                self._skip -= 1
            else:
                self.waiting.set()
                assert self.gate.wait(20), "the test never released the writer"

    def acquire(self, *args, **kwargs):
        if not self._inside:
            self._pause()
        acquired = self._lock.acquire(*args, **kwargs)
        if self._inside:
            self._pause()
        return acquired

    def release(self):
        self._lock.release()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *exc):
        self.release()
        return False


def _gate(obj, attr: str, skip: int = 0, inside: bool = False) -> _GatedLock:
    # A storage without that lock (before the fix) gets a fresh one that nothing takes: the
    # writer then never waits, and the test fails on its ``waiting`` assertion.
    gated = _GatedLock(getattr(obj, attr, None) or threading.Lock(), skip, inside)
    setattr(obj, attr, gated)
    return gated


def _stale(op):
    """Run ``op`` on the :data:`WRITER` thread; ``(thread, out)`` with ``out["error"]`` or
    ``out["result"]``."""
    out = {}

    def run():
        try:
            out["result"] = op()
        except Exception as e:      # the outcome under test
            out["error"] = e

    t = threading.Thread(target=run, name=WRITER, daemon=True)
    t.start()
    return t, out


def _files(root) -> dict:
    """Every file under ``root`` with its bytes."""
    out = {}
    for directory, _, names in os.walk(str(root)):
        for name in names:
            path = os.path.join(directory, name)
            with open(path, "rb") as f:
                out[os.path.relpath(path, str(root))] = f.read()
    return out


def _claim(number: int, path: str) -> int:
    """Open ``path`` so that it gets descriptor ``number`` (just closed), as the next ``open``
    anywhere in the process would: descriptors are allocated lowest first."""
    held = []
    try:
        while True:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
            if fd == number:
                return fd
            held.append(fd)
            assert fd < number, f"descriptor {number} was taken meanwhile"
    finally:
        for fd in held:
            os.close(fd)


def _tiered(tmp_path, store, name, **kw):
    kw.setdefault("durability", "fsync")
    kw.setdefault("ship_interval", 0.0)
    kw.setdefault("backoff", (0.0, 0.0))
    kw.setdefault("retries", 1)
    return TieredStorage(str(tmp_path / name), store, **kw)


def _replication(tmp_path, store):
    """Three records written by node ``w`` and the root on the last: ``(writer, records, root)``."""
    writer = _tiered(tmp_path, store, "w")
    ptrs = [writer.push_atom({"i": i}) for i in range(3)]
    root = RootRecord(FORMAT_VERSION, ptrs[2].segment, ptrs[2].offset, 3)
    return writer, writer.outgoing(), root


def _refused(t, out):
    t.join(10)
    assert not t.is_alive()
    assert isinstance(out.get("error"), ProtoValidationError), out


# -- the reviewer's probe ----------------------------------------------------------------------

@pytest.mark.parametrize("variant", ["reused", "other2"])
def test_a1_an_apply_paused_across_close_writes_through_no_reused_descriptor(tmp_path, variant):
    """``probe_stale_fd_apply``.  The worker's ``apply`` passed its open check and is paused
    outside every lock when the storage closes.  ``reused``: the closed ``ranges.log``
    descriptor number now belongs to another file.  ``other2``: the probe as written (a spare
    descriptor, then another storage ``x`` logging accepted roots, then ``s`` reopened).  The
    stale write is refused and no file changes; ``x``'s acceptor log keeps every root.  Before
    the fix the apply succeeded after ``close()`` and its ranges entry landed in the file that
    got the number (in ``other2`` on the probe's run, ``x/roots.log``, which then reloaded
    accepted (4, 5) instead of (6, 8))."""
    store = FakeObjectStore()
    writer, records, root = _replication(tmp_path, store)
    s = _tiered(tmp_path, store, "s")
    number = s._ranges_fd
    paused, gate = threading.Event(), threading.Event()
    log_ranges = s._log_ranges

    def slow_log_ranges(*args, **kwargs):
        paused.set()                # after the open check and _remember, outside every lock
        assert gate.wait(20)
        return log_ranges(*args, **kwargs)

    s._log_ranges = slow_log_ranges
    opened = []
    t, out = _stale(lambda: s.apply(records, root, 1))
    try:
        assert paused.wait(10)
        s.close()
        if variant == "reused":
            opened.append(_claim(number, str(tmp_path / "victim.log")))
        else:
            opened.append(os.open(os.devnull, os.O_RDONLY))
            x = _tiered(tmp_path, store, "x")
            x.log_promise(3)
            x.log_root("accepted", 4, RootRecord(FORMAT_VERSION, uuid.uuid4(), 64, 5), sync=True)
        s2 = _tiered(tmp_path, store, "s")
        before = _files(tmp_path)
    finally:
        gate.set()
    _refused(t, out)
    assert _files(tmp_path) == before, "a write after close() changed a file"
    if variant == "other2":
        r8 = RootRecord(FORMAT_VERSION, uuid.uuid4(), 64, 8)
        x.log_root("accepted", 6, r8, sync=True)
        x.close()
        x = _tiered(tmp_path, store, "x")
        assert x.load_roots().accepted == (6, r8), "an accepted root was lost on reload"
        x.close()
    for fd in opened:
        os.close(fd)
    s2.close()
    writer.close()


# -- every guarded write, paused at its lock ---------------------------------------------------

def test_a_ranges_log_write_waiting_for_its_lock_is_refused_once_closed(tmp_path):
    store = FakeObjectStore()
    writer, records, root = _replication(tmp_path, store)
    s = _tiered(tmp_path, store, "s")
    number = s._ranges_fd
    gated = _gate(s, "_ranges_lock")
    t, out = _stale(lambda: s.apply(records, root, 1))
    try:
        assert gated.waiting.wait(10)
        s.close()
        victim = _claim(number, str(tmp_path / "victim.log"))
        before = _files(tmp_path)
    finally:
        gated.gate.set()
    _refused(t, out)
    assert _files(tmp_path) == before
    os.close(victim)
    writer.close()


def test_a_manifest_write_waiting_for_its_lock_is_refused_once_closed(tmp_path):
    s = _tiered(tmp_path, FakeObjectStore(), "s")
    number = s._manifest_fd
    gated = _gate(s, "_manifest_lock")
    t, out = _stale(lambda: s.push_atom({"x": 1}))     # the first record: a new own segment
    try:
        assert gated.waiting.wait(10)
        s.close()
        victim = _claim(number, str(tmp_path / "victim.log"))
        before = _files(tmp_path)
    finally:
        gated.gate.set()
    _refused(t, out)
    assert _files(tmp_path) == before
    os.close(victim)


def test_a_manifest_sync_waiting_for_its_lock_is_refused_once_closed(tmp_path):
    s = _tiered(tmp_path, FakeObjectStore(), "s")
    s.push_atom({"x": 1})                               # the manifest needs a sync
    gated = _gate(s, "_manifest_lock")
    t, out = _stale(s.flush)
    try:
        assert gated.waiting.wait(10)
        s.close()
    finally:
        gated.gate.set()
    _refused(t, out)


def test_a_copy_write_waiting_for_its_lock_is_refused_once_closed(tmp_path):
    store = FakeObjectStore()
    writer, records, root = _replication(tmp_path, store)
    s = _tiered(tmp_path, store, "s")
    gated = _gate(s, "_copies_lock")
    t, out = _stale(lambda: s.apply(records, root, 1))
    try:
        assert gated.waiting.wait(10)
        s.close()
        before = _files(tmp_path)
    finally:
        gated.gate.set()
    _refused(t, out)
    assert _files(tmp_path) == before, "a copy was written after close()"
    assert not os.path.exists(str(tmp_path / "s" / (root.segment.hex + ".seg")))
    writer.close()


def _releasable(tmp_path, store, name, writer, records, root):
    """A storage holding a copy of ``writer``'s segment that the store holds whole."""
    s = _tiered(tmp_path, store, name)
    s.apply(records, root, 1)
    s.refresh_remote()
    return s


def test_a_local_release_waiting_for_its_lock_deletes_nothing_once_closed(tmp_path):
    store = FakeObjectStore()
    writer, records, root = _replication(tmp_path, store)
    assert writer.ship(force=True)
    copy = root.segment.hex + ".seg"
    control = _releasable(tmp_path, store, "control", writer, records, root)
    assert control.release_local(root_key(1, root)) == 1            # the setup releases the copy
    assert not os.path.exists(str(tmp_path / "control" / copy))
    control.close()
    s = _releasable(tmp_path, store, "s", writer, records, root)
    gated = _gate(s, "_copies_lock")
    t, out = _stale(lambda: s.release_local(root_key(1, root)))
    try:
        assert gated.waiting.wait(10)
        s.close()
        before = _files(tmp_path)
    finally:
        gated.gate.set()
    _refused(t, out)
    assert _files(tmp_path) == before
    assert os.path.exists(str(tmp_path / "s" / copy)), "a copy was deleted after close()"
    writer.close()


def test_a_ranges_log_compaction_waiting_for_its_lock_is_refused_once_closed(tmp_path, monkeypatch):
    monkeypatch.setattr(tiered_module, "_ROOTS_LOG_COMPACT", 0)        # compact at every release
    store = FakeObjectStore()
    writer, records, root = _replication(tmp_path, store)
    s = _tiered(tmp_path, store, "s")
    s.apply(records, root, 1)
    gated = _gate(s, "_ranges_lock")
    t, out = _stale(lambda: s.release_local((0, 0, 0)))
    try:
        assert gated.waiting.wait(10)
        s.close()
        before = _files(tmp_path)
    finally:
        gated.gate.set()
    _refused(t, out)
    assert _files(tmp_path) == before
    writer.close()


def test_a_persist_waiting_for_the_shipping_lock_is_refused_once_closed(tmp_path):
    store = FakeObjectStore()
    s = _tiered(tmp_path, store, "s")
    ptr = s.push_atom({"x": 1})
    gated = _gate(s, "_ship_lock")
    t, out = _stale(lambda: s.persist(1, RootRecord(FORMAT_VERSION, ptr.segment, ptr.offset, 1)))
    try:
        assert gated.waiting.wait(10)
        s.close()
        before = store.keys()
    finally:
        gated.gate.set()
    _refused(t, out)
    assert store.keys() == before and not any(k.endswith("space.root") for k in before)


def test_a_part_upload_waiting_for_the_shipping_lock_is_refused_once_closed(tmp_path):
    store = FakeObjectStore()
    writer, records, root = _replication(tmp_path, store)               # never shipped by w
    s = _tiered(tmp_path, store, "s")
    s.apply(records, root, 1)
    gated = _gate(s, "_ship_lock")
    end = root.offset + len(records[-1][2])
    t, out = _stale(lambda: s.ensure_remote([(root.segment, end)], upload_copies=True))
    try:
        assert gated.waiting.wait(10)
        s.close()
        before = store.keys()
    finally:
        gated.gate.set()
    _refused(t, out)
    assert store.keys() == before and not any(k.endswith(".part") for k in before)
    writer.close()


def test_a_ship_waiting_for_the_shipping_lock_is_refused_once_closed(tmp_path):
    store = FakeObjectStore()
    s = _tiered(tmp_path, store, "s")
    s.push_atom({"x": 1})
    gated = _gate(s, "_ship_lock")
    t, out = _stale(lambda: s.ship(force=True))
    try:
        assert gated.waiting.wait(10)
        s.close()
    finally:
        gated.gate.set()
    _refused(t, out)
    assert store.keys() == []


def test_a_file_storage_root_write_waiting_for_the_flush_lock_is_refused_once_closed(tmp_path):
    fs = FileStorage(str(tmp_path / "f"), durability="fsync")
    ptr = fs.push_atom({"x": 1})
    gated = _gate(fs, "_flush_lock", skip=1)           # the publish's own flush passes
    t, out = _stale(lambda: fs.publish_root(ptr, None))
    try:
        assert gated.waiting.wait(10)
        fs.close()
        before = _files(tmp_path)
    finally:
        gated.gate.set()
    _refused(t, out)
    assert _files(tmp_path) == before
    assert not os.path.exists(str(tmp_path / "f" / "space.root"))


def test_a_cloud_storage_root_write_waiting_for_the_upload_lock_is_refused_once_closed():
    store = FakeObjectStore()
    cs = CloudStorage(store, backoff=(0.0, 0.0), retries=1)
    ptr = cs.push_atom({"x": 1})
    gated = _gate(cs, "_upload_lock", skip=1)          # the publish's own upload passes
    t, out = _stale(lambda: cs.publish_root(ptr, None))
    try:
        assert gated.waiting.wait(10)
        cs.close()
        before = store.keys()
    finally:
        gated.gate.set()
    _refused(t, out)
    assert store.keys() == before and not any(k.endswith("space.root") for k in before)


# -- close() waits for a write in progress -----------------------------------------------------

def _apply_op(tmp_path, store, s):
    writer, records, root = _replication(tmp_path, store)
    return lambda: s.apply(records, root, 1)


@pytest.mark.parametrize("lock,make_op", [
    ("_ranges_lock", _apply_op),
    ("_copies_lock", _apply_op),
    ("_manifest_lock", lambda tmp_path, store, s: lambda: s.push_atom({"x": 1})),
    ("_ship_lock", lambda tmp_path, store, s: lambda: s.persist(1, RootRecord(FORMAT_VERSION, uuid.uuid4(), 64, 1))),
])
def test_close_waits_for_a_write_in_progress_under_each_lock(tmp_path, lock, make_op):
    """The other half of the rule: ``close()`` takes each lock before it releases what the
    lock guards.  While a writer holds the lock, ``close()`` waits; the writer then either
    completes on the open descriptor (it passed its check before ``close()`` started) or is
    refused (it checks after ``close()`` set the flag) -- never an I/O error on a closed
    descriptor.  A ``close()`` that released a descriptor without the lock would return at
    once and leave the write to a closed, reusable number."""
    store = FakeObjectStore()
    s = _tiered(tmp_path, store, "s")
    op = make_op(tmp_path, store, s)
    gated = _gate(s, lock, inside=True)
    t, out = _stale(op)
    closer = threading.Thread(target=s.close, daemon=True)
    try:
        assert gated.waiting.wait(10)
        closer.start()
        closer.join(0.5)
        assert closer.is_alive(), f"close() did not wait for a write holding {lock}"
    finally:
        gated.gate.set()
    t.join(10)
    closer.join(10)
    assert not t.is_alive() and not closer.is_alive()
    assert "error" not in out or isinstance(out["error"], ProtoValidationError), out
