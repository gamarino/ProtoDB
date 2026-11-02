"""A failed local write poisons its handle, and a compaction never leaves a closed descriptor
in place (8.3d re-review 9, B1 and B2; ruling R57).

* B1, the reviewer's ``probe_compact_reopen_fail``: ``RootsLog._compact`` and
  ``TieredStorage._compact_ranges`` closed the owned descriptor, then reopened the file.  When
  the reopen failed (or the close raised after releasing the number), the attribute kept the
  closed number while the handle still accepted writes, and the next append landed in
  whichever file the operating system had handed that number to: the probe's acceptor append
  "returned normally" while ``load()`` lost it.  The faults are injected deterministically by
  patching ``os`` functions for the handle's own files and descriptors only.  Whatever the
  step that fails, the handle never writes through a number another file holds, and after
  any failure every later write raises (``ProtoUnexpectedError``).
* Every other local write: an I/O error on ``roots.log``, ``ranges.log``, ``segments.log`` or a
  copy poisons the handle instead of being reported once and forgotten (a failed ``fsync``
  used to be retried; a failed append left a torn entry that hides every later one on reload).
* B2: ``CloudStorage.publish_root`` releases its upload lock while it backs off, so
  ``close()`` does not wait for the sleeps; an attempt after ``close()`` is refused, as an
  unknown outcome when an earlier attempt may still land.
"""
import errno
import os
import threading
import time
import uuid

import pytest

from proto_db.cluster import rootlog as rootlog_module
from proto_db.cluster.rootlog import ENTRY_SIZE, RootsLog
from proto_db.storage.logfile import HEADER_SIZE
from proto_db.errors import ProtoOutcomeUnknownError, ProtoUnexpectedError, ProtoValidationError
from proto_db.storage import tiered as tiered_module
from proto_db.storage.cloud import CloudStorage
from proto_db.storage.format import FORMAT_VERSION, RootRecord
from proto_db.storage.tiered import RANGES_LOG, TieredStorage
from tests.support.fake_object_store import FakeObjectStore

pytestmark = [pytest.mark.timeout(60)]

#: The compaction step that fails.  ``number_reused``: nothing fails, but the descriptor number
#: released by the compaction is taken by another file at once and every later open of the log
#: fails (the probe's schedule when the reopen comes after the close).  ``close_eio``: closing
#: the old descriptor releases its number, which another file takes, and then raises ``EIO``.
#: ``open_fails``: the first open of the log's files during the compaction fails with
#: ``EMFILE`` (after another file took the old number, if it was already released).
#: ``fsync_fails``: the first ``fsync`` of a descriptor other than the old one fails.
#: ``replace_fails``: the rename of the rewritten file fails.
STEPS = ["number_reused", "close_eio", "open_fails", "fsync_fails", "replace_fails"]
REAL = {name: getattr(os, name) for name in ("open", "close", "fsync", "replace", "write")}


def rec(epoch: int) -> RootRecord:
    return RootRecord(FORMAT_VERSION, uuid.UUID(int=epoch), 64, epoch)


def _claim(number: int, path: str) -> int:
    """Open ``path`` until it gets descriptor ``number`` (free), as the next ``open`` anywhere in
    the process would: descriptors are allocated lowest first."""
    held = []
    try:
        while True:
            fd = REAL["open"](path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
            if fd == number:
                return fd
            held.append(fd)
            assert fd < number, f"descriptor {number} was taken meanwhile"
    finally:
        for fd in held:
            REAL["close"](fd)


def _is_open(fd: int) -> bool:
    try:
        os.fstat(fd)
        return True
    except OSError:
        return False


class CompactionFault:
    """Injects ``step`` into the next compaction of the file ``path`` whose descriptor is
    ``old`` (see :data:`STEPS`); ``victim`` is the file that takes a released number."""

    def __init__(self, monkeypatch, step: str, path: str, old: int, victim: str):
        self.step, self.path, self.old, self.victim_path = step, path, old, victim
        self.victim = None
        self.fired = False
        for name in REAL:
            if hasattr(self, "_" + name):
                monkeypatch.setattr(os, name, getattr(self, "_" + name))

    def _ours(self, path) -> bool:
        return isinstance(path, (str, bytes, os.PathLike)) and os.fspath(path).startswith(self.path)

    def _take_old_number(self) -> None:
        if self.victim is None and not _is_open(self.old):
            self.victim = _claim(self.old, self.victim_path)

    def _open(self, path, flags, mode=0o777, *args, **kwargs):
        if self._ours(path) and self.step in ("number_reused", "close_eio", "open_fails"):
            if self.step == "open_fails" and not self.fired:
                self.fired = True
                self._take_old_number()
                raise OSError(errno.EMFILE, "Too many open files (injected)")
            if self.step != "open_fails" and self.victim is not None:
                raise OSError(errno.EMFILE, "Too many open files (injected)")
        return REAL["open"](path, flags, mode, *args, **kwargs)

    def _close(self, fd):
        REAL["close"](fd)
        if fd == self.old and self.step in ("number_reused", "close_eio") and not self.fired:
            self.fired = True
            self._take_old_number()
            if self.step == "close_eio":
                raise OSError(errno.EIO, "Input/output error (injected)")

    def _fsync(self, fd):
        if self.step == "fsync_fails" and not self.fired and fd != self.old and not self._is_dir(fd):
            self.fired = True
            raise OSError(errno.EIO, "Input/output error (injected)")
        return REAL["fsync"](fd)

    def _replace(self, src, dst, *args, **kwargs):
        if self.step == "replace_fails" and not self.fired and self._ours(dst):
            self.fired = True
            raise OSError(errno.EIO, "Input/output error (injected)")
        return REAL["replace"](src, dst, *args, **kwargs)

    @staticmethod
    def _is_dir(fd) -> bool:
        import stat
        return stat.S_ISDIR(os.fstat(fd).st_mode)

    def victim_size(self) -> int:
        return 0 if self.victim is None else os.fstat(self.victim).st_size


# -- B1: RootsLog --------------------------------------------------------------------------------

@pytest.mark.parametrize("step", STEPS)
def test_b1_a_roots_log_compaction_never_writes_through_a_released_number(tmp_path, monkeypatch, step):
    """``probe_compact_reopen_fail roots``.  The fifth append compacts the log, and ``step``
    fails.  The handle's descriptor is never a number another file holds, the victim file never
    grows, and the next append either lands in the log (``number_reused``: nothing failed,
    since the replacement is open before the old descriptor is closed) or raises, as does
    every later write: a failed compaction poisons the handle.  Before the fix, the next
    acceptor append returned normally into the victim and ``load()`` lost it."""
    monkeypatch.setattr(rootlog_module, "_COMPACT_BYTES", HEADER_SIZE + 4 * ENTRY_SIZE + 1)
    path = str(tmp_path / "n.roots")
    log = RootsLog(path, durability="fsync")
    for i in range(1, 5):
        log.append("accepted", 1, rec(i), sync=True)
    fault = CompactionFault(monkeypatch, step, path, log._fd, str(tmp_path / "victim.log"))
    failed = None
    try:
        log.append("accepted", 1, rec(5), sync=True)     # compacts
    except ProtoUnexpectedError as e:
        failed = e
    assert fault.fired or step == "number_reused"
    assert fault.victim is None or log._fd != fault.victim, "the log holds a number another file owns"
    assert _is_open(log._fd)
    before = fault.victim_size()
    try:
        log.append("accepted", 2, rec(99), sync=True)
        appended = True
    except ProtoUnexpectedError:
        appended = False
    assert fault.victim_size() == before, "an append landed in another file"
    monkeypatch.undo()
    if step == "number_reused":
        assert failed is None and appended
        assert log.load().accepted == (2, rec(99))
    else:
        assert failed is not None, "the injected failure was not reported"
        assert not appended, "a log whose compaction failed accepted a write"
        with pytest.raises(ProtoUnexpectedError, match="refuses writes"):
            log.promise(7)
        with pytest.raises(ProtoUnexpectedError, match="refuses writes"):
            log.mark_rejoined()
        assert log.load().accepted[0] == 1, "load() must keep working on a poisoned log"
    log.close()
    reopened = RootsLog(path, durability="fsync")
    assert reopened.load().accepted == ((2, rec(99)) if appended else (1, rec(5)))
    reopened.append("accepted", 3, rec(100), sync=True)     # a new handle writes again
    assert reopened.load().accepted == (3, rec(100))
    reopened.close()
    if fault.victim is not None:
        os.close(fault.victim)


@pytest.mark.parametrize("fault_on", ["write", "fsync"])
def test_a_failed_roots_log_write_poisons_the_handle(tmp_path, monkeypatch, fault_on):
    """A failed append or ``fsync`` poisons the handle: the file may end in a torn entry (after
    which the decoder stops) or hold what the ``fsync`` could not confirm, so no later write is
    accepted.  Before, the next append was made after the torn bytes and lost on reload."""
    path = str(tmp_path / "n.roots")
    log = RootsLog(path, durability="fsync")
    log.append("accepted", 1, rec(1), sync=True)
    target = log._fd
    armed = {"on": True}

    def failing_write(fd, data):
        if fd == target and armed["on"]:
            armed["on"] = False
            REAL["write"](fd, bytes(data)[:7])           # a torn entry
            raise OSError(errno.ENOSPC, "No space left on device (injected)")
        return REAL["write"](fd, data)

    def failing_fsync(fd):
        if fd == target and armed["on"]:
            armed["on"] = False
            raise OSError(errno.EIO, "Input/output error (injected)")
        return REAL["fsync"](fd)

    monkeypatch.setattr(os, fault_on, failing_write if fault_on == "write" else failing_fsync)
    with pytest.raises(ProtoUnexpectedError):
        log.append("accepted", 2, rec(2), sync=True)
    with pytest.raises(ProtoUnexpectedError, match="refuses writes"):
        log.append("accepted", 3, rec(3), sync=True)
    with pytest.raises(ProtoUnexpectedError, match="refuses writes"):
        log.promise(9)
    monkeypatch.undo()
    log.close()
    with pytest.raises(ProtoValidationError):
        log.append("accepted", 4, rec(4), sync=True)


def test_a_short_roots_log_write_is_completed(tmp_path, monkeypatch):
    """A short ``write`` (allowed by POSIX) is continued, not mistaken for a full entry."""
    path = str(tmp_path / "n.roots")
    log = RootsLog(path, durability="fsync")
    target = log._fd

    def short_write(fd, data):
        if fd == target:
            return REAL["write"](fd, bytes(data)[:5])
        return REAL["write"](fd, data)

    monkeypatch.setattr(os, "write", short_write)
    log.append("accepted", 1, rec(1), sync=True)
    log.promise(4)
    monkeypatch.undo()
    roots = log.load()
    assert roots.accepted == (1, rec(1)) and roots.promised == 4
    log.close()


# -- B1: TieredStorage ranges.log ------------------------------------------------------------------

def _tiered(tmp_path, store, name, **kw):
    kw.setdefault("durability", "fsync")
    kw.setdefault("ship_interval", 0.0)
    kw.setdefault("backoff", (0.0, 0.0))
    kw.setdefault("retries", 1)
    return TieredStorage(str(tmp_path / name), store, **kw)


def _replication(tmp_path, store):
    writer = _tiered(tmp_path, store, "w")
    ptrs = [writer.push_atom({"i": i}) for i in range(3)]
    return writer, writer.outgoing(), ptrs[2]


@pytest.mark.parametrize("step", STEPS)
def test_b1_a_ranges_log_compaction_never_writes_through_a_released_number(tmp_path, monkeypatch, step):
    """``probe_compact_reopen_fail ranges``: the same schedules on ``ranges.log``.  Before the
    fix the next ``apply`` returned normally and its 80-byte entry landed in the victim."""
    monkeypatch.setattr(tiered_module, "_ROOTS_LOG_COMPACT", 0)    # compact on the next call
    store = FakeObjectStore()
    writer, records, ptr = _replication(tmp_path, store)
    s = _tiered(tmp_path, store, "s")
    root1 = RootRecord(FORMAT_VERSION, ptr.segment, ptr.offset, 1)
    s.apply(records, root1, 1)
    path = os.path.join(s.path, RANGES_LOG)
    fault = CompactionFault(monkeypatch, step, path, s._ranges_fd, str(tmp_path / "victim.log"))
    failed = None
    try:
        s._compact_ranges()
    except ProtoUnexpectedError as e:
        failed = e
    assert fault.fired or step == "number_reused"
    assert fault.victim is None or s._ranges_fd != fault.victim, "ranges.log holds a number another file owns"
    assert _is_open(s._ranges_fd)
    before = fault.victim_size()
    root2 = RootRecord(FORMAT_VERSION, ptr.segment, ptr.offset, 2)
    try:
        s.apply(records, root2, 2)
        applied = True
    except ProtoUnexpectedError:
        applied = False
    assert fault.victim_size() == before, "a ranges entry landed in another file"
    monkeypatch.undo()
    if step == "number_reused":
        assert failed is None and applied
    else:
        assert failed is not None, "the injected failure was not reported"
        assert not applied, "a storage whose compaction failed accepted a write"
        with pytest.raises(ProtoUnexpectedError, match="failed earlier"):
            s.log_root("accepted", 2, root2, sync=True)
        with pytest.raises(ProtoUnexpectedError, match="failed earlier"):
            s.log_promise(5)
    s.close()
    reopened = _tiered(tmp_path, store, "s")
    assert reopened.commit_ranges(root2 if applied else root1) is not None
    reopened.close()
    writer.close()
    if fault.victim is not None:
        os.close(fault.victim)


@pytest.mark.parametrize("where", ["ranges_write", "copy_fsync", "manifest_fsync"])
def test_a_failed_local_write_poisons_the_tiered_storage(tmp_path, monkeypatch, where):
    """Every local write of a ``TieredStorage`` poisons it on an I/O error: later local writes
    and acceptor-log writes raise ``ProtoUnexpectedError``.  ``manifest_fsync``: a failed
    ``fsync`` of ``segments.log`` used to be retried by the next ``flush()``, which then
    reported success for bytes the failed ``fsync`` may have dropped."""
    store = FakeObjectStore()
    writer, records, ptr = _replication(tmp_path, store)
    s = _tiered(tmp_path, store, "s")
    root = RootRecord(FORMAT_VERSION, ptr.segment, ptr.offset, 1)
    copy = os.path.join(s.path, ptr.segment.hex + ".seg")
    armed = {"on": True}

    def failing_write(fd, data):
        if fd == s._ranges_fd and armed["on"]:
            armed["on"] = False
            raise OSError(errno.ENOSPC, "No space left on device (injected)")
        return REAL["write"](fd, data)

    def failing_fsync(fd):
        if armed["on"]:
            hit = (where == "manifest_fsync" and fd == s._manifest_fd) or (
                where == "copy_fsync" and os.path.exists(copy) and os.path.samestat(os.fstat(fd), os.stat(copy)))
            if hit:
                armed["on"] = False
                raise OSError(errno.EIO, "Input/output error (injected)")
        return REAL["fsync"](fd)

    if where == "ranges_write":
        monkeypatch.setattr(os, "write", failing_write)
    else:
        monkeypatch.setattr(os, "fsync", failing_fsync)
    if where == "manifest_fsync":
        s.push_atom({"own": 1})
        with pytest.raises(ProtoUnexpectedError):
            s.flush()
    else:
        with pytest.raises(ProtoUnexpectedError):
            s.apply(records, root, 1)
    assert not armed["on"], "the fault was never injected"
    monkeypatch.undo()
    with pytest.raises(ProtoUnexpectedError, match="failed earlier"):
        s.flush()
    with pytest.raises(ProtoUnexpectedError, match="failed earlier"):
        s.apply(records, RootRecord(FORMAT_VERSION, ptr.segment, ptr.offset, 2), 2)
    with pytest.raises(ProtoUnexpectedError, match="failed earlier"):
        s.log_root("accepted", 2, root, sync=True)
    with pytest.raises(ProtoUnexpectedError, match="failed earlier"):
        s.log_rejoined()
    s.close()
    writer.close()
    reopened = _tiered(tmp_path, store, "s")          # a new instance writes again
    reopened.log_root("accepted", 3, root, sync=True)
    assert reopened.load_roots().accepted == (3, root)
    reopened.close()


# -- B2: CloudStorage.publish_root ---------------------------------------------------------------

@pytest.mark.parametrize("fault,expected", [("transient", ProtoValidationError),
                                            ("unknown_before", ProtoOutcomeUnknownError)])
def test_b2_close_does_not_wait_for_a_publish_backing_off(fault, expected):
    """The first root write fails and the publish backs off (1 to 2 s).  ``close()`` returns
    without waiting for the sleep, and the next attempt is refused: as ``ProtoValidationError``
    when nothing can have been applied, as ``ProtoOutcomeUnknownError`` when the failed attempt
    may still land.  No root is written after ``close()``.  Before, ``close()`` waited for the
    whole compare-and-swap loop, sleeps included."""
    store = FakeObjectStore()
    cs = CloudStorage(store, backoff=(2.0, 2.0), retries=3)
    ptr = cs.push_atom({"x": 1})
    store.inject("put_if_absent", fault, match=lambda key: key == cs.root_key)
    out = {}

    def run():
        try:
            out["result"] = cs.publish_root(ptr, None)
        except Exception as e:      # the outcome under test
            out["error"] = e

    t = threading.Thread(target=run, daemon=True)
    t.start()
    deadline = time.monotonic() + 10
    while store.pending_faults():
        assert time.monotonic() < deadline, "the publish never tried to write the root"
        time.sleep(0.001)
    time.sleep(0.05)                # the attempt is over (its read-back included): backing off
    started = time.monotonic()
    cs.close()
    waited = time.monotonic() - started
    t.join(10)
    assert not t.is_alive()
    assert waited < 0.9, f"close() waited {waited:.2f} s for a publish backing off"
    assert isinstance(out.get("error"), expected), out
    assert not any(k == cs.root_key for k in store.keys())
