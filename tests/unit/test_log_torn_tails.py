"""Every append-only log trims a torn tail on open and never hides an entry behind torn bytes
(8.3d re-review 10, R2; ruling R58).

The logs: the acceptor log (:class:`~proto_db.cluster.rootlog.RootsLog`, ``roots.log``),
``ranges.log`` and ``segments.log`` of :class:`~proto_db.storage.tiered.TieredStorage`, and the
own segments of its local WAL.

* A torn tail -- what a failed append (the poisoned-handle recovery: close and reopen), a
  ``SIGKILL`` or a power loss left after the last valid entry -- is truncated, and the file
  ``fsync``-ed, when the log is opened, before any append.  Before the fix the reopened log
  appended after the torn bytes, and the reader, which stops at the first invalid entry, lost
  every later entry on reload (an acknowledged acceptance and a sent promise included).
* An invalid entry followed by valid ones is mid-log corruption: opening raises
  ``ProtoCorruptionError`` and the file is left byte for byte as it was.
"""
import errno
import os
import signal
import subprocess
import sys
import textwrap
import uuid

import pytest

from proto_db.cluster.rootlog import RootsLog
from proto_db.errors import ProtoCorruptionError, ProtoNotSupportedError, ProtoUnexpectedError
from proto_db.storage import logfile
from proto_db.storage.format import FORMAT_VERSION, SEGMENT_HEADER_SIZE, RootRecord
from proto_db.storage.tiered import MANIFEST, RANGES_LOG, ROOTS_LOG, TieredStorage
from tests.support.fake_object_store import FakeObjectStore

pytestmark = [pytest.mark.timeout(120)]

REAL_WRITE = os.write
REAL_PWRITE = os.pwrite
REAL_FSYNC = os.fsync
REAL_FTRUNCATE = os.ftruncate
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
KW = dict(durability="fsync", ship_interval=0.0, backoff=(0.0, 0.0), retries=0)


def rec(i: int) -> RootRecord:
    return RootRecord(FORMAT_VERSION, uuid.UUID(int=i), 64, i)


def _tear_next_write(monkeypatch, get_fd, keep: int = 7):
    """The next ``os.write`` on ``get_fd()`` writes ``keep`` bytes and fails (``ENOSPC``)."""
    state = {"armed": True}

    def write(fd, data):
        if state["armed"] and fd == get_fd():
            state["armed"] = False
            REAL_WRITE(fd, bytes(data)[:keep])
            raise OSError(errno.ENOSPC, "No space left on device (injected)")
        return REAL_WRITE(fd, data)

    monkeypatch.setattr(os, "write", write)
    return state


# -- one adapter per log ----------------------------------------------------------------------

class RootsAdapter:
    """``roots.log`` through a bare :class:`RootsLog`; an entry ``i`` is an accepted root."""

    name = "roots"

    def __init__(self, tmp_path):
        self.path = str(tmp_path / "n.roots")

    def open(self):
        return RootsLog(self.path)

    def file(self, h) -> str:
        return self.path

    def fd(self, h) -> int:
        return h._fd

    def append(self, h, i: int) -> None:
        h.append("accepted", i, rec(i))

    def has(self, h, i: int) -> bool:
        """The log holds the accepted root ``i`` or a newer one (``load()`` keeps the newest)."""
        accepted = h.load().accepted
        return accepted is not None and (accepted == (i, rec(i)) or accepted[0] > i)

    def close(self, h) -> None:
        h.close()


class _TieredAdapter:
    def __init__(self, tmp_path):
        self.store = FakeObjectStore()
        self.dir = str(tmp_path / "s")

    def open(self):
        return TieredStorage(self.dir, self.store, **KW)

    def close(self, h) -> None:
        try:
            h.close()
        except ProtoUnexpectedError:
            pass                    # closing a poisoned storage may report its earlier failure


class RangesAdapter(_TieredAdapter):
    """``ranges.log``; an entry ``i`` is the ranges entry of the commit of ``rec(i)``."""

    name = "ranges"

    def file(self, h) -> str:
        return os.path.join(self.dir, RANGES_LOG)

    def fd(self, h) -> int:
        return h._ranges_fd

    def append(self, h, i: int) -> None:
        h.store_commit([], rec(i), i, [(uuid.UUID(int=1000 + i), 64, 96)])

    def has(self, h, i: int) -> bool:
        return h.commit_ranges(rec(i)) == [(uuid.UUID(int=1000 + i), 64, 96)]


class ManifestAdapter(_TieredAdapter):
    """``segments.log``; an entry ``i`` is the own segment holding the record ``{"i": i}``."""

    name = "segments"

    def __init__(self, tmp_path):
        super().__init__(tmp_path)
        self.segments = {}

    def file(self, h) -> str:
        return os.path.join(self.dir, MANIFEST)

    def fd(self, h) -> int:
        return h._manifest_fd

    def append(self, h, i: int) -> None:
        h._local.roll_segment()
        self.segments[i] = None
        ptr = h.push_atom({"i": i})
        self.segments[i] = ptr
        h.flush()

    def has(self, h, i: int) -> bool:
        ptr = self.segments.get(i)
        return ptr is not None and ptr.segment in h._own and h.get_atom(ptr) == {"i": i}


ADAPTERS = [RootsAdapter, RangesAdapter, ManifestAdapter]


def _ids(cls):
    return cls.name


# -- torn tail left by a failed append (the poisoned-handle recovery) ---------------------------

@pytest.mark.parametrize("adapter", ADAPTERS, ids=_ids)
def test_a_failed_append_torn_tail_is_trimmed_on_reopen_and_later_entries_survive(tmp_path, monkeypatch,
                                                                                  adapter):
    """re-review 10's R2 at the storage level: an append tears (7 bytes, then ``ENOSPC``), the
    handle is poisoned, the log is closed and reopened (the documented recovery); entries
    appended after the reopen are visible on the same handle and after a restart.  On 0f9bf1e
    they were appended after the torn bytes and lost on reload."""
    a = adapter(tmp_path)
    h = a.open()
    a.append(h, 1)
    a.append(h, 2)
    if a.name == "segments":
        h._local.roll_segment()
    state = _tear_next_write(monkeypatch, lambda: a.fd(h))
    with pytest.raises(ProtoUnexpectedError):
        if a.name == "segments":
            h.push_atom({"i": 3})
        else:
            a.append(h, 3)
    monkeypatch.undo()
    assert not state["armed"], "the fault did not fire"
    a.close(h)
    torn_size = os.path.getsize(a.file(h))
    h = a.open()
    assert os.path.getsize(a.file(h)) == torn_size - 7, "the torn tail was not truncated"
    assert a.has(h, 2)
    a.append(h, 4)
    a.append(h, 5)
    assert a.has(h, 5)
    a.close(h)
    h = a.open()
    assert a.has(h, 2) and a.has(h, 4) and a.has(h, 5), "an entry appended after the reopen was lost on reload"
    a.close(h)


# -- torn tails left by a crash or a power loss ----------------------------------------------------

TAILS = {
    "partial_frame": lambda frame: frame[:11],
    "zero_fill": lambda frame: bytes(4096),
    "partial_then_zeros": lambda frame: frame[:20] + bytes(100),
    "length_only": lambda frame: frame[:4],
}


@pytest.mark.parametrize("tail", sorted(TAILS))
@pytest.mark.parametrize("adapter", ADAPTERS, ids=_ids)
def test_a_crash_torn_tail_is_trimmed_and_fsynced_before_any_append(tmp_path, monkeypatch, adapter, tail):
    """A ``SIGKILL`` or a power loss leaves part of an entry, or zero bytes, after the last
    valid entry.  Opening the log truncates it, ``fsync``-s the file before returning, and the
    entries appended after it survive a reload."""
    a = adapter(tmp_path)
    h = a.open()
    a.append(h, 1)
    a.append(h, 2)
    a.close(h)
    path = a.file(h)
    valid = os.path.getsize(path)
    with open(path, "ab") as f:
        f.write(TAILS[tail](logfile.encode_frame(b"x" * 41)))
    events = []

    def ftruncate(fd, length):
        events.append(("ftruncate", fd, length))
        return REAL_FTRUNCATE(fd, length)

    def fsync(fd):
        events.append(("fsync", fd))
        return REAL_FSYNC(fd)

    monkeypatch.setattr(os, "ftruncate", ftruncate)
    monkeypatch.setattr(os, "fsync", fsync)
    h = a.open()
    monkeypatch.undo()
    assert os.path.getsize(path) == valid
    cut = [i for i, e in enumerate(events) if e[0] == "ftruncate"]
    assert len(cut) == 1 and events[cut[0]][2] == valid
    fd = events[cut[0]][1]
    assert ("fsync", fd) in events[cut[0] + 1:], "the truncation was not fsynced"
    assert a.has(h, 2)
    a.append(h, 3)
    a.close(h)
    h = a.open()
    assert a.has(h, 3), "the entry appended after the trim was lost on reload"
    a.close(h)


def test_a_sigkilled_writer_leaves_torn_tails_that_the_next_open_trims(tmp_path):
    """A real ``SIGKILL``: a child process appends to every log of a ``TieredStorage``, then
    writes part of one more entry to each (``roots.log``, ``ranges.log``, ``segments.log`` and
    its open segment) and kills itself.  Every log reopens trimmed, takes new entries, and
    keeps them across a restart."""
    path = str(tmp_path / "s")
    child = textwrap.dedent(f"""
        import os, signal, sys, uuid
        sys.path.insert(0, {ROOT!r})
        from proto_db.storage.format import FORMAT_VERSION, RootRecord, encode_record
        from proto_db.storage.logfile import encode_frame
        from proto_db.storage.tiered import TieredStorage
        from tests.support.fake_object_store import FakeObjectStore
        s = TieredStorage({path!r}, FakeObjectStore(), durability="fsync")
        ptr = s.push_atom({{"i": 1}})
        s.flush()
        rec = RootRecord(FORMAT_VERSION, uuid.UUID(int=1), 64, 1)
        s.store_commit([], rec, 1, [(uuid.UUID(int=9), 64, 96)])
        s.log_root("accepted", 1, rec)
        frame = encode_frame(b"y" * 41)
        os.write(s._roots._fd, frame[:13])
        os.write(s._ranges_fd, frame[:13])
        os.write(s._manifest_fd, frame[:13])
        seg = os.open(os.path.join({path!r}, ptr.segment.hex + ".seg"), os.O_WRONLY | os.O_APPEND)
        os.write(seg, encode_record(1, b"z" * 50)[:20])
        print(ptr.segment.hex, ptr.offset, flush=True)
        os.kill(os.getpid(), signal.SIGKILL)
    """)
    out = subprocess.run([sys.executable, "-c", child], capture_output=True, text=True, timeout=60)
    assert out.returncode == -signal.SIGKILL, out.stderr
    seg_hex, offset = out.stdout.split()
    from proto_db.core.atom import AtomPointer
    first = AtomPointer(uuid.UUID(hex=seg_hex), int(offset))
    seg_file = os.path.join(path, seg_hex + ".seg")
    sizes = {n: os.path.getsize(os.path.join(path, n)) for n in (ROOTS_LOG, RANGES_LOG, MANIFEST)}
    seg_size = os.path.getsize(seg_file)
    store = FakeObjectStore()
    s = TieredStorage(path, store, **KW)
    for name, size in sizes.items():
        assert os.path.getsize(os.path.join(path, name)) == size - 13, name
    assert os.path.getsize(seg_file) == seg_size - 20
    assert s.get_atom(first) == {"i": 1}
    assert s.load_roots().accepted == (1, rec(1))
    s.log_root("accepted", 2, rec(2))
    s.store_commit([], rec(2), 2, [(uuid.UUID(int=10), 64, 96)])
    second = s.push_atom({"i": 2})
    s.flush()
    assert s.ship(force=True)
    data, _ = store.get(s.segment_key(first.segment))
    with open(seg_file, "rb") as f:
        assert data == f.read(), "the shipped segment differs from the trimmed local one"
    s.close()
    s = TieredStorage(path, store, **KW)
    assert s.load_roots().accepted == (2, rec(2))
    assert s.commit_ranges(rec(2)) == [(uuid.UUID(int=10), 64, 96)]
    assert second.segment in s._own and s.get_atom(second) == {"i": 2}
    s.close()


# -- mid-log corruption ------------------------------------------------------------------------------

@pytest.mark.parametrize("damage", ["payload_byte", "length_field", "crc_field"])
@pytest.mark.parametrize("adapter", ADAPTERS, ids=_ids)
def test_mid_log_corruption_raises_and_truncates_nothing(tmp_path, adapter, damage):
    """An invalid entry followed by valid ones is corruption, never a torn tail: the open
    raises ``ProtoCorruptionError`` and the file is unchanged, byte for byte."""
    a = adapter(tmp_path)
    h = a.open()
    for i in (1, 2, 3):
        a.append(h, i)
    a.close(h)
    path = a.file(h)
    with open(path, "rb") as f:
        data = bytearray(f.read())
    first = logfile.HEADER_SIZE
    if damage == "payload_byte":
        data[first + logfile.FRAME_OVERHEAD + 3] ^= 0x40
    elif damage == "length_field":
        data[first:first + 4] = (10 ** 6).to_bytes(4, "little")
    else:
        data[first + 5] ^= 0x01
    with open(path, "wb") as f:
        f.write(data)
    with pytest.raises(ProtoCorruptionError, match="corrupt"):
        a.open()
    with open(path, "rb") as f:
        assert f.read() == bytes(data), "a corrupt log was modified"


def test_a_roots_log_load_reports_mid_log_corruption(tmp_path):
    """``RootsLog.load`` (by path) raises on corruption that appears after the open, and
    ignores a torn tail (a poisoned handle's file)."""
    path = str(tmp_path / "n.roots")
    log = RootsLog(path)
    for i in (1, 2, 3):
        log.append("accepted", i, rec(i))
    with open(path, "ab") as f:
        f.write(logfile.encode_frame(b"q" * 41)[:9])
    assert log.load().accepted == (3, rec(3))
    with open(path, "r+b") as f:
        f.seek(logfile.HEADER_SIZE + logfile.FRAME_OVERHEAD + 2)
        f.write(b"\xee")
    with pytest.raises(ProtoCorruptionError):
        log.load()
    log.close()


# -- headers ----------------------------------------------------------------------------------------

def test_a_torn_header_is_a_new_log(tmp_path):
    """A header only partly written (or zero-filled) is a torn tail: the log is rewritten as new."""
    path = str(tmp_path / "n.roots")
    with open(path, "wb") as f:
        f.write(logfile.encode_header(b"ROOT")[:5] + bytes(30))
    log = RootsLog(path)
    assert log.load().accepted is None
    log.append("accepted", 1, rec(1))
    log.close()
    assert RootsLog(path).load().accepted == (1, rec(1))


def test_foreign_headers_and_versions_are_refused(tmp_path):
    """Another format version raises ``ProtoNotSupportedError``; a file of another format (the
    unframed log of 0f9bf1e, say) or another kind raises ``ProtoCorruptionError``.  Neither is
    modified."""
    cases = {
        "version": (b"PDB2LOG\0" + (logfile.LOG_FORMAT_VERSION + 1).to_bytes(2, "little") + b"ROOT"
                    + logfile.encode_frame(b"a" * 41), ProtoNotSupportedError),
        "kind": (logfile.encode_header(b"RANG") + logfile.encode_frame(b"a" * 41), ProtoCorruptionError),
        "unframed": (bytes([1]) + bytes(44) + b"\x01\x02\x03\x04", ProtoCorruptionError),
    }
    for name, (data, error) in cases.items():
        path = str(tmp_path / f"{name}.roots")
        with open(path, "wb") as f:
            f.write(data)
        with pytest.raises(error):
            RootsLog(path)
        with open(path, "rb") as f:
            assert f.read() == data, name


# -- own segments of the local WAL --------------------------------------------------------------------

def _wal(tmp_path, store=None):
    return TieredStorage(str(tmp_path / "s"), store or FakeObjectStore(), **KW)


def test_a_failed_segment_write_torn_tail_is_trimmed_on_reopen(tmp_path, monkeypatch):
    """The local WAL: a segment write tears (``pwrite`` writes part of a record, then
    ``ENOSPC``) and poisons the storage.  Reopened, the own segment is truncated to its last
    valid record (and ``fsync``-ed), so what is shipped is exactly the valid records; the
    earlier records stay readable and new records survive a restart."""
    store = FakeObjectStore()
    s = _wal(tmp_path, store)
    first = s.push_atom({"i": 1})
    s.flush()
    path = os.path.join(s.path, first.segment.hex + ".seg")
    valid = os.path.getsize(path)
    s.push_atom({"i": 2, "pad": "p" * 200})
    seg_fd = s._local._segments[0].fd
    state = {"armed": True}

    def pwrite(fd, data, offset):
        if state["armed"] and fd == seg_fd:
            state["armed"] = False
            REAL_PWRITE(fd, bytes(data)[:30], offset)
            raise OSError(errno.ENOSPC, "No space left on device (injected)")
        return REAL_PWRITE(fd, data, offset)

    monkeypatch.setattr(os, "pwrite", pwrite)
    with pytest.raises(ProtoUnexpectedError):
        s.flush()
    monkeypatch.undo()
    assert not state["armed"]
    try:
        s.close()
    except ProtoUnexpectedError:
        pass
    assert os.path.getsize(path) == valid + 30
    s = _wal(tmp_path, store)
    assert os.path.getsize(path) == valid, "the torn record was not truncated"
    assert s.get_atom(first) == {"i": 1}
    third = s.push_atom({"i": 3})
    s.flush()
    assert s.ship(force=True)
    data, _ = store.get(s.segment_key(first.segment))
    assert len(data) == valid
    s.close()
    s = _wal(tmp_path, store)
    assert s.get_atom(third) == {"i": 3} and s.get_atom(first) == {"i": 1}
    s.close()


@pytest.mark.parametrize("tail", ["partial_record", "zero_fill", "torn_header"])
def test_a_crash_torn_segment_is_trimmed_and_fsynced(tmp_path, monkeypatch, tail):
    """A crash leaves part of a record, zero bytes, or part of the header in an own segment:
    it is truncated (the header written again when it was torn) and ``fsync``-ed on open."""
    s = _wal(tmp_path)
    ptr = s.push_atom({"i": 1})
    s.flush()
    s.close()
    path = os.path.join(str(tmp_path / "s"), ptr.segment.hex + ".seg")
    valid = os.path.getsize(path)
    if tail == "partial_record":
        with open(path, "ab") as f:
            f.write(b"\x40\x00\x00\x00\x01\x02")
    elif tail == "zero_fill":
        with open(path, "ab") as f:
            f.write(bytes(4096))
    else:
        os.truncate(path, 5)
        valid = SEGMENT_HEADER_SIZE
    events = []
    monkeypatch.setattr(os, "ftruncate", lambda fd, n: (events.append(("ftruncate", fd)), REAL_FTRUNCATE(fd, n))[1])
    monkeypatch.setattr(os, "fsync", lambda fd: (events.append(("fsync", fd)), REAL_FSYNC(fd))[1])
    s = _wal(tmp_path)
    monkeypatch.undo()
    assert os.path.getsize(path) == valid
    cut = [i for i, e in enumerate(events) if e[0] == "ftruncate"]
    assert len(cut) == 1 and ("fsync", events[cut[0]][1]) in events[cut[0] + 1:]
    if tail != "torn_header":
        assert s.get_atom(ptr) == {"i": 1}
    s.close()


def test_mid_segment_corruption_raises_and_truncates_nothing(tmp_path):
    """An own segment whose first record is damaged while later records are valid is corrupt:
    opening raises ``ProtoCorruptionError`` and the segment is unchanged."""
    s = _wal(tmp_path)
    ptr = s.push_atom({"i": 1})
    for i in range(2, 5):
        s.push_atom({"i": i})
    s.flush()
    s.close()
    path = os.path.join(str(tmp_path / "s"), ptr.segment.hex + ".seg")
    with open(path, "rb") as f:
        data = bytearray(f.read())
    data[ptr.offset + 12] ^= 0x20
    with open(path, "wb") as f:
        f.write(data)
    with pytest.raises(ProtoCorruptionError, match="corrupt"):
        _wal(tmp_path)
    with open(path, "rb") as f:
        assert f.read() == bytes(data)
