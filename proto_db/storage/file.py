"""Durable file storage of ProtoDB 2.0 (spec sections 6 and 7, decision D19).

Layout of a space directory::

    space.root        published root (crc32 | msgpack), replaced atomically
    space.root.tmp    scratch file of an in-progress publish; ignored when found at open
    space.lock        target of the OS advisory lock taken by ``root_lock``
    <uuid4-hex>.seg   append-only segments (header, then records)

Write path: ``push_atom``/``push_blob`` frame the record and append it to an in-memory
write buffer, returning its pointer at once; buffered records are read from memory.
``flush`` writes the buffer with positioned writes into segments this instance created
(it never appends to a segment it did not create, nor to one it has rolled away from) and
makes it durable according to ``durability``.  ``publish_root`` flushes first, so data
always reaches the disk strictly before the root that references it.

Durability modes (D6, D19):

* ``"fsync"`` (default): segment data, new segment directory entries, the root file and
  the root rename are all fsynced; a returned ``publish_root`` survives power loss.
* ``"flush"``: segment data is handed to the OS without fsync (it survives a process
  crash, not a power loss); the root write keeps its fsyncs.
* ``"none"``: no fsync at all; for tests and benchmarks.

Segments are write-once per instance: each instance writes only segments it created
(``O_EXCL``) and never reopens one for writing, so a torn record a failed write or a crash
left at the end of a segment is never followed by a later append; every record is checked
against its CRC32 when read.  Segments are not trimmed at open: the directory is shared by
processes, and a segment another live process is writing must never be truncated
(:class:`~proto_db.storage.tiered.TieredStorage`, whose directory one node owns, trims its
own segments; ruling R58).

Failure policy: an ``OSError`` while writing segment data or the root raises
``ProtoUnexpectedError`` and leaves the instance *failed*: further pushes, flushes and
publishes raise ``ProtoUnexpectedError`` (reads keep working).  This prevents a later
publish from referencing records whose write was lost; recover by opening a new instance.
After a failure during the root rename or the directory fsync the outcome of that
publish is unknown until the space is reopened.

Thread-safety: every public method may be called from any thread.  One lock guards the
write buffer and segment bookkeeping, one serializes flushes, the page cache has its own
lock, and ``root_lock`` combines a per-instance ``threading.Lock`` with an OS file lock.
Nothing is written once ``close()`` has started: segment data and the root are written under
the flush lock, which ``close()`` takes first, after a closed check made under it; read
descriptors are closed only once no file read is in progress, and a read checks the closed
flag under the state lock before it uses one.
"""
import contextlib
import os
import threading
import time
import uuid
from typing import Optional

import msgpack

from ..core.atom import AtomPointer
from ..errors import (ProtoClosedError, ProtoConflictError, ProtoCorruptionError, ProtoLockingError,
                      ProtoNotSupportedError, ProtoTimeoutError, ProtoUnexpectedError,
                      ProtoValidationError)
from .base import SharedStorage, check_lock_timeout
from .cache import BoundedCache
from .format import (FORMAT_VERSION, RECORD_HEADER_SIZE, SEGMENT_HEADER_SIZE, RecordKind,
                     RootRecord, decode_root, decode_segment_header, encode_record,
                     encode_root, encode_segment_header, read_record, record_size)

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows
    fcntl = None
try:
    import msvcrt
except ImportError:  # pragma: no cover - POSIX
    msvcrt = None

__all__ = ["FileStorage", "OsFileSystem", "DURABILITY_MODES"]

MIB = 1024 * 1024
DURABILITY_MODES = ("fsync", "flush", "none")
ROOT_FILE = "space.root"
ROOT_TMP_FILE = "space.root.tmp"
LOCK_FILE = "space.lock"
SEGMENT_SUFFIX = ".seg"

_O_BINARY = getattr(os, "O_BINARY", 0)
_O_CLOEXEC = getattr(os, "O_CLOEXEC", 0)
_SEGMENT_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | _O_BINARY | _O_CLOEXEC
_ROOT_TMP_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | _O_BINARY | _O_CLOEXEC
_READ_FLAGS = os.O_RDONLY | _O_BINARY | _O_CLOEXEC
_DIRECT_READ_PAGES = 4
_seek_lock = threading.Lock()


def _pread(fd: int, size: int, offset: int) -> bytes:
    """Positioned read of up to ``size`` bytes (may return fewer at end of file)."""
    if hasattr(os, "pread"):
        return os.pread(fd, size, offset)
    with _seek_lock:  # pragma: no cover - platforms without pread
        os.lseek(fd, offset, os.SEEK_SET)
        return os.read(fd, size)


def _flock_try(fd: int) -> bool:
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return False
    return True


def _flock_release(fd: int) -> None:
    fcntl.flock(fd, fcntl.LOCK_UN)


def _msvcrt_try(fd: int) -> bool:  # pragma: no cover - Windows
    os.lseek(fd, 0, os.SEEK_SET)
    try:
        msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
    except PermissionError:
        return False
    return True


def _msvcrt_release(fd: int) -> None:  # pragma: no cover - Windows
    os.lseek(fd, 0, os.SEEK_SET)
    msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)


if fcntl is not None:
    _PROCESS_LOCK = (_flock_try, _flock_release)
elif msvcrt is not None:  # pragma: no cover - Windows
    _PROCESS_LOCK = (_msvcrt_try, _msvcrt_release)
else:  # pragma: no cover - no advisory locking available
    _PROCESS_LOCK = None


class OsFileSystem:
    """The file-system operations FileStorage performs on its write path.

    Testing seam: ``FileStorage(path, _fs=adapter)`` accepts any object with these
    methods, so crash tests can inject failures at each write, fsync, rename and
    directory fsync.  Reads do not go through the adapter.
    """

    def open(self, path: str, flags: int, mode: int = 0o644) -> int:
        """Open ``path`` with ``os.open`` flags and return the descriptor."""
        return os.open(path, flags, mode)

    def write(self, fd: int, data, offset: int) -> int:
        """Write ``data`` at ``offset``; return the number of bytes written (may be short)."""
        if hasattr(os, "pwrite"):
            return os.pwrite(fd, data, offset)
        os.lseek(fd, offset, os.SEEK_SET)  # pragma: no cover - descriptor is not shared
        return os.write(fd, data)  # pragma: no cover

    def fsync(self, fd: int) -> None:
        """Flush the file's data and metadata to stable storage."""
        os.fsync(fd)

    def close(self, fd: int) -> None:
        """Close a descriptor returned by :meth:`open`."""
        os.close(fd)

    def replace(self, src: str, dst: str) -> None:
        """Atomically rename ``src`` over ``dst``."""
        os.replace(src, dst)

    def fsync_dir(self, path: str) -> None:
        """Make the directory entries of ``path`` durable (no-op where unsupported)."""
        if os.name == "nt":  # pragma: no cover - directories cannot be fsynced on Windows
            return
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


class _Segment:
    """Bookkeeping of a segment created by this instance.

    ``end`` (guarded by the state lock) is the logical end including buffered bytes;
    ``written``/``synced`` (guarded by the flush lock) are the bytes handed to the OS and
    the bytes known durable.
    """

    __slots__ = ("uuid", "path", "fd", "end", "written", "synced", "sealed")

    def __init__(self, segment_uuid: uuid.UUID, path: str):
        self.uuid = segment_uuid
        self.path = path
        self.fd = None
        self.end = 0
        self.written = 0
        self.synced = 0
        self.sealed = False


class _Chunk:
    """Contiguous buffered bytes of one segment starting at ``start``."""

    __slots__ = ("segment", "start", "data")

    def __init__(self, segment: _Segment, start: int):
        self.segment = segment
        self.start = start
        self.data = bytearray()


class FileStorage(SharedStorage):
    """Durable :class:`SharedStorage` over a directory of segments (see module docstring).

    Args:
        path: space directory; created if missing.
        durability: ``"fsync"``, ``"flush"`` or ``"none"``.
        cache_bytes: capacity of the page cache serving reads of written segments.
        lock_timeout: default seconds ``root_lock`` (and ``publish_root``) wait; finite and
            positive (:func:`~proto_db.storage.base.check_lock_timeout`).
        segment_max_bytes: a segment rolls over to a new one before exceeding this size
            (a single larger record gets a segment of its own).
        page_size: page size of the read cache.
        write_buffer_bytes: buffered bytes beyond which a push writes the buffer out
            (without fsync) to bound memory; durability still comes from ``flush``.
        _fs: testing seam; a file-system adapter with the :class:`OsFileSystem` methods.

    Opening validates the published root: a corrupt or truncated ``space.root``, a
    missing segment or a damaged record at the root pointer raises
    ``ProtoCorruptionError``.  A missing ``space.root`` is an empty space.
    """

    def __init__(self, path: str, durability: str = "fsync", cache_bytes: int = 64 * MIB,
                 lock_timeout: float = 30.0, *, segment_max_bytes: int = 256 * MIB,
                 page_size: int = 16 * 1024, write_buffer_bytes: int = 8 * MIB, _fs=None):
        if durability not in DURABILITY_MODES:
            raise ProtoValidationError(f"durability must be one of {DURABILITY_MODES}")
        if type(cache_bytes) is not int or cache_bytes < 0:
            raise ProtoValidationError("cache_bytes must be a non-negative int")
        check_lock_timeout(lock_timeout)
        if type(segment_max_bytes) is not int or segment_max_bytes <= SEGMENT_HEADER_SIZE + RECORD_HEADER_SIZE:
            raise ProtoValidationError("segment_max_bytes is too small")
        if type(page_size) is not int or page_size <= 0:
            raise ProtoValidationError("page_size must be a positive int")
        if type(write_buffer_bytes) is not int or write_buffer_bytes <= 0:
            raise ProtoValidationError("write_buffer_bytes must be a positive int")
        fs = OsFileSystem() if _fs is None else _fs
        if not all(callable(getattr(fs, m, None))
                   for m in ("open", "write", "fsync", "close", "replace", "fsync_dir")):
            raise ProtoValidationError("_fs must provide the OsFileSystem methods")
        if _PROCESS_LOCK is None:
            raise ProtoNotSupportedError(
                "No OS advisory file locking is available; FileStorage cannot lock the root")
        path = os.path.abspath(os.fspath(path))
        if os.path.exists(path) and not os.path.isdir(path):
            raise ProtoValidationError(f"{path} exists and is not a directory")
        try:
            os.makedirs(path, exist_ok=True)
        except OSError as e:
            raise ProtoUnexpectedError(f"Cannot create space directory {path}: {e}") from e

        self._path = path
        self._root_path = os.path.join(path, ROOT_FILE)
        self._tmp_path = os.path.join(path, ROOT_TMP_FILE)
        self._lock_path = os.path.join(path, LOCK_FILE)
        self._durability = durability
        self._lock_timeout = lock_timeout
        self._segment_max = segment_max_bytes
        self._page_size = page_size
        self._write_buffer_bytes = write_buffer_bytes
        self._fs = fs
        self._cache = BoundedCache(cache_bytes)

        self._state_lock = threading.Lock()
        self._idle = threading.Condition(self._state_lock)
        self._flush_lock = threading.Lock()
        self._fd_lock = threading.Lock()
        self._thread_lock = threading.Lock()
        self._holder = None
        self._current = None          # segment receiving appends
        self._segments = []           # own segments with an open write descriptor or pending data
        self._pending = []            # chunks not yet handed to a flush
        self._pending_bytes = 0
        self._flushing = []           # chunks a flush is writing; still served from memory
        self._dir_dirty = False       # a segment file was created since the last dir fsync
        self._unsynced_closed = set() # paths of segments closed with data no fsync covered
        self._read_fds = {}
        self._readers = 0
        self._failed = None
        self._closed = False

        opened = False
        try:
            root = self._load_root()
            if root is not None:
                self._fetch_record(root.pointer)
            opened = True
        finally:
            if not opened:
                self._close_read_fds()

    @classmethod
    def open_existing(cls, path: str, **kwargs) -> "FileStorage":
        """Open the space at ``path``, which must already exist (``ProtoValidationError`` otherwise)."""
        if not os.path.isdir(path):
            raise ProtoValidationError(f"No space directory at {path}")
        return cls(path, **kwargs)

    @property
    def path(self) -> str:
        """The space directory."""
        return self._path

    @property
    def durability(self) -> str:
        """The configured durability mode."""
        return self._durability

    def cache_stats(self) -> dict:
        """Counters of the page cache that serves reads of written segments (see
        :meth:`~proto_db.storage.cache.BoundedCache.stats`); informational, used by the
        performance suite to report hit rates."""
        return self._cache.stats()

    # --- state checks --------------------------------------------------------------

    def _check_open(self):
        if self._closed:
            raise ProtoClosedError("Storage is closed")

    def _check_writable(self):
        self._check_open()
        if self._failed is not None:
            raise ProtoUnexpectedError(
                f"An earlier write to {self._path} failed ({self._failed}); open a new storage instance")

    # --- write path ----------------------------------------------------------------

    def _start_segment(self) -> _Segment:
        """Seal the current segment and start a new one (state lock held)."""
        if self._current is not None:
            self._current.sealed = True
        seg_uuid = uuid.uuid4()
        seg = _Segment(seg_uuid, os.path.join(self._path, seg_uuid.hex + SEGMENT_SUFFIX))
        chunk = _Chunk(seg, 0)
        chunk.data += encode_segment_header(seg_uuid)
        seg.end = len(chunk.data)
        self._pending.append(chunk)
        self._pending_bytes += len(chunk.data)
        self._segments.append(seg)
        self._current = seg
        return seg

    def _append(self, kind: RecordKind, payload: bytes) -> AtomPointer:
        ptr, spill = self._reserve(encode_record(kind, payload))
        if spill:
            self._write_out(sync=False)
        return ptr

    def _reserve(self, record: bytes) -> tuple:
        """Buffer one framed record and assign its offset; return ``(pointer, spill)``, where
        ``spill`` says the buffer reached its size and should be written out."""
        with self._state_lock:
            self._check_writable()
            seg = self._current
            if seg is None or (seg.end > SEGMENT_HEADER_SIZE
                               and seg.end + len(record) > self._segment_max):
                seg = self._start_segment()
            if not self._pending or self._pending[-1].segment is not seg:
                self._pending.append(_Chunk(seg, seg.end))
            offset = seg.end
            self._pending[-1].data += record
            seg.end += len(record)
            self._pending_bytes += len(record)
            spill = self._pending_bytes >= self._write_buffer_bytes
        return AtomPointer(seg.uuid, offset), spill

    def _write_all(self, fd: int, data, offset: int) -> None:
        view = memoryview(data)
        while view:
            n = self._fs.write(fd, view, offset)
            if not n or n < 0:
                raise OSError(f"write returned {n!r}")
            view = view[n:]
            offset += n

    def _write_out(self, sync: bool) -> None:
        """Write every buffered chunk; fsync segments and the directory when ``sync``."""
        with self._flush_lock:
            with self._state_lock:
                self._check_writable()
                chunks, self._pending = self._pending, []
                self._pending_bytes = 0
                self._flushing = chunks
            try:
                for chunk in chunks:
                    seg = chunk.segment
                    if seg.fd is None:
                        seg.fd = self._fs.open(seg.path, _SEGMENT_FLAGS, 0o644)
                        self._dir_dirty = True
                    self._write_all(seg.fd, chunk.data, chunk.start)
                    seg.written = chunk.start + len(chunk.data)
                if sync:
                    with self._state_lock:
                        segments = list(self._segments)
                    for seg in segments:
                        if seg.fd is not None and seg.synced < seg.written:
                            self._fs.fsync(seg.fd)
                            seg.synced = seg.written
                    if self._dir_dirty:
                        self._fs.fsync_dir(self._path)
                        self._dir_dirty = False
                self._retire_sealed_segments()
            except OSError as e:
                with self._state_lock:
                    self._failed = e
                raise ProtoUnexpectedError(f"Writing segment data to {self._path} failed: {e}") from e
            with self._state_lock:
                self._flushing = []

    def _retire_sealed_segments(self) -> None:
        """Close write descriptors of sealed segments that have nothing left to write or sync."""
        need_sync = self._durability == "fsync"
        with self._state_lock:
            done = [s for s in self._segments
                    if s.sealed and s.written == s.end and (s.synced == s.written or not need_sync)]
            for seg in done:
                self._segments.remove(seg)
                if seg.synced < seg.written:
                    self._unsynced_closed.add(seg.path)     # a later sync() reaches it by path
        for seg in done:
            if seg.fd is not None:
                fd, seg.fd = seg.fd, None
                self._fs.close(fd)

    def push_atom(self, payload: dict) -> AtomPointer:
        """See :meth:`SharedStorage.push_atom`."""
        if not isinstance(payload, dict):
            raise ProtoValidationError("Atom payload must be a dict")
        self._check_writable()
        return self._append(RecordKind.ATOM, msgpack.packb(payload, use_bin_type=True))

    def push_blob(self, data: bytes) -> AtomPointer:
        """See :meth:`SharedStorage.push_blob`."""
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise ProtoValidationError("Blob data must be bytes")
        self._check_writable()
        return self._append(RecordKind.BLOB, bytes(data))

    def flush(self) -> None:
        """Write the buffer and make it durable according to the durability mode."""
        self._check_open()
        self._write_out(sync=self._durability == "fsync")

    def sync(self) -> None:
        """Write the buffer and ``fsync`` every segment written since its last sync, and the
        directory, whatever the durability mode: the checkpoint of a cluster tier (Task 8.3f
        phase B), which must make its own segments durable before its acceptor log drops the
        records it holds for them."""
        self._check_open()
        self._write_out(sync=True)
        with self._flush_lock:
            with self._state_lock:
                self._check_writable()
                paths, self._unsynced_closed = self._unsynced_closed, set()
            try:
                for path in sorted(paths):
                    try:
                        fd = self._fs.open(path, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0), 0o644)
                    except FileNotFoundError:
                        continue            # released meanwhile: nothing left to sync
                    try:
                        self._fs.fsync(fd)
                    finally:
                        self._fs.close(fd)
                self._fs.fsync_dir(self._path)
            except OSError as e:
                with self._state_lock:
                    self._failed = e
                    self._unsynced_closed |= paths
                raise ProtoUnexpectedError(f"Syncing segment data of {self._path} failed: {e}") from e

    def append_record(self, kind: RecordKind, payload: bytes) -> AtomPointer:
        """Append an already encoded record payload of ``kind`` (``push_atom`` without the msgpack step).

        Used by :class:`~proto_db.storage.tiered.TieredStorage`, which keeps the framed bytes.
        """
        if not isinstance(payload, (bytes, bytearray, memoryview)):
            raise ProtoValidationError("Record payload must be bytes")
        self._check_writable()
        return self._append(RecordKind(kind), bytes(payload))

    def reserve_framed(self, record: bytes) -> tuple:
        """Append a record already framed by :func:`~proto_db.storage.format.encode_record`
        without writing out a full buffer: return ``(pointer, spill)``; when ``spill`` is true
        the caller calls :meth:`spill`, outside any lock of its own.  Used by
        :class:`~proto_db.storage.tiered.TieredStorage`, which orders its bookkeeping with the
        offset assignment and keeps encoding and buffer I/O out of that step."""
        if not isinstance(record, bytes):
            raise ProtoValidationError("A framed record must be bytes")
        self._check_writable()
        return self._reserve(record)

    def spill(self) -> None:
        """Write the buffered records out (no sync): the deferred half of
        :meth:`reserve_framed`."""
        self._write_out(sync=False)

    @property
    def current_segment(self) -> Optional[uuid.UUID]:
        """UUID of the segment receiving appends, or ``None`` before the first append or after
        :meth:`roll_segment`."""
        with self._state_lock:
            return self._current.uuid if self._current is not None else None

    def roll_segment(self) -> Optional[uuid.UUID]:
        """Seal the segment receiving appends (the next append starts a new one); return the
        sealed segment's UUID, or ``None`` when no segment holds records.

        A sealed segment is never written again once the buffer is flushed, so its file
        can be copied elsewhere whole (the tiered storage ships it to an object store).
        """
        with self._state_lock:
            self._check_writable()
            seg = self._current
            if seg is None or seg.end <= SEGMENT_HEADER_SIZE:
                return None
            seg.sealed = True
            self._current = None
            return seg.uuid

    # --- read path -----------------------------------------------------------------

    def _buffered(self, ptr: AtomPointer) -> Optional[bytes]:
        """Return the framed record at ``ptr`` if it is still in memory (state lock held)."""
        for chunk in self._flushing + self._pending:
            if chunk.segment.uuid != ptr.segment:
                continue
            rel = ptr.offset - chunk.start
            if 0 <= rel < len(chunk.data):
                data = chunk.data
                if rel + RECORD_HEADER_SIZE > len(data):
                    raise ProtoCorruptionError(f"No record at {ptr}")
                end = rel + record_size(data[rel:rel + RECORD_HEADER_SIZE], ptr)
                if end > len(data):
                    raise ProtoCorruptionError(f"No record at {ptr}")
                return bytes(data[rel:end])
        return None

    def _fetch_record(self, ptr: AtomPointer) -> tuple:
        """Return ``(RecordKind, payload)`` of the verified record at ``ptr``."""
        if ptr.offset < SEGMENT_HEADER_SIZE:
            raise ProtoCorruptionError(f"No record at {ptr}")
        with self._state_lock:
            self._check_open()
            raw = self._buffered(ptr)
            if raw is None:
                self._readers += 1
        if raw is not None:
            return read_record(raw, 0)
        try:
            fd = self._segment_fd(ptr.segment)
            try:
                return self._read_file_record(ptr, fd, use_cache=True)
            except ProtoCorruptionError:
                # A cached page may predate a concurrent append; verify against the file.
                return self._read_file_record(ptr, fd, use_cache=False)
        finally:
            with self._state_lock:
                self._readers -= 1
                if not self._readers:
                    self._idle.notify_all()

    def _fetch(self, ptr: AtomPointer, kind: RecordKind) -> bytes:
        if not isinstance(ptr, AtomPointer):
            raise ProtoValidationError("Expected an AtomPointer")
        found, payload = self._fetch_record(ptr)
        if found != kind:
            raise ProtoCorruptionError(f"Record at {ptr} is a {found.name}, not a {kind.name}")
        return payload

    def _segment_fd(self, segment: uuid.UUID) -> int:
        """Return a cached read descriptor of ``segment``, validating its header on first use."""
        with self._fd_lock:
            fd = self._read_fds.get(segment)
            if fd is not None:
                return fd
            path = os.path.join(self._path, segment.hex + SEGMENT_SUFFIX)
            try:
                fd = os.open(path, _READ_FLAGS)
            except FileNotFoundError as e:
                raise ProtoCorruptionError(f"Segment {segment.hex} is missing") from e
            except OSError as e:
                raise ProtoUnexpectedError(f"Cannot open segment {segment.hex}: {e}") from e
            valid = False
            try:
                if decode_segment_header(_pread(fd, SEGMENT_HEADER_SIZE, 0)) != segment:
                    raise ProtoCorruptionError(f"Segment file {segment.hex} has a foreign header")
                valid = True
            finally:
                if not valid:
                    os.close(fd)
            self._read_fds[segment] = fd
            return fd

    def _read_range(self, segment: uuid.UUID, fd: int, offset: int, size: int,
                    use_cache: bool) -> bytes:
        """Read ``size`` bytes at ``offset`` through the page cache (short at end of file)."""
        page = self._page_size
        if size > _DIRECT_READ_PAGES * page:
            return _pread(fd, size, offset)
        first, last = offset // page, (offset + size - 1) // page
        parts = []
        for page_no in range(first, last + 1):
            key = (segment, page_no)
            data = self._cache.get(key) if use_cache else None
            if data is None:
                data = _pread(fd, page, page_no * page)
                if len(data) == page:   # append-only: only complete pages are stable
                    self._cache.put(key, data)
            parts.append(data)
            if len(data) < page:
                break
        rel = offset - first * page
        return b"".join(parts)[rel:rel + size]

    def _read_file_record(self, ptr: AtomPointer, fd: int, use_cache: bool) -> tuple:
        try:
            header = self._read_range(ptr.segment, fd, ptr.offset, RECORD_HEADER_SIZE, use_cache)
            if len(header) < RECORD_HEADER_SIZE:
                raise ProtoCorruptionError(f"Truncated record header at {ptr}")
            total = record_size(header, ptr)
            if total > _DIRECT_READ_PAGES * self._page_size and ptr.offset + total > os.fstat(fd).st_size:
                raise ProtoCorruptionError(f"Truncated record at {ptr}")
            return read_record(self._read_range(ptr.segment, fd, ptr.offset, total, use_cache), 0)
        except OSError as e:
            raise ProtoUnexpectedError(f"Reading {ptr} failed: {e}") from e

    def get_record(self, ptr: AtomPointer) -> tuple:
        """Return ``(RecordKind, payload)`` of the verified record at ``ptr``, whatever its kind.

        Raises ``ProtoCorruptionError`` for a missing segment or a record that fails its checks.
        """
        if not isinstance(ptr, AtomPointer):
            raise ProtoValidationError("Expected an AtomPointer")
        return self._fetch_record(ptr)

    def forget_segment(self, segment: uuid.UUID) -> None:
        """Close the cached read descriptor of ``segment`` (its file was removed or replaced).

        Waits for file reads in progress (a descriptor is never closed under a reader); a
        later read reopens the file, or fails with ``ProtoCorruptionError`` if it is gone.
        Cached pages of that segment are left to age out of the bounded cache.
        """
        with self._state_lock:
            while self._readers:
                self._idle.wait()
            with self._fd_lock:
                fd = self._read_fds.pop(segment, None)
            self._unsynced_closed.discard(os.path.join(self._path, segment.hex + SEGMENT_SUFFIX))
            if fd is not None:
                os.close(fd)

    def get_atom(self, ptr: AtomPointer) -> dict:
        """See :meth:`SharedStorage.get_atom`."""
        payload = self._fetch(ptr, RecordKind.ATOM)
        try:
            return msgpack.unpackb(payload, raw=False)
        except (msgpack.exceptions.UnpackException, ValueError) as e:
            raise ProtoCorruptionError(f"Undecodable atom at {ptr}") from e

    def get_blob(self, ptr: AtomPointer) -> bytes:
        """See :meth:`SharedStorage.get_blob`."""
        return self._fetch(ptr, RecordKind.BLOB)

    # --- root ----------------------------------------------------------------------

    def _load_root(self) -> Optional[RootRecord]:
        try:
            with open(self._root_path, "rb") as f:
                data = f.read()
        except FileNotFoundError:
            return None
        except OSError as e:
            raise ProtoUnexpectedError(f"Cannot read {self._root_path}: {e}") from e
        return decode_root(data)

    def read_root(self) -> Optional[RootRecord]:
        """Return the root currently published on disk (by any process), or ``None``."""
        self._check_open()
        return self._load_root()

    def _write_root(self, root: RootRecord) -> None:
        sync = self._durability != "none"
        fd = self._fs.open(self._tmp_path, _ROOT_TMP_FLAGS, 0o644)
        try:
            self._write_all(fd, encode_root(root), 0)
            if sync:
                self._fs.fsync(fd)
        finally:
            self._fs.close(fd)
        self._fs.replace(self._tmp_path, self._root_path)
        if sync:
            self._fs.fsync_dir(self._path)

    def publish_root(self, ptr: AtomPointer, expected_epoch: Optional[int]) -> RootRecord:
        """See :meth:`SharedStorage.publish_root`.

        Flushes first (data before root), checks that ``ptr`` names a valid record, then
        compares epochs and replaces ``space.root`` under the root lock (taken here unless
        the calling thread already holds it).
        """
        if not isinstance(ptr, AtomPointer):
            raise ProtoValidationError("Expected an AtomPointer")
        if expected_epoch is not None and (type(expected_epoch) is not int or expected_epoch < 0):
            raise ProtoValidationError("expected_epoch must be a non-negative int or None")
        self.flush()
        self._fetch_record(ptr)
        exclusive = (contextlib.nullcontext() if self._holder == threading.get_ident()
                     else self.root_lock())
        # The root is written under the flush lock, which close() takes: no root is written
        # once close() has started (8.3d re-review 8, A1).
        with exclusive, self._flush_lock:
            with self._state_lock:
                self._check_writable()
            current = self._load_root()
            epoch = current.epoch if current is not None else 0
            if expected_epoch is not None and expected_epoch != epoch:
                raise ProtoConflictError(f"Root epoch is {epoch}, expected {expected_epoch}")
            root = RootRecord(FORMAT_VERSION, ptr.segment, ptr.offset, epoch + 1)
            try:
                self._write_root(root)
            except OSError as e:
                with self._state_lock:
                    self._failed = e
                raise ProtoUnexpectedError(f"Publishing the root of {self._path} failed: {e}") from e
            return root

    # --- locking -------------------------------------------------------------------

    def root_lock(self, timeout: Optional[float] = None):
        """See :meth:`SharedStorage.root_lock`.

        Holds this instance's thread lock and an OS advisory lock on ``space.lock``
        (process exclusion; the descriptor is opened per acquisition and closed on release).
        """
        self._check_open()
        return self._locked(self._lock_timeout if timeout is None else check_lock_timeout(timeout, "timeout"))

    @contextlib.contextmanager
    def _locked(self, timeout: float):
        me = threading.get_ident()
        if self._holder == me:
            raise ProtoLockingError("root_lock is not re-entrant; it is already held by this thread")
        deadline = time.monotonic() + timeout
        if not self._thread_lock.acquire(timeout=timeout):
            raise ProtoTimeoutError(f"Could not acquire the root lock within {timeout} seconds")
        fd = None
        try:
            fd = self._acquire_process_lock(deadline, timeout)
        finally:
            if fd is None:
                self._thread_lock.release()
        self._holder = me
        try:
            yield
        finally:
            self._holder = None
            try:
                self._release_process_lock(fd)
            finally:
                self._thread_lock.release()

    def _acquire_process_lock(self, deadline: float, timeout: float) -> int:
        try_lock, _ = _PROCESS_LOCK
        try:
            fd = os.open(self._lock_path, os.O_RDWR | os.O_CREAT | _O_BINARY | _O_CLOEXEC, 0o644)
        except OSError as e:
            raise ProtoUnexpectedError(f"Cannot open {self._lock_path}: {e}") from e
        acquired = False
        try:
            delay = 0.001
            while True:
                try:
                    acquired = try_lock(fd)
                except OSError as e:
                    raise ProtoUnexpectedError(f"Cannot lock {self._lock_path}: {e}") from e
                if acquired:
                    return fd
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ProtoTimeoutError(
                        f"Could not acquire the root lock within {timeout} seconds "
                        "(held by another process or instance)")
                time.sleep(min(delay, remaining))
                delay = min(delay * 2, 0.05)
        finally:
            if not acquired:
                os.close(fd)

    def _release_process_lock(self, fd: int) -> None:
        _, release = _PROCESS_LOCK
        try:
            release(fd)
        except OSError as e:
            raise ProtoUnexpectedError(f"Cannot unlock {self._lock_path}: {e}") from e
        finally:
            os.close(fd)

    # --- shutdown ------------------------------------------------------------------

    def _close_read_fds(self) -> None:
        with self._fd_lock:
            fds, self._read_fds = list(self._read_fds.values()), {}
        for fd in fds:
            os.close(fd)

    def close(self) -> None:
        """Release descriptors and the cache.  Records not yet flushed are discarded.

        Idempotent; waits for in-flight flushes and file reads to finish.
        """
        with self._flush_lock:
            with self._state_lock:
                if self._closed:
                    return
                self._closed = True
                while self._readers:
                    self._idle.wait()
                self._pending, self._flushing = [], []
                segments, self._segments = self._segments, []
            error = None
            for seg in segments:
                if seg.fd is not None:
                    fd, seg.fd = seg.fd, None
                    try:
                        self._fs.close(fd)
                    except OSError as e:
                        error = error or e
            self._close_read_fds()
            self._cache.clear()
            if error is not None:
                raise ProtoUnexpectedError(f"Closing segments of {self._path} failed: {error}") from error
