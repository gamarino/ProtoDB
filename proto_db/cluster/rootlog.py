"""The order of cluster roots and the node's acceptor log (Task 8.3d, decision D46).

Root order
    Every root the cluster knows carries the term of the lease that replicated it, its
    *label*.  :func:`root_key` is the one ordering key of roots, used by every comparison
    in the coordinator, the persister, the root logs and the persisted-root writes:
    ``(label, epoch, segment, offset)``, a deterministic total order.  Two *different*
    roots never share ``(label, epoch)`` by construction: a label is a proposer-qualified
    ballot (its low bits are the proposer's rank, :mod:`proto_db.cluster.coordinator`), so
    two proposers never share one; a proposer uses a ballot only once a majority granted
    it, and every member persists its promise before granting, and a member whose log was
    lost grants, acknowledges and answers as a witness nothing until it has learned the
    highest promise and the highest accepted root from the cluster (the coordinator's
    rejoin), so the same proposer never wins the same ballot twice (even after losing its
    own disk); one holder replicates at most one root per epoch.
    :func:`compare_roots` asserts it and raises ``ProtoCorruptionError`` on a violation
    instead of breaking the tie silently.

The acceptor log
    What one node must remember across a crash to keep its promises (the acceptor rules
    of Paxos): the highest lease term it granted (*promise*), the highest root it accepted
    and the newest root it knows committed.  :class:`RootsLog` keeps them in a file of
    fixed-size, checksummed entries, appended and ``fsync``-ed per the durability mode; a
    promise is always made durable before the grant is sent, an accepted root before the
    acknowledgement.  A *rejoined* marker (written last by the coordinator's rejoin, or at
    a cluster's first start) tells a log that holds what the node knows from a log that is
    new, wiped, or whose rejoin was interrupted: only a marked log makes the node an
    acceptor and a witness (:attr:`LoggedRoots.empty` is True without it).  :class:`MemoryRootsLog` keeps them in memory, for storages that have
    no directory (``MemoryStorage``): such a cluster is not durable; the log survives a
    coordinator restart in the same process only.  Both refuse writes through a closed
    handle (``ProtoValidationError``), checked under the lock of the append, so a space that
    has closed its log never has a root logged, nor acknowledged, through it.

Failed writes poison the handle (8.3d re-review 9, B1; ruling R57)
    Any I/O error of a :class:`RootsLog` -- a write, an ``fsync``, a compaction's rewrite,
    rename, reopen or close of the replaced descriptor -- leaves the handle refusing every
    later write with ``ProtoUnexpectedError``: after a failed write the file may end in a torn
    entry, after which the decoder stops (a later entry would be lost on reload), and after a
    failed ``fsync`` what reached the disk is unknown.  The node must reopen its log (close
    and reopen the space) to act as an acceptor again.  A compaction opens and fully
    prepares the replacement file before it closes the old descriptor (the descriptor
    attribute is swapped, then the old one closed), so the handle never holds a closed
    descriptor number while it accepts writes: a number the operating system hands to
    another file is never written through it.

Thread-safety: both logs serialize their methods with a lock.
"""
import errno
import os
import struct
import threading
import uuid
import weakref
import zlib
from typing import NamedTuple, Optional

from ..errors import ProtoCorruptionError, ProtoUnexpectedError, ProtoValidationError, attach_secondary_errors
from ..storage.format import FORMAT_VERSION, RootRecord

__all__ = ["root_key", "compare_roots", "LoggedRoots", "RootsLog", "MemoryRootsLog", "memory_log_for",
           "write_all", "prepare_replacement"]

_ENTRY = struct.Struct("<BQQ16sQ")          # kind, label (or term), epoch, segment, offset
_CRC = struct.Struct("<I")
ENTRY_SIZE = _ENTRY.size + _CRC.size
KINDS = {"accepted": 1, "committed": 2, "promise": 3, "rejoined": 4}
_COMPACT_BYTES = 1024 * 1024
_O_CLOEXEC = getattr(os, "O_CLOEXEC", 0)
_O_BINARY = getattr(os, "O_BINARY", 0)


def write_all(fd: int, data) -> None:
    """Write every byte of ``data`` to the descriptor ``fd`` (opened with ``O_APPEND``): a
    short write is continued, a write that makes no progress raises ``OSError``."""
    view = memoryview(data)
    while view:
        n = os.write(fd, view)
        if n <= 0:
            raise OSError(errno.EIO, f"write returned {n}")
        view = view[n:]


def prepare_replacement(path: str, data: bytes, flags: int) -> int:
    """Replace the file ``path`` by one holding ``data`` and return a descriptor open on the
    new file (``flags`` plus ``O_APPEND``), ready for appends.

    ``data`` is written to ``path + ".tmp"`` through the descriptor returned, ``fsync``-ed,
    and only then renamed over ``path``; the descriptor follows the file through the rename.
    On any failure the descriptor is closed and the error raised: ``path`` is either intact
    or replaced by the complete new contents, and the caller's descriptor on the old file is
    untouched.  The caller swaps its descriptor attribute to the one returned before it closes
    the old one (8.3d re-review 9, B1), then makes the directory entry durable."""
    tmp = path + ".tmp"
    fd = os.open(tmp, flags | os.O_CREAT | os.O_TRUNC | os.O_APPEND, 0o644)
    try:
        write_all(fd, data)
        os.fsync(fd)
        os.replace(tmp, path)
    except BaseException as e:
        try:
            os.close(fd)
        except OSError as close_error:
            attach_secondary_errors(e, [close_error])
        raise
    return fd


def root_key(label: int, record: Optional[RootRecord]) -> tuple:
    """The ordering key of the root ``record`` (``None``: the empty space) labelled ``label``:
    ``(label, epoch, segment bytes, offset)``."""
    if record is None:
        return label, 0, b"", 0
    return label, record.epoch, record.segment.bytes, record.offset


def compare_roots(label_a: int, record_a: Optional[RootRecord], label_b: int,
                  record_b: Optional[RootRecord]) -> int:
    """``-1``, ``0`` or ``1`` as root ``a`` orders before, equal to or after root ``b``.

    Raises ``ProtoCorruptionError`` when two different roots share ``(label, epoch)``: the
    protocol makes that impossible (see the module docstring), so it is reported, never
    broken as a tie.
    """
    a, b = root_key(label_a, record_a), root_key(label_b, record_b)
    if a[:2] == b[:2] and a != b:
        raise ProtoCorruptionError(
            f"Protocol violation: two different roots carry label {label_a} and epoch {a[1]} "
            f"(segment {a[2].hex() or '-'} offset {a[3]} and segment {b[2].hex() or '-'} offset {b[3]})")
    return (a > b) - (a < b)


class LoggedRoots(NamedTuple):
    """What a node logged: ``accepted`` and ``committed`` as ``(label, RootRecord or None)`` or
    ``None``, ``top`` the highest label or term logged, ``promised`` the highest term granted,
    ``empty`` True when the log has no rejoined marker (a new or wiped disk, or a rejoin
    that did not complete): the node must rejoin before it acts as an acceptor."""

    accepted: Optional[tuple]
    committed: Optional[tuple]
    top: int
    promised: int
    empty: bool = True


class _Best:
    """Newest accepted and committed roots and highest promise (helper of both logs)."""

    def __init__(self):
        self.accepted = None
        self.committed = None
        self.promised = 0
        self.top = 0
        self.rejoined = False

    def add(self, kind: int, label: int, record: Optional[RootRecord]) -> None:
        if kind == 4:
            self.rejoined = True
            return
        self.top = max(self.top, label)
        if kind == 3:
            self.promised = max(self.promised, label)
            return
        current = self.accepted if kind == 1 else self.committed
        if current is None or compare_roots(label, record, current[0], current[1]) > 0:
            if kind == 1:
                self.accepted = (label, record)
            else:
                self.committed = (label, record)

    def result(self) -> LoggedRoots:
        return LoggedRoots(self.accepted, self.committed, self.top, self.promised, not self.rejoined)


def _check_kind(kind: str) -> int:
    code = KINDS.get(kind)
    if code is None:
        raise ProtoValidationError(f"Unknown root log entry kind {kind!r}")
    return code


def _encode(code: int, label: int, record: Optional[RootRecord]) -> bytes:
    body = _ENTRY.pack(code, label, record.epoch if record is not None else 0,
                       record.segment.bytes if record is not None else bytes(16),
                       record.offset if record is not None else 0)
    return body + _CRC.pack(zlib.crc32(body))


def _decode(data: bytes) -> LoggedRoots:
    best = _Best()
    for i in range(0, len(data) - ENTRY_SIZE + 1, ENTRY_SIZE):
        body = data[i:i + _ENTRY.size]
        (crc,) = _CRC.unpack_from(data, i + _ENTRY.size)
        if zlib.crc32(body) != crc:
            break                   # a torn tail: every entry before it is valid
        code, label, epoch, segment, offset = _ENTRY.unpack(body)
        record = None if epoch == 0 else RootRecord(FORMAT_VERSION, uuid.UUID(bytes=segment), offset, epoch)
        best.add(code, label, record)
    return best.result()


class RootsLog:
    """A node's acceptor log in the file ``path`` (see the module docstring).

    Writes are refused once the log is closed (``ProtoValidationError``) and once any write,
    ``fsync`` or compaction step failed (``ProtoUnexpectedError``; the handle is poisoned,
    module docstring).  Reads (:meth:`load`) go by path and keep working.

    Args:
        path: the log file (created with its directory if missing).
        durability: ``"fsync"`` (synced appends are ``fsync``-ed), ``"flush"`` or ``"none"``.
    """

    def __init__(self, path: str, *, durability: str = "fsync"):
        self._path = os.path.abspath(os.fspath(path))
        self._durability = durability
        self._lock = threading.Lock()
        self._closed = False
        self._failed = None           # the I/O error that poisoned this handle, if any
        self._promised = 0            # highest promise known durable here
        directory = os.path.dirname(self._path)
        try:
            created = not os.path.isdir(directory)
            os.makedirs(directory, exist_ok=True)
            exists = os.path.exists(self._path)
            self._fd = self._open()
            if (created or not exists) and durability == "fsync":
                self._fsync_dir(directory)
                if created:
                    self._fsync_dir(os.path.dirname(directory))
        except OSError as e:
            raise ProtoUnexpectedError(f"Cannot open the root log {self._path}: {e}") from e
        self._promised = self.load().promised

    @property
    def path(self) -> str:
        """The log file."""
        return self._path

    def _open(self) -> int:
        return os.open(self._path, os.O_RDWR | os.O_CREAT | os.O_APPEND | _O_BINARY | _O_CLOEXEC, 0o644)

    @staticmethod
    def _fsync_dir(directory: str) -> None:
        fd = os.open(directory, os.O_RDONLY | _O_CLOEXEC)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _check_writable(self) -> None:
        """Refuse a write through a closed or poisoned handle (lock held)."""
        if self._closed:
            raise ProtoValidationError(f"The root log {self._path} is closed")
        if self._failed is not None:
            raise ProtoUnexpectedError(
                f"The root log {self._path} refuses writes after an earlier I/O failure ({self._failed!r}); "
                f"reopen it (close and reopen the space) to write again")

    def append(self, kind: str, label: int, record: Optional[RootRecord], *, sync: bool) -> None:
        """Append an ``"accepted"`` or ``"committed"`` root (``fsync`` with ``sync``, per the
        durability mode).  Raises ``ProtoValidationError`` when the log is closed: an
        acknowledgement must never be sent for a root that was not logged."""
        entry = _encode(_check_kind(kind), label, record)
        with self._lock:
            self._check_writable()
            self._write(entry, sync)

    def promise(self, term: int) -> None:
        """Make the promise of ``term`` durable (per the durability mode) before it is sent.

        Raises ``ProtoValidationError`` when the log is closed and ``ProtoUnexpectedError`` when
        it is poisoned: a promise that cannot be recorded must not be made.
        """
        with self._lock:
            self._check_writable()
            if term <= self._promised:
                return              # a higher or equal promise is already durable
            self._write(_encode(3, term, None), True)
            self._promised = term

    def mark_rejoined(self) -> None:
        """Append the rejoined marker, durable per the durability mode (written last, after
        the promise and the roots the rejoin learnt)."""
        with self._lock:
            self._check_writable()
            self._write(_encode(4, 0, None), True)

    def _write(self, entry: bytes, sync: bool) -> None:
        """Append ``entry`` (lock held, handle writable).  Any failure poisons the handle: the
        file may end in a torn entry, or hold what an ``fsync`` could not confirm."""
        try:
            write_all(self._fd, entry)
            if sync and self._durability == "fsync":
                os.fsync(self._fd)
            if os.fstat(self._fd).st_size > _COMPACT_BYTES:
                self._compact()
        except OSError as e:
            self._failed = e
            raise ProtoUnexpectedError(f"Writing the root log {self._path} failed: {e}") from e
        except BaseException as e:
            self._failed = e
            raise

    def _read(self) -> LoggedRoots:
        with open(self._path, "rb") as f:
            return _decode(f.read())

    def _compact(self) -> None:
        """Rewrite the log with its newest entry of each kind (lock held)."""
        roots = self._read()
        data = b""
        if roots.accepted is not None:
            data += _encode(1, *roots.accepted)
        if roots.committed is not None:
            data += _encode(2, *roots.committed)
        if roots.promised:
            data += _encode(3, roots.promised, None)
        if not roots.empty:
            data += _encode(4, 0, None)                  # the rejoined marker, last
        # Open and fully prepare the replacement, swap, and only then close the old descriptor
        # (8.3d re-review 9, B1): the handle never holds a closed descriptor number.
        new = prepare_replacement(self._path, data, os.O_RDWR | _O_BINARY | _O_CLOEXEC)
        old, self._fd = self._fd, new
        os.close(old)
        self._fsync_dir(os.path.dirname(self._path))

    def load(self) -> LoggedRoots:
        """The roots and the promise logged (see :class:`LoggedRoots`)."""
        with self._lock:
            try:
                return self._read()
            except OSError as e:
                raise ProtoUnexpectedError(f"Reading the root log {self._path} failed: {e}") from e

    def close(self) -> None:
        """Close the file.  Idempotent."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            fd, self._fd = self._fd, -1
            try:
                os.close(fd)
            except OSError as e:
                raise ProtoUnexpectedError(f"Closing the root log {self._path} failed: {e}") from e


class _MemoryLogContents:
    """What one node's in-memory acceptor log holds, shared by every handle on it."""

    def __init__(self):
        self.lock = threading.Lock()
        self.best = _Best()


class MemoryRootsLog:
    """A handle on an acceptor log kept in memory (not durable; see the module docstring).

    Every handle :func:`memory_log_for` returns for one storage and node shares the same
    contents, like file handles on one :class:`RootsLog` file.  A closed handle refuses
    writes (``ProtoValidationError``), exactly as a closed :class:`RootsLog` does, and the
    check is made under the lock of the append: a write through a handle whose space has
    closed never lands, so an acknowledgement is never sent for it (8.3d re-review 7, N1).

    Args:
        contents: the shared contents (a new, empty log when omitted).
    """

    def __init__(self, contents: Optional[_MemoryLogContents] = None):
        self._contents = contents if contents is not None else _MemoryLogContents()
        self._closed = False

    def _add(self, code: int, label: int, record: Optional[RootRecord]) -> None:
        with self._contents.lock:
            if self._closed:
                raise ProtoValidationError("This in-memory root log handle is closed")
            self._contents.best.add(code, label, record)

    def append(self, kind: str, label: int, record: Optional[RootRecord], *, sync: bool) -> None:
        """Record an ``"accepted"`` or ``"committed"`` root.  Raises ``ProtoValidationError``
        when this handle is closed."""
        self._add(_check_kind(kind), label, record)

    def promise(self, term: int) -> None:
        """Record the promise of ``term``.  Raises ``ProtoValidationError`` when this handle
        is closed."""
        self._add(3, term, None)

    def mark_rejoined(self) -> None:
        """Record the rejoined marker.  Raises ``ProtoValidationError`` when this handle is
        closed."""
        self._add(4, 0, None)

    def load(self) -> LoggedRoots:
        """The roots and the promise recorded (see :class:`LoggedRoots`)."""
        with self._contents.lock:
            return self._contents.best.result()

    def close(self) -> None:
        """Refuse every later write through this handle (the contents live on, as long as their
        storage object, for the other handles).  Idempotent."""
        with self._contents.lock:
            self._closed = True


_memory_logs = weakref.WeakKeyDictionary()      # storage object -> {node id: _MemoryLogContents}
_memory_lock = threading.Lock()


def memory_log_for(storage, node_id: str) -> MemoryRootsLog:
    """A new handle on the in-memory acceptor log of node ``node_id`` for ``storage`` (a
    storage without a directory).  The log is one per storage object and node, so it
    survives a coordinator restart in the same process, like a disk, but not the process;
    each handle closes on its own (see :class:`MemoryRootsLog`)."""
    with _memory_lock:
        logs = _memory_logs.get(storage)
        if logs is None:
            logs = _memory_logs[storage] = {}
        contents = logs.get(node_id)
        if contents is None:
            contents = logs[node_id] = _MemoryLogContents()
    return MemoryRootsLog(contents)
