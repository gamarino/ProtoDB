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
    and the newest root it knows committed.  :class:`RootsLog` keeps them in a framed log
    file (:mod:`proto_db.storage.logfile`: a versioned header, then fixed-size entries each
    framed with a CRC32), appended and ``fsync``-ed **whatever the data durability mode** of
    the storage it belongs to (Task 8.3e, defect D-SAFE-2): a promise is made durable before
    the grant is sent, an accepted root before the acknowledgement, and the rejoined marker
    before the node acts as an acceptor.  The storage's ``"flush"``/``"none"`` modes govern its
    data segments only, as Raft persists ``currentTerm``, ``votedFor`` and its log whatever
    else it caches.  A *committed* entry records knowledge, not a promise: like Raft's
    ``commitIndex`` (volatile state in Raft) nothing in the safety argument depends on it --
    committed knowledge learnt from grants, answers or the store is never logged at all -- so
    it is appended without its own ``fsync`` and becomes durable with the next synced entry;
    reads stay monotonic across a restart through the confirmation under a lease
    (:attr:`~proto_db.cluster.coordinator.ClusterCoordinator.needs_confirmation`).  Opening the log makes
    its file, its directory and that directory's parent entry durable too, so a log that
    holds a promise never vanishes from its directory in a power loss.  A *rejoined* marker (written last by the coordinator's rejoin, or at
    a cluster's first start) tells a log that holds what the node knows from a log that is
    new, wiped, or whose rejoin was interrupted: only a marked log makes the node an
    acceptor and a witness (:attr:`LoggedRoots.empty` is True without it).  :class:`MemoryRootsLog` keeps them in memory, for storages that have
    no directory (``MemoryStorage``): such a cluster is not durable and has no power-loss
    story at all; the log survives a coordinator restart in the same process only.  The
    storage, and so every node of such a cluster, lives in one process: the process's death
    ends the space, its data and every log together (Task 8.3e decision; coordinator module
    docstring, "The commit point").  Both refuse writes through a closed
    handle (``ProtoValidationError``), checked under the lock of the append, so a space that
    has closed its log never has a root logged, nor acknowledged, through it.

Self-describing acceptances (Task 8.3f phase B, D50)
    An acceptance of a replicated root is ONE entry of kind 5 (:meth:`RootsLog.append_records`):
    the root, its label, the permission ``seq``, the root it was built on (``pred``) and every
    record the replication carried, framed with the log's CRC and ``fsync``-ed once, before
    the acknowledgement.  The data files of the local tier (own segments, copies of other
    nodes' segments, ``ranges.log``) are then written without a sync: the storage rebuilds
    them from the kind-5 entries when it opens (*redo*), and syncs them in a *checkpoint*
    before any kind-5 entry loses its records.  So a root is durable exactly when its records
    are (defect D-AVAIL-1 closed by construction).  A kind-5 entry counts as an accepted root
    for every reader of the log (:class:`LoggedRoots`).  Compaction never drops the records of
    an entry that a checkpoint has not covered: the automatic compaction of :meth:`_write`
    runs only while no such entry exists, and :meth:`RootsLog.compact_records` summarizes only
    the entries before the mark its caller checkpointed (:meth:`RootsLog.mark`), keeping
    every later entry verbatim.  The mark carries the log's compaction generation: a mark
    taken before any compaction rewrote the file names no position in the new file, and
    :meth:`RootsLog.compact_records` then summarizes nothing (Task 8.3f-B review I1; every
    entry before such a mark was already summarized by that compaction).  Kinds 1 to 4 are unchanged; a reader older than kind 5
    refuses such a log with ``ProtoCorruptionError`` (an entry of an unexpected size), never
    misreads it, so the log's header version is unchanged.

Failed writes poison the handle (8.3d re-review 9, B1; ruling R57)
    Any I/O error of a :class:`RootsLog` -- a write, an ``fsync``, a compaction's rewrite,
    rename, reopen or close of the replaced descriptor -- leaves the handle refusing every
    later write with ``ProtoUnexpectedError``: after a failed write the file may end in a torn
    entry, and after a failed ``fsync`` what reached the disk is unknown.  The node must
    reopen its log (close and reopen the space) to act as an acceptor again.

Torn tails are trimmed on open (8.3d re-review 10, R2; ruling R58)
    Opening a :class:`RootsLog` truncates a torn tail -- what a failed append, a ``SIGKILL``
    or a power loss left after the last valid entry -- and ``fsync``-s the file before any
    append, so an entry appended after the reopen is never hidden behind torn bytes (the
    reader stops at the first invalid entry).  An invalid entry followed by valid ones is
    mid-log corruption: opening or loading the log raises ``ProtoCorruptionError`` and
    nothing is truncated (:func:`proto_db.storage.logfile.scan`).  A compaction opens and fully
    prepares the replacement file before it closes the old descriptor (the descriptor
    attribute is swapped, then the old one closed), so the handle never holds a closed
    descriptor number while it accepts writes: a number the operating system hands to
    another file is never written through it.

One live handle per log file (8.3d re-review 11, I1; ruling R59)
    Because opening trims, a second live handle on the same file could truncate an entry the
    first is still appending, which the first then completes past the cut; the next open
    would trim that acknowledged entry.  A :class:`RootsLog` therefore holds an exclusive
    :class:`~proto_db.storage.ownerlock.OwnerLock` on ``<path>.lock`` from before it opens the
    log until :meth:`RootsLog.close`; a second open, from this process or another one, raises
    ``ProtoLockingError``.  This also refuses two processes using the same ``node_id`` on a
    shared ``FileStorage`` (``cluster/<node>.roots``).  Aliases are refused too (8.3d
    re-review 12, M3): the path is resolved with ``os.path.realpath`` before the lock file is
    named, the lock registers the log file's device and inode in this process, and a log file
    with more than one hard link is refused before it is read or repaired
    (:func:`~proto_db.storage.logfile.open_log`).

Thread-safety: both logs serialize their methods with a lock.
"""
import os
import struct
import threading
import uuid
import weakref
from typing import NamedTuple, Optional

from ..errors import ProtoCorruptionError, ProtoUnexpectedError, ProtoValidationError, attach_secondary_errors
from ..storage.format import FORMAT_VERSION, RootRecord
from ..storage.ownerlock import OwnerLock
from ..storage.logfile import (FRAME_OVERHEAD, HEADER_SIZE, encode_frame, fsync_dir, log_contents, open_log,
                               prepare_replacement, read_log, write_all)

__all__ = ["root_key", "compare_roots", "LoggedRoots", "RedoEntry", "RootsLog", "MemoryRootsLog",
           "memory_log_for", "write_all", "prepare_replacement", "encode_records_entry", "decode_records_entry",
           "encode_replica_entry", "RECORDS_KIND", "REPLICA_KIND", "DATA_KINDS"]

_ENTRY = struct.Struct("<BQQ16sQ")          # kind, label (or term), epoch, segment, offset
#: Head of a kind-5 entry: kind, label, epoch, segment, offset, seq, pred label, pred epoch,
#: pred segment, pred offset, record count.
_RECORDS_HEAD = struct.Struct("<BQQ16sQQQQ16sQI")
_RECORD_ITEM = struct.Struct("<16sQI")      # segment, offset, framed length (then the framed bytes)
#: Kind code of an acceptance carrying its records (Task 8.3f phase B).
RECORDS_KIND = 5
#: Head of a kind-6 entry: kind, flags, label, epoch, segment, offset, seq, pred label, pred
#: epoch, pred segment, pred offset, range count, record count (Task 8.3g, D54).
_REPLICA_HEAD = struct.Struct("<BBQQ16sQQQQ16sQII")
_RANGE_ITEM = struct.Struct("<16sQQ")       # segment, start, end
#: Kind code of an entry naming its commit's byte ranges explicitly (Task 8.3g, D54): an
#: acceptance whose records live on the commit's data replicas (``ACCEPTED`` flag set; it may
#: carry none of them), or the records a data replica holds without accepting the root.
REPLICA_KIND = 6
#: Flag of a kind-6 entry: the entry is an accepted root.
ACCEPTED = 1
#: Kinds of entries carrying records (redone at open, kept until a checkpoint covers them).
DATA_KINDS = (RECORDS_KIND, REPLICA_KIND)
#: Size of one framed entry on disk (:mod:`proto_db.storage.logfile` framing).
ENTRY_SIZE = _ENTRY.size + FRAME_OVERHEAD
#: Kind of the acceptor log in its file header.
LOG_KIND = b"ROOT"
KINDS = {"accepted": 1, "committed": 2, "promise": 3, "rejoined": 4}
_COMPACT_BYTES = 1024 * 1024
_O_CLOEXEC = getattr(os, "O_CLOEXEC", 0)
_O_BINARY = getattr(os, "O_BINARY", 0)


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


def _payload(code: int, label: int, record: Optional[RootRecord]) -> bytes:
    return _ENTRY.pack(code, label, record.epoch if record is not None else 0,
                       record.segment.bytes if record is not None else bytes(16),
                       record.offset if record is not None else 0)


def _encode(code: int, label: int, record: Optional[RootRecord]) -> bytes:
    return encode_frame(_payload(code, label, record))


class RedoEntry(NamedTuple):
    """A kind-5 or kind-6 entry: the root (``label``, ``root``), its permission ``seq``, the
    root it was built on (``pred``: ``(label, RootRecord or None)`` or ``None``) and its
    ``records`` as ``[segment bytes, offset, framed record]``.  ``accepted`` is False for the
    records a data replica holds without accepting the root (kind 6, D54); ``ranges`` lists the
    commit's byte ranges ``[(segment UUID, start, end)]`` when the entry names them (kind 6),
    else ``None`` (kind 5: the ranges of its records)."""

    label: int
    root: Optional[RootRecord]
    seq: int
    pred: Optional[tuple]
    records: list
    accepted: bool = True
    ranges: Optional[list] = None


def _record_or_none(epoch: int, segment: bytes, offset: int) -> Optional[RootRecord]:
    return None if epoch == 0 else RootRecord(FORMAT_VERSION, uuid.UUID(bytes=segment), offset, epoch)


def encode_records_entry(label: int, root: Optional[RootRecord], seq: int, pred: Optional[tuple],
                         records: list) -> bytes:
    """The payload of a kind-5 entry (see the module docstring); ``records`` are
    ``[segment bytes, offset, framed record]`` items (an item whose record is ``None`` is
    skipped)."""
    items = [(raw, offset, data) for raw, offset, data in records if data is not None]
    pred_label, pred_root = pred if pred is not None else (0, None)
    parts = [_RECORDS_HEAD.pack(
        RECORDS_KIND, label, root.epoch if root is not None else 0,
        root.segment.bytes if root is not None else bytes(16), root.offset if root is not None else 0,
        seq, pred_label, pred_root.epoch if pred_root is not None else 0,
        pred_root.segment.bytes if pred_root is not None else bytes(16),
        pred_root.offset if pred_root is not None else 0, len(items))]
    for raw, offset, data in items:
        parts.append(_RECORD_ITEM.pack(bytes(raw), offset, len(data)))
        parts.append(bytes(data))
    return b"".join(parts)


def encode_replica_entry(label: int, root: Optional[RootRecord], seq: int, pred: Optional[tuple],
                         records: list, ranges: list, *, accepted: bool) -> bytes:
    """The payload of a kind-6 entry (Task 8.3g, D54): like kind 5, plus the commit's byte
    ``ranges`` (``[(segment UUID, start, end)]``) and whether the root is ``accepted``."""
    items = [(raw, offset, data) for raw, offset, data in records if data is not None]
    pred_label, pred_root = pred if pred is not None else (0, None)
    parts = [_REPLICA_HEAD.pack(
        REPLICA_KIND, ACCEPTED if accepted else 0, label, root.epoch if root is not None else 0,
        root.segment.bytes if root is not None else bytes(16), root.offset if root is not None else 0,
        seq, pred_label, pred_root.epoch if pred_root is not None else 0,
        pred_root.segment.bytes if pred_root is not None else bytes(16),
        pred_root.offset if pred_root is not None else 0, len(ranges), len(items))]
    for segment, start, end in ranges:
        parts.append(_RANGE_ITEM.pack(segment.bytes, start, end))
    for raw, offset, data in items:
        parts.append(_RECORD_ITEM.pack(bytes(raw), offset, len(data)))
        parts.append(bytes(data))
    return b"".join(parts)


def decode_records_entry(payload, path: str = "") -> RedoEntry:
    """Decode the payload of a kind-5 or kind-6 entry; ``ProtoCorruptionError`` when it is
    malformed (the frame's CRC matched, so only a writer bug or another format can cause it)."""
    view = memoryview(payload)
    ranges, accepted = None, True
    if len(view) and view[0] == REPLICA_KIND:
        if len(view) < _REPLICA_HEAD.size:
            raise ProtoCorruptionError(f"The root log {path} holds a short replica entry")
        (code, flags, label, epoch, segment, offset, seq, pred_label, pred_epoch, pred_segment, pred_offset,
         nranges, count) = _REPLICA_HEAD.unpack_from(view, 0)
        if flags & ~ACCEPTED:
            raise ProtoCorruptionError(f"The root log {path} holds a replica entry with unknown flags {flags}")
        accepted = bool(flags & ACCEPTED)
        pos = _REPLICA_HEAD.size
        ranges = []
        for _ in range(nranges):
            if pos + _RANGE_ITEM.size > len(view):
                raise ProtoCorruptionError(f"The root log {path} holds a replica entry cut inside a range")
            rseg, start, end = _RANGE_ITEM.unpack_from(view, pos)
            if start > end:
                raise ProtoCorruptionError(f"The root log {path} holds a replica entry with an inverted range")
            ranges.append((uuid.UUID(bytes=rseg), start, end))
            pos += _RANGE_ITEM.size
    else:
        if len(view) < _RECORDS_HEAD.size:
            raise ProtoCorruptionError(f"The root log {path} holds a short records entry")
        (code, label, epoch, segment, offset, seq, pred_label, pred_epoch, pred_segment, pred_offset,
         count) = _RECORDS_HEAD.unpack_from(view, 0)
        if code != RECORDS_KIND:
            raise ProtoCorruptionError(f"The root log {path} holds an entry of kind {code}, not {RECORDS_KIND}")
        pos = _RECORDS_HEAD.size
    records = []
    for _ in range(count):
        if pos + _RECORD_ITEM.size > len(view):
            raise ProtoCorruptionError(f"The root log {path} holds a records entry cut inside an item")
        raw, roff, length = _RECORD_ITEM.unpack_from(view, pos)
        pos += _RECORD_ITEM.size
        if pos + length > len(view):
            raise ProtoCorruptionError(f"The root log {path} holds a records entry cut inside a record")
        records.append([raw, roff, bytes(view[pos:pos + length])])
        pos += length
    if pos != len(view):
        raise ProtoCorruptionError(f"The root log {path} holds a records entry with trailing bytes")
    pred_root = _record_or_none(pred_epoch, pred_segment, pred_offset)
    pred = None if pred_label == 0 and pred_root is None else (pred_label, pred_root)
    return RedoEntry(label, _record_or_none(epoch, segment, offset), seq, pred, records, accepted, ranges)


def _decode(payloads: list, path: str) -> LoggedRoots:
    """The roots and the promise held by the entries ``payloads`` of the log at ``path``."""
    best = _Best()
    for payload in payloads:
        if len(payload) and payload[0] in DATA_KINDS:
            entry = decode_records_entry(payload, path)
            if entry.accepted:
                best.add(1, entry.label, entry.root)      # an accepted root, with its records
            continue
        if len(payload) != _ENTRY.size:
            raise ProtoCorruptionError(f"The root log {path} holds an entry of {len(payload)} bytes")
        code, label, epoch, segment, offset = _ENTRY.unpack(payload)
        if code not in (1, 2, 3, 4):
            raise ProtoCorruptionError(f"The root log {path} holds an entry of unknown kind {code}")
        record = None if epoch == 0 else RootRecord(FORMAT_VERSION, uuid.UUID(bytes=segment), offset, epoch)
        best.add(code, label, record)
    return best.result()


#: Suffix of the lock file held next to a :class:`RootsLog` file while it is open.
LOCK_SUFFIX = ".lock"


class RootsLog:
    """A node's acceptor log in the file ``path`` (see the module docstring).

    Writes are refused once the log is closed (``ProtoValidationError``) and once any write,
    ``fsync`` or compaction step failed (``ProtoUnexpectedError``; the handle is poisoned,
    module docstring).  Reads (:meth:`load`) go by path and keep working.

    Args:
        path: the log file (created with its directory if missing), resolved with
            ``os.path.realpath``.  ``<resolved path>.lock`` is held exclusively until
            :meth:`close`; a second open of the file while this handle is open, by any path
            (a symbolic or hard link included), raises ``ProtoLockingError`` (module docstring).

    Every promise, accepted root and rejoined marker is ``fsync``-ed before the call returns
    (committed entries ride on the next sync): there is no durability knob, because the acceptor state is
    what the cluster's safety argument rests on (module docstring; Task 8.3e, D-SAFE-2).
    """

    def __init__(self, path: str):
        # Resolved (8.3d re-review 12, M3): a symbolic link to the log names the same lock file,
        # and a compaction replaces the file itself, never the link.
        self._path = os.path.realpath(os.fspath(path))
        self._lock = threading.Lock()
        self._closed = False
        self._failed = None           # the I/O error that poisoned this handle, if any
        self._promised = 0            # highest promise known durable here
        self._pending = 0             # bytes of kind-5 entries no checkpoint has covered yet
        self._generation = 0          # bumped by every compaction: offsets restart (review I1)
        self._redo = []               # the kind-5 entries found at open (taken by take_redo)
        try:
            os.makedirs(os.path.dirname(self._path), exist_ok=True)
        except OSError as e:
            raise ProtoUnexpectedError(f"Cannot create the directory of the root log {self._path}: {e}") from e
        # Held for the handle's lifetime, taken before the open trims anything (ruling R59).
        self._owner = OwnerLock(self._path + LOCK_SUFFIX, f"the root log {self._path}", target=self._path)
        fd = -1
        try:
            try:
                fd, payloads = open_log(self._path, LOG_KIND, durability="fsync")
                # Whatever made the file (an older version writing without syncs included), its
                # contents, its name and its directory's name are durable from now on.
                directory = os.path.dirname(self._path)
                os.fsync(fd)
                fsync_dir(directory)
                fsync_dir(os.path.dirname(directory))
            except OSError as e:
                raise ProtoUnexpectedError(f"Cannot open the root log {self._path}: {e}") from e
            self._owner.track(fd)         # the file exists now: register its inode (M3)
            self._promised = _decode(payloads, self._path).promised
            # Every kind-5 entry is uncheckpointed at open: its records are redone by the
            # storage (take_redo), which then checkpoints and compacts.
            self._redo = [decode_records_entry(p, self._path) for p in payloads
                          if len(p) and p[0] in DATA_KINDS]
            self._pending = sum(FRAME_OVERHEAD + len(p) for p in payloads if len(p) and p[0] in DATA_KINDS)
        except BaseException as e:
            closing = ([lambda: os.close(fd)] if fd >= 0 else []) + [self._owner.release]
            secondary = []
            interrupt = None
            for close in closing:
                try:
                    close()
                except Exception as close_error:
                    # Never replaces the open error (8.3d re-review 13, N4): reported through
                    # ``e``, raised below, and the remaining release still runs.
                    secondary.append(close_error)
                except BaseException as stop:            # KeyboardInterrupt, SystemExit: finish, then propagate
                    if interrupt is None:
                        interrupt = stop
                    else:
                        secondary.append(stop)
            if secondary:
                attach_secondary_errors(e, secondary)
            if interrupt is not None and isinstance(e, Exception):
                raise interrupt                          # __context__ is ``e`` (re-review 14, R1)
            if interrupt is not None:
                attach_secondary_errors(e, [interrupt])  # ``e`` is itself an interrupt: it propagates
            raise
        self._fd = fd

    @property
    def path(self) -> str:
        """The log file."""
        return self._path

    def _check_writable(self) -> None:
        """Refuse a write through a closed or poisoned handle (lock held)."""
        if self._closed:
            raise ProtoValidationError(f"The root log {self._path} is closed")
        if self._failed is not None:
            raise ProtoUnexpectedError(
                f"The root log {self._path} refuses writes after an earlier I/O failure ({self._failed!r}); "
                f"reopen it (close and reopen the space) to write again")

    def append(self, kind: str, label: int, record: Optional[RootRecord]) -> None:
        """Append an ``"accepted"`` root, ``fsync``-ed before this returns, or a ``"committed"``
        root, durable with the next synced entry (module docstring; Task 8.3e).  Raises
        ``ProtoValidationError`` when the log is closed: an acknowledgement must never be sent
        for a root that was not logged."""
        code = _check_kind(kind)
        entry = _encode(code, label, record)
        with self._lock:
            self._check_writable()
            self._write(entry, sync=code != KINDS["committed"])

    def append_records(self, label: int, record: Optional[RootRecord], records: list, *, seq: int = 0,
                       pred: Optional[tuple] = None, ranges: Optional[list] = None, accepted: bool = True) -> int:
        """Append the acceptance of ``record`` labelled ``label`` together with ``records``
        (``[segment bytes, offset, framed record]``) as ONE kind-5 entry, ``fsync``-ed before
        this returns (module docstring, Task 8.3f phase B); return the bytes of kind-5 entries
        no checkpoint has covered yet, this one included.  ``seq`` and ``pred`` (``(label,
        RootRecord or None)``) describe the permission and the root it was built on (kept for
        the record; acceptance never reads them back).  With explicit ``ranges`` (the commit's
        byte ranges) or ``accepted=False`` (records a data replica holds without accepting the
        root), the entry is of kind 6 (Task 8.3g, D54).  Raises ``ProtoValidationError`` when
        the log is closed, ``ProtoUnexpectedError`` when it is poisoned."""
        if ranges is None and accepted:
            payload = encode_records_entry(label, record, seq, pred, records)
        else:
            payload = encode_replica_entry(label, record, seq, pred, records, list(ranges or ()), accepted=accepted)
        entry = encode_frame(payload)
        with self._lock:
            self._check_writable()
            self._write(entry, sync=True)
            self._pending += len(entry)
            return self._pending

    def _size_locked(self) -> int:
        self._check_writable()
        try:
            return os.fstat(self._fd).st_size
        except OSError as e:
            self._failed = e
            raise ProtoUnexpectedError(f"Reading the size of the root log {self._path} failed: {e}") from e

    def mark(self) -> tuple:
        """A checkpoint mark: ``(compaction generation, size of the log file)``, an entry
        boundary since appends hold the lock.  A checkpoint taken after this call covers every
        entry before it (:meth:`compact_records`)."""
        with self._lock:
            size = self._size_locked()
            return self._generation, size

    def pending_records(self) -> int:
        """Bytes of kind-5 entries that no checkpoint has covered yet."""
        with self._lock:
            return self._pending

    def take_redo(self) -> list:
        """The kind-5 entries found when the log was opened (:class:`RedoEntry` list, oldest
        first), handed over once: the storage rebuilds its data files from them."""
        with self._lock:
            entries, self._redo = self._redo, []
            return entries

    def compact_records(self, mark: tuple) -> None:
        """Rewrite the log once every data write of the entries before ``mark`` (a value of
        :meth:`mark`) has been synced by the caller's checkpoint: the entries before the mark
        are summarized (newest of each kind, a kind-5 entry as an accepted root without its
        records) and every later entry is kept verbatim, so no record a checkpoint has not
        covered is ever dropped.  When a compaction rewrote the file since the mark was taken,
        nothing is done: that compaction already summarized every entry before the mark, and
        the mark's offset names no position in the new file (review I1)."""
        generation, upto = mark
        with self._lock:
            self._check_writable()
            if generation != self._generation:
                return
            try:
                self._compact(keep_from=upto)
            except OSError as e:
                self._failed = e
                raise ProtoUnexpectedError(f"Compacting the root log {self._path} failed: {e}") from e
            except BaseException as e:
                self._failed = e
                raise

    def promise(self, term: int) -> None:
        """Make the promise of ``term`` durable (``fsync``) before it is sent.

        Raises ``ProtoValidationError`` when the log is closed and ``ProtoUnexpectedError`` when
        it is poisoned: a promise that cannot be recorded must not be made.
        """
        with self._lock:
            self._check_writable()
            if term <= self._promised:
                return              # a higher or equal promise is already durable
            self._write(_encode(3, term, None), sync=True)
            self._promised = term

    def mark_rejoined(self) -> None:
        """Append the rejoined marker, ``fsync``-ed (written last, after the promise and the
        roots the rejoin learnt)."""
        with self._lock:
            self._check_writable()
            self._write(_encode(4, 0, None), sync=True)

    def _write(self, entry: bytes, *, sync: bool) -> None:
        """Append ``entry``, ``fsync``-ed with ``sync`` whatever the storage's data durability
        mode (lock held, handle writable).  Any failure poisons the handle: the file may end in
        a torn entry, or hold what an ``fsync`` could not confirm."""
        try:
            write_all(self._fd, entry)
            if sync:
                os.fsync(self._fd)
            if self._pending == 0 and os.fstat(self._fd).st_size > _COMPACT_BYTES:
                # Only while no kind-5 entry awaits a checkpoint: its records must stay until
                # the storage has synced their data files (compact_records).
                self._compact()
        except OSError as e:
            self._failed = e
            raise ProtoUnexpectedError(f"Writing the root log {self._path} failed: {e}") from e
        except BaseException as e:
            self._failed = e
            raise

    def _read(self) -> LoggedRoots:
        return _decode(read_log(self._path, LOG_KIND), self._path)

    def _compact(self, keep_from: Optional[int] = None) -> None:
        """Rewrite the log with its newest entry of each kind (lock held); with ``keep_from``,
        only the entries before that offset are summarized and the later ones are kept
        verbatim (:meth:`compact_records`)."""
        all_payloads = read_log(self._path, LOG_KIND)
        kept = []
        if keep_from is not None:
            offset, before = HEADER_SIZE, []
            for payload in all_payloads:
                (before if offset < keep_from else kept).append(payload)
                offset += FRAME_OVERHEAD + len(payload)
            all_payloads = before
        roots = _decode(all_payloads, self._path)
        payloads = []
        if roots.accepted is not None:
            payloads.append(_payload(1, *roots.accepted))
        if roots.committed is not None:
            payloads.append(_payload(2, *roots.committed))
        if roots.promised:
            payloads.append(_payload(3, roots.promised, None))
        if not roots.empty:
            payloads.append(_payload(4, 0, None))        # the rejoined marker, last
        payloads.extend(kept)
        data = log_contents(LOG_KIND, payloads)
        # Open and fully prepare the replacement, swap, and only then close the old descriptor
        # (8.3d re-review 9, B1): the handle never holds a closed descriptor number.
        new = prepare_replacement(self._path, data, os.O_RDWR | _O_BINARY | _O_CLOEXEC)
        old, self._fd = self._fd, new
        os.close(old)
        self._owner.track(new)             # the log is a new file (M3)
        fsync_dir(os.path.dirname(self._path))
        self._pending = sum(FRAME_OVERHEAD + len(p) for p in kept if len(p) and p[0] in DATA_KINDS)
        self._generation += 1

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
            error = None
            try:
                os.close(fd)
            except OSError as e:
                error = ProtoUnexpectedError(f"Closing the root log {self._path} failed: {e}")
                error.__cause__ = e
            finally:
                # Released last, whatever was raised: no write through this handle can follow
                # (rulings R59, R60).  A release error is reported, as a secondary error of a
                # close error when there is one.
                try:
                    self._owner.release()
                except ProtoUnexpectedError as release_error:
                    if error is None:
                        raise
                    attach_secondary_errors(error, [release_error])
            if error is not None:
                raise error


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

    def append(self, kind: str, label: int, record: Optional[RootRecord]) -> None:
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
