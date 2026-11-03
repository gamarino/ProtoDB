"""Tiered storage of a cluster node: a local WAL tier and an object-store tier (Task 8.3d, D44, D46).

In cluster mode the object store is entirely off the commit path.  Each node keeps a
local directory (the commit-durability tier) and a background
replicator (:mod:`proto_db.cluster.persister`) ships it to the object store.

Durability (Task 8.3e, defect D-SAFE-2; Task 8.3f phase B, D50): the acceptor log
``roots.log`` -- lease promises, accepted roots, the rejoined marker -- is ``fsync``-ed before
the reply each entry justifies **whatever the durability mode**
(:class:`~proto_db.cluster.rootlog.RootsLog`).  An acceptance of a replicated root
(:meth:`TieredStorage.accept_replication`) is ONE kind-5 entry holding the root and every
record its replication carried, synced once; the copies of other nodes' records, the own
segment's buffered records and ``ranges.log`` are written without a sync.  When the storage
opens, the kind-5 entries are *redone* (each record written again at its offset, each commit's
ranges remembered) before anything reads or repairs the data files; a *checkpoint*
(:meth:`TieredStorage.checkpoint`: own segments, copies, ``ranges.log``, ``segments.log`` and
the directory synced, then the acceptor log compacted up to the checkpoint's mark) runs before
any kind-5 entry loses its records -- when the entries reach :data:`CHECKPOINT_BYTES`, after a
redo, and before :meth:`TieredStorage.release_local` deletes anything.  So in every mode a
root is durable exactly when its records are: an acknowledged commit survives power loss of
any set of nodes, and the open defect D-AVAIL-1 of Task 8.3e (a durable root whose records a
majority power loss erased in ``"flush"``/``"none"``) cannot arise.  Records a rejoin adopts,
and the base records a holder re-proposes (ruling R50), are synced in every mode before the
root that needs them is logged (:meth:`TieredStorage.store_commit`).

``durability`` (``"fsync"``, ``"flush"``, ``"none"``) now governs only writes outside the commit
path: the persister's flush of an own segment before its upload, the closing flush, and the
records of a replication this node refused (:meth:`TieredStorage.apply`).

Layout of the local directory::

    <uuid4-hex>.seg     this node's own segments (written through a FileStorage) and
                        copies of other nodes' segments: records received with their
                        commits are written at their original offsets, so every
                        AtomPointer is valid cluster-wide; a copy may have holes
    segments.log        the node's own segments in creation order (one 16-byte UUID per entry)
    roots.log           the node's acceptor log: lease promises, roots accepted and committed
                        (:class:`~proto_db.cluster.rootlog.RootsLog`)
    ranges.log          per accepted commit root, the byte ranges of the records shipped
                        with it (so a restarted node can still upload a lost writer's data)
    tier.lock           held exclusively by the open instance (no data; never delete it)
    roots.log.lock      held exclusively by the open ``roots.log`` handle (no data)

The three logs are framed log files (:mod:`proto_db.storage.logfile`: a versioned header, then
entries each framed with a CRC32).  Opening the storage truncates a torn tail of each log to its
last valid entry, and ``fsync``-s it, before any append; an invalid entry followed by valid ones
raises ``ProtoCorruptionError`` (8.3d re-review 10, R2; ruling R58).  Own segments are checked the
same way, record by record, and a torn tail is truncated (:meth:`TieredStorage._repair_own_segment`);
copies of other nodes' segments are not logs (records are written at their original offsets,
holes are expected, and a torn record is overwritten when the record is received again).

Because opening repairs files, one directory has one live instance (8.3d re-review 11, I1;
ruling R59): the constructor takes an exclusive :class:`~proto_db.storage.ownerlock.OwnerLock`
on ``tier.lock`` before it opens, reads or repairs anything, and :meth:`TieredStorage.close`
releases it last (a failed constructor releases it too).  A second open of the directory while
an instance is live, in this process or another one, raises ``ProtoLockingError``.

Layout in the object store under ``prefix``::

    <prefix>space.root                         the persisted root: cluster epoch and label term
    <prefix>segments/<uuid4-hex>.seg           segment objects, uploaded whole and sealed by
                                               their writer, in creation order
    <prefix>segments/<uuid4-hex>.<len>.sealed  empty marker written after a segment object:
                                               its length in bytes (16 hex digits)
    <prefix>segments/<uuid4-hex>.<end>.part    the first ``end`` bytes (16 hex digits) of a
                                               segment, uploaded from a copy by another node
                                               when the writer lags (a lost node)

While no part object exists the layout is the :class:`~proto_db.storage.cloud.CloudStorage`
one (which never lists, so it ignores the markers) and can then read the persisted space; a
TieredStorage reads both.

What the store holds is judged by length (8.3d re-review 6, M2; ruling R54): a sealed object
covers a byte range only up to its recorded length -- from this node's upload, its marker,
or a download, the shortest known -- and a part up to its ``end``.  The marker records the
length its writer *uploaded*, not what the store kept.  So a sealed object uploaded shorter
than the records a peer holds (a writer that restarted on a segment missing its tail, for
instance after a host crash under ``flush`` or ``none`` durability) never makes a commit look
persisted, never lets a holder skip shipping those records, and never lets a peer release
its longer copy; a sealed object whose length is unknown covers nothing.  A store that loses
bytes after acknowledging an upload is not detected here: the marker still claims the
uploaded length, and peers may release their copies on its strength.  Such a loss shows at
the first download that reaches it: the object is then recorded at its downloaded length,
and a record read from a store object that turns out short or corrupt is read from the
peers, then from a part; with no full copy anywhere the read raises
``ProtoCorruptionError``, never short data.

Write path: a record larger than :data:`~proto_db.cluster.protocol.MAX_RECORD_BYTES` is
refused (``ProtoValidationError``): it could never be replicated in one frame.
``push_atom``/``push_blob`` append to the local WAL and keep the framed
record in a bounded in-memory record cache and in the list of records not yet shipped to
the peers (:meth:`TieredStorage.outgoing`); the cluster commit sends them with the root.

Read path, in order: the record cache, the local directory (own segments and copies),
then, for a segment not known to be in the object store, a peer page request (the
fetcher the coordinator installs), else the object store (whole segment objects or
parts, cached).  What the store holds is known from this node's uploads and downloads
and from the persister's listings (when it starts and when it checks persistability).  Reads made while the calling thread is inside a commit's
critical section (:meth:`TieredStorage.critical_section`) and not served by the record
cache are counted, so tests can assert that the steady-state critical section reads nothing.

Shipping (called by the persister thread only, never by a commit): the open segment is
sealed when it holds data older than ``ship_interval`` (or on demand), and sealed own
segments are uploaded in creation order with ``put_if_absent`` (idempotent: a retry
meeting our own earlier upload succeeds).  Because the order is strict, an own segment
in the object store implies every earlier own segment is there too.

Parts: a writer that dies before shipping leaves its records only in the copies of its
peers.  Each node remembers, per commit root it accepted, the byte ranges of the records
shipped with it (:meth:`TieredStorage.commit_ranges`; logged in ``ranges.log``).  When persistence lags,
:meth:`TieredStorage.ensure_remote` uploads, for a segment not yet covered in the store,
the contiguous prefix of the local copy (records are validated one by one from the
header; a copy with a hole before the needed end cannot be uploaded and another node's
copy must serve).  The bytes of a prefix are identical at every node (records are written
at their original offsets), so concurrent part uploads agree.

Records of an adopted root (ruling R50): a root a node adopts without receiving its
replication -- a rejoin after a lost disk, or a lease holder re-proposing a base nobody
knew committed -- must travel with its records.  The coordinator asks the peers which byte
ranges a commit shipped (:meth:`TieredStorage.commit_ranges` on the peer), then
:meth:`TieredStorage.commit_records` gathers the records of those ranges that the object
store is not known to hold, from the local disk or by peer page requests, and
:meth:`TieredStorage.store_commit` makes them durable here as copies (with the ranges, so
this node can serve the next node that needs them and upload parts).  A holder remembers
the ranges of its own commit before its outcome is known (:meth:`TieredStorage.remember_commit`).

Local release: an own segment or a copy is deleted locally once the object store holds
every byte of it (a sealed object or a part at least as long as the local file) and every
commit that referenced it orders at or below the persisted root in the full order of roots
(:func:`~proto_db.cluster.rootlog.root_key`; ``release_local``).

Backlog: bytes and age of own data not yet in the object store, exposed by
:meth:`TieredStorage.backlog`.  ``on_backlog_alert(backlog)`` is called when the backlog
reaches ``backlog_alert_bytes`` (default 80 % of the limit).  At ``backlog_limit_bytes``
(default: no limit) commits are refused before the lease with
:class:`~proto_db.errors.ProtoBacklogFullError`; nothing stops committing below it.

Thread-safety: every public method may be called from any thread.  One lock guards the
bookkeeping, one serializes shipping, one the ranges log (the roots log has its own); the caches have their own
locks.  No lock is held across peer requests; object-store writes are made under the shipping
lock (below).

Closing (8.3d re-review 8, A1; ruling R56): no write reaches the local directory or the object
store once :meth:`TieredStorage.close` has started, whatever thread captured this storage
before.  Every write is guarded by the lock that ``close()`` takes before it releases what the
write uses, and the closed flag is checked under that same lock, so a write either completes
before ``close()`` passes that lock or is refused with ``ProtoValidationError``:

* ``segments.log`` (its descriptor): the manifest lock;
* ``ranges.log`` (its descriptor, and the compaction that replaces the file): the ranges lock;
* copies of other nodes' segments (written by path) and the local release that deletes copies
  and own segments: the copies lock;
* object-store writes (segment objects, markers, parts, the persisted root): the shipping lock;
* own segments and ``roots.log``: the :class:`~proto_db.storage.file.FileStorage` and
  :class:`~proto_db.cluster.rootlog.RootsLog` they belong to, each with its own lock.

A descriptor is therefore never written after it was closed, so a descriptor number the
operating system hands to another file meanwhile is never written through a stale storage.

Failed local writes (8.3d re-review 9, B1; ruling R57): any I/O error of a write to the local
directory -- ``segments.log``, ``ranges.log`` (an append, an ``fsync``, or a compaction's
rewrite, rename, reopen or close), a copy of another node's segment, the directory ``fsync``,
a local release -- poisons the local tier: every later local write, and every write to
``roots.log`` through this storage, raises ``ProtoUnexpectedError`` until the storage is
closed and reopened.  A log that failed mid-write may end in a torn entry, and what an ``fsync``
that failed left on disk is unknown, so nothing written after the failure could be relied on;
the reopen truncates the torn entry before any append (above).  A compaction of ``ranges.log`` opens and fully
prepares the replacement before it closes the old descriptor, so the descriptor attribute
never holds a closed number.  Object-store writes are not affected (they read what is local,
never write it); the inner :class:`~proto_db.storage.file.FileStorage` poisons itself the same
way on a failed segment write.
"""
import contextlib
import errno
import logging
import math
import os
import random
import re
import struct
import threading
import time
import uuid
import zlib
from typing import Callable, Optional

import msgpack

from ..core.atom import AtomPointer
from ..errors import (ProtoBacklogFullError, ProtoCorruptionError, ProtoDBError, ProtoLockingError,
                      ProtoNotSupportedError, ProtoTimeoutError, ProtoUnexpectedError,
                      ProtoValidationError, attach_secondary_errors)
from ..cluster import protocol as _protocol
from ..cluster.rootlog import LoggedRoots, RootsLog, compare_roots, root_key
from .base import SharedStorage, check_lock_timeout
from .cache import BoundedCache
from .cloud import (ObjectNotFoundError, OutcomeUnknownError, PreconditionFailedError,
                    TransientObjectStoreError)
from .crc import PrefixCrc
from .file import DURABILITY_MODES, FileStorage
from .format import (FORMAT_VERSION, RECORD_HEADER_SIZE, RECORD_MAGIC, SEGMENT_HEADER_SIZE, RecordKind,
                     RootRecord, decode_root, decode_segment_header, encode_record, encode_segment_header,
                     read_record, record_header)
from .logfile import encode_frame, log_contents, open_log, prepare_replacement, read_fd, write_all
from .ownerlock import OwnerLock

__all__ = ["TieredStorage"]

logger = logging.getLogger(__name__)

MIB = 1024 * 1024
ROOT_OBJECT = "space.root"
SEGMENT_PREFIX = "segments/"
SEGMENT_SUFFIX = ".seg"
PART_SUFFIX = ".part"
SEALED_SUFFIX = ".sealed"
MANIFEST = "segments.log"
MANIFEST_KIND = b"SEGS"                          # framed log kinds (:mod:`proto_db.storage.logfile`)
RANGES_KIND = b"RANG"
ROOTS_LOG = "roots.log"
_ROOT_CRC = struct.Struct("<I")
_ROOTS_LOG_COMPACT = 1 * MIB
#: Bytes of kind-5 acceptor-log entries after which an acceptance first checkpoints the local
#: tier (Task 8.3f phase B): it bounds the log and the work of a redo.
CHECKPOINT_BYTES = 8 * MIB
RANGES_LOG = "ranges.log"
#: Lock file held by the live instance of a local directory (ruling R59).
TIER_LOCK = "tier.lock"
_RANGES_HEAD = struct.Struct("<16sQQQI")         # root segment, root offset, label, epoch, count
_RANGE = struct.Struct("<16sQQ")                 # segment, start, end
_RECORD_KINDS = frozenset(int(k) for k in RecordKind)
_O_CLOEXEC = getattr(os, "O_CLOEXEC", 0)
_O_BINARY = getattr(os, "O_BINARY", 0)


class _Own:
    """Bookkeeping of one own segment."""

    __slots__ = ("uuid", "end", "first", "uploaded", "max_key", "released")

    def __init__(self, segment: uuid.UUID):
        self.uuid = segment
        self.end = SEGMENT_HEADER_SIZE
        self.first = None          # time.monotonic() of the first write not yet uploaded
        self.uploaded = False
        self.max_key = None        # root_key of the newest commit that shipped records of this segment
        self.released = False


def _is_seconds(value) -> bool:
    """True for a finite non-negative ``int`` or ``float`` (not a ``bool``)."""
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return False
    try:
        seconds = float(value)
    except OverflowError:
        return False
    return math.isfinite(seconds) and seconds >= 0


def _pwrite_all(fd: int, data, offset: int) -> None:
    """Write every byte of ``data`` at ``offset`` of ``fd``: a short write is continued, a write
    that makes no progress raises ``OSError``."""
    view = memoryview(data)
    while view:
        n = os.pwrite(fd, view, offset)
        if n <= 0:
            raise OSError(errno.EIO, f"pwrite returned {n}")
        view = view[n:]
        offset += n


def _record_length(data, pos: int, size: int) -> Optional[int]:
    """The framed length of a valid segment record at ``pos`` of ``data`` (``size`` bytes), or
    ``None`` (truncated, invalid header -- magic, kind or header CRC -- or payload CRC mismatch)."""
    if pos + RECORD_HEADER_SIZE > size:
        return None
    header = record_header(data, pos)
    if header is None:
        return None
    _, length, crc = header
    end = pos + RECORD_HEADER_SIZE + length
    if end > size or zlib.crc32(data[pos + RECORD_HEADER_SIZE:end]) != crc:
        return None
    return end - pos


# -- the search for a valid record after an invalid one (8.3d re-review 12, I3; ruling R60) ---
#
# Segment records have self-validating headers (format version 2, :mod:`proto_db.storage.format`):
# a header CRC covers the magic, kind, length and payload CRC.  The repair of an own segment is
# therefore framing-driven (:meth:`TieredStorage._repair_own_segment`): an invalid record whose
# header is valid has a trustworthy extent, and the search below only runs over the bytes after
# that extent, or, when the header itself is invalid, over every offset after it.  The search
# answers exactly as trying :func:`_record_length` at every offset, in time linear in the region
# and with no cap: candidate offsets (the record magic followed by a valid kind byte) are found
# by the regular-expression engine; a candidate's header CRC costs eleven bytes; and a candidate
# whose header is valid and whose extent fits has its payload CRC computed from CRCs of the data
# up to its start and up to its end, kept at checkpoints, with the GF(2) arithmetic of zlib's
# ``crc32_combine`` (exact; :mod:`proto_db.storage.crc`), so it costs at most two checkpoint
# intervals of CRC, never its whole claimed payload.  A payload of at most ``_CRC_DIRECT``
# bytes gets a direct CRC instead, so no candidate costs more than ``_CRC_DIRECT`` (16 KiB) of
# CRC: linear, with that constant for crafted header-valid candidates.

_CRC_CHECKPOINT = 4 * 1024                      # bytes between prefix-CRC checkpoints
_CRC_DIRECT = 4 * _CRC_CHECKPOINT               # payloads up to this size get a direct CRC
_CANDIDATE = re.compile(
    re.escape(RECORD_MAGIC[:1]) + b"(?=" + re.escape(RECORD_MAGIC[1:]) + b"["
    + b"".join(re.escape(bytes([k])) for k in sorted(_RECORD_KINDS)) + b"])", re.DOTALL)
_CANDIDATE_SPAN = len(RECORD_MAGIC) + 1         # bytes the candidate pattern inspects


def _first_valid_record(data, start: int, size: int) -> Optional[int]:
    """The first offset at or after ``start`` of ``data`` (``size`` bytes) that holds a valid
    segment record, or ``None``.  Exact (the answer of trying :func:`_record_length` at every
    offset) and linear in ``size - start`` with no cap (see the comment above)."""
    last = size - RECORD_HEADER_SIZE              # the last offset where a header fits
    if start > last:
        return None
    prefix = PrefixCrc(data, start, _CRC_CHECKPOINT)
    for match in _CANDIDATE.finditer(data, start, last + _CANDIDATE_SPAN):
        pos = match.start()
        header = record_header(data, pos)
        if header is None:
            continue
        _, length, crc = header
        payload = pos + RECORD_HEADER_SIZE
        end = payload + length
        if end > size:
            continue
        if length <= _CRC_DIRECT:
            actual = zlib.crc32(data[payload:end])
        else:
            actual = prefix.payload_crc(payload, end)   # crc32_combine arithmetic (crc.py)
        if actual == crc:
            return pos
    return None


def _encode_root_object(record: RootRecord, label: int) -> bytes:
    body = msgpack.packb({"version": record.version, "segment": record.segment.bytes,
                          "offset": record.offset, "epoch": record.epoch, "recent": [],
                          "term": label}, use_bin_type=True)
    return _ROOT_CRC.pack(zlib.crc32(body)) + body


def _decode_root_object(data: bytes) -> tuple:
    """``(label, RootRecord)`` of a persisted root object (label 0 when absent)."""
    record = decode_root(data)
    fields = msgpack.unpackb(bytes(data[_ROOT_CRC.size:]), raw=False)
    label = fields.get("term", 0)
    if type(label) is not int or label < 0:
        raise ProtoCorruptionError("Malformed root term")
    return label, record


class TieredStorage(SharedStorage):
    """A cluster node's storage: local WAL tier plus object-store tier (see the module docstring).

    Args:
        path: local directory (created if missing).
        store: the :class:`~proto_db.storage.cloud.ObjectStore` shared by the cluster.
        prefix: key prefix of the space in the store.
        durability: ``"fsync"`` (default), ``"flush"`` or ``"none"`` for the local tier's
            writes outside the commit path (module docstring).  Acknowledged commits survive
            power loss in every mode: an acceptance logs the root with its records in the
            always-synced acceptor log (Task 8.3f phase B).
        cache_bytes: capacity of the in-memory record cache.
        segment_cache_bytes: capacity of the cache of downloaded segment objects.
        segment_max_bytes: a local segment rolls over before exceeding this size.
        ship_interval: seconds after which the open segment's data is sealed for shipping.
        retries: retries of object-store requests failing transiently.
        backoff: ``(min_seconds, max_seconds)`` between those retries.
        backlog_limit_bytes: own bytes not yet in the store at which commits are refused
            (``None``: no limit).
        backlog_alert_bytes: backlog at which ``on_backlog_alert`` is called (default 80 % of
            the limit; ``None`` without a limit).
        on_backlog_alert: callable receiving :meth:`backlog`'s dict; called by the persister
            thread (at most once per second while the backlog stays above the threshold).
        release_local: delete local segments once the store holds them and a persisted root
            covers them.
        lock_timeout: default seconds :meth:`root_lock` waits; finite and positive
            (:func:`~proto_db.storage.base.check_lock_timeout`).
    """

    #: The root lock is in-process: cluster mode coordinates writers with the lease.
    root_lock_is_global = False
    #: Marks a storage that provides the cluster tier operations natively (D46).
    cluster_tier = True
    #: Records live on each node's disk, not in a storage every node reads: a root adopted
    #: without its replication must be given its records (ruling R50).
    local_records = True

    def __init__(self, path: str, store, prefix: str = "", *, durability: str = "fsync",
                 cache_bytes: int = 64 * MIB, segment_cache_bytes: int = 64 * MIB,
                 segment_max_bytes: int = 64 * MIB, ship_interval: float = 0.5, retries: int = 5,
                 backoff: tuple = (0.05, 2.0), backlog_limit_bytes: Optional[int] = None,
                 backlog_alert_bytes: Optional[int] = None,
                 on_backlog_alert: Optional[Callable[[dict], None]] = None,
                 release_local: bool = True, lock_timeout: float = 30.0):
        for method in ("put_if_absent", "put_if_match", "get", "list", "delete"):
            if not callable(getattr(store, method, None)):
                raise ProtoValidationError("store must provide the ObjectStore methods")
        if durability not in DURABILITY_MODES:
            raise ProtoValidationError(f"durability must be one of {DURABILITY_MODES}")
        if not isinstance(prefix, str):
            raise ProtoValidationError("prefix must be a str")
        if not _is_seconds(ship_interval):
            raise ProtoValidationError("ship_interval must be a finite non-negative number of seconds")
        if type(retries) is not int or retries < 0:
            raise ProtoValidationError("retries must be a non-negative int")
        try:
            low, high = backoff
        except (TypeError, ValueError) as e:
            raise ProtoValidationError("backoff must be a (min_seconds, max_seconds) pair") from e
        if not all(_is_seconds(x) for x in (low, high)) or low > high:
            raise ProtoValidationError("backoff must be a (min_seconds, max_seconds) pair of finite "
                                       "non-negative numbers with min_seconds <= max_seconds")
        for name, value in (("cache_bytes", cache_bytes), ("segment_cache_bytes", segment_cache_bytes)):
            if type(value) is not int or value < 0:
                raise ProtoValidationError(f"{name} must be a non-negative int")
        if type(segment_max_bytes) is not int or segment_max_bytes <= SEGMENT_HEADER_SIZE + RECORD_HEADER_SIZE:
            raise ProtoValidationError(
                f"segment_max_bytes must be an int above {SEGMENT_HEADER_SIZE + RECORD_HEADER_SIZE}")
        check_lock_timeout(lock_timeout)                                        # FileStorage's rule
        if backlog_limit_bytes is not None and (type(backlog_limit_bytes) is not int or backlog_limit_bytes <= 0):
            raise ProtoValidationError("backlog_limit_bytes must be a positive int or None")
        if backlog_alert_bytes is None and backlog_limit_bytes is not None:
            backlog_alert_bytes = int(backlog_limit_bytes * 0.8)
        if backlog_alert_bytes is not None and (type(backlog_alert_bytes) is not int or backlog_alert_bytes <= 0):
            raise ProtoValidationError("backlog_alert_bytes must be a positive int or None")
        if on_backlog_alert is not None and not callable(on_backlog_alert):
            raise ProtoValidationError("on_backlog_alert must be callable")
        self._path = os.path.abspath(os.fspath(path))
        if os.path.exists(self._path) and not os.path.isdir(self._path):
            raise ProtoValidationError(f"{self._path} exists and is not a directory")
        # Every argument is validated and every attribute that holds no resource is set before
        # the owner lock is taken (8.3d re-review 12, I2; ruling R60); everything after the lock
        # runs under one handler that releases what was opened, the lock last.
        self._store = store
        self._prefix = prefix
        self._root_key = prefix + ROOT_OBJECT
        self._durability = durability
        self._ship_interval = float(ship_interval)
        self._retries = retries
        self._backoff = (float(low), float(high))
        self._limit = backlog_limit_bytes
        self._alert_at = backlog_alert_bytes
        self._on_alert = on_backlog_alert
        self._release = bool(release_local)
        self._lock_timeout = lock_timeout

        self._lock = threading.Lock()
        self._ship_lock = threading.Lock()
        self._manifest_lock = threading.Lock()
        self._root_lock = threading.Lock()
        self._holder = None
        self._cache = BoundedCache(cache_bytes)
        self._segments = BoundedCache(segment_cache_bytes)
        self._tls = threading.local()
        self._fetcher = None
        self._own = {}                 # UUID -> _Own, in creation order (dicts keep it)
        self._copies = {}              # UUID -> root_key of the newest commit that wrote into the copy
        self._remote = set()           # segments whose sealed object is in the store
        self._sealed = {}              # UUID -> known length of its sealed object (the shortest seen)
        self._parts = {}               # UUID -> largest end of a part object in the store
        self._ranges = {}              # (root segment, offset) -> (root_key, [(UUID, start, end)])
        self._unshipped = []           # [segment bytes, offset, framed record], push order
        self._manifest_dirty = False
        self._last_alert = 0.0
        self._closed = False
        self._failed = None            # the I/O error that poisoned the local tier, if any
        self._stats = {"reads_cache": 0, "reads_local": 0, "reads_peer": 0, "reads_remote": 0,
                       "critical_misses": 0, "remote_segment_gets": 0, "segments_uploaded": 0,
                       "root_writes": 0, "root_writes_in_critical": 0, "segments_released": 0,
                       "copies_released": 0, "records_applied": 0, "parts_uploaded": 0,
                       "acceptances_logged": 0, "checkpoints": 0, "redo_entries": 0, "redo_records": 0}
        self._ranges_lock = threading.Lock()
        self._copies_lock = threading.Lock()
        # Phase B (Task 8.3f): an acceptance (its kind-5 entry and the unsynced data writes it
        # covers) is atomic against the start of a checkpoint, so a checkpoint's mark never
        # separates an entry from its data writes.
        self._accept_lock = threading.Lock()
        self._dirty_copies = set()     # copies written without a sync since the last checkpoint
        self._dirty_dir = False        # files created without a directory sync since then
        self._manifest_fd = self._ranges_fd = -1
        self._roots = None
        self._local = None
        try:
            os.makedirs(self._path, exist_ok=True)
        except OSError as e:
            raise ProtoUnexpectedError(f"Cannot create the local directory {self._path}: {e}") from e
        # Held for the instance's lifetime, taken before anything is opened or repaired (R59).
        self._owner = OwnerLock(os.path.join(self._path, TIER_LOCK), f"the tiered storage directory {self._path}",
                                target=self._path)
        try:
            self._open_local(durability, cache_bytes, lock_timeout, segment_max_bytes)
        except BaseException as e:
            self._abandon_open(e)
            raise

    def _open_local(self, durability: str, cache_bytes: int, lock_timeout: float, segment_max_bytes: int) -> None:
        """Open the local tier (owner lock held): the segment storage, then the logs (each
        repaired on open).  The caller releases what was opened when this raises."""
        self._local = FileStorage(self._path, durability=durability, cache_bytes=min(cache_bytes, 16 * MIB),
                                  lock_timeout=lock_timeout, segment_max_bytes=segment_max_bytes)
        present = self._load_manifest()
        self._roots = RootsLog(os.path.join(self._path, ROOTS_LOG))     # always synced (Task 8.3e)
        self._load_ranges()
        # Phase B (Task 8.3f): rebuild the data files from the acceptances logged since the
        # last checkpoint before anything reads or repairs them, then check own segments.
        entries = self._roots.take_redo()
        redone = self._redo(entries, present)
        self._repair_own_segments(present)
        if entries:
            self.checkpoint()
        elif redone:
            self._checkpoint_data()

    # -- properties --------------------------------------------------------------------------

    @property
    def path(self) -> str:
        """The local directory."""
        return self._path

    @property
    def store(self):
        """The object store."""
        return self._store

    @property
    def storage(self):
        """This storage (the cluster tier interface names the storage it serves)."""
        return self

    def segment_key(self, segment: uuid.UUID) -> str:
        """Key of the segment object ``segment``."""
        return f"{self._prefix}{SEGMENT_PREFIX}{segment.hex}{SEGMENT_SUFFIX}"

    def stats(self) -> dict:
        """Counters: reads by source (cache, local, peer, remote), critical-section misses,
        segment downloads and uploads, root writes (and those made inside a critical
        section), local releases, replicated records applied."""
        with self._lock:
            return dict(self._stats)

    def _count(self, key: str, n: int = 1) -> None:
        with self._lock:
            self._stats[key] += n

    def _check_open(self) -> None:
        if self._closed:
            raise ProtoValidationError("Storage is closed")

    def _refuse_closed(self, what: str, *, local: bool = True) -> None:
        """Refuse a write once :meth:`close` has started (``ProtoValidationError``) and, for a
        ``local`` write, once an earlier local write failed (``ProtoUnexpectedError``; module
        docstring).  Called with the lock guarding ``what`` held, the lock ``close()`` takes
        before releasing it (module docstring), so a write that passes this check completes
        before ``close()`` does."""
        if self._closed:
            raise ProtoValidationError(f"Storage is closed: {what} refused")
        if local:
            self._refuse_failed(what)

    def _refuse_failed(self, what: str) -> None:
        """Refuse a local write once an earlier one failed (``ProtoUnexpectedError``)."""
        failed = self._failed
        if failed is not None:
            raise ProtoUnexpectedError(
                f"The local tier at {self._path} failed earlier ({failed!r}): {what} refused; close and "
                f"reopen the storage to write again")

    def _fail(self, error: BaseException) -> None:
        """Poison the local tier after the failed local write ``error`` (module docstring); the
        first failure is kept.  Never called with ``_lock`` held."""
        with self._lock:
            if self._failed is None:
                self._failed = error

    # -- startup -----------------------------------------------------------------------------

    def _open_log(self, name: str, kind: bytes) -> tuple:
        """Open the framed log ``name`` of the local directory for appends (a torn tail is
        trimmed first, :func:`~proto_db.storage.logfile.open_log`); return ``(fd, payloads)``."""
        path = os.path.join(self._path, name)
        try:
            return open_log(path, kind, durability=self._durability)
        except OSError as e:
            raise ProtoUnexpectedError(f"Cannot open {path}: {e}") from e

    def _abandon_open(self, error: BaseException) -> None:
        """Release what a failed constructor opened; ``error`` is the open error, which the
        caller re-raises.  Every release runs even when an earlier one raised, and the owner
        lock is released last (8.3d re-review 12, I2; ruling R60).  An ``Exception`` raised by a
        release step never replaces ``error``: it is attached to it as a secondary error
        (:func:`~proto_db.errors.secondary_errors`) and the remaining steps still run (8.3d
        re-review 13, N4; ruling R61).  A control-flow exception (``KeyboardInterrupt``,
        ``SystemExit``: a ``BaseException`` that is not an ``Exception``) raised by a step is
        kept, the remaining steps still run, and the first one is then raised with ``error`` as
        its context, so an interrupt is never lost (8.3d re-review 14, R1; ruling R62)."""
        steps = [lambda fd=fd: os.close(fd) for fd in (self._manifest_fd, self._ranges_fd) if fd >= 0]
        if self._roots is not None:
            steps.append(self._roots.close)
        if self._local is not None:
            steps.append(self._local.close)
        steps.append(self._owner.release)           # last: nothing of this instance remains open
        secondary = []
        interrupt = None
        for release in steps:
            try:
                release()
            except Exception as release_error:
                secondary.append(release_error)          # reported through ``error``
            except BaseException as stop:                # KeyboardInterrupt, SystemExit: finish, then propagate
                if interrupt is None:
                    interrupt = stop
                else:
                    secondary.append(stop)
        if secondary:
            attach_secondary_errors(error, secondary)
        if interrupt is not None and isinstance(error, Exception):
            raise interrupt                              # __context__ is ``error`` (called inside its handler)
        if interrupt is not None:
            attach_secondary_errors(error, [interrupt])  # ``error`` is itself an interrupt: it propagates

    def _load_manifest(self) -> set:
        """Load ``segments.log`` and list the copies; return the own segments whose file exists.
        Own segments are checked (:meth:`_repair_own_segments`) only after the redo."""
        self._manifest_fd, payloads = self._open_log(MANIFEST, MANIFEST_KIND)
        present = set()
        for payload in payloads:
            if len(payload) != 16:
                raise ProtoCorruptionError(f"{MANIFEST} in {self._path} holds an entry of {len(payload)} bytes")
            seg = uuid.UUID(bytes=payload)
            own = _Own(seg)
            if os.path.exists(os.path.join(self._path, seg.hex + SEGMENT_SUFFIX)):
                present.add(seg)
            else:
                own.uploaded = own.released = True     # released only after the store held it
            self._own[seg] = own
        for name in os.listdir(self._path):
            if name.endswith(SEGMENT_SUFFIX):
                try:
                    seg = uuid.UUID(hex=name[:-len(SEGMENT_SUFFIX)])
                except ValueError:
                    continue
                if seg not in self._own:
                    self._copies[seg] = None        # released by key only once written again
        return present

    def _repair_own_segments(self, present: set) -> None:
        """Check every own segment whose file exists (after the redo) and truncate a torn tail
        (:meth:`_repair_own_segment`)."""
        for seg, own in self._own.items():
            if seg not in present:
                continue
            file = os.path.join(self._path, seg.hex + SEGMENT_SUFFIX)
            try:
                own.end = self._repair_own_segment(file, seg)
                own.first = time.monotonic()
            except FileNotFoundError:
                own.uploaded = own.released = True
            except OSError as e:
                raise ProtoUnexpectedError(f"Cannot check own segment {seg.hex}: {e}") from e

    def _redo(self, entries: list, present: set) -> bool:
        """Rebuild the data files from the kind-5 entries ``entries`` of the acceptor log (Task
        8.3f phase B; at open, before anything reads them): each record is written at its
        original offset into its own segment or into the copy of another node's segment
        (identical bytes wherever they already are), and each entry's byte ranges are
        remembered and logged.  An own segment listed in the manifest whose file is missing is
        recreated: a released own segment never appears in an entry kept after the checkpoint
        that preceded its release (:meth:`release_local`), so a missing file here was never
        written out.  Nothing is synced here; the caller checkpoints.  Return True when
        anything was written."""
        if not entries:
            return False
        written = False
        files = {}
        try:
            for entry in entries:
                if entry.root is None:
                    continue
                order = root_key(entry.label, entry.root)
                for raw, offset, data in entry.records:
                    segment = uuid.UUID(bytes=raw)
                    own = self._own.get(segment)
                    fd = files.get(segment)
                    if fd is None:
                        path = os.path.join(self._path, segment.hex + SEGMENT_SUFFIX)
                        fd = os.open(path, os.O_RDWR | os.O_CREAT | _O_BINARY | _O_CLOEXEC, 0o644)
                        files[segment] = fd
                        if os.fstat(fd).st_nlink > 1:
                            raise ProtoLockingError(
                                f"Segment {segment.hex} at {path} has {os.fstat(fd).st_nlink} hard links; a "
                                f"local segment must have exactly one name because opening repairs it")
                        if os.fstat(fd).st_size < SEGMENT_HEADER_SIZE:
                            _pwrite_all(fd, encode_segment_header(segment), 0)
                            self._dirty_dir = True
                        if own is not None:
                            if segment not in present:
                                own.uploaded = own.released = False
                                present.add(segment)
                        elif segment not in self._copies or self._copies[segment] is None or \
                                order > self._copies[segment]:
                            self._copies[segment] = order
                    elif own is None and (self._copies.get(segment) is None or order > self._copies[segment]):
                        self._copies[segment] = order
                    _pwrite_all(fd, data, offset)
                    self._stats["redo_records"] += 1
                    self._dirty_copies.add(segment)     # own or copy: synced by path at the checkpoint
                    written = True
                key = (entry.root.segment, entry.root.offset)
                if key not in self._ranges:
                    ranges = self._ranges_of(entry.records)
                    self._ranges[key] = (order, ranges)
                    write_all(self._ranges_fd, self._encode_ranges(key, order, ranges))
                    written = True
                self._stats["redo_entries"] += 1
        except OSError as e:
            raise ProtoUnexpectedError(f"Redoing the acceptor log's records in {self._path} failed: {e}") from e
        finally:
            for fd in files.values():
                os.close(fd)
        if written:
            logger.info("Rebuilt the local tier at %s from %d acceptor-log entries (%d records)", self._path,
                        self._stats["redo_entries"], self._stats["redo_records"])
        return written

    @staticmethod
    def _repair_own_segment(path: str, segment: uuid.UUID) -> int:
        """Check the own segment file ``path`` (part of the local WAL) record by record and
        truncate a torn tail; return its resulting size.

        Own segments are never appended to after a restart (each
        :class:`~proto_db.storage.file.FileStorage` writes only segments it creates), but one
        may end in a torn record left by a failed write, a ``SIGKILL`` or a power loss.  Such a
        tail is truncated and the file ``fsync``-ed, so the segment shipped to the object store
        holds valid records only; a header that is missing or torn is written again.

        The classification is framing-driven (record headers are self-validating, format
        version 2; 8.3d re-review 12, I3, ruling R60).  At the first invalid record, at offset
        ``pos``:

        * fewer bytes than a record header remain, or the header is valid and its record
          extends past the end of the file: a torn tail (the writer stopped inside that
          record; any later record of the writer would start past the end), trimmed without a
          search;
        * the header is valid and the record fits but its payload CRC fails: a torn tail when
          no valid record starts at or after the record's end (searched; the bytes inside its
          extent are its payload);
        * the header is invalid (its extent is unknown): a torn tail when no valid record starts
          at any offset after ``pos`` (searched).

        A valid record found by a search is mid-log corruption: ``ProtoCorruptionError``,
        nothing truncated (ruling R58); when the header was invalid the message says that the
        valid record may be the content of the torn record (ambiguous).  The search is exact
        and linear, with no cap (:func:`_first_valid_record`), so an ordinary torn tail is
        always trimmed.  A file with more than one hard link raises ``ProtoLockingError``
        before it is read (8.3d re-review 14; ruling R62).  ``FileNotFoundError`` propagates
        for a missing file."""
        fd = os.open(path, os.O_RDWR | _O_BINARY | _O_CLOEXEC)
        try:
            links = os.fstat(fd).st_nlink
            if links > 1:
                # Another name for the file (for example a snapshot that hard-links unchanged
                # files into another directory) may belong to a live instance the directory lock
                # does not cover; truncating the shared inode would cut its data (8.3d re-review
                # 14; ruling R62).  Refused before anything is read or repaired, as open_log does.
                raise ProtoLockingError(
                    f"Own segment {segment.hex} at {path} has {links} hard links; an own segment must have "
                    f"exactly one name (remove the other links) because opening repairs the file")
            data = read_fd(fd)
            size = len(data)
            view = memoryview(data)
            header = encode_segment_header(segment)
            if size >= SEGMENT_HEADER_SIZE and data[:SEGMENT_HEADER_SIZE] == header:
                pos = SEGMENT_HEADER_SIZE
                while pos < size:
                    length = _record_length(view, pos, size)
                    if length is None:
                        break
                    pos += length
                if pos == size:
                    return size
                decoded = record_header(view, pos)
                if decoded is None:
                    later = _first_valid_record(data, pos + 1, size)
                    ambiguous = True
                elif pos + RECORD_HEADER_SIZE + decoded[1] > size:
                    later = None                    # the writer's record, cut short
                    ambiguous = False
                else:
                    later = _first_valid_record(data, pos + RECORD_HEADER_SIZE + decoded[1], size)
                    ambiguous = False
            else:
                keep = 0
                while keep < min(size, SEGMENT_HEADER_SIZE) and data[keep] == header[keep]:
                    keep += 1
                if any(view[keep:]):
                    raise ProtoCorruptionError(
                        f"Own segment {segment.hex} at {path} does not start with its segment header but holds "
                        f"other data (a power loss before the segment's first sync can cause this, and then "
                        f"nothing in it was acknowledged; no entry offset applies: see D46 for the operator "
                        f"procedure, wipe and rejoin)")
                pos, later, ambiguous = 0, None, False
            if later is not None:
                raise ProtoCorruptionError(
                    f"Own segment {segment.hex} at {path} is corrupt: the record at offset {pos} is invalid "
                    f"but a valid record follows at offset {later} (not a torn tail; nothing was truncated)"
                    + ("; the invalid record's header is damaged, so its extent is unknown and the valid "
                       "record may be the content of a torn record (for example a blob holding segment "
                       "data) rather than a later record: ambiguous (see D46 for the operator procedure)"
                       if ambiguous else ""))
            logger.warning("Own segment %s ends in a torn tail of %d bytes (an interrupted write); truncated "
                           "to its last valid record at offset %d", path, size - pos, pos)
            os.ftruncate(fd, pos)
            if pos == 0:
                _pwrite_all(fd, header, 0)
                pos = SEGMENT_HEADER_SIZE
            os.fsync(fd)
            return pos
        finally:
            os.close(fd)

    def _load_ranges(self) -> None:
        self._ranges_fd, payloads = self._open_log(RANGES_LOG, RANGES_KIND)
        for payload in payloads:
            if len(payload) < _RANGES_HEAD.size:
                raise ProtoCorruptionError(f"{RANGES_LOG} in {self._path} holds a short entry")
            root_seg, root_off, label, epoch, count = _RANGES_HEAD.unpack_from(payload)
            if len(payload) != _RANGES_HEAD.size + count * _RANGE.size:
                raise ProtoCorruptionError(f"{RANGES_LOG} in {self._path} holds an entry of inconsistent length")
            ranges = [(uuid.UUID(bytes=seg), start, stop) for seg, start, stop in
                      _RANGE.iter_unpack(payload[_RANGES_HEAD.size:])]
            root = RootRecord(FORMAT_VERSION, uuid.UUID(bytes=root_seg), root_off, epoch)
            self._ranges[(root.segment, root_off)] = (root_key(label, root), ranges)

    @staticmethod
    def _ranges_payload(key: tuple, order: tuple, ranges: list) -> bytes:
        return _RANGES_HEAD.pack(key[0].bytes, key[1], order[0], order[1], len(ranges)) + b"".join(
            _RANGE.pack(seg.bytes, start, end) for seg, start, end in ranges)

    @classmethod
    def _encode_ranges(cls, key: tuple, order: tuple, ranges: list) -> bytes:
        return encode_frame(cls._ranges_payload(key, order, ranges))

    def _log_ranges(self, entry: Optional[bytes], *, sync: bool, force: bool = False) -> None:
        if entry is None:
            return
        with self._ranges_lock:
            self._refuse_closed(f"writing {RANGES_LOG}")
            try:
                write_all(self._ranges_fd, entry)
                if sync and (force or self._durability == "fsync"):
                    os.fsync(self._ranges_fd)
            except OSError as e:
                self._fail(e)
                raise ProtoUnexpectedError(f"Writing {RANGES_LOG} failed: {e}") from e

    def _compact_ranges(self) -> None:
        """Rewrite ``ranges.log`` with the live entries when it grew past the compaction size."""
        with self._ranges_lock:
            self._refuse_closed(f"compacting {RANGES_LOG}")
            try:
                if os.fstat(self._ranges_fd).st_size <= _ROOTS_LOG_COMPACT:
                    return
                with self._lock:
                    live = log_contents(RANGES_KIND, [self._ranges_payload(k, e, r)
                                                      for k, (e, r) in self._ranges.items()])
                # Open and fully prepare the replacement, swap, and only then close the old
                # descriptor (8.3d re-review 9, B1): the attribute never holds a closed number.
                new = prepare_replacement(os.path.join(self._path, RANGES_LOG), live,
                                          os.O_WRONLY | _O_BINARY | _O_CLOEXEC)
                old, self._ranges_fd = self._ranges_fd, new
                os.close(old)
            except OSError as e:
                self._fail(e)
                raise ProtoUnexpectedError(f"Compacting {RANGES_LOG} failed: {e}") from e
            self._fsync_dir()

    # -- writes ------------------------------------------------------------------------------

    def _push(self, kind: RecordKind, payload: bytes) -> AtomPointer:
        self._check_open()
        limit = _protocol.MAX_RECORD_BYTES
        if len(payload) > limit:
            raise ProtoValidationError(
                f"A record of {len(payload)} bytes exceeds the {limit} bytes a cluster node can replicate")
        ptr = self._local.append_record(kind, payload)
        framed = encode_record(kind, payload)
        self._cache.put((ptr.segment, ptr.offset), framed)
        with self._lock:
            known = ptr.segment in self._own
        if not known:
            # A new own segment: registered and appended to the manifest in creation order under
            # the manifest lock, never under the bookkeeping lock that every read takes.
            with self._manifest_lock:
                self._refuse_closed(f"writing {MANIFEST}")
                with self._lock:
                    write = ptr.segment not in self._own
                    if write:
                        self._own[ptr.segment] = _Own(ptr.segment)
                if write:
                    try:
                        write_all(self._manifest_fd, encode_frame(ptr.segment.bytes))
                    except OSError as e:
                        # The segment is registered but not in the manifest: nothing may be
                        # flushed or shipped as if it were (the tier is poisoned).
                        self._fail(e)
                        raise ProtoUnexpectedError(f"Writing {MANIFEST} failed: {e}") from e
                    with self._lock:
                        self._manifest_dirty = True
        with self._lock:
            own = self._own[ptr.segment]
            own.end = max(own.end, ptr.offset + len(framed))
            if own.first is None:
                own.first = time.monotonic()
            self._unshipped.append([ptr.segment.bytes, ptr.offset, framed])
        return ptr

    def push_atom(self, payload: dict) -> AtomPointer:
        """See :meth:`SharedStorage.push_atom`."""
        if not isinstance(payload, dict):
            raise ProtoValidationError("Atom payload must be a dict")
        return self._push(RecordKind.ATOM, msgpack.packb(payload, use_bin_type=True))

    def push_blob(self, data: bytes) -> AtomPointer:
        """See :meth:`SharedStorage.push_blob`."""
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise ProtoValidationError("Blob data must be bytes")
        return self._push(RecordKind.BLOB, bytes(data))

    def flush(self) -> None:
        """Make every pushed record durable in the local tier (``fsync`` per the durability mode)."""
        self._check_open()
        self._local.flush()
        with self._manifest_lock:
            self._refuse_closed(f"syncing {MANIFEST}")
            with self._lock:
                dirty, self._manifest_dirty = self._manifest_dirty, False
            if dirty and self._durability == "fsync":
                try:
                    os.fsync(self._manifest_fd)
                except OSError as e:
                    # Never retried: after a failed fsync what reached the disk is unknown.
                    with self._lock:
                        self._manifest_dirty = True
                    self._fail(e)
                    raise ProtoUnexpectedError(f"Syncing {MANIFEST} failed: {e}") from e

    # -- reads -------------------------------------------------------------------------------

    @contextlib.contextmanager
    def critical_section(self):
        """Context manager marking the calling thread as inside a commit's critical section:
        reads it makes that miss the record cache, and root writes, are counted."""
        previous = getattr(self._tls, "critical", False)
        self._tls.critical = True
        try:
            yield
        finally:
            self._tls.critical = previous

    def set_fetcher(self, fetcher: Optional[Callable]) -> None:
        """Install the peer page request function (``fetcher(ptr) -> framed bytes or None``)."""
        self._fetcher = fetcher

    def _local_record(self, ptr: AtomPointer) -> Optional[tuple]:
        try:
            return self._local.get_record(ptr)
        except ProtoCorruptionError:
            return None            # not here (missing file, a hole, or a torn write): look elsewhere

    def local_record(self, ptr: AtomPointer) -> Optional[bytes]:
        """The framed record at ``ptr`` if this node holds it (cache or local files), else ``None``.

        Serves peer page requests; never asks a peer or the object store.
        """
        framed = self._cache.get((ptr.segment, ptr.offset))
        if framed is not None:
            return framed
        if self._closed:
            return None
        found = self._local_record(ptr)
        return None if found is None else encode_record(found[0], found[1])

    def _record(self, ptr: AtomPointer) -> tuple:
        if not isinstance(ptr, AtomPointer):
            raise ProtoValidationError("Expected an AtomPointer")
        self._check_open()
        key = (ptr.segment, ptr.offset)
        framed = self._cache.get(key)
        if framed is not None:
            self._count("reads_cache")
            return read_record(framed, 0)
        if getattr(self._tls, "critical", False):
            self._count("critical_misses")
        found = self._local_record(ptr)
        if found is not None:
            self._count("reads_local")
        else:
            asked = not self._known_remote(ptr)
            found = self._from_peer(ptr) if asked else None
            if found is not None:
                self._count("reads_peer")
            else:
                found = self._store_record(ptr, ask_peers=not asked)
        self._cache.put(key, encode_record(found[0], found[1]))
        return found

    def _store_record(self, ptr: AtomPointer, *, ask_peers: bool) -> tuple:
        """The record at ``ptr`` from the object store.  When the store's object turns out
        short or corrupt there, the record is read from the peers (unless they were just asked,
        ``ask_peers`` False), then from a part; ``ProtoCorruptionError`` when no full copy is
        anywhere -- never short data."""
        try:
            found = self._remote_record(ptr)
            self._count("reads_remote")
            return found
        except ProtoCorruptionError as e:
            failure = e
        found = self._from_peer(ptr) if ask_peers else None
        if found is not None:
            logger.warning("The object store's copy of the record at %s is short or corrupt (%s); read "
                           "from a peer", ptr, failure)
            self._count("reads_peer")
            return found
        found = self._part_record(ptr)
        if found is not None:
            logger.warning("The object store's sealed copy of the record at %s is short or corrupt (%s); "
                           "read from a part", ptr, failure)
            self._count("reads_remote")
            return found
        raise ProtoCorruptionError(f"There is no full copy of the record at {ptr}: the object store's "
                                   f"copy is short or corrupt ({failure}) and no reachable peer holds "
                                   f"it") from failure

    def _part_record(self, ptr: AtomPointer) -> Optional[tuple]:
        """The record at ``ptr`` from the largest part of its segment in the store, or ``None``."""
        try:
            self._list_remote()
        except (ProtoDBError, OSError) as e:
            logger.info("Listing the object store failed while looking for a part: %s", e)
        with self._lock:
            end = self._parts.get(ptr.segment)
        if end is None or end <= ptr.offset:
            return None
        part = self._part_key(ptr.segment, end)
        try:
            data, _ = self._retrying(lambda: self._store.get(part), f"Reading segment part {part}")
            if decode_segment_header(data) != ptr.segment:
                return None
            return read_record(data, ptr.offset)
        except (ObjectNotFoundError, ProtoCorruptionError) as e:
            logger.warning("Segment part %s cannot serve the record at %s: %s", part, ptr, e)
            return None

    def _known_remote(self, ptr: AtomPointer) -> bool:
        """True when the store is known to hold the record at ``ptr`` (from uploads, downloads
        and the listings of :meth:`refresh_remote` and :meth:`ensure_remote`; never lists)."""
        with self._lock:
            return self._covered(ptr.segment, ptr.offset + 1)

    def refresh_remote(self) -> None:
        """List the store and record the segments and parts it holds (the persister calls it
        when it starts)."""
        self._check_open()
        self._list_remote()

    def _from_peer(self, ptr: AtomPointer) -> Optional[tuple]:
        fetcher = self._fetcher
        if fetcher is None:
            return None
        data = fetcher(ptr)
        if data is None:
            return None
        try:
            return read_record(data, 0)
        except ProtoCorruptionError as e:
            logger.warning("A peer answered a corrupt record for %s: %s", ptr, e)
            return None

    def _segment_object(self, segment: uuid.UUID, *, reload: bool = False) -> bytes:
        """The sealed object of ``segment``, else its largest part; cached (``reload`` skips the cache)."""
        data = None if reload else self._segments.get(segment)
        if data is not None:
            return data
        key = self.segment_key(segment)
        try:
            data, _ = self._retrying(lambda: self._store.get(key), f"Reading segment {key}")
            sealed = True
        except ObjectNotFoundError as e:
            self._list_remote()
            with self._lock:
                end = self._parts.get(segment)
            if end is None:
                raise ProtoCorruptionError(f"Segment {segment.hex} is in no tier (local, peers, object store)") from e
            part = self._part_key(segment, end)
            data, _ = self._retrying(lambda: self._store.get(part), f"Reading segment part {part}")
            sealed = False
        self._count("remote_segment_gets")
        if decode_segment_header(data) != segment:
            raise ProtoCorruptionError(f"Segment object of {segment.hex} has a foreign header")
        self._segments.put(segment, data)
        if sealed:
            self._note_sealed(segment, len(data))
        return data

    def _note_sealed(self, segment: uuid.UUID, length: int) -> None:
        """The sealed object of ``segment`` is in the store, ``length`` bytes long as uploaded,
        listed (marker) or downloaded; the shortest length known is kept, so an object found
        shorter than recorded covers only what it holds."""
        with self._lock:
            self._remote.add(segment)
            known = self._sealed.get(segment)
            if known is None or length < known:
                self._sealed[segment] = length

    def _remote_record(self, ptr: AtomPointer) -> tuple:
        data = self._segment_object(ptr.segment)
        if ptr.offset >= len(data):
            data = self._segment_object(ptr.segment, reload=True)     # a cached part may be short
        return read_record(data, ptr.offset)

    def get_atom(self, ptr: AtomPointer) -> dict:
        """See :meth:`SharedStorage.get_atom`."""
        kind, payload = self._record(ptr)
        if kind != RecordKind.ATOM:
            raise ProtoCorruptionError(f"Record at {ptr} is a {kind.name}, not an ATOM")
        try:
            return msgpack.unpackb(payload, raw=False)
        except (msgpack.exceptions.UnpackException, ValueError) as e:
            raise ProtoCorruptionError(f"Undecodable atom at {ptr}") from e

    def get_blob(self, ptr: AtomPointer) -> bytes:
        """See :meth:`SharedStorage.get_blob`."""
        kind, payload = self._record(ptr)
        if kind != RecordKind.BLOB:
            raise ProtoCorruptionError(f"Record at {ptr} is a {kind.name}, not a BLOB")
        return payload

    # -- cluster tier: replication -----------------------------------------------------------

    def outgoing(self) -> list:
        """Records not yet shipped to the peers, ``[segment bytes, offset, framed record]``."""
        with self._lock:
            return list(self._unshipped)

    def flush_local(self, force: bool = False) -> None:
        """Make this node's records durable (the commit-durability tier): per the durability
        mode, or with ``force`` synced whatever the mode (a holder whose own acceptance is
        refused still makes its records durable before they leave the node, Task 8.3f)."""
        if not force:
            self.flush()
            return
        self._check_open()
        self._local.sync()
        self._sync_manifest()

    def _sync_manifest(self) -> None:
        """``fsync`` ``segments.log`` when an own segment was registered since its last sync,
        whatever the durability mode."""
        with self._manifest_lock:
            self._refuse_closed(f"syncing {MANIFEST}")
            with self._lock:
                dirty, self._manifest_dirty = self._manifest_dirty, False
            if dirty:
                try:
                    os.fsync(self._manifest_fd)
                except OSError as e:
                    with self._lock:
                        self._manifest_dirty = True
                    self._fail(e)
                    raise ProtoUnexpectedError(f"Syncing {MANIFEST} failed: {e}") from e

    def accept_replication(self, label: int, root: RootRecord, records: list, *, seq: int = 0,
                           pred: Optional[tuple] = None) -> None:
        """Accept the replicated root ``root`` labelled ``label`` with the ``records`` its
        replication carried (``[segment bytes, offset, framed record]``) by ONE append to
        ``roots.log`` (kind 5: the root, ``seq``, ``pred`` and the records), ``fsync``-ed in
        every durability mode, before this returns (Task 8.3f phase B, D50).  The copies of
        other nodes' records and the commit's byte ranges are then written without a sync (own
        records are already in this node's segment buffer): a crash or a power loss is
        repaired from the entry when the storage opens (redo), and a checkpoint syncs them
        before the entry loses its records.  So the root is durable exactly when its records
        are (D-AVAIL-1).  When an own segment was registered since the manifest's last sync,
        the manifest is synced first: a redo must know its own segments.

        Raises ``ProtoValidationError`` once closed and ``ProtoUnexpectedError`` once the local
        tier failed; a failure after the append poisons the tier (the entry may be durable: the
        node then never acknowledges, which an acceptor may always do)."""
        self._check_open()
        self._refuse_failed(f"writing {ROOTS_LOG}")
        if self._roots.pending_records() >= CHECKPOINT_BYTES:
            self.checkpoint()        # before the append: a failure here accepts nothing
        with self._lock:
            own_dirty = self._manifest_dirty and any(uuid.UUID(bytes=raw) in self._own for raw, _, _ in records)
        if own_dirty:
            self._sync_manifest()
        order = root_key(label, root)
        with self._accept_lock:
            self._refuse_closed(f"writing {ROOTS_LOG}")
            self._roots.append_records(label, root, records, seq=seq, pred=pred)
            with self._lock:
                entry = self._remember(records, root, label)
                self._stats["acceptances_logged"] += 1
            self._log_ranges(entry, sync=False)
            self._write_copies(records, order, sync=False)

    def checkpoint(self) -> None:
        """Sync every data write of the acceptances logged so far -- own segments, copies of
        other nodes' segments, ``ranges.log``, ``segments.log`` and the directory -- whatever
        the durability mode, then compact ``roots.log``, whose entries before the checkpoint
        no longer need their records (Task 8.3f phase B).  Entries appended meanwhile keep
        theirs.  A failure poisons the local tier."""
        self._check_open()
        with self._accept_lock:
            mark = self._roots.size()
        self._checkpoint_data()
        self._roots.compact_records(mark)
        with self._lock:
            self._stats["checkpoints"] += 1

    def _checkpoint_data(self) -> None:
        """The sync half of :meth:`checkpoint` (every data write made before this call)."""
        with self._lock:
            dirty, self._dirty_copies = self._dirty_copies, set()
            dirty_dir, self._dirty_dir = self._dirty_dir, False
        try:
            self._local.sync()
            with self._copies_lock:
                self._refuse_closed("checkpointing copies")
                for segment in sorted(dirty):
                    path = os.path.join(self._path, segment.hex + SEGMENT_SUFFIX)
                    try:
                        fd = os.open(path, os.O_RDONLY | _O_BINARY | _O_CLOEXEC)
                    except FileNotFoundError:
                        continue            # released meanwhile (the store holds it)
                    try:
                        os.fsync(fd)
                    finally:
                        os.close(fd)
            with self._ranges_lock:
                self._refuse_closed(f"syncing {RANGES_LOG}")
                os.fsync(self._ranges_fd)
            self._sync_manifest()
            if dirty_dir:
                self._fsync_dir()
        except OSError as e:
            with self._lock:
                self._dirty_copies |= dirty
                self._dirty_dir = self._dirty_dir or dirty_dir
            self._fail(e)
            raise ProtoUnexpectedError(f"Checkpointing the local tier at {self._path} failed: {e}") from e
        except BaseException:
            with self._lock:
                self._dirty_copies |= dirty
                self._dirty_dir = self._dirty_dir or dirty_dir
            raise

    @staticmethod
    def _ranges_of(records: list) -> list:
        spans = {}
        for raw, offset, data in records:
            if data is None:
                continue
            segment = uuid.UUID(bytes=raw)
            start, end = spans.get(segment, (offset, offset))
            spans[segment] = (min(start, offset), max(end, offset + len(data)))
        return [(segment, start, end) for segment, (start, end) in spans.items()]

    def _remember(self, records: list, root: Optional[RootRecord], label: int) -> Optional[bytes]:
        """Remember the ranges of the records shipped with the commit of ``root`` labelled
        ``label`` (lock held); return the ``ranges.log`` entry to write."""
        if root is None:
            return None
        key, order, ranges = (root.segment, root.offset), root_key(label, root), self._ranges_of(records)
        self._ranges[key] = (order, ranges)
        return self._encode_ranges(key, order, ranges)

    def commit_ranges(self, root: RootRecord) -> Optional[list]:
        """``[(segment, start, end)]`` of the records shipped with the commit of ``root``, if this
        node accepted it since it started (``None`` otherwise)."""
        with self._lock:
            entry = self._ranges.get((root.segment, root.offset))
        return None if entry is None else list(entry[1])

    def remember_commit(self, records: list, root: Optional[RootRecord], label: int) -> None:
        """Remember the byte ranges of ``records``, shipped by this node with the commit of
        ``root`` labelled ``label``, before the outcome of that commit is known (logged without
        ``fsync``: own records reach the store in order anyway).  A peer that adopts the root
        later (a rejoin, or a holder re-proposing it) learns from here where its records are."""
        with self._lock:
            entry = self._remember(records, root, label)
        self._log_ranges(entry, sync=False)

    def commit_records(self, ranges: list) -> Optional[list]:
        """The records of one commit's byte ``ranges`` (``[(segment, start, end)]``) that the
        object store is not known to hold, as ``[segment bytes, offset, framed record]``: read
        from this node's disk, else from the peers (page requests).  Records of a commit are
        contiguous in each range, so each range is walked record by record.  Returns ``None``
        when a record is nowhere -- not here, not on a peer that answered, and its segment not
        in the store (listed once, as a last resort).  Never writes."""
        out = []
        listed = False
        for segment, start, end in ranges:
            with self._lock:
                covered = self._covered(segment, end)
            offset = start
            found = []
            while not covered and offset < end:
                ptr = AtomPointer(segment, offset)
                framed = self.local_record(ptr)
                if framed is None:
                    framed = self._peer_framed(ptr)
                if framed is None:
                    if listed:
                        return None
                    listed = True
                    try:
                        self._list_remote()
                    except (ProtoDBError, OSError) as e:
                        logger.info("Listing the object store failed while gathering records: %s", e)
                    with self._lock:
                        covered = self._covered(segment, end)
                    if covered:
                        found = []
                        break
                    return None
                found.append([segment.bytes, offset, framed])
                offset += len(framed)
            out.extend(found)
        return out

    def _peer_framed(self, ptr: AtomPointer) -> Optional[bytes]:
        """The framed record at ``ptr`` from a peer (page request), validated, or ``None``."""
        fetcher = self._fetcher
        data = fetcher(ptr) if fetcher is not None else None
        if data is None:
            return None
        try:
            kind, payload = read_record(data, 0)
        except ProtoCorruptionError as e:
            logger.warning("A peer answered a corrupt record for %s: %s", ptr, e)
            return None
        if len(encode_record(kind, payload)) != len(data):
            logger.warning("A peer answered a malformed record for %s", ptr)
            return None
        return data

    def store_commit(self, records: list, root: RootRecord, label: int, ranges: list) -> None:
        """Store ``records`` of the commit of ``root`` labelled ``label`` at their original
        offsets and remember the commit's byte ``ranges``, both ``fsync``-ed whatever the
        durability mode (own records are skipped): the caller then logs the root as accepted
        (a rejoin) or sends it (a re-proposed base), so the records must be durable first
        (D-AVAIL-1, Task 8.3f).  This node then
        holds every record of that commit the object store lacks, can serve them to its peers
        and can upload them as parts."""
        self._check_open()
        order = root_key(label, root)
        self._write_copies(records, order, sync=True)
        with self._lock:
            key = (root.segment, root.offset)
            self._ranges[key] = (order, list(ranges))
            entry = self._encode_ranges(key, order, ranges)
        self._log_ranges(entry, sync=True, force=True)

    def parent_root(self, record: RootRecord) -> Optional[tuple]:
        """``(label, RootRecord)`` of the cluster root the root ``record`` was built on, or
        ``None`` when it has none (the first root, or a root written outside cluster mode:
        persisted before the cluster).  Reads the root's space root atom."""
        from ..cluster.persister import cluster_fields
        _, _, previous = cluster_fields(self, record)
        if previous is None or previous.cluster_epoch is None:
            return None
        ptr = previous.atom_pointer
        return previous.cluster_term or 0, RootRecord(FORMAT_VERSION, ptr.segment, ptr.offset,
                                                      previous.cluster_epoch)

    def shipped(self, records: list, root: Optional[RootRecord] = None, label: int = 0) -> None:
        """``records`` (a prefix returned by :meth:`outgoing`) reached a majority with the commit
        of ``root`` labelled ``label``."""
        order = root_key(label, root) if root is not None else None
        with self._lock:
            n = len(records)
            if n and self._unshipped[:1] and self._unshipped[0] is records[0]:
                del self._unshipped[:n]
            for raw, _, _ in records:
                own = self._own.get(uuid.UUID(bytes=raw))
                if own is not None and order is not None and (own.max_key is None or order > own.max_key):
                    own.max_key = order
            # Remembered when the root was sent (:meth:`remember_commit`), maybe with the records
            # of a re-proposed base too: keep that entry.
            known = root is not None and (root.segment, root.offset) in self._ranges
            entry = None if known else self._remember(records, root, label)
        self._log_ranges(entry, sync=False)     # own records reach the store in order anyway

    def apply(self, records: list, root: Optional[RootRecord] = None, label: int = 0) -> None:
        """Store records received with the replicated root ``root`` labelled ``label`` at their
        original offsets (durable per the durability mode) and cache them.  Own records are
        skipped."""
        self._check_open()
        order = root_key(label, root) if root is not None else None
        with self._lock:
            entry = self._remember(records, root, label)
        self._log_ranges(entry, sync=True)
        self._write_copies(records, order)

    def _write_copies(self, records: list, order: Optional[tuple], *, sync: Optional[bool] = None) -> None:
        """Write other nodes' ``records`` into the local copies of their segments at their
        original offsets and cache them; ``order`` is the root_key of the commit that wrote
        them (local release, see :meth:`release_local`).  ``sync``: ``None`` syncs per the
        durability mode, ``True`` always, ``False`` never (an acceptance's records, made
        durable by its kind-5 entry and synced at the next checkpoint)."""
        sync = self._durability == "fsync" if sync is None else sync
        groups = {}
        for raw, offset, data in records:
            if data is not None:
                groups.setdefault(uuid.UUID(bytes=raw), []).append((offset, data))
        if not groups:
            return
        # Under the copies lock, which close() takes: no copy is written once it has started.
        with self._copies_lock:
            self._refuse_closed("storing replicated records")
            created = False
            for segment, items in groups.items():
                with self._lock:
                    if segment in self._own:
                        continue
                    known = segment in self._copies
                    newest = self._copies.get(segment)
                    if order is not None and (newest is None or order > newest):
                        newest = order
                    self._copies[segment] = newest
                path = os.path.join(self._path, segment.hex + SEGMENT_SUFFIX)
                try:
                    fd = os.open(path, os.O_RDWR | os.O_CREAT | _O_BINARY | _O_CLOEXEC, 0o644)
                    try:
                        if os.fstat(fd).st_size < SEGMENT_HEADER_SIZE:
                            _pwrite_all(fd, encode_segment_header(segment), 0)
                            created = created or not known
                        for offset, data in items:
                            _pwrite_all(fd, data, offset)
                        if sync:
                            os.fsync(fd)
                        else:
                            with self._lock:
                                self._dirty_copies.add(segment)
                    finally:
                        os.close(fd)
                except OSError as e:
                    self._fail(e)
                    raise ProtoUnexpectedError(f"Storing replicated records of segment {segment.hex} failed: {e}") from e
                for offset, data in items:
                    self._cache.put((segment, offset), data)
                self._count("records_applied", len(items))
            if created and sync:
                self._fsync_dir()
            elif created:
                with self._lock:
                    self._dirty_dir = True

    def prefetch(self, records: list) -> None:
        """Warm the caches with replicated records (already cached by :meth:`apply`)."""

    def _fsync_dir(self) -> None:
        """Make the local directory's entries durable; a failure poisons the local tier."""
        try:
            fd = os.open(self._path, os.O_RDONLY | _O_CLOEXEC)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        except OSError as e:
            self._fail(e)
            raise ProtoUnexpectedError(f"Cannot fsync {self._path}: {e}") from e

    def log_root(self, kind: str, label: int, record: Optional[RootRecord]) -> None:
        """Append a root this node accepted or learnt committed to ``roots.log``, ``fsync``-ed
        whatever this storage's durability mode (Task 8.3e; see
        :class:`~proto_db.cluster.rootlog.RootsLog`).  Refused once the local tier failed (an
        accepted root vouches for records stored here)."""
        self._check_open()
        self._refuse_failed(f"writing {ROOTS_LOG}")
        self._roots.append(kind, label, record)

    def log_promise(self, term: int) -> None:
        """Make this node's promise of lease ``term`` durable in ``roots.log`` before it is sent.
        Refused once the local tier failed."""
        self._check_open()
        self._refuse_failed(f"writing {ROOTS_LOG}")
        self._roots.promise(term)

    def log_rejoined(self) -> None:
        """Append the rejoined marker to ``roots.log`` (the node's rejoin completed).  Refused
        once the local tier failed."""
        self._check_open()
        self._refuse_failed(f"writing {ROOTS_LOG}")
        self._roots.mark_rejoined()

    def load_roots(self) -> LoggedRoots:
        """The roots and the promise logged in ``roots.log`` (a
        :class:`~proto_db.cluster.rootlog.LoggedRoots`)."""
        return self._roots.load()

    # -- cluster tier: object store ----------------------------------------------------------

    def _sleep(self, attempt: int) -> None:
        low, high = self._backoff
        delay = min(high, low * (2 ** attempt))
        if delay > 0:
            time.sleep(delay * (0.5 + random.random() / 2))

    def _retrying(self, operation, what: str):
        for attempt in range(self._retries + 1):
            try:
                return operation()
            except (TransientObjectStoreError, OutcomeUnknownError) as e:
                if attempt == self._retries:
                    raise ProtoUnexpectedError(f"{what} failed after {attempt + 1} attempt(s): {e}") from e
            self._sleep(attempt)
        raise ProtoUnexpectedError("unreachable: the retry loop always returns or raises")  # pragma: no cover

    def read_persisted(self) -> Optional[tuple]:
        """``(label, RootRecord)`` of the root persisted in the object store, or ``None``."""
        self._check_open()
        try:
            data, _ = self._retrying(lambda: self._store.get(self._root_key), f"Reading {self._root_key}")
        except ObjectNotFoundError:
            return None
        return _decode_root_object(data)

    def persist(self, label: int, record: RootRecord) -> bool:
        """Write ``record`` (cluster epoch, label ``label``) as the persisted root by
        compare-and-swap, never moving the persisted root backwards in the order of roots
        (:func:`~proto_db.cluster.rootlog.root_key`, D46).

        Returns True when written, False when the store already holds that root or a newer
        one.  Raises ``ProtoUnexpectedError`` when the store stays unreachable.
        """
        data = _encode_root_object(record, label)
        with self._ship_lock:       # object-store writes: never once close() has started
            self._refuse_closed("persisting the root", local=False)
            for attempt in range(self._retries + 1):
                try:
                    try:
                        current, etag = self._store.get(self._root_key)
                        current_label, current_root = _decode_root_object(current)
                    except ObjectNotFoundError:
                        etag, current_label, current_root = None, 0, None
                    if etag is not None and compare_roots(current_label, current_root, label, record) >= 0:
                        return False
                    if etag is None:
                        self._store.put_if_absent(self._root_key, data)
                    else:
                        self._store.put_if_match(self._root_key, data, etag)
                except PreconditionFailedError:
                    continue            # another node wrote meanwhile: read again, maybe we are behind
                except (TransientObjectStoreError, OutcomeUnknownError) as e:
                    if attempt == self._retries:
                        raise ProtoUnexpectedError(f"Persisting root epoch {record.epoch} failed: {e}") from e
                    self._sleep(attempt)
                    continue            # an unknown outcome is decided by reading the root again
                with self._lock:
                    self._stats["root_writes"] += 1
                    if getattr(self._tls, "critical", False):
                        self._stats["root_writes_in_critical"] += 1
                return True
            raise ProtoUnexpectedError(f"Persisting root epoch {record.epoch} lost {self._retries + 1} races")

    def ship(self, force: bool = False) -> bool:
        """Seal the open segment when due (or ``force``) and upload sealed own segments in order.

        Returns True when something was uploaded.  Stops at the first failure (the order is
        strict) and raises it; the next call resumes.  Called by the persister only.
        """
        with self._ship_lock:
            self._refuse_closed("shipping", local=False)     # its flush() checks the local tier
            with self._lock:
                pending = [o for o in self._own.values() if not o.uploaded]
            if not pending:
                return False
            current = self._local.current_segment
            open_seg = self._own.get(current) if current is not None else None
            if open_seg is not None and not open_seg.uploaded and open_seg.first is not None and \
                    (force or time.monotonic() - open_seg.first >= self._ship_interval):
                self._local.roll_segment()
            self.flush()
            current = self._local.current_segment
            uploaded = False
            for own in pending:
                if own.uuid == current:
                    break           # still receiving appends; later segments wait for it
                path = os.path.join(self._path, own.uuid.hex + SEGMENT_SUFFIX)
                try:
                    with open(path, "rb") as f:
                        data = f.read()
                except FileNotFoundError as e:
                    # Released only once uploaded, so a missing segment not uploaded is lost data:
                    # never report it as being in the store.
                    raise ProtoCorruptionError(
                        f"Own segment {own.uuid.hex} is missing from {self._path} before its upload") from e
                except OSError as e:
                    raise ProtoUnexpectedError(f"Reading segment {own.uuid.hex} for upload failed: {e}") from e
                self._put_segment(own.uuid, data)
                self._put_sealed_marker(own.uuid, len(data))
                self._note_sealed(own.uuid, len(data))
                with self._lock:
                    own.uploaded = True
                    own.first = None
                    self._stats["segments_uploaded"] += 1
                self._segments.put(own.uuid, data)
                uploaded = True
            return uploaded

    def _put_segment(self, segment: uuid.UUID, data: bytes) -> None:
        key = self.segment_key(segment)

        def attempt():
            try:
                self._store.put_if_absent(key, data)
                return
            except PreconditionFailedError as e:
                conflict = e
            existing, _ = self._store.get(key)
            if existing != data:
                raise ProtoUnexpectedError(f"Segment object {key} already exists with different content") from conflict

        self._retrying(attempt, f"Uploading segment {key}")

    def _put_sealed_marker(self, segment: uuid.UUID, length: int) -> None:
        """Record the length of ``segment``'s sealed object in the store (an empty marker
        object whose key holds it), after the object itself: a listing that shows the marker
        shows the object, and peers judge what the object covers without downloading it."""
        key = f"{self._prefix}{SEGMENT_PREFIX}{segment.hex}.{length:016x}{SEALED_SUFFIX}"

        def attempt():
            try:
                self._store.put_if_absent(key, b"")
            except PreconditionFailedError:
                pass                # our own earlier upload: the same empty marker

        self._retrying(attempt, f"Recording the length of segment object {segment.hex}")

    def _part_key(self, segment: uuid.UUID, end: int) -> str:
        return f"{self._prefix}{SEGMENT_PREFIX}{segment.hex}.{end:016x}{PART_SUFFIX}"

    def _list_remote(self) -> None:
        """List the segment objects and parts in the store and record them."""
        prefix = self._prefix + SEGMENT_PREFIX
        keys = self._retrying(lambda: self._store.list(prefix), f"Listing {prefix}")
        sealed, lengths, parts = set(), {}, {}
        for key in keys:
            name = key[len(prefix):]
            try:
                if name.endswith(PART_SUFFIX):
                    hex_id, end = name[:-len(PART_SUFFIX)].split(".")
                    segment = uuid.UUID(hex=hex_id)
                    parts[segment] = max(parts.get(segment, 0), int(end, 16))
                elif name.endswith(SEALED_SUFFIX):
                    hex_id, length = name[:-len(SEALED_SUFFIX)].split(".")
                    segment = uuid.UUID(hex=hex_id)
                    lengths[segment] = min(lengths.get(segment, int(length, 16)), int(length, 16))
                elif name.endswith(SEGMENT_SUFFIX):
                    sealed.add(uuid.UUID(hex=name[:-len(SEGMENT_SUFFIX)]))
            except ValueError:
                continue            # not ours
        with self._lock:
            self._remote |= sealed
            for segment, length in lengths.items():
                if segment in sealed:
                    known = self._sealed.get(segment)
                    self._sealed[segment] = length if known is None else min(known, length)
            for segment, end in parts.items():
                self._parts[segment] = max(self._parts.get(segment, 0), end)

    def _covered(self, segment: uuid.UUID, end: Optional[int]) -> bool:
        """True when the store holds ``segment`` up to ``end`` (lock held): its sealed object of
        a known length of at least ``end`` bytes, or a part at least that long.  ``None`` asks
        for the sealed object of a known length, whatever it is (module docstring)."""
        length = self._sealed.get(segment) if segment in self._remote else None
        if end is None:
            return length is not None
        return (length is not None and length >= end) or self._parts.get(segment, 0) >= end

    def _copy_prefix(self, segment: uuid.UUID, end: int) -> Optional[bytes]:
        """The first bytes of the local copy of ``segment``, up to the end of the last valid
        record of its contiguous prefix, if that reaches ``end``; else ``None``."""
        path = os.path.join(self._path, segment.hex + SEGMENT_SUFFIX)
        try:
            with open(path, "rb") as f:
                data = f.read()
        except FileNotFoundError:
            return None
        except OSError as e:
            raise ProtoUnexpectedError(f"Reading the copy of segment {segment.hex} failed: {e}") from e
        try:
            if decode_segment_header(data) != segment:
                return None
        except ProtoCorruptionError:
            return None
        offset = SEGMENT_HEADER_SIZE
        while offset < end:
            try:
                kind, payload = read_record(data, offset)
            except ProtoCorruptionError:
                return None         # a hole or a torn record before ``end``
            offset += len(encode_record(kind, payload))
        while True:                 # extend to the end of the contiguous prefix
            try:
                kind, payload = read_record(data, offset)
            except ProtoCorruptionError:
                break
            offset += len(encode_record(kind, payload))
        return data[:offset]

    def _put_part(self, segment: uuid.UUID, data: bytes) -> None:
        key = self._part_key(segment, len(data))

        def attempt():
            try:
                self._store.put_if_absent(key, data)
            except PreconditionFailedError as e:
                existing, _ = self._store.get(key)
                if existing != data:
                    raise ProtoCorruptionError(f"Part object {key} disagrees with the local copy") from e

        self._retrying(attempt, f"Uploading segment part {key}")
        with self._lock:
            self._parts[segment] = max(self._parts.get(segment, 0), len(data))
            self._stats["parts_uploaded"] += 1

    def ensure_remote(self, needs, *, upload_copies: bool = False) -> bool:
        """True when the store holds every ``(segment, end)`` of ``needs``: the segment's sealed
        object with a recorded length of at least ``end`` bytes, or a part at least that long
        (``end`` ``None``: a sealed object of a known length, see :meth:`_covered`).  Several
        requirements on one segment must all hold: the longest ``end``, and, if any is
        ``None``, a sealed object of a known length too (neither implies the other: a part
        satisfies an ``end`` but is no sealed object, and a short sealed object is one).

        Lists the store once when something is not known to be there.  With
        ``upload_copies``, a missing segment this node holds a copy of (not an own segment,
        which its writer ships in order) is uploaded as a part when the copy's contiguous
        prefix reaches ``end``; a part never satisfies a ``None`` requirement.  Called by the
        persister only.
        """
        needed = {}                 # segment -> (a sealed object required, longest end)
        for segment, end in needs:
            sealed, longest = needed.get(segment, (False, 0))
            needed[segment] = (sealed or end is None, longest if end is None else max(longest, end))

        def covered(segment, need):
            sealed, end = need
            return (not sealed or self._covered(segment, None)) and self._covered(segment, end)

        with self._lock:
            missing = {s: n for s, n in needed.items() if not covered(s, n)}
        if not missing:
            return True
        self._list_remote()
        with self._lock:
            missing = {s: n for s, n in missing.items() if not covered(s, n)}
            # A part covers only a length; a missing sealed object stays missing.
            copies = {s: n[1] for s, n in missing.items()
                      if s not in self._own and (not n[0] or self._covered(s, None))}
        if not missing:
            return True
        if not upload_copies or len(copies) < len(missing):
            return False
        with self._ship_lock:       # object-store writes: never once close() has started
            self._refuse_closed("uploading segment parts", local=False)
            for segment, end in copies.items():
                data = self._copy_prefix(segment, end)
                if data is None:
                    return False    # a hole: another node's copy must serve
                self._put_part(segment, data)
        return True

    def release_local(self, persisted_key: tuple) -> int:
        """Delete local segments (own and copies) the store holds in full (as long as the local
        file, see :meth:`_covered`) and whose commits all order
        at or below the persisted root whose :func:`~proto_db.cluster.rootlog.root_key` is
        ``persisted_key``; return how many were deleted.  A segment written by a commit that
        orders above it (a new lineage after a refound, say) is kept."""
        if self._closed:
            return 0
        if not self._release:
            with self._lock:
                for key in [k for k, (order, _) in self._ranges.items() if order <= persisted_key]:
                    del self._ranges[key]
            return 0
        # Phase B (Task 8.3f): checkpoint first, so the acceptor log keeps no record of a
        # segment about to be deleted (a redo would recreate it) and every acceptance logged
        # so far has its data synced.  Only when something may be released: a checkpoint
        # costs a few syncs.
        current = self._local.current_segment
        with self._lock:
            releasable = any(o.uploaded and not o.released and o.uuid != current and o.max_key is not None
                             and o.max_key <= persisted_key for o in self._own.values()) or any(
                s in self._remote and k is not None and k <= persisted_key for s, k in self._copies.items())
        if releasable:
            self.checkpoint()
        # Under the copies lock, which close() takes: nothing is deleted once it has started,
        # and no copy is written between the decision to delete it and its deletion (a write
        # would extend it past what the store was found to cover).
        with self._copies_lock:
            self._refuse_closed("releasing local segments")
            current = self._local.current_segment
            with self._lock:
                candidates = [s for s, k in self._copies.items() if s in self._remote and k is not None
                              and k <= persisted_key]
            sizes = {}                  # every byte of a copy must be in the store before it goes
            for segment in candidates:
                try:
                    sizes[segment] = os.path.getsize(os.path.join(self._path, segment.hex + SEGMENT_SUFFIX))
                except FileNotFoundError:
                    sizes[segment] = 0
                except OSError as e:
                    raise ProtoUnexpectedError(f"Reading the size of local segment {segment.hex} "
                                               f"failed: {e}") from e
            with self._lock:
                for key in [k for k, (order, _) in self._ranges.items() if order <= persisted_key]:
                    del self._ranges[key]
                unshipped = {raw for raw, _, _ in self._unshipped}
                # An own segment with records not yet shipped is kept: they ride with a later
                # replication, so a kind-5 entry after the checkpoint above may name it.
                own = [o for o in self._own.values() if o.uploaded and not o.released and o.uuid != current
                       and o.max_key is not None and o.max_key <= persisted_key and self._covered(o.uuid, o.end)
                       and o.uuid.bytes not in unshipped]
                copies = [s for s in candidates if self._copies.get(s) is not None
                          and self._copies[s] <= persisted_key and self._covered(s, sizes[s])]
                for o in own:
                    o.released = True
                for s in copies:
                    del self._copies[s]
            for segment in [o.uuid for o in own] + copies:
                self._local.forget_segment(segment)
                try:
                    os.remove(os.path.join(self._path, segment.hex + SEGMENT_SUFFIX))
                except FileNotFoundError:
                    pass                # already gone
                except OSError as e:
                    self._fail(e)
                    raise ProtoUnexpectedError(f"Releasing local segment {segment.hex} failed: {e}") from e
        with self._lock:
            self._stats["segments_released"] += len(own)
            self._stats["copies_released"] += len(copies)
        self._compact_ranges()
        return len(own) + len(copies)

    # -- backlog -----------------------------------------------------------------------------

    def backlog(self) -> dict:
        """``bytes`` and ``age`` (seconds) of own data not yet in the object store, the number
        of such ``segments``, the bytes of peer ``copy_bytes`` kept locally, the ``limit`` and
        whether it is reached (``over_limit``)."""
        now = time.monotonic()
        with self._lock:
            pending = [o for o in self._own.values() if not o.uploaded]
            size = sum(o.end for o in pending)
            first = min((o.first for o in pending if o.first is not None), default=None)
            copies = list(self._copies)
        copy_bytes = 0
        for segment in copies:
            try:
                copy_bytes += os.path.getsize(os.path.join(self._path, segment.hex + SEGMENT_SUFFIX))
            except OSError:
                continue            # released meanwhile
        return {"bytes": size, "age": 0.0 if first is None else now - first, "segments": len(pending),
                "copy_bytes": copy_bytes, "limit": self._limit,
                "over_limit": self._limit is not None and size >= self._limit}

    def check_backlog(self) -> dict:
        """Compute the backlog and call ``on_backlog_alert`` when it is at or above the alert
        threshold (at most once per second); return the backlog."""
        backlog = self.backlog()
        hook = self._on_alert
        if hook is not None and self._alert_at is not None and backlog["bytes"] >= self._alert_at:
            now = time.monotonic()
            if now - self._last_alert >= 1.0:
                self._last_alert = now
                try:
                    hook(backlog)
                except Exception as e:
                    raise ProtoUnexpectedError(f"The backlog alert hook failed: {e}") from e
        return backlog

    def check_commit_allowed(self) -> None:
        """Raise ``ProtoBacklogFullError`` when the backlog reached ``backlog_limit_bytes``."""
        if self._limit is None:
            return
        backlog = self.backlog()
        if backlog["over_limit"]:
            raise ProtoBacklogFullError(
                f"The local backlog not yet in the object store is {backlog['bytes']} bytes "
                f"(limit {self._limit}, oldest {backlog['age']:.1f} s); commits resume once it drains")

    # -- SharedStorage root API --------------------------------------------------------------

    def read_root(self) -> Optional[RootRecord]:
        """The persisted root (cluster epoch).  In cluster mode transactions read the committed
        root from the coordinator instead, because the persisted one may lag."""
        persisted = self.read_persisted()
        return None if persisted is None else persisted[1]

    def publish_root(self, ptr: AtomPointer, expected_epoch: Optional[int]) -> RootRecord:
        """Not supported: a tiered storage's root is committed by the cluster and persisted by
        the persister (D46)."""
        raise ProtoNotSupportedError("TieredStorage roots are committed by the cluster (use a ClusterCoordinator)")

    def root_lock(self, timeout: Optional[float] = None):
        """See :meth:`SharedStorage.root_lock`; in-process only (cluster mode uses the lease)."""
        self._check_open()
        return self._locked(self._lock_timeout if timeout is None else check_lock_timeout(timeout, "timeout"))

    @contextlib.contextmanager
    def _locked(self, timeout: float):
        me = threading.get_ident()
        if self._holder == me:
            raise ProtoLockingError("root_lock is not re-entrant; it is already held by this thread")
        if not self._root_lock.acquire(timeout=timeout):
            raise ProtoTimeoutError(f"Could not acquire the root lock within {timeout} seconds")
        self._holder = me
        try:
            yield
        finally:
            self._holder = None
            self._root_lock.release()

    def close(self) -> None:
        """Flush the local tier and release descriptors and caches.  Idempotent.

        Waits for the writes in progress; every later write is refused (module docstring):
        the closed flag is set first, then each lock guarding a descriptor or a kind of write
        is taken before what it guards is released, and every write checks the flag under its
        lock.  Object-store writes are excluded by the shipping lock, held throughout."""
        with self._ship_lock:
            with self._lock:
                if self._closed:
                    return
                self._closed = True
            errors = []

            def attempt(step, *expected):
                def run():
                    try:
                        step()
                    except expected as e:
                        errors.append(e)
                return run

            def close_fd(lock, name):
                def run():
                    with lock:
                        fd = getattr(self, name)
                        setattr(self, name, -1)
                        os.close(fd)
                return run

            def wait_copies():
                with self._copies_lock:
                    pass                # waits for a copy write or a local release in progress

            def clear_caches():
                self._cache.clear()
                self._segments.clear()

            # Every step runs even when an earlier one raised, whatever it raised (an exit stack
            # runs each callback and propagates the exceptions, 8.3d re-review 12, I2); the
            # owner lock is released last (ruling R59): every descriptor is closed and every
            # write refused or finished, so a new instance can repair the directory without
            # racing this one, and a closed instance never keeps the lock.
            with contextlib.ExitStack() as steps:
                steps.callback(attempt(self._owner.release, ProtoDBError))
                steps.callback(clear_caches)
                steps.callback(attempt(self._roots.close, ProtoDBError))
                steps.callback(wait_copies)
                steps.callback(attempt(close_fd(self._ranges_lock, "_ranges_fd"), OSError))
                steps.callback(attempt(close_fd(self._manifest_lock, "_manifest_fd"), OSError))
                steps.callback(attempt(self._local.close, ProtoDBError, OSError))
                attempt(self._local.flush, ProtoDBError)()
            if errors:
                error = errors[0]
                if len(errors) > 1:
                    attach_secondary_errors(error, errors[1:])
                raise ProtoUnexpectedError(f"Closing the tiered storage at {self._path} failed: {error}") from error
