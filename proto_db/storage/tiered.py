"""Tiered storage of a cluster node: a local WAL tier and an object-store tier (Task 8.3d, D44, D46).

In cluster mode the object store is entirely off the commit path.  Each node keeps a
local directory, written with ``fsync`` (the commit-durability tier), and a background
replicator (:mod:`proto_db.cluster.persister`) ships it to the object store.

Layout of the local directory::

    <uuid4-hex>.seg     this node's own segments (written through a FileStorage) and
                        copies of other nodes' segments: records received with their
                        commits are written at their original offsets, so every
                        AtomPointer is valid cluster-wide; a copy may have holes
    segments.log        the node's own segments in creation order (16-byte UUIDs)
    roots.log           the node's acceptor log: lease promises, roots accepted and committed
                        (:class:`~proto_db.cluster.rootlog.RootsLog`; fixed-size entries)
    ranges.log          per accepted commit root, the byte ranges of the records shipped
                        with it (so a restarted node can still upload a lost writer's data)

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
or a download, the shortest known -- and a part up to its ``end``.  A sealed object shorter
than the records a peer holds (a writer that restarted on a segment missing its tail, a store
or a disk that acknowledged bytes it did not keep) therefore never makes a commit look
persisted, never lets a holder skip shipping those records, and never lets a peer release
its longer copy; a sealed object whose length is unknown covers nothing.  A record read from
a store object that turns out short or corrupt is read from the peers, then from a part;
with no full copy anywhere the read raises ``ProtoCorruptionError``, never short data.

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
locks.  No lock is held across object-store or peer requests.
"""
import contextlib
import logging
import os
import random
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
                      ProtoValidationError)
from ..cluster import protocol as _protocol
from ..cluster.rootlog import LoggedRoots, RootsLog, compare_roots, root_key
from .base import SharedStorage
from .cache import BoundedCache
from .cloud import (ObjectNotFoundError, OutcomeUnknownError, PreconditionFailedError,
                    TransientObjectStoreError)
from .file import DURABILITY_MODES, FileStorage
from .format import (FORMAT_VERSION, SEGMENT_HEADER_SIZE, RecordKind, RootRecord, decode_root,
                     decode_segment_header, encode_record, encode_segment_header, read_record)

__all__ = ["TieredStorage"]

logger = logging.getLogger(__name__)

MIB = 1024 * 1024
ROOT_OBJECT = "space.root"
SEGMENT_PREFIX = "segments/"
SEGMENT_SUFFIX = ".seg"
PART_SUFFIX = ".part"
SEALED_SUFFIX = ".sealed"
MANIFEST = "segments.log"
ROOTS_LOG = "roots.log"
_ROOT_CRC = struct.Struct("<I")
_ROOTS_LOG_COMPACT = 1 * MIB
RANGES_LOG = "ranges.log"
_RANGES_HEAD = struct.Struct("<16sQQQI")         # root segment, root offset, label, epoch, count
_RANGE = struct.Struct("<16sQQ")                 # segment, start, end
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
        durability: ``"fsync"`` (default), ``"flush"`` or ``"none"`` for the local tier.
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
        lock_timeout: default seconds :meth:`root_lock` waits.
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
        if not isinstance(ship_interval, (int, float)) or isinstance(ship_interval, bool) or ship_interval < 0:
            raise ProtoValidationError("ship_interval must be a non-negative number of seconds")
        if type(retries) is not int or retries < 0:
            raise ProtoValidationError("retries must be a non-negative int")
        try:
            low, high = backoff
        except (TypeError, ValueError) as e:
            raise ProtoValidationError("backoff must be a (min_seconds, max_seconds) pair") from e
        if backlog_limit_bytes is not None and (type(backlog_limit_bytes) is not int or backlog_limit_bytes <= 0):
            raise ProtoValidationError("backlog_limit_bytes must be a positive int or None")
        if backlog_alert_bytes is None and backlog_limit_bytes is not None:
            backlog_alert_bytes = int(backlog_limit_bytes * 0.8)
        if backlog_alert_bytes is not None and (type(backlog_alert_bytes) is not int or backlog_alert_bytes <= 0):
            raise ProtoValidationError("backlog_alert_bytes must be a positive int or None")
        if on_backlog_alert is not None and not callable(on_backlog_alert):
            raise ProtoValidationError("on_backlog_alert must be callable")
        self._path = os.path.abspath(os.fspath(path))
        self._local = FileStorage(self._path, durability=durability, cache_bytes=min(cache_bytes, 16 * MIB),
                                  lock_timeout=lock_timeout, segment_max_bytes=segment_max_bytes)
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
        self._stats = {"reads_cache": 0, "reads_local": 0, "reads_peer": 0, "reads_remote": 0,
                       "critical_misses": 0, "remote_segment_gets": 0, "segments_uploaded": 0,
                       "root_writes": 0, "root_writes_in_critical": 0, "segments_released": 0,
                       "copies_released": 0, "records_applied": 0, "parts_uploaded": 0}
        self._load_manifest()
        self._manifest_fd = os.open(os.path.join(self._path, MANIFEST),
                                    os.O_WRONLY | os.O_CREAT | os.O_APPEND | _O_BINARY | _O_CLOEXEC, 0o644)
        self._roots = RootsLog(os.path.join(self._path, ROOTS_LOG), durability=durability)
        self._ranges_lock = threading.Lock()
        self._load_ranges()
        self._ranges_fd = self._open_ranges()

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

    # -- startup -----------------------------------------------------------------------------

    def _load_manifest(self) -> None:
        path = os.path.join(self._path, MANIFEST)
        try:
            with open(path, "rb") as f:
                data = f.read()
        except FileNotFoundError:
            data = b""
        except OSError as e:
            raise ProtoUnexpectedError(f"Cannot read {path}: {e}") from e
        for i in range(0, len(data) - len(data) % 16, 16):
            seg = uuid.UUID(bytes=data[i:i + 16])
            own = _Own(seg)
            file = os.path.join(self._path, seg.hex + SEGMENT_SUFFIX)
            try:
                own.end = os.path.getsize(file)
                own.first = time.monotonic()
            except FileNotFoundError:
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

    def _open_ranges(self) -> int:
        return os.open(os.path.join(self._path, RANGES_LOG),
                       os.O_WRONLY | os.O_CREAT | os.O_APPEND | _O_BINARY | _O_CLOEXEC, 0o644)

    def _load_ranges(self) -> None:
        try:
            with open(os.path.join(self._path, RANGES_LOG), "rb") as f:
                data = f.read()
        except FileNotFoundError:
            return
        except OSError as e:
            raise ProtoUnexpectedError(f"Cannot read {RANGES_LOG}: {e}") from e
        pos = 0
        while pos + _RANGES_HEAD.size <= len(data):
            root_seg, root_off, label, epoch, count = _RANGES_HEAD.unpack_from(data, pos)
            end = pos + _RANGES_HEAD.size + count * _RANGE.size
            if end + _ROOT_CRC.size > len(data):
                break               # a torn tail
            (crc,) = _ROOT_CRC.unpack_from(data, end)
            if zlib.crc32(data[pos:end]) != crc:
                break
            ranges = [(uuid.UUID(bytes=seg), start, stop) for seg, start, stop in
                      _RANGE.iter_unpack(data[pos + _RANGES_HEAD.size:end])]
            root = RootRecord(FORMAT_VERSION, uuid.UUID(bytes=root_seg), root_off, epoch)
            self._ranges[(root.segment, root_off)] = (root_key(label, root), ranges)
            pos = end + _ROOT_CRC.size

    @staticmethod
    def _encode_ranges(key: tuple, order: tuple, ranges: list) -> bytes:
        body = _RANGES_HEAD.pack(key[0].bytes, key[1], order[0], order[1], len(ranges)) + b"".join(
            _RANGE.pack(seg.bytes, start, end) for seg, start, end in ranges)
        return body + _ROOT_CRC.pack(zlib.crc32(body))

    def _log_ranges(self, entry: Optional[bytes], *, sync: bool) -> None:
        if entry is None:
            return
        with self._ranges_lock:
            try:
                os.write(self._ranges_fd, entry)
                if sync and self._durability == "fsync":
                    os.fsync(self._ranges_fd)
            except OSError as e:
                raise ProtoUnexpectedError(f"Writing {RANGES_LOG} failed: {e}") from e

    def _compact_ranges(self) -> None:
        """Rewrite ``ranges.log`` with the live entries when it grew past the compaction size."""
        with self._ranges_lock:
            try:
                if os.fstat(self._ranges_fd).st_size <= _ROOTS_LOG_COMPACT:
                    return
                with self._lock:
                    live = b"".join(self._encode_ranges(k, e, r) for k, (e, r) in self._ranges.items())
                tmp = os.path.join(self._path, RANGES_LOG + ".tmp")
                with open(tmp, "wb") as f:
                    f.write(live)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, os.path.join(self._path, RANGES_LOG))
                os.close(self._ranges_fd)
                self._ranges_fd = self._open_ranges()
            except OSError as e:
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
                with self._lock:
                    write = ptr.segment not in self._own
                    if write:
                        self._own[ptr.segment] = _Own(ptr.segment)
                if write:
                    try:
                        os.write(self._manifest_fd, ptr.segment.bytes)
                    except OSError as e:
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
        with self._lock:
            dirty, self._manifest_dirty = self._manifest_dirty, False
        if dirty and self._durability == "fsync":
            os.fsync(self._manifest_fd)

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

    def flush_local(self) -> None:
        """Make this node's records durable (the commit-durability tier)."""
        self.flush()

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
        offsets (durable per the durability mode, like :meth:`apply`; own records are skipped)
        and remember the commit's byte ``ranges`` (logged with ``fsync``): this node then
        holds every record of that commit the object store lacks, can serve them to its peers
        and can upload them as parts."""
        self._check_open()
        order = root_key(label, root)
        self._write_copies(records, order)
        with self._lock:
            key = (root.segment, root.offset)
            self._ranges[key] = (order, list(ranges))
            entry = self._encode_ranges(key, order, ranges)
        self._log_ranges(entry, sync=True)

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

    def _write_copies(self, records: list, order: Optional[tuple]) -> None:
        """Write other nodes' ``records`` into the local copies of their segments at their
        original offsets (``fsync`` per the durability mode) and cache them; ``order`` is the
        root_key of the commit that wrote them (local release, see :meth:`release_local`)."""
        groups = {}
        for raw, offset, data in records:
            if data is not None:
                groups.setdefault(uuid.UUID(bytes=raw), []).append((offset, data))
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
                        os.pwrite(fd, encode_segment_header(segment), 0)
                        created = created or not known
                    for offset, data in items:
                        os.pwrite(fd, data, offset)
                    if self._durability == "fsync":
                        os.fsync(fd)
                finally:
                    os.close(fd)
            except OSError as e:
                raise ProtoUnexpectedError(f"Storing replicated records of segment {segment.hex} failed: {e}") from e
            for offset, data in items:
                self._cache.put((segment, offset), data)
            self._count("records_applied", len(items))
        if created and self._durability == "fsync":
            self._fsync_dir()

    def prefetch(self, records: list) -> None:
        """Warm the caches with replicated records (already cached by :meth:`apply`)."""

    def _fsync_dir(self) -> None:
        try:
            fd = os.open(self._path, os.O_RDONLY | _O_CLOEXEC)
        except OSError as e:
            raise ProtoUnexpectedError(f"Cannot open {self._path}: {e}") from e
        try:
            os.fsync(fd)
        except OSError as e:
            raise ProtoUnexpectedError(f"Cannot fsync {self._path}: {e}") from e
        finally:
            os.close(fd)

    def log_root(self, kind: str, label: int, record: Optional[RootRecord], *, sync: bool) -> None:
        """Append a root this node accepted or learnt committed to ``roots.log`` (``fsync`` with
        ``sync``; see :class:`~proto_db.cluster.rootlog.RootsLog`)."""
        self._roots.append(kind, label, record, sync=sync)

    def log_promise(self, term: int) -> None:
        """Make this node's promise of lease ``term`` durable in ``roots.log`` before it is sent."""
        self._check_open()
        self._roots.promise(term)

    def log_rejoined(self) -> None:
        """Append the rejoined marker to ``roots.log`` (the node's rejoin completed)."""
        self._check_open()
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
        self._check_open()
        data = _encode_root_object(record, label)
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
        self._check_open()
        with self._ship_lock:
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
        (``end`` ``None``: a sealed object of a known length, see :meth:`_covered`).

        Lists the store once when something is not known to be there.  With
        ``upload_copies``, a missing segment this node holds a copy of (not an own segment,
        which its writer ships in order) is uploaded as a part when the copy's contiguous
        prefix reaches ``end``.  Called by the persister only.
        """
        needed = {}
        for segment, end in needs:
            if segment in needed and (needed[segment] is None or end is None):
                needed[segment] = None
            else:
                needed[segment] = max(needed.get(segment, 0), end) if end is not None else None
        with self._lock:
            missing = {s: e for s, e in needed.items() if not self._covered(s, e)}
        if not missing:
            return True
        self._list_remote()
        with self._lock:
            missing = {s: e for s, e in missing.items() if not self._covered(s, e)}
            copies = {s: e for s, e in missing.items() if e is not None and s not in self._own}
        if not missing:
            return True
        if not upload_copies or len(copies) < len(missing):
            return False
        for segment, end in copies.items():
            data = self._copy_prefix(segment, end)
            if data is None:
                return False        # a hole: another node's copy must serve
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
            own = [o for o in self._own.values() if o.uploaded and not o.released and o.uuid != current
                   and o.max_key is not None and o.max_key <= persisted_key and self._covered(o.uuid, o.end)]
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
        return self._locked(self._lock_timeout if timeout is None else timeout)

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
        """Flush the local tier and release descriptors and caches.  Idempotent."""
        with self._ship_lock:
            with self._lock:
                if self._closed:
                    return
                self._closed = True
            error = None
            try:
                self._local.flush()
            except ProtoDBError as e:
                error = e
            self._local.close()
            with self._ranges_lock:
                for fd in (self._manifest_fd, self._ranges_fd):
                    try:
                        os.close(fd)
                    except OSError as e:
                        error = error or e
            try:
                self._roots.close()
            except ProtoDBError as e:
                error = error or e
            self._cache.clear()
            self._segments.clear()
            if error is not None:
                raise ProtoUnexpectedError(f"Closing the tiered storage at {self._path} failed: {error}") from error
