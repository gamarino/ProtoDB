"""Cloud object-store storage of ProtoDB 2.0 (spec section 9, decisions D5 and D43).

Layout under a key ``prefix`` of one bucket::

    <prefix>space.root                 the published root, replaced by compare-and-swap
    <prefix>segments/<uuid4-hex>.seg   immutable segments (format v2 header, then records)

Write path: ``push_atom``/``push_blob`` frame the record (format v2) and append it to a
local segment buffer, returning ``AtomPointer(segment UUID, byte offset)`` at once;
buffered records are read from memory.  A segment is *sealed* when it would exceed
``segment_max_bytes`` or when ``flush`` runs, and every sealed segment is uploaded once,
whole, with ``put_if_absent`` under its UUID key; a sealed segment is never appended to,
so every segment object is immutable.  Uploads are idempotent: a retry that finds the
object already there compares its bytes with ours (equal: our earlier attempt landed).
A failed upload keeps the segment buffered (still readable) and the next ``flush``
retries it.  When sealed segments waiting for upload exceed ``write_buffer_bytes`` a push
uploads them, bounding memory; durability still comes from ``flush``.

Root: one object holding a format v2 root record (``crc32 | msgpack``; see
:func:`proto_db.storage.format.encode_root`) with one extra msgpack field, ``recent``,
the ``(segment, offset, epoch)`` of up to ``root_history`` previously published roots.
``publish_root`` flushes first and checks that the pointer names a stored record, so a
root is written only after every segment it can reference is uploaded; then it reads
the root object with its etag and writes the new root with ``put_if_match`` on that etag
(``put_if_absent`` for the first root).  A failed precondition raises
``ProtoConflictError``.

Unknown outcomes: a write that timed out may or may not have been applied.  After a
timeout or a failed precondition on the root, ``publish_root`` reads the root back:
our root is the current one or in ``recent`` -> success (the write landed; it may even
have been superseded already); the etag is unchanged -> not applied, retried; the
epoch we tried to take belongs to another root -> ``ProtoConflictError``; the history no
longer reaches that epoch, or the read-back fails -> ``ProtoOutcomeUnknownError`` (never
a conflict, so ``Database.run`` does not retry it).  ``publish_outcome(ptr, epoch)``
gives the same verdict later from the root and its history; transactions.py consults it
(step 6 of D34).  ``root_lock_is_global`` is False: an epoch mismatch alone proves
nothing, because other processes publish without taking this instance's lock.  A request still in flight can only land while the etag it was
conditioned on is current, so a conclusion drawn from a changed etag is final.

Locking: an object store has no cross-process lock.  ``root_lock`` is an in-process
lock (exclusion among the threads of this instance only).  Without a cluster
coordinator the compare-and-swap of ``publish_root`` is the arbiter across instances and
processes; with a coordinator (D44) it is the fencing safety net.

Retries: ``TransientObjectStoreError`` (not applied) and ``OutcomeUnknownError`` are
retried up to ``retries`` times with exponential backoff and jitter; reads and segment
uploads are idempotent, root writes follow the resolution above.  The SDK adapters turn
their SDK's own retries off so that no hidden retry re-sends a conditional write.

Thread-safety: every public method may be called from any thread.  One lock guards the
local buffers, one serializes uploads, the read cache (:class:`BoundedCache` of whole
segment objects, which are immutable) has its own lock.  Root writes are made under the
upload lock too, which ``close()`` takes: nothing is written to the store once ``close()``
has started.

SDKs are optional and imported lazily by the adapters (:class:`S3ObjectStore`, extra
``s3``; :class:`GCSObjectStore`, extra ``gcs``); a missing SDK raises
``ProtoNotSupportedError`` with an install hint.
"""
import contextlib
import random
import struct
import threading
import time
import uuid
import zlib
from typing import Optional, Protocol

import msgpack

from ..core.atom import AtomPointer
from ..errors import (ProtoClosedError, ProtoConflictError, ProtoCorruptionError, ProtoLockingError,
                      ProtoNotAuthorizedError, ProtoNotSupportedError, ProtoOutcomeUnknownError,
                      ProtoTimeoutError,
                      ProtoUnexpectedError, ProtoValidationError)
from .base import SharedStorage, check_lock_timeout
from .cache import BoundedCache
from .format import (WRITE_FORMAT_VERSION, RECORD_HEADER_SIZE, SEGMENT_HEADER_SIZE, RecordKind,
                     RootRecord, decode_root, decode_segment_header, encode_record,
                     encode_segment_header, read_record, record_size,
                     undecodable_atom)

__all__ = ["ObjectStore", "ObjectStoreError", "ObjectNotFoundError", "PreconditionFailedError",
           "TransientObjectStoreError", "OutcomeUnknownError", "CloudStorage", "S3ObjectStore",
           "GCSObjectStore", "ROOT_OBJECT", "SEGMENT_PREFIX", "SEGMENT_SUFFIX"]

MIB = 1024 * 1024
ROOT_OBJECT = "space.root"
SEGMENT_PREFIX = "segments/"
SEGMENT_SUFFIX = ".seg"
_ROOT_CRC = struct.Struct("<I")
#: Attempts of an unconditional publish (``expected_epoch=None``) losing races to other writers.
_UNCONDITIONAL_ATTEMPTS = 100


# --- the object-store protocol ----------------------------------------------------------------

class ObjectStoreError(ProtoUnexpectedError):
    """An object-store operation failed for a reason not covered by a subclass."""


class ObjectNotFoundError(ObjectStoreError):
    """The object does not exist."""


class PreconditionFailedError(ObjectStoreError):
    """A conditional write was rejected: nothing was written."""


class TransientObjectStoreError(ObjectStoreError):
    """A temporary failure (throttling, unavailable, no connection); the request was NOT applied."""


class OutcomeUnknownError(ObjectStoreError):
    """The request may or may not have been applied (timeout or connection lost after sending)."""


class ObjectStore(Protocol):
    """Minimal object-store interface used by :class:`CloudStorage`.

    Implementations are thread-safe and strongly consistent per key (S3 and GCS are).
    Etags are opaque strings that change on every write of a key.  Every method raises
    only :class:`ObjectStoreError` subclasses or ``ProtoNotAuthorizedError`` for store
    failures.
    """

    def put_if_absent(self, key: str, data: bytes) -> str:
        """Create ``key`` with ``data``; return its etag.  ``PreconditionFailedError`` if it exists."""

    def put_if_match(self, key: str, data: bytes, etag: str) -> str:
        """Replace ``key`` if its current etag is ``etag``; return the new etag.

        ``PreconditionFailedError`` if the etag differs or the key does not exist.
        """

    def get(self, key: str) -> tuple:
        """Return ``(data, etag)`` of ``key``; ``ObjectNotFoundError`` if it does not exist."""

    def list(self, prefix: str) -> list:
        """Return the keys starting with ``prefix``."""

    def delete(self, key: str) -> None:
        """Delete ``key``; deleting a missing key is not an error."""


_PROTOCOL_METHODS = ("put_if_absent", "put_if_match", "get", "list", "delete")


# --- root object ------------------------------------------------------------------------------

def _encode_root_object(root: RootRecord, recent: list) -> bytes:
    """A format v2 root record carrying the extra ``recent`` field (ignored by ``decode_root``)."""
    body = msgpack.packb({"version": root.version, "segment": root.segment.bytes,
                          "offset": root.offset, "epoch": root.epoch,
                          "recent": [[s.bytes, o, e] for s, o, e in recent]}, use_bin_type=True)
    return _ROOT_CRC.pack(zlib.crc32(body)) + body


def _decode_root_object(data: bytes) -> tuple:
    """Return ``(RootRecord, recent)``; ``recent`` is a list of ``(UUID, offset, epoch)``."""
    root = decode_root(data)
    fields = msgpack.unpackb(bytes(data[_ROOT_CRC.size:]), raw=False)
    recent = []
    raw_recent = fields.get("recent", [])
    if not isinstance(raw_recent, list):
        raise ProtoCorruptionError("Malformed root history")
    for entry in raw_recent:
        if (not isinstance(entry, list) or len(entry) != 3 or not isinstance(entry[0], bytes)
                or len(entry[0]) != 16 or type(entry[1]) is not int or type(entry[2]) is not int):
            raise ProtoCorruptionError("Malformed root history")
        recent.append((uuid.UUID(bytes=entry[0]), entry[1], entry[2]))
    return root, recent


# --- storage ----------------------------------------------------------------------------------

class _LocalSegment:
    """A segment not yet uploaded: open (receiving appends) or sealed (waiting for upload)."""

    __slots__ = ("uuid", "data", "sealed")

    def __init__(self, segment_uuid: uuid.UUID):
        self.uuid = segment_uuid
        self.data = bytearray(encode_segment_header(segment_uuid))
        self.sealed = False


class CloudStorage(SharedStorage):
    """:class:`SharedStorage` over an :class:`ObjectStore` (see the module docstring).

    Args:
        store: the object store (an adapter such as :class:`S3ObjectStore`).
        prefix: key prefix of this space inside the bucket (e.g. ``"spaces/orders/"``).
        segment_max_bytes: a segment is sealed before exceeding this size (a single larger
            record gets a segment of its own).
        write_buffer_bytes: sealed-but-not-uploaded bytes beyond which a push uploads them.
        cache_bytes: capacity of the cache of downloaded segment objects (``0`` disables it).
        lock_timeout: default seconds ``root_lock`` waits; finite and positive
            (:func:`~proto_db.storage.base.check_lock_timeout`).
        retries: retries of an operation failing with ``TransientObjectStoreError`` or
            ``OutcomeUnknownError`` (``0``: no retry).
        backoff: ``(min_seconds, max_seconds)`` of the exponential backoff between retries.
        root_history: previous roots recorded in the root object, used to resolve an
            unknown publish outcome after other writers have published on top of it.
        verify_root: on construction, read the root and check that its record exists
            (``ProtoCorruptionError`` otherwise).
    """

    #: The root lock is in-process only: other instances and processes are arbitrated by CAS.
    root_lock_is_global = False

    def __init__(self, store: ObjectStore, prefix: str = "", *, segment_max_bytes: int = 8 * MIB,
                 write_buffer_bytes: int = 32 * MIB, cache_bytes: int = 64 * MIB,
                 lock_timeout: float = 30.0, retries: int = 5, backoff: tuple = (0.05, 2.0),
                 root_history: int = 64, verify_root: bool = True):
        if not all(callable(getattr(store, m, None)) for m in _PROTOCOL_METHODS):
            raise ProtoValidationError(f"store must provide the ObjectStore methods {_PROTOCOL_METHODS}")
        if not isinstance(prefix, str):
            raise ProtoValidationError("prefix must be a str")
        if type(segment_max_bytes) is not int or segment_max_bytes <= SEGMENT_HEADER_SIZE + RECORD_HEADER_SIZE:
            raise ProtoValidationError("segment_max_bytes is too small")
        if type(write_buffer_bytes) is not int or write_buffer_bytes <= 0:
            raise ProtoValidationError("write_buffer_bytes must be a positive int")
        if type(cache_bytes) is not int or cache_bytes < 0:
            raise ProtoValidationError("cache_bytes must be a non-negative int")
        check_lock_timeout(lock_timeout)
        if type(retries) is not int or retries < 0:
            raise ProtoValidationError("retries must be a non-negative int")
        try:
            low, high = backoff
        except (TypeError, ValueError) as e:
            raise ProtoValidationError("backoff must be a (min_seconds, max_seconds) pair") from e
        if not all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in (low, high)) \
                or low < 0 or high < low:
            raise ProtoValidationError("backoff must satisfy 0 <= min_seconds <= max_seconds")
        if type(root_history) is not int or root_history < 1:
            raise ProtoValidationError("root_history must be a positive int")

        self._store = store
        self._prefix = prefix
        self._root_key = prefix + ROOT_OBJECT
        self._segment_max = segment_max_bytes
        self._write_buffer_bytes = write_buffer_bytes
        self._cache = BoundedCache(cache_bytes)
        self._lock_timeout = lock_timeout
        self._retries = retries
        self._backoff = (low, high)
        self._root_history = root_history

        self._state_lock = threading.Lock()
        self._upload_lock = threading.Lock()
        self._root_lock = threading.Lock()
        self._holder = None
        self._current = None      # open _LocalSegment receiving appends
        self._local = {}          # UUID -> _LocalSegment not yet uploaded (open or sealed)
        self._sealed = []         # sealed segments in upload order
        self._sealed_bytes = 0
        self._uploaded = {}       # UUID -> size of segments this instance uploaded
        self._closed = False

        if verify_root:
            root = self.read_root()
            if root is not None:
                self._fetch_record(root.pointer)

    @property
    def store(self) -> ObjectStore:
        """The underlying object store."""
        return self._store

    @property
    def root_key(self) -> str:
        """Key of the root object."""
        return self._root_key

    def segment_key(self, segment: uuid.UUID) -> str:
        """Key of the segment object ``segment``."""
        return f"{self._prefix}{SEGMENT_PREFIX}{segment.hex}{SEGMENT_SUFFIX}"

    def _check_open(self):
        if self._closed:
            raise ProtoClosedError("Storage is closed")

    # --- retries ---------------------------------------------------------------------------

    def _sleep(self, attempt: int) -> None:
        low, high = self._backoff
        delay = min(high, low * (2 ** attempt))
        if delay > 0:
            time.sleep(delay * (0.5 + random.random() / 2))

    def _retrying(self, operation, what: str):
        """Run ``operation()``, retrying transient and unknown-outcome failures (idempotent operations only)."""
        for attempt in range(self._retries + 1):
            try:
                return operation()
            except (TransientObjectStoreError, OutcomeUnknownError) as e:
                if attempt == self._retries:
                    raise ProtoUnexpectedError(
                        f"{what} failed after {attempt + 1} attempt(s): {e}") from e
            self._sleep(attempt)
        raise ProtoUnexpectedError("unreachable: the retry loop always returns or raises")  # pragma: no cover

    # --- write path ------------------------------------------------------------------------

    def _seal_current(self) -> None:
        """Seal the open segment, if it holds records (state lock held)."""
        seg = self._current
        if seg is None or len(seg.data) <= SEGMENT_HEADER_SIZE:
            return
        seg.data = bytes(seg.data)
        seg.sealed = True
        self._sealed.append(seg)
        self._sealed_bytes += len(seg.data)
        self._current = None

    def _append(self, kind: RecordKind, payload: bytes) -> AtomPointer:
        record = encode_record(kind, payload)
        with self._state_lock:
            self._check_open()
            seg = self._current
            if seg is not None and len(seg.data) + len(record) > self._segment_max:
                self._seal_current()
                seg = None
            if seg is None:
                seg = _LocalSegment(uuid.uuid4())
                self._current = seg
                self._local[seg.uuid] = seg
            offset = len(seg.data)
            seg.data += record
            spill = self._sealed_bytes >= self._write_buffer_bytes
        if spill:
            self._upload(seal_current=False)
        return AtomPointer(seg.uuid, offset)

    def push_atom(self, payload: dict) -> AtomPointer:
        """See :meth:`SharedStorage.push_atom`.

        When sealed segments exceed ``write_buffer_bytes`` this push uploads them and may
        raise ``ProtoUnexpectedError`` for that upload; the record was buffered but its
        pointer is lost to the caller, so its bytes are unreferenced garbage.  The pending
        segments stay buffered and the next flush retries them.
        """
        if not isinstance(payload, dict):
            raise ProtoValidationError("Atom payload must be a dict")
        self._check_open()
        return self._append(RecordKind.ATOM, msgpack.packb(payload, use_bin_type=True))

    def push_blob(self, data: bytes) -> AtomPointer:
        """See :meth:`SharedStorage.push_blob`; may raise for a spill upload as :meth:`push_atom`."""
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise ProtoValidationError("Blob data must be bytes")
        self._check_open()
        return self._append(RecordKind.BLOB, bytes(data))

    def _put_segment(self, seg: _LocalSegment) -> None:
        """Upload a sealed segment once; a retry meeting our own earlier upload succeeds."""
        key = self.segment_key(seg.uuid)
        data = seg.data

        def attempt():
            try:
                self._store.put_if_absent(key, data)
                return
            except PreconditionFailedError as e:
                conflict = e
            try:
                existing, _ = self._store.get(key)
            except ObjectNotFoundError as e:
                raise TransientObjectStoreError(
                    f"Segment object {key} was reported as existing but is absent") from e
            if existing != data:
                raise ProtoUnexpectedError(
                    f"Segment object {key} already exists with different content") from conflict

        self._retrying(attempt, f"Uploading segment {key}")

    def _upload(self, seal_current: bool) -> None:
        """Upload every sealed segment (sealing the open one first when ``seal_current``)."""
        with self._upload_lock:
            with self._state_lock:
                self._check_open()
                if seal_current:
                    self._seal_current()
                batch = list(self._sealed)
            for seg in batch:
                self._put_segment(seg)
                with self._state_lock:
                    # Cache before dropping the local copy, so readers never miss the segment.
                    self._cache.put(seg.uuid, seg.data)
                    self._sealed.remove(seg)
                    self._sealed_bytes -= len(seg.data)
                    self._local.pop(seg.uuid, None)
                    self._uploaded[seg.uuid] = len(seg.data)

    def flush(self) -> None:
        """Seal the open segment and upload every pending segment.

        On failure the segments stay buffered and readable, and the next ``flush`` retries
        them; the error is ``ProtoUnexpectedError`` (or ``ProtoNotAuthorizedError``).
        """
        self._check_open()
        self._upload(seal_current=True)

    # --- read path -------------------------------------------------------------------------

    def _segment_bytes(self, segment: uuid.UUID) -> bytes:
        """Return the bytes of an uploaded segment object, through the cache."""
        data = self._cache.get(segment)
        if data is not None:
            return data
        key = self.segment_key(segment)
        try:
            data, _ = self._retrying(lambda: self._store.get(key), f"Reading segment {key}")
        except ObjectNotFoundError as e:
            raise ProtoCorruptionError(f"Segment {segment.hex} is missing") from e
        if decode_segment_header(data) != segment:
            raise ProtoCorruptionError(f"Segment object {key} has a foreign header")
        self._cache.put(segment, data)
        return data

    def _fetch_record(self, ptr: AtomPointer) -> tuple:
        """Return ``(RecordKind, payload)`` of the verified record at ``ptr``."""
        if not isinstance(ptr, AtomPointer):
            raise ProtoValidationError("Expected an AtomPointer")
        if ptr.offset < SEGMENT_HEADER_SIZE:
            raise ProtoCorruptionError(f"No record at {ptr}")
        with self._state_lock:
            self._check_open()
            seg = self._local.get(ptr.segment)
            raw = None
            if seg is not None:
                data = seg.data
                if ptr.offset + RECORD_HEADER_SIZE > len(data):
                    raise ProtoCorruptionError(f"No record at {ptr}")
                end = ptr.offset + record_size(data[ptr.offset:ptr.offset + RECORD_HEADER_SIZE], ptr)
                raw = bytes(data[ptr.offset:end])
        if raw is not None:
            return read_record(raw, 0)
        return read_record(self._segment_bytes(ptr.segment), ptr.offset)

    def _fetch(self, ptr: AtomPointer, kind: RecordKind) -> bytes:
        found, payload = self._fetch_record(ptr)
        if found != kind:
            raise ProtoCorruptionError(f"Record at {ptr} is a {found.name}, not a {kind.name}")
        return payload

    def get_atom(self, ptr: AtomPointer) -> dict:
        """See :meth:`SharedStorage.get_atom`."""
        payload = self._fetch(ptr, RecordKind.ATOM)
        try:
            return msgpack.unpackb(payload, raw=False)
        except (msgpack.exceptions.UnpackException, ValueError) as e:
            raise undecodable_atom(ptr, e) from e

    def get_blob(self, ptr: AtomPointer) -> bytes:
        """See :meth:`SharedStorage.get_blob`."""
        return self._fetch(ptr, RecordKind.BLOB)

    # --- root ------------------------------------------------------------------------------

    def _read_root_object(self) -> tuple:
        """Return ``(RootRecord or None, etag or None, recent)`` read from the store."""
        try:
            data, etag = self._retrying(lambda: self._store.get(self._root_key),
                                        f"Reading the root object {self._root_key}")
        except ObjectNotFoundError:
            return None, None, []
        root, recent = _decode_root_object(data)
        return root, etag, recent

    def read_root(self) -> Optional[RootRecord]:
        """Return the root currently published in the store (by any writer), or ``None``."""
        self._check_open()
        return self._read_root_object()[0]

    @staticmethod
    def _verdict(current: Optional[RootRecord], recent: list, segment: uuid.UUID, offset: int,
                 epoch: int) -> Optional[bool]:
        """True: ``(segment, offset)`` holds ``epoch`` in the root or its history; False: another
        root holds it; None: the history does not reach ``epoch`` (or it is not taken yet)."""
        entries = recent + ([(current.segment, current.offset, current.epoch)] if current else [])
        if (segment, offset, epoch) in entries:
            return True
        if any(e == epoch for _, _, e in entries):
            return False
        return None

    def publish_outcome(self, ptr: AtomPointer, epoch: int) -> Optional[bool]:
        """See :meth:`SharedStorage.publish_outcome`; decided from the root and its ``recent`` history.

        ``None`` when the history no longer reaches ``epoch``, or when the root has not
        reached ``epoch`` yet (a write still in flight could land).
        """
        if not isinstance(ptr, AtomPointer) or type(epoch) is not int:
            raise ProtoValidationError("publish_outcome expects an AtomPointer and an int epoch")
        self._check_open()
        current, _, recent = self._read_root_object()
        return self._verdict(current, recent, ptr.segment, ptr.offset, epoch)

    def _resolve(self, mine: RootRecord, etag_before: Optional[str], error,
                 refused: bool = False) -> str:
        """After an uncertain root write: ``"applied"``, ``"unchanged"`` or ``"taken"``.

        ``refused`` says that this attempt's write was refused by its precondition and no
        earlier attempt of this call had an unknown outcome: nothing of ours can have been
        applied (the adapters turn the SDK's own retries off, D43), so when the history no
        longer reaches our epoch the answer is ``"taken"``, a conflict.  The read-back still
        runs first, so a write that did land (a client passed in with hidden retries left on)
        is found and reported as applied, never as a conflict ``Database.run`` would apply again.

        Raises ``ProtoOutcomeUnknownError`` if the root cannot be read, or if its history no
        longer reaches the epoch we tried to take and the write was not ``refused``.
        """
        try:
            current, etag, recent = self._read_root_object()
        except ProtoUnexpectedError as e:
            raise ProtoOutcomeUnknownError(
                f"The outcome of publishing the root is unknown ({error}) and reading it back "
                f"failed ({e}); read the root to resolve it") from error
        if etag == etag_before:
            return "unchanged"
        verdict = self._verdict(current, recent, mine.segment, mine.offset, mine.epoch)
        if verdict is True:
            return "applied"
        if verdict is False or refused:
            return "taken"
        raise ProtoOutcomeUnknownError(
            f"The outcome of publishing the root is unknown ({error}): the root changed and its "
            f"history no longer reaches epoch {mine.epoch}") from error

    def _check_publishable(self, ptr: AtomPointer) -> None:
        """Check that ``ptr`` names a record, without downloading a segment this instance wrote."""
        with self._state_lock:
            self._check_open()
            local = ptr.segment in self._local
            uploaded_end = self._uploaded.get(ptr.segment)
        if local:
            self._fetch_record(ptr)  # served from the local buffer
            return
        if uploaded_end is not None:
            if not SEGMENT_HEADER_SIZE <= ptr.offset < uploaded_end:
                raise ProtoCorruptionError(f"No record at {ptr}")
            data = self._cache.get(ptr.segment)
            if data is not None:
                read_record(data, ptr.offset)
            return
        self._fetch_record(ptr)  # a segment written by another instance

    def publish_root(self, ptr: AtomPointer, expected_epoch: Optional[int]) -> RootRecord:
        """See :meth:`SharedStorage.publish_root` and the module docstring.

        Checks that ``ptr`` names a record, uploads every pending segment, then writes the
        root by compare-and-swap on the root object's etag.  Raises ``ProtoConflictError``
        when another root won, ``ProtoOutcomeUnknownError`` when the write may have been
        applied and the read-back could not decide.
        """
        if not isinstance(ptr, AtomPointer):
            raise ProtoValidationError("Expected an AtomPointer")
        if expected_epoch is not None and (type(expected_epoch) is not int or expected_epoch < 0):
            raise ProtoValidationError("expected_epoch must be a non-negative int or None")
        self._check_publishable(ptr)
        self.flush()
        failures = 0
        races = 0
        uncertain = False   # an earlier attempt may still land: exhaustion is then an unknown outcome
        while True:
            # Each attempt reads and writes the root under the upload lock, which close() takes,
            # after the closed check made under it: no root is written once close() has started
            # (8.3d re-review 8, A1).  The lock is released for the back-off sleeps, so close()
            # and spilling pushes never wait for them (re-review 9, B2).
            with self._upload_lock:
                with self._state_lock:
                    if self._closed and uncertain:
                        # An earlier attempt of ours may still land: never report "not applied".
                        raise ProtoOutcomeUnknownError(
                            "The storage was closed while publishing the root, after an attempt whose "
                            "outcome is unknown; read the root to resolve it")
                    self._check_open()
                    if ptr.segment in self._local:
                        raise ProtoUnexpectedError(f"Segment {ptr.segment.hex} is not uploaded; refusing to publish")
                current, etag, recent = self._read_root_object()
                epoch = current.epoch if current is not None else 0
                if expected_epoch is not None and expected_epoch != epoch:
                    if uncertain:
                        # An earlier attempt of ours may be what moved the root.
                        verdict = self._verdict(current, recent, ptr.segment, ptr.offset, expected_epoch + 1)
                        if verdict is True:
                            return RootRecord(WRITE_FORMAT_VERSION, ptr.segment, ptr.offset, expected_epoch + 1)
                        if verdict is None:
                            raise ProtoOutcomeUnknownError(
                                f"The root moved to epoch {epoch} after an attempt with an unknown outcome "
                                f"and its history no longer reaches epoch {expected_epoch + 1}")
                    raise ProtoConflictError(f"Root epoch is {epoch}, expected {expected_epoch}")
                mine = RootRecord(WRITE_FORMAT_VERSION, ptr.segment, ptr.offset, epoch + 1)
                history = ([(current.segment, current.offset, current.epoch)] + recent
                           if current is not None else [])
                data = _encode_root_object(mine, history[:self._root_history])
                try:
                    if etag is None:
                        self._store.put_if_absent(self._root_key, data)
                    else:
                        self._store.put_if_match(self._root_key, data, etag)
                    return mine
                except TransientObjectStoreError as e:
                    error, outcome = e, "unchanged"
                except (PreconditionFailedError, OutcomeUnknownError) as e:
                    refused = isinstance(e, PreconditionFailedError) and not uncertain
                    uncertain = uncertain or isinstance(e, OutcomeUnknownError)
                    error, outcome = e, self._resolve(mine, etag, e, refused)
                if outcome == "applied":
                    return mine
            # The upload lock is released: decide, and back off without holding it.
            if isinstance(error, PreconditionFailedError) or outcome == "taken":
                if expected_epoch is not None:
                    raise ProtoConflictError(
                        f"Another root was published concurrently (epoch {epoch + 1} is taken)") from error
                races += 1
                if races >= _UNCONDITIONAL_ATTEMPTS:
                    raise ProtoConflictError(
                        f"Unconditional publish lost {races} races to other writers") from error
                self._sleep(min(races, 10))
                continue
            # Not applied and the root is unchanged: safe to send again (same etag condition).
            if failures == self._retries:
                if uncertain:
                    raise ProtoOutcomeUnknownError(
                        f"Publishing the root failed after {failures + 1} attempt(s) and an earlier "
                        f"attempt may still be applied; read the root to resolve it: {error}") from error
                raise ProtoUnexpectedError(
                    f"Publishing the root failed after {failures + 1} attempt(s) (not applied): {error}") from error
            self._sleep(failures)
            failures += 1

    # --- locking ---------------------------------------------------------------------------

    def root_lock(self, timeout: Optional[float] = None):
        """See :meth:`SharedStorage.root_lock`.

        In-process only: it excludes the threads of this instance.  Other instances and
        processes are arbitrated by the compare-and-swap of :meth:`publish_root`.
        """
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
        """Release buffers and the cache.  Records not yet uploaded are discarded.

        Idempotent; waits for an upload in progress to finish.
        """
        with self._upload_lock:
            with self._state_lock:
                self._closed = True
                self._current = None
                self._local = {}
                self._sealed = []
                self._sealed_bytes = 0
                self._uploaded = {}
            self._cache.clear()


# --- SDK adapters -----------------------------------------------------------------------------

class S3ObjectStore:
    """:class:`ObjectStore` over Amazon S3 (or a compatible service) with boto3.

    Conditional writes use ``If-None-Match: *`` and ``If-Match: <etag>`` on ``PutObject``
    (supported by S3 since 2024; S3-compatible services must support them too).  The
    client is created with SDK retries disabled (``max_attempts=1``) because
    :class:`CloudStorage` decides about retries; a client passed in should be configured
    the same way.

    Args:
        bucket: bucket name.
        client: an existing boto3 S3 client (optional).
        **client_kwargs: passed to ``boto3.client("s3", ...)`` when ``client`` is omitted.
    """

    def __init__(self, bucket: str, *, client=None, **client_kwargs):
        try:
            import botocore.exceptions as exceptions
            if client is None:
                import boto3
                from botocore.config import Config
                client = boto3.client("s3", config=Config(retries={"max_attempts": 1, "mode": "standard"}),
                                      **client_kwargs)
        except ImportError as e:
            raise ProtoNotSupportedError(
                "S3ObjectStore requires boto3: pip install boto3 (or proto_db[s3])") from e
        self._exc = exceptions
        # Names present in the installed botocore (older releases lack some of them).
        self._credential_errors = tuple(
            getattr(exceptions, name) for name in
            ("NoCredentialsError", "PartialCredentialsError", "CredentialRetrievalError")
            if hasattr(exceptions, name))
        self._streaming_errors = tuple(
            getattr(exceptions, name) for name in ("IncompleteReadError", "ResponseStreamingError")
            if hasattr(exceptions, name))
        self._bucket = bucket
        self.client = client

    def _call(self, what: str, operation, *, read: bool = False):
        """Run ``operation()`` mapping botocore errors to the protocol.

        ``read``: the operation only reads, so a failure while streaming the response
        body (``IncompleteReadError``, ``ResponseStreamingError``) is transient.
        """
        exc = self._exc
        streaming = self._streaming_errors if read else ()
        try:
            return operation()
        except exc.ClientError as e:
            raise self._client_error(what, e) from e
        except streaming as e:
            raise TransientObjectStoreError(f"S3 {what}: the response body could not be read: {e}") from e
        except exc.ConnectionError as e:   # botocore's: the connection was never established
            raise TransientObjectStoreError(f"S3 {what}: {e}") from e
        except exc.HTTPClientError as e:   # read timeouts, connection closed mid-request
            raise OutcomeUnknownError(f"S3 {what}: {e}") from e
        except self._credential_errors as e:
            raise ProtoNotAuthorizedError(f"S3 {what}: {e}") from e
        except exc.BotoCoreError as e:
            raise ObjectStoreError(f"S3 {what}: {e}") from e

    @staticmethod
    def _client_error(what: str, e) -> Exception:
        response = getattr(e, "response", None) or {}
        code = str(response.get("Error", {}).get("Code", ""))
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        message = f"S3 {what}: {code or status}: {e}"
        if status == 412 or code == "PreconditionFailed":
            return PreconditionFailedError(message)
        if status == 409 or code == "ConditionalRequestConflict":
            return PreconditionFailedError(message)
        if status == 404 or code in ("NoSuchKey", "NotFound"):
            return ObjectNotFoundError(message)
        if status in (401, 403) or code in ("AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch"):
            return ProtoNotAuthorizedError(message)
        if status in (429, 503) or code in ("SlowDown", "ServiceUnavailable", "Throttling",
                                            "RequestTimeout", "RequestTimeTooSkewed"):
            return TransientObjectStoreError(message)
        if isinstance(status, int) and status >= 500:
            return OutcomeUnknownError(message)
        return ObjectStoreError(message)

    def put_if_absent(self, key: str, data: bytes) -> str:
        """See :meth:`ObjectStore.put_if_absent`."""
        response = self._call(f"put_if_absent {key}", lambda: self.client.put_object(
            Bucket=self._bucket, Key=key, Body=bytes(data), IfNoneMatch="*"))
        return response["ETag"]

    def put_if_match(self, key: str, data: bytes, etag: str) -> str:
        """See :meth:`ObjectStore.put_if_match`."""
        try:
            response = self._call(f"put_if_match {key}", lambda: self.client.put_object(
                Bucket=self._bucket, Key=key, Body=bytes(data), IfMatch=etag))
        except ObjectNotFoundError as e:
            raise PreconditionFailedError(f"S3 put_if_match {key}: the object does not exist") from e
        return response["ETag"]

    def get(self, key: str) -> tuple:
        """See :meth:`ObjectStore.get`."""
        def read():
            response = self.client.get_object(Bucket=self._bucket, Key=key)
            return response["Body"].read(), response["ETag"]
        return self._call(f"get {key}", read, read=True)

    def list(self, prefix: str) -> list:
        """See :meth:`ObjectStore.list`."""
        def collect():
            keys = []
            for page in self.client.get_paginator("list_objects_v2").paginate(
                    Bucket=self._bucket, Prefix=prefix):
                keys.extend(item["Key"] for item in page.get("Contents", []))
            return keys
        return self._call(f"list {prefix}", collect, read=True)

    def delete(self, key: str) -> None:
        """See :meth:`ObjectStore.delete`."""
        self._call(f"delete {key}", lambda: self.client.delete_object(Bucket=self._bucket, Key=key))


class GCSObjectStore:
    """:class:`ObjectStore` over Google Cloud Storage with google-cloud-storage.

    Etags are object generations; conditional writes use ``if_generation_match`` (``0``
    for "absent").  Writes pass ``retry=None`` so the SDK never re-sends a conditional
    write on its own (:class:`CloudStorage` decides); reads keep the SDK defaults.

    Args:
        bucket: bucket name.
        client: an existing ``google.cloud.storage.Client`` (optional).
        **client_kwargs: passed to ``google.cloud.storage.Client(...)`` when ``client`` is omitted.
    """

    _GET_ATTEMPTS = 3

    def __init__(self, bucket: str, *, client=None, **client_kwargs):
        try:
            from google.api_core import exceptions
            if client is None:
                from google.cloud import storage
                client = storage.Client(**client_kwargs)
            from google.auth import exceptions as auth_exceptions
            import requests.exceptions as transport
        except ImportError as e:
            raise ProtoNotSupportedError(
                "GCSObjectStore requires google-cloud-storage: pip install google-cloud-storage "
                "(or proto_db[gcs])") from e
        self._exc = exceptions
        self._auth_exc = auth_exceptions
        self._transport = transport
        self.client = client
        self._bucket = client.bucket(bucket)

    def _call(self, what: str, operation):
        exc, transport = self._exc, self._transport
        try:
            return operation()
        except exc.PreconditionFailed as e:
            raise PreconditionFailedError(f"GCS {what}: {e}") from e
        except exc.Conflict as e:
            raise PreconditionFailedError(f"GCS {what}: {e}") from e
        except exc.NotFound as e:
            raise ObjectNotFoundError(f"GCS {what}: {e}") from e
        except (exc.Forbidden, exc.Unauthorized) as e:
            raise ProtoNotAuthorizedError(f"GCS {what}: {e}") from e
        except (exc.TooManyRequests, exc.ServiceUnavailable) as e:
            raise TransientObjectStoreError(f"GCS {what}: {e}") from e
        except exc.GoogleAPICallError as e:
            code = getattr(e, "code", None)
            if isinstance(code, int) and code >= 500:
                raise OutcomeUnknownError(f"GCS {what}: {e}") from e
            raise ObjectStoreError(f"GCS {what}: {e}") from e
        except exc.RetryError as e:
            raise OutcomeUnknownError(f"GCS {what}: {e}") from e
        except transport.ConnectTimeout as e:   # never connected: not applied
            raise TransientObjectStoreError(f"GCS {what}: {e}") from e
        except (transport.Timeout, transport.ConnectionError) as e:
            raise OutcomeUnknownError(f"GCS {what}: {e}") from e
        except self._auth_exc.TransportError as e:
            raise OutcomeUnknownError(f"GCS {what}: {e}") from e

    def _upload(self, what: str, key: str, data: bytes, generation: int) -> str:
        blob = self._bucket.blob(key)
        self._call(what, lambda: blob.upload_from_string(
            bytes(data), if_generation_match=generation, retry=None))
        return str(blob.generation)

    def put_if_absent(self, key: str, data: bytes) -> str:
        """See :meth:`ObjectStore.put_if_absent`."""
        return self._upload(f"put_if_absent {key}", key, data, 0)

    def put_if_match(self, key: str, data: bytes, etag: str) -> str:
        """See :meth:`ObjectStore.put_if_match`."""
        try:
            generation = int(etag)
        except (TypeError, ValueError) as e:
            raise PreconditionFailedError(f"GCS put_if_match {key}: {etag!r} is not a generation") from e
        try:
            return self._upload(f"put_if_match {key}", key, data, generation)
        except ObjectNotFoundError as e:
            raise PreconditionFailedError(f"GCS put_if_match {key}: the object does not exist") from e

    def get(self, key: str) -> tuple:
        """See :meth:`ObjectStore.get`; data and generation always belong to the same version."""
        for _ in range(self._GET_ATTEMPTS):
            blob = self._call(f"get {key}", lambda: self._bucket.get_blob(key))
            if blob is None:
                raise ObjectNotFoundError(f"GCS get {key}: not found")
            generation = blob.generation
            try:
                data = self._call(f"get {key}", lambda: blob.download_as_bytes(if_generation_match=generation))
            except (PreconditionFailedError, ObjectNotFoundError):
                continue  # replaced or deleted between metadata and download: read again
            return data, str(generation)
        raise TransientObjectStoreError(f"GCS get {key}: the object kept changing while being read")

    def list(self, prefix: str) -> list:
        """See :meth:`ObjectStore.list`."""
        return self._call(f"list {prefix}", lambda: [
            b.name for b in self.client.list_blobs(self._bucket, prefix=prefix)])

    def delete(self, key: str) -> None:
        """See :meth:`ObjectStore.delete`."""
        try:
            self._call(f"delete {key}", lambda: self._bucket.blob(key).delete())
        except ObjectNotFoundError:
            return
