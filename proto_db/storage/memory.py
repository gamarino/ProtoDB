"""In-memory :class:`SharedStorage`, used for tests, caches and ephemeral spaces."""
import contextlib
import threading
import uuid
from typing import Optional

import msgpack

from ..core.atom import AtomPointer
from ..errors import (ProtoClosedError, ProtoConflictError, ProtoCorruptionError, ProtoLockingError,
                      ProtoTimeoutError, ProtoValidationError)
from .base import SharedStorage, check_lock_timeout
from .format import RecordKind, RootRecord, atom_version, root_version, write_version

__all__ = ["MemoryStorage"]


class MemoryStorage(SharedStorage):
    """Thread-safe storage keeping encoded records in memory.

    Atoms are stored as msgpack bytes, exactly as a file storage would, so callers never
    alias stored objects.  Records are kept in segments with a format version, like the other
    storages: a record needing a later version than the current segment's opens a new one
    (ruling R151); the version has no bytes here, so it is kept in a table.  Thread-safety:
    one internal lock guards the record table and the root; ``root_lock`` uses a separate lock
    so holding it never blocks pushes.

    Args:
        lock_timeout: default seconds ``root_lock`` waits before raising
            ``ProtoTimeoutError``; finite and positive
            (:func:`~proto_db.storage.base.check_lock_timeout`).
    """

    def __init__(self, *, lock_timeout: float = 30.0):
        check_lock_timeout(lock_timeout)
        self._lock_timeout = lock_timeout
        self._state_lock = threading.Lock()
        self._root_lock = threading.Lock()
        self._holder = None
        self._segment = uuid.uuid4()
        self._records = []
        self._segments = {self._segment: self._records}
        self._segment_versions = {self._segment: write_version()}
        self._written_version = write_version()
        self._root = None
        self._closed = False

    def _check_open(self):
        if self._closed:
            raise ProtoClosedError("Storage is closed")

    def _append(self, kind: RecordKind, data: bytes, version: int = None) -> AtomPointer:
        version = write_version() if version is None else version
        with self._state_lock:
            self._check_open()
            if self._segment_versions[self._segment] < version:
                self._segment, self._records = uuid.uuid4(), []
                self._segments[self._segment] = self._records
                self._segment_versions[self._segment] = version
                self._written_version = max(self._written_version, version)
            ptr = AtomPointer(self._segment, len(self._records))
            self._records.append((kind, data))
        return ptr

    def _fetch(self, ptr: AtomPointer, kind: RecordKind) -> bytes:
        with self._state_lock:
            self._check_open()
            record = None
            records = self._segments.get(ptr.segment)
            if records is not None and ptr.offset < len(records):
                record = records[ptr.offset]
        if record is None:
            raise ProtoCorruptionError(f"No record at {ptr}")
        if record[0] != kind:
            raise ProtoCorruptionError(f"Record at {ptr} is a {record[0].name}, not a {kind.name}")
        return record[1]

    def push_atom(self, payload: dict, *, features=frozenset()) -> AtomPointer:
        """See :meth:`SharedStorage.push_atom`."""
        if not isinstance(payload, dict):
            raise ProtoValidationError("Atom payload must be a dict")
        version = atom_version(payload, features)
        self._check_open()
        return self._append(RecordKind.ATOM, msgpack.packb(payload, use_bin_type=True), version)

    @property
    def written_version(self) -> int:
        """See :attr:`SharedStorage.written_version`."""
        with self._state_lock:
            return self._written_version

    def get_atom(self, ptr: AtomPointer) -> dict:
        """See :meth:`SharedStorage.get_atom`."""
        return msgpack.unpackb(self._fetch(ptr, RecordKind.ATOM), raw=False)

    def push_blob(self, data: bytes) -> AtomPointer:
        """See :meth:`SharedStorage.push_blob`."""
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise ProtoValidationError("Blob data must be bytes")
        self._check_open()
        return self._append(RecordKind.BLOB, bytes(data))

    def get_blob(self, ptr: AtomPointer) -> bytes:
        """See :meth:`SharedStorage.get_blob`."""
        return self._fetch(ptr, RecordKind.BLOB)

    def flush(self) -> None:
        """No-op beyond the open check: memory has no durability."""
        with self._state_lock:
            self._check_open()

    def read_root(self) -> Optional[RootRecord]:
        """See :meth:`SharedStorage.read_root`."""
        with self._state_lock:
            self._check_open()
            return self._root

    def publish_root(self, ptr: AtomPointer, expected_epoch: Optional[int], *,
                     version: Optional[int] = None) -> RootRecord:
        """See :meth:`SharedStorage.publish_root`."""
        if expected_epoch is not None and (type(expected_epoch) is not int or expected_epoch < 0):
            raise ProtoValidationError("expected_epoch must be a non-negative int or None")
        with self._state_lock:
            self._check_open()
            current = self._root.epoch if self._root is not None else 0
            if expected_epoch is not None and expected_epoch != current:
                raise ProtoConflictError(
                    f"Root epoch is {current}, expected {expected_epoch}")
            version = root_version(self._written_version, self._root.version if self._root is not None else 0,
                                   version or 0)
            self._root = RootRecord(version, ptr.segment, ptr.offset, current + 1)
            return self._root

    def root_lock(self, timeout: Optional[float] = None):
        """See :meth:`SharedStorage.root_lock`."""
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
        """See :meth:`SharedStorage.close`."""
        with self._state_lock:
            self._closed = True
            self._records = []
            self._segments = {}
            self._segment_versions = {}
            self._root = None
