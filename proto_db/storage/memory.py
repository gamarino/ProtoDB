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
from .format import FORMAT_VERSION, RecordKind, RootRecord

__all__ = ["MemoryStorage"]


class MemoryStorage(SharedStorage):
    """Thread-safe storage keeping encoded records in memory.

    Atoms are stored as msgpack bytes, exactly as a file storage would, so callers never
    alias stored objects.  Thread-safety: one internal lock guards the record table and
    the root; ``root_lock`` uses a separate lock so holding it never blocks pushes.

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
        self._root = None
        self._closed = False

    def _check_open(self):
        if self._closed:
            raise ProtoClosedError("Storage is closed")

    def _append(self, kind: RecordKind, data: bytes) -> AtomPointer:
        with self._state_lock:
            self._check_open()
            ptr = AtomPointer(self._segment, len(self._records))
            self._records.append((kind, data))
        return ptr

    def _fetch(self, ptr: AtomPointer, kind: RecordKind) -> bytes:
        with self._state_lock:
            self._check_open()
            record = None
            if ptr.segment == self._segment and ptr.offset < len(self._records):
                record = self._records[ptr.offset]
        if record is None:
            raise ProtoCorruptionError(f"No record at {ptr}")
        if record[0] != kind:
            raise ProtoCorruptionError(f"Record at {ptr} is a {record[0].name}, not a {kind.name}")
        return record[1]

    def push_atom(self, payload: dict) -> AtomPointer:
        """See :meth:`SharedStorage.push_atom`."""
        if not isinstance(payload, dict):
            raise ProtoValidationError("Atom payload must be a dict")
        self._check_open()
        return self._append(RecordKind.ATOM, msgpack.packb(payload, use_bin_type=True))

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

    def publish_root(self, ptr: AtomPointer, expected_epoch: Optional[int]) -> RootRecord:
        """See :meth:`SharedStorage.publish_root`."""
        if expected_epoch is not None and (type(expected_epoch) is not int or expected_epoch < 0):
            raise ProtoValidationError("expected_epoch must be a non-negative int or None")
        with self._state_lock:
            self._check_open()
            current = self._root.epoch if self._root is not None else 0
            if expected_epoch is not None and expected_epoch != current:
                raise ProtoConflictError(
                    f"Root epoch is {current}, expected {expected_epoch}")
            self._root = RootRecord(FORMAT_VERSION, ptr.segment, ptr.offset, current + 1)
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
            self._root = None
