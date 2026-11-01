"""The storage contract of ProtoDB 2.0 (spec sections 6, 7 and 9).

A :class:`SharedStorage` is an append-only store of atoms and blobs plus a single
published *root* guarded by compare-and-swap on an epoch.  Implementations are
thread-safe: every method may be called concurrently from any thread.
"""
from abc import ABC, abstractmethod
from typing import ContextManager, Optional

from ..core.atom import AtomPointer
from .format import RootRecord

__all__ = ["SharedStorage"]


class SharedStorage(ABC):
    """Abstract storage shared by all transactions of a space.

    Epoch protocol: a fresh store has no root and epoch ``0``.  ``publish_root`` is a
    compare-and-swap on the epoch; the first publish must pass ``expected_epoch=0`` and
    every publish returns a :class:`RootRecord` whose epoch is the previous one plus one.

    After :meth:`close`, every method raises ``ProtoValidationError``; ``close`` itself
    is idempotent.
    """

    @abstractmethod
    def push_atom(self, payload: dict) -> AtomPointer:
        """Serialize ``payload`` (produced by core/atom) and buffer it; return its pointer.

        Synchronous: when it returns the atom is readable via :meth:`get_atom`, though not
        necessarily durable until :meth:`flush`.
        """

    @abstractmethod
    def get_atom(self, ptr: AtomPointer) -> dict:
        """Return a freshly decoded copy of the atom payload stored at ``ptr``.

        Raises ``ProtoCorruptionError`` for an unknown pointer or one that is not an atom.
        """

    @abstractmethod
    def push_blob(self, data: bytes) -> AtomPointer:
        """Buffer an opaque binary blob; return its pointer."""

    @abstractmethod
    def get_blob(self, ptr: AtomPointer) -> bytes:
        """Return the blob stored at ``ptr`` (``ProtoCorruptionError`` if absent or not a blob)."""

    @abstractmethod
    def flush(self) -> None:
        """Make everything pushed so far durable, according to the durability mode."""

    @abstractmethod
    def read_root(self) -> Optional[RootRecord]:
        """Return the currently published root, or ``None`` for a store never published to."""

    @abstractmethod
    def publish_root(self, ptr: AtomPointer, expected_epoch: Optional[int]) -> RootRecord:
        """Publish ``ptr`` as the root if the current epoch equals ``expected_epoch``.

        Raises ``ProtoConflictError`` otherwise, leaving the root unchanged.
        ``expected_epoch=None`` publishes unconditionally and is meant for tooling only.
        Returns the new :class:`RootRecord`.
        """

    @abstractmethod
    def root_lock(self, timeout: Optional[float] = None) -> ContextManager:
        """Return a context manager giving exclusive access to the root.

        ``timeout`` (seconds) defaults to the storage's configured lock timeout; expiry
        raises ``ProtoTimeoutError`` and never proceeds unlocked.  The lock is not
        re-entrant: acquiring it again from the holding thread raises ``ProtoLockingError``
        immediately instead of deadlocking.

        Releasing the lock (exiting the context manager) may raise only a
        ``ProtoDBError`` subclass or an ``OSError``; transactions rely on this to log,
        never raise, a release failure after the root is published.
        """

    @abstractmethod
    def close(self) -> None:
        """Release resources.  Idempotent; further use raises ``ProtoValidationError``."""
