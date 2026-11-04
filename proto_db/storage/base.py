"""The storage contract of ProtoDB 2.0 (spec sections 6, 7 and 9).

A :class:`SharedStorage` is an append-only store of atoms and blobs plus a single
published *root* guarded by compare-and-swap on an epoch.  Implementations are
thread-safe: every method may be called concurrently from any thread.
"""
import math
import threading
from abc import ABC, abstractmethod
from typing import ContextManager, Optional

from ..core.atom import AtomPointer
from ..errors import ProtoValidationError
from .format import RootRecord

__all__ = ["SharedStorage", "check_lock_timeout"]


def check_lock_timeout(value, name: str = "lock_timeout"):
    """Return ``value`` if it is a valid root-lock wait in seconds, else raise
    ``ProtoValidationError`` (8.3d re-review 14; ruling R62).

    Valid: an ``int`` or ``float`` (not ``bool``) that is finite, positive and at most
    ``threading.TIMEOUT_MAX``.  NaN, infinities and values above ``TIMEOUT_MAX`` used to be
    accepted and made every later ``root_lock`` raise a raw ``ValueError`` or
    ``OverflowError``; there is no "wait forever" spelling."""
    if (isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value)
            or value <= 0 or value > threading.TIMEOUT_MAX):
        raise ProtoValidationError(
            f"{name} must be a finite positive number of seconds (at most {threading.TIMEOUT_MAX:g}), "
            f"not {value!r}")
    return value


class SharedStorage(ABC):
    """Abstract storage shared by all transactions of a space.

    Epoch protocol: a fresh store has no root and epoch ``0``.  ``publish_root`` is a
    compare-and-swap on the epoch; the first publish must pass ``expected_epoch=0`` and
    every publish returns a :class:`RootRecord` whose epoch is the previous one plus one.

    After :meth:`close`, every method raises ``ProtoValidationError``; ``close`` itself
    is idempotent.

    Publish failures: a ``publish_root`` that may or may not have been applied raises
    ``ProtoOutcomeUnknownError``; any other error means nothing was published.
    ``root_lock_is_global`` tells whether :meth:`root_lock` excludes every writer of the
    space (all processes): when it does, a reader of the root after an uncertain publish
    may infer a conflict from an epoch mismatch; when it does not, only
    :meth:`publish_outcome` (and the compare-and-swap itself) can decide.
    """

    #: True when :meth:`root_lock` excludes every writer of the space, in every process.
    root_lock_is_global = True

    @abstractmethod
    def push_atom(self, payload: dict) -> AtomPointer:
        """Serialize ``payload`` (produced by core/atom) and buffer it; return its pointer.

        ``payload`` is only read: the caller may keep it and decode it later as the content at
        the returned pointer (the decoded-atom cache does, Task 8.3i), so a storage must neither
        modify it nor return it, or anything it holds, from :meth:`get_atom`.

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

    def publish_outcome(self, ptr: AtomPointer, epoch: int) -> Optional[bool]:
        """Tell whether ``ptr`` was published as the root of ``epoch``.

        ``True``: it was (possibly superseded since); ``False``: another root holds that
        epoch, so it was not; ``None``: cannot tell.  Used to resolve a
        ``ProtoOutcomeUnknownError`` from :meth:`publish_root`.  The default cannot tell.
        """
        return None

    @abstractmethod
    def root_lock(self, timeout: Optional[float] = None) -> ContextManager:
        """Return a context manager giving exclusive access to the root.

        The lock may be in-process only (``root_lock_is_global = False``, e.g. an object
        store, which has no cross-process lock); the correctness of publishing then rests
        on the compare-and-swap of :meth:`publish_root` and on :meth:`publish_outcome`.

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
