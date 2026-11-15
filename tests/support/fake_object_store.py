"""In-memory object store with conditional writes and injectable faults (Task 8.3).

``FakeObjectStore`` implements the :class:`proto_db.storage.cloud.ObjectStore` protocol
the way S3 and GCS behave for single objects: every operation is atomic and strongly
consistent, ``put_if_absent`` and ``put_if_match`` are compare-and-swap writes, and every
write gives the object a new, never reused etag (a generation counter, as in GCS).

Faults are armed per operation with :meth:`FakeObjectStore.inject`:

* ``"transient"``: raises ``TransientObjectStoreError``; nothing is applied;
* ``"unknown_before"``: raises ``OutcomeUnknownError``; nothing is applied;
* ``"unknown_after"``: the write IS applied, then ``OutcomeUnknownError`` is raised (a
  timeout after the request reached the service);
* ``"precondition"``: raises ``PreconditionFailedError`` without applying anything.

``random_faults`` arms the same faults at random (``transient``, ``unknown_before`` and
``unknown_after`` on writes; ``transient`` on reads) with a seeded generator.  ``log``
records every applied write as ``(operation, key)`` in application order, so tests can
check that a root write came after the segment writes it depends on.  ``failed_preconditions``
counts, per key, the conditional writes refused because their precondition did not hold
(injected ``precondition`` faults excluded): the storage-level measure of root write
contention (D44).  ``before_write``
is an optional hook ``(operation, key) -> None`` called before each write is applied,
used to interleave a competing writer at an exact point.

``set_outage(True)`` makes every operation fail with ``TransientObjectStoreError`` (the
store is unreachable) until ``set_outage(False)``; ``latency`` seconds are slept before
every operation (outside the lock, so concurrent requests overlap as in a real service).
"""
import random
import threading
import time
from typing import Callable, Optional

from proto_db.storage.cloud import (ObjectNotFoundError, OutcomeUnknownError,
                                    PreconditionFailedError, TransientObjectStoreError)

OPERATIONS = ("put_if_absent", "put_if_match", "get", "list", "delete")
FAULTS = ("transient", "unknown_before", "unknown_after", "precondition")
_WRITES = ("put_if_absent", "put_if_match", "delete")


class FakeObjectStore:
    """Thread-safe in-memory object store (see module docstring)."""

    #: Nothing re-sends a write below this store, so a refusal is definitive (ruling R147a).  A
    #: test that emulates a hidden SDK retry sets it to ``False`` on its instance.
    refusals_are_definitive = True

    def __init__(self, *, random_faults: float = 0.0, seed: int = 0,
                 before_write: Optional[Callable[[str, str], None]] = None):
        self._lock = threading.Lock()
        self._objects = {}
        self._generation = 0
        self._faults = []
        self._random_faults = random_faults
        self._rng = random.Random(seed)
        self.before_write = before_write
        self.log = []
        self.calls = {op: 0 for op in OPERATIONS}
        self.failed_preconditions = {}     # key -> conditional writes refused by a real precondition
        self.latency = 0.0
        self._outage = False
        self.outage_rejections = 0

    def set_outage(self, down: bool) -> None:
        """Make the store unreachable (every operation fails transiently) or reachable again."""
        with self._lock:
            self._outage = bool(down)

    def _enter(self, operation: str, key: str) -> None:
        """Apply the latency, then refuse the call during an outage."""
        if self.latency:
            time.sleep(self.latency)
        with self._lock:
            if self._outage:
                self.outage_rejections += 1
                raise TransientObjectStoreError(f"store unreachable: {operation} {key}")

    # --- fault control ------------------------------------------------------------

    def inject(self, operation: str, fault: str, *, match: Optional[Callable[[str], bool]] = None,
               times: int = 1) -> None:
        """Arm ``fault`` for the next ``times`` calls of ``operation`` whose key satisfies ``match``."""
        assert operation in OPERATIONS and fault in FAULTS
        if fault == "unknown_after":
            assert operation in _WRITES
        with self._lock:
            self._faults.append([operation, fault, match, times])

    def pending_faults(self) -> int:
        """Number of armed faults not yet consumed."""
        with self._lock:
            return sum(f[3] for f in self._faults)

    def _take_fault(self, operation: str, key: str) -> Optional[str]:
        """Return the fault to apply to this call, if any (lock held)."""
        for entry in self._faults:
            op, fault, match, _ = entry
            if op == operation and (match is None or match(key)):
                entry[3] -= 1
                if entry[3] == 0:
                    self._faults.remove(entry)
                return fault
        if self._random_faults and self._rng.random() < self._random_faults:
            if operation in _WRITES:
                return self._rng.choice(("transient", "unknown_before", "unknown_after"))
            return "transient"
        return None

    @staticmethod
    def _raise_before(fault: Optional[str], operation: str, key: str) -> None:
        if fault == "transient":
            raise TransientObjectStoreError(f"injected transient failure of {operation} {key}")
        if fault == "unknown_before":
            raise OutcomeUnknownError(f"injected timeout of {operation} {key} (not applied)")
        if fault == "precondition":
            raise PreconditionFailedError(f"injected precondition failure of {operation} {key}")

    def _next_etag(self) -> str:
        self._generation += 1
        return str(self._generation)

    # --- the ObjectStore protocol -------------------------------------------------

    def put_if_absent(self, key: str, data: bytes) -> str:
        """Create ``key``; ``PreconditionFailedError`` if it exists."""
        return self._write("put_if_absent", key, bytes(data), None)

    def put_if_match(self, key: str, data: bytes, etag: str) -> str:
        """Replace ``key`` if its etag is ``etag``; ``PreconditionFailedError`` otherwise."""
        return self._write("put_if_match", key, bytes(data), etag)

    def _write(self, operation: str, key: str, data: bytes, etag: Optional[str]) -> str:
        self._enter(operation, key)
        hook = self.before_write
        if hook is not None:
            hook(operation, key)
        with self._lock:
            self.calls[operation] += 1
            fault = self._take_fault(operation, key)
            self._raise_before(fault, operation, key)
            current = self._objects.get(key)
            if (operation == "put_if_absent" and current is not None) or \
                    (operation == "put_if_match" and (current is None or current[1] != etag)):
                self.failed_preconditions[key] = self.failed_preconditions.get(key, 0) + 1
                raise PreconditionFailedError(f"{key}: precondition of {operation} failed")
            new_etag = self._next_etag()
            self._objects[key] = (data, new_etag)
            self.log.append((operation, key))
        if fault == "unknown_after":
            raise OutcomeUnknownError(f"injected timeout of {operation} {key} (applied)")
        return new_etag

    def get(self, key: str) -> tuple:
        """Return ``(data, etag)``; ``ObjectNotFoundError`` if ``key`` is absent."""
        self._enter("get", key)
        with self._lock:
            self.calls["get"] += 1
            self._raise_before(self._take_fault("get", key), "get", key)
            current = self._objects.get(key)
        if current is None:
            raise ObjectNotFoundError(f"{key} not found")
        return current

    def list(self, prefix: str) -> list:
        """Return the sorted keys starting with ``prefix``."""
        self._enter("list", prefix)
        with self._lock:
            self.calls["list"] += 1
            self._raise_before(self._take_fault("list", prefix), "list", prefix)
            return sorted(k for k in self._objects if k.startswith(prefix))

    def delete(self, key: str) -> None:
        """Delete ``key``; deleting an absent key is not an error (as in S3)."""
        self._enter("delete", key)
        hook = self.before_write
        if hook is not None:
            hook("delete", key)
        with self._lock:
            self.calls["delete"] += 1
            fault = self._take_fault("delete", key)
            self._raise_before(fault, "delete", key)
            if self._objects.pop(key, None) is not None:
                self.log.append(("delete", key))
        if fault == "unknown_after":
            raise OutcomeUnknownError(f"injected timeout of delete {key} (applied)")

    # --- inspection ---------------------------------------------------------------

    def keys(self) -> list:
        """Every key, sorted."""
        with self._lock:
            return sorted(self._objects)

    def raw(self, key: str) -> bytes:
        """The stored bytes of ``key`` (no fault, no call counted)."""
        with self._lock:
            return self._objects[key][0]

    def overwrite(self, key: str, data: bytes) -> None:
        """Replace ``key`` unconditionally (simulates outside tampering)."""
        with self._lock:
            self._objects[key] = (bytes(data), self._next_etag())
