"""Fault-injecting SharedStorage double for transaction tests (Task 5.1).

``PublishFaultStorage`` wraps a :class:`MemoryStorage` and delegates every call.  Its
``fail_next_publish(mode)`` arms a one-shot fault for the next ``publish_root``:

* ``"visible"``: the root IS published, then ``ProtoUnexpectedError`` is raised (a
  failure after the rename, e.g. in the directory fsync: the outcome looks unknown but
  the new root is on disk);
* ``"lost"``: nothing is published and ``ProtoUnexpectedError`` is raised (a failure
  before the rename);
* ``"foreign"``: another writer's root is published (an unrelated atom, epoch advanced)
  and ``ProtoUnexpectedError`` is raised;
* ``"oserror"``: like ``"lost"`` but a raw ``OSError`` escapes, as from a storage that
  does not wrap its I/O errors.

``fault_error`` (default ``ProtoUnexpectedError``) is the class raised by the
non-OSError faults; tests set it to ``ProtoOutcomeUnknownError`` and may set the
instance attributes ``root_lock_is_global`` and ``publish_outcome`` to model a storage
with an in-process root lock and an outcome hook.

``fail_read_root_after_fault`` makes the next ``read_root`` after the fault raise as
well, so the outcome stays unknown.  ``publish_calls`` counts publishes.
"""
from proto_db.errors import ProtoUnexpectedError
from proto_db.storage.memory import MemoryStorage


class PublishFaultStorage:
    """Delegating storage double with a one-shot publish fault (see module docstring)."""

    def __init__(self, inner=None):
        self.inner = inner if inner is not None else MemoryStorage()
        self._mode = None
        self.fail_read_root_after_fault = False
        self._read_root_fails = False
        self.publish_calls = 0
        self.fault_error = ProtoUnexpectedError

    def fail_next_publish(self, mode: str) -> None:
        """Arm the fault for the next ``publish_root``."""
        assert mode in ("visible", "lost", "foreign", "oserror")
        self._mode = mode

    def push_atom(self, payload):
        return self.inner.push_atom(payload)

    def get_atom(self, ptr):
        return self.inner.get_atom(ptr)

    def push_blob(self, data):
        return self.inner.push_blob(data)

    def get_blob(self, ptr):
        return self.inner.get_blob(ptr)

    def flush(self):
        self.inner.flush()

    def read_root(self):
        if self._read_root_fails:
            self._read_root_fails = False
            raise ProtoUnexpectedError("injected read_root failure")
        return self.inner.read_root()

    def root_lock(self, timeout=None):
        return self.inner.root_lock(timeout)

    def close(self):
        self.inner.close()

    def publish_root(self, ptr, expected_epoch):
        self.publish_calls += 1
        mode, self._mode = self._mode, None
        if mode is None:
            return self.inner.publish_root(ptr, expected_epoch)
        self._read_root_fails = self.fail_read_root_after_fault
        if mode == "visible":
            self.inner.publish_root(ptr, expected_epoch)
        elif mode == "foreign":
            other = self.inner.push_atom({"foreign": True})
            self.inner.publish_root(other, expected_epoch)
        elif mode == "oserror":
            raise OSError(5, "injected raw publish failure")
        raise self.fault_error(f"injected publish failure ({mode})")
