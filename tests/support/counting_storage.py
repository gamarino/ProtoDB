"""A storage wrapper that counts the reads made through it (precondition model, Tasks 4 and 12).

``CountingStorage(inner)`` delegates every call to ``inner`` and records each read call in
``reads``, in order, as ``(method name, first argument)``: ``get_atom(ptr)``, ``get_blob(ptr)``
and every ``read_*`` method (``read_root``, ...; the argument is ``None`` when there is none).
``reset()`` empties the record.  Tests use it to assert that a step reads nothing, or does not
read a given pointer::

    storage = CountingStorage(MemoryStorage())
    ...
    storage.reset()
    tr.commit()
    assert box_pointer not in storage.pointers_read()
"""
from __future__ import annotations

import threading

_COUNTED = frozenset({"get_atom", "get_blob"})


class CountingStorage:
    """Delegating storage double that records its read calls (see the module docstring)."""

    def __init__(self, inner):
        self.inner = inner
        self.reads: list = []
        self._lock = threading.Lock()

    def reset(self) -> None:
        """Forget the reads recorded so far."""
        with self._lock:
            self.reads.clear()

    def pointers_read(self) -> list:
        """The pointers of the ``get_atom`` and ``get_blob`` calls recorded, in order."""
        with self._lock:
            return [arg for name, arg in self.reads if name in _COUNTED]

    def __getattr__(self, name):
        attribute = getattr(self.inner, name)
        if not callable(attribute) or not (name in _COUNTED or name.startswith("read_")):
            return attribute

        def counted(*args, **kwargs):
            with self._lock:
                self.reads.append((name, args[0] if args else None))
            return attribute(*args, **kwargs)

        return counted
