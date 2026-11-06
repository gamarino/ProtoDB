"""A generic, thread-safe, size-bounded LRU cache.

Used by :mod:`proto_db.storage.file` for segment pages and usable for any value whose
size can be measured (decoded atoms, for example, with a custom ``sizeof``).
"""
import threading
from collections import OrderedDict
from typing import Any, Callable, Hashable, Optional

from ..errors import ProtoValidationError

__all__ = ["BoundedCache"]


class BoundedCache:
    """Least-recently-used cache bounded by the total size of its values.

    Thread-safety: every method takes one internal lock, so the cache may be shared by
    any number of threads.  Values must be immutable (or treated as such by callers),
    because the same object is returned to every reader.

    Args:
        max_bytes: capacity, measured with ``sizeof``.  ``0`` disables caching.  A value
            larger than the capacity is never cached.
        sizeof: function returning the size of a value; defaults to ``len``.
    """

    def __init__(self, max_bytes: int, sizeof: Callable[[Any], int] = len):
        if type(max_bytes) is not int or max_bytes < 0:
            raise ProtoValidationError("max_bytes must be a non-negative int")
        self._max = max_bytes
        self._sizeof = sizeof
        self._lock = threading.Lock()
        self._items = OrderedDict()
        self._size = 0
        self.hits = 0
        self.misses = 0

    @property
    def max_bytes(self) -> int:
        """The configured capacity."""
        return self._max

    @property
    def size_bytes(self) -> int:
        """Total size of the cached values."""
        with self._lock:
            return self._size

    def __len__(self) -> int:
        with self._lock:
            return len(self._items)

    def stats(self) -> dict:
        """A consistent snapshot of the counters: ``hits``, ``misses``, ``entries``, ``bytes``
        (total size of the cached values) and ``capacity`` (``max_bytes``)."""
        with self._lock:
            return {"hits": self.hits, "misses": self.misses, "entries": len(self._items),
                    "bytes": self._size, "capacity": self._max}

    def get(self, key: Hashable) -> Optional[Any]:
        """Return the value cached under ``key`` (marking it recently used) or ``None``."""
        with self._lock:
            entry = self._items.get(key)
            if entry is None:
                self.misses += 1
                return None
            self._items.move_to_end(key)
            self.hits += 1
            return entry[0]

    def peek(self, key: Hashable) -> Optional[Any]:
        """Return the value cached under ``key`` or ``None``, without counting a hit or a miss
        and without changing the eviction order."""
        with self._lock:
            entry = self._items.get(key)
            return None if entry is None else entry[0]

    def put(self, key: Hashable, value: Any) -> None:
        """Cache ``value`` under ``key``, evicting least recently used entries as needed."""
        size = self._sizeof(value)
        with self._lock:
            old = self._items.pop(key, None)
            if old is not None:
                self._size -= old[1]
            if size > self._max:
                return
            self._items[key] = (value, size)
            self._size += size
            while self._size > self._max:
                _, (_, evicted) = self._items.popitem(last=False)
                self._size -= evicted

    def touch(self, key: Hashable) -> None:
        """Mark ``key`` recently used if present, without counting a hit or a miss."""
        with self._lock:
            if key in self._items:
                self._items.move_to_end(key)

    def put_evicting(self, key: Hashable, value: Any) -> list:
        """:meth:`put`, returning the values it dropped: the one ``key`` held before, when it is
        another object, and those evicted for room (callers release them outside the lock)."""
        size = self._sizeof(value)
        dropped = []
        with self._lock:
            old = self._items.pop(key, None)
            if old is not None:
                self._size -= old[1]
                if old[0] is not value:
                    dropped.append(old[0])
            if size > self._max:
                return dropped
            self._items[key] = (value, size)
            self._size += size
            while self._size > self._max:
                _, (evicted, evicted_size) = self._items.popitem(last=False)
                self._size -= evicted_size
                dropped.append(evicted)
        return dropped

    def drain(self) -> list:
        """Remove every entry and return the values (callers release them outside the lock)."""
        with self._lock:
            values = [entry[0] for entry in self._items.values()]
            self._items.clear()
            self._size = 0
        return values

    def invalidate(self, key: Hashable) -> None:
        """Drop ``key`` if present."""
        with self._lock:
            old = self._items.pop(key, None)
            if old is not None:
                self._size -= old[1]

    def clear(self) -> None:
        """Drop every entry."""
        with self._lock:
            self._items.clear()
            self._size = 0
