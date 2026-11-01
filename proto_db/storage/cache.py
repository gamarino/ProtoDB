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
