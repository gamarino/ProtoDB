"""List: an immutable, persistent, indexable sequence over a positional AVL tree (spec section 8.2, D23).

Elements are addressed by position.  Every mutator returns a new ``List`` and
keeps the associated indexes in step (one index event per element added,
removed or replaced); the receiver is never modified.  ``elements()`` yields the
items in order.

Positions follow Python conventions: a negative position counts from the end.
Single-element operations (``get_at``, ``set_at``, ``remove_at``, ``insert_at``)
raise ``ProtoValidationError`` for an out-of-range position, ``bool`` or
non-``int`` positions; ``slice``, ``head`` and ``tail`` clamp like Python slices.

A ``List`` is not ``ConcurrentOptimized``: positional operations from concurrent
transactions have no meaningful per-key net effect, so a written ``List`` root
follows the "other types" rule of spec section 7 (last writer wins, D23).

Thread safety: a ``List`` version is immutable once built and may be shared
between threads; lazily loaded nodes are published atomically by the atom layer.
"""
from __future__ import annotations

from collections.abc import Iterable, Iterator
from itertools import islice

from ..core.atom import PersistenceContext
from ..errors import ProtoValidationError
from ..values import canonical
from . import avl
from .base import IndexedCollection

__all__ = ["List"]


class List(IndexedCollection):
    """Persistent sequence; persisted fields ``root`` (a ``PosNode`` tree or ``None``), ``count``, ``indexes``."""

    __type_name__ = "proto_db.collections.List"
    _fields = ("root", "count", "indexes")

    def __init__(self, *, context: PersistenceContext | None = None):
        """Create an empty list."""
        super().__init__(context=context)
        object.__setattr__(self, "root", None)

    @classmethod
    def from_iterable(cls, items: Iterable, context: PersistenceContext | None = None) -> "List":
        """Return a list of ``items`` (canonicalised) built as a balanced tree in O(n)."""
        values = [canonical(v) for v in items]
        empty = cls(context=context)
        if not values:
            return empty
        return empty._with(root=avl.pos_from_iterable(values), count=len(values))

    # -- reading ------------------------------------------------------------------------

    def elements(self) -> Iterator:
        """Items in order."""
        return avl.pos_iter(self.root)

    def _scan_window(self, start: int, stop: int | None):
        """Items ``start .. stop-1`` by position, O(log n + k) (D56)."""
        if type(self).elements is not List.elements:
            return None  # a subclass that redefines its elements cannot be windowed by position
        stop = self.count if stop is None else min(stop, self.count)
        if start >= stop:
            return iter(())
        return islice(avl.pos_iter_from(self.root, start), stop - start)

    def _position(self, index, upper: int) -> int:
        """Translate a Python-style position to ``0..upper``; ``ProtoValidationError`` if outside."""
        if type(index) is not int:
            raise ProtoValidationError(f"A list position must be an int, not {type(index).__name__}")
        position = index + self.count if index < 0 else index
        if not 0 <= position <= upper:
            raise ProtoValidationError(f"List position {index} out of range for a list of {self.count}")
        return position

    def get_at(self, index: int):
        """Return the item at ``index`` (negative counts from the end)."""
        return avl.pos_get(self.root, self._position(index, self.count - 1))

    # -- mutators -----------------------------------------------------------------------

    def set_at(self, index: int, value) -> "List":
        """Return a list whose item at ``index`` is ``value`` (fires one replace event)."""
        position = self._position(index, self.count - 1)
        value = canonical(value)
        old = avl.pos_get(self.root, position)
        return self._with(root=avl.pos_set(self.root, position, value),
                          indexes=self._after_replace(old, value))

    def insert_at(self, index: int, value) -> "List":
        """Return a list with ``value`` inserted before position ``index`` (``count`` appends)."""
        position = self._position(index, self.count)
        value = canonical(value)
        return self._with(root=avl.pos_insert(self.root, position, value), count=self.count + 1,
                          indexes=self._after_add(value))

    def remove_at(self, index: int) -> "List":
        """Return a list without the item at ``index``."""
        position = self._position(index, self.count - 1)
        old = avl.pos_get(self.root, position)
        return self._with(root=avl.pos_remove(self.root, position), count=self.count - 1,
                          indexes=self._after_remove(old))

    def append_first(self, value) -> "List":
        """Return a list with ``value`` before the first item."""
        return self.insert_at(0, value)

    def append_last(self, value) -> "List":
        """Return a list with ``value`` after the last item."""
        return self.insert_at(self.count, value)

    def extend(self, items: Iterable) -> "List":
        """Return a list with ``items`` appended in order (one add event per item).

        ``items`` is any iterable, a ``List`` included; it is fully read before
        anything is built.  An empty ``items`` returns the receiver.
        """
        values = [canonical(v) for v in items]
        if not values:
            return self
        indexes = self.indexes
        for v in values:
            indexes = indexes.on_add(v)
        return self._with(root=avl.pos_concat(self.root, avl.pos_from_iterable(values)),
                          count=self.count + len(values), indexes=indexes)

    # -- derived lists --------------------------------------------------------------------

    def slice(self, start: int | None = None, stop: int | None = None) -> "List":
        """Return the list of items ``start .. stop-1`` with Python slice semantics (step 1).

        The result is a new collection, not an edit of this one: its indexes are
        the same index definitions rebuilt (``empty().build``) over the slice.
        Runs in O(log n) plus the index rebuilds.
        """
        lo, hi, _ = slice(start, stop).indices(self.count)
        hi = max(lo, hi)
        root = avl.pos_slice(self.root, lo, hi)
        indexes = self.indexes
        if len(indexes):
            items = list(avl.pos_iter(root))
            indexes = indexes._map(lambda index: index.empty().build(items))
        return self._with(root=root, count=hi - lo, indexes=indexes)

    def head(self, n: int) -> "List":
        """Return the first ``n`` items (``slice(None, n)``; a negative ``n`` drops items from the end)."""
        return self.slice(None, n)

    def tail(self, n: int) -> "List":
        """Return the items from position ``n`` on (``slice(n, None)``; a negative ``n`` keeps the last items)."""
        return self.slice(n, None)

    def __repr__(self):
        if not self._loaded_now():
            return super().__repr__()  # never load a stub to describe it
        return f"<List count={self.count}>"
