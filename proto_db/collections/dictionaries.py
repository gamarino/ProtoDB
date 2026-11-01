"""Dictionary: an immutable, persistent, indexable key-ordered map (spec section 8.2, D8, D9, D23).

A ``Dictionary`` is a keyed AVL tree ordered by :func:`proto_db.values.order_key`
of the key, so lookups, inserts and removals are O(log n) and iteration is in
key order.  Keys are equal when their order keys are equal: ``1``, ``1.0`` and
``Decimal(1)`` are one key, ``True`` is another.  Keys must be orderable
(scalars, tuples, atoms); ``list``/``dict``/NaN keys raise
``ProtoValidationError``.  Values are any canonical value.

``elements()`` yields the values (what the indexes see).  Setting a new key
fires ``on_add(value)``, setting an existing key fires
``on_replace(old_value, new_value)`` and removing a key fires
``on_remove(value)``.

The dictionary is ``ConcurrentOptimized``: every ``set_at``/``remove_at`` call is
logged as intent (``("set", key, value)`` / ``("remove", key)``), no-ops
included, and :meth:`~proto_db.collections.base.ConcurrentOptimized.rebase_onto`
replays the last intent per key onto a concurrently committed version.

Thread safety: a version is immutable once built and may be shared between
threads; lazily loaded nodes are published atomically by the atom layer.
"""
from __future__ import annotations

from collections.abc import Iterable, Iterator

from ..core.atom import PersistenceContext
from ..errors import ProtoValidationError
from ..values import canonical, order_key
from . import avl
from .base import ConcurrentOptimized, IndexedCollection

__all__ = ["Dictionary"]

_SET = "set"
_REMOVE = "remove"


def _canonical_key(key):
    """Return the canonical form of ``key``, checking that it is orderable."""
    key = canonical(key)
    order_key(key)
    return key


def sorted_unique_pairs(pairs: Iterable[tuple]) -> list[tuple]:
    """Canonicalise ``(key, value)`` pairs and return them sorted by ``order_key(key)``, one per key.

    A later pair for an equal key wins its value; the first key object is kept,
    as ``dict`` does.
    """
    by_key: dict = {}
    for key, value in pairs:
        key, value = _canonical_key(key), canonical(value)
        okey = order_key(key)
        previous = by_key.get(okey)
        by_key[okey] = (key if previous is None else previous[0], value)
    return [by_key[okey] for okey in sorted(by_key)]


class Dictionary(ConcurrentOptimized, IndexedCollection):
    """Persistent key-ordered map; persisted fields ``root`` (a ``KeyNode`` tree), ``count``, ``indexes``."""

    __type_name__ = "proto_db.collections.Dictionary"
    _fields = ("root", "count", "indexes")

    def __init__(self, *, context: PersistenceContext | None = None):
        """Create an empty dictionary."""
        super().__init__(context=context)
        object.__setattr__(self, "root", None)

    @classmethod
    def from_items(cls, pairs: Iterable[tuple], context: PersistenceContext | None = None) -> "Dictionary":
        """Return a dictionary of ``(key, value)`` pairs built as a balanced tree in O(n log n).

        A later pair for an equal key wins; the result has an empty op log.
        """
        items = sorted_unique_pairs(pairs)
        empty = cls(context=context)
        if not items:
            return empty
        return empty._with(root=avl.key_from_sorted(items), count=len(items))

    # -- reading ------------------------------------------------------------------------

    def elements(self) -> Iterator:
        """Values in key order."""
        for _, value in avl.key_iter(self.root):
            yield value

    def get_at(self, key, default=None):
        """Return the value stored under ``key`` or ``default`` when absent."""
        return avl.key_get(self.root, key, default)

    def has(self, key) -> bool:
        """True iff ``key`` is present."""
        return avl.key_get(self.root, key, _ABSENT) is not _ABSENT

    __contains__ = has

    def items(self) -> Iterator[tuple]:
        """``(key, value)`` pairs in key order."""
        return avl.key_iter(self.root)

    def keys(self) -> Iterator:
        """Keys in order."""
        for key, _ in avl.key_iter(self.root):
            yield key

    def values(self) -> Iterator:
        """Values in key order (same as :meth:`elements`)."""
        return self.elements()

    # -- mutators -----------------------------------------------------------------------

    def set_at(self, key, value) -> "Dictionary":
        """Return a dictionary with ``key`` mapped to ``value`` (add or replace event)."""
        key, value = _canonical_key(key), canonical(value)
        root, replaced, old = avl.key_put(self.root, key, value)
        log = self._logged((_SET, key, value))
        if replaced:
            return self._with(root=root, indexes=self._after_replace(old, value), _op_log=log)
        return self._with(root=root, count=self.count + 1, indexes=self._after_add(value), _op_log=log)

    def remove_at(self, key) -> "Dictionary":
        """Return a dictionary without ``key``; an absent key yields a new version with the same content."""
        key = _canonical_key(key)
        root, removed, old = avl.key_remove(self.root, key)
        log = self._logged((_REMOVE, key))
        if not removed:
            return self._with(_op_log=log)
        return self._with(root=root, count=self.count - 1, indexes=self._after_remove(old), _op_log=log)

    def _replay_op(self, target: "Dictionary", op: tuple) -> "Dictionary":
        if op[0] == _SET:
            return target.set_at(op[1], op[2])
        if op[0] == _REMOVE:
            return target.remove_at(op[1])
        raise ProtoValidationError(f"Unknown Dictionary op {op!r}")

    def __repr__(self):
        if not self.__dict__.get("_loaded", True):
            return super().__repr__()  # never load a stub to describe it
        return f"<Dictionary count={self.count}>"


class _Absent:
    """Private sentinel type for lookups (``None`` and ``MISSING`` are storable values)."""

    __slots__ = ()


_ABSENT = _Absent()
