"""Dictionary and RepeatedKeysDictionary: immutable, persistent, indexable key-ordered maps
(spec section 8.2, D8, D9, D23, D26).

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

``RepeatedKeysDictionary`` is a multimap on the same keyed tree: each key maps to a
:class:`~proto_db.collections.sets.CountedSet` of records (values must therefore
be orderable).  ``elements()`` yields each distinct record of each key once, in
key order; indexes see a record when it first appears under a key and lose it
when its last occurrence under that key goes.  Its concurrent merge replays,
per key, a ``remove_key`` if the log has one and then the counter transform of
the record ops after it (D26).

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
from .sets import CountedSet, counter_net_effect

__all__ = ["Dictionary", "RepeatedKeysDictionary"]

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


_ADD = "add"
_REMOVE_RECORD = "remove_record"
_REMOVE_KEY = "remove_key"
_NET = "net"


class RepeatedKeysDictionary(ConcurrentOptimized, IndexedCollection):
    """Persistent key-ordered multimap ``key -> CountedSet`` of records.

    Persisted fields: ``root`` (a ``KeyNode`` tree whose values are non-empty
    CountedSets without indexes), ``count`` (distinct ``(key, record)`` pairs,
    the number of elements), ``total_count`` (all record occurrences) and
    ``indexes``.  Keys follow :class:`Dictionary` rules; records follow
    CountedSet rules (``True`` and ``1`` distinct, ``1`` and ``1.0`` equal).
    """

    __type_name__ = "proto_db.collections.RepeatedKeysDictionary"
    _fields = ("root", "count", "total_count", "indexes")

    def __init__(self, *, context: PersistenceContext | None = None):
        """Create an empty multimap."""
        super().__init__(context=context)
        object.__setattr__(self, "root", None)
        object.__setattr__(self, "total_count", 0)

    @classmethod
    def from_items(cls, pairs: Iterable[tuple],
                   context: PersistenceContext | None = None) -> "RepeatedKeysDictionary":
        """Return a multimap holding every ``(key, record)`` pair, built as balanced trees in O(n log n).

        The first object of equal keys is kept.  The result has an empty op log.
        """
        groups: dict = {}
        for key, record in pairs:
            key = _canonical_key(key)
            groups.setdefault(order_key(key), (key, []))[1].append(record)
        empty = cls(context=context)
        if not groups:
            return empty
        tree = [(key, CountedSet.from_iterable(records, context=context))
                for key, records in (groups[okey] for okey in sorted(groups))]
        return empty._with(root=avl.key_from_sorted(tree), count=sum(cs.count for _, cs in tree),
                           total_count=sum(cs.total_count for _, cs in tree))

    # -- reading ------------------------------------------------------------------------

    def elements(self) -> Iterator:
        """Each distinct record of each key once, in key order."""
        for _, records in avl.key_iter(self.root):
            yield from records.elements()

    def get_at(self, key) -> CountedSet:
        """The CountedSet of records under ``key`` (an empty one when absent).

        The returned set is a value: deriving versions from it never changes
        this multimap.
        """
        records = avl.key_get(self.root, key)
        return records if records is not None else CountedSet(context=self.context)

    def has(self, key) -> bool:
        """True iff ``key`` holds at least one record."""
        return avl.key_get(self.root, key) is not None

    __contains__ = has

    def keys(self) -> Iterator:
        """Keys in order."""
        for key, _ in avl.key_iter(self.root):
            yield key

    def items(self) -> Iterator[tuple]:
        """``(key, CountedSet)`` pairs in key order."""
        return avl.key_iter(self.root)

    # -- mutators -----------------------------------------------------------------------

    def set_at(self, key, record) -> "RepeatedKeysDictionary":
        """Return a multimap with one more occurrence of ``record`` under ``key``.

        Fires ``on_add(record)`` when the record is new under that key.
        """
        key, record = _canonical_key(key), canonical(record)
        records = avl.key_get(self.root, key)
        if records is None:
            records = CountedSet(context=self.context)
        known = records.has(record)
        root = avl.key_put(self.root, key, records._adjusted(record, True, ()))[0]
        log = self._logged((_ADD, key, record))
        if known:
            return self._with(root=root, total_count=self.total_count + 1, _op_log=log)
        return self._with(root=root, count=self.count + 1, total_count=self.total_count + 1,
                          indexes=self._after_add(record), _op_log=log)

    def remove_record(self, key, record) -> "RepeatedKeysDictionary":
        """Return a multimap with one occurrence of ``record`` fewer under ``key``.

        The key's other records are kept; the key goes when its last record
        goes.  Fires ``on_remove`` when the last occurrence of the record under
        that key goes.  An absent key or record yields a new version with the
        same content.
        """
        key, record = _canonical_key(key), canonical(record)
        log = self._logged((_REMOVE_RECORD, key, record))
        records = avl.key_get(self.root, key)
        if records is None:
            return self._with(_op_log=log)
        _, bucket, position = records._find(record)
        if position < 0:
            return self._with(_op_log=log)
        stored, n = bucket[position]
        remaining = records._adjusted(record, False, ())
        if remaining.count:
            root = avl.key_put(self.root, key, remaining)[0]
        else:
            root = avl.key_remove(self.root, key)[0]
        if n > 1:
            return self._with(root=root, total_count=self.total_count - 1, _op_log=log)
        return self._with(root=root, count=self.count - 1, total_count=self.total_count - 1,
                          indexes=self._after_remove(stored), _op_log=log)

    def remove_at(self, key) -> "RepeatedKeysDictionary":
        """Return a multimap without ``key`` and all its records (one remove event per distinct record).

        An absent key yields a new version with the same content.
        """
        key = _canonical_key(key)
        log = self._logged((_REMOVE_KEY, key))
        root, removed, records = avl.key_remove(self.root, key)
        if not removed:
            return self._with(_op_log=log)
        indexes = self.indexes
        for record in records.elements():
            indexes = indexes.on_remove(record)
        return self._with(root=root, count=self.count - records.count,
                          total_count=self.total_count - records.total_count, indexes=indexes, _op_log=log)

    # Names used by spec section 8.2.
    add = set_at
    remove_key = remove_at
    get = get_at

    # -- concurrent merge -----------------------------------------------------------------

    def _net_ops(self, element_ops: list) -> list:
        """Per key, in order of last occurrence: ``("net", key, cleared, transforms)`` (D26).

        ``cleared`` is True when the log removed the key; ``transforms`` are the
        counter transforms (:func:`~proto_db.collections.sets.counter_net_effect`)
        of the record ops after the last ``remove_key``.
        """
        per_key: dict = {}
        for op in element_ops:
            okey = order_key(op[1])
            entry = per_key.pop(okey, None) or [op[1], False, []]
            if op[0] == _REMOVE_KEY:
                entry[1], entry[2] = True, []
            elif op[0] == _ADD:
                entry[2].append(("add", op[2]))
            elif op[0] == _REMOVE_RECORD:
                entry[2].append(("remove", op[2]))
            else:
                raise ProtoValidationError(f"Unknown RepeatedKeysDictionary op {op!r}")
            per_key[okey] = entry
        net = []
        for key, cleared, record_ops in per_key.values():
            transforms = counter_net_effect(record_ops)
            if cleared or transforms:
                net.append((_NET, key, cleared, tuple(transforms)))
        return net

    def _replay_op(self, target: "RepeatedKeysDictionary", op: tuple) -> "RepeatedKeysDictionary":
        if op[0] != _NET:
            raise ProtoValidationError(f"Unknown RepeatedKeysDictionary net op {op!r}")
        _, key, cleared, transforms = op
        if cleared:
            target = target.remove_at(key)
        for record, removes, adds in transforms:
            for _ in range(removes):
                target = target.remove_record(key, record)
            for _ in range(adds):
                target = target.set_at(key, record)
        return target

    def __repr__(self):
        if not self.__dict__.get("_loaded", True):
            return super().__repr__()  # never load a stub to describe it
        return f"<RepeatedKeysDictionary keys={0 if self.root is None else self.root.count} count={self.count}>"


class _Absent:
    """Private sentinel type for lookups (``None`` and ``MISSING`` are storable values)."""

    __slots__ = ()


_ABSENT = _Absent()
