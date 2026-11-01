"""Set and CountedSet: immutable, persistent, indexable hash sets (spec section 8.2, D24, D26).

Both are keyed AVL trees whose node key is the bucket hash of an element
(:func:`proto_db.values.hash_key`, process-independent) and whose node value is
a *bucket*: a tuple holding the entries whose elements share that hash.
Membership is value equality (``order_key`` equality, i.e.
:func:`proto_db.values.values_equal`): ``1``, ``1.0`` and ``Decimal(1)`` are one
element, ``True`` is another.  Elements must be orderable (scalars, tuples,
atoms); ``list``/``dict``/NaN elements raise ``ProtoValidationError``.  An atom
element is identified by its oid, so a reloaded stub of it is the same member.
Iteration is in bucket-hash order: deterministic per content, not part of the API.

:class:`Set` buckets hold elements.  ``add`` of a present element and ``remove``
of an absent one change nothing (no index event) but, as intent, are logged.
``union``, ``intersection`` and ``difference`` are folds of ``add``/``remove``
over the operand, so indexes are maintained incrementally and the op log
records each element's intent.  Concurrent merge replays the last intent per
element (presence or absence).

:class:`CountedSet` buckets hold ``(element, count)`` pairs with ``count >= 1``.
``add`` increments, ``remove`` decrements (and deletes at zero; absent is a
no-op).  ``count`` is the number of distinct elements, ``total_count`` the sum of
counts; ``elements()`` yields each distinct element once, so indexes see an
element when it first appears and lose it when its count drops to zero.
Concurrent merge replays, per element, the composed counter transform of the
transaction's ops (D26): ``c -> max(c - removes, 0) + adds``, which equals
replaying the ops one by one after the concurrent version, so concurrent
increments are all kept.

Thread safety: a version is immutable once built (buckets are tuples) and may be
shared between threads; lazily loaded nodes are published atomically by the
atom layer.
"""
from __future__ import annotations

from collections.abc import Iterable, Iterator

from ..core.atom import PersistenceContext
from ..errors import ProtoValidationError
from ..values import canonical, hash_key, order_key
from . import avl
from .base import ConcurrentOptimized, IndexedCollection

__all__ = ["Set", "CountedSet", "counter_net_effect"]

_ADD = "add"
_REMOVE = "remove"
_NET = "net"


def counter_net_effect(ops: Iterable[tuple]) -> list[tuple]:
    """Compose counter ops per element; return ``(element, removes, adds)`` in order of last occurrence.

    ``ops`` are ``(kind, element)`` pairs, kind ``"add"`` (increment) or
    ``"remove"`` (decrement clamped at zero).  Any sequence of them maps a count
    ``c`` to ``max(c - removes, 0) + adds``: an add increments ``adds``; a remove
    consumes a pending add if there is one and otherwise increments
    ``removes``.  Replaying ``removes`` decrements and then ``adds`` increments
    on any count therefore equals replaying the ops one by one (D26).  Elements
    whose transform is the identity are omitted.
    """
    net: dict = {}
    for kind, element in ops:
        okey = order_key(element)
        entry = net.pop(okey, None) or [element, 0, 0]
        if kind == _ADD:
            entry[2] += 1
        elif kind == _REMOVE:
            if entry[2]:
                entry[2] -= 1
            else:
                entry[1] += 1
        else:
            raise ProtoValidationError(f"Unknown counter op kind {kind!r}")
        net[okey] = entry  # re-inserting moves the element to the end: order of last occurrence
    return [tuple(entry) for entry in net.values() if entry[1] or entry[2]]


class _HashBuckets(ConcurrentOptimized, IndexedCollection):
    """Shared machinery of the hash sets: a ``KeyNode`` tree ``bucket_hash -> bucket tuple``.

    Subclasses define ``_entry_element(entry)`` (the element an entry holds).
    """

    _fields = ("root", "count", "indexes")

    def __init__(self, *, context: PersistenceContext | None = None):
        """Create an empty collection."""
        super().__init__(context=context)
        object.__setattr__(self, "root", None)

    def _bucket_hash(self, element) -> int:
        """Bucket hash of an element (``hash_key``).

        Subclasses may override it to force collisions, but must keep it a pure
        function of the element's value: persisted trees are ordered by it.
        """
        return hash_key(element)

    @staticmethod
    def _entry_element(entry):
        raise NotImplementedError

    def _find(self, element) -> tuple:
        """Return ``(bucket_hash, bucket, position)``; position is -1 when ``element`` is absent."""
        h = self._bucket_hash(element)
        okey = order_key(element)
        bucket = avl.key_get(self.root, h, ())
        for position, entry in enumerate(bucket):
            if order_key(self._entry_element(entry)) == okey:
                return h, bucket, position
        return h, bucket, -1

    def _put_bucket(self, h: int, bucket: tuple):
        """Return the root with bucket ``h`` replaced by ``bucket`` (removed when empty)."""
        if bucket:
            return avl.key_put(self.root, h, bucket)[0]
        return avl.key_remove(self.root, h)[0]

    @classmethod
    def _tree_from_entries(cls, empty, entries: Iterable) -> avl.KeyNode | None:
        """Balanced tree of ``entries`` grouped into buckets by ``empty._bucket_hash``."""
        buckets: dict = {}
        for entry in entries:
            buckets.setdefault(empty._bucket_hash(cls._entry_element(entry)), []).append(entry)
        return avl.key_from_sorted((h, tuple(buckets[h])) for h in sorted(buckets))

    def has(self, element) -> bool:
        """True iff ``element`` is a member."""
        return self._find(element)[2] >= 0

    __contains__ = has

    def elements(self) -> Iterator:
        """Distinct members in bucket-hash order."""
        entry_element = self._entry_element
        for _, bucket in avl.key_iter(self.root):
            for entry in bucket:
                yield entry_element(entry)

    def __repr__(self):
        if not self.__dict__.get("_loaded", True):
            return super().__repr__()  # never load a stub to describe it
        return f"<{type(self).__name__} count={self.count}>"


class Set(_HashBuckets):
    """Persistent hash set; persisted fields ``root`` (``KeyNode`` tree of element buckets), ``count``, ``indexes``."""

    __type_name__ = "proto_db.collections.Set"

    @staticmethod
    def _entry_element(entry):
        return entry

    @classmethod
    def from_iterable(cls, items: Iterable, context: PersistenceContext | None = None) -> "Set":
        """Return a set of ``items`` built as a balanced tree in O(n log n).

        Equal items are one member; the first item object is kept.  The result
        has an empty op log.
        """
        unique: dict = {}
        for item in items:
            item = canonical(item)
            unique.setdefault(order_key(item), item)
        empty = cls(context=context)
        if not unique:
            return empty
        return empty._with(root=cls._tree_from_entries(empty, unique.values()), count=len(unique))

    # -- mutators -----------------------------------------------------------------------

    def add(self, element) -> "Set":
        """Return a set containing ``element`` (add event unless already present)."""
        element = canonical(element)
        h, bucket, position = self._find(element)
        log = self._logged((_ADD, element))
        if position >= 0:
            return self._with(_op_log=log)
        root = avl.key_put(self.root, h, bucket + (element,))[0]
        return self._with(root=root, count=self.count + 1, indexes=self._after_add(element), _op_log=log)

    def remove(self, element) -> "Set":
        """Return a set without ``element``; an absent element yields a new version with the same content."""
        element = canonical(element)
        h, bucket, position = self._find(element)
        log = self._logged((_REMOVE, element))
        if position < 0:
            return self._with(_op_log=log)
        old = bucket[position]
        root = self._put_bucket(h, bucket[:position] + bucket[position + 1:])
        return self._with(root=root, count=self.count - 1, indexes=self._after_remove(old), _op_log=log)

    def union(self, other: Iterable) -> "Set":
        """Return this set plus every element of ``other`` (a Set or any iterable of elements).

        A fold of :meth:`add` in ``other``'s iteration order: one add event per
        new element, one logged add per element of ``other``.
        """
        result = self
        for element in list(other):
            result = result.add(element)
        return result

    def intersection(self, other: Iterable) -> "Set":
        """Return the members of this set that are also in ``other`` (a Set or any iterable).

        A fold of :meth:`remove` over this set's members absent from ``other``,
        in this set's iteration order.
        """
        keep = other if isinstance(other, _HashBuckets) else Set.from_iterable(other)
        result = self
        for element in list(self.elements()):
            if not keep.has(element):
                result = result.remove(element)
        return result

    def difference(self, other: Iterable) -> "Set":
        """Return the members of this set that are not in ``other`` (a Set or any iterable).

        A fold of :meth:`remove` in ``other``'s iteration order: one remove
        event per member removed, one logged remove per element of ``other``.
        """
        result = self
        for element in list(other):
            result = result.remove(element)
        return result

    def _replay_op(self, target: "Set", op: tuple) -> "Set":
        if op[0] == _ADD:
            return target.add(op[1])
        if op[0] == _REMOVE:
            return target.remove(op[1])
        raise ProtoValidationError(f"Unknown Set op {op!r}")


class CountedSet(_HashBuckets):
    """Persistent multiset; persisted fields ``root`` (buckets of ``(element, count)``), ``count``
    (distinct elements), ``total_count`` (sum of counts), ``indexes``."""

    __type_name__ = "proto_db.collections.CountedSet"
    _fields = ("root", "count", "total_count", "indexes")

    def __init__(self, *, context: PersistenceContext | None = None):
        """Create an empty counted set."""
        super().__init__(context=context)
        object.__setattr__(self, "total_count", 0)

    @staticmethod
    def _entry_element(entry):
        return entry[0]

    @classmethod
    def from_iterable(cls, items: Iterable, context: PersistenceContext | None = None) -> "CountedSet":
        """Return a counted set holding each item as many times as it occurs, in O(n log n).

        The first object of equal items is kept.  The result has an empty op log.
        """
        counts: dict = {}
        total = 0
        for item in items:
            item = canonical(item)
            okey = order_key(item)
            entry = counts.get(okey)
            counts[okey] = (item, 1) if entry is None else (entry[0], entry[1] + 1)
            total += 1
        empty = cls(context=context)
        if not counts:
            return empty
        return empty._with(root=cls._tree_from_entries(empty, counts.values()), count=len(counts),
                           total_count=total)

    # -- reading ------------------------------------------------------------------------

    def count_of(self, element) -> int:
        """Number of occurrences of ``element`` (0 when absent)."""
        _, bucket, position = self._find(element)
        return 0 if position < 0 else bucket[position][1]

    def items(self) -> Iterator[tuple]:
        """``(element, count)`` pairs in bucket-hash order."""
        for _, bucket in avl.key_iter(self.root):
            yield from bucket

    # -- mutators -----------------------------------------------------------------------

    def _adjusted(self, element, increment: bool, log: tuple) -> "CountedSet":
        """Return the version with ``element`` (canonical) incremented or decremented, carrying ``log``.

        Fires ``on_add`` when the element appears and ``on_remove`` when it
        disappears; a decrement of an absent element reuses the content.
        """
        h, bucket, position = self._find(element)
        if increment:
            if position >= 0:
                stored, n = bucket[position]
                bucket = bucket[:position] + ((stored, n + 1),) + bucket[position + 1:]
                return self._with(root=avl.key_put(self.root, h, bucket)[0],
                                  total_count=self.total_count + 1, _op_log=log)
            return self._with(root=avl.key_put(self.root, h, bucket + ((element, 1),))[0],
                              count=self.count + 1, total_count=self.total_count + 1,
                              indexes=self._after_add(element), _op_log=log)
        if position < 0:
            return self._with(_op_log=log)
        stored, n = bucket[position]
        if n > 1:
            bucket = bucket[:position] + ((stored, n - 1),) + bucket[position + 1:]
            return self._with(root=avl.key_put(self.root, h, bucket)[0],
                              total_count=self.total_count - 1, _op_log=log)
        root = self._put_bucket(h, bucket[:position] + bucket[position + 1:])
        return self._with(root=root, count=self.count - 1, total_count=self.total_count - 1,
                          indexes=self._after_remove(stored), _op_log=log)

    def add(self, element) -> "CountedSet":
        """Return a counted set with one more occurrence of ``element`` (add event if it is new)."""
        element = canonical(element)
        return self._adjusted(element, True, self._logged((_ADD, element)))

    def remove(self, element) -> "CountedSet":
        """Return a counted set with one occurrence fewer (remove event when the last one goes).

        An absent element yields a new version with the same content.
        """
        element = canonical(element)
        return self._adjusted(element, False, self._logged((_REMOVE, element)))

    def _net_ops(self, element_ops: list) -> list:
        """Per-element counter transforms ``("net", element, removes, adds)`` (D26)."""
        return [(_NET, element, removes, adds) for element, removes, adds in counter_net_effect(element_ops)]

    def _replay_op(self, target: "CountedSet", op: tuple) -> "CountedSet":
        if op[0] != _NET:
            raise ProtoValidationError(f"Unknown CountedSet net op {op!r}")
        _, element, removes, adds = op
        for _ in range(removes):
            target = target.remove(element)
        for _ in range(adds):
            target = target.add(element)
        return target
