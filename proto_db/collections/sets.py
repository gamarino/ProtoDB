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
Concurrent merge applies, per element, the composed counter transform of the
transaction's ops (D26): ``c -> max(c - removes, 0) + adds``, which equals
replaying the ops one by one after the concurrent version, so concurrent
increments are all kept.  The transform is applied as one adjustment per
element and logged as ``("transform", element, removes, adds)``.

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

__all__ = ["Set", "CountedSet", "counter_net_effect", "counter_transform"]

_ADD = "add"
_REMOVE = "remove"
_TRANSFORM = "transform"
_NET = "net"


def counter_transform(count: int, removes: int, adds: int) -> int:
    """Apply the counter transform ``(removes, adds)`` to ``count``: ``max(count - removes, 0) + adds``."""
    return max(count - removes, 0) + adds


def counter_net_effect(ops: Iterable[tuple]) -> list[tuple]:
    """Compose counter ops per element; return ``(element, removes, adds)`` in order of last occurrence.

    ``ops`` are ``("add", element)`` (increment), ``("remove", element)``
    (decrement clamped at zero) or ``("transform", element, removes, adds)``
    (:func:`counter_transform`).  Any sequence of them is itself a transform:
    add is ``(0, 1)``, remove is ``(1, 0)``, and ``(r1, a1)`` followed by
    ``(r2, a2)`` is ``(r1, a1 - r2 + a2)`` when ``a1 >= r2``, otherwise
    ``(r1 + r2 - a1, a2)``.  Applying the result to any count therefore equals
    applying the ops one by one (D26).  Elements whose transform is the
    identity are omitted.
    """
    net: dict = {}
    for op in ops:
        kind, element = op[0], op[1]
        if kind == _ADD:
            r, a = 0, 1
        elif kind == _REMOVE:
            r, a = 1, 0
        elif kind == _TRANSFORM:
            r, a = op[2], op[3]
        else:
            raise ProtoValidationError(f"Unknown counter op kind {kind!r}")
        okey = order_key(element)
        entry = net.pop(okey, None) or [element, 0, 0]
        if entry[2] >= r:
            entry[2] += a - r
        else:
            entry[1] += r - entry[2]
            entry[2] = a
        net[okey] = entry  # re-inserting moves the element to the end: order of last occurrence
    return [tuple(entry) for entry in net.values() if entry[1] or entry[2]]


class _HashBuckets(ConcurrentOptimized, IndexedCollection):
    """Shared machinery of the hash sets: a ``KeyNode`` tree ``bucket_hash -> bucket tuple``.

    Subclasses define ``_entry_element(entry)`` (the element an entry holds).
    """

    __type_name__ = "proto_db.collections.sets._HashBuckets"
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

    def _same_content(self, other) -> bool:
        """Order-independent: every member of this set is a member of ``other`` (counts are equal)."""
        return all(other.has(e) for e in self.elements())

    def __repr__(self):
        if not self._loaded_now():
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
        new element, one logged add per element of ``other``.  Always a new version.
        """
        result = self._with()
        for element in list(other):
            result = result.add(element)
        return result

    def intersection(self, other: Iterable) -> "Set":
        """Return the members of this set that are also in ``other`` (a Set or any iterable).

        A fold of :meth:`remove` over this set's members absent from ``other``,
        in this set's iteration order.  Always a new version.
        """
        keep = other if isinstance(other, _HashBuckets) else Set.from_iterable(other)
        result = self._with()
        for element in list(self.elements()):
            if not keep.has(element):
                result = result.remove(element)
        return result

    def difference(self, other: Iterable) -> "Set":
        """Return the members of this set that are not in ``other`` (a Set or any iterable).

        A fold of :meth:`remove` in ``other``'s iteration order: one remove
        event per member removed, one logged remove per element of ``other``.
        Always a new version.
        """
        result = self._with()
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

    def _same_content(self, other) -> bool:
        """Order-independent: equal totals and the same count for every element."""
        return (self.total_count == other.total_count
                and all(other.count_of(e) == n for e, n in self.items()))

    def _retarget_at(self, found: tuple, element, final: int, log: tuple) -> "CountedSet":
        """Return the version where ``element`` (canonical) occurs ``final`` times, carrying ``log``.

        ``found`` is ``self._find(element)``.  Fires ``on_add`` when the element
        appears and ``on_remove`` when it disappears; changing a count that
        stays positive fires nothing, and an unchanged count reuses the content.
        """
        h, bucket, position = found
        stored, n = bucket[position] if position >= 0 else (element, 0)
        if final == n:
            return self._with(_op_log=log)
        total = self.total_count + final - n
        if n and final:
            bucket = bucket[:position] + ((stored, final),) + bucket[position + 1:]
            return self._with(root=avl.key_put(self.root, h, bucket)[0], total_count=total, _op_log=log)
        if final:
            return self._with(root=avl.key_put(self.root, h, bucket + ((element, final),))[0],
                              count=self.count + 1, total_count=total,
                              indexes=self._after_add(element), _op_log=log)
        root = self._put_bucket(h, bucket[:position] + bucket[position + 1:])
        return self._with(root=root, count=self.count - 1, total_count=total,
                          indexes=self._after_remove(stored), _op_log=log)

    def _step(self, element, removes: int, adds: int, op: tuple) -> "CountedSet":
        """Apply the counter transform ``(removes, adds)`` to ``element`` (canonical) and log ``op``."""
        found = self._find(element)
        n = found[1][found[2]][1] if found[2] >= 0 else 0
        return self._retarget_at(found, element, counter_transform(n, removes, adds), self._logged(op))

    def add(self, element) -> "CountedSet":
        """Return a counted set with one more occurrence of ``element`` (add event if it is new)."""
        element = canonical(element)
        return self._step(element, 0, 1, (_ADD, element))

    def remove(self, element) -> "CountedSet":
        """Return a counted set with one occurrence fewer (remove event when the last one goes).

        An absent element yields a new version with the same content.
        """
        element = canonical(element)
        return self._step(element, 1, 0, (_REMOVE, element))

    def _transform(self, element, removes: int, adds: int) -> "CountedSet":
        """Apply a composed counter transform as one adjustment (concurrent merge, D26).

        Logged as ``("transform", element, removes, adds)``, so a merged version
        can itself be rebased.
        """
        element = canonical(element)
        return self._step(element, removes, adds, (_TRANSFORM, element, removes, adds))

    def _net_ops(self, element_ops: list) -> list:
        """Per-element counter transforms ``("net", element, removes, adds)`` (D26)."""
        return [(_NET, element, removes, adds) for element, removes, adds in counter_net_effect(element_ops)]

    def _replay_op(self, target: "CountedSet", op: tuple) -> "CountedSet":
        if op[0] != _NET:
            raise ProtoValidationError(f"Unknown CountedSet net op {op!r}")
        _, element, removes, adds = op
        return target._transform(element, removes, adds)
