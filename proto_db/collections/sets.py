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
of an absent one change nothing (no index event) but, as intent, are logged in
``_changes``.  ``union``, ``intersection`` and ``difference`` are folds of
``add``/``remove`` over the operand, so indexes are maintained incrementally and
the record holds each element's intent.  Under the precondition model (spec
sections 5 and 6) a linked version -- one a transaction obtained from a root --
records ``Unit(ELEMENT, root, key=e)`` with e's presence in the base for
``has(e)``, ``e in s`` and ``remove(e)`` (a remove that found e absent expects it
still absent), unless the version itself added or removed e (R9); ``add`` has no
precondition; ``intersection`` takes the whole collection, as does every other read.
At commit the last intention per element is reapplied on the current set (R34).

:class:`CountedSet` buckets hold ``(element, count)`` pairs with ``count >= 1``.
``add`` increments, ``remove`` decrements (and deletes at zero; absent is a
no-op).  ``count`` is the number of distinct elements, ``total_count`` the sum of
counts; ``elements()`` yields each distinct element once, so indexes see an
element when it first appears and lose it when its count drops to zero.
``add`` and ``remove`` have no precondition: at commit a linked version's
intentions are reapplied as, per element, the composed counter transform of its
ops (D26): ``c -> max(c - removes, 0) + adds``, which equals replaying the ops one
by one after the concurrent commits, so concurrent increments are all kept and a
decrement past 0 is the serial no-op.  The transform is applied as one adjustment
per element and logged as ``("transform", element, removes, adds)``.  ``has(e)``
and ``count_of(e)`` record ``Unit(COUNT, root, key=e)`` with e's count in the base
(R18: the version's count composes the base's).

Indexed sets (spec section 7, R49).  The reapplication under the space root maintains the
indexes from keys computed in preparation for the elements the intentions name and the members
the BASE stores for them.  A removal takes out the member the CURRENT collection stores, which a
concurrent commit may have replaced by an equal member of another exact form (``1`` by ``1.0``)
whose keys nobody computed.  So on a collection with an index, the ``ELEMENT`` and ``COUNT``
tokens also hold the exact stored member, and ``CountedSet.remove`` records
``Unit(COUNT, root, key=e)`` from the base, and ``Set.remove`` ``Unit(ELEMENT, root, key=e)``
from the base even after an own write of ``e`` (R9 does not exempt it there): such a
concurrent change is a declared conflict on that unit (R36).  ``CountedSet.add`` needs none: on
the current collection it either finds the element (no index event) or files the very element
it names.  Unindexed collections keep D26 composition without any precondition.

A subclass that overrides an operation the model relies on (``SET_OPERATIONS``,
``COUNTED_SET_OPERATIONS``; ``_bucket_hash`` included) is an object root (N5).

Thread safety: a version is immutable once built (buckets are tuples) and may be
shared between threads; lazily loaded nodes are published atomically by the
atom layer.
"""
from __future__ import annotations

from collections.abc import Iterable, Iterator

from ..core.atom import PersistenceContext
from ..errors import ProtoValidationError
from ..preconditions import Unit, UnitKind, token
from ..values import canonical, canonical_for, check_wrapped, exact_token, hash_key, order_key
from . import avl, diff
from .base import CATCH_UP_OPERATIONS, ConcurrentOptimized, IndexedCollection, prepared_parts
from .linking import NOT_A_UNIT, engine_reads, record_key, register_fine_grained, unit_key, wrote, written_with

__all__ = ["Set", "CountedSet", "counter_net_effect", "counter_transform"]

_ADD = "add"
_REMOVE = "remove"
_TRANSFORM = "transform"
_NET = "net"


def same_exact(a, b) -> bool:
    """Whether ``a`` and ``b`` (equal members) are the same exact form (``10`` is not ``10.0``)."""
    return a is b or exact_token(a) == exact_token(b)


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

    The element returned is the exact form the ops leave when they take the count through zero
    (a count of at most ``removes``): the element of the first add after the last point where
    the running sum of the ops reaches its minimum -- the last time the count is zero, whatever
    the starting count -- or the first op's element when no add follows it (then ``adds`` is
    0).  A starting count above ``removes`` never reaches zero, and the stored member keeps its
    form (:meth:`CountedSet._step`).  A transform op counts as its removes, then its adds.
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
        # [first element, running sum, its minimum (<= 0), the re-creating form or None]
        entry = net.pop(okey, None) or [element, 0, 0, None]
        if r:
            entry[1] -= r
            if entry[1] <= entry[2]:
                entry[2], entry[3] = entry[1], None     # the count is zero here, on any start
        if a:
            if entry[3] is None:
                entry[3] = element
            entry[1] += a
        net[okey] = entry  # re-inserting moves the element to the end: order of last occurrence
    out = []
    for first, total, low, form in net.values():
        removes, adds = -low, total - low
        if removes or adds:
            out.append((form if adds else first, removes, adds))
    return out


class _HashBuckets(ConcurrentOptimized, IndexedCollection):
    """Shared machinery of the hash sets: a ``KeyNode`` tree ``bucket_hash -> bucket tuple``.

    Subclasses define ``_entry_element(entry)`` (the element an entry holds).
    """

    __type_name__ = "proto_db.collections.sets._HashBuckets"
    _fields = ("root", "count", "indexes")

    #: MessagePack containers a bucket node value adds around an element (format spec, "Value
    #: nesting depth"): a subclass sets it from the shape of its entries.
    ELEMENT_WRAPPING = 0
    #: What adds them, as the error of an element nested too deep says it.
    _WRAPPER = ""

    def __init__(self, *, context: PersistenceContext | None = None):
        """Create an empty collection."""
        super().__init__(context=context)
        object.__setattr__(self, "root", None)

    def _bucket_hash(self, element) -> int:
        """Bucket hash of an element (``hash_key``).

        Subclasses may override it to force collisions, but must keep it a pure
        function of the element's value: persisted trees are ordered by it.  Such a
        subclass is an object root (N5: the hook would run under the root lock).
        """
        return hash_key(element)

    @staticmethod
    def _entry_element(entry):
        raise NotImplementedError

    def _element(self, element):
        """``element`` canonical, refused (``ProtoValidationError``) when its bucket would nest it
        deeper than MAX_VALUE_DEPTH: at the call, not at commit."""
        holder = f"{type(self).__name__} element"
        element = canonical_for(element, holder)
        check_wrapped(element, self.ELEMENT_WRAPPING, holder, self._WRAPPER)
        return element

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

    def _check_placement(self) -> None:
        for h, bucket in avl.key_iter(self.root):
            for entry in bucket:
                element = self._entry_element(entry)
                if self._bucket_hash(element) != h:
                    self._misfiled(f"the element {element!r} is in the bucket of another hash")
        avl.check_invariants(self.root)

    def elements(self) -> Iterator:
        """Distinct members in bucket-hash order."""
        entry_element = self._entry_element
        for _, bucket in avl.key_iter(self.root):
            for entry in bucket:
                yield entry_element(entry)

    #: The unit kind of one member (``ELEMENT`` for a Set, ``COUNT`` for a CountedSet).
    _MEMBER_UNIT = UnitKind.WHOLE

    def changed_units(self, other, root: str) -> Iterator[Unit]:
        """See :meth:`IndexedCollection.changed_units`: the member unit of every member whose
        entry (the exact member, and its count) differs in a bucket that changed between the two
        bucket trees (R65).  When one version is indexed and the other is not, a member's token
        has another form (the exact member is part of it only when indexed): every unit."""
        if type(other) is not type(self) or self._indexed() != other._indexed():
            yield Unit(UnitKind.WHOLE, root)
            return
        with engine_reads():
            old, new = self.root, other.root
        kind = self._MEMBER_UNIT
        for element in diff.changed_bucket_members(old, new, self._entry_element):
            yield Unit(kind, root, key=unit_key(element))

    def _unit_elements(self, kind, key):
        """See :meth:`IndexedCollection._unit_elements`: the stored member equal to ``key``, once
        (an index holds each distinct member once, R69)."""
        if kind is not self._MEMBER_UNIT:
            return None
        _, bucket, position = self._find(key)
        return [] if position < 0 else [self._entry_element(bucket[position])]

    def _same_content(self, other) -> bool:
        """Order-independent: every member of this set is a member of ``other`` (counts are equal)."""
        return all(other.has(e) for e in self.elements())

    def _intention_elements(self, base):
        """See :meth:`IndexedCollection._intention_elements`: each element named, and the stored
        element ``base`` holds for it (which a removal on the current collection takes; a
        concurrent commit that stored another equal element leaves it without a precomputed key,
        and the reapplication then conflicts)."""
        entry_element = self._entry_element
        for op in prepared_parts(self._changes)[0]:
            yield op[1]
            _, bucket, position = base._find(op[1])
            if position >= 0:
                yield entry_element(bucket[position])

    def __repr__(self):
        if not self._loaded_now():
            return super().__repr__()  # never load a stub to describe it
        with engine_reads():           # describing it is not a read of the transaction
            return f"<{type(self).__name__} count={self.count}>"


class Set(_HashBuckets):
    """Persistent hash set; persisted fields ``root`` (``KeyNode`` tree of element buckets), ``count``, ``indexes``."""

    __type_name__ = "proto_db.collections.Set"

    #: A bucket is a tuple of the elements (``@tup``: 2 containers).
    ELEMENT_WRAPPING = 2
    _WRAPPER = "its bucket tuple"

    _MEMBER_UNIT = UnitKind.ELEMENT

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
        empty = cls(context=context)
        for item in items:
            item = empty._element(item)
            unique.setdefault(order_key(item), item)
        if not unique:
            return empty
        return empty._with(root=cls._tree_from_entries(empty, unique.values()), count=len(unique))

    def _refiled(self) -> "Set":
        # Members that become one element are one member of a set.
        new = type(self).from_iterable(self.elements(), context=self.context)
        return self._with(root=new.root, count=new.count)

    # -- reading ------------------------------------------------------------------------

    def has(self, element) -> bool:
        """True iff ``element`` (canonicalized, as stored elements are) is a member.

        On a linked version it records ``Unit(ELEMENT, root, key=element)`` with the element's
        presence in the base, unless this version added or removed it (spec section 6, R9)."""
        element = canonical(element)
        if self._link is not None and not wrote(self, element):
            record_key(self, element, UnitKind.ELEMENT)
        with engine_reads():
            return self._find(element)[2] >= 0

    __contains__ = has

    def _unit_token(self, kind, key):
        """The token of unit ``kind`` for the canonical element ``key``: for ``ELEMENT`` its
        presence, and on an indexed set also the exact stored member (a removal takes that one
        out of the indexes: see the module docstring); ``NOT_A_UNIT`` for any other kind."""
        if kind is not UnitKind.ELEMENT:
            return NOT_A_UNIT
        _, bucket, position = self._find(key)
        if not self._indexed():
            return token(position >= 0)
        return token((True, bucket[position]) if position >= 0 else (False,))

    # -- mutators -----------------------------------------------------------------------

    def add(self, element) -> "Set":
        """Return a set containing ``element`` (add event unless already present).

        No precondition: on a linked version the intention "present" is recorded in
        ``_changes`` and reapplied at commit (spec section 5)."""
        element = self._element(element)
        with engine_reads():
            h, bucket, position = self._find(element)
            log = self._logged((_ADD, element))
            written = written_with(self, element)
            if position >= 0:
                return self._with(_changes=log, **written)
            root = avl.key_put(self.root, h, bucket + (element,))[0]
            return self._with(root=root, count=self.count + 1, indexes=self._after_add(element), _changes=log,
                              **written)

    def remove(self, element) -> "Set":
        """Return a set without ``element``; an absent element yields a new version with the same content.

        On a linked version it records ``Unit(ELEMENT, root, key=element)`` with the element's
        presence in the base ("still present", or "still absent": then reapplying the intention
        changes nothing), unless this version added or removed it (spec section 5, R9).  On an
        indexed set it records the unit from the base also after such an own write: the
        reapplication takes out the member the current collection stores, which must be the
        one the preparation keyed (module docstring)."""
        element = canonical_for(element, f"{type(self).__name__} element")
        if self._link is not None:
            if self._indexed():
                # Even after an own write of the element (R9 does not apply): the reapplication
                # takes out the member the current collection stores (module docstring).
                record_key(self, element, UnitKind.ELEMENT, from_base=True)
            elif not wrote(self, element):
                record_key(self, element, UnitKind.ELEMENT)
        with engine_reads():
            h, bucket, position = self._find(element)
            log = self._logged((_REMOVE, element))
            written = written_with(self, element)
            if position < 0:
                return self._with(_changes=log, **written)
            old = bucket[position]
            root = self._put_bucket(h, bucket[:position] + bucket[position + 1:])
            return self._with(root=root, count=self.count - 1, indexes=self._after_remove(old), _changes=log,
                              **written)

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
        result = self._with(_whole=True)    # takes the whole collection (spec section 5, P-B)
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

    def _net_ops(self, element_ops: list) -> list:
        """Per element, in order of last occurrence, the ops that leave it as the whole sequence
        does on any set, exact form included (R34): ``remove`` when the last op removes it; else
        ``remove`` then the first ``add`` after the last remove, when the log removed it; else
        its first ``add`` (on a set holding an equal member that add keeps the stored one, as the
        sequence did; the last op alone would file another form)."""
        per: dict = {}
        for op in element_ops:
            okey = order_key(op[1])
            removed, added = per.pop(okey, (None, None))
            if op[0] == _REMOVE:
                removed, added = op, None
            elif added is None:
                added = op
            per[okey] = (removed, added)   # re-inserting: order of last occurrence
        net = []
        for removed, added in per.values():
            if removed is not None:
                net.append(removed)
            if added is not None:
                net.append(added)
        return net

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

    #: A bucket is a tuple of ``(element, count)`` tuples (two ``@tup``: 4 containers).
    ELEMENT_WRAPPING = 4
    _WRAPPER = "its bucket entry (element, count) in its bucket tuple"

    def __init__(self, *, context: PersistenceContext | None = None):
        """Create an empty counted set."""
        super().__init__(context=context)
        object.__setattr__(self, "total_count", 0)

    _MEMBER_UNIT = UnitKind.COUNT

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
        empty = cls(context=context)
        for item in items:
            item = empty._element(item)
            okey = order_key(item)
            entry = counts.get(okey)
            counts[okey] = (item, 1) if entry is None else (entry[0], entry[1] + 1)
            total += 1
        if not counts:
            return empty
        return empty._with(root=cls._tree_from_entries(empty, counts.values()), count=len(counts),
                           total_count=total)

    # -- reading ------------------------------------------------------------------------

    def count_of(self, element) -> int:
        """Number of occurrences of ``element`` (canonicalized; 0 when absent).

        On a linked version it records ``Unit(COUNT, root, key=element)`` with the element's count
        in the base: the version's count composes the base's with its own changes (spec section 6,
        R18)."""
        element = canonical(element)
        if self._link is not None:
            record_key(self, element, UnitKind.COUNT, from_base=True)
        with engine_reads():
            _, bucket, position = self._find(element)
        return 0 if position < 0 else bucket[position][1]

    def has(self, element) -> bool:
        """True iff ``element`` occurs at least once; on a linked version it records
        ``Unit(COUNT, root, key=element)`` as :meth:`count_of` does."""
        return self.count_of(element) > 0

    __contains__ = has

    def _unit_token(self, kind, key):
        """The token of unit ``kind`` for the canonical element ``key``: for ``COUNT`` its count,
        and on an indexed counted set also the exact stored member (a removal takes that one out
        of the indexes: see the module docstring); ``NOT_A_UNIT`` for any other kind."""
        if kind is not UnitKind.COUNT:
            return NOT_A_UNIT
        _, bucket, position = self._find(key)
        if not self._indexed():
            return token(0 if position < 0 else bucket[position][1])
        return token(bucket[position] if position >= 0 else (0,))

    def items(self) -> Iterator[tuple]:
        """``(element, count)`` pairs in bucket-hash order."""
        for _, bucket in avl.key_iter(self.root):
            yield from bucket

    def _refiled(self) -> "CountedSet":
        # Members that become one element add up their counts.
        new = type(self).from_iterable((e for e, n in self.items() for _ in range(n)), context=self.context)
        return self._with(root=new.root, count=new.count, total_count=new.total_count)

    # -- mutators -----------------------------------------------------------------------

    def _same_content(self, other) -> bool:
        """Order-independent: equal totals and the same count for every element."""
        return (self.total_count == other.total_count
                and all(other.count_of(e) == n for e, n in self.items()))

    def _retarget_at(self, found: tuple, element, final: int, log: tuple, recreated: bool = False) -> "CountedSet":
        """Return the version where ``element`` (canonical) occurs ``final`` times, carrying ``log``.

        ``found`` is ``self._find(element)``.  Fires ``on_add`` when the element
        appears and ``on_remove`` when it disappears; changing a count that
        stays positive fires nothing, and an unchanged count reuses the content.
        ``recreated``: the transform took the count through zero and ``element`` is the form
        that re-created it (:func:`counter_net_effect`): a stored member of another exact form
        is replaced by it (an index sees the stored one go and ``element`` come).
        """
        h, bucket, position = found
        stored, n = bucket[position] if position >= 0 else (element, 0)
        if n and final and recreated and not same_exact(stored, element):
            bucket = bucket[:position] + ((element, final),) + bucket[position + 1:]
            return self._with(root=avl.key_put(self.root, h, bucket)[0], total_count=self.total_count + final - n,
                              indexes=self._after_replace(stored, element), _changes=log)
        if final == n:
            return self._with(_changes=log)
        total = self.total_count + final - n
        if n and final:
            bucket = bucket[:position] + ((stored, final),) + bucket[position + 1:]
            return self._with(root=avl.key_put(self.root, h, bucket)[0], total_count=total, _changes=log)
        if final:
            return self._with(root=avl.key_put(self.root, h, bucket + ((element, final),))[0],
                              count=self.count + 1, total_count=total,
                              indexes=self._after_add(element), _changes=log)
        root = self._put_bucket(h, bucket[:position] + bucket[position + 1:])
        return self._with(root=root, count=self.count - 1, total_count=total,
                          indexes=self._after_remove(stored), _changes=log)

    def _step(self, element, removes: int, adds: int, op: tuple) -> "CountedSet":
        """Apply the counter transform ``(removes, adds)`` to ``element`` (canonical) and log ``op``."""
        found = self._find(element)
        n = found[1][found[2]][1] if found[2] >= 0 else 0
        return self._retarget_at(found, element, counter_transform(n, removes, adds), self._logged(op),
                                 recreated=0 < n <= removes and adds > 0)

    def add(self, element) -> "CountedSet":
        """Return a counted set with one more occurrence of ``element`` (add event if it is new).

        No precondition: concurrent increments and decrements compose (D26)."""
        element = self._element(element)
        with engine_reads():
            return self._step(element, 0, 1, (_ADD, element))

    def remove(self, element) -> "CountedSet":
        """Return a counted set with one occurrence fewer (remove event when the last one goes).

        An absent element yields a new version with the same content: the count never goes below
        0, also when the decrement is reapplied on a count a concurrent commit lowered (D26).
        No precondition, except on an indexed counted set: ``Unit(COUNT, root, key=element)``
        with the base's count and stored member (module docstring).
        """
        element = canonical_for(element, f"{type(self).__name__} element")
        if self._link is not None and self._indexed():
            record_key(self, element, UnitKind.COUNT, from_base=True)    # indexed: module docstring
        with engine_reads():
            return self._step(element, 1, 0, (_REMOVE, element))

    def _transform(self, element, removes: int, adds: int) -> "CountedSet":
        """Apply a composed counter transform as one adjustment (reapplication, D26).

        Logged as ``("transform", element, removes, adds)``.
        """
        element = self._element(element)
        with engine_reads():
            return self._step(element, removes, adds, (_TRANSFORM, element, removes, adds))

    def _net_ops(self, element_ops: list) -> list:
        """Per-element counter transforms ``("net", element, removes, adds)`` (D26)."""
        return [(_NET, element, removes, adds) for element, removes, adds in counter_net_effect(element_ops)]

    def _replay_op(self, target: "CountedSet", op: tuple) -> "CountedSet":
        if op[0] != _NET:
            raise ProtoValidationError(f"Unknown CountedSet net op {op!r}")
        _, element, removes, adds = op
        return target._transform(element, removes, adds)


#: The operations of a Set the precondition model lists (spec sections 5 and 6) or relies on to
#: record, validate and reapply them (N5): a subclass overriding one is an object root.
#: ``_bucket_hash`` is among them: a unit's validation and an intention's reapplication run it
#: under the root lock (P-C).
SET_OPERATIONS = ("has", "__contains__", "add", "remove", "union", "difference", "intersection", "_find",
                  "_bucket_hash", "_entry_element", "_element", "_put_bucket", "_unit_token", "elements",
                  "__iter__", "__len__", "content_equals", "_same_content", "_replay_op", "_net_ops", "_op_key",
                  "_reapplied_on", "_with", "_intention_elements", "prepare_changes", "_indexed",
                  "changed_units", "changed_index_groups", "_MEMBER_UNIT") + CATCH_UP_OPERATIONS

#: The operations of a CountedSet the precondition model lists or relies on (N5, as above).
COUNTED_SET_OPERATIONS = ("has", "__contains__", "count_of", "add", "remove", "_transform", "_step",
                          "_retarget_at", "_find", "_bucket_hash", "_entry_element", "_element", "_put_bucket",
                          "_unit_token", "elements", "items", "__iter__", "__len__", "content_equals",
                          "_same_content", "_replay_op", "_net_ops", "_op_key", "_reapplied_on", "_with",
                          "_intention_elements", "prepare_changes", "_indexed", "changed_units",
                          "changed_index_groups", "_MEMBER_UNIT") + CATCH_UP_OPERATIONS

register_fine_grained(Set, SET_OPERATIONS)
register_fine_grained(CountedSet, COUNTED_SET_OPERATIONS)
