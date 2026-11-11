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
from itertools import islice

from ..core.atom import PersistenceContext
from ..errors import ProtoValidationError
from ..values import canonical, canonical_for, check_wrapped, order_key
from . import avl
from .base import ConcurrentOptimized, IndexedCollection, check_sorted_keys, repair_pairs, same_value
from .sets import CountedSet, counter_net_effect, counter_transform

__all__ = ["Dictionary", "RepeatedKeysDictionary"]

_SET = "set"
_REMOVE = "remove"


def _canonical_key(key, holder: str = "Key"):
    """Return the canonical form of ``key``, checking that it is orderable; errors name ``holder``."""
    key = canonical_for(key, holder)
    order_key(key)
    return key


def sorted_unique_pairs(pairs: Iterable[tuple], owner: str = "Dictionary", wrapping: int = 0,
                        wrapper: str = "") -> list[tuple]:
    """Canonicalise ``(key, value)`` pairs and return them sorted by ``order_key(key)``, one per key.

    A later pair for an equal key wins its value; the first key object is kept,
    as ``dict`` does.  Errors name ``owner``'s key or value; a key or value that ``wrapping``
    containers (added by ``wrapper``) would nest deeper than MAX_VALUE_DEPTH is refused.
    """
    by_key: dict = {}
    key_holder, value_holder = f"{owner} key", f"{owner} value"
    for key, value in pairs:
        key, value = _canonical_key(key, key_holder), canonical_for(value, value_holder)
        if wrapping:
            check_wrapped(key, wrapping, key_holder, wrapper)
            check_wrapped(value, wrapping, value_holder, wrapper)
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
        items = sorted_unique_pairs(pairs, cls.__name__)
        empty = cls(context=context)
        if not items:
            return empty
        return empty._with(root=avl.key_from_sorted(items), count=len(items))

    # -- reading ------------------------------------------------------------------------

    def elements(self) -> Iterator:
        """Values in key order."""
        for _, value in avl.key_iter(self.root):
            yield value

    def _scan_window(self, start: int, stop: int | None):
        """Values ``start .. stop-1`` in key order, by the node counts: O(log n + k) (D56)."""
        if type(self).elements is not Dictionary.elements:
            return None  # a subclass that redefines its elements cannot be windowed by position
        stop = self.count if stop is None else min(stop, self.count)
        if start >= stop:
            return iter(())
        return islice((value for _, value in avl.key_iter_from(self.root, start)), stop - start)

    def get_at(self, key, default=None):
        """Return the value stored under ``key`` (canonicalized, as stored keys are) or ``default`` when absent."""
        return avl.key_get(self.root, canonical(key), default)

    def has(self, key) -> bool:
        """True iff ``key`` (canonicalized) is present."""
        return avl.key_get(self.root, canonical(key), _ABSENT) is not _ABSENT

    __contains__ = has

    def items(self) -> Iterator[tuple]:
        """``(key, value)`` pairs in key order."""
        return avl.key_iter(self.root)

    def keys(self) -> Iterator:
        """Keys in order."""
        for key, _ in avl.key_iter(self.root):
            yield key

    def _check_placement(self) -> None:
        check_sorted_keys(self, self.root)

    def _refiled(self) -> "Dictionary":
        pairs = repair_pairs(self.items())
        return self._with(root=avl.key_from_sorted(pairs) if pairs else None, count=len(pairs))

    def values(self) -> Iterator:
        """Values in key order (same as :meth:`elements`)."""
        return self.elements()

    # -- mutators -----------------------------------------------------------------------

    def set_at(self, key, value) -> "Dictionary":
        """Return a dictionary with ``key`` mapped to ``value`` (add or replace event)."""
        name = type(self).__name__
        key = _canonical_key(key, f"{name} key")
        stored = avl.boxed(canonical_for(value, f"{name} value"))
        value = avl.unboxed(stored)
        root, replaced, old = avl.key_put(self.root, key, stored)
        # The log keeps the stored form: a merge replaying it (D33) reuses the box, which the
        # transaction saved before the merge, instead of writing a large value twice (D51).
        log = self._logged((_SET, key, stored))
        if replaced:
            return self._with(root=root, indexes=self._after_replace(old, value), _op_log=log)
        return self._with(root=root, count=self.count + 1, indexes=self._after_add(value), _op_log=log)

    def remove_at(self, key) -> "Dictionary":
        """Return a dictionary without ``key``; an absent key yields a new version with the same content."""
        key = _canonical_key(key, f"{type(self).__name__} key")
        root, removed, old = avl.key_remove(self.root, key)
        log = self._logged((_REMOVE, key))
        if not removed:
            return self._with(_op_log=log)
        return self._with(root=root, count=self.count - 1, indexes=self._after_remove(old), _op_log=log)

    def _same_content(self, other) -> bool:
        """Equal ``(key, value)`` pairs in key order (keys by ``order_key``, values by ``same_value``)."""
        return all(order_key(k1) == order_key(k2) and same_value(v1, v2)
                   for (k1, v1), (k2, v2) in zip(self.items(), other.items()))

    def _replay_op(self, target: "Dictionary", op: tuple) -> "Dictionary":
        if op[0] == _SET:
            return target.set_at(op[1], op[2])
        if op[0] == _REMOVE:
            return target.remove_at(op[1])
        raise ProtoValidationError(f"Unknown Dictionary op {op!r}")

    def __repr__(self):
        if not self._loaded_now():
            return super().__repr__()  # never load a stub to describe it
        return f"<Dictionary count={self.count}>"


_ADD = "add"
_REMOVE_RECORD = "remove_record"
_REMOVE_KEY = "remove_key"
_TRANSFORM_RECORD = "transform_record"
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
        empty = cls(context=context)
        for key, record in pairs:
            key, record = _canonical_key(key, f"{cls.__name__} key"), empty._record(record)
            groups.setdefault(order_key(key), (key, []))[1].append(record)
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
        records = avl.key_get(self.root, _canonical_key(key))
        return records if records is not None else CountedSet(context=self.context)

    def has(self, key) -> bool:
        """True iff ``key`` (canonicalized) holds at least one record."""
        return avl.key_get(self.root, _canonical_key(key)) is not None

    __contains__ = has

    def keys(self) -> Iterator:
        """Keys in order."""
        for key, _ in avl.key_iter(self.root):
            yield key

    def items(self) -> Iterator[tuple]:
        """``(key, CountedSet)`` pairs in key order."""
        return avl.key_iter(self.root)

    def _check_placement(self) -> None:
        check_sorted_keys(self, self.root)
        for _, records in avl.key_iter(self.root):
            records._check_placement()

    def _refiled(self) -> "RepeatedKeysDictionary":
        # Records of keys that become one key are merged (a multimap holds every record), and
        # each key's records are refiled by from_items.
        new = type(self).from_items(((key, record) for key, records in self.items()
                                     for record, n in records.items() for _ in range(n)), context=self.context)
        return self._with(root=new.root, count=new.count, total_count=new.total_count)

    # -- mutators -----------------------------------------------------------------------

    def _key(self, key):
        return _canonical_key(key, f"{type(self).__name__} key")

    def _record(self, record):
        """``record`` canonical, refused when the CountedSet of its key would nest it deeper than
        MAX_VALUE_DEPTH (:attr:`CountedSet.ELEMENT_WRAPPING`): at the call, not at commit."""
        holder = f"{type(self).__name__} record"
        record = canonical_for(record, holder)
        check_wrapped(record, CountedSet.ELEMENT_WRAPPING, holder,
                      "the CountedSet of its key (a bucket entry (record, count) in a bucket tuple)")
        return record

    def _record_step(self, key, record, removes: int, adds: int, op: tuple) -> "RepeatedKeysDictionary":
        """Apply the counter transform ``(removes, adds)`` to ``record`` under ``key`` and log ``op``.

        ``key`` and ``record`` are canonical; both are validated (orderable)
        before anything is logged.  Fires ``on_add(record)`` when the record
        appears under the key and ``on_remove(stored record)`` when it
        disappears; the key goes with its last record.
        """
        order_key(record)
        log = self._logged(op)
        records = avl.key_get(self.root, key)
        if records is None:
            if not adds:
                return self._with(_op_log=log)
            records = CountedSet(context=self.context)
        found = records._find(record)
        n = found[1][found[2]][1] if found[2] >= 0 else 0
        final = counter_transform(n, removes, adds)
        if final == n:
            return self._with(_op_log=log)
        updated = records._retarget_at(found, record, final, ())
        if updated.count:
            root = avl.key_put(self.root, key, updated)[0]
        else:
            root = avl.key_remove(self.root, key)[0]
        total = self.total_count + final - n
        if n and final:
            return self._with(root=root, total_count=total, _op_log=log)
        if final:
            return self._with(root=root, count=self.count + 1, total_count=total,
                              indexes=self._after_add(record), _op_log=log)
        stored = found[1][found[2]][0]
        return self._with(root=root, count=self.count - 1, total_count=total,
                          indexes=self._after_remove(stored), _op_log=log)

    def set_at(self, key, record) -> "RepeatedKeysDictionary":
        """Return a multimap with one more occurrence of ``record`` under ``key``.

        Fires ``on_add(record)`` when the record is new under that key.
        """
        key, record = self._key(key), self._record(record)
        return self._record_step(key, record, 0, 1, (_ADD, key, record))

    def remove_record(self, key, record) -> "RepeatedKeysDictionary":
        """Return a multimap with one occurrence of ``record`` fewer under ``key``.

        The key's other records are kept; the key goes when its last record
        goes.  Fires ``on_remove`` when the last occurrence of the record under
        that key goes.  An absent key or record yields a new version with the
        same content; the record must be orderable even then.
        """
        key, record = self._key(key), canonical_for(record, f"{type(self).__name__} record")
        return self._record_step(key, record, 1, 0, (_REMOVE_RECORD, key, record))

    def _transform_record(self, key, record, removes: int, adds: int) -> "RepeatedKeysDictionary":
        """Apply a composed counter transform to one record as one adjustment (concurrent merge, D26)."""
        key, record = self._key(key), self._record(record)
        return self._record_step(key, record, removes, adds, (_TRANSFORM_RECORD, key, record, removes, adds))

    def remove_at(self, key) -> "RepeatedKeysDictionary":
        """Return a multimap without ``key`` and all its records (one remove event per distinct record).

        An absent key yields a new version with the same content.
        """
        key = self._key(key)
        log = self._logged((_REMOVE_KEY, key))
        root, removed, records = avl.key_remove(self.root, key)
        if not removed:
            return self._with(_op_log=log)
        return self._with(root=root, count=self.count - records.count,
                          total_count=self.total_count - records.total_count,
                          indexes=self._after_removes(records.elements()), _op_log=log)

    def _same_content(self, other) -> bool:
        """Equal keys in key order, each with an equal CountedSet of records."""
        return self.total_count == other.total_count and all(
            order_key(k1) == order_key(k2) and r1.content_equals(r2)
            for (k1, r1), (k2, r2) in zip(self.items(), other.items()))

    # Names used by spec section 8.2.
    add = set_at
    remove_key = remove_at
    get = get_at

    # -- concurrent merge -----------------------------------------------------------------

    def _net_ops(self, element_ops: list) -> list:
        """Per key, in order of last occurrence: ``("net", key, cleared, transforms)`` (D26).

        ``cleared`` is True when the log removed the key; ``transforms`` are the
        composed counter transforms (:func:`~proto_db.collections.sets.counter_net_effect`)
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
            elif op[0] == _TRANSFORM_RECORD:
                entry[2].append(("transform", op[2], op[3], op[4]))
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
            target = target._transform_record(key, record, removes, adds)
        return target

    def __repr__(self):
        if not self._loaded_now():
            return super().__repr__()  # never load a stub to describe it
        return f"<RepeatedKeysDictionary keys={0 if self.root is None else self.root.count} count={self.count}>"


class _Absent:
    """Private sentinel type for lookups (``None`` and ``MISSING`` are storable values)."""

    __slots__ = ()


_ABSENT = _Absent()
