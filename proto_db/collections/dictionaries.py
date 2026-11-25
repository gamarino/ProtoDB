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
recorded as an intention in ``_changes`` (``("set", key, stored value)`` /
``("remove", key)``), no-ops included.  Under the precondition model (spec sections 5, 6 and 9)
a linked version -- one a transaction obtained from a root -- records ``Unit(KEY, root, key=k)``
for ``get_at(k)``, ``has(k)``, ``k in d``, ``set_at(k, v)`` and ``remove_at(k)``, expecting the
stored form of k in the base (a large value by its ``ValueBox`` pointer, D51), and the whole
collection for every other read (iteration, ``len``, ``count``, ``items``, ``keys``, ``values``,
``content_equals``, :meth:`Dictionary.key_at_position`, :meth:`Dictionary.position_of`).  At
commit its changes are reapplied, last intention per key, on the current dictionary
(:meth:`~proto_db.collections.base.ConcurrentOptimized._reapplied_on`, R34).

``RepeatedKeysDictionary`` is a multimap on the same keyed tree: each key maps to a
:class:`~proto_db.collections.sets.CountedSet` of records (values must therefore
be orderable).  ``elements()`` yields each distinct record of each key once, in
key order; indexes see a record when it first appears under a key and lose it
when its last occurrence under that key goes.  Its writes have no precondition:
at commit a linked version's intentions are reapplied per key as a ``remove_key``
if the version has one and then the counter transform of the record ops after it
(D26).  ``get_at(k)``, ``get(k)``, ``has(k)`` and ``k in r`` record
``Unit(RECORDS, root, key=k)`` with the token of k's CountedSet in the base,
unless the version cleared k (R9); the CountedSet returned has no units (R5).

On an indexed RepeatedKeysDictionary (spec section 7, R49) ``remove_key``/``remove_at`` and
``remove_record`` record ``Unit(RECORDS, root, key=k)`` with the token of k's records in the
base: their reapplication takes out the records the CURRENT collection stores under k (records
a concurrent commit added, or a record it replaced by an equal one of another exact form), whose
index keys the preparation, outside the space root, could not compute.  A concurrent change of
k's records is then a declared conflict on that unit (R36); changes of other keys compose.
``set_at`` needs none (it files the record it names, or finds it stored).  Unindexed, every
write composes (D26).

Thread safety: a version is immutable once built and may be shared between
threads; lazily loaded nodes are published atomically by the atom layer.
"""
from __future__ import annotations

from collections.abc import Iterable, Iterator
from itertools import islice

from ..core.atom import PersistenceContext
from ..errors import ProtoCorruptionError, ProtoValidationError
from ..preconditions import ABSENT, UnitKind, token
from ..values import canonical, canonical_for, check_wrapped, order_key
from . import avl
from .base import (ConcurrentOptimized, IndexedCollection, check_sorted_keys, prepared_parts, repair_pairs,
                   same_value)
from .linking import NOT_A_UNIT, engine_reads, record_key, record_whole, register_fine_grained, wrote, written_with
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
        return empty._with(root=avl.key_from_sorted(items), count=len(items), _whole=True)   # P-B

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
        """Return the value stored under ``key`` (canonicalized, as stored keys are) or ``default`` when absent.

        On a linked version it records ``Unit(KEY, root, key)`` (spec section 6)."""
        key = canonical(key)
        if self._link is None:
            return avl.key_get(self.root, key, default)
        record_key(self, key)
        with engine_reads():
            return avl.key_get(self.root, key, default)

    def has(self, key) -> bool:
        """True iff ``key`` (canonicalized) is present.  On a linked version it records
        ``Unit(KEY, root, key)`` (spec section 6)."""
        key = canonical(key)
        if self._link is None:
            return avl.key_get_stored(self.root, key, ABSENT) is not ABSENT
        record_key(self, key)
        with engine_reads():
            return avl.key_get_stored(self.root, key, ABSENT) is not ABSENT

    __contains__ = has

    def _stored_at(self, key):
        """The stored form of the value under the canonical ``key`` (a :class:`~proto_db.collections.avl.ValueBox`
        as is, never loaded) or ABSENT: what ``Unit(KEY, ...)`` compares (D51).  Records nothing."""
        with engine_reads():
            return avl.key_get_stored(self.root, key, ABSENT)

    def _unit_token(self, kind, key):
        """The token of unit ``kind`` for the canonical ``key`` (what a precondition compares): for
        ``KEY`` the token of :meth:`_stored_at`; ``NOT_A_UNIT`` for any other kind."""
        if kind is not UnitKind.KEY:
            return NOT_A_UNIT
        return token(self._stored_at(key))

    def key_at_position(self, i: int):
        """The key at position ``i`` in key order (a negative ``i`` counts from the end), in
        O(log n) by the node counts (R17).  ``ProtoValidationError`` for a position out of range.
        On a linked version it records the whole collection (spec section 6)."""
        record_whole(self)
        if type(i) is not int:
            raise ProtoValidationError(f"A Dictionary position must be an int, not {type(i).__name__}")
        with engine_reads():
            count, root = self.count, self.root
        position = i + count if i < 0 else i
        if not 0 <= position < count:
            raise ProtoValidationError(f"Dictionary position {i} out of range for a dictionary of {count}")
        return next(avl.key_iter_from(root, position))[0]

    def position_of(self, key) -> int:
        """The position of ``key`` in key order, in O(log n) by the node counts (R17).
        ``ProtoValidationError`` when ``key`` is absent.  On a linked version it records the whole
        collection (spec section 6)."""
        record_whole(self)
        key = _canonical_key(key, f"{type(self).__name__} key")
        with engine_reads():
            root = self.root
        if avl.key_get_stored(root, key, ABSENT) is ABSENT:
            raise ProtoValidationError(f"The key {key!r} is not in the {type(self).__name__}")
        return avl.key_count_below(root, order_key(key))

    def items(self) -> Iterator[tuple]:
        """``(key, value)`` pairs in key order."""
        return avl.key_iter(self.root)

    def keys(self, *, after=avl.UNBOUNDED) -> Iterator:
        """Keys in order; with ``after``, only the keys strictly after it (by ``order_key``, D20), lazily:
        O(log n) to start and O(1) per key, a cursor from the last key given."""
        pairs = avl.key_iter(self.root) if after is avl.UNBOUNDED else \
            avl.key_iter_range(self.root, lo=after, lo_inc=False)
        for key, _ in pairs:
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
        """Return a dictionary with ``key`` mapped to ``value`` (add or replace event).

        On a linked version it records ``Unit(KEY, root, key)`` with the key's base value, unless
        this transaction already wrote that key (spec section 5, R9)."""
        if self._link is None:
            return self._set_at(key, value)
        key = _canonical_key(key, f"{type(self).__name__} key")
        record_key(self, key)
        with engine_reads():
            return self._set_at(key, value)

    def _set_at(self, key, value) -> "Dictionary":
        name = type(self).__name__
        key = _canonical_key(key, f"{name} key")
        stored = avl.boxed(canonical_for(value, f"{name} value"))
        value = avl.unboxed(stored)
        root, replaced, old = avl.key_put(self.root, key, stored)
        # The changes keep the stored form: reapplying them (R34) reuses the box, which the
        # transaction saved before the commit, instead of writing a large value twice (D51).
        log = self._logged((_SET, key, stored))
        if replaced:
            return self._with(root=root, indexes=self._after_replace(old, value), _changes=log)
        return self._with(root=root, count=self.count + 1, indexes=self._after_add(value), _changes=log)

    def remove_at(self, key) -> "Dictionary":
        """Return a dictionary without ``key``; an absent key yields a new version with the same content.

        On a linked version it records ``Unit(KEY, root, key)`` with the key's base value (absent
        included), unless this transaction already wrote that key (spec section 5, R9)."""
        key = _canonical_key(key, f"{type(self).__name__} key")
        if self._link is None:
            return self._remove_at(key)
        record_key(self, key)
        with engine_reads():
            return self._remove_at(key)

    def _remove_at(self, key) -> "Dictionary":
        root, removed, old = avl.key_remove(self.root, key)
        log = self._logged((_REMOVE, key))
        if not removed:
            return self._with(_changes=log)
        return self._with(root=root, count=self.count - 1, indexes=self._after_remove(old), _changes=log)

    def _same_content(self, other) -> bool:
        """Equal ``(key, value)`` pairs in key order (keys by ``order_key``, values by ``same_value``)."""
        return all(order_key(k1) == order_key(k2) and same_value(v1, v2)
                   for (k1, v1), (k2, v2) in zip(self.items(), other.items()))

    def _intention_elements(self, base):
        """See :meth:`IndexedCollection._intention_elements`: each value set, and the value
        ``base`` holds under each key set or removed (the value a ``KEY`` precondition keeps)."""
        for op in prepared_parts(self._changes)[0]:
            if op[0] == _SET:
                yield avl.unboxed(op[2])
            held = avl.key_get(base.root, op[1], ABSENT)
            if held is not ABSENT:
                yield held

    def _replay_op(self, target: "Dictionary", op: tuple) -> "Dictionary":
        if op[0] == _SET:
            return target.set_at(op[1], op[2])
        if op[0] == _REMOVE:
            return target.remove_at(op[1])
        raise ProtoValidationError(f"Unknown Dictionary op {op!r}")

    def __repr__(self):
        if not self._loaded_now():
            return super().__repr__()  # never load a stub to describe it
        with engine_reads():           # describing it is not a read of the transaction
            return f"<Dictionary count={self.count}>"


#: The operations of a Dictionary the precondition model lists (spec sections 5 and 6) or relies
#: on to validate and reapply them: a subclass overriding one is an object root (N5).
DICTIONARY_OPERATIONS = ("get_at", "has", "__contains__", "set_at", "remove_at", "_set_at", "_remove_at",
                         "_stored_at", "_unit_token", "elements", "items", "keys", "values", "__iter__", "__len__",
                         "content_equals", "_same_content", "_scan_window", "key_at_position", "position_of",
                         "_replay_op", "_net_ops", "_op_key", "_reapplied_on", "_with", "_intention_elements",
                         "prepare_changes")

register_fine_grained(Dictionary, DICTIONARY_OPERATIONS)


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
        this multimap, and it has no units of its own (R5).  On a linked version it records
        ``Unit(RECORDS, root, key=key)`` with the token of the key's CountedSet in the base (the
        version's records compose the base's with its own changes, R18), unless this version
        cleared the key with :meth:`remove_at` (R9).
        """
        key = self._recorded_key(key)
        with engine_reads():
            records = avl.key_get(self.root, key)
            return records if records is not None else CountedSet(context=self.context)

    def has(self, key) -> bool:
        """True iff ``key`` (canonicalized) holds at least one record; on a linked version it
        records ``Unit(RECORDS, root, key=key)`` as :meth:`get_at` does."""
        key = self._recorded_key(key)
        with engine_reads():
            return avl.key_get(self.root, key) is not None

    __contains__ = has

    def _recorded_key(self, key):
        """``key`` canonical (refused when not orderable), its read recorded on a linked version."""
        key = _canonical_key(key, f"{type(self).__name__} key")
        if self._link is not None and not wrote(self, key):
            record_key(self, key, UnitKind.RECORDS, from_base=True)
        return key

    def _unit_token(self, kind, key):
        """The token of unit ``kind`` for the canonical ``key``: for ``RECORDS`` the token of the
        key's CountedSet (compared by storage pointer, never loaded) or ABSENT; ``NOT_A_UNIT`` for
        any other kind.  Tokens are taken from saved versions only (the base, the current root),
        whose nested sets are saved: an unsaved one is a ``ProtoCorruptionError``."""
        if kind is not UnitKind.RECORDS:
            return NOT_A_UNIT
        records = avl.key_get(self.root, key, ABSENT)
        if records is not ABSENT and records.atom_pointer is None:
            raise ProtoCorruptionError(
                f"The records of key {key!r} of a saved {type(self).__name__} are not saved: no token")
        return token(records)

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
        with engine_reads():
            return self._record_stepped(key, record, removes, adds, log)

    def _record_stepped(self, key, record, removes: int, adds: int, log: tuple) -> "RepeatedKeysDictionary":
        records = avl.key_get(self.root, key)
        if records is None:
            if not adds:
                return self._with(_changes=log)
            records = CountedSet(context=self.context)
        found = records._find(record)
        n = found[1][found[2]][1] if found[2] >= 0 else 0
        final = counter_transform(n, removes, adds)
        if final == n:
            return self._with(_changes=log)
        updated = records._retarget_at(found, record, final, ())
        if updated.count:
            root = avl.key_put(self.root, key, updated)[0]
        else:
            root = avl.key_remove(self.root, key)[0]
        total = self.total_count + final - n
        if n and final:
            return self._with(root=root, total_count=total, _changes=log)
        if final:
            return self._with(root=root, count=self.count + 1, total_count=total,
                              indexes=self._after_add(record), _changes=log)
        stored = found[1][found[2]][0]
        return self._with(root=root, count=self.count - 1, total_count=total,
                          indexes=self._after_remove(stored), _changes=log)

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
        self._record_removal(key)
        return self._record_step(key, record, 1, 0, (_REMOVE_RECORD, key, record))

    def _record_removal(self, key) -> None:
        """On a linked, indexed version: ``Unit(RECORDS, root, key=key)`` from the base before a
        removal under ``key`` (module docstring); nothing otherwise (D26)."""
        if self._link is not None and self._indexed():
            record_key(self, key, UnitKind.RECORDS, from_base=True)

    def _transform_record(self, key, record, removes: int, adds: int) -> "RepeatedKeysDictionary":
        """Apply a composed counter transform to one record as one adjustment (reapplication, D26)."""
        key, record = self._key(key), self._record(record)
        return self._record_step(key, record, removes, adds, (_TRANSFORM_RECORD, key, record, removes, adds))

    def remove_at(self, key) -> "RepeatedKeysDictionary":
        """Return a multimap without ``key`` and all its records (one remove event per distinct record).

        An absent key yields a new version with the same content.  No precondition: the clear
        is an intention, serializable after any concurrent change of the key (D26); after it the
        key's records are this version's own (R9).  On an indexed multimap it records
        ``Unit(RECORDS, root, key=key)`` from the base (module docstring).
        """
        key = self._key(key)
        self._record_removal(key)
        log = self._logged((_REMOVE_KEY, key))
        with engine_reads():
            written = written_with(self, key)
            root, removed, records = avl.key_remove(self.root, key)
            if not removed:
                return self._with(_changes=log, **written)
            return self._with(root=root, count=self.count - records.count,
                              total_count=self.total_count - records.total_count,
                              indexes=self._after_removes(records.elements()), _changes=log, **written)

    def _same_content(self, other) -> bool:
        """Equal keys in key order, each with an equal CountedSet of records."""
        return self.total_count == other.total_count and all(
            order_key(k1) == order_key(k2) and r1.content_equals(r2)
            for (k1, r1), (k2, r2) in zip(self.items(), other.items()))

    # Names used by spec section 8.2.
    add = set_at
    remove_key = remove_at
    get = get_at

    # -- reapplication (R34, D26) ---------------------------------------------------------

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

    def _intention_elements(self, base):
        """See :meth:`IndexedCollection._intention_elements`: each record named, the stored
        record ``base`` holds for it under its key, and every record ``base`` holds under a key
        removed.  (A record a concurrent commit stored under a removed key has no precomputed key:
        the reapplication then conflicts.)"""
        for op in prepared_parts(self._changes)[0]:
            records = avl.key_get(base.root, op[1])
            if op[0] == _REMOVE_KEY:
                if records is not None:
                    yield from records.elements()
                continue
            yield op[2]
            if records is not None:
                _, bucket, position = records._find(op[2])
                if position >= 0:
                    yield bucket[position][0]

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
        with engine_reads():           # describing it is not a read of the transaction
            return f"<RepeatedKeysDictionary keys={0 if self.root is None else self.root.count} count={self.count}>"


#: The operations of a RepeatedKeysDictionary the precondition model lists (spec sections 5 and 6)
#: or relies on to record, validate and reapply them (N5): a subclass overriding one is an object root.
REPEATED_KEYS_DICTIONARY_OPERATIONS = (
    "get_at", "get", "has", "__contains__", "_recorded_key", "set_at", "add", "remove_record", "remove_at",
    "remove_key", "_transform_record", "_record_step", "_record_stepped", "_key", "_record", "_unit_token",
    "elements", "items", "keys", "__iter__", "__len__", "content_equals", "_same_content", "_replay_op",
    "_net_ops", "_op_key", "_reapplied_on", "_with", "_intention_elements", "prepare_changes", "_indexed",
    "_record_removal")

register_fine_grained(RepeatedKeysDictionary, REPEATED_KEYS_DICTIONARY_OPERATIONS)


class _Absent:
    """Private sentinel type for lookups (``None`` and ``MISSING`` are storable values)."""

    __slots__ = ()


_ABSENT = _Absent()
