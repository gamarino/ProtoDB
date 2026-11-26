"""HashDictionary: an immutable, persistent, indexable hash map (spec section 8.2, D23, D24).

The structure is a keyed AVL tree whose node key is the bucket hash of the
entry key (:func:`proto_db.values.hash_key`, a process-independent 64-bit hash)
and whose node value is a *bucket*: a tuple of ``(key, value)`` pairs whose keys
share that hash, compared with :func:`proto_db.values.values_equal`.  Buckets
almost always hold one pair; a collision just lengthens the tuple.

Keys must satisfy ``hash_key``: scalars, tuples and atoms; ``list``/``dict``/NaN
keys raise ``ProtoValidationError`` (they are no more hashable than orderable).
Key equality is value equality: ``1``, ``1.0`` and ``Decimal(1)`` are one key,
``True`` is another.  Iteration order is the bucket-hash order: deterministic
for a given content, but unrelated to key order and not part of the API.

``elements()`` yields the values.  Index events, the record of intentions (``_changes``) and
the precondition model behave exactly as in :class:`~proto_db.collections.dictionaries.Dictionary`
(``("set", key, value)`` / ``("remove", key)`` ops, ``Unit(KEY, root, key=k)`` for ``get_at``,
``has``, ``in``, ``set_at`` and ``remove_at`` on a linked version, the whole collection for every
other read, per-key reapplication at commit).  Values are stored inline in their bucket, so a
key's precondition compares the value's exact token.

Thread safety: a version is immutable once built (buckets are tuples) and may be
shared between threads; lazily loaded nodes are published atomically by the
atom layer.
"""
from __future__ import annotations

from collections.abc import Iterable, Iterator

from ..core.atom import PersistenceContext
from ..errors import ProtoValidationError
from ..preconditions import ABSENT, Unit, UnitKind, token
from ..values import canonical, canonical_for, check_wrapped, hash_key, order_key
from . import avl, diff
from .base import (CATCH_UP_OPERATIONS, ConcurrentOptimized, IndexedCollection, keyed_net_ops, prepared_parts,
                   repair_pairs, same_value)
from .dictionaries import sorted_unique_pairs
from .linking import NOT_A_UNIT, engine_reads, record_key, register_fine_grained, unit_key

__all__ = ["HashDictionary"]

_SET = "set"
_REMOVE = "remove"


def _entry_key(entry):
    """The key a bucket entry ``(key, value)`` holds."""
    return entry[0]


class HashDictionary(ConcurrentOptimized, IndexedCollection):
    """Persistent hash map; persisted fields ``root`` (``KeyNode`` tree of buckets), ``count``, ``indexes``."""

    __type_name__ = "proto_db.collections.HashDictionary"
    _fields = ("root", "count", "indexes")

    #: MessagePack containers a bucket node value adds around a key and its value (format spec,
    #: "Value nesting depth"): the bucket tuple and the entry ``(key, value)``, two ``@tup``.
    ENTRY_WRAPPING = 4
    _WRAPPER = "its bucket entry (key, value) in its bucket tuple"

    def __init__(self, *, context: PersistenceContext | None = None):
        """Create an empty hash dictionary."""
        super().__init__(context=context)
        object.__setattr__(self, "root", None)

    def _bucket_hash(self, key) -> int:
        """Bucket hash of a key (``hash_key``).

        Subclasses may override it to force collisions, but must keep it a pure
        function of the key's value: persisted trees are ordered by it.  Such a
        subclass is an object root (N5: the hook would run under the root lock).
        """
        return hash_key(key)

    @classmethod
    def from_items(cls, pairs: Iterable[tuple], context: PersistenceContext | None = None) -> "HashDictionary":
        """Return a hash dictionary of ``(key, value)`` pairs built as a balanced tree in O(n log n).

        A later pair for an equal key wins.
        The result is not linked and has no intentions: assigned to a root, it replaces it (R11).
        """
        empty = cls(context=context)
        items = sorted_unique_pairs(pairs, cls.__name__, cls.ENTRY_WRAPPING, cls._WRAPPER)
        if not items:
            return empty
        buckets: dict = {}
        for key, value in items:
            buckets.setdefault(empty._bucket_hash(key), []).append((key, value))
        tree = avl.key_from_sorted((h, tuple(buckets[h])) for h in sorted(buckets))
        return empty._with(root=tree, count=len(items), _whole=True)   # not a listed operation (P-B)

    # -- lookup -------------------------------------------------------------------------

    def _find(self, key) -> tuple:
        """Return ``(bucket_hash, bucket, position)``; position is -1 when ``key`` is absent."""
        h = self._bucket_hash(key)
        okey = order_key(key)
        bucket = avl.key_get(self.root, h, ())
        for position, (k, _) in enumerate(bucket):
            if order_key(k) == okey:
                return h, bucket, position
        return h, bucket, -1

    def get_at(self, key, default=None):
        """Return the value stored under ``key`` (canonicalized, as stored keys are) or ``default`` when absent.

        On a linked version it records ``Unit(KEY, root, key)`` (spec section 6)."""
        key = canonical(key)
        if self._link is not None:
            record_key(self, key)
            with engine_reads():
                _, bucket, position = self._find(key)
        else:
            _, bucket, position = self._find(key)
        return default if position < 0 else bucket[position][1]

    def has(self, key) -> bool:
        """True iff ``key`` (canonicalized) is present.  On a linked version it records
        ``Unit(KEY, root, key)`` (spec section 6)."""
        key = canonical(key)
        if self._link is None:
            return self._find(key)[2] >= 0
        record_key(self, key)
        with engine_reads():
            return self._find(key)[2] >= 0

    __contains__ = has

    def _stored_at(self, key):
        """The value under the canonical ``key`` as its bucket stores it, or ABSENT: what
        ``Unit(KEY, ...)`` compares.  Records nothing."""
        with engine_reads():
            _, bucket, position = self._find(key)
        return ABSENT if position < 0 else bucket[position][1]

    def _unit_token(self, kind, key):
        """The token of unit ``kind`` for the canonical ``key``: for ``KEY`` the token of
        :meth:`_stored_at`; ``NOT_A_UNIT`` for any other kind."""
        if kind is not UnitKind.KEY:
            return NOT_A_UNIT
        return token(self._stored_at(key))

    def changed_units(self, other, root: str) -> Iterator[Unit]:
        """See :meth:`IndexedCollection.changed_units`: ``Unit(KEY)`` of every key whose entry
        differs in a bucket that changed between the two bucket trees (R65)."""
        if type(other) is not type(self):
            yield Unit(UnitKind.WHOLE, root)
            return
        with engine_reads():
            old, new = self.root, other.root
        for key in diff.changed_bucket_members(old, new, _entry_key):
            yield Unit(UnitKind.KEY, root, key=unit_key(key))

    def _unit_elements(self, kind, key):
        """See :meth:`IndexedCollection._unit_elements`: the value under ``key`` (R69)."""
        if kind is not UnitKind.KEY:
            return None
        _, bucket, position = self._find(key)
        return [] if position < 0 else [bucket[position][1]]

    def items(self) -> Iterator[tuple]:
        """``(key, value)`` pairs in bucket-hash order."""
        for _, bucket in avl.key_iter(self.root):
            yield from bucket

    def _check_placement(self) -> None:
        for h, bucket in avl.key_iter(self.root):
            for key, _ in bucket:
                if self._bucket_hash(key) != h:
                    self._misfiled(f"the key {key!r} is in the bucket of another hash")
        avl.check_invariants(self.root)

    def _refiled(self) -> "HashDictionary":
        new = type(self).from_items(repair_pairs(self.items()), context=self.context)
        return self._with(root=new.root, count=new.count)

    def keys(self) -> Iterator:
        """Keys in bucket-hash order."""
        for key, _ in self.items():
            yield key

    def values(self) -> Iterator:
        """Values in bucket-hash order (same as :meth:`elements`)."""
        for _, value in self.items():
            yield value

    def elements(self) -> Iterator:
        """Values in bucket-hash order."""
        return self.values()

    # -- mutators -----------------------------------------------------------------------

    def set_at(self, key, value) -> "HashDictionary":
        """Return a hash dictionary with ``key`` mapped to ``value`` (add or replace event).

        Replacing keeps the stored key object, as ``dict`` does.  On a linked version it records
        ``Unit(KEY, root, key)`` with the key's base value, unless this transaction already wrote
        that key (spec section 5, R9).
        """
        if self._link is None:
            return self._set_at(key, value)
        key = canonical_for(key, f"{type(self).__name__} key")
        record_key(self, key)
        with engine_reads():
            return self._set_at(key, value)

    def _set_at(self, key, value) -> "HashDictionary":
        name = type(self).__name__
        key, value = canonical_for(key, f"{name} key"), canonical_for(value, f"{name} value")
        check_wrapped(key, self.ENTRY_WRAPPING, f"{name} key", self._WRAPPER)
        check_wrapped(value, self.ENTRY_WRAPPING, f"{name} value", self._WRAPPER)
        h, bucket, position = self._find(key)
        log = self._logged((_SET, key, value))
        if position >= 0:
            stored_key, old = bucket[position]
            new_bucket = bucket[:position] + ((stored_key, value),) + bucket[position + 1:]
            root, _, _ = avl.key_put(self.root, h, new_bucket)
            return self._with(root=root, indexes=self._after_replace(old, value), _changes=log)
        root, _, _ = avl.key_put(self.root, h, bucket + ((key, value),))
        return self._with(root=root, count=self.count + 1, indexes=self._after_add(value), _changes=log)

    def remove_at(self, key) -> "HashDictionary":
        """Return a hash dictionary without ``key``; an absent key yields a new version with the same content.

        On a linked version it records ``Unit(KEY, root, key)`` with the key's base value (absent
        included), unless this transaction already wrote that key (spec section 5, R9)."""
        key = canonical_for(key, f"{type(self).__name__} key")
        if self._link is None:
            return self._remove_at(key)
        record_key(self, key)
        with engine_reads():
            return self._remove_at(key)

    def _remove_at(self, key) -> "HashDictionary":
        h, bucket, position = self._find(key)
        log = self._logged((_REMOVE, key))
        if position < 0:
            return self._with(_changes=log)
        old = bucket[position][1]
        rest = bucket[:position] + bucket[position + 1:]
        if rest:
            root, _, _ = avl.key_put(self.root, h, rest)
        else:
            root, _, _ = avl.key_remove(self.root, h)
        return self._with(root=root, count=self.count - 1, indexes=self._after_remove(old), _changes=log)

    def _same_content(self, other) -> bool:
        """Order-independent: every key maps to an equal value (``same_value``) in ``other``."""
        for key, value in self.items():
            _, bucket, position = other._find(key)
            if position < 0 or not same_value(value, bucket[position][1]):
                return False
        return True

    def _intention_elements(self, base):
        """See :meth:`IndexedCollection._intention_elements`: each value set, and the value
        ``base`` holds under each key set or removed (the value a ``KEY`` precondition keeps)."""
        for op in prepared_parts(self._changes)[0]:
            if op[0] == _SET:
                yield op[2]
            _, bucket, position = base._find(op[1])
            if position >= 0:
                yield bucket[position][1]

    def _net_ops(self, element_ops: list) -> list:
        """Per key, the ops that leave it as the log does, exact key form included
        (:func:`~proto_db.collections.base.keyed_net_ops`)."""
        return keyed_net_ops(element_ops, _SET, _REMOVE)

    def _replay_op(self, target: "HashDictionary", op: tuple) -> "HashDictionary":
        if op[0] == _SET:
            return target.set_at(op[1], op[2])
        if op[0] == _REMOVE:
            return target.remove_at(op[1])
        raise ProtoValidationError(f"Unknown HashDictionary op {op!r}")

    def __repr__(self):
        if not self._loaded_now():
            return super().__repr__()  # never load a stub to describe it
        with engine_reads():           # describing it is not a read of the transaction
            return f"<HashDictionary count={self.count}>"


#: The operations of a HashDictionary the precondition model lists or relies on (N5).  ``_bucket_hash``
#: is among them: validating a key's unit and reapplying an intention run it under the root lock,
#: so an overriding subclass would run user code there (P-C); it is an object root.
HASH_DICTIONARY_OPERATIONS = ("get_at", "has", "__contains__", "set_at", "remove_at", "_set_at", "_remove_at",
                              "_stored_at", "_unit_token", "_find", "_bucket_hash", "elements", "items", "keys",
                              "values", "__iter__", "__len__", "content_equals", "_same_content", "_scan_window",
                              "_replay_op", "_net_ops", "_op_key", "_reapplied_on", "_with",
                              "_intention_elements", "prepare_changes", "changed_units",
                              "changed_index_groups") + CATCH_UP_OPERATIONS

register_fine_grained(HashDictionary, HASH_DICTIONARY_OPERATIONS)
