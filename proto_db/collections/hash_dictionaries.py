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

``elements()`` yields the values.  Index events, intent logging and
``rebase_onto`` behave exactly as in :class:`~proto_db.collections.dictionaries.Dictionary`
(``("set", key, value)`` / ``("remove", key)`` ops, per-key replay).

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
from .base import ConcurrentOptimized, IndexedCollection, same_value
from .dictionaries import sorted_unique_pairs

__all__ = ["HashDictionary"]

_SET = "set"
_REMOVE = "remove"


class HashDictionary(ConcurrentOptimized, IndexedCollection):
    """Persistent hash map; persisted fields ``root`` (``KeyNode`` tree of buckets), ``count``, ``indexes``."""

    __type_name__ = "proto_db.collections.HashDictionary"
    _fields = ("root", "count", "indexes")

    def __init__(self, *, context: PersistenceContext | None = None):
        """Create an empty hash dictionary."""
        super().__init__(context=context)
        object.__setattr__(self, "root", None)

    def _bucket_hash(self, key) -> int:
        """Bucket hash of a key (``hash_key``).

        Subclasses may override it to force collisions, but must keep it a pure
        function of the key's value: persisted trees are ordered by it.
        """
        return hash_key(key)

    @classmethod
    def from_items(cls, pairs: Iterable[tuple], context: PersistenceContext | None = None) -> "HashDictionary":
        """Return a hash dictionary of ``(key, value)`` pairs built as a balanced tree in O(n log n).

        A later pair for an equal key wins; the result has an empty op log.
        """
        empty = cls(context=context)
        items = sorted_unique_pairs(pairs)
        if not items:
            return empty
        buckets: dict = {}
        for key, value in items:
            buckets.setdefault(empty._bucket_hash(key), []).append((key, value))
        tree = avl.key_from_sorted((h, tuple(buckets[h])) for h in sorted(buckets))
        return empty._with(root=tree, count=len(items))

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
        """Return the value stored under ``key`` or ``default`` when absent."""
        _, bucket, position = self._find(key)
        return default if position < 0 else bucket[position][1]

    def has(self, key) -> bool:
        """True iff ``key`` is present."""
        return self._find(key)[2] >= 0

    __contains__ = has

    def items(self) -> Iterator[tuple]:
        """``(key, value)`` pairs in bucket-hash order."""
        for _, bucket in avl.key_iter(self.root):
            yield from bucket

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

        Replacing keeps the stored key object, as ``dict`` does.
        """
        key, value = canonical(key), canonical(value)
        h, bucket, position = self._find(key)
        log = self._logged((_SET, key, value))
        if position >= 0:
            stored_key, old = bucket[position]
            new_bucket = bucket[:position] + ((stored_key, value),) + bucket[position + 1:]
            root, _, _ = avl.key_put(self.root, h, new_bucket)
            return self._with(root=root, indexes=self._after_replace(old, value), _op_log=log)
        root, _, _ = avl.key_put(self.root, h, bucket + ((key, value),))
        return self._with(root=root, count=self.count + 1, indexes=self._after_add(value), _op_log=log)

    def remove_at(self, key) -> "HashDictionary":
        """Return a hash dictionary without ``key``; an absent key yields a new version with the same content."""
        key = canonical(key)
        h, bucket, position = self._find(key)
        log = self._logged((_REMOVE, key))
        if position < 0:
            return self._with(_op_log=log)
        old = bucket[position][1]
        rest = bucket[:position] + bucket[position + 1:]
        if rest:
            root, _, _ = avl.key_put(self.root, h, rest)
        else:
            root, _, _ = avl.key_remove(self.root, h)
        return self._with(root=root, count=self.count - 1, indexes=self._after_remove(old), _op_log=log)

    def _same_content(self, other) -> bool:
        """Order-independent: every key maps to an equal value (``same_value``) in ``other``."""
        for key, value in self.items():
            _, bucket, position = other._find(key)
            if position < 0 or not same_value(value, bucket[position][1]):
                return False
        return True

    def _replay_op(self, target: "HashDictionary", op: tuple) -> "HashDictionary":
        if op[0] == _SET:
            return target.set_at(op[1], op[2])
        if op[0] == _REMOVE:
            return target.remove_at(op[1])
        raise ProtoValidationError(f"Unknown HashDictionary op {op!r}")

    def __repr__(self):
        if not self._loaded_now():
            return super().__repr__()  # never load a stub to describe it
        return f"<HashDictionary count={self.count}>"
