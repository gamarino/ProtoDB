from __future__ import annotations
from typing import cast, TYPE_CHECKING, Iterable
from .common import Atom, QueryPlan, AbstractTransaction, AtomPointer, DBCollections, canonical_hash
from .lists import List
from .hash_dictionaries import HashDictionary
from .queries import IndexedQueryPlan

if TYPE_CHECKING:
    # Only for type annotations
    from .dictionaries import Dictionary, RepeatedKeysDictionary


class Set(Atom):
    """
    A custom implementation of a mathematical set, storing unique elements of type `Atom`.
    The internal data structure is backed by a `HashDictionary` which ensures that
    duplicates are avoided and allows for efficient operations such as lookup, insertion,
    and element removal.
    Sets can handle any object, but only using Atoms the Set will be durable. Mixing any other
    objects with Atoms is not supported (no warning will be emitted)
    """
    content: HashDictionary  # The underlying container storing the set elements.

    """
    Initializes a `Set` instance.

    :param content: The `HashDictionary` instance that represents the underlying storage of the set.
    :param transaction_id: (Optional) A unique transaction identifier for audit or rollback use.
    :param offset: An optional offset for identifying the set's relative position in an operation.
    :param kwargs: Any additional data passed for extended configurations.
    """

    # _new_objects is a temporary storage for objects that have not been saved yet.
    # This is used to ensure that the set will not be persisted in storage until commit.
    # This logic enables the creation of temporary sets inside the transaction, that will
    # be persisted only when the transaction is committed. If the set is not part of
    # the finally committed objects, no storage will be consumed.
    _new_objects: HashDictionary = None

    def __init__(
            self,
            content: HashDictionary = None,
            new_objects: HashDictionary = None,
            transaction: AbstractTransaction = None,
            atom_pointer: AtomPointer = None,
            indexes: DBCollections | None = None,
            **kwargs):
        super().__init__(transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self.content = content if content else HashDictionary(transaction=transaction)
        self._new_objects = new_objects if new_objects else HashDictionary(transaction=transaction)
        self.count = self.content.count + self._new_objects.count

        if indexes is None:
            # Local import to avoid circular dependency
            from .dictionaries import Dictionary as _Dictionary
            self.indexes = _Dictionary(transaction=transaction)
        else:
            self.indexes = indexes

    # Unified hashing using canonical_hash for identity stability
    def _hash_of(self, key: object) -> int:
        return canonical_hash(key)

    def _save(self):
        if not self._saved:
            for element in self._new_objects:
                if isinstance(element, Atom):
                    element._save()
                self.content = self.content.set_at(self._hash_of(element), element)

            self.content.transaction = self.transaction
            self.content._save()

            super()._save()

    def as_iterable(self) -> Iterable:
        """
        Converts the `Set` to an iterable structure, essentially a collection of its unique
        elements, and yields each element stored in the set.

        :return: A generator containing all the elements (`Atom`) in the set.
        """
        # Iterate over the stored hash dictionary's iterable and yield its items (the stored Atoms).
        self._load()

        for element in self._new_objects:
            yield element

        for hash_value, item in self.content.as_iterable():
            yield item

    def __iter__(self):
        # Allow Python iteration protocols (e.g., list(set_obj))
        return iter(self.as_iterable())

    def as_query_plan(self) -> QueryPlan:
        """
        Converts the `Set` into a `QueryPlan` object for integration with larger query
        execution frameworks. Delegates the conversion to the underlying `HashDictionary`.

        :return: A `QueryPlan` representation of the current Set.
        """
        self._load()

        if self.indexes:
            return IndexedQueryPlan(base=self, indexes=self.indexes)
        else:
            return QueryPlan(base=self)

    def add_index(self, field_name: str):
        # Local import to avoid circular dependency at module import time
        from .dictionaries import RepeatedKeysDictionary
        new_index = RepeatedKeysDictionary(self.transaction)
        # Regenerate index on creation
        if not self.empty:
            for v in self.as_iterable():
                new_index = new_index.common_add(v)

        new_indexes = self.indexes.set_at(field_name, new_index)

        return Set(
            content=self.content,
            indexes=new_indexes,
            transaction=self.transaction
        )

    def remove_index(self, field_name: str):
        if self.indexes and self.indexes.has(field_name):
            new_indexes = self.indexes.remove_at(field_name)

            return Set(
                content=self.content,
                indexes=new_indexes,
                transaction=self.transaction
            )
        else:
            return self

    def has(self, key: object) -> bool:
        """
        Checks whether the specified `key` exists in the `Set`.

        :param key: The object to search for in the set. This can be an instance of `Atom`.
        :return: `True` if the key exists in the set, otherwise `False`.
        """
        # Calculate the canonical hash of the key
        item_hash = self._hash_of(key)

        # Check if the computed hash exists in the `HashDictionary`.
        self._load()

        if self._new_objects.has(item_hash):
            return True
        else:
            return self.content.has(item_hash)

    def add(self, key: object) -> Set:
        """
        Adds the specified `key` to the `Set`, creating and returning a new `Set`
        that includes the newly added key.

        The current `Set` instance remains immutable, and instead, a new instance is returned.

        :param key: The object to add to the set. This can be an instance of `Atom`.
        :return: A new `Set` object that contains the additional key.
        """

        # Create and return a new `Set` with the updated `HashDictionary`.
        self._load()

        if self.has(key):
            return self

        self._new_objects = self._new_objects.set_at(self._hash_of(key), key)

        new_indexes = self.indexes
        if self.indexes:
            new_indexes = self.indexes.add2indexes(key)

        return Set(
            content=self.content,
            new_objects=self._new_objects,
            transaction=self.transaction,
            indexes=new_indexes
        )

    def remove_at(self, key: object) -> Set:
        """
        Removes the specified `key` from the `Set`, creating and returning a new `Set`
        that excludes the specified key. Returns the same set if the key does not exist.

        :param key: The object to remove from the set. This can be an instance of `Atom`.
        :return: A new `Set` object with the key removed, or unchanged if the key is absent.
        """
        self._load()

        # Calculate the canonical hash of the key for removal
        item_hash = self._hash_of(key)

        if not self.has(key):
            return self

        new_objects = self._new_objects
        new_content = self.content
        if self._new_objects.has(item_hash):
            new_objects = self._new_objects.remove_at(item_hash)
        else:
            new_content = self.content.remove_at(item_hash)

        # Create and return a new `Set` with the updated `HashDictionary`.
        new_indexes = self.indexes
        if new_indexes:
            new_indexes = new_indexes.remove_from_indexes(key)

        return Set(
            content=new_content,
            new_objects=new_objects,
            transaction=self.transaction,
            indexes=new_indexes
        )

    def union(self, other: Set) -> Set:
        """
        Creates a new set containing all elements from both this set and the other set.

        :param other: Another Set to union with this one.
        :return: A new Set containing all elements from both sets.
        """
        self._load()
        other._load()

        result = self
        for item in other.as_iterable():
            result = result.add(item)

        return result

    def intersection(self, other: Set) -> Set:
        """
        Creates a new set containing only elements that are present in both this set and the other set.

        :param other: Another Set to intersect with this one.
        :return: A new Set containing only elements present in both sets.
        """
        self._load()
        other._load()

        result = Set(transaction=self.transaction)
        for item in self.as_iterable():
            item_hash = self._hash_of(item)

            if other.has(item):
                result = result.add(item)

        return result

    def difference(self, other: Set) -> Set:
        """
        Creates a new set containing elements that are in this set but not in the other set.

        :param other: Another Set to subtract from this one.
        :return: A new Set containing elements in this set that are not in the other set.
        """
        self._load()
        other._load()

        result = Set(transaction=self.transaction)
        for item in self.as_iterable():
            item_hash = self._hash_of(item)

            if not other.has(item):
                result = result.add(item)

        return result



class CountedSet(Set):
    """
    A multiset variant of Set that counts occurrences internally while preserving Set's external semantics:
    - Iteration yields unique items (no duplicates)
    - count reflects the number of unique items
    - total_count reflects the total number of occurrences (sum of per-item counters)

    Index update semantics:
    - On first insertion (0 -> 1) of a key, add2indexes(key) is invoked
    - On last removal (1 -> 0), remove_from_indexes(key) is invoked
    - Intermediate increments/decrements do not touch indexes
    """

    # _new_objects is a temporary storage for objects that have not been saved yet.
    # This is used to ensure that the set will not be persisted in storage until commit.
    # This logic enables the creation of temporary sets inside the transaction, that will
    # be persisted only when the transaction is committed. If the set is not part of
    # the finally committed objects, no storage will be consumed.
    _new_objects: HashDictionary = None
    _new_counts: HashDictionary = None

    def __init__(self,
                 items: HashDictionary | None = None,
                 counts: HashDictionary | None = None,
                 new_objects: HashDictionary | None = None,
                 new_counts: HashDictionary | None = None,
                 transaction: AbstractTransaction = None,
                 atom_pointer: AtomPointer = None,
                 indexes: DBCollections | None = None,
                 **kwargs):
        super().__init__(content=items if items is not None else HashDictionary(transaction=transaction),
                         transaction=transaction, atom_pointer=atom_pointer, indexes=indexes, **kwargs)
        # Internals
        self.items: HashDictionary = self.content  # alias for clarity
        self.counts: HashDictionary = counts if counts is not None else HashDictionary(transaction=transaction)
        self._new_objects = new_objects if new_objects is not None else HashDictionary(transaction=transaction)
        # Unique count mirrors items.count
        self._new_counts = new_counts if new_counts is not None else HashDictionary(transaction=transaction)
        # Compute total_count from counts if not set

        self.count = self.items.count + self._new_objects.count

    # Hashing strategy identical to Set
    def _hash_of(self, key: object) -> int:
        if isinstance(key, Atom):
            return key.hash()
        return hash(key)

    def _save(self):
        if not self._saved:
            super()._save()
            for element in self._new_objects:
                if isinstance(element, Atom):
                    element._save()
                hash_index = self._hash_of(element)
                self.items = self.items.set_at(hash_index, element)
                self.counts = self.counts.set_at(hash_index,
                                                 (cast(int, self._new_counts.get_at(hash_index)) or 0) + 1)

            # Save both dictionaries
            self.items.transaction = self.transaction
            self.items._save()
            self.counts.transaction = self.transaction
            self.counts._save()

    # External API
    def as_iterable(self):
        self._load()
        for item in self._new_objects:
            yield item

        for h, item in self.items.as_iterable():
            yield item

    def __iter__(self):
        return iter(self.as_iterable())

    def as_query_plan(self) -> QueryPlan:
        self._load()
        if self.indexes:
            return IndexedQueryPlan(base=self, indexes=self.indexes)
        return QueryPlan(base=self)

    def has(self, key: object) -> bool:
        h = self._hash_of(key)

        self._load()
        if self._new_counts.has(h):
            return True
        return self.counts.has(h)

    def get_count(self, key: object) -> int:
        h = self._hash_of(key)
        self._load()
        if self.counts.has(h):
            return cast(int, self.counts.get_at(h))
        elif self._new_counts.has(h):
            return cast(int, self._new_counts.get_at(h))
        else:
            return 0

    def add(self, key: object) -> 'CountedSet':
        h = self._hash_of(key)
        self._load()
        present = self.counts.has(h) or self._new_counts.has(h)
        if self.counts.has(h):
            new_counts = self.counts.set_at(h, cast(int, self.counts.get_at(h)) + 1)
            return CountedSet(
                items=self.items,
                counts=new_counts,
                indexes=self.indexes,
                new_objects=self._new_objects,
                new_counts=self._new_counts,
                transaction=self.transaction,
            )
            return CountedSet(
                items=self.items,
                counts=new_counts,
                indexes=self.indexes,
                new_objects=self._new_objects,
                new_counts=self._new_counts,
                transaction=self.transaction,
            )
        elif self._new_counts.has(h):
            new_new_counts = self._new_counts.set_at(h, cast(int, self._new_counts.get_at(h)) + 1)
            return CountedSet(
                items=self.items,
                counts=self.counts,
                indexes=self.indexes,
                new_objects=self._new_objects,
                new_counts=new_new_counts,
            )
            return CountedSet(
                items=self.items,
                counts=self.counts,
                indexes=self.indexes,
                new_objects=self._new_objects,
                new_counts=new_new_counts,
                transaction=self.transaction,
            )
        else:
            new_objects = self._new_objects.set_at(h, key)
            new_new_counts = self._new_counts.set_at(h, 1)
            if self.indexes:
                new_indexes = self.indexes.add2indexes(key)
            else:
                new_indexes = self.indexes
            return CountedSet(
                items=self.items,
                counts=self.counts,
                indexes=new_indexes,
                new_objects=new_objects,
                new_counts=new_new_counts,
                transaction=self.transaction,
            )

    def remove_at(self, key: object) -> 'CountedSet':
        h = self._hash_of(key)
        self._load()
        if self.counts.has(h):
            repetition = cast(int, self.counts.get_at(h)) - 1
            new_counts = self.counts
            new_items = self.items
            new_indexes = self.indexes
            if repetition > 0:
                new_counts = new_counts.set_at(h, repetition)
            else:
                new_counts = new_counts.remove_at(h)
                new_items = new_items.remove_at(h)
                new_indexes = new_indexes.remove_from_indexes(key)

            return CountedSet(
                items=new_items,
                counts=new_counts,
                new_objects=self._new_objects,
                new_counts=self._new_counts,
                transaction=self.transaction,
                indexes=self.indexes
            )
        elif self._new_counts.has(h):
            repetition = cast(int, self._new_counts.get_at(h)) - 1
            new_new_counts = self._new_counts
            new_objects = self._new_objects
            new_indexes = self.indexes
            if repetition > 0:
                new_new_counts = new_new_counts.set_at(h, repetition)
            else:
                new_counts = new_new_counts.remove_at(h)
                new_objects = new_objects.remove_at(h)
                new_indexes = new_indexes.remove_from_indexes(key)

            return CountedSet(
                items=self.items,
                counts=self.counts,
                indexes=new_indexes,
                new_objects=new_objects,
                new_counts=new_new_counts,
                transaction=self.transaction,
            )
        else:
            return self
