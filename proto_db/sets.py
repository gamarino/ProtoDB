from __future__ import annotations
from typing import cast, TYPE_CHECKING
from .common import Atom, QueryPlan, AbstractTransaction, AtomPointer, DBCollections
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

    def __init__(
            self,
            content: HashDictionary = None,
            transaction: AbstractTransaction = None,
            atom_pointer: AtomPointer = None,
            indexes: DBCollections | None = None,
            **kwargs):
        super().__init__(transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self.content = content if content else HashDictionary(
            transaction=transaction)  # Store the underlying hash-based dictionary.
        self.count = self.content.count
        if indexes is None:
            # Local import to avoid circular dependency
            from .dictionaries import Dictionary as _Dictionary
            self.indexes = _Dictionary(transaction=transaction)
        else:
            self.indexes = indexes

    def _save(self):
        if not self._saved:
            super()._save()
            self.content.transaction = self.transaction
            self.content._save()

    def as_iterable(self) -> list[Atom]:
        """
        Converts the `Set` to an iterable structure, essentially a collection of its unique
        elements, and yields each element stored in the set.

        :return: A generator containing all the elements (`Atom`) in the set.
        """
        # Iterate over the stored hash dictionary's iterable and yield its items (the stored Atoms).
        self._load()

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
        return self.content.as_query_plan()

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
        # Calculate the hash of the key, considering whether the key is an `Atom` or not.
        if isinstance(key, Atom):
            item_hash = key.hash()  # Use the `hash` method of the `Atom`.
        else:
            item_hash = hash(key)  # Fallback to the built-in Python hash.

        # Check if the computed hash exists in the `HashDictionary`.
        self._load()

        return self.content.has(item_hash)

    def add(self, key: object) -> Set:
        """
        Adds the specified `key` to the `Set`, creating and returning a new `Set`
        that includes the newly added key.

        The current `Set` instance remains immutable, and instead, a new instance is returned.

        :param key: The object to add to the set. This can be an instance of `Atom`.
        :return: A new `Set` object that contains the additional key.
        """
        # Calculate the hash of the key to ensure appropriate insertion; handle `Atom` objects.
        if isinstance(key, Atom):
            item_hash = key.hash()  # Use the `hash` method for `Atom`.
        else:
            item_hash = hash(key)  # Use the default Python hash for non-Atom objects.

        # Create and return a new `Set` with the updated `HashDictionary`.
        self._load()

        new_content = self.content.set_at(item_hash, key)
        new_indexes = self.indexes

        if self.indexes:
            new_indexes = self.indexes.add2indexes(key)

        return Set(
            content=new_content,
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

        # Calculate the hash of the key for removal; handle `Atom` objects.
        if isinstance(key, Atom):
            item_hash = key.hash()  # Use the `hash` method for `Atom`.
        else:
            item_hash = hash(key)  # Use the default Python hash for non-Atom objects.

        if not self.has(key):
            return self

        # Create and return a new `Set` with the updated `HashDictionary`.
        new_content = self.content.remove_at(item_hash)
        new_indexes = self.indexes
        if new_indexes:
            new_indexes = new_indexes.remove_from_indexes(key)

        return Set(
            content=new_content,
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
            if isinstance(item, Atom):
                item_hash = item.hash()
            else:
                item_hash = hash(item)

            if other.has(item_hash):
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
            if isinstance(item, Atom):
                item_hash = item.hash()
            else:
                item_hash = hash(item)

            if not other.has(item_hash):
                result = result.add(item)

        return result

from __future__ import annotations
from typing import cast, TYPE_CHECKING
from .common import Atom, QueryPlan, AbstractTransaction, AtomPointer, DBCollections
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

    def __init__(
            self,
            content: HashDictionary = None,
            transaction: AbstractTransaction = None,
            atom_pointer: AtomPointer = None,
            indexes: DBCollections | None = None,
            **kwargs):
        super().__init__(transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self.content = content if content else HashDictionary(
            transaction=transaction)  # Store the underlying hash-based dictionary.
        self.count = self.content.count
        if indexes is None:
            # Local import to avoid circular dependency
            from .dictionaries import Dictionary as _Dictionary
            self.indexes = _Dictionary(transaction=transaction)
        else:
            self.indexes = indexes

    def _save(self):
        if not self._saved:
            super()._save()
            self.content.transaction = self.transaction
            self.content._save()

    def as_iterable(self) -> list[Atom]:
        """
        Converts the `Set` to an iterable structure, essentially a collection of its unique
        elements, and yields each element stored in the set.

        :return: A generator containing all the elements (`Atom`) in the set.
        """
        # Iterate over the stored hash dictionary's iterable and yield its items (the stored Atoms).
        self._load()

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
        return self.content.as_query_plan()

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
        # Calculate the hash of the key, considering whether the key is an `Atom` or not.
        if isinstance(key, Atom):
            item_hash = key.hash()  # Use the `hash` method of the `Atom`.
        else:
            item_hash = hash(key)  # Fallback to the built-in Python hash.

        # Check if the computed hash exists in the `HashDictionary`.
        self._load()

        return self.content.has(item_hash)

    def add(self, key: object) -> Set:
        """
        Adds the specified `key` to the `Set`, creating and returning a new `Set`
        that includes the newly added key.

        The current `Set` instance remains immutable, and instead, a new instance is returned.

        :param key: The object to add to the set. This can be an instance of `Atom`.
        :return: A new `Set` object that contains the additional key.
        """
        # Calculate the hash of the key to ensure appropriate insertion; handle `Atom` objects.
        if isinstance(key, Atom):
            item_hash = key.hash()  # Use the `hash` method for `Atom`.
        else:
            item_hash = hash(key)  # Use the default Python hash for non-Atom objects.

        # Create and return a new `Set` with the updated `HashDictionary`.
        self._load()

        new_content = self.content.set_at(item_hash, key)
        new_indexes = self.indexes

        if self.indexes:
            new_indexes = self.indexes.add2indexes(key)

        return Set(
            content=new_content,
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

        # Calculate the hash of the key for removal; handle `Atom` objects.
        if isinstance(key, Atom):
            item_hash = key.hash()  # Use the `hash` method for `Atom`.
        else:
            item_hash = hash(key)  # Use the default Python hash for non-Atom objects.

        if not self.has(key):
            return self

        # Create and return a new `Set` with the updated `HashDictionary`.
        new_content = self.content.remove_at(item_hash)
        new_indexes = self.indexes
        if new_indexes:
            new_indexes = new_indexes.remove_from_indexes(key)

        return Set(
            content=new_content,
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
            if isinstance(item, Atom):
                item_hash = item.hash()
            else:
                item_hash = hash(item)

            if other.has(item_hash):
                result = result.add(item)

        return result

    def difference(self, other: Set) -> Set:
        """
        Creates a new set containing only elements that are present in this set but not in the other set.

        :param other: Another Set to differentiate from this one.
        :return: A new Set containing elements present in this set but not in the other set.
        """
        self._load()
        other._load()

        result = Set(transaction=self.transaction)
        for item in self.as_iterable():
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
    def __init__(self,
                 items: HashDictionary | None = None,
                 counts: HashDictionary | None = None,
                 transaction: AbstractTransaction = None,
                 atom_pointer: AtomPointer = None,
                 indexes: DBCollections | None = None,
                 **kwargs):
        super().__init__(content=items if items is not None else HashDictionary(transaction=transaction),
                         transaction=transaction, atom_pointer=atom_pointer, indexes=indexes, **kwargs)
        # Internals
        self.items: HashDictionary = self.content  # alias for clarity
        self.counts: HashDictionary = counts if counts is not None else HashDictionary(transaction=transaction)
        # Unique count mirrors items.count
        self.count = self.items.count
        # Compute total_count from counts if not set
        try:
            tc = 0
            for h, v in self.counts.as_iterable():
                try:
                    tc += int(v or 0)
                except Exception:
                    pass
            self.total_count = tc
        except Exception:
            self.total_count = self.count  # fallback

    # Hashing strategy identical to Set
    def _hash_of(self, key: object) -> int:
        if isinstance(key, Atom):
            return key.hash()
        return hash(key)

    def _save(self):
        if not self._saved:
            super()._save()
            # Save both dictionaries
            self.items.transaction = self.transaction
            self.items._save()
            self.counts.transaction = self.transaction
            self.counts._save()

    # External API
    def as_iterable(self):
        self._load()
        for h, item in self.items.as_iterable():
            yield item

    def __iter__(self):
        return iter(self.as_iterable())

    def as_query_plan(self) -> QueryPlan:
        self._load()
        if self.indexes:
            return IndexedQueryPlan(base=self, indexes=self.indexes)
        return self.items.as_query_plan()

    def has(self, key: object) -> bool:
        h = self._hash_of(key)
        self._load()
        if not self.counts.has(h):
            return False
        c = self.counts.get_at(h)
        try:
            return bool(c) and int(c) > 0
        except Exception:
            return False

    def get_count(self, key: object) -> int:
        h = self._hash_of(key)
        self._load()
        if not self.counts.has(h):
            return 0
        try:
            return int(self.counts.get_at(h) or 0)
        except Exception:
            return 0

    def add(self, key: object) -> 'CountedSet':
        h = self._hash_of(key)
        self._load()
        present = self.counts.has(h)
        if not present:
            # First occurrence: insert in items and counts=1; update indexes
            new_items = self.items.set_at(h, key)
            new_counts = self.counts.set_at(h, 1)
            new_indexes = self.indexes
            try:
                if self.indexes:
                    # Update indexes only on first insertion
                    new_indexes = self.indexes.add2indexes(key)
            except Exception:
                new_indexes = self.indexes
            new_obj = CountedSet(items=new_items, counts=new_counts, transaction=self.transaction,
                                  indexes=new_indexes)
            new_obj.count = new_items.count
            new_obj.total_count = self.total_count + 1
            return new_obj
        # Already present: increment counter only
        cur = self.get_count(key)
        new_counts = self.counts.set_at(h, cur + 1)
        new_obj = CountedSet(items=self.items, counts=new_counts, transaction=self.transaction, indexes=self.indexes)
        new_obj.count = self.items.count  # unchanged unique count
        new_obj.total_count = self.total_count + 1
        return new_obj

    def remove_at(self, key: object) -> 'CountedSet':
        h = self._hash_of(key)
        self._load()
        if not self.counts.has(h):
            return self
        cur = self.get_count(key)
        if cur <= 1:
            # Last removal: physically remove from both dicts; update indexes
            new_items = self.items.remove_at(h)
            new_counts = self.counts.remove_at(h)
            new_indexes = self.indexes
            try:
                if new_indexes:
                    new_indexes = new_indexes.remove_from_indexes(key)
            except Exception:
                new_indexes = self.indexes
            new_obj = CountedSet(items=new_items, counts=new_counts, transaction=self.transaction,
                                  indexes=new_indexes)
            new_obj.count = new_items.count
            new_obj.total_count = max(0, self.total_count - 1)
            return new_obj
        # Intermediate removal: decrement only
        new_counts = self.counts.set_at(h, cur - 1)
        new_obj = CountedSet(items=self.items, counts=new_counts, transaction=self.transaction, indexes=self.indexes)
        new_obj.count = self.items.count
        new_obj.total_count = max(0, self.total_count - 1)
        return new_obj
