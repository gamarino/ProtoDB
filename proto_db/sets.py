from __future__ import annotations

import uuid

from .common import Atom, QueryPlan
from .dictionaries import HashDictionary


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

    def __init__(self,
                 content: HashDictionary = None,
                 transaction_id: uuid.UUID = None,
                 offset: int = 0,
                 **kwargs):
        """
        Initializes a `Set` instance.

        :param content: The `HashDictionary` instance that represents the underlying storage of the set.
        :param transaction_id: (Optional) A unique transaction identifier for audit or rollback use.
        :param offset: An optional offset for identifying the set's relative position in an operation.
        :param kwargs: Any additional data passed for extended configurations.
        """
        super().__init__(transaction_id=transaction_id, offset=offset)
        self.content = content if content else HashDictionary()  # Store the underlying hash-based dictionary.
        self.count = self.content.count

    def as_iterable(self) -> list[Atom]:
        """
        Converts the `Set` to an iterable structure, essentially a collection of its unique
        elements, and yields each element stored in the set.

        :return: A generator containing all the elements (`Atom`) in the set.
        """
        # Iterate over the stored hash dictionary's iterable and yield its items (the stored Atoms).
        for hash_value, item in self.content.as_iterable():
            yield item

    def as_query_plan(self) -> QueryPlan:
        """
        Converts the `Set` into a `QueryPlan` object for integration with larger query
        execution frameworks. Delegates the conversion to the underlying `HashDictionary`.

        :return: A `QueryPlan` representation of the current Set.
        """
        return self.content.as_query_plan()

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
        return Set(
            content=self.content.set_at(item_hash, key),  # Add key-hash to the dictionary.
        )

    def remove_at(self, key: object) -> Set:
        """
        Removes the specified `key` from the `Set`, creating and returning a new `Set`
        that excludes the specified key. Returns the same set if the key does not exist.

        :param key: The object to remove from the set. This can be an instance of `Atom`.
        :return: A new `Set` object with the key removed, or unchanged if the key is absent.
        """
        # Calculate the hash of the key for removal; handle `Atom` objects.
        if isinstance(key, Atom):
            item_hash = key.hash()  # Use the `hash` method for `Atom`.
        else:
            item_hash = hash(key)  # Use the default Python hash for non-Atom objects.

        # Create and return a new `Set` with the updated `HashDictionary`.
        return Set(
            content=self.content.remove_at(item_hash),  # Remove key-hash from the dictionary.
        )
