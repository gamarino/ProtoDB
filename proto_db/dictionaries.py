from __future__ import annotations
from typing import cast

from .exceptions import ProtoCorruptionException
from .common import Atom, DBCollections, QueryPlan, Literal, AbstractTransaction, AtomPointer
from .lists import List
from .sets import Set

import uuid
import logging


_logger = logging.getLogger(__name__)


class DictionaryItem(Atom):
    # Represents a key-value pair in a Dictionary, both durable and transaction-safe.
    key: Literal
    value: object

    def __init__(
            self,
            key: str = None,  # The key for the dictionary item.
            value: object = None,  # The value associated with the key.
            transaction: AbstractTransaction = None,  # The associated transaction.
            atom_pointer: AtomPointer = None,  # Pointer to the atom for durability/consistency.
            **kwargs):  # Additional keyword arguments.
        super().__init__(transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self.key = Literal(literal=key, transaction=transaction)  # Wrap the key as a Literal for durability.
        self.value = value  # Assign the value to the dictionary item.


class Dictionary(DBCollections):
    """
    Represents a durable, transaction-safe dictionary-like mapping between strings and values.

    Only Atoms are recommended as keys and values to ensure consistency and durability.
    Mixing other objects is not supported, and no warnings will be issued for doing so.
    """

    content: List  # Internal storage for dictionary items as a list.

    def __init__(
            self,
            content: List = None,  # List to store the content.
            transaction: AbstractTransaction = None,  # Transaction context for operations.
            atom_pointer: AtomPointer = None,  # Pointer to ensure atomicity and durability.
            **kwargs):
        super().__init__(transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self.content = content if content else List(transaction=transaction)  # Initialize content or create a new List.
        self.count = self.content.count  # Number of items in the dictionary.

    def _save(self):
        if not self._saved:
            self.content._save()
            super()._save()

    def as_iterable(self) -> list[tuple[str, object]]:
        """
        Provides an iterable generator for the dictionary's key-value pairs.

        :return: A generator yielding tuples of (key, value).
        """
        for item in self.content.as_iterable():  # Iterate through the content.
            item = (cast(DictionaryItem, item))  # Cast item to a DictionaryItem type.
            item._load()  # Ensure the item is loaded into memory.
            yield item.key.string, item.value  # Yield the key-value pair.

    def as_query_plan(self) -> QueryPlan:
        """
        Converts the dictionary into a QueryPlan, a representation for query execution.
        :return: The dictionary's query plan.
        """
        self._load()
        return self.content.as_query_plan()

    def get_at(self, key: str) -> object | None:
        """
        Gets the element at a given key exists in the dictionary.

        Uses binary search to find the key efficiently.

        :param key: The string key to be searched.
        :return: The value storea at key or None if not found
        """
        self._load()

        left = 0
        right = self.content.count - 1

        while left <= right:
            center = (left + right) // 2

            item = cast(DictionaryItem, self.content.get_at(center))
            if item and str(item.key) == key:
                if isinstance(item.value, Atom):
                    item.value._load()
                return item.value

            if str(item.key) >= key:
                right = center - 1
            else:
                left = center + 1

        return None

    def set_at(self, key: str, value: object) -> Dictionary:
        """
        Inserts or updates a key-value pair in the dictionary.

        If the key exists, updates its value and rebalances the underlying structure.
        If the key does not exist, inserts the new key-value pair at the appropriate position.

        :param key: The string key for the item being added or updated.
        :param value: The value associated with the key.
        :return: A new instance of Dictionary with the updated content.
        """
        self._load()

        left = 0
        right = self.content.count - 1
        center = 0

        while left <= right:
            center = (left + right) // 2

            item = cast(DictionaryItem, self.content.get_at(center))
            if item and str(item.key) == key:  # Check if the key already exists.
                # It's a repeated key, it's ok
                return Dictionary(
                    content=self.content.set_at(
                        center,
                        DictionaryItem(
                            key=key,
                            value=value,
                            transaction=self.transaction
                        )
                    ),
                    transaction=self.transaction
                )

            if str(item.key) > key:
                right = center - 1
            else:
                left = center + 1

        return Dictionary(
            content=self.content.insert_at(
                left,
                DictionaryItem(
                    key=key,
                    value=value,
                    transaction=self.transaction
                )
            ),
            transaction=self.transaction
        )

    def remove_at(self, key: str) -> Dictionary:
        """
        Removes a key-value pair from the dictionary if the key exists.

        If the key is found, it removes the corresponding entry and rebalances the structure.
        If the key does not exist, the method returns the original dictionary.

        :param key: The string key of the item to be removed.
        :return: A new instance of Dictionary reflecting the removal.
        """
        self._load()

        left = 0
        right = self.content.count - 1

        while left <= right:
            center = (left + right) // 2

            item = cast(DictionaryItem, self.content.get_at(center))
            if item and str(item.key) == key:
                # It's a replacement of an existing value
                return Dictionary(
                    content=self.content.remove_at(center),
                    transaction=self.transaction
                )

            if str(item.key) > key:
                right = center - 1
            else:
                left = center + 1

        # Not found, nothing is changed
        return self

    def has(self, key: str) -> bool:
        """
        Checks whether a given key exists in the dictionary.

        Uses binary search to find the key efficiently.

        :param key: The string key to be searched.
        :return: True if the key is found; otherwise, False.
        """
        """
        Test for key

        :param key:
        :return: True if key is in the dictionary, False otherwise
        """
        self._load()

        left = 0
        right = self.content.count - 1

        while left <= right:
            center = (left + right) // 2

            item = cast(DictionaryItem, self.content.get_at(center))
            if item and str(item.key) == key:
                return True

            if str(item.key) > key:
                right = center - 1
            else:
                left = center + 1

        return False


class RepeatedKeysDictionary(Dictionary):
    """
    Represents a dictionary-like data structure allowing multiple records
    associated with a single key.

    This class extends the base Dictionary class and provides additional
    functionality for handling repeated keys, updating, and removing
    associated records. Duplicate values in the list associated with a key
    are allowed.

    :ivar transaction: Reference to the transactional context of the dictionary.
    :type transaction: Transaction
    """
    def get_at(self, key: str) -> Set | None:
        """
        Gets the elements at a given key, as a Set, if exists in the dictionary.

        :param key: The string key to be searched.
        :return: The value storea at key or None if not found
        """
        return super().get_at(key)

    def set_at(self, key: str, value: Atom) -> Dictionary:
        """
        Inserts or updates a key-value pair in the dictionary.

        This method checks if the specified key already exists in the dictionary. If the key exists,
        its associated record list is updated with the new value

        :param key: The string key for the item being added or updated.
        :param value: The value associated with the key.
        :return: A new instance of Dictionary with the updated content.
        """
        if super().has(key):
            record_list = cast(Set, super().get_at(key))
        else:
            record_list  = Set(transaction=self.transaction)
        record_list = record_list.add(value)
        return super().set_at(key, record_list)

    def remove_at(self, key: str) -> Dictionary:
        """
        Removes a key-value pair from the dictionary if the key exists.

        If the key is found, it removes the corresponding entry and rebalances the structure.
        If the key does not exist, the method returns the original dictionary.

        :param key: The string key of the item to be removed.
        :return: A new instance of Dictionary reflecting the removal.
        """
        if super().has(key):
            return super().remove_at(key)
        else:
            return self

    def remove_record_at(self, key: str, record: Atom) -> Dictionary:
        """
        Removes a specific record from a list associated with a given key. If the key exists
           and the record is found within the associated set, it removes the record and updates
           the stored list. If the key does not exist, the original dictionary remains unchanged.

        :param key: The key in the dictionary whose associated set must be updated (e.g., removal
           of a specified record).
        :type key: str
        :param record: The specific record to be removed from the set of associated with the
           provided key.
        :return: Returns the updated dictionary object with the set containing the record
           removed. If the key does not exist or the record is not found, the original dictionary
           remains unchanged.
        :rtype: Dictionary
        """
        if super().has(key):
            record_set = cast(Set, super().get_at(key))
            record_hash = record.hash()
            if record_set.has(record_hash):
                record_set = record_set.remove_at(record_hash)
                return super().set_at(key, record_set)

        return self
