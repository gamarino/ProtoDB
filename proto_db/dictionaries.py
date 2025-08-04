from __future__ import annotations

import logging
from typing import cast

from .common import Atom, DBCollections, QueryPlan, Literal, AbstractTransaction, AtomPointer, ConcurrentOptimized
from .exceptions import ProtoNotSupportedException
from .lists import List
from .sets import Set

_logger = logging.getLogger(__name__)


class DictionaryItem(Atom):
    # Represents a key-value pair in a Dictionary, both durable and transaction-safe.
    key: str
    value: object

    def __init__(
            self,
            key: str = None,  # The key for the dictionary item.
            value: object = None,  # The value associated with the key.
            transaction: AbstractTransaction = None,  # The associated transaction.
            atom_pointer: AtomPointer = None,  # Pointer to the atom for durability/consistency.
            **kwargs):  # Additional keyword arguments.
        super().__init__(transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self.key = key  # Wrap the key as a Literal for durability.
        self.value = value  # Assign the value to the dictionary item.

    def _load(self):
        if not self._loaded:
            super()._load()
            if isinstance(self.key, Literal):
                self.key = self.key.string
            self._loaded = True


class Dictionary(DBCollections, ConcurrentOptimized):
    """
    Represents a durable, transaction-safe dictionary-like mapping between strings and values,
    with support for concurrent modifications.

    Only Atoms are recommended as keys and values to ensure consistency and durability.
    Mixing other objects is not supported, and no warnings will be issued for doing so.
    """

    content: List  # Internal storage for dictionary items as a list.
    _op_log: list

    def __init__(
            self,
            content: List = None,  # List to store the content.
            transaction: AbstractTransaction = None,  # Transaction context for operations.
            atom_pointer: AtomPointer = None,  # Pointer to ensure atomicity and durability.
            op_log: list = None,
            **kwargs):
        super().__init__(transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self.content = content if content else List(transaction=transaction)  # Initialize content or create a new List.
        self.count = self.content.count  # Number of items in the dictionary.
        self._op_log = op_log if op_log is not None else []

    def _load(self):
        if not self._loaded:
            super()._load()
            self.content._load()
            self._loaded = True

    def _save(self):
        self._load()
        if not self._saved:
            self.content.transaction = self.transaction
            self.content._save()
            super()._save()
            self._saved = True

    def as_iterable(self) -> list[tuple[str, object]]:
        """
        Provides an iterable generator for the dictionary's key-value pairs.

        :return: A generator yielding tuples of (key, value).
        """
        for item in self.content.as_iterable():  # Iterate through the content.
            item = (cast(DictionaryItem, item))  # Cast item to a DictionaryItem type.
            item._load()  # Ensure the item is loaded into memory.
            yield item.key, item.value  # Yield the key-value pair.

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
        self.content._load()

        left = 0
        right = self.content.count - 1

        while left <= right:
            center = (left + right) // 2

            item = cast(DictionaryItem, self.content.get_at(center))
            if item and str(item.key) == key:
                if isinstance(item.value, Atom):
                    item.value._load()
                return item.value

            if item and str(item.key) >= key:
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
        new_content = self.content

        while left <= right:
            center = (left + right) // 2

            item = cast(DictionaryItem, self.content.get_at(center))
            if item and str(item.key) == key:  # Check if the key already exists.
                new_content = new_content.set_at(
                    center,
                    DictionaryItem(
                        key=key,
                        value=value,
                        transaction=self.transaction
                    )
                )
                break
            if item and str(item.key) > key:
                right = center - 1
            else:
                left = center + 1
        else:
            new_content = new_content.insert_at(
                left,
                DictionaryItem(
                    key=key,
                    value=value,
                    transaction=self.transaction
                )
            )

        new_op_log = self._op_log + [('set', key, value)]

        return Dictionary(
            content=new_content,
            transaction=self.transaction,
            op_log=new_op_log
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
                new_content = self.content.remove_at(center)
                if new_content is None:
                    # If the content is None, create an empty dictionary
                    new_content = List(transaction=self.transaction)
                new_op_log = self._op_log + [('remove', key, None)]
                return Dictionary(
                    content=new_content,
                    transaction=self.transaction,
                    op_log=new_op_log
                )

            if item and str(item.key) > key:
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
        self._load()

        left = 0
        right = self.content.count - 1

        while left <= right:
            center = (left + right) // 2

            item = cast(DictionaryItem, self.content.get_at(center))
            if item and str(item.key) == key:
                return True

            if item and str(item.key) > key:
                right = center - 1
            else:
                left = center + 1

        return False

    def _rebase_on_concurrent_update(self, current_db_object: Atom) -> Atom:
        if not isinstance(current_db_object, Dictionary):
            raise ProtoNotSupportedException(
                "Cannot rebase onto a different object type."
            )

        rebased_dict = cast(Dictionary, current_db_object)
        rebased_dict._op_log = []

        for op_type, key, value in self._op_log:
            if op_type == 'set':
                rebased_dict = rebased_dict.set_at(key, value)
            elif op_type == 'remove':
                rebased_dict = rebased_dict.remove_at(key)
            else:
                raise ProtoNotSupportedException(
                    f"Unknown operation '{op_type}' during rebase."
                )
        return rebased_dict


class RepeatedKeysDictionary(Dictionary):
    """
    Represents a dictionary-like data structure allowing multiple records
    associated with a single key, with support for concurrent modifications.

    This class extends the base Dictionary class and provides additional
    functionality for handling repeated keys, updating, and removing
    associated records. Duplicate values in the list associated with a key
    are allowed.
    """

    def __init__(
            self,
            content: List = None,
            transaction: AbstractTransaction = None,
            atom_pointer: AtomPointer = None,
            op_log: list = None,
            **kwargs):
        super().__init__(content=content, transaction=transaction, atom_pointer=atom_pointer, op_log=op_log, **kwargs)

    def get_at(self, key: str) -> Set | None:
        """
        Gets the elements at a given key, as a Set, if exists in the dictionary.

        :param key: The string key to be searched.
        :return: The value storea at key or None if not found
        """
        return super().get_at(key)

    def set_at(self, key: str, value: Atom) -> RepeatedKeysDictionary:
        """
        Registers an intent to insert or update a key-value pair in the dictionary.

        This method checks if the specified key already exists in the dictionary. If the key exists,
        its associated record list is updated with the new value

        :param key: The string key for the item being added or updated.
        :param value: The value associated with the key.
        :return: A new instance of Dictionary with the updated content.
        """
        if super().has(key):
            record_list = cast(Set, super().get_at(key))
        else:
            record_list = Set(transaction=self.transaction)
        record_list = record_list.add(value)

        new_content = super(RepeatedKeysDictionary, self).set_at(key, record_list).content
        new_op_log = self._op_log + [('set', key, value)]

        return RepeatedKeysDictionary(
            content=new_content,
            transaction=self.transaction,
            op_log=new_op_log
        )

    def remove_at(self, key: str) -> RepeatedKeysDictionary:
        """
        Registers an intent to remove a key and its associated values from the dictionary.

        :param key: The string key of the item to be removed.
        :return: A new instance of Dictionary reflecting the removal.
        """
        if super().has(key):
            result = super(RepeatedKeysDictionary, self).remove_at(key)
            if result is None:
                # If the result is None, create an empty dictionary
                new_content = List(transaction=self.transaction)
            else:
                new_content = result.content
            new_op_log = self._op_log + [('remove', key, None)]
            return RepeatedKeysDictionary(
                content=new_content,
                transaction=self.transaction,
                op_log=new_op_log
            )
        else:
            return self

    def remove_record_at(self, key: str, record: Atom) -> RepeatedKeysDictionary:
        """
        Registers an intent to remove a specific record from the set associated with a given key.

        :param key: The key in the dictionary whose associated set must be updated.
        :param record: The specific record to be removed from the set.
        :return: Returns an updated dictionary object with the intended removal logged.
        """
        if super().has(key):
            record_set = cast(Set, super().get_at(key))
            record_hash = record.hash()
            if record_set.has(record_hash):
                result = record_set.remove_at(record_hash)
                if result is None:
                    # If the result is None, create an empty set
                    record_set = Set(transaction=self.transaction)
                else:
                    record_set = result
                result = super(RepeatedKeysDictionary, self).set_at(key, record_set)
                if result is None:
                    # If the result is None, create an empty dictionary
                    new_content = List(transaction=self.transaction)
                else:
                    new_content = result.content
                new_op_log = self._op_log + [('remove_record', key, record)]

                return RepeatedKeysDictionary(
                    content=new_content,
                    transaction=self.transaction,
                    op_log=new_op_log
                )

        return self

    def _rebase_on_concurrent_update(self, current_db_object: Atom) -> Atom:
        """
        Re-applies the operations from this transaction on top of the current
        database state for this object, allowing for conflict-free merges.
        """
        if not isinstance(current_db_object, RepeatedKeysDictionary):
            raise ProtoNotSupportedException(
                "Cannot rebase onto a different object type."
            )

        rebased_dict = cast(RepeatedKeysDictionary, current_db_object)
        rebased_dict._op_log = []

        for op_type, key, value in self._op_log:
            if op_type == 'set':
                rebased_dict = rebased_dict.set_at(key, value)
            elif op_type == 'remove':
                rebased_dict = rebased_dict.remove_at(key)
            elif op_type == 'remove_record':
                rebased_dict = rebased_dict.remove_record_at(key, value)
            else:
                raise ProtoNotSupportedException(
                    f"Unknown operation '{op_type}' during rebase."
                )

        return rebased_dict
